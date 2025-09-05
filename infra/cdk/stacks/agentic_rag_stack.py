from aws_cdk import (
    Stack, Duration, RemovalPolicy, Aws, CfnOutput, Tags,
    CustomResource,
    custom_resources as cr,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_events,
    aws_events as events,
    aws_events_targets as targets,
    aws_apigateway as apigw,
    aws_cognito as cognito,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_lambda_python_alpha as lambda_python,
)
from constructs import Construct


class AgenticRagStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Networking ---
        vpc = ec2.Vpc(self, "Vpc", max_azs=2, nat_gateways=1)
        sg_rds = ec2.SecurityGroup(self, "RdsSg", vpc=vpc, allow_all_outbound=True)
        sg_lambda = ec2.SecurityGroup(self, "LambdaSg", vpc=vpc, allow_all_outbound=True)
        sg_rds.add_ingress_rule(sg_lambda, ec2.Port.tcp(5432), "Lambda to RDS")

        # --- RDS (Postgres 16) ---
        db = rds.DatabaseInstance(
            self, "Rds",
            engine=rds.DatabaseInstanceEngine.postgres(version=rds.PostgresEngineVersion.VER_16),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
            credentials=rds.Credentials.from_generated_secret(
                username="raguser",
                secret_name=f"{Aws.STACK_NAME}/RdsCredentials",  # unique name avoids reuse
            ),
            database_name="ragdb",
            removal_policy=RemovalPolicy.DESTROY,
            publicly_accessible=False,
            security_groups=[sg_rds],
            cloudwatch_logs_exports=["postgresql"],
        )
        db_secret = db.secret or db.instance_secret

        # --- S3 bucket for documents ---
        bucket = s3.Bucket(
            self, "DocsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # --- SQS (DLQ + main queue for ingest) ---
        dlq = sqs.Queue(self, "IngestDLQ", retention_period=Duration.days(14))
        ingest_q = sqs.Queue(
            self, "IngestQueue",
            visibility_timeout=Duration.minutes(5),  # must exceed Lambda max processing time
            dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=5),
        )

        # --- API Lambda (FastAPI via Mangum) ---
        fn = lambda_python.PythonFunction(
            self, "ApiFn",
            entry="../../app",
            index="lambda_handler.py",
            handler="handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(30),
            memory_size=1024,
            vpc=vpc,
            security_groups=[sg_lambda],
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "PG_HOST": db.instance_endpoint.hostname,
                "PG_PORT": "5432",
                "PG_DB": "ragdb",
                "PG_SECRET_ARN": db_secret.secret_arn,
                "PG_SSLMODE": "require",
                "TEXT_MODEL_ID": "meta.llama3-70b-instruct-v1:0",
                "EMBED_MODEL_ID": "amazon.titan-embed-text-v2:0",
                "MAX_COSINE_DIST": "0.72",
                "MIN_CTX_HITS": "1",
                "DOC_CTX_CHUNKS": "4",
                "LOG_LEVEL": "INFO",
            },
        )

        api = apigw.RestApi(self, "Api", rest_api_name="AgenticRagApi")

        # --- Ingest Lambda (admin + S3->SQS pipeline consumer) ---
        ingest_fn = lambda_python.PythonFunction(
            self, "IngestFn",
            entry="../../app",
            index="ingest_handler.py",
            handler="handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.minutes(5),
            memory_size=1024,
            vpc=vpc,
            security_groups=[sg_lambda],
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "PG_HOST": db.instance_endpoint.hostname,
                "PG_PORT": "5432",
                "PG_DB": "ragdb",
                "PG_SECRET_ARN": db_secret.secret_arn,
                "TEXT_MODEL_ID": "anthropic.claude-3-5-sonnet-20240620-v1:0",
                "EMBED_MODEL_ID": "amazon.titan-embed-text-v2:0",
                "DOCS_BUCKET": bucket.bucket_name,
                "DEFAULT_PREFIX": "corp/",
                "LOG_LEVEL": "INFO",
            },
        )

        # SQS event source for ingest Fn
        ingest_fn.add_event_source(lambda_events.SqsEventSource(
            ingest_q,
            batch_size=5,
            max_batching_window=Duration.seconds(20),
            report_batch_item_failures=True,
        ))

        # S3 -> SQS notifications for new docs
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(ingest_q),
            s3.NotificationKeyFilter(prefix="corp/"),
        )
        bucket.grant_read(ingest_fn)

        # --- Nightly backfill (EventBridge -> IngestFn) ---
        nightly = events.Rule(
            self, "NightlyBackfill",
            schedule=events.Schedule.cron(minute="0", hour="2"),  # 02:00 UTC daily
        )
        nightly.add_target(targets.LambdaFunction(
            ingest_fn,
            event=events.RuleTargetInput.from_object({
                "mode": "scan_prefix",
                "bucket": bucket.bucket_name,
                "prefix": "corp/",  # change if your ingest prefix differs
            }),
        ))

        # --- DB schema init (Custom Resource) ---
        db_init_fn = lambda_python.PythonFunction(
            self, "DbInitFn",
            entry="../../app",
            index="db_init_handler.py",
            handler="handler",
            runtime=_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.minutes(2),
            memory_size=512,
            vpc=vpc,
            security_groups=[sg_lambda],
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "PG_HOST": db.instance_endpoint.hostname,
                "PG_PORT": "5432",
                "PG_DB": "ragdb",
                "PG_SSLMODE": "require",
                "PG_SECRET_ARN": db_secret.secret_arn,
            },
        )
        db_secret.grant_read(db_init_fn)

        provider = cr.Provider(self, "DbInitProvider", on_event_handler=db_init_fn)
        CustomResource(
            self, "DbInit",
            service_token=provider.service_token,
            properties={"SchemaVersion": "v2-2025-08-23"},
        )

        # === Secrets permissions (GetSecretValue) to Lambdas that need DB access ===
        for f in (fn, ingest_fn, db_init_fn):
            db_secret.grant_read(f)

        # === Bedrock permissions (embed + generate) ===
        for f in (ingest_fn, fn):  # both call Bedrock
            f.add_to_role_policy(iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=["*"],  # optionally restrict to specific model ARNs
            ))

        # --- API Gateway routing ---
        integ = apigw.LambdaIntegration(fn, proxy=True, timeout=Duration.seconds(29))

        # Keep health open
        res_health = api.root.add_resource("health")
        res_health.add_method("GET", integ)

        # Protect everything else with Cognito authorizer
        user_pool = cognito.UserPool(
            self, "UserPool",
            self_sign_up_enabled=False,
            password_policy=cognito.PasswordPolicy(
                min_length=12, require_lowercase=True, require_uppercase=True, require_digits=True, require_symbols=False
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )
        user_pool_client = cognito.UserPoolClient(
            self, "UserPoolClient",
            user_pool=user_pool,
            generate_secret=False,
            auth_flows=cognito.AuthFlow(user_password=True, admin_user_password=True),
        )
        authorizer = apigw.CognitoUserPoolsAuthorizer(self, "Authorizer", cognito_user_pools=[user_pool])

        # --- Admin route: POST /admin/ingest -> IngestFn (Cognito-protected) ---
        res_admin = api.root.add_resource("admin")
        res_ingest = res_admin.add_resource("ingest")
        res_ingest.add_method(
            "POST",
            apigw.LambdaIntegration(ingest_fn, proxy=True),
            authorizer=authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO
        )

        proxy = api.root.add_resource("{proxy+}")
        proxy.add_method("ANY", integ, authorizer=authorizer, authorization_type=apigw.AuthorizationType.COGNITO)
        api.root.add_method("ANY", integ, authorizer=authorizer, authorization_type=apigw.AuthorizationType.COGNITO)

        # --- CloudWatch Dashboard ---
        dash = cloudwatch.Dashboard(self, "OpsDashboard", dashboard_name=f"AgenticRag-{Aws.STACK_NAME}")

        dash.add_widgets(
            cloudwatch.GraphWidget(
                title="ApiFn Duration p95 (ms)",
                left=[cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Duration",
                    dimensions_map={"FunctionName": fn.function_name},
                    statistic="p95",
                    period=Duration.minutes(5),
                )]
            ),
            cloudwatch.GraphWidget(
                title="ApiFn Errors",
                left=[cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Errors",
                    dimensions_map={"FunctionName": fn.function_name},
                    statistic="Sum",
                    period=Duration.minutes(5),
                )]
            ),
        )

        dash.add_widgets(
            cloudwatch.GraphWidget(
                title="IngestFn Errors",
                left=[cloudwatch.Metric(
                    namespace="AWS/Lambda",
                    metric_name="Errors",
                    dimensions_map={"FunctionName": ingest_fn.function_name},
                    statistic="Sum",
                    period=Duration.minutes(5),
                )]
            )
        )

        # Simple alarm on API errors
        api_errors = cloudwatch.Metric(
            namespace="AWS/Lambda",
            metric_name="Errors",
            dimensions_map={"FunctionName": fn.function_name},
            statistic="Sum",
            period=Duration.minutes(5),
        )
        cloudwatch.Alarm(
            self, "ApiFnErrorsAlarm",
            metric=api_errors,
            threshold=1,
            evaluation_periods=1,
            datapoints_to_alarm=1,
        )

        # --- Bastion (SSM-managed) ---
        bastion_sg = ec2.SecurityGroup(
            self, "BastionSg",
            vpc=vpc,
            description="SSM bastion SG",
            allow_all_outbound=True,
        )
        db.connections.allow_from(bastion_sg, ec2.Port.tcp(5432), "Allow bastion to Postgres")

        bastion_role = iam.Role(
            self, "BastionRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
            ],
        )
        bastion = ec2.Instance(
            self, "Bastion",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            instance_type=ec2.InstanceType("t3.micro"),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(),
            role=bastion_role,
            security_group=bastion_sg,
        )

        # Clean tagging
        Tags.of(bastion).add("Name", "AgenticRagBastion")

        # --- Outputs ---
        CfnOutput(self, "BastionInstanceId", value=bastion.instance_id)
        CfnOutput(self, "ApiUrl", value=api.url)
        CfnOutput(self, "DocsBucketName", value=bucket.bucket_name)
        CfnOutput(self, "DbEndpoint", value=db.instance_endpoint.hostname)
        CfnOutput(self, "UserPoolId", value=user_pool.user_pool_id)
        CfnOutput(self, "UserPoolClientId", value=user_pool_client.user_pool_client_id)
