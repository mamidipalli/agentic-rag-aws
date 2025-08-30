#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.agentic_rag_stack import AgenticRagStack

def main() -> None:
    app = cdk.App()
    AgenticRagStack(
        app,
        "AgenticRagStack",
        env=cdk.Environment(
            account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
            region=os.environ.get("CDK_DEFAULT_REGION") or "ap-south-1",
        ),
    )
    app.synth()

if __name__ == "__main__":
    main()
