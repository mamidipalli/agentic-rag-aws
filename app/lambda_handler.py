# app/lambda_handler.py
"""
AWS Lambda entrypoint for the ASGI app using Mangum.
Keeps a single global handler for re-use across cold starts.
"""
from mangum import Mangum
from main import app

# Exported Lambda handler
handler = Mangum(app)  # type: ignore[call-arg]
