from __future__ import annotations

import os

try:  # pragma: no cover - boto3 is provided in AWS runtime
    import boto3
except Exception as exc:  # pragma: no cover - local dev without boto3
    raise RuntimeError("boto3 is required for AWS Lambda runtime") from exc

_REGION = os.getenv("AWS_REGION", "us-east-1")

sqs = boto3.client("sqs", region_name=_REGION)
ddb = boto3.client("dynamodb", region_name=_REGION)
s3 = boto3.client("s3", region_name=_REGION)
secrets = boto3.client("secretsmanager", region_name=_REGION)

__all__ = ["sqs", "ddb", "s3", "secrets"]
