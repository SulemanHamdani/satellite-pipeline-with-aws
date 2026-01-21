from __future__ import annotations

import time
from typing import Any

from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

from ..aws_clients.boto import ddb
from ..schema import ClaimResult, ClaimResultData, JobStatus, S3Checkpoint, TileJobMessage


def _job_key(run_id: str, tile_id: str) -> dict[str, dict[str, str]]:
    return {"run_id": {"S": run_id}, "tile_id": {"S": tile_id}}


def _ensure_tile_id(message: TileJobMessage) -> str:
    """Get the canonical tile_id for a message."""
    return message.get_tile_id()


_DESERIALIZER = TypeDeserializer()


def _deserialize_item(item: dict[str, Any]) -> dict[str, Any]:
    return {key: _DESERIALIZER.deserialize(value) for key, value in item.items()}


def _get_job_status(table_name: str, run_id: str, tile_id: str) -> JobStatus | None:
    """Fetch current job status, returns None if item doesn't exist."""
    try:
        response = ddb.get_item(
            TableName=table_name,
            Key=_job_key(run_id, tile_id),
            ProjectionExpression="#status",
            ExpressionAttributeNames={"#status": "status"},
        )
        item = response.get("Item")
        if item and "status" in item:
            return JobStatus(item["status"]["S"])
        return None
    except ClientError:
        return None


def claim_job(
    table_name: str,
    message: TileJobMessage,
    *,
    now_epoch: int | None = None,
    lock_seconds: int = 900,
) -> ClaimResultData:
    """
    Attempt to claim a job for processing.

    Returns:
        ClaimResultData with result status, tile_id, attempt count, and checkpoint if present.

    The caller should:
        - If result=CLAIMED: proceed with processing
        - If result=ALREADY_COMPLETED: ACK the message, skip processing
        - If result=LOCKED_BY_OTHER: let SQS retry later
    """
    now = now_epoch if now_epoch is not None else int(time.time())
    tile_id = _ensure_tile_id(message)
    lock_until = now + lock_seconds

    expr_names = {"#status": "status"}
    expr_values: dict[str, Any] = {
        ":processing": {"S": JobStatus.PROCESSING.value},
        ":pending": {"S": JobStatus.PENDING.value},
        ":failed": {"S": JobStatus.FAILED.value},
        ":now": {"N": str(now)},
        ":lock": {"N": str(lock_until)},
        ":one": {"N": "1"},
        ":zero": {"N": "0"},
        ":source": {"S": message.imagery_source},
        ":last_claimed": {"N": str(now)},
    }

    update_expr = (
        "SET #status = :processing, "
        "attempts = if_not_exists(attempts, :zero) + :one, "
        "lock_until_epoch = :lock, "
        "started_at_epoch = if_not_exists(started_at_epoch, :now), "
        "last_claimed_at_epoch = :last_claimed, "
        "imagery_source = if_not_exists(imagery_source, :source)"
    )

    if message.imagery_source == "mapbox":
        expr_values.update(
            {
                ":z": {"N": str(message.z)},
                ":x": {"N": str(message.x)},
                ":y": {"N": str(message.y)},
            }
        )
        update_expr += ", z = if_not_exists(z, :z), x = if_not_exists(x, :x), y = if_not_exists(y, :y)"
        if message.region:
            expr_values[":region"] = {"S": message.region}
            # "region" is a DynamoDB reserved keyword; use an expression attribute name.
            expr_names["#region"] = "region"
            update_expr += ", #region = if_not_exists(#region, :region)"
    else:
        expr_values.update(
            {
                ":lat": {"N": str(message.lat)},
                ":lon": {"N": str(message.lon)},
                ":zoom": {"N": str(message.zoom)},
            }
        )
        update_expr += (
            ", lat = if_not_exists(lat, :lat), "
            "lon = if_not_exists(lon, :lon), "
            "zoom = if_not_exists(zoom, :zoom)"
        )

    # Condition allows claiming if:
    # 1. Item doesn't exist (new job)
    # 2. Status is PENDING or FAILED (available)
    # 3. Status is PROCESSING but lock is stale (expired or missing)
    condition_expr = (
        "attribute_not_exists(#status) OR "
        "#status IN (:pending, :failed) OR "
        "(#status = :processing AND ("
        "attribute_not_exists(lock_until_epoch) OR lock_until_epoch < :now))"
    )

    try:
        response = ddb.update_item(
            TableName=table_name,
            Key=_job_key(message.run_id, tile_id),
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ConditionExpression=condition_expr,
            ReturnValues="ALL_NEW",
        )
        attrs = response.get("Attributes", {})
        parsed = _deserialize_item(attrs) if attrs else {}
        attempt = int(parsed.get("attempts", 0)) if parsed else None
        checkpoint = None
        s3_bucket = parsed.get("s3_bucket")
        s3_key = parsed.get("s3_key")
        if s3_bucket and s3_key:
            checkpoint = S3Checkpoint(bucket=s3_bucket, key=s3_key)
        return ClaimResultData(
            result=ClaimResult.CLAIMED,
            tile_id=tile_id,
            attempt=attempt,
            claimed_at_epoch=now,
            checkpoint=checkpoint,
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            current_status = _get_job_status(table_name, message.run_id, tile_id)
            result = (
                ClaimResult.ALREADY_COMPLETED
                if current_status == JobStatus.COMPLETED
                else ClaimResult.LOCKED_BY_OTHER
            )
            return ClaimResultData(result=result, tile_id=tile_id)
        raise


def complete_job(
    table_name: str,
    run_id: str,
    tile_id: str,
    *,
    s3_bucket: str,
    s3_key: str,
    status_ai: str | None = None,
    reasoning: str | None = None,
    openai_usage: dict | None = None,
    claimed_at_epoch: int | None = None,
    finished_at_epoch: int | None = None,
) -> None:
    finished = finished_at_epoch if finished_at_epoch is not None else int(time.time())
    expr_values: dict[str, Any] = {
        ":status": {"S": JobStatus.COMPLETED.value},
        ":finished": {"N": str(finished)},
        ":bucket": {"S": s3_bucket},
        ":key": {"S": s3_key},
    }
    update_expr = (
        "SET #status = :status, finished_at_epoch = :finished, "
        "s3_bucket = :bucket, s3_key = :key"
    )
    if status_ai is not None:
        expr_values[":status_ai"] = {"S": status_ai}
        update_expr += ", status_ai = :status_ai"
    if reasoning is not None:
        expr_values[":reasoning"] = {"S": reasoning}
        update_expr += ", reasoning = :reasoning"
    if openai_usage is not None:
        expr_values[":openai_usage"] = {"M": _to_ddb_map(openai_usage)}
        update_expr += ", openai_usage = :openai_usage"
    if claimed_at_epoch is not None:
        duration_ms = (finished - claimed_at_epoch) * 1000
        expr_values[":duration_ms"] = {"N": str(duration_ms)}
        update_expr += ", duration_ms = :duration_ms"

    ddb.update_item(
        TableName=table_name,
        Key=_job_key(run_id, tile_id),
        UpdateExpression=update_expr,
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues=expr_values,
    )


def checkpoint_s3(
    table_name: str,
    run_id: str,
    tile_id: str,
    *,
    s3_bucket: str,
    s3_key: str,
) -> dict[str, Any]:
    """
    Record S3 upload checkpoint for a job.

    Called after imagery is uploaded to S3 but before OpenAI analysis.
    On retry, if s3_key exists, the worker can skip fetch+upload and
    download from S3 instead.
    """
    response = ddb.update_item(
        TableName=table_name,
        Key=_job_key(run_id, tile_id),
        UpdateExpression="SET s3_bucket = :bucket, s3_key = :key",
        ExpressionAttributeValues={
            ":bucket": {"S": s3_bucket},
            ":key": {"S": s3_key},
        },
        ReturnValues="UPDATED_NEW",
    )
    return response.get("Attributes", {})


def fail_job(
    table_name: str,
    run_id: str,
    tile_id: str,
    *,
    error_code: str,
    error_message: str,
    finished_at_epoch: int | None = None,
) -> dict[str, Any]:
    finished = finished_at_epoch if finished_at_epoch is not None else int(time.time())
    response = ddb.update_item(
        TableName=table_name,
        Key=_job_key(run_id, tile_id),
        UpdateExpression=(
            "SET #status = :status, finished_at_epoch = :finished, "
            "error_code = :code, error_message = :message"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": JobStatus.FAILED.value},
            ":finished": {"N": str(finished)},
            ":code": {"S": error_code},
            ":message": {"S": error_message},
        },
        ReturnValues="UPDATED_NEW",
    )
    return response.get("Attributes", {})


def _to_ddb_map(payload: dict) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, str):
            converted[key] = {"S": value}
        elif isinstance(value, bool):
            converted[key] = {"BOOL": value}
        elif isinstance(value, (int, float)):
            converted[key] = {"N": str(value)}
        elif isinstance(value, dict):
            converted[key] = {"M": _to_ddb_map(value)}
        elif isinstance(value, list):
            converted[key] = {"L": [_to_ddb_value(item) for item in value]}
        else:
            converted[key] = {"S": str(value)}
    return converted


def _to_ddb_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"NULL": True}
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    if isinstance(value, dict):
        return {"M": _to_ddb_map(value)}
    if isinstance(value, list):
        return {"L": [_to_ddb_value(item) for item in value]}
    return {"S": str(value)}


__all__ = ["claim_job", "complete_job", "checkpoint_s3", "fail_job"]
