from __future__ import annotations

import time
from typing import Any

from botocore.exceptions import ClientError

from ..aws_clients.boto import ddb
from ..schema import RunStatus


def create_run(
    table_name: str,
    run_id: str,
    source_bucket: str,
    source_key: str,
    total_tiles: int,
    *,
    status: RunStatus = RunStatus.RUNNING,
    now_epoch: int | None = None,
) -> dict[str, Any]:
    created_at = now_epoch if now_epoch is not None else int(time.time())
    item = {
        "run_id": {"S": run_id},
        "status": {"S": status.value},
        "total_tiles": {"N": str(total_tiles)},
        "completed_tiles": {"N": "0"},
        "failed_tiles": {"N": "0"},
        "source_bucket": {"S": source_bucket},
        "source_key": {"S": source_key},
        "created_at_epoch": {"N": str(created_at)},
    }
    ddb.put_item(
        TableName=table_name,
        Item=item,
        ConditionExpression="attribute_not_exists(run_id)",
    )
    return item


def update_run_counters(
    table_name: str,
    run_id: str,
    *,
    completed_delta: int = 0,
    failed_delta: int = 0,
) -> dict[str, Any]:
    if completed_delta == 0 and failed_delta == 0:
        return {}
    response = ddb.update_item(
        TableName=table_name,
        Key={"run_id": {"S": run_id}},
        UpdateExpression="ADD completed_tiles :c, failed_tiles :f",
        ExpressionAttributeValues={
            ":c": {"N": str(completed_delta)},
            ":f": {"N": str(failed_delta)},
        },
        ReturnValues="UPDATED_NEW",
    )
    return response.get("Attributes", {})


def set_run_status(
    table_name: str,
    run_id: str,
    status: RunStatus,
    *,
    finished_at_epoch: int | None = None,
) -> dict[str, Any]:
    attrs: dict[str, Any] = {
        ":status": {"S": status.value},
    }
    update_expr = "SET #status = :status"
    if finished_at_epoch is not None:
        update_expr += ", finished_at_epoch = :finished"
        attrs[":finished"] = {"N": str(finished_at_epoch)}

    response = ddb.update_item(
        TableName=table_name,
        Key={"run_id": {"S": run_id}},
        UpdateExpression=update_expr,
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues=attrs,
        ReturnValues="UPDATED_NEW",
    )
    return response.get("Attributes", {})


def set_total_tiles(
    table_name: str,
    run_id: str,
    total_tiles: int,
) -> dict[str, Any]:
    response = ddb.update_item(
        TableName=table_name,
        Key={"run_id": {"S": run_id}},
        UpdateExpression="SET total_tiles = :total",
        ExpressionAttributeValues={":total": {"N": str(total_tiles)}},
        ReturnValues="UPDATED_NEW",
    )
    return response.get("Attributes", {})


def safe_create_run(*args: Any, **kwargs: Any) -> bool:
    try:
        create_run(*args, **kwargs)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


__all__ = [
    "create_run",
    "safe_create_run",
    "update_run_counters",
    "set_run_status",
    "set_total_tiles",
]
