"""
Tile Worker Lambda Handler

Processes SQS messages containing tile job requests.
Each message triggers: claim → fetch imagery → upload S3 → analyze → complete/fail.

Event source: SQS queue with batch_size=1
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from pydantic import ValidationError

from pyrolysis_aws.core.config import WorkerConfig
from pyrolysis_aws.core.logging import get_logger, log_structured
from pyrolysis_aws.core.pipeline import process_tile
from pyrolysis_aws.core.schema import TileJobMessage

# Module-level config (loaded once per warm environment)
_config: WorkerConfig | None = None
_logger = get_logger(__name__)


def _get_config() -> WorkerConfig:
    """Get or load the worker config (singleton per Lambda environment)."""
    global _config
    if _config is None:
        _config = WorkerConfig.from_env()
    return _config


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda entry point for SQS tile job processing.

    Args:
        event: SQS event with Records array (batch_size=1)
        context: Lambda context with get_remaining_time_in_millis()

    Returns:
        dict with processing status
    """
    return asyncio.run(_async_handler(event, context))


async def _async_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Async implementation of the Lambda handler."""
    config = _get_config()
    records = event.get("Records", [])

    if not records:
        _logger.warning("No records in SQS event")
        return {"status": "no_records"}

    # With batch_size=1, we expect exactly one record
    record = records[0]
    message_id = record.get("messageId", "unknown")

    log_structured(
        _logger,
        logging.INFO,
        "Received SQS message",
        message_id=message_id,
    )

    try:
        # Parse and validate the message body
        body = json.loads(record.get("body", "{}"))
        message = TileJobMessage.model_validate(body)
    except (json.JSONDecodeError, ValidationError) as exc:
        log_structured(
            _logger,
            logging.ERROR,
            f"Invalid message format: {exc}",
            message_id=message_id,
        )
        # Don't retry malformed messages - they'll never succeed
        # Return success so SQS deletes it (or configure DLQ for inspection)
        return {"status": "invalid_message", "error": str(exc)}

    # Get remaining time for time budgeting
    remaining_ms = None
    if hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()

    # Process the tile (will raise on failure to trigger SQS retry)
    result = await process_tile(message, config, remaining_time_ms=remaining_ms)

    return result.model_dump()
