from __future__ import annotations

import logging

from ..config import WorkerConfig
from ..ddb.runs import update_run_counters
from ..ddb.tilejobs import checkpoint_s3, claim_job, complete_job, fail_job
from ..errors import ErrorCode, error_code_from_http_status
from ..imagery import ImageryFetchError, fetch_google_tile, fetch_mapbox_tile
from ..logging import get_logger, log_structured, timed_stage
from ..openai import AgentOutput, AnalysisError, analyze_image
from ..io.s3 import download_image, upload_tile_image
from ..schema import ClaimResult, ProcessTileResult, S3Checkpoint, TileJobMessage

_logger = get_logger(__name__)

# Minimum remaining time (ms) to start OpenAI call
# With 120s timeout, we want at least 20s buffer
MIN_REMAINING_MS_FOR_OPENAI = 20_000


def _get_s3_checkpoint(checkpoint: S3Checkpoint | None) -> tuple[str, str] | None:
    """Extract S3 checkpoint if present."""
    if checkpoint is None:
        return None
    return checkpoint.bucket, checkpoint.key


async def process_tile(
    message: TileJobMessage,
    config: WorkerConfig,
    *,
    remaining_time_ms: int | None = None,
) -> ProcessTileResult:
    """
    Process a single tile job.

    This is the core orchestration function called by the Lambda handler.
    Supports checkpointing: if S3 upload succeeded on a previous attempt,
    skips fetch+upload and downloads from S3 instead.

    Args:
        message: Parsed and validated TileJobMessage from SQS
        config: Worker configuration
        remaining_time_ms: Lambda remaining time in ms (for time budgeting)

    Returns:
        dict with processing result info

    Raises:
        Exception: On failure (to trigger SQS retry)
    """
    run_id = message.run_id
    tile_id = message.get_tile_id()
    log_ctx = {"run_id": run_id, "tile_id": tile_id}

    log_structured(_logger, logging.INFO, "Processing tile", **log_ctx)

    # Step 1: Claim the job (idempotent)
    with timed_stage(_logger, "claim", run_id=run_id, tile_id=tile_id):
        claim_result = claim_job(
            config.tilejobs_table,
            message,
            lock_seconds=config.job_stale_lock_seconds,
        )

    if claim_result.result == ClaimResult.ALREADY_COMPLETED:
        log_structured(
            _logger,
            logging.INFO,
            "Job already completed, skipping",
            **log_ctx,
        )
        return ProcessTileResult(
            status="skipped",
            tile_id=tile_id,
            reason="already_completed",
        )

    if claim_result.result == ClaimResult.LOCKED_BY_OTHER:
        log_structured(
            _logger,
            logging.INFO,
            "Job locked by another worker, letting SQS retry",
            **log_ctx,
        )
        raise RuntimeError("Job locked by another worker")

    attempt = claim_result.attempt or 1
    claimed_at_epoch = claim_result.claimed_at_epoch
    log_ctx["attempt"] = attempt

    try:
        # Step 2: Get image bytes (from checkpoint or fresh fetch)
        checkpoint = _get_s3_checkpoint(claim_result.checkpoint)

        if checkpoint:
            s3_bucket, s3_key = checkpoint
            log_structured(
                _logger,
                logging.INFO,
                "S3 checkpoint found, downloading instead of fetching",
                **log_ctx,
            )
            image_bytes = await _download_from_checkpoint(s3_bucket, s3_key, log_ctx)
        else:
            image_bytes = await _fetch_imagery(message, config, log_ctx)
            s3_bucket, s3_key = await _upload_to_s3(message, image_bytes, config, log_ctx)

            # Checkpoint: record S3 location before OpenAI call
            with timed_stage(_logger, "checkpoint_s3", **log_ctx):
                checkpoint_s3(config.tilejobs_table, run_id, tile_id, s3_bucket=s3_bucket, s3_key=s3_key)

        # Step 3: Check time budget before OpenAI call
        if remaining_time_ms is not None and remaining_time_ms < MIN_REMAINING_MS_FOR_OPENAI:
            raise TimeoutError(
                f"Only {remaining_time_ms}ms remaining, aborting before OpenAI call"
            )

        # Step 4: Analyze with OpenAI
        agent_output, usage_dict = await _analyze_with_openai(image_bytes, config, log_ctx)

        # Step 5: Complete the job
        with timed_stage(_logger, "complete_job", **log_ctx):
            complete_job(
                config.tilejobs_table,
                run_id,
                tile_id,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                status_ai=agent_output.status.value,
                reasoning=agent_output.reasoning,
                openai_usage=usage_dict,
                claimed_at_epoch=claimed_at_epoch,
            )

        # Step 6: Update run counters
        with timed_stage(_logger, "update_counters", **log_ctx):
            update_run_counters(config.runs_table, run_id, completed_delta=1)

        log_structured(
            _logger,
            logging.INFO,
            "Tile processed successfully",
            status_ai=agent_output.status.value,
            **log_ctx,
        )

        return ProcessTileResult(
            status="completed",
            tile_id=tile_id,
            status_ai=agent_output.status.value,
            s3_key=s3_key,
        )

    except Exception as exc:
        await _handle_failure(exc, message, config, log_ctx)
        raise  # Re-raise to trigger SQS retry


async def _download_from_checkpoint(
    s3_bucket: str,
    s3_key: str,
    log_ctx: dict,
) -> bytes:
    """Download image from S3 checkpoint."""
    with timed_stage(
        _logger,
        "download_s3",
        run_id=log_ctx.get("run_id"),
        tile_id=log_ctx.get("tile_id"),
        attempt=log_ctx.get("attempt"),
    ) as ctx:
        image_bytes = await download_image(s3_bucket, s3_key)
        ctx["bytes"] = len(image_bytes)
        return image_bytes


async def _fetch_imagery(
    message: TileJobMessage,
    config: WorkerConfig,
    log_ctx: dict,
) -> bytes:
    """Fetch imagery from Mapbox or Google based on message source."""
    with timed_stage(
        _logger,
        "fetch_imagery",
        run_id=log_ctx.get("run_id"),
        tile_id=log_ctx.get("tile_id"),
        attempt=log_ctx.get("attempt"),
    ) as ctx:
        if message.imagery_source == "mapbox":
            image_bytes = await fetch_mapbox_tile(
                message.z,
                message.x,
                message.y,
                secrets_id=config.secrets_id,
                max_retries=config.max_retries,
                timeout=config.request_timeout,
            )
        else:
            image_bytes = await fetch_google_tile(
                message.lat,
                message.lon,
                message.get_zoom(),
                secrets_id=config.secrets_id,
                max_retries=config.max_retries,
                timeout=config.request_timeout,
            )
        ctx["bytes"] = len(image_bytes)
        return image_bytes


async def _upload_to_s3(
    message: TileJobMessage,
    image_bytes: bytes,
    config: WorkerConfig,
    log_ctx: dict,
) -> tuple[str, str]:
    """Upload imagery to S3 with deterministic key."""
    with timed_stage(
        _logger,
        "upload_s3",
        run_id=log_ctx.get("run_id"),
        tile_id=log_ctx.get("tile_id"),
        attempt=log_ctx.get("attempt"),
    ) as ctx:
        if message.imagery_source == "mapbox":
            bucket, key = await upload_tile_image(
                image_bytes,
                bucket=config.s3_bucket,
                run_id=message.run_id,
                imagery_source="mapbox",
                z=message.z,
                x=message.x,
                y=message.y,
            )
        else:
            bucket, key = await upload_tile_image(
                image_bytes,
                bucket=config.s3_bucket,
                run_id=message.run_id,
                imagery_source="google",
                lat=message.lat,
                lon=message.lon,
                zoom=message.get_zoom(),
            )
        ctx["s3_key"] = key
        return bucket, key


async def _analyze_with_openai(
    image_bytes: bytes,
    config: WorkerConfig,
    log_ctx: dict,
) -> tuple[AgentOutput, dict]:
    """Analyze image with OpenAI vision model."""
    with timed_stage(
        _logger,
        "openai",
        run_id=log_ctx.get("run_id"),
        tile_id=log_ctx.get("tile_id"),
        attempt=log_ctx.get("attempt"),
    ) as ctx:
        agent_output, usage_dict = await analyze_image(
            image_bytes,
            secrets_id=config.secrets_id,
        )
        ctx["status_ai"] = agent_output.status.value
        ctx["tokens"] = usage_dict.get("total_tokens")
        return agent_output, usage_dict


async def _handle_failure(
    exc: Exception,
    message: TileJobMessage,
    config: WorkerConfig,
    log_ctx: dict,
) -> None:
    """Handle tile processing failure: update DynamoDB, log error."""
    run_id = message.run_id
    tile_id = message.get_tile_id()

    error_code = _exception_to_error_code(exc, message.imagery_source)
    error_message = str(exc)[:500]

    log_structured(
        _logger,
        logging.ERROR,
        f"Tile processing failed: {error_message}",
        error_code=error_code.value,
        **log_ctx,
    )

    try:
        fail_job(
            config.tilejobs_table,
            run_id,
            tile_id,
            error_code=error_code.value,
            error_message=error_message,
        )
        update_run_counters(config.runs_table, run_id, failed_delta=1)
    except Exception as ddb_exc:
        log_structured(
            _logger,
            logging.ERROR,
            f"Failed to update DynamoDB on failure: {ddb_exc}",
            **log_ctx,
        )


def _exception_to_error_code(exc: Exception, imagery_source: str) -> ErrorCode:  # noqa: ARG001
    """Map an exception to an ErrorCode for DynamoDB storage."""
    if isinstance(exc, ImageryFetchError):
        if exc.status_code is not None:
            return error_code_from_http_status(exc.source, exc.status_code)
        return ErrorCode(f"{exc.source.upper()}_TIMEOUT")

    if isinstance(exc, AnalysisError):
        return ErrorCode.OPENAI_BAD_RESPONSE

    if isinstance(exc, TimeoutError):
        return ErrorCode.DEADLINE_EXCEEDED

    if "S3" in type(exc).__name__ or "s3" in str(exc).lower():
        return ErrorCode.S3_PUT_FAILED

    return ErrorCode.UNKNOWN_ERROR


__all__ = ["process_tile"]
