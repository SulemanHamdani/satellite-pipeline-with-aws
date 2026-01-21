from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

_logger = logging.getLogger(__name__)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name."""
    return logging.getLogger(name)


def log_structured(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    run_id: str | None = None,
    tile_id: str | None = None,
    attempt: int | None = None,
    stage: str | None = None,
    dur_ms: float | None = None,
    error_code: str | None = None,
    **extra: Any,
) -> None:
    """
    Log a structured message with standard pipeline context fields.

    Args:
        logger: Logger instance to use
        level: Log level (e.g., logging.INFO)
        message: Log message
        run_id: Run identifier
        tile_id: Tile identifier (z/x/y or coord:lat,lon,zoom)
        attempt: Attempt number for this job
        stage: Processing stage (claim, fetch_imagery, upload_s3, openai, complete)
        dur_ms: Duration in milliseconds
        error_code: Error code if applicable
        **extra: Additional fields to include
    """
    data: dict[str, Any] = {"msg": message}

    if run_id is not None:
        data["run_id"] = run_id
    if tile_id is not None:
        data["tile_id"] = tile_id
    if attempt is not None:
        data["attempt"] = attempt
    if stage is not None:
        data["stage"] = stage
    if dur_ms is not None:
        data["dur_ms"] = round(dur_ms, 2)
    if error_code is not None:
        data["error_code"] = error_code

    data.update(extra)

    logger.log(level, json.dumps(data, default=str))


@contextmanager
def timed_stage(
    logger: logging.Logger,
    stage: str,
    *,
    run_id: str | None = None,
    tile_id: str | None = None,
    attempt: int | None = None,
    log_start: bool = False,
) -> Iterator[dict[str, Any]]:
    """
    Context manager for timing a processing stage.

    Usage:
        with timed_stage(logger, "fetch_imagery", run_id=rid, tile_id=tid) as ctx:
            # do work
            ctx["bytes"] = 12345  # add extra fields to completion log

    Args:
        logger: Logger instance
        stage: Stage name for logging
        run_id: Run identifier
        tile_id: Tile identifier
        attempt: Attempt number
        log_start: If True, log at INFO when entering the stage

    Yields:
        dict that can be modified to add extra fields to the completion log
    """
    extra: dict[str, Any] = {}
    start = time.perf_counter()

    if log_start:
        log_structured(
            logger,
            logging.INFO,
            f"Starting {stage}",
            run_id=run_id,
            tile_id=tile_id,
            attempt=attempt,
            stage=stage,
        )

    try:
        yield extra
        dur_ms = (time.perf_counter() - start) * 1000
        log_structured(
            logger,
            logging.INFO,
            f"Completed {stage}",
            run_id=run_id,
            tile_id=tile_id,
            attempt=attempt,
            stage=stage,
            dur_ms=dur_ms,
            **extra,
        )
    except Exception as exc:
        dur_ms = (time.perf_counter() - start) * 1000
        log_structured(
            logger,
            logging.ERROR,
            f"Failed {stage}: {exc}",
            run_id=run_id,
            tile_id=tile_id,
            attempt=attempt,
            stage=stage,
            dur_ms=dur_ms,
            error=str(exc),
            **extra,
        )
        raise


__all__ = ["get_logger", "log_structured", "timed_stage"]
