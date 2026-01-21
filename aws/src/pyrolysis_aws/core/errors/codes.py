from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """
    Standardized error codes for tile job failures.

    These codes are stored in TileJobs.error_code for fast debugging
    via CloudWatch and DynamoDB queries.
    """

    # Mapbox errors
    MAPBOX_429 = "MAPBOX_429"
    MAPBOX_5XX = "MAPBOX_5XX"
    MAPBOX_4XX = "MAPBOX_4XX"
    MAPBOX_BAD_REQUEST = "MAPBOX_BAD_REQUEST"
    MAPBOX_TIMEOUT = "MAPBOX_TIMEOUT"

    # Google errors
    GOOGLE_429 = "GOOGLE_429"
    GOOGLE_5XX = "GOOGLE_5XX"
    GOOGLE_4XX = "GOOGLE_4XX"
    GOOGLE_BAD_REQUEST = "GOOGLE_BAD_REQUEST"
    GOOGLE_TIMEOUT = "GOOGLE_TIMEOUT"

    # OpenAI errors
    OPENAI_429 = "OPENAI_429"
    OPENAI_5XX = "OPENAI_5XX"
    OPENAI_4XX = "OPENAI_4XX"
    OPENAI_BAD_RESPONSE = "OPENAI_BAD_RESPONSE"
    OPENAI_TIMEOUT = "OPENAI_TIMEOUT"

    # S3 errors
    S3_PUT_FAILED = "S3_PUT_FAILED"
    S3_GET_FAILED = "S3_GET_FAILED"

    # Schema/validation errors
    SCHEMA_INVALID = "SCHEMA_INVALID"
    MESSAGE_PARSE_ERROR = "MESSAGE_PARSE_ERROR"

    # Processing errors
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    CLAIM_FAILED = "CLAIM_FAILED"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


def error_code_from_http_status(
    source: str,
    status_code: int,
) -> ErrorCode:
    """
    Map an HTTP status code to an ErrorCode for a given imagery source.

    Args:
        source: "mapbox", "google", or "openai"
        status_code: HTTP status code

    Returns:
        Appropriate ErrorCode enum value
    """
    source_upper = source.upper()

    if status_code == 429:
        return ErrorCode(f"{source_upper}_429")

    if 500 <= status_code < 600:
        return ErrorCode(f"{source_upper}_5XX")

    if 400 <= status_code < 500:
        if status_code == 400:
            return ErrorCode(f"{source_upper}_BAD_REQUEST")
        return ErrorCode(f"{source_upper}_4XX")

    return ErrorCode.UNKNOWN_ERROR


__all__ = ["ErrorCode", "error_code_from_http_status"]
