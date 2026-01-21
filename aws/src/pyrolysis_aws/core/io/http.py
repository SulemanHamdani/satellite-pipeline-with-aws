from __future__ import annotations

import time
from typing import Iterable

try:  # pragma: no cover - requests may be packaged with the lambda
    import requests
except Exception as exc:  # pragma: no cover - local dev without requests
    raise RuntimeError("requests is required for HTTP calls in Lambda") from exc

_SESSION = requests.Session()
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class RetryExhaustedError(Exception):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, response: requests.Response, attempts: int) -> None:
        self.response = response
        self.attempts = attempts
        super().__init__(
            f"Retry exhausted after {attempts} attempts, "
            f"last status: {response.status_code}"
        )


class DeadlineExceededError(Exception):
    """Raised when the deadline would be exceeded before completing a retry."""

    def __init__(self, remaining_ms: float) -> None:
        self.remaining_ms = remaining_ms
        super().__init__(
            f"Deadline exceeded, only {remaining_ms:.0f}ms remaining"
        )


def request_with_retry(
    method: str,
    url: str,
    *,
    timeout: float,
    max_retries: int,
    backoff_base: float = 0.5,
    retryable_status: Iterable[int] = _RETRYABLE_STATUS,
    deadline_epoch: float | None = None,
    min_time_for_attempt_ms: float = 5000.0,
    **kwargs: object,
) -> requests.Response:
    """
    Make an HTTP request with exponential backoff retry.

    Args:
        method: HTTP method (GET, POST, etc.)
        url: Target URL
        timeout: Per-request timeout in seconds
        max_retries: Maximum number of attempts
        backoff_base: Base for exponential backoff (default 0.5s)
        retryable_status: Set of HTTP status codes that trigger retry
        deadline_epoch: Absolute Unix timestamp deadline. If set, will raise
            DeadlineExceededError if there isn't enough time for another attempt.
        min_time_for_attempt_ms: Minimum milliseconds needed to start an attempt
            (used with deadline_epoch). Default 5000ms.
        **kwargs: Passed to requests.Session.request()

    Returns:
        requests.Response on success (non-retryable status)

    Raises:
        RetryExhaustedError: All retries exhausted with retryable status
        DeadlineExceededError: Not enough time remaining before deadline
        requests.RequestException: Network-level errors (not retried)
    """
    retryable = frozenset(retryable_status)
    last_response: requests.Response | None = None

    for attempt in range(1, max_retries + 1):
        # Check deadline before starting attempt
        if deadline_epoch is not None:
            remaining_ms = (deadline_epoch - time.time()) * 1000
            if remaining_ms < min_time_for_attempt_ms:
                raise DeadlineExceededError(remaining_ms)

        response = _SESSION.request(method, url, timeout=timeout, **kwargs)

        if response.status_code not in retryable:
            return response

        # Extract retry-after before closing
        retry_after = response.headers.get("Retry-After")

        # Close the response to release the connection back to the pool
        # This prevents connection leaks in warm Lambda environments
        response.close()

        last_response = response

        # Don't sleep after the last attempt
        if attempt >= max_retries:
            break

        # Exponential backoff: 0.5, 1.0, 2.0, 4.0, ...
        if retry_after:
            try:
                sleep_for = float(retry_after)
            except ValueError:
                sleep_for = backoff_base * (2 ** (attempt - 1))
        else:
            sleep_for = backoff_base * (2 ** (attempt - 1))

        # Check if sleeping would exceed deadline
        if deadline_epoch is not None:
            time_after_sleep = time.time() + sleep_for
            if time_after_sleep + (min_time_for_attempt_ms / 1000) > deadline_epoch:
                raise DeadlineExceededError(
                    (deadline_epoch - time.time()) * 1000
                )

        time.sleep(sleep_for)

    # All retries exhausted with retryable status
    assert last_response is not None
    raise RetryExhaustedError(last_response, max_retries)


__all__ = [
    "request_with_retry",
    "RetryExhaustedError",
    "DeadlineExceededError",
]
