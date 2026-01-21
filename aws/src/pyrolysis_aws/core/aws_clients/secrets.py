from __future__ import annotations

import base64
import json
import time
from typing import Any

from .boto import secrets

_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _parse_secret(response: dict[str, Any]) -> dict[str, Any]:
    if "SecretString" in response and response["SecretString"]:
        return json.loads(response["SecretString"])
    if "SecretBinary" in response and response["SecretBinary"]:
        decoded = base64.b64decode(response["SecretBinary"]).decode("utf-8")
        return json.loads(decoded)
    raise RuntimeError("Secret has no SecretString or SecretBinary")


def get_secret_json(secret_id: str, ttl_seconds: int = 900) -> dict[str, Any]:
    now = time.time()
    cached = _cache.get(secret_id)
    if cached and cached[0] > now:
        return cached[1]

    response = secrets.get_secret_value(SecretId=secret_id)
    value = _parse_secret(response)
    _cache[secret_id] = (now + ttl_seconds, value)
    return value


__all__ = ["get_secret_json"]
