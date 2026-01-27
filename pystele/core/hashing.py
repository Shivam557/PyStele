# pystele/core/hashing.py
from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_hex(data: bytes) -> str:
    """Return SHA-256 hex digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def content_hash(obj: Any) -> str:
    """
    Deterministically hash supported Python primitives.
    Supported: dict, list, str, int, float, bool, None.
    """
    serialized = json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return sha256_hex(serialized)


if __name__ == "__main__":
    print(content_hash({"a": 1, "b": [True, None, 3.14]}))
