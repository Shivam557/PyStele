# pystele/core/ids.py
from __future__ import annotations

import secrets
from datetime import datetime, timezone


def _timestamp() -> str:
    """UTC timestamp in YYYYMMDDThhmmss format."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _random_hex() -> str:
    """8 hex chars of cryptographically strong randomness."""
    return secrets.token_hex(4)


def new_execution_id() -> str:
    """
    Create a unique execution identifier.
    Format: execution-YYYYMMDDThhmmss-<8hex>
    """
    return f"execution-{_timestamp()}-{_random_hex()}"


def new_run_id() -> str:
    """
    Create a unique run identifier.
    Format: run-YYYYMMDDThhmmss-<8hex>
    """
    return f"run-{_timestamp()}-{_random_hex()}"


def new_branch_id() -> str:
    """
    Create a unique branch identifier.
    Format: branch-YYYYMMDDThhmmss-<8hex>
    """
    return f"branch-{_timestamp()}-{_random_hex()}"


if __name__ == "__main__":
    print(new_execution_id())
    print(new_run_id())
    print(new_branch_id())
