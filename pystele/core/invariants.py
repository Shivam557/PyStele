# pystele/core/invariants.py
from __future__ import annotations

from typing import Dict, List

CORE_INVARIANTS = [
    "commit-log-append-only",
    "ids-immutable",
    "snapshot-consistency",
    "clock-monotonic",
    "config-version-set",
    "storage-path-set",
]


def check_invariants(state: Dict) -> List[str]:
    """Return list of violated invariants (empty = pass)."""
    violated: List[str] = []

    if not isinstance(state.get("commit_log"), list):
        violated.append("commit-log-append-only")

    if state.get("ids_mutable") is True:
        violated.append("ids-immutable")

    if state.get("snapshots_consistent") is not True:
        violated.append("snapshot-consistency")

    if state.get("clock_monotonic") is not True:
        violated.append("clock-monotonic")

    if not isinstance(state.get("version"), str) or not state.get("version"):
        violated.append("config-version-set")

    if not state.get("storage_path"):
        violated.append("storage-path-set")

    return violated
