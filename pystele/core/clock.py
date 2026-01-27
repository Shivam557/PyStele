# pystele/core/clock.py
from __future__ import annotations

from datetime import datetime, timezone
import threading


def logical_now() -> str:
    """Return UTC ISO8601 timestamp with millisecond precision."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class LogicalClock:
    """Monotonic logical clock."""
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = ""

    def tick(self) -> str:
        with self._lock:
            now = logical_now()
            if now <= self._last:
                now = self._last
            self._last = now
            return now

    def now(self) -> str:
        return self._last or self.tick()
