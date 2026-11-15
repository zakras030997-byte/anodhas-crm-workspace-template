"""Small in-memory rate limits for a single local operator process."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import Callable
import time


class WindowRateLimiter:
    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, key: str, *, limit: int, window_seconds: float) -> bool:
        if not key or limit < 1 or window_seconds <= 0:
            raise ValueError("invalid rate limit")
        async with self._lock:
            now = self._clock()
            events = self._events[key]
            cutoff = now - window_seconds
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= limit:
                return False
            events.append(now)
            return True
