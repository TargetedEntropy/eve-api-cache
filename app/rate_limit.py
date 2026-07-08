"""Small in-memory per-client sliding-window rate limiter."""
import asyncio
import time
from collections import defaultdict, deque


class InMemoryRateLimiter:
    """
    Per-key sliding-window limiter. Single-process only: with multiple uvicorn
    workers each holds its own counters, so the effective limit is per worker.
    Move to Redis (INCR+EXPIRE) if a shared limit across workers is required.
    """
    def __init__(self, limit: int, window_seconds: int = 60) -> None:
        self._limit = limit
        self._window_seconds = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        self._ops_since_prune = 0
        self._prune_interval = 1000

    async def allow(self, key: str) -> tuple[bool, int]:
        if self._limit <= 0:
            return True, self._limit

        now = time.monotonic()
        cutoff = now - self._window_seconds
        async with self._lock:
            # Periodically drop keys that have gone fully idle, so a churn of
            # distinct client keys can't grow _hits without bound.
            self._ops_since_prune += 1
            if self._ops_since_prune >= self._prune_interval:
                self._prune(cutoff)
                self._ops_since_prune = 0

            hits = self._hits[key]
            while hits and hits[0] <= cutoff:
                hits.popleft()

            if len(hits) >= self._limit:
                return False, 0

            hits.append(now)
            return True, self._limit - len(hits)

    def _prune(self, cutoff: float) -> None:
        idle = [k for k, dq in self._hits.items() if not dq or dq[-1] <= cutoff]
        for k in idle:
            del self._hits[k]
