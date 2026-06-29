"""
Request coalescing to prevent cache stampedes.

When multiple callers request the same uncached key simultaneously,
only one upstream ESI request is made; all waiters share the result.
"""
import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

_inflight: dict[str, asyncio.Future] = {}
_lock = asyncio.Lock()


async def coalesce(key: str, coro_fn: Callable[[], Awaitable[T]]) -> T:
    """
    Execute coro_fn() for `key`, or wait for an in-flight execution to finish.
    All callers for the same key during a single fetch share one upstream request.
    """
    async with _lock:
        if key in _inflight:
            fut = _inflight[key]
            wait_existing = True
        else:
            fut = asyncio.get_event_loop().create_future()
            _inflight[key] = fut
            wait_existing = False

    if wait_existing:
        return await asyncio.shield(fut)

    try:
        result = await coro_fn()
        fut.set_result(result)
        return result
    except Exception as exc:
        fut.set_exception(exc)
        raise
    finally:
        async with _lock:
            _inflight.pop(key, None)
