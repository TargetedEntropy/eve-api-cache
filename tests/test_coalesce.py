"""Tests for request coalescing (fix 5.7)."""
import asyncio

import pytest

from app.coalesce import coalesce


async def test_concurrent_waiters_share_one_execution():
    calls = {"n": 0}
    started = asyncio.Event()
    release = asyncio.Event()

    async def fn():
        calls["n"] += 1
        started.set()
        await release.wait()
        return "shared-result"

    leader = asyncio.create_task(coalesce("k1", fn))
    await started.wait()                 # leader registered + in-flight
    follower = asyncio.create_task(coalesce("k1", fn))
    await asyncio.sleep(0)               # let the follower attach to the future
    release.set()

    assert await leader == "shared-result"
    assert await follower == "shared-result"
    assert calls["n"] == 1               # coro_fn ran exactly once


async def test_leader_failure_propagates_to_waiters():
    started = asyncio.Event()
    release = asyncio.Event()

    async def fn():
        started.set()
        await release.wait()
        raise ValueError("boom")

    leader = asyncio.create_task(coalesce("k2", fn))
    await started.wait()
    follower = asyncio.create_task(coalesce("k2", fn))
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(ValueError):
        await leader
    with pytest.raises(ValueError):
        await follower


async def test_key_is_released_after_completion():
    async def fn():
        return 1

    await coalesce("k3", fn)
    # A subsequent call with the same key runs fresh (not a stale shared future).
    calls = {"n": 0}

    async def fn2():
        calls["n"] += 1
        return 2

    assert await coalesce("k3", fn2) == 2
    assert calls["n"] == 1


async def test_distinct_keys_run_independently():
    calls = {"n": 0}

    async def fn():
        calls["n"] += 1
        return calls["n"]

    r1 = await coalesce("a", fn)
    r2 = await coalesce("b", fn)
    assert calls["n"] == 2
    assert {r1, r2} == {1, 2}
