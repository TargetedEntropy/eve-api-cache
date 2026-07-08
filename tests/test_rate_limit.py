"""Tests for the in-memory per-client rate limiter (fix 5.7)."""
import app.rate_limit as rate_limit
from app.rate_limit import InMemoryRateLimiter


class _FakeTime:
    """Controllable monotonic clock, swapped into app.rate_limit.time."""
    def __init__(self, start: float = 1000.0):
        self.now = start

    def monotonic(self) -> float:
        return self.now


async def test_allows_up_to_limit_then_denies():
    rl = InMemoryRateLimiter(2)
    assert await rl.allow("c") == (True, 1)
    assert await rl.allow("c") == (True, 0)
    ok, remaining = await rl.allow("c")
    assert ok is False and remaining == 0


async def test_zero_limit_always_allows():
    rl = InMemoryRateLimiter(0)
    for _ in range(5):
        ok, _ = await rl.allow("c")
        assert ok is True


async def test_per_key_isolation():
    rl = InMemoryRateLimiter(1)
    assert (await rl.allow("a"))[0] is True
    assert (await rl.allow("b"))[0] is True   # separate bucket
    assert (await rl.allow("a"))[0] is False  # "a" already spent


async def test_window_evicts_expired_hits(monkeypatch):
    fake = _FakeTime(1000.0)
    monkeypatch.setattr(rate_limit, "time", fake)
    rl = InMemoryRateLimiter(1, window_seconds=60)

    assert (await rl.allow("c"))[0] is True    # t=1000
    assert (await rl.allow("c"))[0] is False   # t=1000, over limit
    fake.now = 1061.0                           # 61s later → prior hit aged out
    assert (await rl.allow("c"))[0] is True


async def test_idle_keys_are_pruned(monkeypatch):
    fake = _FakeTime(1000.0)
    monkeypatch.setattr(rate_limit, "time", fake)
    rl = InMemoryRateLimiter(5, window_seconds=60)
    rl._prune_interval = 2

    await rl.allow("a")
    await rl.allow("b")            # ops=2 → prune runs; a,b still fresh
    assert set(rl._hits) == {"a", "b"}

    fake.now = 1100.0              # a and b now idle (> window)
    await rl.allow("c")
    await rl.allow("c")           # ops=2 → prune drops idle a,b; keeps c
    assert set(rl._hits) == {"c"}
