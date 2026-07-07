"""
Scheduler tests: per-job staggering (fix 3.1) and first-run-after-boot (fix 3.2).

Jobs are inspected before the scheduler is started, via each job's IntervalTrigger
(`start_date`, `interval`, `jitter`) — deterministic and requires no event loop work.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

from app.config import Settings
from app.scheduler import create_scheduler

_UNIVERSE_IDS = {
    "system_jumps", "system_kills", "sovereignty_map",
    "sovereignty_structures", "incursions", "industry_facilities",
}


def _make(**overrides):
    base = dict(
        market_region_ids=[10000002, 10000043, 10000030, 10000032, 10000042],
        poll_market_orders_seconds=300,
        poll_market_history_seconds=86400,
        poll_universe_seconds=3600,
        esi_max_requests_per_second=0.0,
    )
    base.update(overrides)
    settings = Settings(**base)
    return create_scheduler(AsyncMock(), AsyncMock(), settings), settings


def _offsets(jobs, ref: datetime):
    return sorted((j.trigger.start_date - ref).total_seconds() for j in jobs)


async def test_market_order_jobs_staggered_and_start_soon():
    ref = datetime.now(timezone.utc)
    sched, settings = _make()
    jobs = [j for j in sched.get_jobs() if j.id.startswith("market_orders_")]
    assert len(jobs) == 5

    offs = _offsets(jobs, ref)
    assert len(set(offs)) == 5                                   # 3.1: distinct → staggered
    assert offs[0] >= 0 and offs[-1] < settings.poll_market_orders_seconds  # 3.2: within one interval
    # evenly phase-shifted ~interval/N apart across the poll window
    step = settings.poll_market_orders_seconds / 5
    for a, b in zip(offs, offs[1:]):
        assert abs((b - a) - step) < 1.0
    for j in jobs:
        assert j.trigger.interval == timedelta(seconds=300)
        assert j.trigger.jitter == 10


async def test_daily_history_jobs_run_soon_after_boot_not_one_interval_later():
    """Core 3.2 fix: daily jobs must NOT first run ~24h out, or a service restarted
    more often than daily would never collect market history."""
    ref = datetime.now(timezone.utc)
    sched, settings = _make()
    jobs = [j for j in sched.get_jobs() if j.id.startswith("market_history_")]
    assert len(jobs) == 5

    offs = _offsets(jobs, ref)
    assert len(set(offs)) == 5                                   # staggered
    assert offs[-1] < 3600                                       # every first-run well under 24h
    assert offs[-1] < settings.poll_market_history_seconds / 10
    for j in jobs:
        assert j.trigger.interval == timedelta(seconds=86400)


async def test_universe_jobs_staggered_and_start_within_interval():
    ref = datetime.now(timezone.utc)
    sched, settings = _make()
    jobs = [j for j in sched.get_jobs() if j.id in _UNIVERSE_IDS]
    assert len(jobs) == 6

    offs = _offsets(jobs, ref)
    assert len(set(offs)) == 6                                   # 3.1: not all firing together
    assert offs[0] >= 0 and offs[-1] < settings.poll_universe_seconds  # 3.2


async def test_no_job_first_runs_a_full_interval_out():
    """Regression guard for 3.2 across every scheduled job."""
    ref = datetime.now(timezone.utc)
    sched, _ = _make()
    for job in sched.get_jobs():
        interval = job.trigger.interval.total_seconds()
        first = (job.trigger.start_date - ref).total_seconds()
        assert 0 <= first < interval, f"{job.id} first run {first}s ≥ interval {interval}s"


async def test_single_region_does_not_break_stagger_math():
    """n_regions=1 must not divide-by-zero in the stagger step."""
    sched, _ = _make(market_region_ids=[10000002])
    order_jobs = [j for j in sched.get_jobs() if j.id.startswith("market_orders_")]
    assert len(order_jobs) == 1
    assert order_jobs[0].trigger.jitter == 10
