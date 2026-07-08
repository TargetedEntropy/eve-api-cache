"""
APScheduler setup for background data collection.

Jobs are fire-and-forget async coroutines running on the FastAPI event loop.
Each job is given an explicit near-future, per-job `start_date` so that:
  - its first run happens shortly after boot, not one full interval later
    (a service restarted more often than the interval would otherwise never
    run its daily jobs); and
  - jobs within a group are phase-shifted so they don't all hit ESI at the
    same instant. A small random jitter decorrelates steady-state runs too.
"""
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text

import app.collector as collector
from app.cache import CacheClient
from app.config import Settings
from app.db import AsyncSessionLocal
from app.esi_client import ESIClient
from app.metrics import metrics

logger = logging.getLogger(__name__)


def _record_job_event(event) -> None:
    """APScheduler listener → collector_job_runs_total{job,result} + log gaps.

    Missed/errored runs are logged so archive gaps (e.g. a Forge snapshot that
    overran its interval) are visible, not silent.
    """
    if event.code == EVENT_JOB_ERROR:
        result = "error"
        logger.error(
            "Collector job %s errored", event.job_id,
            exc_info=getattr(event, "exception", None),
        )
    elif event.code == EVENT_JOB_MISSED:
        result = "missed"
        logger.warning("Collector job %s missed its scheduled run", event.job_id)
    else:
        result = "success"
    metrics.inc_counter(
        "collector_job_runs_total", job=event.job_id, result=result,
        help="Collector job runs by job and result",
    )

# First-run / stagger offsets, in seconds after process start. STARTUP_DELAY
# exceeds JITTER so a job's first fire never lands before "now" once jitter is
# applied. History starts after the first orders snapshot has had a chance to
# archive, since history discovery reads type IDs from that snapshot.
_STARTUP_DELAY_SECONDS = 15
_JITTER_SECONDS = 10
_PRICES_START_SECONDS = 20
_UNIVERSE_START_SECONDS = 30
_UNIVERSE_STAGGER_SECONDS = 60
_HISTORY_START_SECONDS = 120
_HISTORY_STAGGER_SECONDS = 600


def _first_run(offset_seconds: float) -> datetime:
    """A near-future start_date `offset_seconds` after now (UTC)."""
    return datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)


def create_scheduler(esi: ESIClient, cache: CacheClient, settings: Settings) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_listener(
        _record_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED
    )
    ds = settings.default_datasource

    # --- Market orders: one job per region, phase-shifted across the poll window ---
    n_regions = max(len(settings.market_region_ids), 1)
    orders_step = settings.poll_market_orders_seconds / n_regions
    for i, region_id in enumerate(settings.market_region_ids):
        scheduler.add_job(
            _run_singleton_job,
            IntervalTrigger(
                seconds=settings.poll_market_orders_seconds,
                start_date=_first_run(_STARTUP_DELAY_SECONDS + i * orders_step),
                jitter=_JITTER_SECONDS,
            ),
            args=[f"market_orders_{region_id}", collector.collect_market_orders, region_id, esi, cache, ds],
            id=f"market_orders_{region_id}",
            name=f"Market orders — region {region_id}",
            misfire_grace_time=60,
        )

    # --- Market prices: global, once per poll window ---
    scheduler.add_job(
        _run_singleton_job,
        IntervalTrigger(
            seconds=settings.poll_market_prices_seconds,
            start_date=_first_run(_PRICES_START_SECONDS),
            jitter=_JITTER_SECONDS,
        ),
        args=["market_prices", collector.collect_market_prices, esi, cache, ds],
        id="market_prices",
        name="Market prices (global)",
        misfire_grace_time=120,
    )

    # --- Market history: daily per region (discovers type IDs from archived orders) ---
    for i, region_id in enumerate(settings.market_region_ids):
        scheduler.add_job(
            _run_singleton_job,
            IntervalTrigger(
                seconds=settings.poll_market_history_seconds,
                start_date=_first_run(_HISTORY_START_SECONDS + i * _HISTORY_STAGGER_SECONDS),
                jitter=_JITTER_SECONDS,
            ),
            args=[
                f"market_history_{region_id}",
                collector.collect_market_history_for_region,
                region_id,
                esi,
                cache,
                ds,
            ],
            id=f"market_history_{region_id}",
            name=f"Market history — region {region_id}",
            misfire_grace_time=3600,
        )

    # --- Universe time-series: all on the same interval ---
    universe_jobs = [
        ("system_jumps",              collector.collect_system_jumps,             "System jumps"),
        ("system_kills",              collector.collect_system_kills,             "System kills"),
        ("sovereignty_map",           collector.collect_sovereignty_map,          "Sovereignty map"),
        ("sovereignty_structures",    collector.collect_sovereignty_structures,   "Sovereignty structures"),
        ("incursions",                collector.collect_incursions,               "Incursions"),
        ("industry_facilities",       collector.collect_industry_facilities,      "Industry facilities"),
    ]
    for i, (job_id, fn, name) in enumerate(universe_jobs):
        scheduler.add_job(
            _run_singleton_job,
            IntervalTrigger(
                seconds=settings.poll_universe_seconds,
                start_date=_first_run(_UNIVERSE_START_SECONDS + i * _UNIVERSE_STAGGER_SECONDS),
                jitter=_JITTER_SECONDS,
            ),
            args=[job_id, fn, esi, cache, ds],
            id=job_id,
            name=name,
            misfire_grace_time=120,
        )

    logger.info(
        "Scheduler configured: %d market-order regions, %d total jobs",
        len(settings.market_region_ids),
        len(scheduler.get_jobs()),
    )
    return scheduler


async def _run_singleton_job(job_id: str, fn, *args):
    lock_id = _advisory_lock_id(job_id)
    async with AsyncSessionLocal() as session:
        got_lock = await session.scalar(
            text("SELECT pg_try_advisory_lock(:lock_id)"),
            {"lock_id": lock_id},
        )
        if not got_lock:
            logger.info("Skipping collector job %s; another instance holds the lock", job_id)
            return None

        try:
            return await fn(*args)
        finally:
            await session.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": lock_id},
            )
            await session.commit()


def _advisory_lock_id(job_id: str) -> int:
    digest = hashlib.blake2b(f"eve-api-cache:{job_id}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") & ((1 << 63) - 1)


def scheduler_status(scheduler: AsyncIOScheduler) -> list[dict[str, Any]]:
    """Return a summary of all scheduled jobs for the /collector/status endpoint."""
    jobs = []
    for job in scheduler.get_jobs():
        next_run = getattr(job, "next_run_time", None)
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": next_run.isoformat() if next_run else None,
        })
    return jobs
