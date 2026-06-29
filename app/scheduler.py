"""
APScheduler setup for background data collection.

Jobs are fire-and-forget async coroutines running on the FastAPI event loop.
Market-order jobs are jittered so N regions don't all hammer ESI simultaneously.
"""
import logging
import hashlib
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text

import app.collector as collector
from app.cache import CacheClient
from app.config import Settings
from app.db import AsyncSessionLocal
from app.esi_client import ESIClient

logger = logging.getLogger(__name__)


def create_scheduler(esi: ESIClient, cache: CacheClient, settings: Settings) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    ds = settings.default_datasource

    # --- Market orders: one job per region, jittered across the poll window ---
    n_regions = len(settings.market_region_ids)
    for i, region_id in enumerate(settings.market_region_ids):
        # Spread start times evenly so regions don't all fire at once
        jitter = int(settings.poll_market_orders_seconds / max(n_regions, 1) * i)
        scheduler.add_job(
            _run_singleton_job,
            IntervalTrigger(seconds=settings.poll_market_orders_seconds),
            args=[f"market_orders_{region_id}", collector.collect_market_orders, region_id, esi, cache, ds],
            id=f"market_orders_{region_id}",
            name=f"Market orders — region {region_id}",
            misfire_grace_time=60,
            jitter=max(1, jitter),
        )

    # --- Market prices: global, once per poll window ---
    scheduler.add_job(
        _run_singleton_job,
        IntervalTrigger(seconds=settings.poll_market_prices_seconds),
        args=["market_prices", collector.collect_market_prices, esi, cache, ds],
        id="market_prices",
        name="Market prices (global)",
        misfire_grace_time=120,
    )

    # --- Market history: daily per region (discovers type IDs from archived orders) ---
    for region_id in settings.market_region_ids:
        scheduler.add_job(
            _run_singleton_job,
            IntervalTrigger(seconds=settings.poll_market_history_seconds),
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
    for job_id, fn, name in universe_jobs:
        scheduler.add_job(
            _run_singleton_job,
            IntervalTrigger(seconds=settings.poll_universe_seconds),
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
