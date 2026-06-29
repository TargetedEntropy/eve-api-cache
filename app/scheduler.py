"""
APScheduler setup for background data collection.

Jobs are fire-and-forget async coroutines running on the FastAPI event loop.
Market-order jobs are jittered so N regions don't all hammer ESI simultaneously.
"""
import logging
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import app.collector as collector
from app.cache import CacheClient
from app.config import Settings
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
            collector.collect_market_orders,
            IntervalTrigger(seconds=settings.poll_market_orders_seconds),
            args=[region_id, esi, cache, ds],
            id=f"market_orders_{region_id}",
            name=f"Market orders — region {region_id}",
            misfire_grace_time=60,
            jitter=max(1, jitter),
        )

    # --- Market prices: global, once per poll window ---
    scheduler.add_job(
        collector.collect_market_prices,
        IntervalTrigger(seconds=settings.poll_market_prices_seconds),
        args=[esi, cache, ds],
        id="market_prices",
        name="Market prices (global)",
        misfire_grace_time=120,
    )

    # --- Market history: daily per region (discovers type IDs from archived orders) ---
    for region_id in settings.market_region_ids:
        scheduler.add_job(
            collector.collect_market_history_for_region,
            IntervalTrigger(seconds=settings.poll_market_history_seconds),
            args=[region_id, esi, cache, ds],
            id=f"market_history_{region_id}",
            name=f"Market history — region {region_id}",
            misfire_grace_time=3600,
        )

    # --- Universe time-series: all on the same interval ---
    universe_jobs = [
        ("system_jumps",    collector.collect_system_jumps,    "System jumps"),
        ("system_kills",    collector.collect_system_kills,    "System kills"),
        ("sovereignty_map", collector.collect_sovereignty_map, "Sovereignty map"),
        ("incursions",      collector.collect_incursions,      "Incursions"),
    ]
    for job_id, fn, name in universe_jobs:
        scheduler.add_job(
            fn,
            IntervalTrigger(seconds=settings.poll_universe_seconds),
            args=[esi, cache, ds],
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


def scheduler_status(scheduler: AsyncIOScheduler) -> list[dict[str, Any]]:
    """Return a summary of all scheduled jobs for the /collector/status endpoint."""
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": next_run.isoformat() if next_run else None,
        })
    return jobs
