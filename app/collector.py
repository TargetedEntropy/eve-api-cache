"""
Background data collector — proactively fetches and archives ESI endpoints
without waiting for a downstream caller to request them.

Uses the same ESI client, cache, and archive layer as the proxy so that
proactively collected data is served as cache HITs to callers.

Cache key scheme matches the proxy exactly: params never include an explicit
datasource= for tranquility (matching client requests that also omit it),
so collector-populated keys are found on proxy cache lookups.
"""
import asyncio
import hashlib
import logging
from typing import Optional

from app.allowlist import ArchiveType, build_cache_key, compute_query_hash
from app.archive import write_snapshot
from app.cache import CacheClient
from app.db import AsyncSessionLocal, TimeSeriesSnapshot
from app.esi_client import ESIClient
from sqlalchemy import select

logger = logging.getLogger(__name__)


async def _fetch_and_store(
    path: str,
    params: dict,
    archive_type: ArchiveType,
    esi: ESIClient,
    cache: CacheClient,
    datasource: str,
    method: str = "GET",
    body: Optional[bytes] = None,
) -> bool:
    """
    Fetch one ESI endpoint and store the result in Redis + PostgreSQL.
    Returns True on success, False on ESI error.
    """
    esi_params = dict(params)
    if datasource != "tranquility":
        esi_params["datasource"] = datasource

    resp = await esi.fetch(path, method=method, params=esi_params or None, body=body)

    if resp.status != 200:
        logger.warning("Collector: ESI %s for %s — skipping", resp.status, path)
        return False

    cache_key = build_cache_key(datasource, method, path, params, body)
    ttl = resp.max_age or 300
    await cache.set(cache_key, resp.body, ttl, resp.etag)

    query_hash = compute_query_hash(params, body)
    content_hash = hashlib.sha256(resp.body).hexdigest()

    async with AsyncSessionLocal() as session:
        await write_snapshot(
            session, datasource, path, query_hash, content_hash,
            resp.body, 200, resp.etag, resp.expires_at, archive_type,
        )

    return True


# ---------------------------------------------------------------------------
# Market
# ---------------------------------------------------------------------------

async def collect_market_orders(
    region_id: int, esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    """Fetch and archive all market orders for a region (paginated, merged)."""
    path = f"/v1/markets/{region_id}/orders/"
    params = {"order_type": "all"}
    ok = await _fetch_and_store(path, params, ArchiveType.TIME_SERIES, esi, cache, datasource)
    if ok:
        logger.info("Collector: market orders region %s archived", region_id)
    return ok


async def collect_market_prices(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    """Fetch and archive global adjusted/average prices."""
    ok = await _fetch_and_store(
        "/v1/markets/prices/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: market prices archived")
    return ok


async def collect_market_history(
    region_id: int, type_id: int, esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    """Fetch and archive market price history for a single (region, type) pair."""
    path = f"/v1/markets/{region_id}/history/"
    params = {"type_id": str(type_id)}
    return await _fetch_and_store(path, params, ArchiveType.TIME_SERIES, esi, cache, datasource)


async def collect_market_history_for_region(
    region_id: int,
    esi: ESIClient,
    cache: CacheClient,
    datasource: str = "tranquility",
    concurrency: int = 10,
) -> int:
    """
    Discover type IDs from the most recent archived market orders snapshot for
    a region, then fetch history for each one.  Returns the number of successful
    history fetches.

    Run this less frequently than collect_market_orders (daily is fine; ESI
    history only updates once per day).
    """
    type_ids = await _discover_type_ids(region_id, datasource)
    if not type_ids:
        logger.debug("Collector: no type IDs found in archive for region %s — skipping history", region_id)
        return 0

    sem = asyncio.Semaphore(concurrency)

    async def fetch_one(type_id: int) -> bool:
        async with sem:
            return await collect_market_history(region_id, type_id, esi, cache, datasource)

    results = await asyncio.gather(*[fetch_one(t) for t in type_ids], return_exceptions=True)
    successes = sum(1 for r in results if r is True)
    logger.info(
        "Collector: market history region %s — %d/%d types archived",
        region_id, successes, len(type_ids),
    )
    return successes


async def _discover_type_ids(region_id: int, datasource: str) -> list[int]:
    """Return distinct type_ids from the most recent market-orders snapshot."""
    path = f"/v1/markets/{region_id}/orders/"
    query_hash = compute_query_hash({"order_type": "all"}, None)

    async with AsyncSessionLocal() as session:
        stmt = (
            select(TimeSeriesSnapshot.payload)
            .where(
                TimeSeriesSnapshot.datasource == datasource,
                TimeSeriesSnapshot.path == path,
                TimeSeriesSnapshot.query_hash == query_hash,
            )
            .order_by(TimeSeriesSnapshot.fetched_at.desc())
            .limit(1)
        )
        row = await session.execute(stmt)
        payload = row.scalar_one_or_none()

    if not payload or not isinstance(payload, list):
        return []

    return list({int(o["type_id"]) for o in payload if isinstance(o, dict) and "type_id" in o})


# ---------------------------------------------------------------------------
# Universe time-series
# ---------------------------------------------------------------------------

async def collect_system_jumps(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    ok = await _fetch_and_store(
        "/v1/universe/system_jumps/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: system jumps archived")
    return ok


async def collect_system_kills(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    ok = await _fetch_and_store(
        "/v1/universe/system_kills/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: system kills archived")
    return ok


async def collect_sovereignty_map(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    ok = await _fetch_and_store(
        "/v1/sovereignty/map/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: sovereignty map archived")
    return ok


async def collect_incursions(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    ok = await _fetch_and_store(
        "/v1/incursions/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: incursions archived")
    return ok
