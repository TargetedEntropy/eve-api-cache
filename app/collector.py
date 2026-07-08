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
import json
import logging
from typing import Optional

from sqlalchemy import text

from app.allowlist import ArchiveType, build_cache_key, compute_query_hash
from app.archive import write_snapshot
from app.cache import CacheClient
from app.config import settings
from app.db import AsyncSessionLocal
from app.esi_client import ESIClient

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
    stale_ttl = _stale_ttl_for_payload(resp.body)
    await cache.set(cache_key, resp.body, ttl, resp.etag, stale_ttl)

    # Optionally warm the same body under alias version prefixes (e.g. /latest/) so
    # downstream apps not calling /v1/ still hit collector-warmed keys. The archive
    # is written once under the canonical path below — no archive identity merge.
    for alias_version in settings.collector_warm_alias_versions:
        alias_path = _swap_version(path, alias_version)
        if alias_path != path:
            alias_key = build_cache_key(datasource, method, alias_path, params, body)
            await cache.set(alias_key, resp.body, ttl, resp.etag, stale_ttl)

    query_hash = compute_query_hash(params, body)
    content_hash = hashlib.sha256(resp.body).hexdigest()

    async with AsyncSessionLocal() as session:
        try:
            await write_snapshot(
                session, datasource, path, query_hash, content_hash,
                resp.body, 200, resp.etag, resp.expires_at, archive_type,
            )
        except Exception:
            await session.rollback()
            logger.exception("Collector: archive write failed for %s", path)
            return False

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
    if esi.is_budget_blocked():
        logger.warning(
            "Collector: ESI error budget blocked — skipping market history for region %s",
            region_id,
        )
        return 0

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
    """
    Return the type_ids to fetch history for: the most recent orders snapshot
    unioned with every type_id ever archived for the region, so thin markets
    with no currently-open orders still get history archived (3.5).
    """
    from app.archive import get_latest_payload

    path = f"/v1/markets/{region_id}/orders/"
    query_hash = compute_query_hash({"order_type": "all"}, None)
    type_ids: set[int] = set()

    async with AsyncSessionLocal() as session:
        raw_payload = await get_latest_payload(session, datasource, path, query_hash)
        if raw_payload is not None:
            try:
                payload = json.loads(raw_payload)
                type_ids.update(
                    int(o["type_id"]) for o in payload
                    if isinstance(o, dict) and "type_id" in o
                )
            except (TypeError, ValueError):
                pass
        type_ids.update(await _accumulated_type_ids(session, datasource, region_id))

    return list(type_ids)


async def _accumulated_type_ids(session, datasource: str, region_id: int) -> set[int]:
    """
    Distinct type_ids across all archived market-order versions for a region
    (requires enable_market_order_deltas). Any error degrades to an empty set so
    discovery still works from the latest snapshot alone.
    """
    try:
        result = await session.execute(
            text(
                "SELECT DISTINCT (v.payload ->> 'type_id') AS type_id "
                "FROM market_order_snapshot_entries e "
                "JOIN market_order_versions v "
                "  ON v.datasource = :ds AND v.order_id = e.order_id "
                "  AND v.version_hash = e.version_hash "
                "WHERE e.region_id = :region"
            ),
            {"ds": datasource, "region": region_id},
        )
        rows = list(result.all())
    except Exception:
        return set()

    ids: set[int] = set()
    for row in rows:
        raw = row[0]
        if raw is None:
            continue
        try:
            ids.add(int(raw))
        except (TypeError, ValueError):
            continue
    return ids


def _swap_version(path: str, version: str) -> str:
    """Replace the leading version segment: '/v1/x/y' → '/{version}/x/y'."""
    parts = path.split("/", 2)  # ["", "v1", "x/y"]
    if len(parts) < 3:
        return path
    return f"/{version}/{parts[2]}"


def _stale_ttl_for_payload(body: bytes) -> int:
    if settings.stale_cache_max_body_bytes > 0 and len(body) > settings.stale_cache_max_body_bytes:
        return 0
    return settings.stale_cache_seconds


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


async def collect_sovereignty_structures(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    ok = await _fetch_and_store(
        "/v1/sovereignty/structures/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: sovereignty structures archived")
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


async def collect_industry_facilities(
    esi: ESIClient, cache: CacheClient, datasource: str = "tranquility"
) -> bool:
    ok = await _fetch_and_store(
        "/v1/industry/facilities/", {}, ArchiveType.TIME_SERIES, esi, cache, datasource
    )
    if ok:
        logger.info("Collector: industry facilities archived")
    return ok
