"""
Core proxy logic: cache → ESI → archive.

Request flow:
1. Validate path against allowlist (security gate)
2. Build cache key from datasource + method + path + params/body hash
3. Check Redis hot cache → return HIT immediately (positive, then negative 4xx cache)
4. Recover ETag from Redis (outlives the body key by 60s) for conditional request
5. Coalesce identical in-flight requests (stampede protection)
6. Fetch ESI with If-None-Match if ETag known
7. 304: restore the body from stale Redis if available, otherwise re-fetch (coalesced)
8. 200: store in Redis, attempt archive write, return body
9. 420/5xx: return stale Redis data or archive fallback
10. 4xx: negative-cache briefly, then pass through to caller
"""
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

import app.archive as archive
from app.allowlist import (
    ArchiveType,
    build_cache_key,
    check_params,
    compute_query_hash,
    endpoint_label,
    match_endpoint,
    normalize_body,
    normalize_params,
    validate_datasource,
    validate_path,
)
from app.cache import CacheClient
from app.coalesce import coalesce
from app.config import Settings
from app.esi_client import ESIClient
from app.metrics import metrics

logger = logging.getLogger(__name__)


@dataclass
class ProxyResult:
    status: int
    body: bytes
    cache_status: str  # "HIT", "MISS", "STALE", "ARCHIVE_FALLBACK", "ERROR"
    content_type: str = "application/json"
    location: Optional[str] = None  # Location header for pass-through redirects


async def proxy_request(
    full_path: str,       # e.g. "/v1/markets/10000002/orders/"
    method: str,
    params: dict,
    body: Optional[bytes],
    cache: CacheClient,
    esi: ESIClient,
    db: AsyncSession,
    settings: Settings,
) -> ProxyResult:
    """Dispatch a proxy request and record per-endpoint request/latency metrics."""
    start = time.perf_counter()
    result = await _dispatch(full_path, method, params, body, cache, esi, db, settings)
    endpoint = _endpoint_label_for(full_path, method)
    metrics.inc_counter(
        "proxy_requests_total", endpoint=endpoint, cache_status=result.cache_status,
        help="Proxy requests by matched endpoint and cache status",
    )
    metrics.observe(
        "proxy_request_duration_seconds", time.perf_counter() - start,
        endpoint=endpoint, help="Proxy request duration in seconds",
    )
    return result


def _endpoint_label_for(full_path: str, method: str) -> str:
    validated = validate_path(full_path)
    if validated is None:
        return "invalid"
    _version, rest_path = validated
    spec = match_endpoint(rest_path, method)
    return endpoint_label(spec) if spec is not None else "unmatched"


async def _dispatch(
    full_path: str,       # e.g. "/v1/markets/10000002/orders/"
    method: str,
    params: dict,
    body: Optional[bytes],
    cache: CacheClient,
    esi: ESIClient,
    db: AsyncSession,
    settings: Settings,
) -> ProxyResult:

    # --- 1. Security: validate path ---
    validated = validate_path(full_path)
    if validated is None:
        return ProxyResult(400, b'{"error":"invalid path"}', "ERROR")

    _version, rest_path = validated
    spec = match_endpoint(rest_path, method)
    if spec is None:
        return ProxyResult(404, b'{"error":"endpoint not in allowlist"}', "ERROR")

    datasource = params.get("datasource", settings.default_datasource)
    if not validate_datasource(datasource):
        return ProxyResult(400, b'{"error":"invalid datasource"}', "ERROR")

    # Reject unknown/missing query params before touching cache, ESI, or archive.
    param_error = check_params(spec, params)
    if param_error is not None:
        return ProxyResult(400, json.dumps({"error": param_error}).encode(), "ERROR")

    if method.upper() == "POST":
        validation_error = _validate_post_body(body, settings.max_post_batch_items)
        if validation_error:
            return ProxyResult(400, validation_error, "ERROR")
        body = normalize_body(body)

    # --- 2. Build cache key ---
    clean_params = normalize_params(params)
    query_hash = compute_query_hash(clean_params, body)
    cache_key = build_cache_key(datasource, method, full_path, params, body)
    esi_params = dict(clean_params)
    if datasource != "tranquility":
        esi_params["datasource"] = datasource

    # --- 3. Redis hot cache (positive) ---
    cached = await cache.get(cache_key)
    if cached is not None:
        return ProxyResult(200, cached[0], "HIT")

    # --- 3b. Negative cache: a recently-seen 4xx short-circuits before ESI ---
    negative = await cache.get_negative(cache_key)
    if negative is not None:
        neg_body, neg_status = negative
        return ProxyResult(neg_status, neg_body, "HIT")

    # --- 4. Recover ETag for conditional request ---
    # ETag outlives the body key (TTL + 60s buffer in cache.set), so even after
    # body eviction we can send If-None-Match to avoid re-downloading unchanged payloads.
    raw_etag = await cache._r.get(f"esi:etag:{cache_key}")
    stored_etag: Optional[str] = raw_etag.decode() if raw_etag else None

    # --- 5. Coalesced ESI fetch (stampede protection) ---
    async def do_fetch():
        return await esi.fetch(full_path, method, esi_params, body, stored_etag)

    esi_resp, is_leader = await coalesce(cache_key, do_fetch)

    # --- 7. 304 Not Modified ---
    if esi_resp.not_modified:
        stale_body = await cache.get_stale(cache_key)
        if stale_body is not None:
            # Extend the hot-cache TTL using the new Cache-Control, otherwise
            # preserve the prior TTL (CLAUDE.md) rather than a flat 300.
            ttl = esi_resp.max_age
            if ttl is None:
                ttl = await cache.get_ttl(cache_key) or 300
            if is_leader:
                await cache.set(
                    cache_key,
                    stale_body,
                    ttl,
                    esi_resp.etag or stored_etag,
                    _stale_ttl_for_payload(stale_body, settings),
                )
            return ProxyResult(200, stale_body, "MISS")

        # Body was evicted from Redis and no stale copy remains, so re-fetch without
        # ETag to repopulate the body cache. Coalesce under a derived key so a cold
        # cache doesn't stampede ESI with one unconditional refetch per waiter.
        logger.debug("304 but body unavailable; re-fetching %s without ETag", full_path)

        async def do_refetch():
            return await esi.fetch(full_path, method, esi_params, body, None)

        esi_resp, is_leader = await coalesce(f"{cache_key}:refetch", do_refetch)

    # --- 8. ESI 200 ---
    if esi_resp.status == 200:
        # Only the coalesce leader writes: waiters share the body and would just
        # re-run the same idempotent Redis/blob/parquet/delta writes (2.7).
        if is_leader:
            ttl = esi_resp.max_age or 300
            await cache.set(
                cache_key,
                esi_resp.body,
                ttl,
                esi_resp.etag,
                _stale_ttl_for_payload(esi_resp.body, settings),
            )

            await _archive_response(
                db=db,
                cache=cache,
                datasource=datasource,
                full_path=full_path,
                query_hash=query_hash,
                body=esi_resp.body,
                etag=esi_resp.etag,
                expires_at=esi_resp.expires_at,
                archive_type=spec.archive_type,
                extract_names=spec.extract_names,
            )

        return ProxyResult(200, esi_resp.body, "MISS", content_type=esi_resp.content_type)

    # --- 9. ESI degraded mode: stale/archive fallback ---
    if esi_resp.status == 420 or esi_resp.status >= 500:
        logger.warning("ESI %s for %s; serving fallback if available", esi_resp.status, full_path)
        stale_body = await cache.get_stale(cache_key)
        if stale_body is not None:
            return ProxyResult(200, stale_body, "STALE")
        archive_body = await archive.get_latest_payload(db, datasource, full_path, query_hash)
        if archive_body:
            return ProxyResult(200, archive_body, "ARCHIVE_FALLBACK")
        # Don't relay a raw upstream 5xx body (may be non-JSON HTML from an
        # intermediary, mislabeled as JSON and leaking infra details) — 4.4.
        envelope = json.dumps({"error": "upstream_error", "status": esi_resp.status}).encode()
        return ProxyResult(esi_resp.status, envelope, "ERROR")

    # --- 9b. 3xx: pass the redirect through with its Location; never followed ---
    # ESI portrait/image endpoints 30x to a CDN. We hand the redirect back to the
    # caller (ESI-faithful) rather than fetching binary bytes and caching/archiving
    # them as JSON. Not archived (redirects aren't archiveable observations).
    if 300 <= esi_resp.status < 400:
        return ProxyResult(esi_resp.status, esi_resp.body, "MISS", location=esi_resp.location)

    # --- 10. 4xx: negative-cache briefly, then pass through ---
    # A looping client on a nonexistent ID / bad params otherwise drives one ESI
    # error per request. 429 is excluded (transient throttle, not a stable answer);
    # error payloads are never archived. Respect ESI's Cache-Control when it gives one.
    if 400 <= esi_resp.status < 500 and esi_resp.status != 429:
        neg_ttl = (
            esi_resp.max_age
            if esi_resp.max_age is not None
            else settings.negative_cache_ttl_seconds
        )
        if neg_ttl > 0:
            await cache.set_negative(cache_key, esi_resp.body, esi_resp.status, neg_ttl)
    return ProxyResult(esi_resp.status, esi_resp.body, "MISS", content_type=esi_resp.content_type)


def _validate_post_body(body: Optional[bytes], max_items: int) -> Optional[bytes]:
    """Validate public ESI batch lookup bodies before forwarding or archiving."""
    if not body:
        return b'{"error":"POST body required"}'

    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return b'{"error":"invalid JSON body"}'

    if not isinstance(parsed, list):
        return b'{"error":"POST body must be a JSON list"}'

    if len(parsed) > max_items:
        return b'{"error":"POST batch too large"}'

    if not all(isinstance(item, (int, str)) and not isinstance(item, bool) for item in parsed):
        return b'{"error":"POST batch items must be strings or integers"}'

    return None


async def _archive_response(
    db: AsyncSession,
    cache: CacheClient,
    datasource: str,
    full_path: str,
    query_hash: str,
    body: bytes,
    etag: Optional[str],
    expires_at,
    archive_type: ArchiveType,
    extract_names: bool,
) -> None:
    """Archive a successful ESI response without failing the live proxy response."""
    if archive_type == ArchiveType.NONE and not extract_names:
        return
    try:
        if archive_type != ArchiveType.NONE:
            content_hash = hashlib.sha256(body).hexdigest()
            await archive.write_snapshot(
                db, datasource, full_path, query_hash, content_hash,
                body, 200, etag, expires_at, archive_type,
            )

        if extract_names:
            await archive.write_names(db, cache, datasource, body)
        metrics.inc_counter(
            "archive_writes_total", archive_type=archive_type.value, result="ok",
            help="Archive writes by type and result",
        )
    except Exception:
        await db.rollback()
        metrics.inc_counter(
            "archive_writes_total", archive_type=archive_type.value, result="error",
            help="Archive writes by type and result",
        )
        logger.exception("Archive write failed for %s", full_path)


def _stale_ttl_for_payload(body: bytes, settings: Settings) -> int:
    if settings.stale_cache_max_body_bytes > 0 and len(body) > settings.stale_cache_max_body_bytes:
        return 0
    return settings.stale_cache_seconds
