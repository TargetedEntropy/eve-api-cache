"""
Unit tests for proxy_request().

Uses:
  - Real CacheClient backed by fakeredis (cache hit/miss behaviour is real)
  - AsyncMock for ESIClient (no real HTTP calls)
  - patch() for app.archive functions (no real PostgreSQL)
"""
import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch, MagicMock

from app.proxy import proxy_request, ProxyResult
from app.cache import CacheClient
from app.config import Settings
from app.esi_client import ESIResponse
from app.allowlist import build_cache_key


# ---------------------------------------------------------------------------
# Helper factories
# ---------------------------------------------------------------------------

def make_esi_200(body: bytes, etag: str = '"etag1"', max_age: int = 300) -> ESIResponse:
    return ESIResponse(
        status=200,
        body=body,
        etag=etag,
        max_age=max_age,
        expires_at=datetime.now(timezone.utc),
        not_modified=False,
        error_limit_remain=100,
        error_limit_reset=60,
    )


def make_esi_500(body: bytes = b'{"error":"server error"}') -> ESIResponse:
    return ESIResponse(
        status=500,
        body=body,
        etag=None,
        max_age=None,
        expires_at=None,
        not_modified=False,
        error_limit_remain=50,
        error_limit_reset=60,
    )


def make_esi_404(body: bytes = b'{"error":"not found"}') -> ESIResponse:
    return ESIResponse(
        status=404,
        body=body,
        etag=None,
        max_age=None,
        expires_at=None,
        not_modified=False,
        error_limit_remain=None,
        error_limit_reset=None,
    )


def make_esi_420(body: bytes = b'{"error":"error limit exceeded"}') -> ESIResponse:
    return ESIResponse(
        status=420,
        body=body,
        etag=None,
        max_age=None,
        expires_at=None,
        not_modified=False,
        error_limit_remain=0,
        error_limit_reset=60,
    )


def make_esi_429(body: bytes = b'{"error":"too many requests"}') -> ESIResponse:
    return ESIResponse(
        status=429,
        body=body,
        etag=None,
        max_age=None,
        expires_at=None,
        not_modified=False,
        error_limit_remain=None,
        error_limit_reset=None,
    )


def make_esi_302(location: str = "https://images.evetech.net/characters/95/portrait") -> ESIResponse:
    return ESIResponse(
        status=302,
        body=b"",
        etag=None,
        max_age=None,
        expires_at=None,
        not_modified=False,
        location=location,
    )


# ---------------------------------------------------------------------------
# Test 1: Cache HIT — ESI is never contacted
# ---------------------------------------------------------------------------

async def test_cache_hit(cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings):
    """A warm cache entry short-circuits the ESI fetch entirely."""
    # Pre-populate with the real key that proxy_request will compute
    key = build_cache_key("tranquility", "GET", "/v1/status/", {}, None)
    await cache_client.set(key, b'{"players":500}', ttl=300)

    result = await proxy_request(
        "/v1/status/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )

    assert result.status == 200
    assert result.cache_status == "HIT"
    assert result.body == b'{"players":500}'
    mock_esi.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2: Cache MISS → ESI 200 → body stored in Redis, MISS returned
# ---------------------------------------------------------------------------

async def test_cache_miss_esi_200(cache_client: CacheClient, mock_db, test_settings: Settings):
    """MISS path: ESI returns 200, body cached in Redis, ProxyResult is MISS."""
    esi_body = b'[{"order_id":1,"price":100.0}]'
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(esi_body)

    with patch("app.archive.write_snapshot", new=AsyncMock()):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert result.cache_status == "MISS"
    assert result.body == esi_body
    mock_esi.fetch.assert_called_once()

    # Body should now be in Redis under the real cache key
    key = build_cache_key("tranquility", "GET", "/v1/markets/10000002/orders/", {"order_type": "all"}, None)
    cached = await cache_client.get(key)
    assert cached is not None
    assert cached[0] == esi_body


# ---------------------------------------------------------------------------
# Test 3: Cache MISS → ESI 500 → archive fallback returns ARCHIVE_FALLBACK
# ---------------------------------------------------------------------------

async def test_cache_miss_esi_500_archive_fallback(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """ESI 5xx with an archived payload → 200 ARCHIVE_FALLBACK."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_500()
    archive_body = b'[{"order_id":9,"price":50.0}]'

    with patch("app.archive.get_latest_payload", new=AsyncMock(return_value=archive_body)):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert result.cache_status == "ARCHIVE_FALLBACK"
    assert result.body == archive_body


# ---------------------------------------------------------------------------
# Test 4: Cache MISS → ESI 500, archive empty → propagate 500 ERROR
# ---------------------------------------------------------------------------

async def test_cache_miss_esi_500_no_archive(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """ESI 5xx with nothing in the archive → upstream error propagated."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_500()

    with patch("app.archive.get_latest_payload", new=AsyncMock(return_value=None)):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 500
    assert result.cache_status == "ERROR"


# ---------------------------------------------------------------------------
# Test 5: Path traversal → 400 ERROR, never reaches ESI
# ---------------------------------------------------------------------------

async def test_invalid_path_traversal(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """Path containing '..' is rejected before any cache or ESI access."""
    result = await proxy_request(
        "/v1/../etc/passwd", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 400
    assert result.cache_status == "ERROR"
    mock_esi.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 6: Endpoint not in allowlist → 404 ERROR
# ---------------------------------------------------------------------------

async def test_unlisted_path_returns_404(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """Paths absent from the allowlist get a 404 before any cache or ESI access."""
    result = await proxy_request(
        "/v1/characters/12345/assets/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 404
    assert result.cache_status == "ERROR"
    mock_esi.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 7: ESI 404 passes through to caller with MISS
# ---------------------------------------------------------------------------

async def test_esi_404_passthrough(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """ESI 4xx (not 5xx) is passed through to the caller with cache_status MISS."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_404()

    result = await proxy_request(
        "/v1/universe/types/999999999/", "GET",
        {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 404
    assert result.cache_status == "MISS"
    mock_esi.fetch.assert_called_once()


# ---------------------------------------------------------------------------
# Test 8: Non-default datasource propagates through cache key
# ---------------------------------------------------------------------------

async def test_datasource_param_changes_cache_key(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """Requests for different datasources use distinct cache keys."""
    tq_key = build_cache_key("tranquility", "GET", "/v1/status/", {}, None)
    sisi_key = build_cache_key("singularity", "GET", "/v1/status/", {"datasource": "singularity"}, None)
    assert tq_key != sisi_key

    # Warm the tranquility slot only
    await cache_client.set(tq_key, b'{"players":500}', ttl=300)

    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(b'{"players":10}', max_age=60)

    # Singularity request should miss and go to ESI
    with patch("app.archive.write_snapshot", new=AsyncMock()):
        result = await proxy_request(
            "/v1/status/", "GET", {"datasource": "singularity"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert result.cache_status == "MISS"
    mock_esi.fetch.assert_called_once()
    assert mock_esi.fetch.call_args.args[2] == {"datasource": "singularity"}


# ---------------------------------------------------------------------------
# Test 9: Invalid datasource → 400 ERROR, never reaches ESI
# ---------------------------------------------------------------------------

async def test_invalid_datasource_rejected(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """Only known ESI datasources may create cache/archive namespaces."""
    result = await proxy_request(
        "/v1/status/", "GET", {"datasource": "totally-real"}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 400
    assert result.cache_status == "ERROR"
    assert result.body == b'{"error":"invalid datasource"}'
    mock_esi.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 10: Invalid POST body → 400 ERROR, never reaches ESI
# ---------------------------------------------------------------------------

async def test_invalid_post_body_rejected(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """Batch POST endpoints require a JSON list before forwarding."""
    result = await proxy_request(
        "/v1/universe/names/", "POST", {}, b'{"id":123}',
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 400
    assert result.cache_status == "ERROR"
    assert result.body == b'{"error":"POST body must be a JSON list"}'
    mock_esi.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 11: Oversized POST batch → 400 ERROR, never reaches ESI
# ---------------------------------------------------------------------------

async def test_oversized_post_batch_rejected(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """Batch POST endpoints are capped before forwarding or archiving."""
    body = ("[" + ",".join(str(i) for i in range(test_settings.max_post_batch_items + 1)) + "]").encode()
    result = await proxy_request(
        "/v1/universe/names/", "POST", {}, body,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 400
    assert result.cache_status == "ERROR"
    assert result.body == b'{"error":"POST batch too large"}'
    mock_esi.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 12: ESI 200 triggers write_snapshot for archiveable endpoints
# ---------------------------------------------------------------------------

async def test_esi_200_calls_write_snapshot(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """archive.write_snapshot is called exactly once on a successful ESI fetch."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(b'[{"id":1}]')

    mock_write_snapshot = AsyncMock()
    with patch("app.archive.write_snapshot", new=mock_write_snapshot):
        await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    mock_write_snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# Test 13: /status/ endpoint is proxy-only — write_snapshot is NOT called
# ---------------------------------------------------------------------------

async def test_esi_200_no_snapshot_for_none_archive_type(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """Endpoints with archive_type=NONE skip the write_snapshot call."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(b'{"players":500}')

    mock_write_snapshot = AsyncMock()
    with patch("app.archive.write_snapshot", new=mock_write_snapshot):
        result = await proxy_request(
            "/v1/status/", "GET", {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    mock_write_snapshot.assert_not_called()


async def test_esi_420_uses_archive_fallback(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_420()
    archive_body = b'[{"order_id":10}]'

    with patch("app.archive.get_latest_payload", new=AsyncMock(return_value=archive_body)):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert result.cache_status == "ARCHIVE_FALLBACK"
    assert result.body == archive_body


async def test_esi_503_uses_stale_redis_before_archive(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_500(b'{"error":"upstream failed"}')

    key = build_cache_key(
        "tranquility", "GET", "/v1/markets/10000002/orders/", {"order_type": "all"}, None
    )
    await cache_client.set(key, b'[{"order_id":1}]', ttl=300, etag='"e1"')
    await cache_client._r.delete(f"esi:body:{key}")

    mock_archive = AsyncMock(return_value=b'[{"order_id":2}]')
    with patch("app.archive.get_latest_payload", new=mock_archive):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert result.cache_status == "STALE"
    assert result.body == b'[{"order_id":1}]'
    mock_archive.assert_not_called()


async def test_post_body_is_normalized_before_forwarding(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(
        b'[{"id":1,"name":"A","category":"character"},{"id":2,"name":"B","category":"character"}]'
    )

    with patch("app.archive.write_snapshot", new=AsyncMock()), patch(
        "app.archive.write_names", new=AsyncMock()
    ):
        result = await proxy_request(
            "/v1/universe/names/", "POST",
            {}, b"[2,1,2]",
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    forwarded_body = mock_esi.fetch.call_args.args[3]
    assert forwarded_body == b"[1,2]"


async def test_archive_write_failure_does_not_fail_proxy_response(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(b'[{"order_id":1}]')
    mock_db.rollback = AsyncMock()

    with patch("app.archive.write_snapshot", new=AsyncMock(side_effect=RuntimeError("db down"))):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert result.cache_status == "MISS"
    assert result.body == b'[{"order_id":1}]'
    mock_db.rollback.assert_awaited_once()


# ---------------------------------------------------------------------------
# 3xx redirect pass-through, not followed / not archived (fix 2.2)
# ---------------------------------------------------------------------------

async def test_3xx_passed_through_with_location_not_archived(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_302()
    mock_write = AsyncMock()

    with patch("app.archive.write_snapshot", new=mock_write):
        result = await proxy_request(
            "/v1/characters/95/portrait/", "GET", {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 302
    assert result.location == "https://images.evetech.net/characters/95/portrait"
    assert result.cache_status == "MISS"
    mock_write.assert_not_called()  # redirects are not archiveable observations

    # The redirect is not written to the positive cache either.
    key = build_cache_key("tranquility", "GET", "/v1/characters/95/portrait/", {}, None)
    assert await cache_client._r.get(f"esi:body:{key}") is None


# ---------------------------------------------------------------------------
# Per-endpoint query-param allowlist (fix 4.2)
# ---------------------------------------------------------------------------

async def test_unknown_query_param_rejected_before_esi(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """Junk params are rejected before any cache/ESI/archive activity."""
    result = await proxy_request(
        "/v1/markets/10000002/orders/", "GET",
        {"order_type": "all", "junk": "1"}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 400
    assert result.cache_status == "ERROR"
    mock_esi.fetch.assert_not_called()


async def test_rejected_param_creates_no_cache_key(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """A request killed by param validation must not touch Redis at all."""
    await proxy_request(
        "/v1/status/", "GET", {"evil": "1"}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    keys = [k async for k in cache_client._r.scan_iter("esi:*")]
    assert keys == []
    mock_esi.fetch.assert_not_called()


async def test_history_missing_required_type_id_rejected(
    cache_client: CacheClient, mock_esi, mock_db, test_settings: Settings
):
    """history 400s at ESI without type_id — reject before contacting ESI."""
    result = await proxy_request(
        "/v1/markets/10000002/history/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 400
    assert result.cache_status == "ERROR"
    mock_esi.fetch.assert_not_called()


async def test_history_with_type_id_reaches_esi(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(b"[]")

    with patch("app.archive.write_snapshot", new=AsyncMock()):
        result = await proxy_request(
            "/v1/markets/10000002/history/", "GET", {"type_id": "34"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )
    assert result.status == 200
    mock_esi.fetch.assert_called_once()


async def test_allowed_language_param_forwarded_to_esi(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_200(b'{"type_id":34}')

    with patch("app.archive.write_snapshot", new=AsyncMock()):
        result = await proxy_request(
            "/v1/universe/types/34/", "GET", {"language": "en"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )
    assert result.status == 200
    assert mock_esi.fetch.call_args.args[2].get("language") == "en"


# ---------------------------------------------------------------------------
# Negative caching of 4xx responses (fix 1.3)
# ---------------------------------------------------------------------------

async def test_4xx_is_negatively_cached(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """A 404 is cached briefly so a looping client stops hitting ESI (and its budget)."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_404()

    r1 = await proxy_request(
        "/v1/universe/types/999999999/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert r1.status == 404
    assert r1.cache_status == "MISS"

    # Second identical request is served from the negative cache, ESI untouched.
    r2 = await proxy_request(
        "/v1/universe/types/999999999/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert r2.status == 404
    assert r2.cache_status == "HIT"
    assert r2.body == r1.body
    mock_esi.fetch.assert_called_once()


async def test_429_is_not_negatively_cached(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """429 is a transient throttle, not a stable answer — never negative-cached."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_429()

    for _ in range(2):
        result = await proxy_request(
            "/v1/universe/types/34/", "GET", {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )
        assert result.status == 429
        assert result.cache_status == "MISS"

    assert mock_esi.fetch.call_count == 2  # re-fetched each time, not cached


async def test_negative_cache_respects_esi_max_age(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """When ESI supplies Cache-Control on the 4xx, honour it over the local default."""
    mock_esi = AsyncMock()
    resp = make_esi_404()
    resp.max_age = 300
    mock_esi.fetch.return_value = resp

    await proxy_request(
        "/v1/universe/types/5/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    key = build_cache_key("tranquility", "GET", "/v1/universe/types/5/", {}, None)
    ttl = await cache_client._r.ttl(f"esi:neg:body:{key}")
    assert 290 <= ttl <= 300


async def test_negative_cache_defaults_ttl_without_cache_control(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    settings = test_settings.model_copy(update={"negative_cache_ttl_seconds": 45})
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_404()  # max_age is None

    await proxy_request(
        "/v1/universe/types/6/", "GET", {}, None,
        cache_client, mock_esi, mock_db, settings,
    )
    key = build_cache_key("tranquility", "GET", "/v1/universe/types/6/", {}, None)
    ttl = await cache_client._r.ttl(f"esi:neg:body:{key}")
    assert 35 <= ttl <= 45


# ---------------------------------------------------------------------------
# 304 refetch is coalesced (fix 1.6)
# ---------------------------------------------------------------------------

async def test_304_refetch_goes_through_coalesce(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """When 304 arrives but the body is gone, the unconditional refetch is coalesced."""
    import app.proxy as proxymod

    key = build_cache_key("tranquility", "GET", "/v1/status/", {}, None)
    # Only an ETag survives (no body, no stale) → conditional request returns 304,
    # forcing the refetch branch.
    await cache_client._r.set(f"esi:etag:{key}", '"e1"')

    not_modified = ESIResponse(
        status=304, body=b"", etag='"e1"', max_age=300,
        expires_at=None, not_modified=True,
    )
    mock_esi = AsyncMock()
    mock_esi.fetch.side_effect = [not_modified, make_esi_200(b'{"players":1}')]

    seen_keys: list[str] = []
    orig_coalesce = proxymod.coalesce

    async def spy_coalesce(k, fn):
        seen_keys.append(k)
        return await orig_coalesce(k, fn)

    with patch("app.proxy.coalesce", new=spy_coalesce):
        result = await proxy_request(
            "/v1/status/", "GET", {}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert result.status == 200
    assert mock_esi.fetch.call_count == 2  # conditional 304 + unconditional refetch
    assert any(k.endswith(":refetch") for k in seen_keys)


async def test_304_preserves_prior_ttl_without_cache_control(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """304 with no Cache-Control reuses the stored TTL, not a flat 300 (fix 2.4)."""
    key = build_cache_key(
        "tranquility", "GET", "/v1/markets/10000002/orders/", {"order_type": "all"}, None
    )
    await cache_client.set(key, b'[{"order_id":1}]', ttl=600, etag='"e1"')
    await cache_client._r.delete(f"esi:body:{key}")  # evict body; stale+etag+ttl remain

    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = ESIResponse(
        status=304, body=b"", etag='"e1"', max_age=None, expires_at=None, not_modified=True
    )

    result = await proxy_request(
        "/v1/markets/10000002/orders/", "GET", {"order_type": "all"}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 200
    body_ttl = await cache_client._r.ttl(f"esi:body:{key}")
    assert body_ttl > 400  # ~600 preserved, not reset to 300


async def test_upstream_5xx_no_fallback_returns_json_envelope(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """A 5xx with no stale/archive is enveloped, not relayed raw (fix 4.4)."""
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = make_esi_500(b"<html>gateway blew up</html>")

    with patch("app.archive.get_latest_payload", new=AsyncMock(return_value=None)):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET", {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )
    assert result.status == 500
    assert result.cache_status == "ERROR"
    assert json.loads(result.body) == {"error": "upstream_error", "status": 500}


async def test_4xx_content_type_is_propagated(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """A non-JSON upstream 4xx keeps its real Content-Type instead of forced JSON (fix 5.6)."""
    mock_esi = AsyncMock()
    resp = make_esi_404(b"<html>nope</html>")
    resp.content_type = "text/html"
    mock_esi.fetch.return_value = resp

    result = await proxy_request(
        "/v1/universe/types/999999/", "GET", {}, None,
        cache_client, mock_esi, mock_db, test_settings,
    )
    assert result.status == 404
    assert result.content_type == "text/html"


async def test_coalesced_waiter_does_not_rewrite_or_rearchive(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    """Only the coalesce leader writes cache/archive; the waiter shares the body (fix 2.7)."""
    body = b'[{"order_id":1}]'
    started = asyncio.Event()
    release = asyncio.Event()
    fetch_calls = {"n": 0}

    async def slow_fetch(*args, **kwargs):
        fetch_calls["n"] += 1
        started.set()
        await release.wait()
        return make_esi_200(body)

    mock_esi = AsyncMock()
    mock_esi.fetch.side_effect = slow_fetch
    write = AsyncMock()

    args = ("/v1/markets/10000002/orders/", "GET", {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings)
    with patch("app.archive.write_snapshot", new=write):
        leader = asyncio.create_task(proxy_request(*args))
        await started.wait()                 # leader is mid-fetch (inflight registered)
        follower = asyncio.create_task(proxy_request(*args))
        await asyncio.sleep(0.05)            # let the follower reach + park at coalesce
        release.set()
        r1 = await leader
        r2 = await follower

    assert r1.status == r2.status == 200
    assert r1.body == r2.body == body
    assert fetch_calls["n"] == 1             # coalesced upstream fetch
    write.assert_called_once()               # only the leader archived


async def test_large_payload_skips_stale_redis_copy(
    cache_client: CacheClient, mock_db, test_settings: Settings
):
    settings = test_settings.model_copy(update={"stale_cache_max_body_bytes": 10})
    mock_esi = AsyncMock()
    body = b'[{"order_id":1,"padding":"larger-than-threshold"}]'
    mock_esi.fetch.return_value = make_esi_200(body)

    with patch("app.archive.write_snapshot", new=AsyncMock()):
        result = await proxy_request(
            "/v1/markets/10000002/orders/", "GET",
            {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, settings,
        )

    key = build_cache_key(
        "tranquility", "GET", "/v1/markets/10000002/orders/", {"order_type": "all"}, None
    )
    assert result.status == 200
    assert await cache_client._r.get(f"esi:body:{key}") == body
    assert await cache_client._r.get(f"esi:stale:{key}") is None
