"""
Unit tests for proxy_request().

Uses:
  - Real CacheClient backed by fakeredis (cache hit/miss behaviour is real)
  - AsyncMock for ESIClient (no real HTTP calls)
  - patch() for app.archive functions (no real PostgreSQL)
"""
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
