"""
Route-level tests for the POST body-size streaming cap (fix 4.1).

Covers the incremental reader in isolation (deterministic early-abort) and the
full ASGI route (chunked upload with no Content-Length is still capped).
"""
from unittest.mock import AsyncMock, patch

import httpx

from fastapi import FastAPI

from app.deps import get_cache, get_db, get_esi, get_settings
from app.proxy import ProxyResult
from app.rate_limit import InMemoryRateLimiter
from app.routes import _read_body_capped, router


# ---------------------------------------------------------------------------
# Unit: incremental reader aborts as soon as the cap is crossed
# ---------------------------------------------------------------------------

class _FakeStreamRequest:
    """Minimal stand-in exposing an async stream() like Starlette's Request."""
    def __init__(self, chunks):
        self._chunks = chunks
        self.consumed = 0

    async def stream(self):
        for chunk in self._chunks:
            self.consumed += 1
            yield chunk


async def test_read_body_capped_returns_full_body_under_limit():
    req = _FakeStreamRequest([b"ab", b"cd", b"ef"])
    body = await _read_body_capped(req, limit=100)
    assert body == b"abcdef"
    assert req.consumed == 3


async def test_read_body_capped_aborts_early_over_limit():
    # 5×10 bytes, limit 25 → must stop after the 3rd chunk (total 30 > 25),
    # proving it does NOT buffer the whole stream before rejecting.
    req = _FakeStreamRequest([b"x" * 10] * 5)
    body = await _read_body_capped(req, limit=25)
    assert body is None
    assert req.consumed == 3


# ---------------------------------------------------------------------------
# Integration: drive the real ASGI route
# ---------------------------------------------------------------------------

def _build_app(settings):
    app = FastAPI()
    app.include_router(router)
    app.state.rate_limiter = InMemoryRateLimiter(10_000)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_cache] = lambda: AsyncMock()
    app.dependency_overrides[get_esi] = lambda: AsyncMock()

    async def _fake_db():
        yield AsyncMock()

    app.dependency_overrides[get_db] = _fake_db
    return app


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_chunked_oversized_body_rejected_413(test_settings):
    """A chunked upload (no Content-Length) that exceeds the cap is rejected."""
    settings = test_settings.model_copy(update={"max_post_body_bytes": 50})
    app = _build_app(settings)

    async def gen():
        for _ in range(20):      # 200 bytes, streamed → no Content-Length header
            yield b"0123456789"

    # Spy on the streaming reader to prove the Content-Length fast path did NOT
    # fire (chunked upload) — the streaming cap is what rejects it.
    from app import routes as routes_mod
    real_reader = routes_mod._read_body_capped
    reader_calls = {"n": 0}

    async def spy_reader(request, limit):
        reader_calls["n"] += 1
        return await real_reader(request, limit)

    with patch("app.routes.proxy_request", new=AsyncMock()) as proxied, \
         patch("app.routes._read_body_capped", new=spy_reader):
        async with _client(app) as client:
            resp = await client.post("/v1/universe/names/", content=gen())

    assert resp.status_code == 413
    assert reader_calls["n"] == 1        # reached the streaming reader (no Content-Length shortcut)
    proxied.assert_not_called()          # never reached the proxy/ESI/archive path


async def test_content_length_oversized_body_rejected_fast(test_settings):
    """A truthful oversized Content-Length is rejected on the fast path."""
    settings = test_settings.model_copy(update={"max_post_body_bytes": 50})
    app = _build_app(settings)

    with patch("app.routes.proxy_request", new=AsyncMock()) as proxied:
        async with _client(app) as client:
            resp = await client.post("/v1/universe/names/", content=b"x" * 200)

    assert resp.status_code == 413
    proxied.assert_not_called()


async def test_valid_post_body_is_read_and_forwarded(test_settings):
    """An in-limit body is assembled from the stream and passed to proxy_request."""
    settings = test_settings.model_copy(update={"max_post_body_bytes": 1000})
    app = _build_app(settings)

    proxied = AsyncMock(return_value=ProxyResult(200, b"[]", "MISS"))
    with patch("app.routes.proxy_request", new=proxied):
        async with _client(app) as client:
            resp = await client.post("/v1/universe/names/", content=b"[1,2,3]")

    assert resp.status_code == 200
    proxied.assert_called_once()
    # proxy_request(full_path, method, params, body, ...) → body is positional arg 3
    assert proxied.call_args.args[3] == b"[1,2,3]"


async def test_junk_query_param_rejected_end_to_end(test_settings):
    """Full stack (route → proxy → allowlist → cache): junk params 400 with no side effects."""
    import fakeredis.aioredis
    from app.cache import CacheClient

    app = FastAPI()
    app.include_router(router)
    app.state.rate_limiter = InMemoryRateLimiter(10_000)
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    esi = AsyncMock()

    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_cache] = lambda: CacheClient(redis)
    app.dependency_overrides[get_esi] = lambda: esi

    async def _fake_db():
        yield AsyncMock()

    app.dependency_overrides[get_db] = _fake_db

    async with _client(app) as client:
        resp = await client.get(
            "/v1/markets/10000002/orders/", params={"order_type": "all", "junk": "1"}
        )

    assert resp.status_code == 400
    esi.fetch.assert_not_called()
    keys = [k async for k in redis.scan_iter("esi:*")]
    assert keys == []


async def test_redirect_location_header_propagated(test_settings):
    """A pass-through 3xx surfaces its Location header to the client (fix 2.2)."""
    app = _build_app(test_settings)
    redirect = ProxyResult(302, b"", "MISS", location="https://images.evetech.net/x")

    with patch("app.routes.proxy_request", new=AsyncMock(return_value=redirect)):
        # httpx AsyncClient does not follow redirects by default, so we see the 302.
        async with _client(app) as client:
            resp = await client.get("/v1/characters/95/portrait/")

    assert resp.status_code == 302
    assert resp.headers.get("location") == "https://images.evetech.net/x"


async def test_stale_response_carries_warning_header(test_settings):
    """STALE / archive-fallback responses include a RFC 7234 Warning header (fix 5.5)."""
    app = _build_app(test_settings)
    stale = ProxyResult(200, b"[]", "STALE")

    with patch("app.routes.proxy_request", new=AsyncMock(return_value=stale)):
        async with _client(app) as client:
            resp = await client.get(
                "/v1/markets/10000002/orders/", params={"order_type": "all"}
            )

    assert resp.headers.get("x-cache") == "STALE"
    assert resp.headers.get("warning", "").startswith("110")
