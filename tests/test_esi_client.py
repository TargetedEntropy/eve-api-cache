"""
Integration-level tests for ESIClient using respx to intercept httpx calls.

No real network connections are made. Each test creates its own ESIClient
instance and closes it after use.
"""
import json
import pytest
import respx
import httpx

from app.esi_client import ESIClient
from app.config import Settings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def esi(test_settings: Settings) -> ESIClient:
    """Fresh ESIClient for each test; properly closed afterward."""
    client = ESIClient(test_settings)
    yield client
    await client.aclose()


# ---------------------------------------------------------------------------
# Test 1: Simple GET returns correct ESIResponse fields
# ---------------------------------------------------------------------------

async def test_simple_get_returns_body_etag_max_age(esi: ESIClient):
    with respx.mock:
        respx.get("https://esi.evetech.net/v1/status/").mock(
            return_value=httpx.Response(
                200,
                json={"players": 500},
                headers={
                    "Cache-Control": "public, max-age=30",
                    "ETag": '"abc"',
                },
            )
        )
        resp = await esi.fetch("/v1/status/")

    assert resp.status == 200
    assert json.loads(resp.body) == {"players": 500}
    assert resp.etag == '"abc"'
    assert resp.max_age == 30
    assert resp.not_modified is False
    assert resp.expires_at is not None


# ---------------------------------------------------------------------------
# Test 2: Paginated GET — all pages are fetched and merged into one array
# ---------------------------------------------------------------------------

async def test_pagination_merges_all_pages(esi: ESIClient):
    page1 = [{"id": 1}, {"id": 2}]
    page2 = [{"id": 3}]
    page3 = [{"id": 4}]

    with respx.mock:
        # Register page-specific routes BEFORE the catch-all so they take priority
        respx.get(
            "https://esi.evetech.net/v1/markets/10000002/orders/",
            params={"page": "2"},
        ).mock(return_value=httpx.Response(200, json=page2))

        respx.get(
            "https://esi.evetech.net/v1/markets/10000002/orders/",
            params={"page": "3"},
        ).mock(return_value=httpx.Response(200, json=page3))

        # Page 1 (no page param) — also advertises X-Pages=3
        respx.get("https://esi.evetech.net/v1/markets/10000002/orders/").mock(
            return_value=httpx.Response(
                200,
                json=page1,
                headers={"Cache-Control": "public, max-age=300", "X-Pages": "3"},
            )
        )

        resp = await esi.fetch("/v1/markets/10000002/orders/")

    assert resp.status == 200
    merged = json.loads(resp.body)
    assert len(merged) == 4
    assert {item["id"] for item in merged} == {1, 2, 3, 4}
    assert resp.max_age == 300


# ---------------------------------------------------------------------------
# Test 3: If-None-Match header sent; 304 returns not_modified=True
# ---------------------------------------------------------------------------

async def test_304_not_modified_preserves_original_etag(esi: ESIClient):
    original_etag = '"abc"'

    with respx.mock:
        respx.get("https://esi.evetech.net/v1/status/").mock(
            return_value=httpx.Response(304, headers={})
        )
        resp = await esi.fetch("/v1/status/", etag=original_etag)

    assert resp.not_modified is True
    assert resp.status == 304
    assert resp.body == b""
    # The original etag must be preserved so proxy.py can store it again
    assert resp.etag == original_etag


# ---------------------------------------------------------------------------
# Test 4: POST bypasses pagination and returns body directly
# ---------------------------------------------------------------------------

async def test_post_returns_body_without_pagination(esi: ESIClient):
    ids = [12345, 67890]
    response_data = [
        {"id": 12345, "name": "Test Char", "category": "character"},
        {"id": 67890, "name": "Test Corp", "category": "corporation"},
    ]

    with respx.mock:
        respx.post("https://esi.evetech.net/v1/universe/names/").mock(
            return_value=httpx.Response(
                200,
                json=response_data,
                headers={"Cache-Control": "public, max-age=3600"},
            )
        )
        resp = await esi.fetch(
            "/v1/universe/names/",
            method="POST",
            body=json.dumps(ids).encode(),
        )

    assert resp.status == 200
    assert json.loads(resp.body) == response_data
    assert resp.max_age == 3600
    assert resp.not_modified is False


# ---------------------------------------------------------------------------
# Test 5: ESI 5xx response is returned as-is (no exception raised)
# ---------------------------------------------------------------------------

async def test_5xx_returned_without_raising(esi: ESIClient):
    with respx.mock:
        respx.get("https://esi.evetech.net/v1/status/").mock(
            return_value=httpx.Response(
                503,
                json={"error": "service unavailable"},
                headers={"X-ESI-Error-Limit-Remain": "40", "X-ESI-Error-Limit-Reset": "30"},
            )
        )
        resp = await esi.fetch("/v1/status/")

    assert resp.status == 503
    assert resp.not_modified is False
    assert resp.error_limit_remain == 40
    assert resp.error_limit_reset == 30


async def test_initial_transport_error_returns_503(esi: ESIClient):
    with respx.mock:
        respx.get("https://esi.evetech.net/v1/status/").mock(
            side_effect=httpx.ConnectError("timeout")
        )
        resp = await esi.fetch("/v1/status/")

    assert resp.status == 503
    assert json.loads(resp.body) == {"error": "upstream request failed"}


async def test_transient_transport_error_is_retried(test_settings: Settings):
    retry_settings = test_settings.model_copy(
        update={"esi_max_retries": 1, "esi_retry_base_delay": 0.0}
    )
    client = ESIClient(retry_settings)
    calls = {"count": 0}

    def handler(request):
        calls["count"] += 1
        if calls["count"] == 1:
            raise httpx.ConnectError("timeout", request=request)
        return httpx.Response(200, json={"players": 100})

    try:
        with respx.mock:
            respx.get("https://esi.evetech.net/v1/status/").mock(side_effect=handler)
            resp = await client.fetch("/v1/status/")
    finally:
        await client.aclose()

    assert resp.status == 200
    assert json.loads(resp.body) == {"players": 100}
    assert calls["count"] == 2


# ---------------------------------------------------------------------------
# Test 6: ESI error-limit headers are parsed correctly
# ---------------------------------------------------------------------------

async def test_error_limit_headers_parsed(esi: ESIClient):
    with respx.mock:
        respx.get("https://esi.evetech.net/v1/status/").mock(
            return_value=httpx.Response(
                200,
                json={"players": 100},
                headers={
                    "Cache-Control": "max-age=30",
                    "X-ESI-Error-Limit-Remain": "97",
                    "X-ESI-Error-Limit-Reset": "55",
                },
            )
        )
        resp = await esi.fetch("/v1/status/")

    assert resp.error_limit_remain == 97
    assert resp.error_limit_reset == 55


# ---------------------------------------------------------------------------
# Test 7: Pagination fetch failure returns 500 error response
# ---------------------------------------------------------------------------

async def test_pagination_partial_failure_returns_500(esi: ESIClient):
    """If any page fetch raises, ESIClient synthesises a 500 error response."""
    page1 = [{"id": 1}]

    with respx.mock:
        respx.get(
            "https://esi.evetech.net/v1/markets/10000002/orders/",
            params={"page": "2"},
        ).mock(side_effect=httpx.ConnectError("timeout"))

        respx.get("https://esi.evetech.net/v1/markets/10000002/orders/").mock(
            return_value=httpx.Response(
                200,
                json=page1,
                headers={"Cache-Control": "max-age=300", "X-Pages": "2"},
            )
        )

        resp = await esi.fetch("/v1/markets/10000002/orders/")

    assert resp.status == 500
    assert resp.not_modified is False


# ---------------------------------------------------------------------------
# Test 8: GET 404 is returned directly (no exception, no pagination)
# ---------------------------------------------------------------------------

async def test_get_404_returned_directly(esi: ESIClient):
    with respx.mock:
        respx.get("https://esi.evetech.net/v1/universe/types/0/").mock(
            return_value=httpx.Response(404, json={"error": "Type not found."})
        )
        resp = await esi.fetch("/v1/universe/types/0/")

    assert resp.status == 404
    assert json.loads(resp.body) == {"error": "Type not found."}
    assert resp.not_modified is False


# ---------------------------------------------------------------------------
# Test 9: max_age is parsed from Expires header when Cache-Control is absent
# ---------------------------------------------------------------------------

async def test_max_age_parsed_from_expires_header(esi: ESIClient):
    from datetime import datetime, timezone, timedelta
    from email.utils import format_datetime

    future = datetime.now(timezone.utc) + timedelta(seconds=120)
    expires_header = format_datetime(future, usegmt=True)

    with respx.mock:
        respx.get("https://esi.evetech.net/v1/status/").mock(
            return_value=httpx.Response(
                200,
                json={"players": 50},
                headers={"Expires": expires_header},
            )
        )
        resp = await esi.fetch("/v1/status/")

    assert resp.status == 200
    # max_age should be approximately 120 (allow 5s tolerance for test timing)
    assert resp.max_age is not None
    assert 110 <= resp.max_age <= 125


# ---------------------------------------------------------------------------
# Error-budget circuit breaker (fix 1.1) + no-retry-on-420 (fix 1.2)
# ---------------------------------------------------------------------------

async def test_420_not_retried_and_trips_breaker(test_settings: Settings):
    """420 is returned on the first try (never retried) and blocks further calls."""
    settings = test_settings.model_copy(
        update={"esi_max_retries": 3, "esi_retry_base_delay": 0.0}
    )
    client = ESIClient(settings)
    try:
        with respx.mock:
            route = respx.get("https://esi.evetech.net/v1/status/").mock(
                return_value=httpx.Response(
                    420,
                    json={"error": "rate limited"},
                    headers={
                        "X-Esi-Error-Limit-Remain": "0",
                        "X-Esi-Error-Limit-Reset": "30",
                    },
                )
            )
            resp1 = await client.fetch("/v1/status/")
            assert resp1.status == 420
            assert route.call_count == 1  # despite esi_max_retries=3

            # Breaker is tripped: the next fetch is a synthetic 503, no upstream call.
            assert client.is_budget_blocked() is True
            resp2 = await client.fetch("/v1/status/")
            assert resp2.status == 503
            assert route.call_count == 1
            assert client.budget_status()["blocked"] is True
    finally:
        await client.aclose()


async def test_low_error_budget_trips_breaker_on_200(test_settings: Settings):
    """A 200 whose remaining-error budget is at/below threshold still trips the breaker."""
    settings = test_settings.model_copy(update={"esi_error_budget_threshold": 10})
    client = ESIClient(settings)
    try:
        with respx.mock:
            respx.get("https://esi.evetech.net/v1/status/").mock(
                return_value=httpx.Response(
                    200,
                    json={"players": 1},
                    headers={
                        "X-Esi-Error-Limit-Remain": "5",
                        "X-Esi-Error-Limit-Reset": "20",
                    },
                )
            )
            resp = await client.fetch("/v1/status/")
            assert resp.status == 200          # this request succeeded
            assert client.is_budget_blocked()  # but budget is now too low to continue

        status = client.budget_status()
        assert status["blocked"] is True
        assert status["error_limit_remain"] == 5
        assert status["block_remaining_seconds"] > 0
    finally:
        await client.aclose()


async def test_503_is_retried_but_500_is_not(test_settings: Settings):
    """502/503/504 remain retriable; 500 is returned directly (fix 1.2)."""
    settings = test_settings.model_copy(
        update={"esi_max_retries": 2, "esi_retry_base_delay": 0.0}
    )

    client = ESIClient(settings)
    try:
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] <= 2:
                return httpx.Response(503)
            return httpx.Response(200, json={"ok": True})

        with respx.mock:
            respx.get("https://esi.evetech.net/v1/status/").mock(side_effect=handler)
            resp = await client.fetch("/v1/status/")
        assert resp.status == 200
        assert calls["n"] == 3  # two 503 retries, then success
    finally:
        await client.aclose()

    client2 = ESIClient(settings)
    try:
        with respx.mock:
            route = respx.get("https://esi.evetech.net/v1/status/").mock(
                return_value=httpx.Response(500, json={"error": "boom"})
            )
            resp = await client2.fetch("/v1/status/")
        assert resp.status == 500
        assert route.call_count == 1  # 500 not retried
    finally:
        await client2.aclose()


# ---------------------------------------------------------------------------
# Outbound token-bucket pacer (fix 1.5)
# ---------------------------------------------------------------------------

async def test_pacer_delays_when_tokens_exhausted(test_settings: Settings):
    import time as _time

    settings = test_settings.model_copy(update={"esi_max_requests_per_second": 10.0})
    client = ESIClient(settings)
    try:
        client._tokens = 0.0
        client._rate_updated = _time.monotonic()
        start = _time.monotonic()
        await client._pace()
        elapsed = _time.monotonic() - start
        assert elapsed >= 0.08  # ~1/rate = 0.1s
    finally:
        await client.aclose()


async def test_pacer_disabled_is_noop(test_settings: Settings):
    import time as _time

    client = ESIClient(test_settings)  # esi_max_requests_per_second=0.0
    try:
        client._tokens = 0.0
        start = _time.monotonic()
        await client._pace()
        assert (_time.monotonic() - start) < 0.02
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Multi-page ETag is not a whole-set validator (fix 2.1)
# ---------------------------------------------------------------------------

async def test_multipage_merged_response_drops_page1_etag(esi: ESIClient):
    """A merged multi-page body must NOT carry page 1's ETag as a whole-set validator."""
    with respx.mock:
        respx.get(
            "https://esi.evetech.net/v1/markets/10000002/orders/", params={"page": "2"}
        ).mock(return_value=httpx.Response(200, json=[{"id": 2}], headers={"ETag": '"p2"'}))
        respx.get("https://esi.evetech.net/v1/markets/10000002/orders/").mock(
            return_value=httpx.Response(
                200, json=[{"id": 1}],
                headers={"Cache-Control": "max-age=300", "X-Pages": "2", "ETag": '"p1"'},
            )
        )
        resp = await esi.fetch("/v1/markets/10000002/orders/")

    assert resp.status == 200
    assert resp.page_count == 2
    assert resp.etag is None  # no faked whole-set validator
    # per-page ETags remain available for future per-page revalidation
    etags = {m.get("etag") for m in resp.page_metadata}
    assert '"p1"' in etags and '"p2"' in etags


async def test_single_page_response_keeps_its_etag(esi: ESIClient):
    with respx.mock:
        respx.get("https://esi.evetech.net/v1/markets/10000002/orders/").mock(
            return_value=httpx.Response(
                200, json=[{"id": 1}],
                headers={"Cache-Control": "max-age=300", "X-Pages": "1", "ETag": '"single"'},
            )
        )
        resp = await esi.fetch("/v1/markets/10000002/orders/")

    assert resp.status == 200
    assert resp.etag == '"single"'  # single-page revalidation still works


# ---------------------------------------------------------------------------
# Redirects are not followed (fix 2.2)
# ---------------------------------------------------------------------------

async def test_redirect_not_followed_returns_location(esi: ESIClient):
    """A 302 is passed through with its Location; the redirect target is never fetched."""
    with respx.mock:
        cdn = respx.get("https://images.evetech.net/characters/95/portrait").mock(
            return_value=httpx.Response(200, content=b"\x89PNG\r\n\x1a\n")
        )
        portrait = respx.get("https://esi.evetech.net/v1/characters/95/portrait/").mock(
            return_value=httpx.Response(
                302, headers={"Location": "https://images.evetech.net/characters/95/portrait"}
            )
        )
        resp = await esi.fetch("/v1/characters/95/portrait/")

    assert resp.status == 302
    assert resp.location == "https://images.evetech.net/characters/95/portrait"
    assert portrait.call_count == 1
    assert cdn.call_count == 0  # upstream redirect target must NOT be fetched
