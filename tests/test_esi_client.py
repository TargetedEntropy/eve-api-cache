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
