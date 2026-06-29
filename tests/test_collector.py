"""
Tests for the background data collector.

Uses fakeredis and AsyncMock for ESI; no real PostgreSQL required.
write_snapshot and AsyncSessionLocal are patched at app.collector level
(since collector.py imports them directly into its namespace).
"""
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.collector import (
    _discover_type_ids,
    collect_incursions,
    collect_market_history,
    collect_market_orders,
    collect_market_prices,
    collect_industry_facilities,
    collect_sovereignty_map,
    collect_sovereignty_structures,
    collect_system_jumps,
    collect_system_kills,
)
from app.esi_client import ESIResponse


def make_200(body: bytes, max_age: int = 300, etag: str = '"etag1"') -> ESIResponse:
    return ESIResponse(
        status=200, body=body, etag=etag, max_age=max_age,
        expires_at=None, not_modified=False,
        error_limit_remain=100, error_limit_reset=60,
    )


def make_500() -> ESIResponse:
    return ESIResponse(
        status=500, body=b'{"error":"server error"}', etag=None,
        max_age=None, expires_at=None, not_modified=False,
        error_limit_remain=50, error_limit_reset=60,
    )


@pytest.fixture
def mock_esi():
    return AsyncMock()


class _FakeSessionCM:
    """Async context manager that yields a mock AsyncSession without DB access."""
    def __init__(self):
        self.session = AsyncMock()

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *args):
        return False


def no_db_ctx(monkeypatch_target=None):
    """
    Context manager that patches both write_snapshot and AsyncSessionLocal
    in app.collector so tests run without a real PostgreSQL connection.
    """
    from contextlib import ExitStack, contextmanager

    @contextmanager
    def _ctx():
        with ExitStack() as stack:
            stack.enter_context(
                patch("app.collector.write_snapshot", new=AsyncMock())
            )
            stack.enter_context(
                patch("app.collector.AsyncSessionLocal", _FakeSessionCM)
            )
            yield

    return _ctx()


# ---------------------------------------------------------------------------
# Market orders
# ---------------------------------------------------------------------------

async def test_collect_market_orders_success(cache_client, mock_esi):
    body = json.dumps([{"order_id": 1, "type_id": 34, "price": 100.0}]).encode()
    mock_esi.fetch.return_value = make_200(body)

    with no_db_ctx():
        ok = await collect_market_orders(10000002, mock_esi, cache_client)

    assert ok is True
    mock_esi.fetch.assert_called_once()
    assert "/v1/markets/10000002/orders/" in mock_esi.fetch.call_args.args[0]


async def test_collect_market_orders_caches_body(cache_client, mock_esi):
    body = json.dumps([{"order_id": 1, "type_id": 34, "price": 100.0}]).encode()
    mock_esi.fetch.return_value = make_200(body)

    with no_db_ctx():
        await collect_market_orders(10000002, mock_esi, cache_client)

    keys = [k async for k in cache_client._r.scan_iter("esi:body:*")]
    assert len(keys) == 1


async def test_collect_market_orders_esi_error_returns_false(cache_client, mock_esi):
    mock_esi.fetch.return_value = make_500()

    ok = await collect_market_orders(10000002, mock_esi, cache_client)

    assert ok is False


# ---------------------------------------------------------------------------
# Market prices
# ---------------------------------------------------------------------------

async def test_collect_market_prices_success(cache_client, mock_esi):
    body = json.dumps([{"type_id": 34, "adjusted_price": 5.0}]).encode()
    mock_esi.fetch.return_value = make_200(body)

    with no_db_ctx():
        ok = await collect_market_prices(mock_esi, cache_client)

    assert ok is True
    assert "/v1/markets/prices/" in mock_esi.fetch.call_args.args[0]


# ---------------------------------------------------------------------------
# Market history
# ---------------------------------------------------------------------------

async def test_collect_market_history_single_type(cache_client, mock_esi):
    body = json.dumps([{"date": "2026-01-01", "average": 5.5, "volume": 1000}]).encode()
    mock_esi.fetch.return_value = make_200(body)

    with no_db_ctx():
        ok = await collect_market_history(10000002, 34, mock_esi, cache_client)

    assert ok is True
    call_args = mock_esi.fetch.call_args
    assert "/v1/markets/10000002/history/" in call_args.args[0]
    assert call_args.kwargs.get("params", {}).get("type_id") == "34"


# ---------------------------------------------------------------------------
# Universe time-series
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fn,expected_path", [
    (collect_system_jumps,              "/v1/universe/system_jumps/"),
    (collect_system_kills,              "/v1/universe/system_kills/"),
    (collect_sovereignty_map,           "/v1/sovereignty/map/"),
    (collect_sovereignty_structures,    "/v1/sovereignty/structures/"),
    (collect_incursions,                "/v1/incursions/"),
    (collect_industry_facilities,       "/v1/industry/facilities/"),
])
async def test_universe_collectors(fn, expected_path, cache_client, mock_esi):
    mock_esi.fetch.return_value = make_200(b"[]")

    with no_db_ctx():
        ok = await fn(mock_esi, cache_client)

    assert ok is True
    assert expected_path in mock_esi.fetch.call_args.args[0]


# ---------------------------------------------------------------------------
# Type ID discovery from archive
# ---------------------------------------------------------------------------

async def test_discover_type_ids_empty_archive():
    with patch("app.collector.AsyncSessionLocal", _FakeSessionCM):
        cm = _FakeSessionCM()
        cm.session.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=None))
        with patch("app.collector.AsyncSessionLocal", lambda: cm):
            result = await _discover_type_ids(10000002, "tranquility")

    assert result == []


async def test_discover_type_ids_extracts_from_payload():
    orders = [
        {"order_id": 1, "type_id": 34, "price": 5.0},
        {"order_id": 2, "type_id": 35, "price": 6.0},
        {"order_id": 3, "type_id": 34, "price": 4.5},  # duplicate type_id
    ]

    cm = _FakeSessionCM()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = orders
    cm.session.execute = AsyncMock(return_value=mock_result)

    with patch("app.collector.AsyncSessionLocal", lambda: cm):
        result = await _discover_type_ids(10000002, "tranquility")

    assert set(result) == {34, 35}
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Datasource param handling
# ---------------------------------------------------------------------------

async def test_singularity_datasource_sends_param_to_esi(cache_client, mock_esi):
    mock_esi.fetch.return_value = make_200(b"[]")

    with no_db_ctx():
        await collect_market_orders(10000002, mock_esi, cache_client, datasource="singularity")

    esi_params = mock_esi.fetch.call_args.kwargs.get("params", {})
    assert esi_params.get("datasource") == "singularity"

    from app.allowlist import build_cache_key
    expected_key = build_cache_key(
        "singularity",
        "GET",
        "/v1/markets/10000002/orders/",
        {"order_type": "all", "datasource": "singularity"},
        None,
    )
    cached = await cache_client.get(expected_key)
    assert cached is not None


async def test_tranquility_datasource_omits_param_from_esi(cache_client, mock_esi):
    mock_esi.fetch.return_value = make_200(b"[]")

    with no_db_ctx():
        await collect_market_orders(10000002, mock_esi, cache_client, datasource="tranquility")

    esi_params = mock_esi.fetch.call_args.kwargs.get("params") or {}
    assert "datasource" not in esi_params


async def test_tranquility_cache_key_excludes_datasource_param(cache_client, mock_esi):
    """
    Cache key for tranquility must match what clients send (no explicit datasource=
    in query string), so collector-populated keys are found on proxy lookups.
    """
    from app.allowlist import build_cache_key
    mock_esi.fetch.return_value = make_200(b"[]")

    with no_db_ctx():
        await collect_market_orders(10000002, mock_esi, cache_client)

    expected_key = build_cache_key(
        "tranquility", "GET", "/v1/markets/10000002/orders/", {"order_type": "all"}, None
    )
    cached = await cache_client.get(expected_key)
    assert cached is not None
    assert cached[0] == b"[]"
