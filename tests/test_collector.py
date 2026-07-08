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
        # _accumulated_type_ids does `result = await session.execute(...); result.all()`.
        # Return a sync result with an empty row set so no unawaited coroutine leaks.
        exec_result = MagicMock()
        exec_result.all.return_value = []
        self.session.execute.return_value = exec_result

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
    cm = _FakeSessionCM()
    with patch("app.collector.AsyncSessionLocal", lambda: cm), patch(
        "app.archive.get_latest_payload", new=AsyncMock(return_value=None)
    ):
        result = await _discover_type_ids(10000002, "tranquility")

    assert result == []


async def test_discover_type_ids_extracts_from_payload():
    orders = [
        {"order_id": 1, "type_id": 34, "price": 5.0},
        {"order_id": 2, "type_id": 35, "price": 6.0},
        {"order_id": 3, "type_id": 34, "price": 4.5},  # duplicate type_id
    ]

    cm = _FakeSessionCM()
    with patch("app.collector.AsyncSessionLocal", lambda: cm), patch(
        "app.archive.get_latest_payload", new=AsyncMock(return_value=json.dumps(orders).encode())
    ):
        result = await _discover_type_ids(10000002, "tranquility")

    assert set(result) == {34, 35}
    assert len(result) == 2


# ---------------------------------------------------------------------------
# Datasource param handling
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Error-budget breaker aborts bulk history collection (fix 1.5)
# ---------------------------------------------------------------------------

async def test_market_history_region_skips_when_budget_blocked(cache_client):
    from app.collector import collect_market_history_for_region

    esi = AsyncMock()
    esi.is_budget_blocked = MagicMock(return_value=True)

    result = await collect_market_history_for_region(10000002, esi, cache_client)

    assert result == 0
    esi.fetch.assert_not_called()  # no discovery, no fan-out


async def test_market_history_region_runs_when_not_blocked(cache_client):
    from app.collector import collect_market_history_for_region

    esi = AsyncMock()
    esi.is_budget_blocked = MagicMock(return_value=False)

    # Not blocked → proceeds to discovery; empty archive yields 0 fetched.
    with patch("app.collector.AsyncSessionLocal", _FakeSessionCM), patch(
        "app.archive.get_latest_payload", new=AsyncMock(return_value=None)
    ):
        result = await collect_market_history_for_region(10000002, esi, cache_client)

    assert result == 0
    esi.fetch.assert_not_called()  # empty archive → nothing to fetch, but discovery ran


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


async def test_collector_warms_alias_version_keys(cache_client, mock_esi, monkeypatch):
    """With collector_warm_alias_versions=['latest'], the body is warmed under
    both /v1/ and /latest/ keys (2.9)."""
    from app.allowlist import build_cache_key
    import app.collector as collector_mod

    monkeypatch.setattr(collector_mod.settings, "collector_warm_alias_versions", ["latest"])
    mock_esi.fetch.return_value = make_200(b"[]")

    with no_db_ctx():
        await collect_market_orders(10000002, mock_esi, cache_client)

    v1_key = build_cache_key("tranquility", "GET", "/v1/markets/10000002/orders/", {"order_type": "all"}, None)
    latest_key = build_cache_key("tranquility", "GET", "/latest/markets/10000002/orders/", {"order_type": "all"}, None)
    assert await cache_client.get(v1_key) is not None
    assert await cache_client.get(latest_key) is not None


async def test_collector_no_alias_by_default(cache_client, mock_esi):
    """Default (no aliases) warms only the canonical key."""
    from app.allowlist import build_cache_key
    mock_esi.fetch.return_value = make_200(b"[]")

    with no_db_ctx():
        await collect_market_orders(10000002, mock_esi, cache_client)

    latest_key = build_cache_key("tranquility", "GET", "/latest/markets/10000002/orders/", {"order_type": "all"}, None)
    assert await cache_client.get(latest_key) is None


def test_swap_version():
    from app.collector import _swap_version
    assert _swap_version("/v1/markets/10000002/orders/", "latest") == "/latest/markets/10000002/orders/"
    assert _swap_version("/v1/universe/types/34/", "legacy") == "/legacy/universe/types/34/"


async def test_accumulated_type_ids_unions_into_discovery():
    """History discovery includes type_ids from accumulated versions, not just the
    latest snapshot (3.5)."""
    from unittest.mock import MagicMock
    from app.collector import _discover_type_ids

    cm = _FakeSessionCM()
    # Accumulated query returns two type_ids (as text, like ->> yields).
    acc_result = MagicMock()
    acc_result.all.return_value = [("34",), ("9999",)]
    cm.session.execute.return_value = acc_result

    # Latest snapshot contributes type_id 34 (overlaps) and 35.
    snapshot = [{"order_id": 1, "type_id": 34}, {"order_id": 2, "type_id": 35}]
    with patch("app.collector.AsyncSessionLocal", lambda: cm), patch(
        "app.archive.get_latest_payload", new=AsyncMock(return_value=json.dumps(snapshot).encode())
    ):
        result = await _discover_type_ids(10000002, "tranquility")

    assert set(result) == {34, 35, 9999}  # 9999 only exists in accumulated history


async def test_accumulated_type_ids_degrades_on_error():
    from app.collector import _accumulated_type_ids
    session = AsyncMock()
    session.execute.side_effect = RuntimeError("db down")
    assert await _accumulated_type_ids(session, "tranquility", 10000002) == set()


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
