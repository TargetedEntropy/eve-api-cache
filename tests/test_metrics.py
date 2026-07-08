"""Tests for the metrics registry and instrumentation (fix 5.1)."""
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import FastAPI

from app.esi_client import ESIClient, ESIResponse
from app.metrics import MetricsRegistry, metrics
from app.proxy import proxy_request
from app.routes import router


@pytest.fixture(autouse=True)
def _reset_metrics():
    metrics.reset()
    yield
    metrics.reset()


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_counter_accumulates_per_label_set():
    r = MetricsRegistry()
    r.inc_counter("reqs_total", endpoint="/x/")
    r.inc_counter("reqs_total", endpoint="/x/")
    r.inc_counter("reqs_total", endpoint="/y/", value=3)
    assert r.counter_value("reqs_total", endpoint="/x/") == 2
    assert r.counter_value("reqs_total", endpoint="/y/") == 3


def test_gauge_keeps_latest_value():
    r = MetricsRegistry()
    r.set_gauge("budget", 5)
    r.set_gauge("budget", 8)
    assert r.gauge_value("budget") == 8


def test_histogram_buckets_are_cumulative():
    r = MetricsRegistry()
    r.observe("dur_seconds", 0.03, endpoint="/x/")
    r.observe("dur_seconds", 0.2, endpoint="/x/")
    text = r.render()
    assert "# TYPE dur_seconds histogram" in text
    assert 'dur_seconds_count{endpoint="/x/"} 2' in text
    assert 'dur_seconds_bucket{endpoint="/x/",le="0.05"} 1' in text   # only 0.03 ≤ 0.05
    assert 'dur_seconds_bucket{endpoint="/x/",le="0.25"} 2' in text   # both ≤ 0.25
    assert 'dur_seconds_bucket{endpoint="/x/",le="+Inf"} 2' in text


def test_render_emits_help_and_type():
    r = MetricsRegistry()
    r.inc_counter("reqs_total", help="Total requests", status="MISS")
    text = r.render()
    assert "# HELP reqs_total Total requests" in text
    assert "# TYPE reqs_total counter" in text
    assert 'reqs_total{status="MISS"} 1' in text


def test_reset_clears_everything():
    r = MetricsRegistry()
    r.inc_counter("c")
    r.set_gauge("g", 1)
    r.observe("h", 0.1)
    r.reset()
    assert r.counter_value("c") == 0
    assert r.render().strip() == ""


# ---------------------------------------------------------------------------
# Proxy instrumentation
# ---------------------------------------------------------------------------

async def test_proxy_records_request_metric_by_endpoint(cache_client, mock_db, test_settings):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = ESIResponse(200, b'{"players":1}', '"e"', 300, None, False)

    await proxy_request(
        "/v1/status/", "GET", {}, None, cache_client, mock_esi, mock_db, test_settings
    )

    assert metrics.counter_value(
        "proxy_requests_total", endpoint="/status/", cache_status="MISS"
    ) == 1


async def test_proxy_records_archive_write_metric(cache_client, mock_db, test_settings):
    mock_esi = AsyncMock()
    mock_esi.fetch.return_value = ESIResponse(200, b'[{"order_id":1}]', '"e"', 300, None, False)

    with patch("app.archive.write_snapshot", new=AsyncMock()):
        await proxy_request(
            "/v1/markets/10000002/orders/", "GET", {"order_type": "all"}, None,
            cache_client, mock_esi, mock_db, test_settings,
        )

    assert metrics.counter_value(
        "proxy_requests_total", endpoint="/markets/{id}/orders/", cache_status="MISS"
    ) == 1
    assert metrics.counter_value(
        "archive_writes_total", archive_type="time_series", result="ok"
    ) == 1


async def test_proxy_invalid_path_labeled_invalid(cache_client, mock_esi, mock_db, test_settings):
    await proxy_request(
        "/v1/../etc/passwd", "GET", {}, None, cache_client, mock_esi, mock_db, test_settings
    )
    assert metrics.counter_value(
        "proxy_requests_total", endpoint="invalid", cache_status="ERROR"
    ) == 1


# ---------------------------------------------------------------------------
# ESI client instrumentation
# ---------------------------------------------------------------------------

async def test_esi_client_records_status_and_budget(test_settings):
    import respx

    client = ESIClient(test_settings)
    try:
        with respx.mock:
            respx.get("https://esi.evetech.net/v1/status/").mock(
                return_value=httpx.Response(
                    420,
                    headers={"X-Esi-Error-Limit-Remain": "5", "X-Esi-Error-Limit-Reset": "30"},
                )
            )
            await client.fetch("/v1/status/")
    finally:
        await client.aclose()

    assert metrics.counter_value("esi_requests_total", status="420") == 1
    assert metrics.gauge_value("esi_budget_blocked") == 1.0
    assert metrics.gauge_value("esi_error_limit_remain") == 5.0


# ---------------------------------------------------------------------------
# Scheduler job listener
# ---------------------------------------------------------------------------

def test_job_event_listener_records_results():
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
    from app.scheduler import _record_job_event

    class _Event:
        def __init__(self, code, job_id):
            self.code = code
            self.job_id = job_id

    _record_job_event(_Event(EVENT_JOB_EXECUTED, "market_orders_1"))
    _record_job_event(_Event(EVENT_JOB_ERROR, "market_orders_1"))
    _record_job_event(_Event(EVENT_JOB_MISSED, "system_jumps"))

    assert metrics.counter_value("collector_job_runs_total", job="market_orders_1", result="success") == 1
    assert metrics.counter_value("collector_job_runs_total", job="market_orders_1", result="error") == 1
    assert metrics.counter_value("collector_job_runs_total", job="system_jumps", result="missed") == 1


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------

async def test_metrics_endpoint_serves_prometheus_text():
    metrics.inc_counter("proxy_requests_total", endpoint="/status/", cache_status="HIT")

    app = FastAPI()
    app.include_router(router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/metrics")

    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert 'proxy_requests_total{cache_status="HIT",endpoint="/status/"} 1' in resp.text
