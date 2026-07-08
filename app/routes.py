"""FastAPI routes — health check, collector status, catch-all ESI proxy."""
from typing import Optional

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import CacheClient
from app.config import Settings
from app.deps import get_cache, get_db, get_esi, get_settings
from app.esi_client import ESIClient
from app.metrics import metrics
from app.proxy import proxy_request
from app.scheduler import scheduler_status

router = APIRouter()

_WARNING_STALE = '110 - "Response is Stale"'
_CACHE_STATUS_HEADERS = {
    "HIT": {"X-Cache": "HIT"},
    "MISS": {"X-Cache": "MISS"},
    "STALE": {"X-Cache": "STALE", "Warning": _WARNING_STALE},
    "ARCHIVE_FALLBACK": {"X-Cache": "STALE", "X-Archive-Fallback": "true", "Warning": _WARNING_STALE},
    "ERROR": {"X-Cache": "ERROR"},
}

_JSON = "application/json"


def _error_response(body: bytes, status: int) -> Response:
    return Response(content=body, status_code=status, media_type=_JSON, headers={"X-Cache": "ERROR"})


async def _read_body_capped(request: Request, limit: int) -> Optional[bytes]:
    """
    Read the request body incrementally, aborting as soon as it exceeds `limit`
    bytes. Returns None if the cap is crossed so the caller can respond 413 — a
    chunked upload with no Content-Length can't buffer unbounded memory first.
    """
    total = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


@router.get("/healthz")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/collector/status")
async def collector_status(request: Request) -> dict:
    """List all scheduled collector jobs and their next run times."""
    jobs = scheduler_status(request.app.state.scheduler)
    return {"jobs": jobs, "count": len(jobs)}


@router.get("/metrics")
async def metrics_endpoint() -> Response:
    """Prometheus text exposition of in-process metrics."""
    return Response(
        content=metrics.render(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.api_route("/{version}/{path:path}", methods=["GET", "POST"])
async def proxy(
    version: str,
    path: str,
    request: Request,
    cache: CacheClient = Depends(get_cache),
    db: AsyncSession = Depends(get_db),
    esi: ESIClient = Depends(get_esi),
    cfg: Settings = Depends(get_settings),
) -> Response:
    client_host = request.client.host if request.client else "unknown"
    allowed, remaining = await request.app.state.rate_limiter.allow(client_host)
    if not allowed:
        metrics.inc_counter("client_rate_limited_total", help="Requests rejected by the per-client rate limiter")
        return Response(
            content=b'{"error":"rate limit exceeded"}',
            status_code=429,
            media_type="application/json",
            headers={
                "X-Cache": "ERROR",
                "X-RateLimit-Remaining": "0",
                "Retry-After": "60",
            },
        )

    full_path = f"/{version}/{path}"
    method = request.method
    params = dict(request.query_params)
    body = None
    if method == "POST":
        content_length = request.headers.get("content-length")
        if content_length:
            # Fast-path rejection when a truthful Content-Length is already oversized.
            try:
                declared_length = int(content_length)
            except ValueError:
                return _error_response(b'{"error":"invalid content-length"}', 400)
            if declared_length > cfg.max_post_body_bytes:
                return _error_response(b'{"error":"request body too large"}', 413)
        # Content-Length is absent/untrusted on chunked uploads, so cap while reading.
        body = await _read_body_capped(request, cfg.max_post_body_bytes)
        if body is None:
            return _error_response(b'{"error":"request body too large"}', 413)

    result = await proxy_request(full_path, method, params, body, cache, esi, db, cfg)

    extra_headers = _CACHE_STATUS_HEADERS.get(result.cache_status, {})
    extra_headers = {**extra_headers, "X-RateLimit-Remaining": str(remaining)}
    if result.location:
        extra_headers["Location"] = result.location
    return Response(
        content=result.body,
        status_code=result.status,
        media_type=result.content_type,
        headers=extra_headers,
    )
