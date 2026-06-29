"""FastAPI routes — health check, collector status, catch-all ESI proxy."""
from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import CacheClient
from app.config import Settings
from app.deps import get_cache, get_db, get_esi, get_settings
from app.esi_client import ESIClient
from app.proxy import proxy_request
from app.scheduler import scheduler_status

router = APIRouter()

_CACHE_STATUS_HEADERS = {
    "HIT": {"X-Cache": "HIT"},
    "MISS": {"X-Cache": "MISS"},
    "STALE": {"X-Cache": "STALE"},
    "ARCHIVE_FALLBACK": {"X-Cache": "STALE", "X-Archive-Fallback": "true"},
    "ERROR": {"X-Cache": "ERROR"},
}


@router.get("/healthz")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/collector/status")
async def collector_status(request: Request) -> dict:
    """List all scheduled collector jobs and their next run times."""
    jobs = scheduler_status(request.app.state.scheduler)
    return {"jobs": jobs, "count": len(jobs)}


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
    full_path = f"/{version}/{path}"
    method = request.method
    params = dict(request.query_params)
    body = None
    if method == "POST":
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                declared_length = int(content_length)
            except ValueError:
                return Response(
                    content=b'{"error":"invalid content-length"}',
                    status_code=400,
                    media_type="application/json",
                    headers={"X-Cache": "ERROR"},
                )
            if declared_length > cfg.max_post_body_bytes:
                return Response(
                    content=b'{"error":"request body too large"}',
                    status_code=413,
                    media_type="application/json",
                    headers={"X-Cache": "ERROR"},
                )
        body = await request.body()
        if len(body) > cfg.max_post_body_bytes:
            return Response(
                content=b'{"error":"request body too large"}',
                status_code=413,
                media_type="application/json",
                headers={"X-Cache": "ERROR"},
            )

    result = await proxy_request(full_path, method, params, body, cache, esi, db, cfg)

    extra_headers = _CACHE_STATUS_HEADERS.get(result.cache_status, {})
    return Response(
        content=result.body,
        status_code=result.status,
        media_type=result.content_type,
        headers=extra_headers,
    )
