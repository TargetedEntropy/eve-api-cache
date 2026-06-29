"""
Async ESI upstream HTTP client.

Handles ETag conditional requests, bounded-concurrency pagination fan-out,
and ESI error budget header tracking. Never called from cache-hit paths.
"""
import asyncio
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

from app.config import Settings


_MAX_AGE_RE = re.compile(r"max-age=(\d+)", re.IGNORECASE)
_TRANSIENT_STATUSES = frozenset({420, 429, 500, 502, 503, 504})


def _parse_max_age(headers: httpx.Headers) -> Optional[int]:
    cc = headers.get("cache-control", "")
    m = _MAX_AGE_RE.search(cc)
    if m:
        return int(m.group(1))
    exp = headers.get("expires")
    if exp:
        try:
            exp_dt = parsedate_to_datetime(exp)
            delta = (exp_dt - datetime.now(timezone.utc)).total_seconds()
            return max(0, int(delta))
        except Exception:
            pass
    return None


def _parse_expires(headers: httpx.Headers) -> Optional[datetime]:
    exp = headers.get("expires")
    if exp:
        try:
            return parsedate_to_datetime(exp)
        except Exception:
            pass
    max_age = _parse_max_age(headers)
    if max_age is not None:
        from datetime import timedelta
        return datetime.now(timezone.utc) + timedelta(seconds=max_age)
    return None


@dataclass
class ESIResponse:
    status: int
    body: bytes
    etag: Optional[str]
    max_age: Optional[int]
    expires_at: Optional[datetime]
    not_modified: bool
    error_limit_remain: Optional[int] = None
    error_limit_reset: Optional[int] = None
    page_count: int = 1
    page_metadata: list[dict] = field(default_factory=list)


class ESIClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._page_sem = asyncio.Semaphore(settings.page_concurrency)
        self._upstream_sem = asyncio.Semaphore(settings.upstream_concurrency)
        self._http = httpx.AsyncClient(
            base_url=settings.esi_base_url,
            headers={"User-Agent": settings.user_agent},
            timeout=settings.esi_timeout,
            follow_redirects=True,
        )

    async def fetch(
        self,
        path: str,
        method: str = "GET",
        params: Optional[dict] = None,
        body: Optional[bytes] = None,
        etag: Optional[str] = None,
    ) -> ESIResponse:
        """
        Fetch one ESI endpoint. For paginated GET responses (X-Pages > 1),
        automatically fetches all remaining pages with bounded concurrency
        and returns a merged JSON array body.

        On ESI 304: returns not_modified=True with empty body.
        On ESI 5xx: returns the error response (caller handles stale fallback).
        """
        headers = {}
        if etag:
            headers["If-None-Match"] = etag

        content_type = "application/json" if body else None
        if content_type:
            headers["Content-Type"] = content_type

        try:
            resp = await self._request_with_retries(
                method,
                path,
                params=params,
                content=body,
                headers=headers,
            )
        except httpx.RequestError:
            return ESIResponse(
                status=503,
                body=b'{"error":"upstream request failed"}',
                etag=None,
                max_age=None,
                expires_at=None,
                not_modified=False,
            )

        error_remain = _int_header(resp.headers, "x-esi-error-limit-remain")
        error_reset = _int_header(resp.headers, "x-esi-error-limit-reset")
        max_age = _parse_max_age(resp.headers)
        expires_at = _parse_expires(resp.headers)
        resp_etag = resp.headers.get("etag")
        page_metadata = [_page_metadata(1, resp)]

        if resp.status_code == 304:
            return ESIResponse(
                status=304,
                body=b"",
                etag=etag,  # preserve original etag
                max_age=max_age,
                expires_at=expires_at,
                not_modified=True,
                error_limit_remain=error_remain,
                error_limit_reset=error_reset,
                page_metadata=page_metadata,
            )

        if resp.status_code != 200 or method.upper() != "GET":
            return ESIResponse(
                status=resp.status_code,
                body=resp.content,
                etag=resp_etag,
                max_age=max_age,
                expires_at=expires_at,
                not_modified=False,
                error_limit_remain=error_remain,
                error_limit_reset=error_reset,
                page_metadata=page_metadata,
            )

        # Fan out additional pages if paginated
        x_pages = _int_header(resp.headers, "x-pages") or 1
        merged_body = resp.content

        if x_pages > 1:
            try:
                merged_body, extra_page_metadata = await self._fetch_all_pages(
                    path, params, resp.content, x_pages
                )
                page_metadata.extend(extra_page_metadata)
            except Exception:
                # Partial failure — return error so caller serves stale/archive
                return ESIResponse(
                    status=500,
                    body=b'{"error": "pagination fetch failed"}',
                    etag=None,
                    max_age=None,
                    expires_at=None,
                    not_modified=False,
                    error_limit_remain=error_remain,
                    error_limit_reset=error_reset,
                    page_count=x_pages,
                    page_metadata=page_metadata,
                )

        return ESIResponse(
            status=200,
            body=merged_body,
            etag=resp_etag,
            max_age=max_age,
            expires_at=expires_at,
            not_modified=False,
            error_limit_remain=error_remain,
            error_limit_reset=error_reset,
            page_count=x_pages,
            page_metadata=page_metadata,
        )

    async def _fetch_all_pages(
        self, path: str, params: Optional[dict], page1_body: bytes, total_pages: int
    ) -> tuple[bytes, list[dict]]:
        """Fetch pages 2..N with a semaphore and merge into a single JSON array."""
        base_params = {k: v for k, v in (params or {}).items() if k != "page"}

        async def fetch_page(page: int) -> tuple[int, list, dict]:
            async with self._page_sem:
                r = await self._request_with_retries(
                    "GET", path, params={**base_params, "page": page}
                )
                r.raise_for_status()
                advertised_pages = _int_header(r.headers, "x-pages")
                if advertised_pages is not None and advertised_pages != total_pages:
                    raise ValueError("inconsistent X-Pages during pagination")
                page_data = json.loads(r.content)
                if not isinstance(page_data, list):
                    raise ValueError("paginated ESI response page is not a JSON list")
                return page, page_data, _page_metadata(page, r)

        tasks = [fetch_page(p) for p in range(2, total_pages + 1)]
        pages = await asyncio.gather(*tasks)

        merged = json.loads(page1_body)
        if not isinstance(merged, list):
            raise ValueError("paginated ESI response page 1 is not a JSON list")

        page_metadata = []
        for _page, page_data, metadata in pages:
            merged.extend(page_data)
            page_metadata.append(metadata)
        return json.dumps(merged).encode(), page_metadata

    async def _request_with_retries(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Run one upstream HTTP request under the global concurrency cap."""
        last_exc: Optional[httpx.RequestError] = None
        for attempt in range(self._settings.esi_max_retries + 1):
            try:
                async with self._upstream_sem:
                    resp = await self._http.request(method, path, **kwargs)
                if resp.status_code not in _TRANSIENT_STATUSES:
                    return resp
                if attempt >= self._settings.esi_max_retries:
                    return resp
                await self._sleep_before_retry(resp.headers, attempt)
            except httpx.RequestError as exc:
                last_exc = exc
                if attempt >= self._settings.esi_max_retries:
                    raise
                await self._sleep_before_retry(None, attempt)
        assert last_exc is not None
        raise last_exc

    async def _sleep_before_retry(self, headers: Optional[httpx.Headers], attempt: int) -> None:
        retry_after = _int_header(headers or httpx.Headers(), "retry-after")
        delay = retry_after if retry_after is not None else self._settings.esi_retry_base_delay * (2 ** attempt)
        await asyncio.sleep(min(delay, 5.0))

    async def aclose(self) -> None:
        await self._http.aclose()


def _int_header(headers: httpx.Headers, name: str) -> Optional[int]:
    v = headers.get(name)
    if v is not None:
        try:
            return int(v)
        except ValueError:
            pass
    return None


def _page_metadata(page: int, response: httpx.Response) -> dict:
    return {
        "page": page,
        "status": response.status_code,
        "etag": response.headers.get("etag"),
        "cache_control": response.headers.get("cache-control"),
        "expires": response.headers.get("expires"),
        "x_pages": response.headers.get("x-pages"),
    }
