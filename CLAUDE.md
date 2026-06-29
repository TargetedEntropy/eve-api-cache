# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

`eve-api-cache` is an unauthenticated caching reverse proxy for the [EVE Online ESI API](https://esi.evetech.net). It acts as a local/shared cache layer so downstream apps (eve-nexum, eve-emptiness, eve-purple, etc.) can call this service instead of hitting ESI directly, reducing rate-limit pressure and improving response times.

**Phase 1 scope:** Public (no-auth) ESI endpoints only — markets, universe, contracts, sovereignty, incursions, killmails, and public character/corp/alliance info.

## ESI Fundamentals

**Base URL:** `https://esi.evetech.net`

**Versioned paths:** Endpoints use explicit versions — `/v1/`, `/v2/`, `/latest/`, `/legacy/`. Always proxy the exact version the caller requests rather than normalizing to `latest`.

**Cache signals ESI provides (must be respected):**
- `Cache-Control: public, max-age=NNN` — primary TTL signal
- `Expires` header — fallback TTL when no max-age
- `ETag` — pass upstream `If-None-Match`; on 304, serve cached body without updating TTL
- `Last-Modified` — secondary conditional request header

**Pagination:** Endpoints like `/markets/{region_id}/orders/` return `X-Pages: N` header. The proxy must fan out pages 2..N concurrently after fetching page 1, then merge and cache the full result set.

**Error budget headers:**
- `X-ESI-Error-Limit-Remain` — errors remaining before ESI throttles
- `X-ESI-Error-Limit-Reset` — seconds until error counter resets
- On 420 (error limit exceeded) or 503: backoff and return cached data if available.

**Datasource:** Default to `tranquility`. Accept `?datasource=singularity` passthrough for testing.

## Public Endpoints to Prioritize

Aggregated from scanning ~15 downstream projects:

**Markets** (highest cache value — shared across all trading tools)
- `GET /markets/{region_id}/orders/` — paginated, ~5 min TTL
- `GET /markets/{region_id}/history/` — per type, ~24 hr TTL
- `GET /markets/prices/` — adjusted/average prices, ~24 hr TTL

**Universe** (near-static, long TTL)
- `GET /universe/types/{type_id}/`
- `GET /universe/systems/`, `/universe/systems/{system_id}/`
- `GET /universe/regions/`, `/universe/regions/{region_id}/`
- `GET /universe/constellations/{constellation_id}/`
- `GET /universe/stations/{station_id}/`
- `GET /universe/planets/{planet_id}/`
- `GET /universe/stars/{star_id}/`
- `GET /universe/factions/`
- `GET /universe/groups/{group_id}/`
- `GET /universe/system_jumps/` — ~1 hr TTL
- `GET /universe/system_kills/` — ~1 hr TTL
- `POST /universe/names/` — batch ID→name, up to 1000 IDs
- `POST /universe/ids/` — batch name→ID

**Characters / Corps / Alliances** (public info only)
- `GET /characters/{character_id}/`
- `GET /characters/{character_id}/portrait/`
- `GET /characters/{character_id}/corporationhistory/`
- `POST /characters/affiliation/` — bulk affiliation lookup
- `GET /corporations/{corporation_id}/`
- `GET /corporations/{corporation_id}/icons/`
- `GET /alliances/`, `/alliances/{alliance_id}/`
- `GET /alliances/{alliance_id}/icons/`
- `GET /alliances/{alliance_id}/corporations/`

**Contracts (public)**
- `GET /contracts/public/{region_id}/` — paginated
- `GET /contracts/public/items/{contract_id}/`
- `GET /contracts/public/bids/{contract_id}/`

**Killmails**
- `GET /killmails/{killmail_id}/{killmail_hash}/`

**Sovereignty / Incursions / Industry**
- `GET /sovereignty/map/`
- `GET /sovereignty/structures/`
- `GET /incursions/`
- `GET /industry/facilities/`
- `GET /status/`

## Architecture

**Stack (matches downstream project ecosystem):** Python + FastAPI, Redis for cache storage, httpx for async upstream calls.

**Cache key scheme:** `esi:{method}:{path}:{sorted_query_string}` — e.g. `esi:GET:/v1/markets/10000002/orders/?order_type=all&page=1`. For merged paginated results use key without `page=` param.

**Request flow:**
1. Incoming request → normalize path/query → check Redis key
2. Cache hit with valid TTL → return immediately with `X-Cache: HIT`
3. Stale/missing → fetch ESI with `If-None-Match` if ETag stored
4. 304 → refresh TTL, return cached body; 200 → store body+ETag, set TTL from `Cache-Control`
5. ESI 5xx → return stale cached data if available (`stale-if-error`)

**Paginated endpoints:** Detect `X-Pages > 1` on first page response, fan out remaining pages concurrently with `asyncio.gather`, merge `items[]` arrays, cache merged result under the base key (no `page=` param). Return merged result to caller regardless of which page they requested.

**POST endpoints** (`/universe/names/`, `/universe/ids/`, `/characters/affiliation/`): Cache individual ID lookups within the batch response so partial cache hits can avoid re-fetching known IDs.

## Development Setup

```bash
# Install deps (once venv/pyproject is established)
pip install -e ".[dev]"

# Run dev server
uvicorn app.main:app --reload --port 8080

# Run tests
pytest

# Run single test
pytest tests/test_markets.py::test_orders_cache_hit -v

# Redis (required)
redis-server --daemonize yes
```

## Key ESI Gotchas

- **`/markets/{region_id}/orders/` with `order_type=all`** is the only call needed — never fetch `buy` and `sell` separately and merge, as `all` is more efficient.
- **`/universe/names/` POST** accepts up to 1000 IDs per call; ESI returns 400 if the list exceeds this.
- **`/characters/{character_id}/portrait/`** returns redirect URLs to image.eveonline.com CDN — cache the redirect target URL, not just the 302.
- **Singularity** (test server) has different region/system IDs than Tranquility; never cross-pollute cache keys between datasources.
- ESI rate limit is per **source IP**. If this proxy is shared across users, error budget is pooled — monitor `X-ESI-Error-Limit-Remain` aggressively.
- Some endpoints (e.g. `/markets/prices/`) don't paginate but are large JSON blobs (~400KB) — store compressed in Redis.
