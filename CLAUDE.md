# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

`eve-api-cache` is an unauthenticated ESI proxy and **permanent historical archive** for the [EVE Online ESI API](https://esi.evetech.net). It serves two inseparable roles:

1. **Proxy cache** — downstream apps (eve-nexum, eve-emptiness, eve-purple, etc.) call this service instead of ESI directly, reducing rate-limit pressure and latency.
2. **Historical archive** — ESI data is ephemeral. Market orders vanish when they fill or expire. System jump/kill stats are only available as a rolling snapshot. Market history is capped at ~13 months. Sovereignty maps overwrite themselves. This service persists every snapshot it collects indefinitely so that long-term analysis is possible.

Data is **never deleted from the archive**. Redis provides the hot-cache layer (short TTL, respects ESI cache headers). A persistent database provides the archive layer (no expiry, append-only for time-series data).

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

**Markets** (highest archive value — time-series snapshots build long-term price history)
- `GET /markets/{region_id}/orders/` — paginated; snapshot every ~5 min, archive each snapshot with timestamp
- `GET /markets/{region_id}/history/` — ESI only exposes ~13 months; archive permanently to extend coverage indefinitely
- `GET /markets/prices/` — adjusted/average prices; archive each daily snapshot

**Universe** (near-static reference data)
- `GET /universe/types/{type_id}/`
- `GET /universe/systems/`, `/universe/systems/{system_id}/`
- `GET /universe/regions/`, `/universe/regions/{region_id}/`
- `GET /universe/constellations/{constellation_id}/`
- `GET /universe/stations/{station_id}/`
- `GET /universe/planets/{planet_id}/`
- `GET /universe/stars/{star_id}/`
- `GET /universe/factions/`
- `GET /universe/groups/{group_id}/`
- `GET /universe/system_jumps/` — rolling snapshot only on ESI; archive each poll to build jump activity history
- `GET /universe/system_kills/` — same; archive each poll for historical kill activity
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

**Sovereignty / Incursions / Industry** (temporal — archive snapshots to track changes over time)
- `GET /sovereignty/map/` — territorial control changes; archive each snapshot to track alliance history
- `GET /sovereignty/structures/` — same
- `GET /incursions/` — archive each poll; ESI only shows active incursions, archive builds historical record
- `GET /industry/facilities/` — archive changes
- `GET /status/` — not archived; proxy-only

## Architecture

**Stack (matches downstream project ecosystem):** Python + FastAPI, Redis (hot cache), PostgreSQL (persistent archive), httpx for async upstream calls.

**Two-tier storage model:**
- **Redis** — hot cache with TTL matching ESI `Cache-Control`. Evicts naturally. Serves repeated proxy requests without hitting ESI or the DB.
- **PostgreSQL** — permanent archive. Data written here is never deleted. Time-series endpoints (markets, jumps, kills, sovereignty) get a timestamp column and append-only inserts. Reference data (universe types, systems, etc.) gets upserted.

**Cache/archive key scheme:** `esi:{method}:{path}:{sorted_query_string}` — e.g. `esi:GET:/v1/markets/10000002/orders/?order_type=all`. For merged paginated results, omit `page=` from the key.

**Request flow:**
1. Incoming request → normalize path/query → check Redis key
2. Redis hit with valid TTL → return immediately with `X-Cache: HIT`
3. Redis miss → fetch ESI with `If-None-Match` if ETag stored
4. ESI 304 → refresh Redis TTL, return cached body (no archive write needed)
5. ESI 200 → write to Redis (with TTL) **and** write to PostgreSQL archive (with `fetched_at` timestamp)
6. ESI 5xx → return stale Redis data if available; fall back to most recent archive row

**Archive write strategy by endpoint type:**
- **Time-series** (orders, prices, jumps, kills, sovereignty, incursions): append every new snapshot with `fetched_at`. Never overwrite.
- **Reference/static** (universe types, systems, regions, corps, characters): upsert on primary key, store `first_seen_at` and `last_updated_at`.
- **Event data** (killmails, contracts, contract items): insert-once by natural key (killmail_id+hash, contract_id). Immutable after first write.

**Paginated endpoints:** Detect `X-Pages > 1` on first page response, fan out remaining pages concurrently with `asyncio.gather`, merge `items[]` arrays. Archive the merged result as a single snapshot. Return merged result to caller.

**POST endpoints** (`/universe/names/`, `/universe/ids/`, `/characters/affiliation/`): Cache and archive individual ID→name mappings extracted from each batch response so future lookups for known IDs never hit ESI.

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

# PostgreSQL (required for archive layer)
# Run migrations before starting the server
alembic upgrade head
```

## Key ESI Gotchas

- **`/markets/{region_id}/orders/` with `order_type=all`** is the only call needed — never fetch `buy` and `sell` separately and merge, as `all` is more efficient.
- **`/universe/names/` POST** accepts up to 1000 IDs per call; ESI returns 400 if the list exceeds this.
- **`/characters/{character_id}/portrait/`** returns redirect URLs to image.eveonline.com CDN — cache the redirect target URL, not just the 302.
- **Singularity** (test server) has different region/system IDs than Tranquility; never cross-pollute cache keys between datasources.
- ESI rate limit is per **source IP**. If this proxy is shared across users, error budget is pooled — monitor `X-ESI-Error-Limit-Remain` aggressively.
- Some endpoints (e.g. `/markets/prices/`) don't paginate but are large JSON blobs (~400KB) — store compressed in Redis and as JSONB in PostgreSQL.
- **Never delete archive rows.** If a market order disappears from the live feed, it still exists in the archive as a historical record. Deletions only happen in Redis (via TTL expiry).
