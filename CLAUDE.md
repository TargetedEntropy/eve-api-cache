# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

`eve-api-cache` is an unauthenticated ESI proxy and **permanent historical archive** for the [EVE Online ESI API](https://esi.evetech.net). It serves two inseparable roles:

1. **Proxy cache** — downstream apps (eve-nexum, eve-emptiness, eve-purple, etc.) call this service instead of ESI directly, reducing rate-limit pressure and latency.
2. **Historical archive** — ESI data is ephemeral. Market orders vanish when they fill or expire. System jump/kill stats are only available as a rolling snapshot. Market history is capped at ~13 months. Sovereignty maps overwrite themselves. This service persists every snapshot it collects indefinitely so that long-term analysis is possible.

Data is **never deleted from the archive**. Redis provides the hot-cache layer (short TTL, respects ESI cache headers). A persistent database provides the archive layer (no expiry, append-only for time-series data).

**Phase 1 scope:** Public (no-auth) ESI endpoints only — markets, universe, contracts, sovereignty, incursions, killmails, and public character/corp/alliance info. Do not add OAuth flows, token storage, or private-character scopes in this project unless the scope is deliberately expanded later.

## Current Repository State

This repository currently contains the project guidance/spec only. Before following the development commands below, first add the actual FastAPI project scaffold, dependency metadata, tests, migrations, and deployment files. Do not assume `app.main`, `pyproject.toml`, Alembic migrations, or test modules already exist until they are present in the repo.

When bootstrapping the project, keep the first implementation small and verifiable:
- `GET /healthz`
- one cached/proxied ESI endpoint
- Redis TTL behavior from ESI headers
- one archive write path in PostgreSQL
- tests proving cache hit, cache miss, archive insert, and stale fallback behavior

## ESI Fundamentals

**Base URL:** `https://esi.evetech.net`

**Versioned paths:** Endpoints use explicit versions — `/v1/`, `/v2/`, `/latest/`, `/legacy/`. Always proxy the exact version the caller requests rather than normalizing to `latest`.

**Cache signals ESI provides (must be respected):**
- `Cache-Control: public, max-age=NNN` — primary TTL signal
- `Expires` header — fallback TTL when no max-age
- `ETag` — pass upstream `If-None-Match`; on 304, serve cached body and extend the hot-cache TTL using the new `Cache-Control` header if present, otherwise preserve the existing TTL. Do not write an archive payload on 304.
- `Last-Modified` — secondary conditional request header
- Always send a clear `User-Agent` identifying this service and contact/project URL; ESI operators expect well-behaved clients, and anonymous default library agents make incident response miserable

**Pagination:** Endpoints like `/markets/{region_id}/orders/` return `X-Pages: N` header. The proxy must fan out pages 2..N after fetching page 1, but with bounded concurrency and retry/backoff. Do not unleash hundreds of simultaneous requests and then act shocked when ESI slaps the shared error budget. Cache/archive only complete page sets; if a page fails, either serve the previous complete cached/archive snapshot or return a partial-data error that is explicitly marked incomplete.

**Error budget headers:**
- `X-ESI-Error-Limit-Remain` — errors remaining before ESI throttles
- `X-ESI-Error-Limit-Reset` — seconds until error counter resets
- On 420 (error limit exceeded) or 503: backoff and return cached data if available.

**Datasource:** Default to `tranquility`. Accept `?datasource=singularity` passthrough for testing. Include datasource in every cache/archive key and database uniqueness constraint.

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

**Cache/archive key scheme:** `esi:{datasource}:{method}:{path}:{sorted_query_string}` — e.g. `esi:tranquility:GET:/v1/markets/10000002/orders/?order_type=all`. For merged paginated results, omit `page=` from the key. For POST endpoints, include a stable hash of the normalized request body in the key and archive extracted normalized entities separately.

**Request flow:**
1. Incoming request → normalize path/query → check Redis key
2. Redis hit with valid TTL → return immediately with `X-Cache: HIT`
3. Redis miss → fetch ESI with `If-None-Match` if ETag stored
4. ESI 304 → extend Redis TTL if new `Cache-Control` header present (otherwise preserve existing TTL), return cached body. No archive write.
5. ESI 200 → write to Redis (with TTL) **and** write to PostgreSQL archive (with `fetched_at` timestamp)
6. ESI 5xx → return stale Redis data if available; fall back to most recent archive row

**Archive write strategy by endpoint type:**
- **Time-series** (orders, prices, jumps, kills, sovereignty, incursions): append each complete observation with `fetched_at`, `observed_until`/ESI expiry when available, and a `content_hash`. Never overwrite historical facts. If payloads are unchanged, it is acceptable to store a compact observation row pointing at the prior payload blob instead of duplicating large JSON forever.
- **Reference/static** (universe types, systems, regions, corps, characters): upsert on primary key plus datasource, store `first_seen_at`, `last_updated_at`, ETag/Last-Modified, and raw payload for traceability.
- **Event data** (killmails, contracts, contract items): insert-once by natural key (killmail_id+hash, contract_id) plus datasource. Immutable after first write except for validation metadata.

**Paginated endpoints:** Detect `X-Pages > 1` on first page response, fan out remaining pages concurrently with a semaphore, merge `items[]` arrays in page order, and preserve response metadata from every page. Archive the merged result as a single snapshot only when all pages succeed for the same request generation. Return merged result to caller.

**POST endpoints** (`/universe/names/`, `/universe/ids/`, `/characters/affiliation/`): Normalize/sort/dedupe request bodies before hashing batch-level cache keys; split oversized batches before sending to ESI. Additionally, extract and store each resolved ID→name mapping into a per-ID key namespace (`esi:{datasource}:name:{id}`) so future single-ID lookups hit the cache without re-batching. The batch key and the per-ID keys are separate: batch key caches the raw ESI response, per-ID keys enable individual lookups.

## Operational Requirements

- **Respect ESI health before freshness.** Track error-limit headers, timeout rates, and 420/5xx responses. Prefer stale cached/archive responses with `Warning`/`X-Cache: STALE` over hammering ESI during trouble.
- **Expose observability from day one:** counters for upstream requests, cache hits/misses/stale serves, archive writes, page fanout failures, 304 revalidations, current ESI error budget, and per-endpoint latency.
- **Make archive writes idempotent:** database constraints should prevent duplicate rows for the same datasource, endpoint identity, natural key or `(fetched_at bucket, content_hash)` as appropriate. Retries must not multiply history.
- **Store enough provenance to trust the archive:** endpoint, versioned path, normalized query/body hash, datasource, fetched_at, ESI expiry/cache headers, ETag/Last-Modified, HTTP status, and payload/content hash.
- **Do not fabricate fixtures as live data.** Tests may use recorded fixtures, but runtime responses must clearly distinguish live ESI, hot cache, stale cache, and archive fallback.

## Proxy Safety Requirements

This service must not become a general-purpose open proxy. Only proxy requests to the canonical ESI host, and only for explicitly allowed public endpoint patterns.

Security rules:
- Never accept a caller-supplied upstream hostname, scheme, or full URL.
- Reject paths containing `..`, encoded path traversal, backslashes, control characters, or duplicate/ambiguous slashes.
- Preserve ESI path versions, but validate the first path segment is one of the supported ESI versions.
- Keep a route allowlist for Phase 1 public endpoints. Return 404/403 for unknown paths instead of blindly forwarding them.
- Reject private/authenticated ESI endpoints until OAuth, token storage, and per-user authorization are designed separately.
- Enforce request body size limits for POST batch endpoints and validate batch lengths before forwarding to ESI.
- Add per-client rate limiting and upstream concurrency caps so one downstream service cannot burn the shared ESI error budget.
- Do not log access tokens, cookies, request bodies for batch lookups, or full upstream error payloads if they may contain caller data.

Reliability rules:
- Coalesce identical in-flight upstream requests so cache stampedes do not fan out to ESI.
- Use a semaphore to bound concurrency for paginated endpoint fan-out.
- Store ESI response metadata with archive rows where useful: status, ETag, Expires, Cache-Control, datasource, request key, and fetched_at.
- Treat stale Redis/archive fallback as degraded mode and mark responses with headers such as `X-Cache: STALE` or `X-Archive-Fallback: true`.

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
- Some endpoints (e.g. `/markets/prices/`) don't paginate but are large JSON blobs (~400KB) — store compressed in Redis and consider compressed raw payload storage plus indexed extracted fields in PostgreSQL rather than assuming every blob belongs wholesale in heavily-indexed JSONB.
- **Never delete archive rows.** If a market order disappears from the live feed, it still exists in the archive as a historical record. Deletions only happen in Redis (via TTL expiry). Schema migrations must preserve archive history or include an explicit export/backfill plan.
- **Public killmail lookups require both ID and hash.** This service can cache/archive a killmail only after a downstream caller or another source provides the hash; do not pretend ESI offers public killmail discovery.
