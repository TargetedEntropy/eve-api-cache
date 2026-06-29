# eve-api-cache

Unauthenticated ESI proxy and permanent historical archive for the [EVE Online ESI API](https://esi.evetech.net).

## What it does

- **Proxy cache** — downstream apps call this instead of ESI directly, reducing rate-limit pressure and latency. Responses are served from Redis with `X-Cache: HIT/MISS/STALE` headers.
- **Historical archive** — every ESI response is written to PostgreSQL permanently. ESI data is ephemeral (market orders expire, system kill stats roll over, market history caps at ~13 months); this service preserves it forever.

Phase 1 covers public (no-auth) endpoints only: markets, universe, contracts, sovereignty, incursions, killmails, and public character/corp/alliance info.

## Architecture

```
caller → FastAPI proxy → Redis (hot cache, TTL-based)
                       → ESI upstream (httpx, ETag/304, paginated fan-out)
                       → PostgreSQL (permanent archive, never deleted)
```

**Storage tiers:**
- Redis — TTL matches ESI `Cache-Control`. Evicts naturally.
- PostgreSQL — three write strategies by endpoint type:
  - *Time-series* (market orders, system jumps/kills, sovereignty): append-only with `fetched_at`
  - *Reference* (universe types, systems, corps): upsert on primary key
  - *Event* (killmails, contract items): insert-once, immutable

**Stampede protection** — identical in-flight upstream requests are coalesced; only one ESI call is made regardless of how many callers hit the same uncached key simultaneously.

**Pagination** — endpoints returning `X-Pages: N` fan out pages 2..N concurrently under a semaphore, merge into a single response, and cache/archive the merged result.

## Setup

```bash
# 1. Install
pip install -e ".[dev]"

# 2. Configure
cp .env.example .env
# edit .env with your Redis and PostgreSQL URLs

# 3. Create database tables
alembic upgrade head

# 4. Run
uvicorn app.main:app --reload --port 8080
```

**Requirements:** Python 3.12+, Redis, PostgreSQL

## Usage

The proxy mirrors the ESI path structure exactly:

```
GET  http://localhost:8080/v1/markets/10000002/orders/?order_type=all
GET  http://localhost:8080/v1/universe/types/34/
GET  http://localhost:8080/latest/sovereignty/map/
POST http://localhost:8080/v1/universe/names/    # body: [12345, 67890]
GET  http://localhost:8080/healthz
```

Response headers:
- `X-Cache: HIT` — served from Redis
- `X-Cache: MISS` — fetched live from ESI
- `X-Cache: STALE` / `X-Archive-Fallback: true` — ESI was down; served from archive

The `datasource` query parameter is supported (`?datasource=singularity`) and is included in every cache/archive key.

## Supported endpoints

All public ESI endpoints across: markets, universe (types, systems, regions, constellations, stations, planets, stars, factions, groups, jumps, kills, names/ids), characters (public info, portraits, corp history, affiliation), corporations, alliances, public contracts, killmails, sovereignty, incursions, industry facilities, and server status.

Private/authenticated endpoints are not proxied and return 404.

## Tests

```bash
pytest
```

31 tests covering the cache layer (Redis key layout, TTL behaviour), ESI client (pagination merge, ETag/304, error headers), and proxy logic (cache hit/miss, archive fallback, path validation, allowlist enforcement).

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `DATABASE_URL` | `postgresql+asyncpg://localhost/eve_cache` | PostgreSQL connection URL |
| `ESI_BASE_URL` | `https://esi.evetech.net` | ESI base URL |
| `USER_AGENT` | `eve-api-cache/0.1 (...)` | User-Agent sent to ESI |
| `ESI_TIMEOUT` | `30.0` | ESI request timeout (seconds) |
| `PAGE_CONCURRENCY` | `10` | Max concurrent page fetches per paginated request |
| `DEFAULT_DATASOURCE` | `tranquility` | Default ESI datasource |
