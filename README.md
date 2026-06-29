# eve-api-cache

Unauthenticated ESI proxy and permanent historical archive for the [EVE Online ESI API](https://esi.evetech.net).

## What it does

- **Proxy cache** — downstream apps call this instead of ESI directly, reducing rate-limit pressure and latency. Responses are served from Redis with `X-Cache: HIT/MISS/STALE` headers.
- **Historical archive** — every ESI response is written to PostgreSQL permanently. ESI data is ephemeral (market orders expire, system kill stats roll over, market history caps at ~13 months); this service preserves it forever.
- **Background collector** — APScheduler polls configured endpoints proactively on a schedule, so market data, sovereignty maps, system stats, and market history are available as cache HITs before anyone requests them.

Phase 1 covers public (no-auth) endpoints only: markets, universe, contracts, sovereignty, incursions, killmails, and public character/corp/alliance info.

## Data risk notes

This service archives only public ESI data, but public does not mean harmless once it is aggregated, indexed, and retained forever. Treat the PostgreSQL archive and Redis cache as sensitive operational data.

Primary risks and mitigations:
- **Character/corporation profiling:** public character, corporation history, affiliation, contract, and killmail data can reveal player activity patterns when aggregated. Do not add private/authenticated ESI scopes without a separate privacy and access-control design.
- **Permanent retention:** time-series history is intentionally long-lived. Any archive pruning, correction, legal request, or operator-requested removal needs a deliberate admin process; do not casually delete rows from normal application code.
- **Archive exposure:** never expose raw database access or broad archive-dump endpoints to public callers. Any future analytics API should be scoped, rate-limited, and reviewed separately from the ESI proxy surface.
- **Backups and exports:** database backups inherit the same sensitivity as production. Encrypt backups, restrict who can download them, and document restore/export handling before production use.
- **Logs:** do not log request bodies for POST batch endpoints, full upstream payloads, Redis values, database URLs, or caller-supplied IDs at high cardinality. Keep logs useful for operations without becoming a second archive.
- **Datasource integrity:** only known ESI datasources are accepted. Unknown datasource strings are rejected so callers cannot create arbitrary cache/archive namespaces.

## Architecture

```
caller → FastAPI proxy → Redis (hot cache, TTL-based)
                       → ESI upstream (httpx, ETag/304, paginated fan-out)
                       → PostgreSQL (permanent archive, never deleted)

APScheduler (background) → ESI upstream
                         → Redis (pre-populate before any caller arrives)
                         → PostgreSQL (same archive layer as proxy)
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
GET  http://localhost:8080/collector/status      # list scheduled jobs + next run times
```

Response headers:
- `X-Cache: HIT` — served from Redis
- `X-Cache: MISS` — fetched live from ESI
- `X-Cache: STALE` / `X-Archive-Fallback: true` — ESI was down; served from archive

The `datasource` query parameter is supported (`?datasource=singularity`) and is included in every cache/archive key.

## Background collector

The collector runs inside the same FastAPI process as background asyncio tasks (via APScheduler). It polls ESI on a configurable schedule and writes results through the same cache + archive pipeline as the proxy.

**Default schedule (configurable via env):**

| Endpoint | Interval |
|---|---|
| Market orders (per region) | 5 min |
| Market prices (global) | 1 hr |
| Market history (per region, type IDs from latest orders) | Daily |
| System jumps, kills, sovereignty, incursions | 1 hr |

Check which jobs are running and their next fire time:

```
GET /collector/status
```

## Supported endpoints

All public ESI endpoints across: markets, universe (types, systems, regions, constellations, stations, planets, stars, factions, groups, jumps, kills, names/ids), characters (public info, portraits, corp history, affiliation), corporations, alliances, public contracts, killmails, sovereignty, incursions, industry facilities, and server status.

Private/authenticated endpoints are not proxied and return 404.

## Tests

```bash
pytest
```

45 tests covering the cache layer (Redis key layout, TTL behaviour), ESI client (pagination merge, ETag/304, error headers), proxy logic (cache hit/miss, archive fallback, path validation, allowlist enforcement), and the background collector (per-endpoint fetch, datasource param handling, cache key consistency, type ID discovery).

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
| `MAX_POST_BODY_BYTES` | `65536` | Max accepted POST body size before forwarding to ESI |
| `MAX_POST_BATCH_ITEMS` | `1000` | Max items in public ESI batch lookup requests |
| `MARKET_REGION_IDS` | `[10000002,...]` | Region IDs to backfill market orders for |
| `POLL_MARKET_ORDERS_SECONDS` | `300` | Market order poll interval |
| `POLL_MARKET_PRICES_SECONDS` | `3600` | Market prices poll interval |
| `POLL_MARKET_HISTORY_SECONDS` | `86400` | Market history poll interval |
| `POLL_UNIVERSE_SECONDS` | `3600` | Universe stats (jumps, kills, sov, incursions) poll interval |
