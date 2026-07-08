# eve-api-cache

Unauthenticated ESI proxy and permanent historical archive for the [EVE Online ESI API](https://esi.evetech.net).

## What it does

- **Proxy cache** — downstream apps call this instead of ESI directly, reducing rate-limit pressure and latency. Responses are served from Redis with `X-Cache: HIT/MISS/STALE` headers.
- **Historical archive** — every ESI response is written to PostgreSQL permanently. ESI data is ephemeral (market orders expire, system kill stats roll over, market history caps at ~13 months); this service preserves it forever.
- **Background collector** — APScheduler polls configured endpoints proactively on a schedule, so market data, sovereignty, industry facilities, system stats, and market history are available as cache HITs before anyone requests them.

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
  - *Time-series* (market orders, system jumps/kills, sovereignty): append-only metadata with compressed payload blobs
  - *Reference* (universe types, systems, corps): upsert on primary key
  - *Event* (killmails, contract items): insert-once, immutable
- Filesystem archive — market-order snapshots are also written as zstd-compressed Parquet under `ARCHIVE_DATA_DIR`, with PostgreSQL manifest and delta tables for long-term storage efficiency.

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
GET  http://localhost:8080/metrics               # Prometheus metrics
```

Response headers:
- `X-Cache: HIT` — served from Redis
- `X-Cache: MISS` — fetched live from ESI
- `X-Cache: STALE` / `X-Archive-Fallback: true` — ESI was down; served from archive (with a `Warning: 110` header)

The `datasource` query parameter is supported (`?datasource=singularity`) and is included in every cache/archive key.

## Background collector

The collector runs inside the same FastAPI process as background asyncio tasks (via APScheduler). It polls ESI on a configurable schedule and writes results through the same cache + archive pipeline as the proxy.

**Default schedule (configurable via env):**

| Endpoint | Interval |
|---|---|
| Market orders (per region) | 5 min |
| Market prices (global) | 1 hr |
| Market history (per region, type IDs from latest orders) | Daily |
| System jumps, kills, sovereignty, incursions, industry facilities | 1 hr |

Check which jobs are running and their next fire time:

```
GET /collector/status
```

## Operations

### Observability

`GET /metrics` exposes Prometheus text-format metrics: proxy requests and latency
(labeled by matched endpoint template, not raw path), upstream ESI responses / retries /
pagination failures, the ESI error-budget gauge, archive writes by type, and collector
job outcomes.

### ESI protection

The proxy is defensive about ESI's shared, per-source-IP error budget:

- An **error-budget circuit breaker** stops calling ESI when the remaining error budget
  is low or a `420/429` is seen, serving stale/archive data until the window resets
  (`ESI_ERROR_BUDGET_THRESHOLD`, `ESI_ERROR_BUDGET_BLOCK_SECONDS`).
- A global **outbound rate pacer** (`ESI_MAX_REQUESTS_PER_SECOND`) smooths bulk collector
  bursts so they trickle rather than spike. `0` disables it.
- `4xx` responses are briefly **negative-cached** (`NEGATIVE_CACHE_TTL_SECONDS`) so a
  client looping on a nonexistent ID doesn't burn one upstream error per request.

### Ops endpoint authentication

`/healthz` is always open (for liveness checks). `/collector/status` and `/metrics` are
open by default, which is fine on a trusted private network. Set `OPS_API_TOKEN` to
require callers to send a matching `X-Ops-Token` header — others get `401`. Update any
Prometheus scrape config to send the header when a token is set.

### Rate limiting behind a proxy

Per-client rate limiting (`CLIENT_RATE_LIMIT_PER_MINUTE`) keys on the client IP. When the
service runs behind something that forwards connections, the direct peer may be the
forwarder rather than the real client:

- A **reverse proxy that sets `X-Forwarded-For`** (nginx, caddy): list its address in
  `TRUSTED_PROXIES` so requests are attributed to the first forwarded hop.
- A **raw TCP forwarder** (e.g. `socat`) does not add `X-Forwarded-For`, so every client
  appears as one address and shares a single bucket. Use a real reverse proxy if you
  need per-client limits.

The limiter is per-process: with multiple workers the effective limit is multiplied by
the worker count. Run a single worker, or move the limiter to Redis, for a shared limit.

## Supported endpoints

Supported Phase 1 endpoints include markets, selected universe reference/stat endpoints, public character/corporation/alliance information, public contracts, public killmails, sovereignty, incursions, industry facilities, and server status.

Private/authenticated endpoints are not proxied and return 404.

## Tests

```bash
pytest
```

Tests cover the cache layer, ESI client, proxy logic, background collector, and PostgreSQL archive write path. PostgreSQL archive integration tests skip automatically when no migrated test database is available.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `DATABASE_URL` | `postgresql+asyncpg://localhost/eve_cache` | PostgreSQL connection URL |
| `DB_POOL_SIZE` | `20` | SQLAlchemy async connection pool size |
| `DB_MAX_OVERFLOW` | `10` | SQLAlchemy async pool overflow connections |
| `ESI_BASE_URL` | `https://esi.evetech.net` | ESI base URL |
| `USER_AGENT` | `eve-api-cache/0.1 (...)` | User-Agent sent to ESI |
| `ESI_TIMEOUT` | `30.0` | ESI request timeout (seconds) |
| `ESI_MAX_RETRIES` | `2` | Retries for transient upstream failures |
| `ESI_RETRY_BASE_DELAY` | `0.25` | Initial retry backoff in seconds |
| `ESI_RETRY_MAX_DELAY` | `10.0` | Max backoff for a single 5xx/transport retry (420/429 don't retry) |
| `ESI_ERROR_BUDGET_THRESHOLD` | `10` | Block upstream when ESI error-limit-remaining ≤ this |
| `ESI_ERROR_BUDGET_BLOCK_SECONDS` | `60` | Fallback block window when ESI supplies no reset hint |
| `ESI_MAX_REQUESTS_PER_SECOND` | `20.0` | Global outbound rate pacer; `0` disables |
| `PAGE_CONCURRENCY` | `10` | Max concurrent page fetches per paginated request |
| `UPSTREAM_CONCURRENCY` | `20` | Global max concurrent upstream ESI requests per process |
| `DEFAULT_DATASOURCE` | `tranquility` | Default ESI datasource |
| `MAX_POST_BODY_BYTES` | `65536` | Max accepted POST body size before forwarding to ESI |
| `MAX_POST_BATCH_ITEMS` | `1000` | Max items in public ESI batch lookup requests |
| `STALE_CACHE_SECONDS` | `3600` | How long Redis keeps stale bodies for degraded fallback |
| `STALE_CACHE_MAX_BODY_BYTES` | `5000000` | Payloads larger than this skip Redis stale copies and use archive fallback |
| `NEGATIVE_CACHE_TTL_SECONDS` | `60` | How long to cache a `4xx` (unless ESI `Cache-Control` says longer) |
| `CACHE_COMPRESS_MIN_BYTES` | `65536` | Compress Redis bodies at/above this size |
| `CLIENT_RATE_LIMIT_PER_MINUTE` | `300` | Per-client proxy request limit per process; `0` disables |
| `TRUSTED_PROXIES` | `[]` | Reverse-proxy IPs whose `X-Forwarded-For` is trusted (JSON list) |
| `OPS_API_TOKEN` | _(unset)_ | Shared token gating `/metrics` and `/collector/status`; unset = open |
| `COLLECTOR_ENABLED` | `true` | Start the in-process APScheduler collector |
| `COLLECTOR_WARM_ALIAS_VERSIONS` | `[]` | Extra version prefixes to also warm in Redis, e.g. `["latest"]` (JSON list) |
| `ARCHIVE_DATA_DIR` | `/var/lib/eve-api-cache/archive` | Filesystem root for Parquet archive files |
| `ENABLE_MARKET_ORDER_PARQUET` | `true` | Write market-order snapshots to Parquet manifests |
| `ENABLE_MARKET_ORDER_DELTAS` | `true` | Populate market-order version and snapshot-membership delta tables |
| `MARKET_REGION_IDS` | `[10000002,...]` | Region IDs to backfill market orders for |
| `POLL_MARKET_ORDERS_SECONDS` | `300` | Market order poll interval |
| `POLL_MARKET_PRICES_SECONDS` | `3600` | Market prices poll interval |
| `POLL_MARKET_HISTORY_SECONDS` | `86400` | Market history poll interval |
| `POLL_UNIVERSE_SECONDS` | `3600` | Universe stats (jumps, kills, sov, incursions) poll interval |
