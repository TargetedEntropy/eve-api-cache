# fixes.md — Audit findings for eve-api-cache

Scan date: 2026-07-07. Baseline: master @ 775f7c8, all 59 unit tests passing (3 postgres-marked skipped).

Each item lists severity, affected files/lines, the problem, and the intended fix. Items are grouped
by category and ordered most-severe-first within each group. Line numbers refer to the current files.

Constraints for whoever applies these (per CLAUDE.md):
- Never delete or rewrite archive history; migrations must be additive/preserving.
- Every fix should come with tests (cache hit/miss, archive write, stale fallback, input validation).
- Keep changes small and verifiable; extend existing modules in place.

---

## Progress (updated 2026-07-07)

Fix-order steps 1–6 applied, plus a first batch of step-7 items. Test suite grew from **59 passed /
3 skipped** to **127 passed / 3 skipped** (the 3 skips are Postgres-marked, skipped without a DB).
No schema migrations were needed. Each item's **Status:** line below has details.

**Done ✅**
- **1.1** ESI error-budget circuit breaker · **1.2** stop retrying 420/429 · **1.3** negative caching of 4xx · **1.6** coalesced 304 refetch
- **2.1** multi-page ETag dropped · **2.2** redirects not followed · **2.5** manifest insert race · **2.6** storage labeling + pyarrow fallback
- **3.1** real per-job staggering · **3.2** first-run-after-boot
- **4.1** streaming POST body cap · **4.2** per-endpoint query-param allowlist
- **5.1** in-process metrics + `GET /metrics`
- _step 7:_ **2.3** contract bids → time-series · **5.4** batched name upserts + Redis pipeline · **5.5** `Warning` header on stale

**Partial ⚠️**
- **1.4** default client rate limit lowered 600→300 + documented (dedicated per-client *error* throttle deferred)
- **1.5** outbound token-bucket pacer + breaker-abort added; per-region history staggering delivered via 3.1

**Remaining (step 7, any order):** 2.4, 2.7, 2.8, 2.9, 3.3, 3.4, 3.5, 4.3, 4.4, 4.5, 5.2, 5.3, 5.6, 5.7.

---

## 1. Rate-limit risks to ESI (external API)

### 1.1 [HIGH] No global circuit breaker — error-budget headers parsed but never used

**Status: ✅ DONE** — `ESIClient` now tracks the budget from every response and blocks upstream (synthetic 503 → existing stale/archive fallback) when remain ≤ `esi_error_budget_threshold` or on 420/429; added `is_budget_blocked()` / `budget_status()`. Ops-endpoint exposure still pending (see 5.1).

`app/esi_client.py:122-123` parses `X-ESI-Error-Limit-Remain` / `X-ESI-Error-Limit-Reset` into
`ESIResponse`, but nothing in the codebase reads them. CLAUDE.md requires: "Track error-limit
headers… prefer stale cached/archive responses over hammering ESI during trouble."

**Fix:** Add shared error-budget state on `ESIClient` (e.g. `self._error_limit_remain`,
`self._budget_blocked_until`), updated from every response. When remain drops below a configurable
threshold (e.g. 10) or a 420 is received, refuse/queue new upstream requests until
`X-ESI-Error-Limit-Reset` seconds elapse and return a synthetic 503 so the proxy serves
stale/archive fallback (that path already exists in `app/proxy.py:153-161`). The collector should
also skip runs while blocked. Expose current budget in an ops endpoint (see 6.1).

### 1.2 [HIGH] 420/429 are retried with a 5s-capped sleep, ignoring long Retry-After

**Status: ✅ DONE** — 420/429 return immediately and trip the breaker (no in-request retry); retries limited to `{502,503,504}` + transport; the 5s cap is now the configurable `esi_retry_max_delay` (10s). Note: 500 is no longer retried either.

`app/esi_client.py:21` includes 420/429 in `_TRANSIENT_STATUSES`; `_sleep_before_retry`
(`app/esi_client.py:246-249`) caps all delays at 5s and then retries anyway. ESI's 420 means "stop
until the error window resets" (up to 60s). Retrying a 420 after ≤5s burns more error budget and
prolongs the throttle — the retry loop actively makes 420s worse.

**Fix:** On 420 (and 429), do NOT retry within the request. Return immediately and set the global
block from 1.1 using `X-ESI-Error-Limit-Reset` / `Retry-After`. Keep bounded retries only for
502/503/504 and transport errors. Remove or raise the 5s cap only for those transient cases.

### 1.3 [HIGH] No negative caching — 4xx responses are re-fetched from ESI on every request

**Status: ✅ DONE** — 4xx (except 429) cached in Redis `esi:neg:*` (respecting ESI `Cache-Control`, else `negative_cache_ttl_seconds`), served as `X-Cache: HIT`; positive writes clear negatives; error payloads never archived.

`app/proxy.py:163-164`: 4xx is passed through without caching. Any downstream client looping on a
nonexistent ID (e.g. `/v1/universe/types/999999999/` → 404, or malformed params → 400) drives one
upstream error per request. Combined with 1.4, a single well-behaved-looking client can exhaust the
shared ESI error budget (~100 errors/min per source IP) and get the proxy's IP throttled.

**Fix:** Cache 4xx responses in Redis for a short TTL (respect the response's `Cache-Control` if
present, else e.g. 60–300s) under the same cache key with the status stored alongside the body
(e.g. a small JSON envelope or a parallel `esi:status:` key). Serve cached errors with
`X-Cache: HIT`. Do not archive error payloads.

### 1.4 [MEDIUM] Per-client rate limit (600/min) far exceeds ESI's error budget (~100 errors/min)

**Status: ⚠️ PARTIAL** — default lowered 600→300 and documented. The separate per-client *upstream-error* throttle was not added: the 1.1 breaker + 1.3 negative cache cover the actual risk. Revisit if a dedicated per-client error budget is still wanted.

`app/config.py:21` (`client_rate_limit_per_minute: int = 600`). One client at full allowance
sending error-producing requests exceeds the shared ESI error window sixfold.

**Fix:** After 1.1–1.3 land this is mostly mitigated, but also: (a) lower the default or make the
error-path budget separate — track per-client upstream *error* counts and throttle clients that
generate upstream 4xx/5xx at a much lower threshold (e.g. 30/min); (b) document the interplay in
CLAUDE.md/README.

### 1.5 [MEDIUM] Daily market-history collection is an unthrottled burst

**Status: ⚠️ PARTIAL** — (b) global token-bucket pacer added (`esi_max_requests_per_second`); (c) history collection aborts when the breaker is tripped. (a) per-region staggering is deferred to 3.1 (same scheduler bug).

`app/collector.py:110-142`: The Forge alone has ~15k actively traded types; the job fires per-region
with `concurrency=10`, and all 5 region jobs are scheduled on the same interval with no stagger
(`app/scheduler.py:54-69`), so up to 50 concurrent history fetches (clamped only by the global
`upstream_concurrency=20` semaphore) hammer ESI once per day, ~75k requests in a tight burst.

**Fix:** (a) Stagger the per-region history jobs (offset start times, see 3.1); (b) add a global
requests-per-second pacer (simple token bucket in `ESIClient`) used by collector traffic, so bulk
jobs trickle instead of bursting; (c) make history collection abort early when the error-budget
breaker (1.1) is tripped.

### 1.6 [LOW] 304-with-evicted-body refetch bypasses request coalescing

**Status: ✅ DONE** — the no-ETag refetch now runs through `coalesce(f"{cache_key}:refetch", …)`.

`app/proxy.py:121-124`: when ESI returns 304 but the stale body is gone, each coalesced *waiter*
independently re-fetches without an ETag (the coalesced result is shared, but this follow-up fetch
is per-caller) — a stampede exactly when the cache is cold.

**Fix:** Run the no-ETag refetch through `coalesce()` with a derived key (e.g. `f"{cache_key}:refetch"`),
or restructure so the 304 recovery happens inside the coalesced `do_fetch`.

---

## 2. Data consistency issues

### 2.1 [HIGH] Page-1 ETag is stored for merged multi-page responses

**Status: ✅ DONE** — `ESIClient.fetch` now returns `etag=None` when `x_pages > 1`, so the proxy stores no whole-set validator and never sends `If-None-Match` for a merged set (verified end-to-end: no `esi:etag:` key after a multi-page fetch). Per-page ETags are retained in `page_metadata` for future per-page validation. Single-page ETag revalidation is unchanged.

`app/esi_client.py:126,180-191`: for paginated endpoints the merged body is returned with
`resp_etag` from page 1 only. The proxy stores that ETag (`app/proxy.py:129-135`) and later sends
`If-None-Match`. If page 1 happens to be unchanged but pages 2..N changed, ESI answers 304 and the
proxy re-validates and re-serves a **stale merged snapshot as fresh** (`app/proxy.py:108-119`),
including extending its TTL and (correctly, but here wrongly-premised) skipping the archive write —
so the archive silently misses changed snapshots too.

**Fix:** In `ESIClient.fetch`, when `x_pages > 1`, return `etag=None` on the merged response so no
conditional validation is attempted for multi-page results. (Optionally: keep per-page ETags in
`page_metadata` for future per-page validation, but do not fake a whole-set validator.)

### 2.2 [HIGH] `follow_redirects=True` lets the proxy fetch and cache redirect targets
**Status: ✅ DONE** — `follow_redirects=False`; 3xx is passed through with its `Location` header (new `ESIResponse.location` → `ProxyResult.location` → `Location` response header) and never archived. Verified end-to-end: a real 302 returns 302+Location, the CDN target is never fetched, nothing archived. Redirect-*caching* of portrait targets (CLAUDE.md gotcha) deferred — needs the header-carrying cache work (overlaps 5.6); pass-through is ESI-faithful in the meantime.

`app/esi_client.py:73-78`. Two problems: (a) `/characters/{id}/portrait/`-style endpoints that 30x
to the image CDN would cause the proxy to download the image bytes and cache/serve them with
`content_type="application/json"`, and archive binary junk as a "reference payload" (the
`json.loads(payload)` in `app/archive.py:113` would raise, get caught in
`app/proxy.py:212-214`, and the archive write silently fails); (b) the proxy blindly follows any
redirect target upstream chooses — it should only ever talk to the configured ESI host (Proxy
Safety Requirements).

**Fix:** Set `follow_redirects=False`. Handle 3xx explicitly in `fetch()`: return the status and
`Location` header to the caller (pass-through), and for portrait-type endpoints cache the redirect
target URL per the CLAUDE.md gotcha. Add a test that a 302 response is passed through, not followed.

### 2.3 [MEDIUM] Contract bids archived as insert-once EVENT although bids are mutable
**Status: ✅ DONE** — bids changed to `ArchiveType.TIME_SERIES` (append each observation); items stay `EVENT`. No migration — the archive tables are shared and keyed by path, so existing event rows remain as history.

`app/allowlist.py:75`: `/contracts/public/bids/{contract_id}/` is `ArchiveType.EVENT`, and
`archive_events` is keyed on `(datasource, path)` with `on_conflict_do_nothing`
(`app/archive.py:138-151`). The first observed bid list is frozen forever; all later bids on an
auction are never archived. Bids grow until the contract closes — this is time-varying data.

**Fix:** Change bids to `ArchiveType.TIME_SERIES`. Contract items (`/contracts/public/items/`) are
genuinely immutable and can stay EVENT. No migration needed (tables are shared, keyed by path);
existing event rows remain as history.

### 2.4 [MEDIUM] 304 revalidation TTL defaults to 300s instead of preserving the existing TTL
`app/proxy.py:111`: `ttl = esi_resp.max_age or 300`. CLAUDE.md: "on 304 … extend the hot-cache TTL
using the new Cache-Control header if present, **otherwise preserve the existing TTL**". Since the
body key already expired, "existing TTL" should come from the stale key's remaining ESI-derived
window, or at minimum the previously stored TTL — a flat 300 can extend beyond ESI's intent or
short-change long-lived resources. Also `CacheClient.refresh_ttl` (`app/cache.py:56-61`) is dead
code that was presumably meant for this path.

**Fix:** Persist the last known TTL alongside the ETag (e.g. `esi:ttl:{key}` with the same expiry
as the ETag key), and on 304 without a new `max-age` reuse it. Remove `refresh_ttl` or use it.
Cover with a test: 304 with no Cache-Control preserves prior TTL.

### 2.5 [MEDIUM] `ArchiveObjectFile` insert races → IntegrityError → whole archive write lost
**Status: ✅ DONE** — the `ArchiveObjectFile` insert now uses `on_conflict_do_nothing(index_elements=["content_hash"]).returning(id)` with a SELECT fallback when the returning yields no row, so a concurrent writer's `content_hash` is reused instead of raising a `UniqueViolation` that would roll back the whole snapshot. `_write_market_orders_parquet` returns `Optional[int]`; the caller skips the relabel UPDATE when it's None. (The temp-name/rename-after-commit orphan-avoidance was not added — the file path embeds `content_hash` and is deterministic, so racing writes are identical bytes; noted as a minor self-healing case.)

`app/archive.py:339-367`: `_get_object_file_id_by_content_hash` then plain `pg_insert(...)
.returning(id)` with **no** `on_conflict` — a check-then-insert race. The proxy and the collector
(or two coalesced-but-separately-archiving requests, see 2.7) can archive the same snapshot
concurrently; the loser raises `UniqueViolation` on `content_hash`, the exception bubbles to
`_archive_response`'s blanket rollback (`app/proxy.py:212-214`), and the *entire* snapshot
(timeseries row + deltas) for that request is dropped. It also leaves an orphaned parquet file on
disk (file written at `app/archive.py:343-349` before commit).

**Fix:** Use `on_conflict_do_nothing(index_elements=["content_hash"]).returning(id)` and fall back
to the SELECT when returning yields no row (same pattern as the timeseries idempotency key at
`app/archive.py:84-90`). Write the parquet file only after determining an insert is needed; consider
writing to a temp name and renaming after commit, or tolerating/reusing an existing file whose name
already embeds `content_hash`.

### 2.6 [MEDIUM] Timeseries row can be permanently mislabeled `market_parquet_delta` with no manifest
**Status: ✅ DONE** — the snapshot row is now inserted as `compressed_json` and only relabeled to `market_parquet_delta` in the UPDATE after the manifest exists. When pyarrow is unavailable (`_pyarrow_available()`), the Parquet path is skipped with a one-time warning and the row stays `compressed_json` — the payload is always preserved in the blob table, so `get_latest_payload` still recovers it. Verified in this environment: pyarrow is absent, `_pyarrow_available()` returns False and warns once, and the write no longer fails.

`app/archive.py:66-110`: the snapshot row is inserted with
`payload_storage="market_parquet_delta"` **before** the parquet write; if
`_write_market_orders_parquet` then raises, the transaction rolls back — fine — but on a *retry in
the same minute bucket*, the idempotency conflict path (`snapshot_id` recovered at line 90) only
runs the parquet write if the first insert committed. Conversely, if pyarrow is missing
(`RuntimeError` at `app/archive.py:400-401`) every snapshot write fails outright even though the
compressed blob storage path works.

**Fix:** Insert the row with `payload_storage="compressed_json"` and only flip it to
`market_parquet_delta` in the UPDATE at `app/archive.py:103-110` after the manifest exists (the
UPDATE already sets it — just change the initial INSERT value). If pyarrow is unavailable, log once
and fall back to compressed-JSON storage instead of failing the archive write.

### 2.7 [LOW] Every coalesced waiter re-writes Redis and re-archives the same response
`app/proxy.py:101-150`: `coalesce()` deduplicates the upstream fetch, but each waiter then executes
`cache.set` + `_archive_response` with the shared `ESIResponse`. Writes are idempotent (idempotency
key / upserts) so this is waste, not corruption — N waiters mean N blob compressions, N parquet
dedupe lookups, N delta upsert storms for a Forge-sized payload.

**Fix:** Move the cache/archive writes inside the coalesced leader path (e.g. have `do_fetch`
perform store+archive and return the final body), or add a flag on the coalesce result marking the
leader.

### 2.8 [LOW] `archive_events` ignores query params — distinct queries collapse to one row
`archive_events` PK is `(datasource, path)` (`app/db.py:126-127`) and the insert doesn't include
`query_hash` (`app/archive.py:140-151`). Killmails/items/bids have no meaningful query params
today, so impact is latent, but `get_latest_payload` also ignores `query_hash` for events
(`app/archive.py:245-249`), so a future params-bearing EVENT endpoint would silently serve the
wrong payload as fallback.

**Fix:** Add `query_hash` to the event insert + a migration adding the column (default '' backfill,
widen PK or add unique constraint `(datasource, path, query_hash)` — additive, preserves rows), and
filter on it in `get_latest_payload`.

### 2.9 [LOW] Collector archives under `/v1/...` while downstream callers commonly use `/latest/...`
`app/collector.py:81,94,105` hardcode `/v1/` paths. Cache keys and archive identity include the
version prefix, so a downstream app calling `/latest/markets/10000002/orders/` never hits the
collector-warmed cache and creates a parallel archive series. This follows the "proxy the exact
version requested" rule, but the fragmentation is a real operational surprise.

**Fix (decision, then small change):** Either document loudly that downstream apps must call `/v1/`
paths for warmed endpoints, or add a config-driven alias map so the collector also warms the
`/latest/` key for the same fetched body (single ESI request, two cache keys). Do not merge archive
identities retroactively.

---

## 3. Scheduling gaps

### 3.1 [HIGH] "Jitter" does not stagger jobs — comment and behavior disagree; all regions fire together
**Status: ✅ DONE** — each job gets an explicit per-job `start_date` phase offset (orders spread ~interval/N apart across the poll window; history/universe staggered by fixed steps) plus a real ±10s `jitter`. Verified: order jobs first-run at 15/75/135/195/255s and universe at 30…330s — no longer all firing at once.

`app/scheduler.py:30-41`: the code computes `jitter = window/n * i` intending an even phase offset,
but APScheduler's `jitter` is a *random ±N seconds applied to every run*, not a start offset. Region
i=0 gets `jitter=max(1,0)=1`. Net effect: all 5 market-order jobs fire within seconds of each other
every 300s, and the daily history jobs (`app/scheduler.py:54-69`) have no stagger at all (see 1.5).

**Fix:** Use `next_run_time=datetime.now(UTC) + timedelta(seconds=offset_i)` (APScheduler 3.x) or
`start_date` to phase-shift each region's IntervalTrigger, keeping a small random `jitter` (e.g.
10s) on top. Apply the same to history jobs (offset by hours across the day) and the six universe
jobs (currently all fire together every 3600s, `app/scheduler.py:80-88`).

### 3.2 [HIGH] First run of every job is one full interval after startup — daily jobs may never run
**Status: ✅ DONE** — every `IntervalTrigger` now has a near-future `start_date`, so the first run happens shortly after boot (history ~120s, not 24h; orders ~15s; universe within ~5.5min). A regression test asserts every job's first run is `< its interval`. Deferred: persisting "last successful run" to skip a just-completed daily run — archive writes are idempotent, so a re-run after restart is harmless.

`IntervalTrigger` without `start_date` fires first at now+interval. Market history
(`poll_market_history_seconds=86400`) therefore runs 24h after process start; a service that is
restarted (deploys, crashes) more often than daily will **never** collect market history. Same
pattern delays first orders snapshot by up to 5 min and universe snapshots by 1h after every restart.

**Fix:** Set an explicit near-future `next_run_time`/`start_date` per job (respecting the offsets
from 3.1), so the first collection happens shortly after startup. For history, consider persisting
"last successful run" (query `archive_timeseries` max(fetched_at) for the path) and scheduling the
first run relative to that instead of process start.

### 3.3 [MEDIUM] Advisory-lock session pins a DB connection for the whole job duration
`app/scheduler.py:98-116`: the session holding `pg_try_advisory_lock` stays checked out while
`fn(*args)` runs (hours, for history jobs), and the inner collector work opens additional sessions
(`app/collector.py:59`) — up to 10 concurrent under the history semaphore. SQLAlchemy's default pool
(5 + 10 overflow) can be exhausted when several jobs overlap, stalling proxy archive writes too.

**Fix:** Either (a) size the pool explicitly in `create_async_engine` (`app/db.py:183`) with
`pool_size`/`max_overflow` derived from job concurrency, or (b) use a dedicated raw connection
(`async_engine.connect()`) for the advisory lock instead of a pooled ORM session, and cap
per-job inner concurrency. Also note `pg_advisory_unlock` + `commit()` in `finally` can raise if
the connection died mid-job — wrap the unlock in try/except and rely on connection close releasing
the lock.

### 3.4 [LOW] `misfire_grace_time=60` + slow Forge fetch can silently skip order snapshots
A Forge order snapshot (300+ pages with retries) can exceed 300s; with `max_instances=1` (default)
and `misfire_grace_time=60` (`app/scheduler.py:39`), the next run is skipped and the gap in the
archive is invisible.

**Fix:** Log skipped/misfired runs via an APScheduler listener (`EVENT_JOB_MISSED`,
`EVENT_JOB_ERROR`) and export a counter (see 6.1) so archive gaps are observable. Consider
`misfire_grace_time=None` (run late rather than skip) for time-series jobs — a late snapshot is
better than a missing one for a permanent archive.

### 3.5 [LOW] History discovery misses types with no currently active orders
`app/collector.py:145-163` discovers type IDs from the latest orders snapshot only. Types that are
traded rarely (no open orders at snapshot time) never get history archived, defeating the "extend
the 13-month window" goal for exactly the thin markets where it matters most.

**Fix:** Accumulate a persistent per-region type-ID set (e.g. distinct type_ids across archived
snapshots, or a small `market_history_targets` table union'd with each discovery pass) and iterate
that instead of the single latest snapshot.

---

## 4. Security issues

### 4.1 [HIGH] POST body size limit is enforced only after buffering the whole body in memory

**Status: ✅ DONE** — `_read_body_capped` reads `request.stream()` incrementally and aborts with 413 the moment the cap is crossed; the Content-Length fast-path rejection is preserved. Tested at unit + ASGI level (chunked upload with no Content-Length is still capped).

`app/routes.py:63-89`: the `Content-Length` header check is skippable (chunked transfer sends no
Content-Length), and `await request.body()` then buffers the entire stream before the length check
at line 83. A client can stream an arbitrarily large chunked body and exhaust memory before the 413
is issued.

**Fix:** Read the body incrementally via `request.stream()` accumulating up to
`max_post_body_bytes + 1`, aborting with 413 as soon as the cap is crossed. Keep the early
Content-Length fast-path rejection.

### 4.2 [HIGH] Arbitrary query params create unbounded cache keys and permanent archive rows

**Status: ✅ DONE** — `EndpointSpec` gained `allowed_params`/`required_params`; `check_params` rejects unknown params and enforces required ones (history requires `type_id`) with 400 before any cache/ESI/archive. `order_type`/`type_id` allowed on orders, `type_id` on history, `language` on localizable universe endpoints; `page`/`datasource` always allowed.

`app/proxy.py:83-85` / `app/allowlist.py:123-125`: only `page` and `datasource` are stripped; every
other caller-supplied param (junk included: `?x=1`, `?x=2`, …) is hashed into the cache key,
forwarded to ESI, and — worse — becomes a distinct `query_hash` in `archive_reference` /
`archive_timeseries`. Since the archive is append-only/never-deleted by policy, a hostile or buggy
client can permanently bloat the archive and Redis with unlimited key cardinality, and each junk
variant is also an upstream ESI request (ties into 1.3/1.4).

**Fix:** Add a per-endpoint allowed-params set to `EndpointSpec` (e.g. orders: `{order_type,
type_id, language}`, history: `{type_id}`, most others: `{}` or `{language}`) and reject requests
carrying unknown params with 400 *before* any cache/ESI/archive activity. Also enforce
endpoint-required params (history requires `type_id`; ESI 400s otherwise) so invalid requests never
reach ESI — CLAUDE.md requires input validation before ESI is contacted.

### 4.3 [MEDIUM] Rate limiter keys on the direct peer IP — broken by the documented socat deployment
`app/routes.py:45` uses `request.client.host`. deploy.md fronts the loopback listener with socat
(`10.0.0.33:8080 -> 127.0.0.1:8080`), so **every** LAN/VPN client arrives as the same source IP:
one noisy client exhausts the single shared bucket for everyone, and per-client attribution is
impossible. Additionally the limiter is per-process (multiple uvicorn workers multiply the limit)
and `_hits` never evicts idle keys (`app/rate_limit.py:11`), an unbounded-memory vector if the
service is ever exposed directly.

**Fix:** (a) Support a trusted-proxy mode: honor `X-Forwarded-For` (first untrusted hop) only when
the peer is in a configured trusted list — note socat does not add XFF, so also document replacing
socat with a real reverse proxy (nginx/caddy) or systemd socket forwarding that preserves source
IPs; (b) periodically prune empty deques (e.g. during `allow()` when a key's deque empties); (c)
document the single-process assumption or move the limiter to Redis (`INCR`+`EXPIRE`) so it is
shared across workers.

### 4.4 [LOW] Upstream error bodies are passed through verbatim with a JSON content type
`app/proxy.py:161,164` and `ProxyResult.content_type` default: non-JSON upstream error payloads
(HTML from an intermediary, etc.) are relayed byte-for-byte labeled `application/json`. Low risk
(ESI is the only upstream), but it can leak upstream infrastructure details and confuse clients.

**Fix:** For upstream ≥400 responses, replace the body with a minimal JSON envelope
(`{"error":"upstream_error","status":N}`) or propagate the real upstream Content-Type; log a
truncated body server-side only.

### 4.5 [LOW] `/collector/status` and `/healthz` are unauthenticated operational surfaces
`app/routes.py:23-32`. On the documented LAN/VPN deployment this is acceptable, but the collector
status enumerates job IDs/schedules. When adding the metrics endpoint (6.1), gate these behind a
shared token header or bind them to a separate localhost-only port; at minimum document the
exposure decision in deploy.md.

---

## 5. Implementation gaps (vs CLAUDE.md requirements)

### 5.1 [HIGH] No observability at all — required "from day one"
**Status: ✅ DONE** — added `app/metrics.py` (dependency-free in-process registry with Prometheus text exposition) and a `GET /metrics` endpoint. Instrumented: `proxy_requests_total{endpoint,cache_status}` + `proxy_request_duration_seconds` histogram (labeled by bounded endpoint template via `endpoint_label`, never raw path); `esi_requests_total{status}` (incl. `304`/`blocked`/`error`), `esi_retries_total`, `esi_pagination_failures_total`, `esi_error_limit_remain` + `esi_budget_blocked` gauges; `archive_writes_total{archive_type,result}`; `collector_job_runs_total{job,result}` via APScheduler listeners; `client_rate_limited_total`. Verified end-to-end by rendering valid exposition output. (Endpoint auth/gating is 4.5.)

CLAUDE.md mandates counters for upstream requests, cache hits/misses/stale serves, archive writes,
page fanout failures, 304 revalidations, current ESI error budget, and per-endpoint latency. None
exist; the only signals are log lines.

**Fix:** Add a lightweight in-process metrics registry (or `prometheus-client`) with a `/metrics`
endpoint. Instrument: `proxy_request` (per cache_status counter + latency histogram per matched
endpoint pattern — not raw path, to bound cardinality), `ESIClient` (upstream request/retry/status
counters, error-budget gauge from 1.1), `archive.write_snapshot` (writes/failures by archive_type),
pagination fanout failures (`app/esi_client.py:166-178`), and scheduler job success/failure/missed
(3.4). This also unblocks verifying several fixes above.

### 5.2 [MEDIUM] Large payloads stored uncompressed (and duplicated) in Redis
CLAUDE.md gotcha: `/markets/prices/` (~400KB) and merged Forge orders (potentially tens of MB)
should be stored compressed in Redis. `app/cache.py:44-54` stores the raw body **twice** (body +
stale copy). A Forge snapshot every 5 min keeps 2 full uncompressed copies resident.

**Fix:** zstd-compress bodies above a size threshold (e.g. 64KB) in `CacheClient.set`, with a small
header/flag byte or a `esi:enc:` marker so `get`/`get_stale` transparently decompress. Store the
stale copy as a reference to the same compressed value where possible (or accept single compressed
duplication). Add tests for round-trip and for the >threshold path.

### 5.3 [MEDIUM] `/characters/affiliation/` results are not extracted per-entity
`app/allowlist.py:63` sets no `extract_names` (correct — affiliations have no names), but CLAUDE.md
says POST batch endpoints should "archive extracted normalized entities separately". Affiliation
rows (character→corp/alliance at time T) are exactly the aggregation-sensitive history the project
wants; today they only live inside opaque per-batch REFERENCE rows keyed by body hash, unqueryable
and overwritten per batch shape. Also `write_names`'s docstring (`app/archive.py:156-168`) claims
affiliation support it doesn't have.

**Fix:** Either add a small `character_affiliations` time-series table (character_id, corp_id,
alliance_id, faction_id, datasource, first/last_seen) populated from affiliation responses, or —
minimum — fix the docstring and change affiliation's archive handling to TIME_SERIES so batches
append instead of upserting by body-hash. Decide deliberately; note the Data Risk section (this is
player-profiling-adjacent data — keep it out of any public query surface).

### 5.4 [MEDIUM] `write_names` does one round-trip per ID — up to 1000 sequential upserts + Redis SETs per request
**Status: ✅ DONE** — `write_names` builds all rows and issues chunked multi-row `on_conflict_do_update` upserts plus a single `CacheClient.set_names` Redis pipeline, instead of 1000 awaited statements each.

`app/archive.py:180-193`. A full-size `/universe/names/` batch performs 1000 awaited DB statements
and 1000 awaited Redis SETs inline in the request path.

**Fix:** Batch: single multi-row `pg_insert(...).values(list).on_conflict_do_update` (chunked like
`_DELTA_INSERT_BATCH_SIZE`), and a Redis pipeline for the name keys.

### 5.5 [LOW] Stale responses lack the `Warning` header CLAUDE.md/Proxy-safety rules mention
`app/routes.py:14-20` sets `X-Cache: STALE` / `X-Archive-Fallback: true` but no `Warning: 110 - "response is stale"`.
**Fix:** Add the header to the STALE and ARCHIVE_FALLBACK header sets.

**Status: ✅ DONE** — `Warning: 110 - "Response is Stale"` added to the STALE and ARCHIVE_FALLBACK header sets.

### 5.6 [LOW] `ProxyResult.content_type` is always `application/json`
Fine for ESI JSON endpoints, wrong for pass-through errors (4.4) and any future non-JSON endpoint.
**Fix:** Thread the upstream `Content-Type` through `ESIResponse` → cache (store alongside body) →
`ProxyResult`. Can be folded into 5.2's cache-envelope work.

### 5.7 [LOW] Test coverage gaps for existing behavior
No tests exist for: `app/rate_limit.py` (window edges, limit=0), `app/coalesce.py` (leader failure
propagation, concurrent waiters, cancellation), proxy 304 flows (TTL handling — will be needed for
2.4), routes-level body-size enforcement (needed for 4.1), scheduler advisory-lock skip path
(`app/scheduler.py:98-116`), and `_extract_name_mappings` list-response edge cases. Add alongside
the respective fixes.

---

## 6. Suggested fix order

1. **1.1 + 1.2 + 1.3** ✅ done (+ 1.6 done; 1.4/1.5 partial — see per-item notes) (ESI protection: circuit breaker, stop retrying 420, negative caching) — protects the shared IP before anything else.
2. **4.1 + 4.2** ✅ done (body streaming cap; per-endpoint param allowlist) — closes the abuse surface and the permanent-archive bloat vector.
3. **2.1 + 2.2** ✅ done (multi-page ETag; disable redirect following) — stops silent wrong-data and archive gaps.
4. **3.1 + 3.2** ✅ done (real staggering; first-run scheduling) — makes the collector actually deliver its archive cadence.
5. **2.5 + 2.6** ✅ done (parquet manifest race; storage labeling) — archive write robustness.
6. **5.1** ✅ done (metrics) — then verify 1–5 with counters.
7. Remaining MEDIUM/LOW items in any order; each with tests.
