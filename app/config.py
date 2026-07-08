from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://localhost/eve_cache"
    # SQLAlchemy async pool sizing. Sized above collector job concurrency so the
    # advisory-lock sessions + inner work can't starve proxy archive writes (3.3).
    db_pool_size: int = 20
    db_max_overflow: int = 10
    esi_base_url: str = "https://esi.evetech.net"
    user_agent: str = "eve-api-cache/0.1 (https://github.com/TargetedEntropy/eve-api-cache)"
    esi_timeout: float = 30.0
    esi_max_retries: int = 2
    esi_retry_base_delay: float = 0.25
    esi_retry_max_delay: float = 10.0          # ceiling on a single 5xx/transport retry backoff
                                               # (420/429 no longer retry — they trip the breaker)
    page_concurrency: int = 10
    upstream_concurrency: int = 20

    # ESI error-budget circuit breaker. ESI throttles per source IP once the
    # shared error counter is exhausted (420). When remaining errors fall to/below
    # this threshold — or a 420/429 is seen — upstream calls are refused for the
    # reset window and the proxy serves stale/archive fallback instead.
    esi_error_budget_threshold: int = 10
    esi_error_budget_block_seconds: int = 60   # fallback block window when ESI gives no reset hint

    # Global outbound pacer (token bucket). Smooths bulk collector bursts so they
    # trickle to ESI instead of firing thousands of requests at once. 0 disables.
    esi_max_requests_per_second: float = 20.0

    default_datasource: str = "tranquility"
    max_post_body_bytes: int = 65536
    max_post_batch_items: int = 1000
    stale_cache_seconds: int = 3600
    stale_cache_max_body_bytes: int = 5_000_000
    negative_cache_ttl_seconds: int = 60       # how long to cache a 4xx (unless ESI Cache-Control says longer)
    cache_compress_min_bytes: int = 65536      # zstd/zlib-compress Redis bodies at/above this size (5.2)

    # Trusted reverse-proxy source IPs. When the direct peer is in this list, the
    # first X-Forwarded-For hop is used for per-client rate limiting. Empty = use
    # the direct peer (safe default; socat does NOT add XFF — see deploy notes) (4.3).
    trusted_proxies: list[str] = []

    # Optional shared token gating /metrics and /collector/status. None = open (4.5).
    ops_api_token: Optional[str] = None

    # Collector: extra version prefixes to also warm in Redis for the same body,
    # so downstream apps calling e.g. /latest/... hit collector-warmed keys. The
    # archive is still written once under the canonical /v1/ path (no identity
    # merge). Empty = only warm the canonical path; document /v1/ for downstream (2.9).
    collector_warm_alias_versions: list[str] = []

    # Per-client request cap. Kept well below ESI's shared ~100-errors/min budget:
    # the error-budget breaker (esi_error_budget_threshold) is the real protection,
    # this only bounds a single caller's share of proxy throughput.
    client_rate_limit_per_minute: int = 300
    collector_enabled: bool = True
    archive_data_dir: str = "/var/lib/eve-api-cache/archive"
    enable_market_order_parquet: bool = True
    enable_market_order_deltas: bool = True

    # Collector — which regions to proactively poll for market data.
    # Defaults: The Forge (Jita), Domain (Amarr), Heimatar (Rens),
    #           Sinq Laison (Dodixie), Metropolis (Hek)
    market_region_ids: list[int] = [10000002, 10000043, 10000030, 10000032, 10000042]

    # Collector poll intervals (seconds)
    poll_market_orders_seconds: int = 300    # ESI cache TTL for orders
    poll_market_prices_seconds: int = 3600   # global prices update hourly
    poll_market_history_seconds: int = 86400 # history updates daily
    poll_universe_seconds: int = 3600        # jumps, kills, sovereignty, incursions


settings = Settings()
