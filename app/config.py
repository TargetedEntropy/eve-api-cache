from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://localhost/eve_cache"
    esi_base_url: str = "https://esi.evetech.net"
    user_agent: str = "eve-api-cache/0.1 (https://github.com/TargetedEntropy/eve-api-cache)"
    esi_timeout: float = 30.0
    esi_max_retries: int = 2
    esi_retry_base_delay: float = 0.25
    page_concurrency: int = 10
    upstream_concurrency: int = 20
    default_datasource: str = "tranquility"
    max_post_body_bytes: int = 65536
    max_post_batch_items: int = 1000
    stale_cache_seconds: int = 3600
    stale_cache_max_body_bytes: int = 5_000_000
    client_rate_limit_per_minute: int = 600
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
