from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://localhost/eve_cache"
    esi_base_url: str = "https://esi.evetech.net"
    user_agent: str = "eve-api-cache/0.1 (https://github.com/TargetedEntropy/eve-api-cache)"
    esi_timeout: float = 30.0
    page_concurrency: int = 10
    default_datasource: str = "tranquility"

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
