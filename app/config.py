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


settings = Settings()
