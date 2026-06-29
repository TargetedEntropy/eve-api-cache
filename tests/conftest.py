"""Shared pytest fixtures."""
import pytest
import fakeredis.aioredis
from unittest.mock import AsyncMock

from app.cache import CacheClient
from app.config import Settings


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        redis_url="redis://localhost:6379/0",
        database_url="postgresql+asyncpg://localhost/eve_cache_test",
        esi_base_url="https://esi.evetech.net",
        user_agent="eve-api-cache-test/0.1",
        esi_timeout=5.0,
        esi_max_retries=0,
        esi_retry_base_delay=0.0,
        page_concurrency=3,
        upstream_concurrency=5,
        default_datasource="tranquility",
    )


@pytest.fixture
async def fake_redis():
    """In-memory fakeredis instance (no real Redis connection)."""
    return fakeredis.aioredis.FakeRedis(decode_responses=False)


@pytest.fixture
async def cache_client(fake_redis) -> CacheClient:
    return CacheClient(fake_redis)


@pytest.fixture
def mock_db():
    """Mock AsyncSession — archive writes are unit-tested separately."""
    return AsyncMock()


@pytest.fixture
def mock_esi():
    return AsyncMock()
