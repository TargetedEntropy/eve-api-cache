"""FastAPI dependency providers for cache, database session, and ESI client."""
from collections.abc import AsyncGenerator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache import CacheClient
from app.config import Settings, settings as _settings
from app.db import AsyncSessionLocal
from app.esi_client import ESIClient


async def get_settings() -> Settings:
    return _settings


async def get_cache(request: Request) -> CacheClient:
    return request.app.state.cache


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session


async def get_esi(request: Request) -> ESIClient:
    return request.app.state.esi
