"""FastAPI application entry point."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.cache import create_cache_client
from app.config import settings
from app.esi_client import ESIClient
from app.rate_limit import InMemoryRateLimiter
from app.routes import router
from app.scheduler import create_scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting eve-api-cache")
    app.state.cache = await create_cache_client(settings)
    app.state.esi = ESIClient(settings)
    app.state.rate_limiter = InMemoryRateLimiter(settings.client_rate_limit_per_minute)
    app.state.scheduler = create_scheduler(app.state.esi, app.state.cache, settings)
    if settings.collector_enabled:
        app.state.scheduler.start()
    else:
        logger.info("Collector scheduler disabled by configuration")
    yield
    logger.info("Shutting down eve-api-cache")
    if settings.collector_enabled:
        app.state.scheduler.shutdown(wait=False)
    await app.state.esi.aclose()
    await app.state.cache._r.aclose()


app = FastAPI(
    title="eve-api-cache",
    description="Unauthenticated ESI proxy and permanent historical archive",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
