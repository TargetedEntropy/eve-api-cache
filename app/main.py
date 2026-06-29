"""FastAPI application entry point."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.cache import create_cache_client
from app.config import settings
from app.esi_client import ESIClient
from app.routes import router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting eve-api-cache")
    app.state.cache = await create_cache_client(settings)
    app.state.esi = ESIClient(settings)
    yield
    logger.info("Shutting down eve-api-cache")
    await app.state.esi.aclose()
    await app.state.cache._r.aclose()


app = FastAPI(
    title="eve-api-cache",
    description="Unauthenticated ESI proxy and permanent historical archive",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)
