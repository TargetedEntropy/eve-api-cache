"""PostgreSQL-backed tests for the archive write layer."""
import hashlib
import uuid

import pytest
from sqlalchemy import delete, func, select, text

from app.allowlist import ArchiveType
from app.archive import get_latest_payload, write_names, write_snapshot
from app.db import AsyncSessionLocal, IdNameCache, TimeSeriesSnapshot

pytestmark = pytest.mark.postgres


class _FakeNameCache:
    def __init__(self) -> None:
        self.names = {}

    async def set_name(self, datasource: str, entity_id: int, name: str, category: str, ttl: int = 86400) -> None:
        self.names[(datasource, entity_id)] = {
            "name": name,
            "category": category,
            "ttl": ttl,
        }


@pytest.fixture
async def postgres_archive():
    try:
        async with AsyncSessionLocal() as session:
            table_exists = await session.scalar(text("SELECT to_regclass('archive_timeseries')"))
            if table_exists is None:
                pytest.skip("PostgreSQL archive schema is not migrated")
    except Exception as exc:
        pytest.skip(f"PostgreSQL test database is unavailable: {exc}")


async def test_timeseries_write_is_idempotent_within_retry_bucket(postgres_archive):
    path = f"/v1/test/{uuid.uuid4()}/"
    payload = b'[{"order_id":1}]'
    content_hash = hashlib.sha256(payload).hexdigest()

    try:
        async with AsyncSessionLocal() as session:
            await write_snapshot(
                session,
                "tranquility",
                path,
                "queryhash",
                content_hash,
                payload,
                200,
                '"etag"',
                None,
                ArchiveType.TIME_SERIES,
            )
            await write_snapshot(
                session,
                "tranquility",
                path,
                "queryhash",
                content_hash,
                payload,
                200,
                '"etag"',
                None,
                ArchiveType.TIME_SERIES,
            )

        async with AsyncSessionLocal() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(TimeSeriesSnapshot)
                .where(TimeSeriesSnapshot.path == path)
            )
            assert count == 1
            fallback = await get_latest_payload(session, "tranquility", path, "queryhash")
            assert fallback == b'[{"order_id": 1}]'
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(delete(TimeSeriesSnapshot).where(TimeSeriesSnapshot.path == path))
            await session.commit()


async def test_write_names_extracts_universe_ids_object_response(postgres_archive):
    datasource = "tranquility"
    entity_ids = [99000001, 99000002]
    payload = (
        b'{"characters":[{"id":99000001,"name":"Pilot One"}],'
        b'"systems":[{"id":99000002,"name":"Jita"}]}'
    )
    cache = _FakeNameCache()

    try:
        async with AsyncSessionLocal() as session:
            await write_names(session, cache, datasource, payload)

        assert cache.names[(datasource, 99000001)]["category"] == "character"
        assert cache.names[(datasource, 99000002)]["category"] == "system"

        async with AsyncSessionLocal() as session:
            rows = (
                await session.execute(
                    select(IdNameCache)
                    .where(IdNameCache.datasource == datasource)
                    .where(IdNameCache.entity_id.in_(entity_ids))
                )
            ).scalars().all()

        assert {row.entity_name for row in rows} == {"Pilot One", "Jita"}
        assert {row.category for row in rows} == {"character", "system"}
    finally:
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(IdNameCache)
                .where(IdNameCache.datasource == datasource)
                .where(IdNameCache.entity_id.in_(entity_ids))
            )
            await session.commit()
