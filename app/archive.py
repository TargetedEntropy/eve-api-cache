"""
PostgreSQL archive write layer.

Three write strategies determined by EndpointSpec.archive_type:
  TIME_SERIES — append every complete snapshot (never overwrite)
  REFERENCE   — upsert by (datasource, path, query_hash) primary key
  EVENT       — insert-once by (datasource, path); silently skip if already exists
  NONE        — no archive write

All writes are idempotent: duplicate retries produce no extra rows.
"""
import json
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.allowlist import ArchiveType
from app.db import EventSnapshot, IdNameCache, ReferenceSnapshot, TimeSeriesSnapshot


async def write_snapshot(
    session: AsyncSession,
    datasource: str,
    path: str,
    query_hash: str,
    content_hash: str,
    payload: bytes,
    http_status: int,
    etag: Optional[str],
    expires_at: Optional[datetime],
    archive_type: ArchiveType,
) -> None:
    """Write a fetched ESI response to the appropriate archive table."""
    if archive_type == ArchiveType.NONE:
        return

    now = datetime.now(timezone.utc)
    payload_json = json.loads(payload)

    if archive_type == ArchiveType.TIME_SERIES:
        stmt = pg_insert(TimeSeriesSnapshot).values(
            datasource=datasource,
            path=path,
            query_hash=query_hash,
            content_hash=content_hash,
            payload=payload_json,
            fetched_at=now,
            esi_expires_at=expires_at,
            etag=etag,
            http_status=http_status,
        ).on_conflict_do_nothing(
            constraint="uq_timeseries_snapshot"
        )
        await session.execute(stmt)

    elif archive_type == ArchiveType.REFERENCE:
        stmt = pg_insert(ReferenceSnapshot).values(
            datasource=datasource,
            path=path,
            query_hash=query_hash,
            content_hash=content_hash,
            payload=payload_json,
            first_seen_at=now,
            last_updated_at=now,
            esi_expires_at=expires_at,
            etag=etag,
            http_status=http_status,
        ).on_conflict_do_update(
            index_elements=["datasource", "path", "query_hash"],
            set_=dict(
                content_hash=content_hash,
                payload=payload_json,
                last_updated_at=now,
                esi_expires_at=expires_at,
                etag=etag,
                http_status=http_status,
            ),
        )
        await session.execute(stmt)

    elif archive_type == ArchiveType.EVENT:
        stmt = pg_insert(EventSnapshot).values(
            datasource=datasource,
            path=path,
            content_hash=content_hash,
            payload=payload_json,
            fetched_at=now,
            etag=etag,
            http_status=http_status,
        ).on_conflict_do_nothing(
            index_elements=["datasource", "path"]
        )
        await session.execute(stmt)

    await session.commit()


async def write_names(
    session: AsyncSession,
    cache,  # CacheClient
    datasource: str,
    payload: bytes,
) -> None:
    """
    Extract per-ID name mappings from /universe/names/, /universe/ids/,
    and /characters/affiliation/ responses and persist them to both
    the Redis name cache and the IdNameCache table.

    /universe/names/ response: [{id, name, category}, ...]
    /characters/affiliation/ response: [{character_id, corporation_id, alliance_id?, faction_id?}, ...]
    """
    now = datetime.now(timezone.utc)
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        return

    if not isinstance(data, list):
        return

    for item in data:
        if not isinstance(item, dict):
            continue
        # /universe/names/ and /universe/ids/ format
        entity_id = item.get("id")
        name = item.get("name")
        category = item.get("category")
        if entity_id and name:
            await cache.set_name(datasource, entity_id, name, category or "unknown")
            stmt = pg_insert(IdNameCache).values(
                datasource=datasource,
                entity_id=entity_id,
                entity_name=name,
                category=category,
                first_seen_at=now,
                last_updated_at=now,
            ).on_conflict_do_update(
                index_elements=["datasource", "entity_id"],
                set_=dict(entity_name=name, category=category, last_updated_at=now),
            )
            await session.execute(stmt)

    await session.commit()


async def get_latest_payload(
    session: AsyncSession,
    datasource: str,
    path: str,
    query_hash: str,
) -> Optional[bytes]:
    """
    Return the most recent archived payload for stale-fallback, or None.
    Checks time-series first (most recent by fetched_at), then reference, then events.
    """
    # Time-series: most recent snapshot
    stmt = (
        select(TimeSeriesSnapshot.payload)
        .where(
            TimeSeriesSnapshot.datasource == datasource,
            TimeSeriesSnapshot.path == path,
            TimeSeriesSnapshot.query_hash == query_hash,
        )
        .order_by(TimeSeriesSnapshot.fetched_at.desc())
        .limit(1)
    )
    row = await session.execute(stmt)
    result = row.scalar_one_or_none()
    if result is not None:
        return json.dumps(result).encode()

    # Reference: single upserted row
    stmt = select(ReferenceSnapshot.payload).where(
        ReferenceSnapshot.datasource == datasource,
        ReferenceSnapshot.path == path,
        ReferenceSnapshot.query_hash == query_hash,
    )
    row = await session.execute(stmt)
    result = row.scalar_one_or_none()
    if result is not None:
        return json.dumps(result).encode()

    # Event: insert-once row
    stmt = select(EventSnapshot.payload).where(
        EventSnapshot.datasource == datasource,
        EventSnapshot.path == path,
    )
    row = await session.execute(stmt)
    result = row.scalar_one_or_none()
    if result is not None:
        return json.dumps(result).encode()

    return None
