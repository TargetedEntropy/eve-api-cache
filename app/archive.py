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
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.allowlist import ArchiveType
from app.config import settings
from app.db import (
    ArchiveObjectFile,
    ArchivePayloadBlob,
    EventSnapshot,
    IdNameCache,
    MarketOrderSnapshotEntry,
    MarketOrderVersion,
    ReferenceSnapshot,
    TimeSeriesSnapshot,
)

_MARKET_ORDERS_RE = re.compile(r"^/[^/]+/markets/(\d+)/orders/?$")
_STORAGE_JSONB = "jsonb"
_STORAGE_COMPRESSED_JSON = "compressed_json"
_STORAGE_MARKET_PARQUET_DELTA = "market_parquet_delta"
_DELTA_INSERT_BATCH_SIZE = 1000


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

    if archive_type == ArchiveType.TIME_SERIES:
        await _write_payload_blob(session, content_hash, payload, now)
        idempotency_key = _timeseries_idempotency_key(
            datasource, path, query_hash, content_hash, now
        )
        is_market_orders = _market_orders_region_id(path) is not None
        payload_storage = (
            _STORAGE_MARKET_PARQUET_DELTA
            if is_market_orders and settings.enable_market_order_parquet
            else _STORAGE_COMPRESSED_JSON
        )
        stmt = pg_insert(TimeSeriesSnapshot).values(
            datasource=datasource,
            path=path,
            query_hash=query_hash,
            content_hash=content_hash,
            idempotency_key=idempotency_key,
            payload=None,
            payload_storage=payload_storage,
            fetched_at=now,
            esi_expires_at=expires_at,
            etag=etag,
            http_status=http_status,
        ).on_conflict_do_nothing(
            constraint="uq_timeseries_idempotency"
        ).returning(TimeSeriesSnapshot.id)
        result = await session.execute(stmt)
        snapshot_id = result.scalar_one_or_none()
        if snapshot_id is None:
            snapshot_id = await _get_timeseries_id_by_idempotency_key(session, idempotency_key)

        if snapshot_id and is_market_orders and settings.enable_market_order_parquet:
            manifest_id = await _write_market_orders_parquet(
                session=session,
                snapshot_id=snapshot_id,
                datasource=datasource,
                path=path,
                query_hash=query_hash,
                content_hash=content_hash,
                payload=payload,
                fetched_at=now,
            )
            await session.execute(
                update(TimeSeriesSnapshot)
                .where(TimeSeriesSnapshot.id == snapshot_id)
                .values(
                    parquet_manifest_id=manifest_id,
                    payload_storage=_STORAGE_MARKET_PARQUET_DELTA,
                )
            )

    elif archive_type == ArchiveType.REFERENCE:
        payload_json = json.loads(payload)
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
        payload_json = json.loads(payload)
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

    mappings = _extract_name_mappings(data)
    if not mappings:
        return

    for entity_id, name, category in mappings:
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
    # Time-series: most recent snapshot. Supports old inline JSONB rows and new
    # compressed-blob rows during the storage migration.
    stmt = (
        select(TimeSeriesSnapshot)
        .where(
            TimeSeriesSnapshot.datasource == datasource,
            TimeSeriesSnapshot.path == path,
            TimeSeriesSnapshot.query_hash == query_hash,
        )
        .order_by(TimeSeriesSnapshot.fetched_at.desc())
        .limit(1)
    )
    row = await session.execute(stmt)
    snapshot = row.scalar_one_or_none()
    if snapshot is not None:
        if snapshot.payload is not None:
            return json.dumps(snapshot.payload).encode()
        blob = await session.get(ArchivePayloadBlob, snapshot.content_hash)
        if blob is not None:
            return _decompress_payload(blob.payload_codec, blob.payload_compressed)
        if snapshot.parquet_manifest_id is not None:
            manifest = await session.get(ArchiveObjectFile, snapshot.parquet_manifest_id)
            if manifest is not None:
                return _read_parquet_payload(manifest.file_path)

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


def _timeseries_idempotency_key(
    datasource: str,
    path: str,
    query_hash: str,
    content_hash: str,
    fetched_at: datetime,
) -> str:
    bucket = fetched_at.replace(second=0, microsecond=0)
    raw = "\0".join((datasource, path, query_hash, bucket.isoformat(), content_hash))
    return hashlib.sha256(raw.encode()).hexdigest()


async def _get_timeseries_id_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Optional[int]:
    stmt = select(TimeSeriesSnapshot.id).where(TimeSeriesSnapshot.idempotency_key == idempotency_key)
    row = await session.execute(stmt)
    return row.scalar_one_or_none()


async def _write_payload_blob(
    session: AsyncSession,
    content_hash: str,
    payload: bytes,
    created_at: datetime,
) -> None:
    codec, compressed = _compress_payload(payload)
    stmt = pg_insert(ArchivePayloadBlob).values(
        content_hash=content_hash,
        payload_codec=codec,
        payload_compressed=compressed,
        raw_size=len(payload),
        compressed_size=len(compressed),
        storage_format="raw_json",
        created_at=created_at,
    ).on_conflict_do_nothing(
        index_elements=["content_hash"]
    )
    await session.execute(stmt)


def _compress_payload(payload: bytes) -> tuple[str, bytes]:
    try:
        import zstandard as zstd

        return "zstd", zstd.ZstdCompressor(level=6).compress(payload)
    except ImportError:
        import zlib

        return "zlib", zlib.compress(payload, level=9)


def _decompress_payload(codec: str, payload: bytes) -> bytes:
    if codec == "zstd":
        import zstandard as zstd

        return zstd.ZstdDecompressor().decompress(payload)
    if codec == "zlib":
        import zlib

        return zlib.decompress(payload)
    raise ValueError(f"unsupported archive payload codec: {codec}")


async def _write_market_orders_parquet(
    session: AsyncSession,
    snapshot_id: int,
    datasource: str,
    path: str,
    query_hash: str,
    content_hash: str,
    payload: bytes,
    fetched_at: datetime,
) -> int:
    region_id = _market_orders_region_id(path)
    if region_id is None:
        raise ValueError(f"not a market-orders path: {path}")

    orders = json.loads(payload)
    if not isinstance(orders, list):
        raise ValueError("market-orders payload must be a JSON list")

    relative_path, stored_size = _write_market_orders_parquet_file(
        datasource=datasource,
        region_id=region_id,
        content_hash=content_hash,
        fetched_at=fetched_at,
        orders=orders,
    )

    metadata = {"region_id": region_id, "snapshot_id": snapshot_id}
    stmt = pg_insert(ArchiveObjectFile).values({
        "content_hash": content_hash,
        "datasource": datasource,
        "path": path,
        "query_hash": query_hash,
        "fetched_at": fetched_at,
        "storage_format": "parquet",
        "codec": "zstd",
        "file_path": relative_path,
        "raw_size": len(payload),
        "stored_size": stored_size,
        "row_count": len(orders),
        ArchiveObjectFile.__table__.c.metadata: metadata,
        "created_at": fetched_at,
    }).on_conflict_do_update(
        index_elements=["content_hash"],
        set_={
            ArchiveObjectFile.file_path: relative_path,
            ArchiveObjectFile.stored_size: stored_size,
            ArchiveObjectFile.row_count: len(orders),
            ArchiveObjectFile.__table__.c.metadata: metadata,
        },
    ).returning(ArchiveObjectFile.id)
    result = await session.execute(stmt)
    manifest_id = result.scalar_one()

    if settings.enable_market_order_deltas:
        await _write_market_order_deltas(
            session=session,
            snapshot_id=snapshot_id,
            datasource=datasource,
            region_id=region_id,
            orders=orders,
            fetched_at=fetched_at,
        )

    return manifest_id


def _write_market_orders_parquet_file(
    datasource: str,
    region_id: int,
    content_hash: str,
    fetched_at: datetime,
    orders: list[dict],
) -> tuple[str, int]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for market-order Parquet archives") from exc

    date_part = fetched_at.strftime("%Y-%m-%d")
    timestamp_part = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    relative_path = Path(
        "markets",
        "orders",
        f"datasource={datasource}",
        f"region_id={region_id}",
        f"date={date_part}",
        f"snapshot_{timestamp_part}_{content_hash[:12]}.parquet",
    )
    full_path = Path(settings.archive_data_dir) / relative_path
    full_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(orders) if orders else pa.table({})
    pq.write_table(table, full_path, compression="zstd")
    return str(relative_path), full_path.stat().st_size


async def _write_market_order_deltas(
    session: AsyncSession,
    snapshot_id: int,
    datasource: str,
    region_id: int,
    orders: list[dict],
    fetched_at: datetime,
) -> None:
    version_rows = []
    entry_rows = []
    for order in orders:
        if not isinstance(order, dict) or "order_id" not in order:
            continue
        order_id = int(order["order_id"])
        version_hash = hashlib.sha256(
            json.dumps(order, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        version_rows.append(
            {
                "datasource": datasource,
                "order_id": order_id,
                "version_hash": version_hash,
                "payload": order,
                "first_seen_at": fetched_at,
                "last_seen_at": fetched_at,
            }
        )
        entry_rows.append(
            {
                "snapshot_id": snapshot_id,
                "order_id": order_id,
                "version_hash": version_hash,
                "region_id": region_id,
            }
        )

    for batch in _chunks(version_rows, _DELTA_INSERT_BATCH_SIZE):
        version_stmt = pg_insert(MarketOrderVersion).values(batch)
        version_stmt = version_stmt.on_conflict_do_update(
            index_elements=["datasource", "order_id", "version_hash"],
            set_=dict(last_seen_at=fetched_at),
        )
        await session.execute(version_stmt)

    for batch in _chunks(entry_rows, _DELTA_INSERT_BATCH_SIZE):
        entry_stmt = pg_insert(MarketOrderSnapshotEntry).values(batch)
        entry_stmt = entry_stmt.on_conflict_do_nothing(
            index_elements=["snapshot_id", "order_id"]
        )
        await session.execute(entry_stmt)


def _market_orders_region_id(path: str) -> Optional[int]:
    match = _MARKET_ORDERS_RE.match(path)
    if match is None:
        return None
    return int(match.group(1))


def _chunks(rows: list[dict], size: int):
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def _read_parquet_payload(file_path: str) -> bytes:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required to read Parquet archive payloads") from exc

    table = pq.read_table(Path(settings.archive_data_dir) / file_path)
    return json.dumps(table.to_pylist()).encode()


def _extract_name_mappings(data) -> list[tuple[int, str, Optional[str]]]:
    if isinstance(data, list):
        return [
            (item["id"], item["name"], item.get("category"))
            for item in data
            if isinstance(item, dict) and item.get("id") and item.get("name")
        ]

    if not isinstance(data, dict):
        return []

    mappings: list[tuple[int, str, Optional[str]]] = []
    for category, values in data.items():
        if not isinstance(values, list):
            continue
        normalized_category = category[:-1] if category.endswith("s") else category
        for item in values:
            if isinstance(item, dict) and item.get("id") and item.get("name"):
                mappings.append((item["id"], item["name"], normalized_category))
    return mappings
