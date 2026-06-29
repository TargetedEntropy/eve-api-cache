"""
Async SQLAlchemy engine and session factory.

All archive tables are defined here via the ORM. The Alembic migration
generates the actual DDL; these models are the source of truth for schema.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, LargeBinary, SmallInteger, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.config import settings


class Base(DeclarativeBase):
    pass


class TimeSeriesSnapshot(Base):
    """
    Append-only archive for time-varying endpoints:
    market orders, prices, system jumps/kills, sovereignty, incursions, etc.

    Each row is a complete snapshot at a point in time. Never updated or deleted.
    Duplicate retries are idempotent via idempotency_key, which is derived from
    datasource, path, query hash, a short fetched-at bucket, and content hash.
    """
    __tablename__ = "archive_timeseries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    datasource: Mapped[str] = mapped_column(String(20), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    query_hash: Mapped[str] = mapped_column(String(16), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    payload: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    payload_storage: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    parquet_manifest_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, ForeignKey("archive_object_files.id"), nullable=True
    )
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    esi_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    etag: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    http_status: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "datasource", "path", "query_hash", "fetched_at", "content_hash",
            name="uq_timeseries_snapshot",
        ),
        UniqueConstraint("idempotency_key", name="uq_timeseries_idempotency"),
        Index("ix_timeseries_lookup", "datasource", "path", "query_hash", "fetched_at"),
    )


class ArchivePayloadBlob(Base):
    """Compressed raw payload bytes, de-duplicated by content hash."""
    __tablename__ = "archive_payload_blobs"

    content_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    payload_codec: Mapped[str] = mapped_column(String(20), nullable=False)
    payload_compressed: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    raw_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    compressed_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_format: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ArchiveObjectFile(Base):
    """Manifest row for archive objects stored outside PostgreSQL, such as Parquet."""
    __tablename__ = "archive_object_files"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    datasource: Mapped[str] = mapped_column(String(20), nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    query_hash: Mapped[str] = mapped_column(String(16), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    storage_format: Mapped[str] = mapped_column(String(30), nullable=False)
    codec: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    raw_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    stored_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    row_count: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    metadata_json: Mapped[Optional[dict]] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_archive_object_files_lookup", "datasource", "path", "query_hash", "fetched_at"),
    )


class ReferenceSnapshot(Base):
    """
    Upsert-based archive for near-static reference data:
    universe types, systems, regions, corporations, alliances, etc.

    Each (datasource, path, query_hash) has exactly one row; updated on change.
    first_seen_at is never changed after initial insert.
    """
    __tablename__ = "archive_reference"

    datasource: Mapped[str] = mapped_column(String(20), primary_key=True)
    path: Mapped[str] = mapped_column(Text, primary_key=True)
    query_hash: Mapped[str] = mapped_column(String(16), primary_key=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    esi_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    etag: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    http_status: Mapped[int] = mapped_column(SmallInteger, nullable=False)


class EventSnapshot(Base):
    """
    Insert-once archive for immutable event data: killmails, contract items/bids.

    After the first successful fetch, the row is never modified.
    """
    __tablename__ = "archive_events"

    datasource: Mapped[str] = mapped_column(String(20), primary_key=True)
    path: Mapped[str] = mapped_column(Text, primary_key=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    etag: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    http_status: Mapped[int] = mapped_column(SmallInteger, nullable=False)


class IdNameCache(Base):
    """
    Persistent per-ID name mapping extracted from POST /universe/names/, /universe/ids/,
    and /characters/affiliation/ responses.

    Supplements the Redis name cache with durable storage.
    """
    __tablename__ = "id_name_cache"

    datasource: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    entity_name: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MarketOrderVersion(Base):
    """Distinct market-order JSON versions, de-duplicated across snapshots."""
    __tablename__ = "market_order_versions"

    datasource: Mapped[str] = mapped_column(String(20), primary_key=True)
    order_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    version_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MarketOrderSnapshotEntry(Base):
    """Membership table linking a market-order snapshot to order versions."""
    __tablename__ = "market_order_snapshot_entries"

    snapshot_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("archive_timeseries.id", ondelete="CASCADE"),
        primary_key=True,
    )
    order_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    version_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    region_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    __table_args__ = (
        Index("ix_market_order_snapshot_entries_order", "order_id", "version_hash"),
        Index("ix_market_order_snapshot_entries_region", "region_id"),
    )


async_engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)
