"""
Async SQLAlchemy engine and session factory.

All archive tables are defined here via the ORM. The Alembic migration
generates the actual DDL; these models are the source of truth for schema.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, Index, SmallInteger, String, Text, UniqueConstraint
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
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
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


async_engine = create_async_engine(settings.database_url, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)
