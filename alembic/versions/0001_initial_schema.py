"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-06-29

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # archive_timeseries
    op.create_table(
        "archive_timeseries",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("datasource", sa.String(20), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("query_hash", sa.String(16), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("esi_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("etag", sa.String(255), nullable=True),
        sa.Column("http_status", sa.SmallInteger(), nullable=False),
        sa.UniqueConstraint(
            "datasource", "path", "query_hash", "fetched_at", "content_hash",
            name="uq_timeseries_snapshot",
        ),
    )
    op.create_index(
        "ix_timeseries_lookup",
        "archive_timeseries",
        ["datasource", "path", "query_hash", "fetched_at"],
    )

    # archive_reference
    op.create_table(
        "archive_reference",
        sa.Column("datasource", sa.String(20), primary_key=True, nullable=False),
        sa.Column("path", sa.Text(), primary_key=True, nullable=False),
        sa.Column("query_hash", sa.String(16), primary_key=True, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("esi_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("etag", sa.String(255), nullable=True),
        sa.Column("http_status", sa.SmallInteger(), nullable=False),
        sa.PrimaryKeyConstraint("datasource", "path", "query_hash"),
    )

    # archive_events
    op.create_table(
        "archive_events",
        sa.Column("datasource", sa.String(20), primary_key=True, nullable=False),
        sa.Column("path", sa.Text(), primary_key=True, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("etag", sa.String(255), nullable=True),
        sa.Column("http_status", sa.SmallInteger(), nullable=False),
        sa.PrimaryKeyConstraint("datasource", "path"),
    )

    # id_name_cache
    op.create_table(
        "id_name_cache",
        sa.Column("datasource", sa.String(20), primary_key=True, nullable=False),
        sa.Column("entity_id", sa.BigInteger(), primary_key=True, nullable=False),
        sa.Column("entity_name", sa.Text(), nullable=False),
        sa.Column("category", sa.String(50), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("datasource", "entity_id"),
    )


def downgrade() -> None:
    op.drop_table("id_name_cache")
    op.drop_table("archive_events")
    op.drop_table("archive_reference")
    op.drop_index("ix_timeseries_lookup", table_name="archive_timeseries")
    op.drop_table("archive_timeseries")
