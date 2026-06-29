"""add compressed archive storage

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-29

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "archive_payload_blobs",
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_codec", sa.String(length=20), nullable=False),
        sa.Column("payload_compressed", sa.LargeBinary(), nullable=False),
        sa.Column("raw_size", sa.BigInteger(), nullable=False),
        sa.Column("compressed_size", sa.BigInteger(), nullable=False),
        sa.Column("storage_format", sa.String(length=30), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("content_hash"),
    )

    op.create_table(
        "archive_object_files",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("datasource", sa.String(length=20), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("query_hash", sa.String(length=16), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("storage_format", sa.String(length=30), nullable=False),
        sa.Column("codec", sa.String(length=20), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("raw_size", sa.BigInteger(), nullable=False),
        sa.Column("stored_size", sa.BigInteger(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("metadata", JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("content_hash"),
    )
    op.create_index(
        "ix_archive_object_files_lookup",
        "archive_object_files",
        ["datasource", "path", "query_hash", "fetched_at"],
    )

    op.add_column(
        "archive_timeseries",
        sa.Column("payload_storage", sa.String(length=30), nullable=True),
    )
    op.add_column(
        "archive_timeseries",
        sa.Column("parquet_manifest_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_timeseries_parquet_manifest",
        "archive_timeseries",
        "archive_object_files",
        ["parquet_manifest_id"],
        ["id"],
    )
    op.alter_column(
        "archive_timeseries",
        "payload",
        existing_type=JSONB(),
        nullable=True,
    )

    op.create_table(
        "market_order_versions",
        sa.Column("datasource", sa.String(length=20), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=False),
        sa.Column("version_hash", sa.String(length=64), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("datasource", "order_id", "version_hash"),
    )

    op.create_table(
        "market_order_snapshot_entries",
        sa.Column("snapshot_id", sa.BigInteger(), nullable=False),
        sa.Column("order_id", sa.BigInteger(), nullable=False),
        sa.Column("version_hash", sa.String(length=64), nullable=False),
        sa.Column("region_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(["snapshot_id"], ["archive_timeseries.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("snapshot_id", "order_id"),
    )
    op.create_index(
        "ix_market_order_snapshot_entries_order",
        "market_order_snapshot_entries",
        ["order_id", "version_hash"],
    )
    op.create_index(
        "ix_market_order_snapshot_entries_region",
        "market_order_snapshot_entries",
        ["region_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_market_order_snapshot_entries_region", table_name="market_order_snapshot_entries")
    op.drop_index("ix_market_order_snapshot_entries_order", table_name="market_order_snapshot_entries")
    op.drop_table("market_order_snapshot_entries")
    op.drop_table("market_order_versions")

    op.alter_column(
        "archive_timeseries",
        "payload",
        existing_type=JSONB(),
        nullable=False,
    )
    op.drop_constraint("fk_timeseries_parquet_manifest", "archive_timeseries", type_="foreignkey")
    op.drop_column("archive_timeseries", "parquet_manifest_id")
    op.drop_column("archive_timeseries", "payload_storage")

    op.drop_index("ix_archive_object_files_lookup", table_name="archive_object_files")
    op.drop_table("archive_object_files")
    op.drop_table("archive_payload_blobs")
