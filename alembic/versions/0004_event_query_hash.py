"""add query_hash to archive_events

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-08

Adds query_hash to the archive_events primary key so params-bearing EVENT
endpoints don't collapse distinct queries onto one insert-once row (2.8).

Additive and history-preserving: existing rows are backfilled with the
empty-params query hash (the value the proxy computes for today's param-less
EVENT endpoints — killmails, contract items/bids), so they remain reachable
via get_latest_payload after the primary key widens.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# compute_query_hash({}, None) — hash of param-less EVENT requests.
_EMPTY_PARAMS_HASH = "44136fa355b3678a"


def upgrade() -> None:
    op.add_column(
        "archive_events",
        sa.Column("query_hash", sa.String(length=16), nullable=False, server_default=_EMPTY_PARAMS_HASH),
    )
    op.drop_constraint("archive_events_pkey", "archive_events", type_="primary")
    op.create_primary_key(
        "archive_events_pkey", "archive_events", ["datasource", "path", "query_hash"]
    )
    # Drop the server default now that existing rows are backfilled; the app
    # always supplies query_hash explicitly on insert.
    op.alter_column("archive_events", "query_hash", server_default=None)


def downgrade() -> None:
    op.drop_constraint("archive_events_pkey", "archive_events", type_="primary")
    op.create_primary_key("archive_events_pkey", "archive_events", ["datasource", "path"])
    op.drop_column("archive_events", "query_hash")
