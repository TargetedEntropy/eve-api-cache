"""add timeseries idempotency key

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-29

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "archive_timeseries",
        sa.Column("idempotency_key", sa.String(length=64), nullable=True),
    )
    op.create_unique_constraint(
        "uq_timeseries_idempotency",
        "archive_timeseries",
        ["idempotency_key"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_timeseries_idempotency",
        "archive_timeseries",
        type_="unique",
    )
    op.drop_column("archive_timeseries", "idempotency_key")
