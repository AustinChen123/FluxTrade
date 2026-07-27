"""add_market_data_application

Revision ID: 9b7e2c4d6f10
Revises: f1a8c3d6e9b2
Create Date: 2026-07-27 00:00:00.000000

Record the durable boundary after a live closed candle has been applied.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "9b7e2c4d6f10"
down_revision: Union[str, Sequence[str], None] = "f1a8c3d6e9b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_data_application",
        sa.Column("environment", sa.String(length=64), nullable=False),
        sa.Column("product_id", sa.String(), nullable=False),
        sa.Column("timeframe", sa.String(), nullable=False),
        sa.Column("timestamp", sa.BigInteger(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.Column("volume", sa.Numeric(), nullable=False),
        sa.Column(
            "applied_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["product_id"], ["product.id"]),
        sa.PrimaryKeyConstraint(
            "environment",
            "product_id",
            "timeframe",
            "timestamp",
            name="pk_market_data_application",
        ),
    )


def downgrade() -> None:
    op.drop_table("market_data_application")
