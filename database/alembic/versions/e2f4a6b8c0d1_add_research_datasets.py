"""add_research_datasets

Revision ID: e2f4a6b8c0d1
Revises: 7a3c9e1b5d2f
Create Date: 2026-07-26 00:00:00.000000

Store immutable, versioned research candles separately from the canonical
live candlestick table.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "e2f4a6b8c0d1"
down_revision: Union[str, Sequence[str], None] = "7a3c9e1b5d2f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "research_dataset",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("product_id", sa.String(), nullable=False),
        sa.Column("timeframe", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("revision", sa.String(length=128), nullable=False),
        sa.Column("timestamp_format", sa.String(length=32), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("roll_policy", sa.String(length=128), nullable=True),
        sa.Column("start_time", sa.BigInteger(), nullable=False),
        sa.Column("end_time", sa.BigInteger(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("quality_status", sa.String(length=32), nullable=False),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "quality_status IN ('validated')",
            name="chk_research_dataset_quality_status",
        ),
        sa.CheckConstraint(
            "row_count > 0",
            name="chk_research_dataset_nonempty",
        ),
        sa.CheckConstraint(
            "start_time <= end_time",
            name="chk_research_dataset_time_range",
        ),
        sa.ForeignKeyConstraint(["product_id"], ["product.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "research_candlestick",
        sa.Column("dataset_id", sa.String(length=128), nullable=False),
        sa.Column("timestamp", sa.BigInteger(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.Column("volume", sa.Numeric(), nullable=False),
        sa.Column("source_contract", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["research_dataset.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dataset_id", "timestamp"),
    )


def downgrade() -> None:
    op.drop_table("research_candlestick")
    op.drop_table("research_dataset")
