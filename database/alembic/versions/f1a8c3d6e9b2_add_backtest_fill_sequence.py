"""add_backtest_fill_sequence

Revision ID: f1a8c3d6e9b2
Revises: e2f4a6b8c0d1
Create Date: 2026-07-27 00:00:00.000000

Record authoritative fill order for new backtest sessions. Existing rows stay
NULL because equal-timestamp ordering cannot be reconstructed from UUIDs.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "f1a8c3d6e9b2"
down_revision: Union[str, Sequence[str], None] = "e2f4a6b8c0d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "backtest_trade_log",
        sa.Column("fill_sequence", sa.BigInteger(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_backtest_trade_log_session_fill_sequence",
        "backtest_trade_log",
        ["session_id", "fill_sequence"],
    )
    op.create_check_constraint(
        "chk_backtest_trade_log_fill_sequence_nonnegative",
        "backtest_trade_log",
        "fill_sequence IS NULL OR fill_sequence >= 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "chk_backtest_trade_log_fill_sequence_nonnegative",
        "backtest_trade_log",
        type_="check",
    )
    op.drop_constraint(
        "uq_backtest_trade_log_session_fill_sequence",
        "backtest_trade_log",
        type_="unique",
    )
    op.drop_column("backtest_trade_log", "fill_sequence")
