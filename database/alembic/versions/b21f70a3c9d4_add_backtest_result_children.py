"""Add ordered equity samples and closed trades to formal backtest results."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "b21f70a3c9d4"
down_revision: Union[str, Sequence[str], None] = "a17c93e24b60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_SPECIAL_NUMERIC_VALUES = "('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)"


def upgrade() -> None:
    op.create_table(
        "backtest_equity_sample",
        sa.Column("summary_id", sa.Integer(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.BigInteger(), nullable=False),
        sa.Column("equity", sa.Numeric(), nullable=False),
        sa.Column("drawdown", sa.Numeric(), nullable=False),
        sa.ForeignKeyConstraint(
            ["summary_id"],
            ["backtest_result_summary.id"],
            name="fk_backtest_equity_sample_summary",
        ),
        sa.PrimaryKeyConstraint("summary_id", "sequence"),
        sa.CheckConstraint("sequence >= 0", name="ck_backtest_equity_sample_sequence"),
        sa.CheckConstraint(
            "drawdown >= 0", name="ck_backtest_equity_sample_drawdown_nonnegative"
        ),
        sa.CheckConstraint(
            "equity NOT IN " + _SPECIAL_NUMERIC_VALUES,
            name="ck_backtest_equity_sample_equity_finite",
        ),
        sa.CheckConstraint(
            "drawdown NOT IN " + _SPECIAL_NUMERIC_VALUES,
            name="ck_backtest_equity_sample_drawdown_finite",
        ),
    )
    op.create_table(
        "backtest_closed_trade",
        sa.Column("summary_id", sa.Integer(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("entry_time", sa.BigInteger(), nullable=False),
        sa.Column("exit_time", sa.BigInteger(), nullable=False),
        sa.Column("side", sa.String(length=5), nullable=False),
        sa.Column("quantity", sa.Numeric(), nullable=False),
        sa.Column("entry_price", sa.Numeric(), nullable=False),
        sa.Column("exit_price", sa.Numeric(), nullable=False),
        sa.Column("fee", sa.Numeric(), nullable=False),
        sa.Column("pnl", sa.Numeric(), nullable=False),
        sa.ForeignKeyConstraint(
            ["summary_id"],
            ["backtest_result_summary.id"],
            name="fk_backtest_closed_trade_summary",
        ),
        sa.PrimaryKeyConstraint("summary_id", "sequence"),
        sa.CheckConstraint("sequence >= 0", name="ck_backtest_closed_trade_sequence"),
        sa.CheckConstraint(
            "side IN ('LONG', 'SHORT')", name="ck_backtest_closed_trade_side"
        ),
        sa.CheckConstraint(
            "quantity > 0", name="ck_backtest_closed_trade_quantity_positive"
        ),
        *(
            sa.CheckConstraint(
                f"{field} NOT IN " + _SPECIAL_NUMERIC_VALUES,
                name=f"ck_backtest_closed_trade_{field}_finite",
            )
            for field in ("quantity", "entry_price", "exit_price", "fee", "pnl")
        ),
    )


def downgrade() -> None:
    op.drop_table("backtest_closed_trade")
    op.drop_table("backtest_equity_sample")
