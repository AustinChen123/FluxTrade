"""Add monthly returns and PnL distribution rows to backtest results."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "e83d14a79b2c"
down_revision: Union[str, Sequence[str], None] = "b21f70a3c9d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_SPECIAL_NUMERIC_VALUES = "('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)"


def upgrade() -> None:
    op.create_table(
        "backtest_monthly_return",
        sa.Column("summary_id", sa.Integer(), nullable=False),
        sa.Column("month", sa.String(length=7), nullable=False),
        sa.Column("return_pct", sa.Numeric(), nullable=False),
        sa.ForeignKeyConstraint(
            ["summary_id"],
            ["backtest_result_summary.id"],
            name="fk_backtest_monthly_return_summary",
        ),
        sa.PrimaryKeyConstraint("summary_id", "month"),
        sa.CheckConstraint(
            "return_pct NOT IN " + _SPECIAL_NUMERIC_VALUES,
            name="ck_backtest_monthly_return_finite",
        ),
    )
    op.create_table(
        "backtest_pnl_distribution",
        sa.Column("summary_id", sa.Integer(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("lower", sa.Numeric(), nullable=True),
        sa.Column("upper", sa.Numeric(), nullable=True),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["summary_id"],
            ["backtest_result_summary.id"],
            name="fk_backtest_pnl_distribution_summary",
        ),
        sa.PrimaryKeyConstraint("summary_id", "sequence"),
        sa.CheckConstraint(
            "sequence >= 0", name="ck_backtest_pnl_distribution_sequence"
        ),
        sa.CheckConstraint("count >= 0", name="ck_backtest_pnl_distribution_count"),
        sa.CheckConstraint(
            "lower IS NULL OR lower NOT IN " + _SPECIAL_NUMERIC_VALUES,
            name="ck_backtest_pnl_distribution_lower_finite",
        ),
        sa.CheckConstraint(
            "upper IS NULL OR upper NOT IN " + _SPECIAL_NUMERIC_VALUES,
            name="ck_backtest_pnl_distribution_upper_finite",
        ),
    )


def downgrade() -> None:
    op.drop_table("backtest_pnl_distribution")
    op.drop_table("backtest_monthly_return")
