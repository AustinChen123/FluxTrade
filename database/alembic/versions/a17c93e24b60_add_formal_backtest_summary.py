"""Add legacy-safe formal backtest summary fields."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "a17c93e24b60"
down_revision: Union[str, Sequence[str], None] = "d95a2b73e608"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_NUMERIC_FIELDS = (
    "initial_balance",
    "net_pnl",
    "return_pct",
    "max_drawdown",
    "sharpe",
    "sortino",
    "calmar",
)
_FORMAL_FIELDS = (
    "dataset_id",
    "subject_kind",
    "subject_id",
    "input_digest",
    "result_digest",
    "product_id",
    "timeframe",
    "currency",
    "completed_at",
    *_NUMERIC_FIELDS,
)
_SPECIAL_NUMERIC_VALUES = "('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)"


def upgrade() -> None:
    op.add_column(
        "backtest_result_summary",
        sa.Column("job_id", sa.String(), nullable=True),
    )
    op.add_column(
        "backtest_result_summary",
        sa.Column("dataset_id", sa.String(length=128), nullable=True),
    )
    for name, size in (
        ("subject_kind", None),
        ("subject_id", None),
        ("input_digest", None),
        ("result_digest", None),
        ("product_id", None),
        ("timeframe", 32),
        ("currency", None),
    ):
        op.add_column(
            "backtest_result_summary",
            sa.Column(
                name,
                sa.String(length=size) if size is not None else sa.String(),
                nullable=True,
            ),
        )
    op.add_column(
        "backtest_result_summary",
        sa.Column("completed_at", sa.BigInteger(), nullable=True),
    )
    for name in _NUMERIC_FIELDS:
        op.add_column(
            "backtest_result_summary",
            sa.Column(name, sa.Numeric(), nullable=True),
        )

    op.create_foreign_key(
        "fk_backtest_summary_dataset_id_research_dataset",
        "backtest_result_summary",
        "research_dataset",
        ["dataset_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_backtest_summary_product_id_product",
        "backtest_result_summary",
        "product",
        ["product_id"],
        ["id"],
    )
    op.create_unique_constraint(
        "uq_backtest_summary_job_id",
        "backtest_result_summary",
        ["job_id"],
    )
    null_fields = " AND ".join(f"{name} IS NULL" for name in _FORMAL_FIELDS)
    present_fields = " AND ".join(f"{name} IS NOT NULL" for name in _FORMAL_FIELDS)
    op.create_check_constraint(
        "ck_backtest_summary_formal_fields",
        "backtest_result_summary",
        f"((job_id IS NULL) AND {null_fields}) OR "
        f"((job_id IS NOT NULL) AND {present_fields})",
    )
    op.create_check_constraint(
        "ck_backtest_summary_subject_kind",
        "backtest_result_summary",
        "subject_kind IS NULL OR subject_kind = 'STRATEGY_ARTIFACT'",
    )
    op.create_check_constraint(
        "ck_backtest_summary_initial_balance_positive",
        "backtest_result_summary",
        "initial_balance IS NULL OR initial_balance > 0",
    )
    op.create_check_constraint(
        "ck_backtest_summary_max_drawdown_nonnegative",
        "backtest_result_summary",
        "max_drawdown IS NULL OR max_drawdown >= 0",
    )
    op.create_check_constraint(
        "ck_backtest_summary_total_pnl_finite_formal",
        "backtest_result_summary",
        "job_id IS NULL OR total_pnl NOT IN " + _SPECIAL_NUMERIC_VALUES,
    )
    for name in _NUMERIC_FIELDS:
        op.create_check_constraint(
            f"ck_backtest_summary_{name}_finite",
            "backtest_result_summary",
            f"{name} IS NULL OR {name} NOT IN {_SPECIAL_NUMERIC_VALUES}",
        )


def downgrade() -> None:
    for name in _NUMERIC_FIELDS:
        op.drop_constraint(
            f"ck_backtest_summary_{name}_finite",
            "backtest_result_summary",
            type_="check",
        )
    for name in (
        "total_pnl_finite_formal",
        "max_drawdown_nonnegative",
        "initial_balance_positive",
        "subject_kind",
        "formal_fields",
    ):
        op.drop_constraint(
            f"ck_backtest_summary_{name}",
            "backtest_result_summary",
            type_="check",
        )
    op.drop_constraint(
        "uq_backtest_summary_job_id",
        "backtest_result_summary",
        type_="unique",
    )
    op.drop_constraint(
        "fk_backtest_summary_product_id_product",
        "backtest_result_summary",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_backtest_summary_dataset_id_research_dataset",
        "backtest_result_summary",
        type_="foreignkey",
    )
    for name in reversed(
        (
            *_NUMERIC_FIELDS,
            "completed_at",
            "currency",
            "timeframe",
            "product_id",
            "result_digest",
            "input_digest",
            "subject_id",
            "subject_kind",
            "dataset_id",
            "job_id",
        )
    ):
        op.drop_column("backtest_result_summary", name)
