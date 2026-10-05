from __future__ import annotations

from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from src.core.orm_models import (
    BacktestPnlDistribution,
    BacktestMonthlyReturn,
    BacktestResultSummary,
)


def test_aggregate_metadata_allows_contract_values_and_rejects_keys() -> None:
    engine = sa.create_engine("sqlite://")
    summary = BacktestResultSummary.__table__
    monthly = BacktestMonthlyReturn.__table__
    distribution = BacktestPnlDistribution.__table__
    summary.create(engine)
    monthly.create(engine)
    distribution.create(engine)

    for table, order_key in ((monthly, "month"), (distribution, "sequence")):
        assert table.primary_key.columns.keys() == ["summary_id", order_key]
        assert table.c.summary_id.type._type_affinity is sa.Integer
        assert all(
            foreign_key.column.table.name == summary.name
            for foreign_key in table.c.summary_id.foreign_keys
        )
    assert all(not monthly.c[field].nullable for field in monthly.c.keys())
    assert distribution.c.lower.nullable and distribution.c.upper.nullable
    assert all(
        not distribution.c[field].nullable
        for field in ("summary_id", "sequence", "count")
    )
    assert isinstance(monthly.c.month.type, sa.String)
    assert isinstance(monthly.c.return_pct.type, sa.Numeric)
    assert isinstance(distribution.c.sequence.type, sa.Integer)
    assert isinstance(distribution.c.count.type, sa.Integer)
    assert isinstance(distribution.c.lower.type, sa.Numeric)
    assert isinstance(distribution.c.upper.type, sa.Numeric)

    with engine.begin() as conn:
        summary_id = conn.execute(
            sa.insert(summary)
            .values(strategy_id="s", start_time=1, end_time=2, total_pnl=Decimal("0"))
            .returning(summary.c.id)
        ).scalar_one()
        conn.execute(
            sa.insert(monthly),
            [
                {
                    "summary_id": summary_id,
                    "month": "2026-01",
                    "return_pct": Decimal("-0.25"),
                },
                {
                    "summary_id": summary_id,
                    "month": "2026-02",
                    "return_pct": Decimal("0.5"),
                },
            ],
        )
        conn.execute(
            sa.insert(distribution),
            [
                {
                    "summary_id": summary_id,
                    "sequence": 0,
                    "lower": Decimal("10"),
                    "upper": None,
                    "count": 0,
                },
                {
                    "summary_id": summary_id,
                    "sequence": 1,
                    "lower": None,
                    "upper": Decimal("-10"),
                    "count": 1,
                },
                {
                    "summary_id": summary_id,
                    "sequence": 2,
                    "lower": None,
                    "upper": None,
                    "count": 0,
                },
            ],
        )

    for table, duplicate_key in (
        (
            monthly,
            {
                "summary_id": summary_id,
                "month": "2026-01",
                "return_pct": Decimal("0"),
            },
        ),
        (
            distribution,
            {
                "summary_id": summary_id,
                "sequence": 0,
                "lower": None,
                "upper": None,
                "count": 1,
            },
        ),
    ):
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(sa.insert(table).values(**duplicate_key))

    invalid_distribution_rows = (
        {"sequence": -1, "lower": None, "upper": None, "count": 0},
        {"sequence": 3, "lower": None, "upper": None, "count": -1},
    )
    for invalid in invalid_distribution_rows:
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    sa.insert(distribution).values(summary_id=summary_id, **invalid)
                )
    engine.dispose()
