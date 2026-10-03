from __future__ import annotations

from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from src.core.orm_models import BacktestMonthlyReturn, BacktestPnlDistribution
from test_backtest_result_aggregates_pg import _insert, _insert_summary
from test_backtest_result_summary_pg import _seed_foreign_keys
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db


def test_aggregate_constraints_reject_invalid_rows_and_nonfinite_values(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _seed_foreign_keys(engine)
    monthly = BacktestMonthlyReturn.__table__
    distribution = BacktestPnlDistribution.__table__
    summary_id = _insert_summary(engine)
    monthly_values = {
        "summary_id": summary_id,
        "month": "2026-01",
        "return_pct": Decimal("-0.25"),
    }
    distribution_values = {
        "summary_id": summary_id,
        "sequence": 0,
        "lower": None,
        "upper": None,
        "count": 0,
    }
    _insert(engine, monthly, monthly_values)
    _insert(engine, distribution, distribution_values)

    for table, values in (
        (monthly, monthly_values),
        (distribution, distribution_values),
    ):
        with pytest.raises(IntegrityError):
            _insert(engine, table, values)
        orphan: dict[str, object] = {**values, "summary_id": -1}
        if table is monthly:
            orphan["month"] = "2026-02"
        else:
            orphan["sequence"] = 1
        with pytest.raises(IntegrityError):
            _insert(engine, table, orphan)

    for sequence, count in ((-1, 0), (1, -1)):
        invalid: dict[str, object] = {
            **distribution_values,
            "sequence": sequence,
            "count": count,
        }
        with pytest.raises(IntegrityError):
            _insert(engine, distribution, invalid)

    for table, values, fields, key in (
        (monthly, monthly_values, ("return_pct",), {"month": "2026-02"}),
        (
            distribution,
            distribution_values,
            ("lower", "upper"),
            {"sequence": 1},
        ),
    ):
        for field in fields:
            for special in ("NaN", "Infinity", "-Infinity"):
                invalid: dict[str, object] = {**values, **key}
                invalid[field] = sa.cast(sa.literal(special), sa.Numeric())
                with pytest.raises(IntegrityError):
                    _insert(engine, table, invalid)
    engine.dispose()
