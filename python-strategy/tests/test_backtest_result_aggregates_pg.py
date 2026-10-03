from __future__ import annotations

from decimal import Decimal

import pytest
import sqlalchemy as sa

from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
)
from test_backtest_result_children_pg import (
    _equity_row,
    _insert,
    _insert_legacy_summary,
    _insert_summary,
    _trade_row,
)
from test_backtest_result_summary_pg import _formal_row, _seed_foreign_keys
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db
from test_migrations import _downgrade

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db


def test_aggregate_rows_roundtrip_and_downgrade_preserves_prior_results(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _seed_foreign_keys(engine)
    monthly = BacktestMonthlyReturn.__table__
    distribution = BacktestPnlDistribution.__table__
    try:
        inspector = sa.inspect(engine)
        for name, key in (
            ("backtest_monthly_return", "month"),
            ("backtest_pnl_distribution", "sequence"),
        ):
            columns = {column["name"]: column for column in inspector.get_columns(name)}
            assert inspector.get_pk_constraint(name)["constrained_columns"] == [
                "summary_id",
                key,
            ]
            assert inspector.get_foreign_keys(name)[0]["referred_table"] == (
                "backtest_result_summary"
            )
            assert inspector.get_foreign_keys(name)[0]["referred_columns"] == ["id"]
            assert isinstance(columns["summary_id"]["type"], sa.Integer)
            assert all(
                not column["nullable"]
                for field, column in columns.items()
                if field not in {"lower", "upper"}
            )
        assert isinstance(monthly.c.month.type, sa.String)
        assert isinstance(monthly.c.return_pct.type, sa.Numeric)
        assert isinstance(distribution.c.sequence.type, sa.Integer)
        assert isinstance(distribution.c.count.type, sa.Integer)
        assert isinstance(distribution.c.lower.type, sa.Numeric)
        assert isinstance(distribution.c.upper.type, sa.Numeric)

        summary_id = _insert_summary(engine)
        legacy_id = _insert_legacy_summary(engine)
        equity = _equity_row(summary_id)
        trade = _trade_row(summary_id)
        _insert(engine, BacktestEquitySample.__table__, equity)
        _insert(engine, BacktestClosedTrade.__table__, trade)
        monthly_rows = [
            {
                "summary_id": summary_id,
                "month": "2026-01",
                "return_pct": Decimal("-0.1234567890123456789012345678901234"),
            },
            {
                "summary_id": summary_id,
                "month": "2026-02",
                "return_pct": Decimal("0.9876543210987654321098765432109876"),
            },
        ]
        distribution_rows = [
            {
                "summary_id": summary_id,
                "sequence": 0,
                "lower": Decimal("1.1234567890123456789012345678901"),
                "upper": None,
                "count": 0,
            },
            {
                "summary_id": summary_id,
                "sequence": 1,
                "lower": None,
                "upper": Decimal("-2.9876543210987654321098765432109"),
                "count": 2,
            },
            {
                "summary_id": summary_id,
                "sequence": 2,
                "lower": None,
                "upper": None,
                "count": 0,
            },
        ]
        with engine.begin() as conn:
            conn.execute(sa.insert(monthly), monthly_rows)
            conn.execute(sa.insert(distribution), distribution_rows)
        with engine.connect() as conn:
            saved_monthly = (
                conn.execute(
                    sa.select(monthly)
                    .where(monthly.c.summary_id == summary_id)
                    .order_by(monthly.c.month)
                )
                .mappings()
                .all()
            )
            saved_distribution = (
                conn.execute(
                    sa.select(distribution)
                    .where(distribution.c.summary_id == summary_id)
                    .order_by(distribution.c.sequence)
                )
                .mappings()
                .all()
            )
        for actual, expected in zip(saved_monthly, monthly_rows, strict=True):
            assert actual["month"] == expected["month"]
            assert actual["return_pct"] == expected["return_pct"]
            assert isinstance(actual["return_pct"], Decimal)
        for actual, expected in zip(saved_distribution, distribution_rows, strict=True):
            assert dict(actual) == expected
            for field in ("lower", "upper"):
                if expected[field] is not None:
                    assert isinstance(actual[field], Decimal)

        engine.dispose()
        _downgrade(fresh_pg_db, "b21f70a3c9d4")
        engine = sa.create_engine(_target_url(fresh_pg_db))
        tables = set(sa.inspect(engine).get_table_names())
        assert "backtest_monthly_return" not in tables
        assert "backtest_pnl_distribution" not in tables
        assert {"backtest_equity_sample", "backtest_closed_trade"} <= tables
        engine.dispose()
        _upgrade(fresh_pg_db, "head")
        engine = sa.create_engine(_target_url(fresh_pg_db))
        with engine.connect() as conn:
            retained = conn.execute(
                sa.text(
                    "SELECT id, total_pnl FROM backtest_result_summary "
                    "WHERE id IN (:formal, :legacy) ORDER BY id"
                ),
                {"formal": summary_id, "legacy": legacy_id},
            ).all()
            retained_equity = (
                conn.execute(
                    sa.select(BacktestEquitySample.__table__).where(
                        BacktestEquitySample.summary_id == summary_id
                    )
                )
                .mappings()
                .one()
            )
            retained_trade = (
                conn.execute(
                    sa.select(BacktestClosedTrade.__table__).where(
                        BacktestClosedTrade.summary_id == summary_id
                    )
                )
                .mappings()
                .one()
            )
        assert retained == [
            (summary_id, _formal_row()["total_pnl"]),
            (legacy_id, Decimal("7.25")),
        ]
        assert dict(retained_equity) == equity
        assert dict(retained_trade) == trade
        with engine.connect() as conn:
            assert (
                conn.execute(
                    sa.select(sa.func.count())
                    .select_from(monthly)
                    .where(monthly.c.summary_id == summary_id)
                ).scalar_one()
                == 0
            )
            assert (
                conn.execute(
                    sa.select(sa.func.count())
                    .select_from(distribution)
                    .where(distribution.c.summary_id == summary_id)
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()
