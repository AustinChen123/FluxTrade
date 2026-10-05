from __future__ import annotations

from decimal import Decimal
import pytest
import sqlalchemy as sa

from src.core.orm_models import BacktestClosedTrade, BacktestEquitySample

from test_backtest_result_summary_pg import _formal_row, _seed_foreign_keys
from test_migrations import (
    _downgrade,
    _target_url,
    _upgrade,
    fresh_pg_db as _fresh_pg_db,
)

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db


def _insert(engine: sa.Engine, table: sa.Table, values: dict[str, object]) -> None:
    with engine.begin() as conn:
        conn.execute(sa.insert(table).values(**values))


def _equity_row(summary_id: int, sequence: int = 0) -> dict[str, object]:
    return {
        "summary_id": summary_id,
        "sequence": sequence,
        "timestamp": 1_700_000_000_000 + sequence,
        "equity": Decimal("-12345678901234567890.123456789012345678901"),
        "drawdown": Decimal("0.1234567890123456789012345678901"),
    }


def _trade_row(summary_id: int, sequence: int = 0) -> dict[str, object]:
    return {
        "summary_id": summary_id,
        "sequence": sequence,
        "entry_time": 1_700_000_000_000,
        "exit_time": 1_700_000_060_000,
        "side": "SHORT",
        "quantity": Decimal("0.000000000000000000000000123456789"),
        "entry_price": Decimal("-10.12345678901234567890123456789"),
        "exit_price": Decimal("0"),
        "fee": Decimal("-0.000000000000000000123456789"),
        "pnl": Decimal("-12345678901234567890.123456789012345678901"),
    }


def test_child_rows_roundtrip_and_migration_preserves_summaries(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _seed_foreign_keys(engine)
    equity = BacktestEquitySample.__table__
    trade = BacktestClosedTrade.__table__
    try:
        inspector = sa.inspect(engine)
        for name in ("backtest_equity_sample", "backtest_closed_trade"):
            columns = {column["name"]: column for column in inspector.get_columns(name)}
            assert all(not column["nullable"] for column in columns.values())
            assert isinstance(columns["summary_id"]["type"], sa.Integer)
            assert inspector.get_pk_constraint(name)["constrained_columns"] == [
                "summary_id",
                "sequence",
            ]
            assert inspector.get_foreign_keys(name)[0]["referred_table"] == (
                "backtest_result_summary"
            )
            assert inspector.get_foreign_keys(name)[0]["referred_columns"] == ["id"]
            assert isinstance(columns["sequence"]["type"], sa.Integer)
            time_fields = (
                ("timestamp",)
                if name == "backtest_equity_sample"
                else (
                    "entry_time",
                    "exit_time",
                )
            )
            assert all(
                isinstance(columns[field]["type"], sa.BigInteger)
                for field in time_fields
            )
            numeric_fields = (
                ("equity", "drawdown")
                if name == "backtest_equity_sample"
                else ("quantity", "entry_price", "exit_price", "fee", "pnl")
            )
            assert all(
                isinstance(columns[field]["type"], sa.Numeric)
                for field in numeric_fields
            )

        summary_id = _insert_summary(engine)
        legacy_id = _insert_legacy_summary(engine)
        equity_values = _equity_row(summary_id)
        trade_values = _trade_row(summary_id)
        _insert(engine, equity, equity_values)
        _insert(engine, trade, trade_values)
        with engine.connect() as conn:
            saved_equity = (
                conn.execute(sa.select(equity).where(equity.c.summary_id == summary_id))
                .mappings()
                .one()
            )
            saved_trade = (
                conn.execute(sa.select(trade).where(trade.c.summary_id == summary_id))
                .mappings()
                .one()
            )
        for field in ("equity", "drawdown"):
            assert saved_equity[field] == equity_values[field]
            assert isinstance(saved_equity[field], Decimal)
        for field in ("quantity", "entry_price", "exit_price", "fee", "pnl"):
            assert saved_trade[field] == trade_values[field]
            assert isinstance(saved_trade[field], Decimal)

        engine.dispose()
        _downgrade(fresh_pg_db, "a17c93e24b60")
        engine = sa.create_engine(_target_url(fresh_pg_db))
        assert not {
            "backtest_equity_sample",
            "backtest_closed_trade",
        } & set(sa.inspect(engine).get_table_names())
        _upgrade(fresh_pg_db, "head")
        with engine.connect() as conn:
            rows = conn.execute(
                sa.text(
                    "SELECT id, total_pnl FROM backtest_result_summary "
                    "WHERE id IN (:formal, :legacy) ORDER BY id"
                ),
                {"formal": summary_id, "legacy": legacy_id},
            ).all()
        assert rows == [
            (summary_id, _formal_row()["total_pnl"]),
            (legacy_id, Decimal("7.25")),
        ]
    finally:
        engine.dispose()


def _insert_summary(engine: sa.Engine) -> int:
    from src.core.orm_models import BacktestResultSummary

    with engine.begin() as conn:
        return conn.execute(
            sa.insert(BacktestResultSummary.__table__)
            .values(**_formal_row())
            .returning(BacktestResultSummary.id)
        ).scalar_one()


def _insert_legacy_summary(engine: sa.Engine) -> int:
    from src.core.orm_models import BacktestResultSummary

    with engine.begin() as conn:
        return conn.execute(
            sa.insert(BacktestResultSummary.__table__)
            .values(
                strategy_id="summary-strategy",
                start_time=10,
                end_time=20,
                total_pnl=Decimal("7.25"),
            )
            .returning(BacktestResultSummary.id)
        ).scalar_one()
