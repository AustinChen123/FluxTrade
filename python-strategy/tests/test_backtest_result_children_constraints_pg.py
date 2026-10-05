from __future__ import annotations

from decimal import Decimal
from typing import cast

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from test_backtest_result_children_pg import (
    _equity_row,
    _insert,
    _insert_summary,
    _trade_row,
)
from test_backtest_result_summary_pg import _seed_foreign_keys
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db
from src.core.orm_models import BacktestClosedTrade, BacktestEquitySample

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db


def test_child_constraints_reject_invalid_rows_and_finite_values(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _seed_foreign_keys(engine)
    equity = BacktestEquitySample.__table__
    trade = BacktestClosedTrade.__table__
    summary_id = _insert_summary(engine)

    equity_values = _equity_row(summary_id)
    trade_values = _trade_row(summary_id)
    _insert(engine, equity, equity_values)
    _insert(engine, trade, trade_values)

    for table, values in ((equity, equity_values), (trade, trade_values)):
        with pytest.raises(IntegrityError):
            _insert(engine, table, values)
        orphan = dict(
            values,
            summary_id=-1,
            sequence=cast(int, values["sequence"]) + 1,
        )
        with pytest.raises(IntegrityError):
            _insert(engine, table, orphan)

    for invalid in (
        dict(equity_values, sequence=-1),
        dict(equity_values, sequence=1, drawdown=Decimal("-0.01")),
    ):
        with pytest.raises(IntegrityError):
            _insert(engine, equity, invalid)
    for invalid in (
        dict(trade_values, sequence=-1),
        dict(trade_values, sequence=1, side="buy"),
        dict(trade_values, sequence=1, quantity=Decimal("0")),
    ):
        with pytest.raises(IntegrityError):
            _insert(engine, trade, invalid)

    finite_fields = (
        (equity, equity_values, ("equity", "drawdown")),
        (trade, trade_values, ("quantity", "entry_price", "exit_price", "fee", "pnl")),
    )
    for table, values, fields in finite_fields:
        for field in fields:
            for special in ("NaN", "Infinity", "-Infinity"):
                invalid = dict(values, sequence=cast(int, values["sequence"]) + 1)
                invalid[field] = sa.cast(sa.literal(special), sa.Numeric())
                with pytest.raises(IntegrityError):
                    _insert(engine, table, invalid)
    engine.dispose()
