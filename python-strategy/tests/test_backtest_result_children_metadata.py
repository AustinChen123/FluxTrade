from __future__ import annotations

from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestResultSummary,
)


def test_child_metadata_and_sqlite_constraints() -> None:
    engine = sa.create_engine("sqlite://")
    summary = BacktestResultSummary.__table__
    equity = BacktestEquitySample.__table__
    trade = BacktestClosedTrade.__table__
    summary.create(engine)
    equity.create(engine)
    trade.create(engine)

    for table in (equity, trade):
        assert table.primary_key.columns.keys() == ["summary_id", "sequence"]
        assert all(not column.nullable for column in table.columns)
        assert isinstance(table.c.summary_id.type, sa.Integer)
        assert isinstance(table.c.sequence.type, sa.Integer)
    assert isinstance(equity.c.timestamp.type, sa.BigInteger)
    assert isinstance(equity.c.equity.type, sa.Numeric)
    assert isinstance(equity.c.drawdown.type, sa.Numeric)
    assert isinstance(trade.c.entry_time.type, sa.BigInteger)
    assert isinstance(trade.c.exit_time.type, sa.BigInteger)
    assert all(
        isinstance(trade.c[name].type, sa.Numeric)
        for name in ("quantity", "entry_price", "exit_price", "fee", "pnl")
    )

    with engine.begin() as conn:
        summary_id = conn.execute(
            sa.insert(summary)
            .values(strategy_id="s", start_time=1, end_time=2, total_pnl=Decimal("0"))
            .returning(summary.c.id)
        ).scalar_one()
        conn.execute(
            sa.insert(equity).values(
                summary_id=summary_id,
                sequence=0,
                timestamp=1_700_000_000_000,
                equity=Decimal("-1.25"),
                drawdown=Decimal("0"),
            )
        )
        conn.execute(
            sa.insert(trade).values(
                summary_id=summary_id,
                sequence=0,
                entry_time=1_700_000_000_000,
                exit_time=1_700_000_060_000,
                side="SHORT",
                quantity=Decimal("1.25"),
                entry_price=Decimal("10"),
                exit_price=Decimal("9"),
                fee=Decimal("-0.01"),
                pnl=Decimal("-1.26"),
            )
        )

    invalid_equity = (
        {"sequence": -1},
        {"drawdown": Decimal("-0.01")},
    )
    invalid_trade = (
        {"sequence": -1},
        {"side": "buy"},
        {"quantity": Decimal("0")},
    )
    for table, invalid_rows, valid_values in (
        (
            equity,
            invalid_equity,
            {"timestamp": 1, "equity": Decimal("1"), "drawdown": Decimal("0")},
        ),
        (
            trade,
            invalid_trade,
            {
                "entry_time": 1,
                "exit_time": 2,
                "side": "LONG",
                "quantity": Decimal("1"),
                "entry_price": Decimal("1"),
                "exit_price": Decimal("1"),
                "fee": Decimal("0"),
                "pnl": Decimal("0"),
            },
        ),
    ):
        for index, invalid in enumerate(invalid_rows, start=1):
            with pytest.raises(IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        sa.insert(table).values(
                            {
                                "summary_id": summary_id,
                                "sequence": index,
                                **valid_values,
                                **invalid,
                            }
                        )
                    )
    engine.dispose()
