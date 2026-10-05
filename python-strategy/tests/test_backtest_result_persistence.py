from dataclasses import FrozenInstanceError
from decimal import Decimal
from typing import cast

import pytest

from src.core.analytics import ClosedTrade
from src.core.backtest_result_persistence import (
    ClosedTradeSnapshot,
    FullBacktestOutcome,
)
from src.core.models import PositionSide


def _result() -> dict[str, object]:
    return {
        "total_pnl": Decimal("-2.3400"),
        "mark_to_market_pnl": Decimal("1.2300"),
        "max_drawdown": Decimal("0.50"),
        "trade_sharpe": Decimal("0.25"),
        "sortino_ratio": Decimal("0.75"),
        "calmar_ratio": Decimal("0.125"),
        "closed_trades": [
            ClosedTrade(
                entry_time=20,
                exit_time=30,
                entry_price=Decimal("101.00"),
                exit_price=Decimal("102.00"),
                side=PositionSide.LONG,
                quantity=Decimal("2.00"),
                pnl=Decimal("1.80"),
                fee=Decimal("0.20"),
            )
        ],
    }


def test_completed_outcome_copies_final_result_and_equity_values() -> None:
    result = _result()
    samples = [(10, Decimal("100.00")), (20, Decimal("101.00"))]

    outcome = FullBacktestOutcome.from_completed_result(
        initial_balance=Decimal("100"),
        result=result,
        equity_samples=samples,
    )

    source_trades = cast(list[object], result["closed_trades"])
    trade = cast(ClosedTrade, source_trades[0])
    assert outcome.initial_balance == Decimal("100")
    assert outcome.total_pnl == Decimal("-2.3400")
    assert outcome.mark_to_market_pnl == Decimal("1.2300")
    assert outcome.max_drawdown == Decimal("0.50")
    assert outcome.trade_sharpe == Decimal("0.25")
    assert outcome.sortino_ratio == Decimal("0.75")
    assert outcome.calmar_ratio == Decimal("0.125")
    assert outcome.equity_samples == tuple(samples)
    assert outcome.closed_trades == (
        ClosedTradeSnapshot(
            entry_time=20,
            exit_time=30,
            entry_price=Decimal("101.00"),
            exit_price=Decimal("102.00"),
            side=PositionSide.LONG,
            quantity=Decimal("2.00"),
            pnl=Decimal("1.80"),
            fee=Decimal("0.20"),
        ),
    )
    with pytest.raises(FrozenInstanceError):
        setattr(outcome, "initial_balance", Decimal("0"))

    samples[0] = (10, Decimal("999"))
    trade.pnl = Decimal("999")
    assert outcome.equity_samples[0] == (10, Decimal("100.00"))
    assert outcome.closed_trades[0].pnl == Decimal("1.80")
    assert isinstance(outcome.closed_trades, tuple)
    assert isinstance(outcome.equity_samples, tuple)


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("total_pnl", None, ValueError),
        ("total_pnl", 1.0, TypeError),
        ("total_pnl", Decimal("NaN"), ValueError),
        ("mark_to_market_pnl", None, ValueError),
        ("mark_to_market_pnl", 1.0, TypeError),
        ("max_drawdown", Decimal("NaN"), ValueError),
        ("trade_sharpe", "0.25", TypeError),
        ("sortino_ratio", Decimal("Infinity"), ValueError),
        ("calmar_ratio", Decimal("-Infinity"), ValueError),
    ],
)
def test_completed_outcome_rejects_missing_wrong_or_nonfinite_metrics(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    result = _result()
    if value is None:
        result.pop(field)
    else:
        result[field] = value

    with pytest.raises(error):
        FullBacktestOutcome.from_completed_result(
            initial_balance=Decimal("100"),
            result=result,
            equity_samples=[],
        )


def test_completed_outcome_rejects_non_decimal_balance_and_equity() -> None:
    result = _result()
    with pytest.raises(TypeError):
        FullBacktestOutcome.from_completed_result(
            initial_balance=cast(Decimal, 100.0),
            result=result,
            equity_samples=[],
        )
    with pytest.raises(TypeError):
        FullBacktestOutcome.from_completed_result(
            initial_balance=Decimal("100"),
            result=result,
            equity_samples=[(10, cast(Decimal, 100.0))],
        )

    with pytest.raises(ValueError):
        FullBacktestOutcome.from_completed_result(
            initial_balance=Decimal("100"),
            result=result,
            equity_samples=[(10, Decimal("NaN"))],
        )
    with pytest.raises(ValueError):
        FullBacktestOutcome.from_completed_result(
            initial_balance=Decimal("Infinity"),
            result=result,
            equity_samples=[],
        )


def test_completed_outcome_rejects_non_closed_trade_snapshot_input() -> None:
    result = _result()
    result["closed_trades"] = [{"pnl": Decimal("1")}]

    with pytest.raises(TypeError):
        FullBacktestOutcome.from_completed_result(
            initial_balance=Decimal("100"),
            result=result,
            equity_samples=[],
        )
