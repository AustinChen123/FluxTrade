from decimal import Decimal

import pytest

from src.core.backtest.equity import portfolio_equity
from src.core.models import Position, PositionSide


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"


class _Adapter:
    def __init__(self, positions, *, supports_strategy_positions=True):
        self.positions = positions
        self.supports_strategy_positions = supports_strategy_positions

    def get_balance(self, asset="USDT"):
        return Decimal("1000")

    def get_position(self, product_id, strategy_id=None):
        assert product_id == PRODUCT_ID
        return self.positions.get(strategy_id)


def _position(strategy_id: str, side: PositionSide, entry_price: str) -> Position:
    return Position(
        strategy_id=strategy_id,
        product_id=PRODUCT_ID,
        side=side,
        quantity=Decimal("1"),
        entry_price=Decimal(entry_price),
        unrealized_pnl=Decimal("0"),
    )


@pytest.mark.parametrize(
    ("positions", "strategy_ids", "expected"),
    [
        ({}, ["a"], Decimal("1000")),
        (
            {"a": _position("a", PositionSide.LONG, "100")},
            ["a"],
            Decimal("1020"),
        ),
        (
            {"a": _position("a", PositionSide.SHORT, "120")},
            ["a"],
            Decimal("1020"),
        ),
        (
            {
                "a": _position("a", PositionSide.LONG, "100"),
                "b": _position("b", PositionSide.LONG, "105"),
            },
            ["a", "b"],
            Decimal("1030"),
        ),
        (
            {
                "a": _position("a", PositionSide.LONG, "100"),
                "b": _position("b", PositionSide.SHORT, "100"),
            },
            ["a", "b"],
            Decimal("1000"),
        ),
    ],
    ids=("flat", "long", "short", "two-long", "long-short"),
)
def test_portfolio_equity_state_matrix(positions, strategy_ids, expected):
    adapter = _Adapter(positions)

    assert portfolio_equity(
        adapter,
        strategy_ids=strategy_ids,
        product_id=PRODUCT_ID,
        mark_price=Decimal("110"),
        contract_multiplier=Decimal("2"),
    ) == expected


def test_portfolio_equity_rejects_unscoped_multi_strategy_adapter():
    adapter = _Adapter({}, supports_strategy_positions=False)

    with pytest.raises(
        RuntimeError,
        match="requires strategy-scoped positions",
    ):
        portfolio_equity(
            adapter,
            strategy_ids=["a", "b"],
            product_id=PRODUCT_ID,
            mark_price=Decimal("110"),
            contract_multiplier=Decimal("2"),
        )


def test_portfolio_equity_allows_one_unscoped_strategy():
    position = _position("a", PositionSide.LONG, "100")
    adapter = _Adapter(
        {None: position},
        supports_strategy_positions=False,
    )

    assert portfolio_equity(
        adapter,
        strategy_ids=["a"],
        product_id=PRODUCT_ID,
        mark_price=Decimal("110"),
        contract_multiplier=Decimal("2"),
    ) == Decimal("1020")
