"""Invariant tests for matcher balance PnL and analytics recomputation."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.core.adapters.simulated import SimulatedAdapter
from src.core.analytics import _build_closed_trades
from src.core.models import Candlestick, Trade
from src.core.product_registry import FeeModel, InstrumentSpec


PRODUCT = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "pnl_invariant_strategy"
TF = "15m"
SATOSHI = Decimal("0.00000001")


def _candle(ts: int, price: Decimal) -> Candlestick:
    return Candlestick(
        product_id=PRODUCT,
        timeframe=TF,
        timestamp=ts,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("100"),
    )


def _fill_market(
    adapter,
    order_factory,
    *,
    side: str,
    quantity: str,
    price: str,
    ts: int,
) -> tuple[Trade, Decimal]:
    order = order_factory(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        order_type="market",
        side=side,
        quantity=Decimal(quantity),
        timestamp=ts,
    )

    adapter.place_order(order)
    fills = adapter.on_market_data(_candle(ts + 1, Decimal(price)))

    assert len(fills) == 1
    fill = fills[0]
    assert fill["order"].id == order.id

    trade = Trade(
        id=f"fill-{ts}",
        product_id=PRODUCT,
        price=fill["price"],
        quantity=fill["quantity"],
        side=side,
        timestamp=ts,
    )
    return trade, fill["fee"]


def test_matcher_balance_delta_matches_closed_trade_pnl_minus_fees(order_factory) -> None:
    adapter = SimulatedAdapter(Decimal("100000"), taker_fee=Decimal("0.001"))
    initial_balance = adapter.get_balance()

    trades_and_fees = [
        _fill_market(
            adapter,
            order_factory,
            side="buy",
            quantity="0.2",
            price="50000",
            ts=1,
        ),
        _fill_market(
            adapter,
            order_factory,
            side="buy",
            quantity="0.1",
            price="51000",
            ts=2,
        ),
        _fill_market(
            adapter,
            order_factory,
            side="sell",
            quantity="0.15",
            price="52000",
            ts=3,
        ),
        _fill_market(
            adapter,
            order_factory,
            side="sell",
            quantity="0.15",
            price="53000",
            ts=4,
        ),
    ]

    trades = [trade for trade, _ in trades_and_fees]
    total_fees = sum(fee for _, fee in trades_and_fees)
    closed_trades, _, _, recomputed_pnl = _build_closed_trades(trades)

    assert len(closed_trades) == 2
    assert adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID) is None

    matcher_balance_delta = adapter.get_balance() - initial_balance
    expected_balance_delta = recomputed_pnl - total_fees

    assert abs(matcher_balance_delta - expected_balance_delta) <= SATOSHI


def test_instrument_multiplier_matches_rust_and_python_pnl(order_factory) -> None:
    spec = InstrumentSpec(
        product_id=PRODUCT,
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
        price_tick=Decimal("0.25"),
    )
    adapter = SimulatedAdapter(Decimal("100000"), instrument_spec=spec)
    initial_balance = adapter.get_balance()

    entry_trade = _fill_market(
        adapter,
        order_factory,
        side="buy",
        quantity="1",
        price="100",
        ts=1,
    )[0]
    context = adapter.get_strategy_context(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        timestamp=2,
        initial_balance=initial_balance,
        mark_price=Decimal("110"),
    )

    assert context.position is not None
    assert context.position.notional == Decimal("220")
    assert context.position.unrealized_pnl == Decimal("20")
    assert context.total_equity == context.available_cash + Decimal("20")

    exit_trade = _fill_market(
        adapter,
        order_factory,
        side="sell",
        quantity="1",
        price="110",
        ts=2,
    )[0]
    trades = [entry_trade, exit_trade]
    closed_trades, _, _, analytics_pnl = _build_closed_trades(
        trades,
        contract_multiplier=spec.multiplier,
    )

    assert adapter.get_balance() - initial_balance == Decimal("20")
    assert analytics_pnl == Decimal("20")
    assert closed_trades[0].pnl == Decimal("20")


@pytest.mark.parametrize(
    ("fee_model", "fee", "expected_net"),
    [
        (FeeModel.PERCENTAGE_NOTIONAL, Decimal("0.01"), Decimal("15.8")),
        (FeeModel.PER_CONTRACT, Decimal("1.5"), Decimal("17")),
    ],
)
def test_fee_model_matches_rust_balance_and_python_analytics(
    order_factory,
    fee_model,
    fee,
    expected_net,
) -> None:
    spec = InstrumentSpec(
        product_id=PRODUCT,
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
        fee_model=fee_model,
    )
    adapter = SimulatedAdapter(
        Decimal("100000"),
        taker_fee=fee,
        instrument_spec=spec,
    )
    initial_balance = adapter.get_balance()
    entry, entry_fee = _fill_market(
        adapter,
        order_factory,
        side="buy",
        quantity="1",
        price="100",
        ts=1,
    )
    exit_trade, exit_fee = _fill_market(
        adapter,
        order_factory,
        side="sell",
        quantity="1",
        price="110",
        ts=2,
    )
    trades = [
        SimpleNamespace(**entry.model_dump(), fee=entry_fee),
        SimpleNamespace(**exit_trade.model_dump(), fee=exit_fee),
    ]

    _, _, _, analytics_pnl = _build_closed_trades(
        trades,
        contract_multiplier=spec.multiplier,
    )

    assert adapter.get_balance() - initial_balance == expected_net
    assert analytics_pnl == expected_net
