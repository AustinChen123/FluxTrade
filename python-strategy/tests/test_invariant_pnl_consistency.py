"""Invariant tests for matcher balance PnL and analytics recomputation."""

from dataclasses import dataclass
from decimal import Decimal

import pytest

from src.core.adapters.simulated import SimulatedAdapter
from src.core.analytics import _build_closed_trades
from src.core.models import Candlestick, OrderSide, Trade
from src.core.product_registry import FeeModel, InstrumentSpec


PRODUCT = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "pnl_invariant_strategy"
TF = "15m"


@dataclass(frozen=True)
class _FeeBearingTrade:
    timestamp: int
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal


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
        side=OrderSide(side),
        timestamp=ts,
    )
    return trade, fill["fee"]


def test_matcher_balance_delta_matches_closed_trade_pnl_minus_fees(
    order_factory,
) -> None:
    adapter = SimulatedAdapter(Decimal("100000"), taker_fee=Decimal("0.001"))
    initial_balance = adapter.get_balance()

    trades: list[Trade] = []
    total_fees = Decimal("0")
    fills = [
        ("buy", "0.2", "50000", 1),
        ("buy", "0.1", "51000", 2),
        ("sell", "0.15", "52000", 3),
        ("sell", "0.15", "53000", 4),
    ]
    expected_net_by_fill = {3: Decimal("227.1"), 4: Decimal("619.15")}
    for side, quantity, price, timestamp in fills:
        trade, fee = _fill_market(
            adapter,
            order_factory,
            side=side,
            quantity=quantity,
            price=price,
            ts=timestamp,
        )
        trades.append(trade)
        total_fees += fee

        expected_net = expected_net_by_fill.get(timestamp)
        if expected_net is None:
            continue
        _, _, _, recomputed_pnl = _build_closed_trades(trades)
        matcher_balance_delta = adapter.get_balance() - initial_balance
        analytics_balance_delta = recomputed_pnl - total_fees
        assert matcher_balance_delta == expected_net
        assert analytics_balance_delta == expected_net

    closed_trades, _, _, _ = _build_closed_trades(trades)
    assert len(closed_trades) == 2
    assert adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID) is None


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
    assert spec.multiplier is not None
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
        _FeeBearingTrade(
            timestamp=entry.timestamp,
            side=entry.side.value,
            price=entry.price,
            quantity=entry.quantity,
            fee=entry_fee,
        ),
        _FeeBearingTrade(
            timestamp=exit_trade.timestamp,
            side=exit_trade.side.value,
            price=exit_trade.price,
            quantity=exit_trade.quantity,
            fee=exit_fee,
        ),
    ]

    assert spec.multiplier is not None
    _, _, _, analytics_pnl = _build_closed_trades(
        trades,
        contract_multiplier=spec.multiplier,
    )

    assert adapter.get_balance() - initial_balance == expected_net
    assert analytics_pnl == expected_net
