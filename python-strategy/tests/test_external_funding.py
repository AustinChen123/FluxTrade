from decimal import Decimal

import pytest

from src.core.adapters.simulated import SimulatedAdapter
from src.core.backtest.external_funding import (
    ExternalFundingEvent,
    ExternalFundingTimeline,
)
from src.core.models import Candlestick
from src.core.product_registry import InstrumentSpec


def _spot_spec() -> InstrumentSpec:
    return InstrumentSpec(
        product_id="BINANCE:BTCUSDT-SPOT",
        exchange="binance",
        symbol="BTC/USDT",
        base="BTC",
        quote="USDT",
        quantity_step=Decimal("0.000001"),
        price_tick=Decimal("0.01"),
        min_notional=Decimal("1"),
    )


def _event(
    event_id: str,
    *,
    amount: str = "10",
    available_at: int = 100,
    account_id: str = "research-account",
    asset: str = "USDT",
    source: str = "test-schedule",
) -> ExternalFundingEvent:
    return ExternalFundingEvent(
        event_id=event_id,
        account_id=account_id,
        asset=asset,
        amount=Decimal(amount),
        available_at=available_at,
        source=source,
    )


def _flat_candle(timestamp: int, price: str = "50000") -> Candlestick:
    value = Decimal(price)
    return Candlestick(
        product_id="BINANCE:BTCUSDT-SPOT",
        timeframe="1m",
        timestamp=timestamp,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=Decimal("10"),
    )


@pytest.mark.parametrize(
    ("changes", "error_type", "message"),
    [
        ({"event_id": ""}, ValueError, "event_id must be non-empty"),
        ({"account_id": " account "}, ValueError, "account_id must be non-empty"),
        ({"asset": ""}, ValueError, "asset must be non-empty"),
        ({"source": ""}, ValueError, "source must be non-empty"),
        ({"amount": 1}, TypeError, "amount must be Decimal"),
        ({"amount": Decimal("0")}, ValueError, "finite and positive"),
        ({"amount": Decimal("-1")}, ValueError, "finite and positive"),
        ({"amount": Decimal("NaN")}, ValueError, "finite and positive"),
        ({"amount": Decimal("Infinity")}, ValueError, "finite and positive"),
        ({"available_at": -1}, ValueError, "non-negative integer"),
        ({"available_at": True}, ValueError, "non-negative integer"),
    ],
)
def test_external_funding_event_validation(changes, error_type, message):
    values = {
        "event_id": "event-1",
        "account_id": "research-account",
        "asset": "USDT",
        "amount": Decimal("10"),
        "available_at": 100,
        "source": "test-schedule",
    }
    values.update(changes)

    with pytest.raises(error_type, match=message):
        ExternalFundingEvent(**values)


def test_timeline_rejects_ambiguous_or_mismatched_contracts():
    with pytest.raises(ValueError, match="ordered by available_at"):
        ExternalFundingTimeline(
            [_event("later", available_at=200), _event("earlier", available_at=100)],
            account_id="research-account",
            quote_asset="USDT",
            start_time=0,
        )

    with pytest.raises(ValueError, match="event_id conflict"):
        ExternalFundingTimeline(
            [_event("same", amount="10"), _event("same", amount="20")],
            account_id="research-account",
            quote_asset="USDT",
            start_time=0,
        )

    with pytest.raises(ValueError, match="account mismatch"):
        ExternalFundingTimeline(
            [_event("wrong-account", account_id="other")],
            account_id="research-account",
            quote_asset="USDT",
            start_time=0,
        )

    with pytest.raises(ValueError, match="configured quote asset only"):
        ExternalFundingTimeline(
            [_event("wrong-asset", asset="BTC")],
            account_id="research-account",
            quote_asset="USDT",
            start_time=0,
        )

    with pytest.raises(ValueError, match="predates replay start"):
        ExternalFundingTimeline(
            [_event("too-early", available_at=99)],
            account_id="research-account",
            quote_asset="USDT",
            start_time=100,
        )


def test_timeline_applies_once_in_canonical_order_and_preserves_reservation(
    order_factory,
):
    event_2 = _event("event-2", amount="30", available_at=100)
    event_1 = _event("event-1", amount="20", available_at=100)
    pending = _event("event-3", amount="40", available_at=300)
    timeline = ExternalFundingTimeline(
        [event_2, event_1, event_1, pending],
        account_id="research-account",
        quote_asset="USDT",
        start_time=0,
    )
    adapter = SimulatedAdapter(
        Decimal("100"),
        instrument_spec=_spot_spec(),
    )
    order = order_factory(
        product_id="BINANCE:BTCUSDT-SPOT",
        order_type="limit",
        side="buy",
        quantity=Decimal("1"),
        price=Decimal("60"),
    )
    adapter.place_order(order)

    assert (
        timeline.apply_due(
            adapter,
            timestamp=99,
            mark_price=Decimal("50"),
        )
        == ()
    )
    applications = timeline.apply_due(
        adapter,
        timestamp=200,
        mark_price=Decimal("50"),
    )

    assert [application.event.event_id for application in applications] == [
        "event-1",
        "event-2",
    ]
    assert [(item.pre_equity, item.post_equity) for item in applications] == [
        (Decimal("100"), Decimal("120")),
        (Decimal("120"), Decimal("150")),
    ]
    assert {application.applied_at for application in applications} == {200}
    assert adapter.get_asset_balance("USDT", "total") == Decimal("150")
    assert adapter.get_asset_balance("USDT", "reserved") == Decimal("60")
    assert adapter.get_asset_balance("USDT", "available") == Decimal("90")
    assert (
        timeline.apply_due(
            adapter,
            timestamp=200,
            mark_price=Decimal("50"),
        )
        == ()
    )
    with pytest.raises(ValueError, match="timestamps must be non-decreasing"):
        timeline.apply_due(
            adapter,
            timestamp=199,
            mark_price=Decimal("50"),
        )

    checkpoint = timeline.checkpoint()
    assert checkpoint.account_id == "research-account"
    assert len(checkpoint.contract_hash) == 64
    assert checkpoint.applied_event_ids == ("event-1", "event-2")
    assert checkpoint.pending_event_ids == ("event-3",)


def test_timeline_requires_cash_spot_adapter():
    timeline = ExternalFundingTimeline(
        [_event("event-1")],
        account_id="research-account",
        quote_asset="USDT",
        start_time=0,
    )
    adapter = SimulatedAdapter(Decimal("100"))

    with pytest.raises(ValueError, match="requires cash_spot settlement"):
        timeline.apply_due(
            adapter,
            timestamp=100,
            mark_price=Decimal("50"),
        )


def test_adapter_matches_existing_order_before_same_timestamp_funding(
    order_factory,
):
    timeline = ExternalFundingTimeline(
        [_event("event-1", amount="60", available_at=100)],
        account_id="research-account",
        quote_asset="USDT",
        start_time=0,
    )
    adapter = SimulatedAdapter(
        Decimal("40"),
        instrument_spec=_spot_spec(),
        external_funding_timeline=timeline,
    )
    existing_order = order_factory(
        product_id="BINANCE:BTCUSDT-SPOT",
        order_type="market",
        side="buy",
        quantity=Decimal("1"),
        price=None,
    )
    existing_order.min_notional_reference_price = Decimal("50")
    adapter.place_order(existing_order)

    fills = adapter.on_market_data(
        Candlestick(
            product_id="BINANCE:BTCUSDT-SPOT",
            timeframe="1m",
            timestamp=100,
            open=Decimal("50"),
            high=Decimal("50"),
            low=Decimal("50"),
            close=Decimal("50"),
            volume=Decimal("10"),
        )
    )

    assert fills == []
    assert len(adapter.drain_order_rejections()) == 1
    assert adapter.get_asset_balance("BTC", "total") == Decimal("0")
    assert adapter.get_asset_balance("USDT", "total") == Decimal("100")
    assert timeline.checkpoint().applied_event_ids == ("event-1",)


@pytest.mark.parametrize("cash_state", ["sufficient", "insufficient"])
@pytest.mark.parametrize("prior_state", ["none", "pending", "partial"])
@pytest.mark.parametrize("with_funding", [False, True])
@pytest.mark.parametrize("terminal_action", ["fill", "reject", "cancel"])
def test_spot_funding_state_matrix_preserves_matcher_owned_assets(
    order_factory,
    cash_state,
    prior_state,
    with_funding,
    terminal_action,
):
    """Cover account states without inventing volume-based partial fills.

    ``partial`` is the documented preflight case: a filled buy followed by a
    smaller filled sell, leaving partially reduced inventory. The simulated
    matcher remains atomic-whole-order, which is recorded in run provenance.
    """
    initial_quote = Decimal("100") if cash_state == "sufficient" else Decimal("20")
    funding = Decimal("30") if with_funding else Decimal("0")
    timeline = ExternalFundingTimeline(
        [_event("matrix-funding", amount="30", available_at=30)]
        if with_funding
        else [],
        account_id="research-account",
        quote_asset="USDT",
        start_time=0,
    )
    adapter = SimulatedAdapter(initial_quote, instrument_spec=_spot_spec())

    if prior_state == "pending":
        pending = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="limit",
            side="buy",
            quantity=Decimal("0.0002"),
            price=Decimal("40000"),
        )
        adapter.place_order(pending)
    elif prior_state == "partial":
        buy = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="market",
            side="buy",
            quantity=Decimal("0.0004"),
            price=None,
        )
        buy.min_notional_reference_price = Decimal("50000")
        adapter.place_order(buy)
        assert len(adapter.on_market_data(_flat_candle(10))) == 1
        sell = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="market",
            side="sell",
            quantity=Decimal("0.0002"),
            price=None,
        )
        sell.min_notional_reference_price = Decimal("50000")
        adapter.place_order(sell)
        assert len(adapter.on_market_data(_flat_candle(20))) == 1

    applications = timeline.apply_due(
        adapter,
        timestamp=30,
        mark_price=Decimal("50000"),
    )
    assert len(applications) == int(with_funding)

    if terminal_action == "fill":
        target = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="market",
            side="buy",
            quantity=Decimal("0.0001"),
            price=None,
        )
        target.min_notional_reference_price = Decimal("50000")
        adapter.place_order(target)
        assert len(adapter.on_market_data(_flat_candle(40))) == 1
        assert adapter.drain_order_rejections() == []
    elif terminal_action == "reject":
        available_base = adapter.get_asset_balance("BTC", "available")
        target = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="market",
            side="sell",
            quantity=available_base + Decimal("0.0001"),
            price=None,
        )
        target.min_notional_reference_price = Decimal("50000")
        adapter.place_order(target)
        assert adapter.on_market_data(_flat_candle(40)) == []
        assert len(adapter.drain_order_rejections()) == 1
    else:
        target = order_factory(
            product_id="BINANCE:BTCUSDT-SPOT",
            order_type="limit",
            side="buy",
            quantity=Decimal("0.0001"),
            price=Decimal("40000"),
        )
        adapter.place_order(target)
        assert target.exchange_order_id is not None
        assert adapter.cancel_order(
            target.exchange_order_id,
            target.product_id,
            order_type=target.type,
        )
        assert adapter.drain_order_rejections() == []

    snapshot = adapter.get_cash_spot_account_snapshot(Decimal("50000"))
    assert snapshot.quote_available + snapshot.quote_reserved == snapshot.quote_total
    assert snapshot.base_available + snapshot.base_reserved == snapshot.base_total
    assert snapshot.quote_total + snapshot.base_total * Decimal("50000") == (
        initial_quote + funding
    )
    assert snapshot.quote_available >= 0
    assert snapshot.base_available >= 0
    checkpoint = timeline.checkpoint()
    assert checkpoint.applied_event_ids == (("matrix-funding",) if with_funding else ())
    assert checkpoint.pending_event_ids == ()
