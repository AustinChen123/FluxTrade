import ast
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core import execution_journal as journal_projection
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.models import Candlestick, OrderSide


class _DictSubclass(dict):
    pass


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (None, None),
        ("raw", None),
        ({}, None),
        ({"order": "raw"}, None),
        ({"order": {}}, None),
        ({"order": {"price": None}}, None),
        ({"order": {"price": Decimal("0.00")}}, "0.00"),
        ({"order": {"price": Decimal("123.450")}}, "123.450"),
        (_DictSubclass(order=_DictSubclass(price="99")), "99"),
    ],
)
def test_intent_signal_price_preserves_mapping_shape_compatibility(payload, expected):
    order = SimpleNamespace(intent_payload=payload)

    assert journal_projection.intent_signal_price(order) == expected


def test_intent_signal_price_missing_payload_is_none():
    assert journal_projection.intent_signal_price(object()) is None


@pytest.mark.parametrize(
    (
        "order_price",
        "stop_loss",
        "take_profit",
        "trailing_distance",
        "expected",
    ),
    [
        (
            Decimal("101.250"),
            Decimal("99.00"),
            Decimal("105.000"),
            Decimal("1.50"),
            {
                "price": "101.250",
                "stop_loss": "99.00",
                "take_profit": "105.000",
                "trailing_distance": "1.50",
            },
        ),
        (
            Decimal("0.00"),
            Decimal("0.00"),
            Decimal("0.00"),
            Decimal("0.00"),
            {
                "price": "market",
                "stop_loss": None,
                "take_profit": None,
                "trailing_distance": None,
            },
        ),
    ],
)
@pytest.mark.parametrize("side", [OrderSide.BUY, OrderSide.SELL])
@pytest.mark.parametrize("order_type", ["market", "limit"])
def test_journal_entry_projects_exact_post_placement_payload(
    order_price,
    stop_loss,
    take_profit,
    trailing_distance,
    expected,
    side,
    order_type,
):
    journal = MagicMock()
    signal = SimpleNamespace(
        timestamp=1_704_067_200_111,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trailing_distance=trailing_distance,
    )
    order = SimpleNamespace(
        id="order-1",
        quantity=Decimal("0.0600"),
        price=order_price,
    )

    journal_projection.journal_entry(
        journal,
        signal,
        order,
        side,
        order_type,
    )

    journal.log.assert_called_once_with(
        "entry",
        {
            "order_id": "order-1",
            "side": side,
            "order_type": order_type,
            "quantity": "0.0600",
            **expected,
        },
        timestamp=1_704_067_200_111,
        trade_id="order-1",
    )
    projected_side = journal.log.call_args.args[1]["side"]
    assert projected_side is side
    assert type(projected_side) is OrderSide


@pytest.mark.parametrize(
    ("fill_type", "expected_tag"),
    [
        ("STOP_LOSS", "sl_hit"),
        ("TAKE_PROFIT", "tp_hit"),
        ("TRAILING_STOP", "trailing_hit"),
        ("MARKET", "fill"),
        ("LIMIT", "fill"),
        ("UNKNOWN", "fill"),
    ],
)
@pytest.mark.parametrize(
    ("fee", "expected_fee"),
    [(None, "0"), (Decimal("0.00"), "0"), (Decimal("0.1250"), "0.1250")],
)
def test_journal_fill_projects_exact_tag_payload_and_identity(
    fill_type, expected_tag, fee, expected_fee
):
    journal = MagicMock()
    order = SimpleNamespace(id="order-1", side="buy")
    candle = Candlestick(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1_704_067_200_123,
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=Decimal("10"),
    )

    journal_projection.journal_fill(
        journal,
        order,
        Decimal("101.25"),
        Decimal("0.060"),
        fee,
        fill_type,
        candle,
    )

    journal.log.assert_called_once_with(
        expected_tag,
        {
            "order_id": "order-1",
            "side": "buy",
            "price": "101.25",
            "quantity": "0.060",
            "fee": expected_fee,
            "fill_type": fill_type,
        },
        timestamp=1_704_067_200_123,
        trade_id="order-1",
    )


def test_journal_fill_without_candle_uses_zero_timestamp():
    journal = MagicMock()

    journal_projection.journal_fill(
        journal,
        SimpleNamespace(id="order-1", side="sell"),
        Decimal("100"),
        Decimal("1"),
        None,
        "MARKET",
    )

    assert journal.log.call_args.kwargs["timestamp"] == 0


@pytest.mark.parametrize("event_timestamp", [1_704_067_200_321, 0, None])
@pytest.mark.parametrize("fee", [None, Decimal("0.00"), Decimal("0.0020")])
def test_exchange_event_fill_projects_exact_payload_and_timestamp(event_timestamp, fee):
    journal = MagicMock()
    clock = MagicMock()
    clock.now.return_value = 1_704_067_299.5
    order = SimpleNamespace(
        id="order-1",
        side="buy",
        price=Decimal("100.00"),
        intent_payload={"order": {"price": Decimal("99.50")}},
    )
    event = ExchangeOrderEvent(
        status="filled",
        product_id="BINANCE:BTCUSDT-PERP",
        client_order_id="client-1",
        exchange_order_id="exchange-1",
        fee=fee,
        fee_asset="USDT",
        event_timestamp=event_timestamp,
    )

    journal_projection.journal_exchange_order_event_fill(
        journal,
        clock,
        order,
        event,
        Decimal("103.25"),
        Decimal("0.06"),
    )

    journal.log.assert_called_once_with(
        "fill",
        {
            "order_id": "order-1",
            "side": "buy",
            "signal_price": "99.50",
            "submitted_price": "100.00",
            "fill_price": "103.25",
            "quantity": "0.06",
            "fee": str(fee) if fee is not None else "0",
            "fee_asset": "USDT",
            "exchange_order_id": "exchange-1",
            "client_order_id": "client-1",
            "exchange_status": "filled",
        },
        timestamp=event_timestamp or 1_704_067_299_500,
        trade_id="order-1",
    )
    assert clock.now.call_count == (0 if event_timestamp else 1)


def test_exchange_event_fill_without_submitted_price_uses_market():
    journal = MagicMock()
    order = SimpleNamespace(id="order-1", side="sell", price=None, intent_payload=None)

    journal_projection.journal_exchange_order_event_fill(
        journal,
        MagicMock(now=MagicMock(return_value=1)),
        order,
        ExchangeOrderEvent(status="open", product_id="BINANCE:BTCUSDT-PERP"),
        Decimal("1"),
        Decimal("2"),
    )

    payload = journal.log.call_args.args[1]
    assert payload["signal_price"] is None
    assert payload["submitted_price"] == "market"


def test_exchange_event_fill_treats_scaled_zero_submitted_price_as_market():
    journal = MagicMock()
    order = SimpleNamespace(
        id="order-1",
        side="sell",
        price=Decimal("0.00"),
        intent_payload=None,
    )

    journal_projection.journal_exchange_order_event_fill(
        journal,
        MagicMock(now=MagicMock(return_value=1)),
        order,
        ExchangeOrderEvent(
            status="open",
            product_id="BINANCE:BTCUSDT-PERP",
            event_timestamp=1,
        ),
        Decimal("1"),
        Decimal("2"),
    )

    assert journal.log.call_args.args[1]["submitted_price"] == "market"


@pytest.mark.parametrize("projection", ["entry", "candle", "exchange_event"])
def test_journal_projection_preserves_exception_identity(projection):
    journal = MagicMock()
    error = RuntimeError(f"{projection} journal sentinel")
    journal.log.side_effect = error

    with pytest.raises(RuntimeError) as raised:
        if projection == "entry":
            journal_projection.journal_entry(
                journal,
                SimpleNamespace(
                    timestamp=1,
                    stop_loss=None,
                    take_profit=None,
                    trailing_distance=None,
                ),
                SimpleNamespace(id="order-1", quantity=Decimal("1"), price=None),
                "buy",
                "market",
            )
        elif projection == "candle":
            journal_projection.journal_fill(
                journal,
                SimpleNamespace(id="order-1", side="buy"),
                Decimal("1"),
                Decimal("1"),
                None,
                "MARKET",
            )
        else:
            journal_projection.journal_exchange_order_event_fill(
                journal,
                MagicMock(now=MagicMock(return_value=1)),
                SimpleNamespace(
                    id="order-1", side="buy", price=None, intent_payload=None
                ),
                ExchangeOrderEvent(status="filled", product_id="BINANCE:BTCUSDT-PERP"),
                Decimal("1"),
                Decimal("1"),
            )

    assert raised.value is error
    assert journal.log.call_count == 1


def test_owner_module_has_projection_only_dependencies():
    source = Path(journal_projection.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)
    functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}

    assert imports == {
        "decimal",
        "typing",
        "src.core.interfaces.exchange",
        "src.core.models",
    }
    assert functions == {
        "intent_signal_price",
        "journal_entry",
        "journal_fill",
        "journal_exchange_order_event_fill",
    }
    compact_source = source.lower().replace("_", "")
    for forbidden in (
        "adapter",
        "repository",
        "ordermanager",
        "matching",
        "filldelta",
        "updateorder",
        "metric",
        "audit",
        "commit",
        "rollback",
        "logging",
        "conditional",
        "rithmic",
        "binance",
        "backpack",
        "bybit",
        "okx",
    ):
        assert forbidden not in compact_source
