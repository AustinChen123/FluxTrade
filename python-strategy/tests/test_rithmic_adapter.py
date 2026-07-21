from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.core.adapters import create_adapter
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.interfaces.exchange import ExchangeError, NetworkError


PRODUCT_ID = "RITHMIC:NQ-202609"
INSTRUMENTS = {
    PRODUCT_ID: {
        "exchange": "CME",
        "quantity_step": "1",
        "price_tick": "0.25",
        "multiplier": "20",
    }
}


def order(**overrides):
    values = {
        "product_id": PRODUCT_ID,
        "client_order_id": "client-1",
        "type": "limit",
        "side": "buy",
        "quantity": Decimal("1"),
        "price": Decimal("20000.25"),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def event(**overrides):
    values = {
        "account_id": "ACCOUNT",
        "client_order_id": "client-1",
        "basket_id": "basket-1",
        "exchange_order_id": "exchange-1",
        "exchange": "CME",
        "symbol": "NQU6",
        "status": "partially_filled",
        "cumulative_filled_quantity": "1",
        "cumulative_average_price": "20000.25",
        "last_fill_quantity": "1",
        "last_fill_price": "20000.25",
        "timestamp_ms": 1_700_000_000_000,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def snapshot(**overrides):
    values = {
        "client_order_id": "client-1",
        "basket_id": "basket-1",
        "exchange_order_id": "exchange-1",
        "status": "OPEN",
        "quantity": "2",
        "filled_quantity": "1",
        "average_fill_price": "20000.25",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def client():
    client = Mock()
    client.submit.return_value = SimpleNamespace(basket_id="basket-1")
    client.cancel.return_value = True
    client.poll_event.return_value = None
    client.lookup.return_value = None
    return client


@pytest.fixture
def adapter(client):
    return RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )


def test_runtime_start_is_explicit_and_idempotent(client):
    factory = Mock(return_value=client)
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=factory,
    )

    assert factory.call_count == 0
    adapter.start_order_event_stream()
    adapter.start_order_event_stream()

    factory.assert_called_once_with("test", "ACCOUNT")


def test_limit_order_uses_native_contract_and_decimal_strings(adapter, client):
    adapter.start_order_event_stream()

    basket_id = adapter.place_order(order())

    assert basket_id == "basket-1"
    client.submit.assert_called_once_with(
        "client-1", "CME", "NQU6", "1", "buy", "limit", "20000.25"
    )


@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        (order(quantity=Decimal("1.5")), "futures_quantity_off_step"),
        (order(price=Decimal("20000.10")), "price_off_tick"),
        (order(type="stop_loss"), "rithmic_order_type_unsupported"),
        (order(client_order_id=None), "rithmic_client_order_id_required"),
    ],
)
def test_order_validation_matrix_fails_before_submit(adapter, client, candidate, message):
    adapter.start_order_event_stream()

    with pytest.raises(ExchangeError, match=message):
        adapter.place_order(candidate)

    client.submit.assert_not_called()


def test_ambiguous_runtime_failure_maps_to_network_error(adapter, client):
    adapter.start_order_event_stream()
    client.submit.side_effect = RuntimeError("order result is ambiguous")

    with pytest.raises(NetworkError, match="ambiguous"):
        adapter.place_order(order())

    with pytest.raises(ExchangeError, match="duplicate_rithmic_client_order_id"):
        adapter.place_order(order())
    assert client.submit.call_count == 1


def test_explicit_submit_rejection_allows_safe_same_id_retry(adapter, client):
    adapter.start_order_event_stream()
    client.submit.side_effect = [RuntimeError("request rejected"), SimpleNamespace(basket_id="basket-1")]

    with pytest.raises(ExchangeError, match="request rejected"):
        adapter.place_order(order())

    assert adapter.place_order(order()) == "basket-1"
    assert client.submit.call_count == 2


def test_cancel_returns_only_after_runtime_confirms_terminal(adapter, client):
    adapter.start_order_event_stream()

    assert adapter.cancel_order("basket-1", PRODUCT_ID) is True

    client.cancel.assert_called_once_with("basket-1")


@pytest.mark.parametrize(
    ("remote_status", "filled", "expected"),
    [
        ("OPEN", "0", "open"),
        ("OPEN", "1", "partially_filled"),
        ("COMPLETE", "2", "filled"),
        ("CANCELLED", "0", "cancelled"),
        ("REJECTED", "0", "rejected"),
    ],
)
def test_lookup_normalizes_remote_state_matrix(
    adapter,
    client,
    remote_status,
    filled,
    expected,
):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(
        status=remote_status,
        filled_quantity=filled,
    )

    result = adapter.get_order_by_client_id("client-1", PRODUCT_ID)

    assert result.status == expected
    assert result.exchange_order_id == "basket-1"
    client.lookup.assert_called_once_with("client-1", "CME", "NQU6")


def test_cancel_by_client_id_uses_lookup_basket_identity(adapter, client):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot()

    assert adapter.cancel_order_by_client_id("client-1", PRODUCT_ID) is True

    client.cancel.assert_called_once_with("basket-1")


def test_cancel_by_client_id_does_not_cancel_terminal_snapshot(adapter, client):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(status="COMPLETE", filled_quantity="2")

    assert adapter.cancel_order_by_client_id("client-1", PRODUCT_ID) is False

    client.cancel.assert_not_called()


def test_lookup_rejects_inconsistent_terminal_snapshot(adapter, client):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(status="COMPLETE", filled_quantity="1")

    with pytest.raises(ExchangeError, match="unsupported_rithmic_order_snapshot_status"):
        adapter.get_order_by_client_id("client-1", PRODUCT_ID)


def test_order_event_maps_native_identity_and_decimal_fields(adapter, client):
    adapter.start_order_event_stream()
    client.poll_event.return_value = event()

    mapped = adapter.poll_order_event()

    assert mapped.product_id == PRODUCT_ID
    assert mapped.exchange_order_id == "basket-1"
    assert mapped.cumulative_filled_quantity == Decimal("1")
    assert mapped.last_fill_price == Decimal("20000.25")


def test_unknown_order_event_instrument_fails_closed(adapter, client):
    adapter.start_order_event_stream()
    client.poll_event.return_value = event(symbol="ESZ6")

    with pytest.raises(ExchangeError, match="unknown_rithmic_order_event_instrument"):
        adapter.poll_order_event()


def test_account_queries_remain_explicitly_unavailable(adapter):
    with pytest.raises(ExchangeError, match="balance_unavailable"):
        adapter.get_balance("USD")
    with pytest.raises(ExchangeError, match="position_unavailable"):
        adapter.get_position(PRODUCT_ID)


def test_factory_selects_rithmic_without_starting_network_runtime():
    adapter = create_adapter(
        {
            "mode": "live",
            "exchange": "rithmic",
            "rithmic_profile": "test",
            "account_id": "ACCOUNT",
            "rithmic_instruments": INSTRUMENTS,
        }
    )

    assert isinstance(adapter, RithmicExchangeAdapter)
    assert adapter._client is None
