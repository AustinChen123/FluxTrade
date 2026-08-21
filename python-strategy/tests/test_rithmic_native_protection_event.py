from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.adapters.rithmic_native_protection_event import (
    process_native_protection_event,
)
from src.core.interfaces import IOrderRepository
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, Position, Trade
from src.core.runtime_capabilities import DefaultRuntimeBootstrap


class _Repository(IOrderRepository):
    def __init__(self, order: Order | None = None) -> None:
        self.order = order
        self.update_order_mock = MagicMock()

    def add_order(self, order: Order) -> None:
        raise AssertionError("add_order is not expected")

    def update_order(self, order: Order) -> None:
        self.update_order_mock(order)

    def get_order_by_client_order_id(self, client_order_id: str) -> Order | None:
        if self.order is None or self.order.client_order_id != client_order_id:
            return None
        return self.order

    def get_order(self, order_id: str) -> Order | None:
        if self.order is None or str(self.order.id) != order_id:
            return None
        return self.order

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        raise AssertionError("update_order_exchange_id is not expected")

    def add_trade(self, trade: Trade) -> None:
        raise AssertionError("add_trade is not expected")

    def update_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        fill_quantity: Decimal,
        fill_price: Decimal,
        position_side: str,
    ) -> None:
        raise AssertionError("update_position is not expected")

    def get_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
    ) -> Position | None:
        raise AssertionError("get_position is not expected")


def _order(**changes: object) -> Order:
    values = {
        "id": "order-1",
        "client_order_id": "client-1",
        "exchange_order_id": "child-1",
        "strategy_id": "strategy-1",
        "product_id": "RITHMIC:MNQ-202609",
        "exchange_id": "rithmic",
        "type": "stop_loss",
        "side": "sell",
        "price": None,
        "trigger_price": Decimal("99.00"),
        "quantity": Decimal("1"),
        "status": "NEW",
        "timestamp": 1,
        "intent_payload": {
            "placement_mode": "attach-at-entry",
            "native_parent_basket_id": "parent-1",
            "native_bracket_type": "stop_only_static",
            "expected_effective_price": "99.25",
        },
    }
    values.update(changes)
    return Order(**values)


_NATIVE_EVENT_RAW: dict[str, object] = {
    "original_basket_id": "parent-1",
    "price_type": "STOP_MARKET",
    "trigger_price": "99.25",
    "bracket_type": "stop_only_static",
}


def _event(**changes) -> ExchangeOrderEvent:
    values = {
        "status": "open",
        "product_id": "RITHMIC:MNQ-202609",
        "client_order_id": "client-1",
        "exchange_order_id": "child-1",
        "raw": dict(_NATIVE_EVENT_RAW),
    }
    values.update(changes)
    return ExchangeOrderEvent(**values)


def test_default_runtime_bootstrap_applies_generic_event_once() -> None:
    apply_event = MagicMock(return_value={"action": "generic"})

    result = DefaultRuntimeBootstrap().process_order_event(
        _Repository(),
        _event(),
        apply_event,
    )

    assert result == {"action": "generic"}
    apply_event.assert_called_once_with()


def test_generic_execution_has_no_rithmic_native_event_policy() -> None:
    source = (Path(__file__).parents[1] / "src" / "core" / "execution.py").read_text()

    for provider_detail in (
        "original_basket_id",
        "unresolved_native_protection_parent_mismatch",
        "unresolved_native_protection_basket_mismatch",
        "unresolved_native_protection_price_type_mismatch",
        "unresolved_native_protection_bracket_type_mismatch",
        "unresolved_native_protection_price_missing",
        "unresolved_native_protection_price_mismatch",
    ):
        assert provider_detail not in source


@pytest.mark.parametrize(
    ("order", "event"),
    [
        (_order(), _event(client_order_id=None)),
        (None, _event()),
        (_order(type="market"), _event()),
        (_order(intent_payload={}), _event()),
    ],
)
def test_non_native_events_apply_once_without_provider_projection(order, event) -> None:
    repository = _Repository(order)
    apply_event = MagicMock(return_value={"action": "ignored"})

    result = process_native_protection_event(repository, event, apply_event)

    assert result == {"action": "ignored"}
    apply_event.assert_called_once_with()
    repository.update_order_mock.assert_not_called()


@pytest.mark.parametrize(
    ("order", "event", "expected_action"),
    [
        (
            _order(),
            _event(raw={"original_basket_id": "other-parent"}),
            "unresolved_native_protection_parent_mismatch",
        ),
        (
            _order(exchange_order_id="other-child"),
            _event(),
            "unresolved_native_protection_basket_mismatch",
        ),
    ],
)
def test_native_identity_mismatch_prevents_generic_application(
    order, event, expected_action
) -> None:
    repository = _Repository(order)
    apply_event = MagicMock()

    result = process_native_protection_event(repository, event, apply_event)

    assert result == {
        "action": expected_action,
        "order_id": "order-1",
        "status": "open",
    }
    apply_event.assert_not_called()
    repository.update_order_mock.assert_not_called()


def test_generic_application_exception_propagates_by_identity() -> None:
    error = RuntimeError("generic apply failed")
    repository = _Repository(_order(type="market"))

    with pytest.raises(RuntimeError) as raised:
        process_native_protection_event(
            repository,
            _event(),
            MagicMock(side_effect=error),
        )

    assert raised.value is error
    repository.update_order_mock.assert_not_called()


@pytest.mark.parametrize(
    ("result", "raw", "expected_action"),
    [
        ({"action": "ignored"}, _event().raw, "ignored"),
        (
            {"action": "applied", "state": "filled", "order_id": "order-1"},
            _event().raw,
            "applied",
        ),
        (
            {"action": "applied", "state": "open", "order_id": "order-1"},
            {},
            "applied",
        ),
        (
            {"action": "applied", "state": "open", "order_id": "order-1"},
            {**_NATIVE_EVENT_RAW, "price_type": "limit"},
            "unresolved_native_protection_price_type_mismatch",
        ),
        (
            {"action": "applied", "state": "open", "order_id": "order-1"},
            {**_NATIVE_EVENT_RAW, "bracket_type": "other"},
            "unresolved_native_protection_bracket_type_mismatch",
        ),
    ],
)
def test_ineligible_or_identity_invalid_post_projection_does_not_persist(
    result, raw, expected_action
) -> None:
    order = _order()
    repository = _Repository(order)
    apply_event = MagicMock(return_value=result)

    projected = process_native_protection_event(
        repository,
        _event(raw=raw),
        apply_event,
    )

    assert projected["action"] == expected_action
    apply_event.assert_called_once_with()
    repository.update_order_mock.assert_not_called()
    assert order.trigger_price == Decimal("99.00")


@pytest.mark.parametrize("raw_price", [None, "0", "NaN", "invalid"])
def test_invalid_remote_price_fails_without_persistence(raw_price) -> None:
    order = _order()
    repository = _Repository(order)
    raw = {**_NATIVE_EVENT_RAW, "trigger_price": raw_price}
    apply_event = MagicMock(
        return_value={"action": "applied", "state": "open", "order_id": "order-1"}
    )

    result = process_native_protection_event(repository, _event(raw=raw), apply_event)

    assert result["action"] == "unresolved_native_protection_price_missing"
    repository.update_order_mock.assert_not_called()


@pytest.mark.parametrize(
    ("order", "expected_action", "confirmation", "trigger_price"),
    [
        (
            _order(
                intent_payload={
                    "placement_mode": "attach-at-entry",
                    "native_parent_basket_id": "parent-1",
                    "native_bracket_type": "stop_only_static",
                }
            ),
            "applied",
            "observed_pending_entry_fill",
            Decimal("99.00"),
        ),
        (_order(), "applied", "confirmed", Decimal("99.25")),
        (
            _order(
                intent_payload={
                    "placement_mode": "attach-at-entry",
                    "native_parent_basket_id": "parent-1",
                    "native_bracket_type": "stop_only_static",
                    "expected_effective_price": "99.50",
                }
            ),
            "unresolved_native_protection_price_mismatch",
            "conflict",
            Decimal("99.00"),
        ),
    ],
)
def test_native_confirmation_projects_and_persists_exactly_once(
    order, expected_action, confirmation, trigger_price
) -> None:
    original_payload = order.intent_payload
    repository = _Repository(order)
    apply_event = MagicMock(
        return_value={"action": "applied", "state": "open", "order_id": "order-1"}
    )

    result = process_native_protection_event(repository, _event(), apply_event)

    assert result["action"] == expected_action
    apply_event.assert_called_once_with()
    repository.update_order_mock.assert_called_once_with(order)
    assert order.intent_payload is not original_payload
    assert order.intent_payload is not None
    assert order.intent_payload["remote_effective_price"] == "99.25"
    assert order.intent_payload["remote_price_type"] == "stop_market"
    assert order.intent_payload["remote_bracket_type"] == "stop_only_static"
    assert order.intent_payload["protection_confirmation"] == confirmation
    assert order.trigger_price == trigger_price
    if expected_action == "unresolved_native_protection_price_mismatch":
        assert result == {
            "action": "unresolved_native_protection_price_mismatch",
            "state": "open",
            "order_id": "order-1",
            "expected_price": "99.50",
            "remote_price": "99.25",
        }


def test_take_profit_uses_limit_price_and_confirms() -> None:
    order = _order(
        type="take_profit",
        trigger_price=Decimal("101.00"),
        intent_payload={
            "placement_mode": "attach-at-entry",
            "native_parent_basket_id": "parent-1",
            "native_bracket_type": "target_only_static",
            "expected_effective_price": "101.25",
        },
    )
    repository = _Repository(order)
    event = _event(
        raw={
            "original_basket_id": "parent-1",
            "price_type": "limit",
            "price": "101.25",
            "bracket_type": "target_only_static",
        }
    )

    result = process_native_protection_event(
        repository,
        event,
        lambda: {"action": "applied", "state": "open", "order_id": "order-1"},
    )

    assert result["action"] == "applied"
    assert order.trigger_price == Decimal("101.25")
    repository.update_order_mock.assert_called_once_with(order)
