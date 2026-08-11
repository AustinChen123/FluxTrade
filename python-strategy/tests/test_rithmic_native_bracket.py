from decimal import Decimal
from types import SimpleNamespace

import pytest

import src.core.adapters.rithmic_recovery as rithmic_recovery_module
from src.core.client_order_id import linked_client_order_id
from src.core.adapters.rithmic_native_bracket import (
    build_native_bracket_plan,
    build_native_protection_request,
    build_restored_native_bracket_groups,
    merge_native_bracket_groups,
    native_bracket_leg_type,
    resolve_native_bracket_event_client_order_id,
    supports_native_bracket_group,
)
from src.core.interfaces.exchange import ExchangeError
from src.core.product_registry import InstrumentSpec


PRODUCT_ID = "RITHMIC:NQ-202609"
SPEC = InstrumentSpec(
    product_id=PRODUCT_ID,
    exchange="rithmic",
    symbol="NQU6",
    base="NQ",
    quote="USD",
    quantity_step=Decimal("1"),
    price_tick=Decimal("0.25"),
)


def _order(**overrides):
    values = {
        "product_id": PRODUCT_ID,
        "client_order_id": "strategy-execution-long-123",
        "type": "limit",
        "side": "buy",
        "quantity": Decimal("1"),
        "price": Decimal("20000.25"),
        "id": "entry-1",
        "trigger_price": None,
        "intent_payload": {},
        "exchange_order_id": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _bracket_orders():
    entry = _order()
    stop = _order(
        id="stop-1",
        type="stop_loss",
        side="sell",
        price=None,
        trigger_price=Decimal("19998.25"),
        client_order_id=linked_client_order_id(entry.client_order_id, "sl"),
        intent_payload={"pending_entry_order_id": "entry-1"},
    )
    target = _order(
        id="target-1",
        type="take_profit",
        side="sell",
        price=None,
        trigger_price=Decimal("20003.25"),
        client_order_id=linked_client_order_id(entry.client_order_id, "tp"),
        intent_payload={"pending_entry_order_id": "entry-1"},
    )
    return entry, stop, target


def _side(order) -> str:
    return str(order.side).lower()


@pytest.mark.parametrize(
    ("order_types", "expected"),
    [
        (("market",), False),
        (("limit",), False),
        (("market", "stop_loss"), True),
        (("limit", "take_profit"), True),
        (("limit", "trailing_stop"), True),
    ],
)
def test_atomic_group_eligibility_is_owned_once(order_types, expected):
    orders = [_order(type=order_type) for order_type in order_types]

    assert supports_native_bracket_group(orders) is expected


@pytest.mark.parametrize(
    ("price_type", "expected"),
    [
        ("stop_market", "stop_loss"),
        ("STOP_LIMIT", "stop_loss"),
        ("limit", "take_profit"),
        ("market_if_touched", "take_profit"),
        ("limit_if_touched", "take_profit"),
        ("market", None),
        (None, None),
    ],
)
def test_price_type_has_one_native_leg_classifier(price_type, expected):
    assert native_bracket_leg_type(price_type) == expected


def test_recovery_native_key_delegates_to_the_single_leg_classifier(monkeypatch):
    calls = []

    def classify(price_type):
        calls.append(price_type)
        return "stop_loss"

    monkeypatch.setattr(
        rithmic_recovery_module,
        "native_bracket_leg_type",
        classify,
    )

    assert rithmic_recovery_module._remote_native_key(
        SimpleNamespace(original_basket_id="parent-1", price_type="provider-token")
    ) == ("parent-1", "stop_loss")
    assert calls == ["provider-token"]


def test_plan_without_persistence_has_exact_ticks_and_no_metadata_mutation():
    entry, stop, target = _bracket_orders()
    original_payloads = tuple(
        dict(order.intent_payload) for order in (entry, stop, target)
    )

    plan = build_native_bracket_plan(
        [entry, stop, target],
        validate_order=lambda order: None,
        get_instrument_spec=lambda product_id: SPEC,
        order_side=_side,
        persist=False,
    )

    assert plan == {
        "entry": entry,
        "stop_ticks": 8,
        "target_ticks": 12,
        "leg_client_order_ids": {
            "stop_loss": stop.client_order_id,
            "take_profit": target.client_order_id,
        },
    }
    assert (
        tuple(order.intent_payload for order in (entry, stop, target))
        == original_payloads
    )


def test_plan_persists_exact_parent_and_child_metadata_after_validation():
    entry, stop, target = _bracket_orders()
    validation_trace = []

    build_native_bracket_plan(
        [entry, stop, target],
        validate_order=lambda order: validation_trace.append(order),
        get_instrument_spec=lambda product_id: SPEC,
        order_side=_side,
        persist=True,
    )

    assert validation_trace == [entry]
    assert entry.intent_payload["native_protection"] == {
        "placement_mode": "attach-at-entry",
        "bracket_type": "target_and_stop_static",
        "reference_price": "20000.25",
        "price_tick": "0.25",
        "legs": {
            "stop_loss": {
                "order_id": "stop-1",
                "client_order_id": stop.client_order_id,
                "requested_price": "19998.25",
                "ticks": "8",
            },
            "take_profit": {
                "order_id": "target-1",
                "client_order_id": target.client_order_id,
                "requested_price": "20003.25",
                "ticks": "12",
            },
        },
    }
    assert stop.intent_payload["native_leg_type"] == "stop_loss"
    assert target.intent_payload["native_leg_type"] == "take_profit"


def test_restore_candidate_and_locked_merge_preserve_current_partial_state():
    entry, stop, target = _bracket_orders()
    build_native_bracket_plan(
        [entry, stop, target],
        validate_order=lambda order: None,
        get_instrument_spec=lambda product_id: SPEC,
        order_side=_side,
        persist=True,
    )
    stop.intent_payload["native_parent_basket_id"] = "parent-1"
    restored = build_restored_native_bracket_groups([stop])

    groups, parent_ids = merge_native_bracket_groups(
        {
            "parent-1": {
                "entry": entry.client_order_id,
                "take_profit": target.client_order_id,
            }
        },
        {entry.client_order_id},
        restored,
    )

    assert groups == {
        "parent-1": {
            "entry": entry.client_order_id,
            "stop_loss": stop.client_order_id,
            "take_profit": target.client_order_id,
        }
    }
    assert parent_ids == {entry.client_order_id}


def test_restore_conflict_does_not_mutate_current_state():
    current_groups = {"parent-1": {"entry": "entry-a", "stop_loss": "stop-a"}}
    current_parent_ids = {"entry-a"}

    with pytest.raises(ExchangeError, match="restore_metadata_conflict"):
        merge_native_bracket_groups(
            current_groups,
            current_parent_ids,
            {"parent-1": {"entry": "entry-b"}},
        )

    assert current_groups == {"parent-1": {"entry": "entry-a", "stop_loss": "stop-a"}}
    assert current_parent_ids == {"entry-a"}


@pytest.mark.parametrize(
    ("basket_id", "original_basket_id", "price_type", "expected"),
    [
        ("parent-1", None, None, "entry-1"),
        ("child-1", "parent-1", "stop_market", "stop-1"),
        ("child-2", "parent-1", "limit", "target-1"),
        ("unknown", "missing", "limit", None),
        ("unknown", None, "limit", None),
    ],
)
def test_event_identity_resolution_uses_exact_parent_state(
    basket_id,
    original_basket_id,
    price_type,
    expected,
):
    assert (
        resolve_native_bracket_event_client_order_id(
            client_order_id="entry-1",
            basket_id=basket_id,
            original_basket_id=original_basket_id,
            price_type=price_type,
            groups={
                "parent-1": {
                    "entry": "entry-1",
                    "stop_loss": "stop-1",
                    "take_profit": "target-1",
                }
            },
            parent_ids={"entry-1"},
        )
        == expected
    )


def test_modify_request_projects_exact_provider_arguments_without_io():
    _, stop, _ = _bracket_orders()
    stop.exchange_order_id = "child-stop-1"
    stop.intent_payload.update(
        {
            "placement_mode": "attach-at-entry",
            "entry_side": "buy",
            "reference_price": "20000.25",
        }
    )

    request = build_native_protection_request(
        stop,
        Decimal("19999.00"),
        get_instrument_spec=lambda product_id: SPEC,
    )

    assert request.basket_id == "child-stop-1"
    assert request.product_id == PRODUCT_ID
    assert request.quantity == "1"
    assert request.leg_type == "stop_loss"
    assert request.price == "19999.00"
