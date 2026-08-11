import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import src.core.adapters.rithmic_adapter as rithmic_adapter_module
from src.core.adapters import create_adapter
from src.core.adapters.rithmic_adapter import (
    RithmicExchangeAdapter,
    RithmicUnmappedOrderEvent,
)
from src.core.interfaces.exchange import ExchangeError, NetworkError
from src.core.models import PositionSide


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
        "original_basket_id": None,
        "linked_basket_ids": None,
        "exchange_order_id": "exchange-1",
        "exchange": "CME",
        "symbol": "NQU6",
        "status": "partially_filled",
        "price": "20000.25",
        "trigger_price": None,
        "price_type": "limit",
        "bracket_type": None,
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
        "notification_type": "STATUS",
        "quantity": "2",
        "filled_quantity": "1",
        "average_fill_price": "20000.25",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def ledger_snapshot(*, positions, account_id="ACCOUNT", orders=None):
    return SimpleNamespace(
        account_id=account_id,
        positions=positions,
        orders=orders or [],
    )


def ledger_position(**overrides):
    values = {
        "exchange": "CME",
        "symbol": "NQU6",
        "net_quantity": "1",
        "average_open_fill_price": "20000.25",
        "open_pnl": "10.50",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def client():
    client = Mock()
    client.submit.return_value = SimpleNamespace(basket_id="basket-1")
    client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
    client.modify_protection.return_value = True
    client.cancel.return_value = True
    client.exit_position.return_value = True
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


def test_cancel_terminal_state_is_delivered_by_order_events(adapter):
    assert adapter.cancel_terminal_state_delivered_by_order_events() is True


def test_exit_position_uses_native_instrument_identity(adapter, client):
    adapter.start_order_event_stream()

    assert adapter.exit_position(PRODUCT_ID) is True

    client.exit_position.assert_called_once_with("CME", "NQU6")
    assert adapter.configured_product_ids == (PRODUCT_ID,)


def test_exit_position_maps_ambiguous_runtime_failure(adapter, client):
    adapter.start_order_event_stream()
    client.exit_position.side_effect = RuntimeError(
        "Rithmic exit-position result is ambiguous"
    )

    with pytest.raises(NetworkError, match="rithmic_exit_position_failed"):
        adapter.exit_position(PRODUCT_ID)


@pytest.mark.parametrize(
    ("net_quantity", "expected_side", "expected_quantity"),
    [
        ("2", PositionSide.LONG, Decimal("2")),
        ("-3", PositionSide.SHORT, Decimal("3")),
    ],
)
def test_ledger_positions_are_authoritative_signed_positions(
    adapter,
    net_quantity,
    expected_side,
    expected_quantity,
):
    positions = adapter.positions_from_ledger_snapshot(
        ledger_snapshot(positions=[ledger_position(net_quantity=net_quantity)])
    )

    assert len(positions) == 1
    assert positions[0].strategy_id == "LIVE"
    assert positions[0].product_id == PRODUCT_ID
    assert positions[0].side == expected_side
    assert positions[0].quantity == expected_quantity


@pytest.mark.parametrize("symbol", ["NQU6", "ESU6"])
def test_ledger_zero_position_is_omitted_before_instrument_mapping(adapter, symbol):
    positions = adapter.positions_from_ledger_snapshot(
        ledger_snapshot(positions=[ledger_position(symbol=symbol, net_quantity="0")])
    )

    assert positions == []


@pytest.mark.parametrize(
    ("snapshot_value", "error"),
    [
        (
            ledger_snapshot(positions=[], account_id="OTHER"),
            "rithmic_ledger_account_id_mismatch",
        ),
        (
            ledger_snapshot(positions=[ledger_position(symbol="ESU6")]),
            "rithmic_ledger_position_instrument_unmapped",
        ),
        (
            ledger_snapshot(positions=[ledger_position(net_quantity="not-a-number")]),
            "rithmic_ledger_position_value_invalid",
        ),
    ],
)
def test_ledger_position_conversion_fails_closed(adapter, snapshot_value, error):
    with pytest.raises(ExchangeError, match=error):
        adapter.positions_from_ledger_snapshot(snapshot_value)


def test_close_waits_for_active_order_client_call(adapter, client):
    call_started = threading.Event()
    release_call = threading.Event()
    close_finished = threading.Event()

    def blocking_submit(*_args):
        call_started.set()
        assert release_call.wait(1.0)
        return SimpleNamespace(basket_id="basket-1")

    client.submit.side_effect = blocking_submit
    adapter.start_order_event_stream()

    def close_adapter():
        adapter.close()
        close_finished.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        submit_future = executor.submit(adapter.place_order, order())
        assert call_started.wait(1.0)
        close_future = executor.submit(close_adapter)
        assert not close_finished.wait(0.05)
        release_call.set()
        assert submit_future.result(timeout=1.0) == "basket-1"
        close_future.result(timeout=1.0)

    assert close_finished.is_set()
    assert adapter._client is None


def test_limit_order_uses_native_contract_and_decimal_strings(adapter, client):
    adapter.start_order_event_stream()

    basket_id = adapter.place_order(order())

    assert basket_id == "basket-1"
    client.submit.assert_called_once_with(
        "client-1", "CME", "NQU6", "1", "buy", "limit", "20000.25"
    )


def bracket_orders(*, side="buy", entry_type="limit", quantity=Decimal("1")):
    entry_client_id = "strategy-execution-long-123"
    entry = order(
        id="entry-1",
        client_order_id=entry_client_id,
        side=side,
        type=entry_type,
        quantity=quantity,
        price=Decimal("20000.25") if entry_type == "limit" else None,
        intent_payload={},
        min_notional_reference_price=Decimal("20000.25"),
    )
    close_side = "sell" if side == "buy" else "buy"
    stop_price = Decimal("19998.25") if side == "buy" else Decimal("20002.25")
    target_price = Decimal("20003.25") if side == "buy" else Decimal("19997.25")
    stop = order(
        id="stop-1",
        client_order_id="strategy-execution-sl-123",
        side=close_side,
        type="stop_loss",
        quantity=quantity,
        price=None,
        trigger_price=stop_price,
        intent_payload={"pending_entry_order_id": "entry-1"},
    )
    target = order(
        id="target-1",
        client_order_id="strategy-execution-tp-123",
        side=close_side,
        type="take_profit",
        quantity=quantity,
        price=None,
        trigger_price=target_price,
        intent_payload={"pending_entry_order_id": "entry-1"},
    )
    return entry, stop, target


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize("entry_type", ["market", "limit"])
@pytest.mark.parametrize(
    ("leg_types", "stop_ticks", "target_ticks", "bracket_type"),
    [
        (("stop_loss",), 8, None, "stop_only_static"),
        (("take_profit",), None, 12, "target_only_static"),
        (
            ("stop_loss", "take_profit"),
            8,
            12,
            "target_and_stop_static",
        ),
    ],
)
def test_native_bracket_submits_one_atomic_single_contract_request(
    adapter,
    client,
    side,
    entry_type,
    leg_types,
    stop_ticks,
    target_ticks,
    bracket_type,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders(side=side, entry_type=entry_type)
    legs = {"stop_loss": stop, "take_profit": target}
    orders = [entry, *(legs[leg_type] for leg_type in leg_types)]

    adapter.validate_order_group(orders)
    basket_id = adapter.place_order_group(orders)

    assert basket_id == "parent-1"
    client.submit.assert_not_called()
    client.submit_bracket.assert_called_once_with(
        entry.client_order_id,
        "CME",
        "NQU6",
        "1",
        side,
        entry_type,
        str(entry.price) if entry.price is not None else None,
        stop_ticks,
        target_ticks,
    )
    native = entry.intent_payload["native_protection"]
    assert native["reference_price"] == "20000.25"
    assert native["bracket_type"] == bracket_type
    assert set(native["legs"]) == set(leg_types)
    for leg_type in leg_types:
        leg = legs[leg_type]
        assert leg.intent_payload["placement_mode"] == "attach-at-entry"
        assert (
            leg.intent_payload["native_parent_client_order_id"] == entry.client_order_id
        )
    assert adapter._submitted_client_order_ids == {entry.client_order_id}
    assert adapter._native_bracket_parent_client_order_ids == {entry.client_order_id}
    assert adapter._native_brackets_by_parent == {
        "parent-1": {
            "entry": entry.client_order_id,
            **{leg_type: legs[leg_type].client_order_id for leg_type in leg_types},
        }
    }


def test_atomic_group_eligibility_delegates_to_native_bracket_owner(
    adapter,
    monkeypatch,
):
    entry, stop, target = bracket_orders()
    orders = [entry, stop, target]
    calls = []

    def reject(candidate_orders):
        calls.append(candidate_orders)
        return False

    monkeypatch.setattr(
        rithmic_adapter_module,
        "supports_native_bracket_group",
        reject,
        raising=False,
    )

    assert adapter.supports_atomic_order_group(orders) is False
    assert calls == [orders]


def test_group_validation_delegates_to_native_bracket_owner(
    adapter,
    monkeypatch,
):
    class Delegated(Exception):
        pass

    entry, stop, target = bracket_orders()
    orders = [entry, stop, target]

    def delegated(*args, **kwargs):
        assert args == (orders,)
        assert kwargs["persist"] is True
        assert kwargs["validate_order"].__self__ is adapter
        assert kwargs["get_instrument_spec"].__self__ is adapter
        assert adapter._client_lock.locked() is False
        raise Delegated

    monkeypatch.setattr(
        rithmic_adapter_module,
        "build_native_bracket_plan",
        delegated,
        raising=False,
    )

    with pytest.raises(Delegated):
        adapter.validate_order_group(orders)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda entry, stop, target: setattr(entry, "quantity", Decimal("2")),
            "single_contract",
        ),
        (
            lambda entry, stop, target: setattr(stop, "quantity", Decimal("2")),
            "single_contract",
        ),
        (
            lambda entry, stop, target: setattr(
                stop, "trigger_price", Decimal("19998.30")
            ),
            "distance_off_tick",
        ),
        (
            lambda entry, stop, target: setattr(
                stop, "trigger_price", Decimal("20001")
            ),
            "wrong_side",
        ),
        (
            lambda entry, stop, target: setattr(target, "side", "buy"),
            "close_side_mismatch",
        ),
        (
            lambda entry, stop, target: setattr(
                target, "product_id", "RITHMIC:ES-202609"
            ),
            "product_mismatch",
        ),
        (
            lambda entry, stop, target: setattr(
                stop, "client_order_id", "strategy-execution-wrong-123"
            ),
            "client_order_id_mismatch",
        ),
        (
            lambda entry, stop, target: setattr(stop, "type", "trailing_stop"),
            "leg_unsupported",
        ),
    ],
)
def test_native_bracket_validation_fails_before_remote_submission(
    adapter, client, mutate, message
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    mutate(entry, stop, target)

    with pytest.raises(ExchangeError, match=message):
        adapter.place_order_group([entry, stop, target])

    client.submit.assert_not_called()
    client.submit_bracket.assert_not_called()
    assert adapter._submitted_client_order_ids == set()
    assert adapter._native_bracket_parent_client_order_ids == set()
    assert adapter._native_brackets_by_parent == {}


def test_native_market_bracket_requires_a_reference_price(adapter, client):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders(entry_type="market")
    entry.min_notional_reference_price = None

    with pytest.raises(ExchangeError, match="reference_price_required"):
        adapter.place_order_group([entry, stop, target])

    client.submit_bracket.assert_not_called()


def test_native_bracket_child_event_maps_to_local_leg_identity(adapter, client):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.place_order_group([entry, stop, target])
    client.poll_event.return_value = event(
        client_order_id=entry.client_order_id,
        basket_id="child-stop-1",
        original_basket_id="parent-1",
        price_type="stop_market",
    )

    mapped = adapter.poll_order_event()

    assert mapped.client_order_id == stop.client_order_id
    assert mapped.raw["native_parent_client_order_id"] == entry.client_order_id


def test_event_resolution_and_restore_merge_share_the_existing_client_lock(
    adapter,
    client,
    monkeypatch,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.place_order_group([entry, stop, target])
    client.poll_event.return_value = event(
        client_order_id=entry.client_order_id,
        basket_id="child-stop-1",
        original_basket_id="parent-1",
        price_type="stop_market",
    )
    resolver_entered = threading.Event()
    release_resolver = threading.Event()
    restore_built = threading.Event()
    merge_started = threading.Event()
    original_resolve = (
        rithmic_adapter_module.resolve_native_bracket_event_client_order_id
    )
    original_build = rithmic_adapter_module.build_restored_native_bracket_groups
    original_merge = rithmic_adapter_module.merge_native_bracket_groups

    def resolve(**kwargs):
        assert adapter._client_lock.locked() is True
        resolver_entered.set()
        assert release_resolver.wait(timeout=1)
        return original_resolve(**kwargs)

    def build_restored(orders):
        restored = original_build(orders)
        restore_built.set()
        return restored

    def merge_restored(*args):
        assert adapter._client_lock.locked() is True
        merge_started.set()
        return original_merge(*args)

    monkeypatch.setattr(
        rithmic_adapter_module,
        "resolve_native_bracket_event_client_order_id",
        resolve,
    )
    monkeypatch.setattr(
        rithmic_adapter_module,
        "build_restored_native_bracket_groups",
        build_restored,
    )
    monkeypatch.setattr(
        rithmic_adapter_module,
        "merge_native_bracket_groups",
        merge_restored,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        event_future = executor.submit(adapter.poll_order_event)
        assert resolver_entered.wait(timeout=1)
        stop.intent_payload["native_parent_basket_id"] = "parent-1"
        restore_future = executor.submit(adapter.restore_order_groups, [stop])
        assert restore_built.wait(timeout=1)
        assert merge_started.is_set() is False
        release_resolver.set()
        assert event_future.result(timeout=1).client_order_id == stop.client_order_id
        restore_future.result(timeout=1)


@pytest.mark.parametrize(
    ("basket_id", "remote_client_order_id", "expected_client_order_id"),
    [
        ("parent-1", "strategy-execution-long-123", "strategy-execution-long-123"),
        ("parent-1", None, "strategy-execution-long-123"),
        ("child-without-parent-1", "strategy-execution-long-123", None),
    ],
)
def test_native_bracket_event_without_parent_uses_only_exact_parent_basket(
    adapter,
    client,
    basket_id,
    remote_client_order_id,
    expected_client_order_id,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.place_order_group([entry, stop, target])
    client.poll_event.return_value = event(
        client_order_id=remote_client_order_id,
        basket_id=basket_id,
        original_basket_id=None,
    )

    mapped = adapter.poll_order_event()

    assert mapped.client_order_id == expected_client_order_id


def test_native_bracket_parent_basket_rejects_conflicting_client_identity(
    adapter,
    client,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.place_order_group([entry, stop, target])
    client.poll_event.return_value = event(
        client_order_id=stop.client_order_id,
        basket_id="parent-1",
        original_basket_id=None,
    )

    with pytest.raises(ExchangeError, match="parent_client_id_mismatch"):
        adapter.poll_order_event()


def test_ambiguous_bracket_submit_keeps_parent_tag_from_claiming_child(
    adapter,
    client,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    failure = RuntimeError("order result is ambiguous")
    client.submit_bracket.side_effect = failure

    with pytest.raises(NetworkError, match="ambiguous") as captured:
        adapter.place_order_group([entry, stop, target])

    assert captured.value.__cause__ is failure
    assert adapter._submitted_client_order_ids == {entry.client_order_id}
    assert adapter._native_bracket_parent_client_order_ids == {entry.client_order_id}
    assert adapter._native_brackets_by_parent == {}
    client.submit_bracket.side_effect = None
    client.poll_event.return_value = event(
        client_order_id=entry.client_order_id,
        basket_id="child-stop-1",
        original_basket_id=None,
        price_type="stop_market",
    )

    assert adapter.poll_order_event().client_order_id is None


def test_explicit_bracket_rejection_allows_safe_same_id_retry(adapter, client):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    failure = RuntimeError("request rejected")
    client.submit_bracket.side_effect = [
        failure,
        SimpleNamespace(basket_id="parent-1"),
    ]

    with pytest.raises(ExchangeError, match="request rejected") as captured:
        adapter.place_order_group([entry, stop, target])

    assert captured.value.__cause__ is failure
    assert adapter._submitted_client_order_ids == set()
    assert adapter._native_bracket_parent_client_order_ids == set()
    assert adapter._native_brackets_by_parent == {}
    assert adapter.place_order_group([entry, stop, target]) == "parent-1"


def test_duplicate_bracket_parent_fails_before_transport_without_state_drift(
    adapter,
    client,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter._submitted_client_order_ids.add(entry.client_order_id)

    with pytest.raises(ExchangeError, match="duplicate_rithmic_client_order_id"):
        adapter.place_order_group([entry, stop, target])

    client.submit_bracket.assert_not_called()
    assert adapter._submitted_client_order_ids == {entry.client_order_id}
    assert adapter._native_bracket_parent_client_order_ids == set()
    assert adapter._native_brackets_by_parent == {}


def test_submit_ack_and_restore_merge_share_client_lock_without_lost_update(
    adapter,
    client,
    monkeypatch,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    submit_entered = threading.Event()
    release_submit = threading.Event()
    restore_built = threading.Event()
    merge_started = threading.Event()
    original_build = rithmic_adapter_module.build_restored_native_bracket_groups
    original_merge = rithmic_adapter_module.merge_native_bracket_groups

    def submit_bracket(*args):
        assert adapter._client_lock.locked() is True
        submit_entered.set()
        assert release_submit.wait(timeout=1)
        return SimpleNamespace(basket_id="parent-1")

    def build_restored(orders):
        restored = original_build(orders)
        restore_built.set()
        return restored

    def merge_restored(*args):
        assert adapter._client_lock.locked() is True
        merge_started.set()
        return original_merge(*args)

    client.submit_bracket.side_effect = submit_bracket
    monkeypatch.setattr(
        rithmic_adapter_module,
        "build_restored_native_bracket_groups",
        build_restored,
    )
    monkeypatch.setattr(
        rithmic_adapter_module,
        "merge_native_bracket_groups",
        merge_restored,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        submit_future = executor.submit(
            adapter.place_order_group, [entry, stop, target]
        )
        assert submit_entered.wait(timeout=1)
        stop.intent_payload["native_parent_basket_id"] = "parent-1"
        restore_future = executor.submit(adapter.restore_order_groups, [stop])
        assert restore_built.wait(timeout=1)
        assert merge_started.is_set() is False
        release_submit.set()
        assert submit_future.result(timeout=1) == "parent-1"
        restore_future.result(timeout=1)

    assert adapter._native_brackets_by_parent == {
        "parent-1": {
            "entry": entry.client_order_id,
            "stop_loss": stop.client_order_id,
            "take_profit": target.client_order_id,
        }
    }


def test_unknown_bracket_parent_cannot_claim_local_child_identity(adapter, client):
    adapter.start_order_event_stream()
    client.poll_event.return_value = event(
        client_order_id="strategy-execution-long-123",
        basket_id="child-stop-1",
        original_basket_id="unknown-parent",
        price_type="stop_market",
    )

    mapped = adapter.poll_order_event()

    assert mapped.client_order_id is None


def test_native_bracket_child_identity_restores_from_persisted_parent(client):
    original = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )
    entry, stop, target = bracket_orders()
    original.validate_order_group([entry, stop, target])
    entry.exchange_order_id = "parent-1"
    restored = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )
    restored.restore_order_groups([entry, stop, target])
    restored.start_order_event_stream()
    client.poll_event.return_value = event(
        client_order_id=entry.client_order_id,
        basket_id="child-target-1",
        original_basket_id="parent-1",
        price_type="limit",
    )

    mapped = restored.poll_order_event()

    assert mapped.client_order_id == target.client_order_id


def test_native_bracket_child_identity_restores_without_terminal_parent(client):
    original = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )
    entry, stop, target = bracket_orders()
    original.validate_order_group([entry, stop, target])
    for leg in (stop, target):
        leg.intent_payload["native_parent_basket_id"] = "parent-1"
    restored = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )

    restored.restore_order_groups([stop, target])
    restored.start_order_event_stream()
    client.poll_event.return_value = event(
        client_order_id=entry.client_order_id,
        basket_id="child-stop-1",
        original_basket_id="parent-1",
        price_type="stop_market",
    )

    assert restored.poll_order_event().client_order_id == stop.client_order_id


def test_restored_bracket_parent_tag_cannot_claim_unknown_child(client):
    original = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )
    entry, stop, target = bracket_orders()
    original.validate_order_group([entry, stop, target])
    entry.exchange_order_id = "parent-1"
    restored = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )
    restored.restore_order_groups([entry, stop, target])
    restored.start_order_event_stream()
    client.poll_event.return_value = event(
        client_order_id=entry.client_order_id,
        basket_id="unknown-child-1",
        original_basket_id=None,
        price_type="stop_market",
    )

    assert restored.poll_order_event().client_order_id is None


def test_native_bracket_restore_merges_parent_with_partial_active_children(client):
    entry, stop, target = bracket_orders()
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments=INSTRUMENTS,
        client_factory=Mock(return_value=client),
    )
    adapter.validate_order_group([entry, stop, target])
    entry.exchange_order_id = "parent-1"
    stop.intent_payload["native_parent_basket_id"] = "parent-1"

    adapter.restore_order_groups([entry, stop])

    assert adapter._native_brackets_by_parent["parent-1"] == {
        "entry": entry.client_order_id,
        "stop_loss": stop.client_order_id,
        "take_profit": target.client_order_id,
    }


def test_native_bracket_restore_preserves_existing_legs_on_partial_replay(
    adapter,
    client,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.place_order_group([entry, stop, target])
    stop.intent_payload["native_parent_basket_id"] = "parent-1"

    adapter.restore_order_groups([stop])

    assert adapter._native_brackets_by_parent["parent-1"] == {
        "entry": entry.client_order_id,
        "stop_loss": stop.client_order_id,
        "take_profit": target.client_order_id,
    }


def test_native_bracket_restore_conflict_is_atomic(adapter, client):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.place_order_group([entry, stop, target])
    before_groups = {
        parent_basket_id: dict(group)
        for parent_basket_id, group in adapter._native_brackets_by_parent.items()
    }
    before_parent_ids = set(adapter._native_bracket_parent_client_order_ids)
    stop.intent_payload["native_parent_basket_id"] = "parent-1"
    stop.intent_payload["native_parent_client_order_id"] = "other-parent-client"

    with pytest.raises(ExchangeError, match="restore_metadata_conflict"):
        adapter.restore_order_groups([stop])

    assert adapter._native_brackets_by_parent == before_groups
    assert adapter._native_bracket_parent_client_order_ids == before_parent_ids


def test_modify_native_protection_uses_known_child_basket(adapter, client):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.validate_order_group([entry, stop, target])
    stop.exchange_order_id = "child-stop-1"
    stop.intent_payload["actual_entry_fill_price"] = "20000.75"

    assert adapter.modify_protection(stop, trigger_price=Decimal("19999.00"))

    client.modify_protection.assert_called_once_with(
        "child-stop-1",
        "CME",
        "NQU6",
        "1",
        "stop_loss",
        "19999.00",
    )


def test_modify_projection_precedes_the_existing_transport_lock(
    adapter,
    client,
    monkeypatch,
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.validate_order_group([entry, stop, target])
    stop.exchange_order_id = "child-stop-1"
    stop.intent_payload["actual_entry_fill_price"] = "20000.75"
    original_build = rithmic_adapter_module.build_native_protection_request

    def build_request(*args, **kwargs):
        assert adapter._client_lock.locked() is False
        return original_build(*args, **kwargs)

    def modify_protection(*args):
        assert adapter._client_lock.locked() is True
        return True

    monkeypatch.setattr(
        rithmic_adapter_module,
        "build_native_protection_request",
        build_request,
    )
    client.modify_protection.side_effect = modify_protection

    assert adapter.modify_protection(stop, trigger_price=Decimal("19999.00"))


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda stop: setattr(stop, "exchange_order_id", None), "basket_id_required"),
        (
            lambda stop: setattr(stop, "trigger_price", Decimal("19998.30")),
            "price_off_tick",
        ),
        (
            lambda stop: setattr(stop, "intent_payload", {}),
            "identity_required",
        ),
    ],
)
def test_modify_native_protection_validation_fails_before_remote_call(
    adapter, client, mutate, message
):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.validate_order_group([entry, stop, target])
    stop.exchange_order_id = "child-stop-1"
    mutate(stop)
    requested = getattr(stop, "trigger_price", Decimal("19999.00"))

    with pytest.raises(ExchangeError, match=message):
        adapter.modify_protection(stop, trigger_price=requested)

    client.modify_protection.assert_not_called()


def test_ambiguous_modify_failure_maps_to_network_error(adapter, client):
    adapter.start_order_event_stream()
    entry, stop, target = bracket_orders()
    adapter.validate_order_group([entry, stop, target])
    stop.exchange_order_id = "child-stop-1"
    client.modify_protection.side_effect = RuntimeError(
        "Rithmic modify-order result is ambiguous: disconnected"
    )

    with pytest.raises(NetworkError, match="ambiguous"):
        adapter.modify_protection(stop, trigger_price=Decimal("19999.00"))


def test_reduce_only_order_fails_before_submit(adapter, client):
    adapter.start_order_event_stream()

    with pytest.raises(ExchangeError, match="rithmic_reduce_only_unsupported"):
        adapter.place_order(
            order(intent_payload={"reduce_only": True, "source": "kill_switch"})
        )

    client.submit.assert_not_called()
    assert adapter._submitted_client_order_ids == set()


@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        (order(quantity=Decimal("1.5")), "futures_quantity_off_step"),
        (order(price=Decimal("20000.10")), "price_off_tick"),
        (order(type="stop_loss"), "rithmic_order_type_unsupported"),
        (order(client_order_id=None), "rithmic_client_order_id_required"),
    ],
)
def test_order_validation_matrix_fails_before_submit(
    adapter, client, candidate, message
):
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
    client.submit.side_effect = [
        RuntimeError("request rejected"),
        SimpleNamespace(basket_id="basket-1"),
    ]

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

    with pytest.raises(
        ExchangeError, match="unsupported_rithmic_order_snapshot_status"
    ):
        adapter.get_order_by_client_id("client-1", PRODUCT_ID)


def test_lookup_uses_cancel_notification_to_disambiguate_complete_status(
    adapter,
    client,
):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(
        status="COMPLETE",
        notification_type="CANCEL",
        filled_quantity="0",
    )

    result = adapter.get_order_by_client_id("client-1", PRODUCT_ID)

    assert result.status == "cancelled"


def test_lookup_rejects_cancel_notification_after_a_full_fill(adapter, client):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(
        status="COMPLETE",
        notification_type="CANCEL",
        filled_quantity="2",
    )

    with pytest.raises(
        ExchangeError, match="invalid_rithmic_cancel_snapshot_quantities"
    ):
        adapter.get_order_by_client_id("client-1", PRODUCT_ID)


def test_lookup_uses_reject_notification_to_disambiguate_complete_status(
    adapter,
    client,
):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(
        status="COMPLETE",
        notification_type="REJECT",
        filled_quantity="0",
    )

    result = adapter.get_order_by_client_id("client-1", PRODUCT_ID)

    assert result.status == "rejected"


def test_lookup_rejects_reject_notification_after_a_full_fill(adapter, client):
    adapter.start_order_event_stream()
    client.lookup.return_value = snapshot(
        status="COMPLETE",
        notification_type="REJECT",
        filled_quantity="2",
    )

    with pytest.raises(
        ExchangeError, match="invalid_rithmic_reject_snapshot_quantities"
    ):
        adapter.get_order_by_client_id("client-1", PRODUCT_ID)


def test_order_event_maps_native_identity_and_decimal_fields(adapter, client):
    adapter.start_order_event_stream()
    client.poll_event.return_value = event(
        original_basket_id="parent-1",
        linked_basket_ids="stop-1,target-1",
        trigger_price="19998.25",
        price_type="stop_market",
        bracket_type="target_and_stop_static",
    )

    mapped = adapter.poll_order_event()

    assert mapped.product_id == PRODUCT_ID
    assert mapped.exchange_order_id == "basket-1"
    assert mapped.cumulative_filled_quantity == Decimal("1")
    assert mapped.last_fill_price == Decimal("20000.25")
    assert mapped.raw["original_basket_id"] == "parent-1"
    assert mapped.raw["linked_basket_ids"] == "stop-1,target-1"
    assert mapped.raw["trigger_price"] == "19998.25"
    assert mapped.raw["price_type"] == "stop_market"
    assert mapped.raw["bracket_type"] == "target_and_stop_static"


def test_unknown_order_event_instrument_fails_closed(adapter, client):
    adapter.start_order_event_stream()
    client.poll_event.return_value = event(symbol="ESZ6")

    with pytest.raises(
        RithmicUnmappedOrderEvent,
        match="unknown_rithmic_order_event_instrument",
    ) as caught:
        adapter.poll_order_event()

    assert caught.value.account_id == "ACCOUNT"
    assert caught.value.exchange == "CME"
    assert caught.value.symbol == "ESZ6"


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
