import json
from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from src.core.execution import ExecutionEngine
from src.core.models import SignalType
from src.core.orm_models import Order, Strategy
from src.core.repositories import LiveOrderRepository
import src.core.execution as execution_module
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.client_order_id import generate_client_order_id


@pytest.fixture
def execution_engine(mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
    return ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )


@pytest.mark.parametrize("declaration", [None, "NONE", "SL", "TP", "SL+TP"])
def test_entry_declaration_snapshot_state_matrix(
    execution_engine, mock_db_session, declaration
):
    configuration = {}
    if declaration is not None:
        configuration = {
            "system_placed_protection": declaration,
            "system_placed_protection_revision": "catalog-r7",
        }
    mock_db_session.get.return_value = Strategy(
        id="test_strategy",
        name="Test strategy",
        configuration_json=json.dumps(configuration),
    )
    signal = type(
        "SignalStub",
        (),
        {"type": SignalType.LONG, "strategy_id": "test_strategy"},
    )()

    snapshot = execution_engine._entry_protection_snapshot(signal)

    assert snapshot == {
        "system_placed_protection": declaration or "UNDECLARED",
        "system_placed_protection_revision": "catalog-r7" if declaration else None,
    }


@pytest.mark.parametrize("audit", [False, True], ids=["signal-order", "audited-order"])
def test_entry_paths_persist_the_same_immutable_declaration_snapshot(
    execution_engine,
    mock_db_session,
    signal_factory,
    audit,
):
    execution_engine.audit_external_orders = audit
    execution_engine._db_session_factory = lambda: nullcontext(mock_db_session)
    mock_db_session.get.return_value = Strategy(
        id="test_strategy",
        name="Test strategy",
        configuration_json=json.dumps(
            {
                "system_placed_protection": "SL+TP",
                "system_placed_protection_revision": "catalog-r7",
            }
        ),
    )
    signal = signal_factory(signal_type=SignalType.LONG)

    order_id = execution_engine.execute_signal(signal)

    assert order_id is not None
    order = execution_engine.order_manager.repo.get_order(order_id)
    assert order is not None
    assert order.intent_payload["system_placed_protection"] == "SL+TP"
    assert order.intent_payload["system_placed_protection_revision"] == "catalog-r7"


@pytest.mark.parametrize("constructor", ["signal-order", "native-bracket"])
def test_changed_declaration_applies_only_to_new_entries_and_retry_keeps_old_snapshot(
    sqlite_order_session_factory,
    mock_clock,
    mock_exchange_adapter,
    signal_factory,
    constructor,
    monkeypatch,
):
    session_factory = sqlite_order_session_factory
    monkeypatch.setattr(execution_module, "write_signal_audit_intent", lambda *_a: None)
    monkeypatch.setattr(execution_module, "write_signal_audit_outcome", lambda *_a, **_k: None)
    with session_factory() as db:
        strategy = db.get(Strategy, "test_strategy")
        assert strategy is not None
        strategy.configuration_json = json.dumps(
            {
                "system_placed_protection": "SL",
                "system_placed_protection_revision": "catalog-old",
            }
        )
        db.commit()
    if constructor == "native-bracket":
        basket_ids = iter(("basket-native-1", "basket-native-2"))
        client = SimpleNamespace(
            submit_bracket=lambda *_args: SimpleNamespace(basket_id=next(basket_ids))
        )
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=cast(Any, lambda *_args: client),
        )
        adapter.start_order_event_stream()
        product_id = "RITHMIC:NQ-202609"
    else:
        adapter = mock_exchange_adapter
        product_id = "BINANCE:BTCUSDT-PERP"
    repository = LiveOrderRepository(db_session_factory=session_factory)
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=adapter,
        order_repository=repository,
        is_backtest=True,
        db_session_factory=session_factory,
        audit_external_orders=True,
    )
    observed_payloads = []
    if constructor == "native-bracket":
        submit = adapter.place_order_group

        def observe_group(orders):
            with session_factory() as db:
                persisted = db.get(Order, orders[0].id)
                assert persisted is not None
                observed_payloads.append(dict(persisted.intent_payload or {}))
            return submit(orders)

        adapter.place_order_group = observe_group
    else:
        def observe_order(order):
            with session_factory() as db:
                persisted = db.get(Order, order.id)
                assert persisted is not None
                observed_payloads.append(dict(persisted.intent_payload or {}))
            return f"exchange-{order.id}"

        adapter.place_order = observe_order

    first_signal = signal_factory(
        product_id=product_id,
        timestamp=1_704_067_200_000,
        quantity=Decimal("1") if constructor == "native-bracket" else None,
        metadata={
            "client_order_id": generate_client_order_id(
                "test_strategy", "execution", "long"
            )
        },
        price=Decimal("20000.25") if constructor == "native-bracket" else None,
        stop_loss=Decimal("19998.25") if constructor == "native-bracket" else None,
        take_profit=Decimal("20003.25") if constructor == "native-bracket" else None,
    )
    first_id = execution.execute_signal(first_signal)
    assert first_id is not None
    first = repository.get_order(first_id)
    assert first is not None

    with session_factory() as db:
        strategy = db.get(Strategy, "test_strategy")
        assert strategy is not None
        strategy.configuration_json = json.dumps(
            {
                "system_placed_protection": "TP",
                "system_placed_protection_revision": "catalog-new",
            }
        )
        db.commit()

    assert execution.execute_signal(first_signal) == first_id
    new_signal = signal_factory(
        product_id=product_id,
        timestamp=1_704_067_260_000,
        quantity=Decimal("1") if constructor == "native-bracket" else None,
        metadata={
            "client_order_id": generate_client_order_id(
                "test_strategy", "execution", "long"
            )
        },
        price=Decimal("20000.25") if constructor == "native-bracket" else None,
        stop_loss=Decimal("19998.25") if constructor == "native-bracket" else None,
        take_profit=Decimal("20003.25") if constructor == "native-bracket" else None,
    )
    second_id = execution.execute_signal(new_signal)

    assert second_id is not None and second_id != first_id
    second = repository.get_order(second_id)
    assert second is not None
    assert isinstance(first.intent_payload, dict)
    assert isinstance(second.intent_payload, dict)
    assert first.intent_payload["system_placed_protection"] == "SL"
    assert first.intent_payload["system_placed_protection_revision"] == "catalog-old"
    assert second.intent_payload["system_placed_protection"] == "TP"
    assert second.intent_payload["system_placed_protection_revision"] == "catalog-new"
    assert [payload["system_placed_protection_revision"] for payload in observed_payloads] == [
        "catalog-old",
        "catalog-new",
    ]


def test_native_bracket_entry_persists_snapshot_before_atomic_adapter_submission(
    mock_db_session,
    mock_clock,
    mock_order_repo,
    signal_factory,
):
    client = cast(
        Any,
        SimpleNamespace(
            submit_bracket=lambda *args: SimpleNamespace(basket_id="basket-1")
        ),
    )
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=cast(Any, lambda *_args: client),
    )
    adapter.start_order_event_stream()
    mock_db_session.get.return_value = Strategy(
        id="test_strategy",
        name="Test strategy",
        configuration_json=json.dumps(
            {
                "system_placed_protection": "SL+TP",
                "system_placed_protection_revision": "catalog-r7",
            }
        ),
    )
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=adapter,
        order_repository=mock_order_repo,
        db_session_factory=cast(
            Any,
            lambda: nullcontext(mock_db_session),
        ),
        audit_external_orders=True,
        is_backtest=True,
    )

    order_id = engine.execute_signal(
        signal_factory(
            product_id="RITHMIC:NQ-202609",
            price=Decimal("20000.25"),
            quantity=Decimal("1"),
            stop_loss=Decimal("19998.25"),
            take_profit=Decimal("20003.25"),
        )
    )

    assert order_id is not None
    entry = mock_order_repo.get_order(order_id)
    assert entry is not None
    assert entry.intent_payload["system_placed_protection"] == "SL+TP"
    assert entry.intent_payload["system_placed_protection_revision"] == "catalog-r7"
    assert entry.exchange_order_id == "basket-1"


def test_entry_intent_persistence_failure_precedes_adapter_submission(
    execution_engine,
    mock_db_session,
    monkeypatch,
    signal_factory,
):
    place_order = Mock()
    monkeypatch.setattr(execution_engine.adapter, "place_order", place_order)
    execution_engine._db_session_factory = lambda: nullcontext(mock_db_session)
    mock_db_session.get.return_value = Strategy(
        id="test_strategy",
        name="Test strategy",
        configuration_json='{"system_placed_protection":"SL",'
        '"system_placed_protection_revision":"catalog-r7"}',
    )
    execution_engine.order_manager.create_order = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("intent persistence failure")
    )

    with pytest.raises(RuntimeError, match="intent persistence failure"):
        execution_engine.execute_signal(signal_factory(signal_type=SignalType.LONG))

    place_order.assert_not_called()


@pytest.mark.parametrize("submission_api", ["audited", "native"])
def test_intent_persistence_failure_precedes_audited_and_native_submission(
    submission_api,
    execution_engine,
    mock_db_session,
    mock_clock,
    mock_order_repo,
    monkeypatch,
    signal_factory,
):
    mock_db_session.get.return_value = Strategy(
        id="test_strategy",
        name="Test strategy",
        configuration_json=json.dumps(
            {
                "system_placed_protection": "SL+TP",
                "system_placed_protection_revision": "catalog-r7",
            }
        ),
    )
    if submission_api == "native":
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
        )
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            is_backtest=True,
            db_session_factory=cast(Any, lambda: nullcontext(mock_db_session)),
            audit_external_orders=True,
        )
        submit = Mock()
        monkeypatch.setattr(adapter, "place_order_group", submit)
        signal = signal_factory(
            product_id="RITHMIC:NQ-202609",
            quantity=Decimal("1"),
            price=Decimal("20000.25"),
            stop_loss=Decimal("19998.25"),
            take_profit=Decimal("20003.25"),
        )
    else:
        engine = execution_engine
        engine.audit_external_orders = True
        engine._db_session_factory = lambda: nullcontext(mock_db_session)
        submit = Mock()
        monkeypatch.setattr(engine.adapter, "place_order", submit)
        signal = signal_factory(signal_type=SignalType.LONG)

    def reject_intent_persistence(**_kwargs):
        raise RuntimeError("intent persistence failure")

    monkeypatch.setattr(engine.order_manager, "create_order", reject_intent_persistence)
    with pytest.raises(RuntimeError, match="intent persistence failure"):
        engine.execute_signal(signal)

    submit.assert_not_called()


def test_existing_order_retry_returns_persisted_snapshot_without_config_reread(
    execution_engine,
    mock_db_session,
    signal_factory,
    order_factory,
):
    signal = signal_factory(signal_type=SignalType.LONG)
    client_order_id = execution_engine._client_order_id_for_signal(signal)
    signal = signal_factory(
        signal_type=SignalType.LONG,
        metadata={"client_order_id": client_order_id},
    )
    existing = order_factory(
        order_id="existing-entry",
        client_order_id=client_order_id,
        status="SUBMITTED",
    )
    existing.intent_payload = {
        "system_placed_protection": "TP",
        "system_placed_protection_revision": "entry-revision",
    }
    execution_engine.order_manager.repo.add_order(existing)
    execution_engine.audit_external_orders = True
    execution_engine._db_session_factory = lambda: nullcontext(mock_db_session)
    execution_engine._entry_protection_snapshot = lambda _signal: (_ for _ in ()).throw(
        AssertionError("retry reread mutable strategy configuration")
    )

    result = execution_engine.execute_signal(signal)

    assert result == existing.id
    assert existing.intent_payload["system_placed_protection"] == "TP"


def test_unavailable_configuration_produces_explicit_undeclared_snapshot(
    execution_engine, mock_db_session
):
    mock_db_session.get.return_value = None
    signal = type(
        "SignalStub",
        (),
        {"type": SignalType.SHORT, "strategy_id": "legacy"},
    )()

    assert execution_engine._entry_protection_snapshot(signal) == {
        "system_placed_protection": "UNDECLARED",
        "system_placed_protection_revision": None,
    }


@pytest.mark.parametrize(
    "configuration",
    [
        '{"system_placed_protection":"STOP"}',
        '{"system_placed_protection":"SL"}',
    ],
)
def test_invalid_or_unversioned_entry_configuration_rejects_before_submission(
    execution_engine,
    mock_db_session,
    monkeypatch,
    signal_factory,
    configuration,
):
    place_order = Mock()
    monkeypatch.setattr(execution_engine.adapter, "place_order", place_order)
    mock_db_session.get.return_value = Strategy(
        id="test_strategy",
        name="Test strategy",
        configuration_json=configuration,
    )

    with pytest.raises(ValueError):
        execution_engine.execute_signal(signal_factory(signal_type=SignalType.LONG))

    place_order.assert_not_called()


