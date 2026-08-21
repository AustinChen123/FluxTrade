from decimal import Decimal
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core import execution_verified_net_reduction
from src.core.execution import ExecutionEngine
from src.core.interfaces.verified_net_reduction import (
    VerifiedNetReductionOrderSnapshot,
    VerifiedNetReductionRepository,
)
from src.core.models import OrderSide, OrderStatus, SignalType


def _order(signal, **changes):
    order = VerifiedNetReductionOrderSnapshot(
        id="order-1",
        client_order_id="client-1",
        strategy_id=signal.strategy_id,
        product_id=signal.product_id,
        type="market",
        side="sell",
        quantity=Decimal("2.00"),
        filled_quantity=Decimal("2.00"),
        status=OrderStatus.FILLED.value,
        intent_payload={
            "source": "authoritative_net_reduction",
            "signal": {"type": signal.type.value},
        },
    )
    return replace(order, **changes)


def test_snapshot_detaches_and_freezes_the_source_payload(signal_factory):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    source_payload = {
        "source": "authoritative_net_reduction",
        "signal": {"type": signal.type.value},
    }

    snapshot = _order(signal, intent_payload=source_payload)
    source_payload["source"] = "mutated"
    source_payload["extra"] = True

    assert type(snapshot.intent_payload) is MappingProxyType
    assert snapshot.intent_payload == {
        "source": "authoritative_net_reduction",
        "signal": {"type": signal.type.value},
    }


@pytest.mark.parametrize(
    ("changes", "expected_error"),
    [
        ({"strategy_id": "other"}, "identity_mismatch"),
        ({"product_id": "BINANCE:ETHUSDT-PERP"}, "identity_mismatch"),
        ({"type": "limit"}, "identity_mismatch"),
        ({"side": "buy"}, "identity_mismatch"),
        ({"quantity": Decimal("0")}, "identity_mismatch"),
        ({"quantity": Decimal("NaN")}, "identity_mismatch"),
        ({"filled_quantity": Decimal("1")}, "identity_mismatch"),
        ({"status": OrderStatus.NEW.value}, "identity_mismatch"),
        ({"intent_payload": None}, "identity_mismatch"),
        ({"intent_payload": {}}, "identity_mismatch"),
        (
            {
                "intent_payload": {
                    "source": "other",
                    "signal": {"type": SignalType.EXIT_LONG.value},
                }
            },
            "identity_mismatch",
        ),
        (
            {
                "intent_payload": {
                    "source": "authoritative_net_reduction",
                    "signal": SignalType.EXIT_LONG.value,
                }
            },
            "identity_mismatch",
        ),
        (
            {
                "intent_payload": {
                    "source": "authoritative_net_reduction",
                    "signal": {"type": SignalType.EXIT_SHORT.value},
                }
            },
            "identity_mismatch",
        ),
    ],
)
def test_validated_order_payload_rejects_each_identity_mismatch(
    signal_factory,
    changes,
    expected_error,
):
    signal = signal_factory(
        signal_type=SignalType.EXIT_LONG,
        quantity=Decimal("2"),
    )

    with pytest.raises(RuntimeError, match=expected_error):
        execution_verified_net_reduction.validated_order_payload(
            signal,
            _order(signal, **changes),
            expected_side=OrderSide.SELL,
        )


@pytest.mark.parametrize(
    ("signal_type", "side", "expected_side"),
    [
        (SignalType.EXIT_LONG, "sell", OrderSide.SELL),
        (SignalType.EXIT_SHORT, "buy", OrderSide.BUY),
    ],
)
def test_validated_order_payload_returns_exact_mapping(
    signal_factory,
    signal_type,
    side,
    expected_side,
):
    signal = signal_factory(signal_type=signal_type)
    order = _order(signal, side=side)

    assert (
        execution_verified_net_reduction.validated_order_payload(
            signal,
            order,
            expected_side=expected_side,
        )
        is order.intent_payload
    )


def test_validated_order_payload_rejects_unresolved_side(signal_factory):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)

    with pytest.raises(RuntimeError, match="identity_mismatch"):
        execution_verified_net_reduction.validated_order_payload(
            signal,
            _order(signal),
            expected_side=None,
        )


def test_completed_replay_uses_lookup_and_exact_verification(signal_factory):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    order = _order(signal)

    assert (
        execution_verified_net_reduction.completed_replay(
            signal,
            None,
            expected_side=OrderSide.SELL,
        )
        is False
    )

    order = replace(
        order,
        intent_payload={
            **dict(order.intent_payload or {}),
            "authoritative_verification": {
                "status": "verified_portfolio_reduction",
                "strategy_id": signal.strategy_id,
                "product_id": signal.product_id,
            },
        },
    )
    assert (
        execution_verified_net_reduction.completed_replay(
            signal,
            order,
            expected_side=OrderSide.SELL,
        )
        is True
    )


@pytest.mark.parametrize(
    "verification",
    [
        None,
        {},
        {"status": "other"},
        {
            "status": "verified_portfolio_reduction",
            "strategy_id": "other",
        },
        {
            "status": "verified_portfolio_reduction",
            "strategy_id": "test-strategy",
            "product_id": "BINANCE:ETHUSDT-PERP",
        },
    ],
)
def test_completed_replay_rejects_missing_or_mismatched_verification(
    signal_factory,
    verification,
):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    order = _order(signal)
    if verification is not None:
        order = replace(
            order,
            intent_payload={
                **dict(order.intent_payload or {}),
                "authoritative_verification": verification,
            },
        )
    with pytest.raises(
        RuntimeError,
        match="authoritative_exit_replay_verification_missing",
    ):
        execution_verified_net_reduction.completed_replay(
            signal,
            order,
            expected_side=OrderSide.SELL,
        )


@pytest.mark.parametrize("remaining", [Decimal("-1"), Decimal("NaN")])
def test_record_verification_rejects_invalid_remaining_before_lookup(
    signal_factory,
    remaining,
):
    repository = MagicMock()
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)

    with pytest.raises(
        ValueError,
        match="verified_net_reduction_remaining_quantity_invalid",
    ):
        execution_verified_net_reduction.record_verification(
            repository,
            signal,
            _order(signal),
            client_order_id="client-1",
            expected_side=OrderSide.SELL,
            remaining_remote_quantity=remaining,
        )

    repository.persist_verified_net_reduction.assert_not_called()


def test_record_verification_requires_client_identity(signal_factory):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    repository = MagicMock()
    with pytest.raises(
        RuntimeError,
        match="verified_net_reduction_order_identity_mismatch",
    ):
        execution_verified_net_reduction.record_verification(
            repository,
            signal,
            _order(signal, client_order_id="other"),
            client_order_id="client-1",
            expected_side=OrderSide.SELL,
            remaining_remote_quantity=Decimal("0"),
        )
    repository.persist_verified_net_reduction.assert_not_called()


def test_record_verification_copies_payload_and_updates_once(signal_factory):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    order = _order(signal)
    original_payload = order.intent_payload
    assert original_payload is not None
    repository = MagicMock()

    execution_verified_net_reduction.record_verification(
        repository,
        signal,
        order,
        client_order_id="client-1",
        expected_side=OrderSide.SELL,
        remaining_remote_quantity=Decimal("0.00"),
    )

    assert order.intent_payload is original_payload
    assert "authoritative_verification" not in original_payload
    assert type(original_payload) is MappingProxyType
    repository.persist_verified_net_reduction.assert_called_once()
    order_id, persisted_payload = (
        repository.persist_verified_net_reduction.call_args.args
    )
    assert order_id == "order-1"
    assert persisted_payload is not original_payload
    assert persisted_payload["authoritative_verification"] == {
        "status": "verified_portfolio_reduction",
        "strategy_id": signal.strategy_id,
        "product_id": signal.product_id,
        "remaining_remote_quantity": "0.00",
    }


def test_record_verification_preserves_persistence_failure_identity(signal_factory):
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    order = _order(signal)
    original_payload = order.intent_payload
    repository = MagicMock(spec=VerifiedNetReductionRepository)
    failure = RuntimeError("persistence-sentinel")
    repository.persist_verified_net_reduction.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        execution_verified_net_reduction.record_verification(
            repository,
            signal,
            order,
            client_order_id="client-1",
            expected_side=OrderSide.SELL,
            remaining_remote_quantity=Decimal("0"),
        )

    assert raised.value is failure
    assert order.intent_payload is original_payload
    repository.persist_verified_net_reduction.assert_called_once()


def test_execution_facades_resolve_current_dependencies(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    signal_factory,
):
    execution_engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    order = _order(signal)
    replacement_repository = MagicMock(spec=VerifiedNetReductionRepository)
    execution_engine.order_manager.repo = replacement_repository

    with (
        patch.object(
            execution_engine,
            "_client_order_id_for_signal",
            return_value="current-client",
        ) as client_id,
        patch.object(
            execution_engine,
            "_determine_side",
            return_value=OrderSide.SELL,
        ) as determine_side,
        patch.object(
            execution_verified_net_reduction,
            "completed_replay",
            return_value=True,
        ) as completed,
        patch.object(
            execution_verified_net_reduction,
            "record_verification",
        ) as record,
    ):
        replacement_repository.get_verified_net_reduction_order.return_value = order
        replacement_repository.get_verified_net_reduction_order_by_client_id.return_value = order
        assert execution_engine._completed_verified_net_reduction_replay(signal)
        execution_engine.record_verified_net_reduction(
            signal,
            str(order.id),
            remaining_remote_quantity=Decimal("0"),
        )

    assert client_id.call_count == 2
    assert determine_side.call_count == 2
    completed.assert_called_once_with(
        signal,
        order,
        expected_side=OrderSide.SELL,
    )
    replacement_repository.get_verified_net_reduction_order_by_client_id.assert_called_once_with(
        "current-client"
    )
    record.assert_called_once_with(
        replacement_repository,
        signal,
        order,
        client_order_id="current-client",
        expected_side=OrderSide.SELL,
        remaining_remote_quantity=Decimal("0"),
    )


@pytest.mark.parametrize(
    ("remaining", "expected_error"),
    [
        (Decimal("-1"), "remaining_quantity_invalid"),
        (Decimal("0"), "order_missing"),
    ],
)
def test_execution_record_facade_preserves_error_precedence(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    signal_factory,
    remaining,
    expected_error,
):
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )
    mock_order_repo.get_verified_net_reduction_order = MagicMock(return_value=None)
    engine._client_order_id_for_signal = MagicMock(
        side_effect=AssertionError("client id resolved too early")
    )

    with pytest.raises((ValueError, RuntimeError), match=expected_error):
        engine.record_verified_net_reduction(
            signal_factory(signal_type=SignalType.EXIT_LONG),
            "order-1",
            remaining_remote_quantity=remaining,
        )

    engine._client_order_id_for_signal.assert_not_called()


def test_execution_replay_missing_order_does_not_resolve_side(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    signal_factory,
):
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )
    engine._determine_side = MagicMock(
        side_effect=AssertionError("side resolved without an order")
    )
    mock_order_repo.get_verified_net_reduction_order_by_client_id = MagicMock(
        return_value=None
    )

    assert (
        engine._completed_verified_net_reduction_replay(
            signal_factory(signal_type=SignalType.EXIT_LONG)
        )
        is False
    )
    engine._determine_side.assert_not_called()


def test_execution_facades_fail_before_repository_io_without_capability(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    signal_factory,
):
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )
    missing_capability = SimpleNamespace()
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    engine._determine_side = MagicMock(
        side_effect=AssertionError("side resolved without repository capability")
    )

    with patch.object(engine.order_manager, "repo", missing_capability):
        with pytest.raises(
            ValueError,
            match="^verified_net_reduction_remaining_quantity_invalid$",
        ):
            engine.record_verified_net_reduction(
                signal,
                "order-1",
                remaining_remote_quantity=Decimal("-1"),
            )
        with pytest.raises(
            RuntimeError,
            match="^verified_net_reduction_repository_capability_required$",
        ):
            engine._completed_verified_net_reduction_replay(signal)
        with pytest.raises(
            RuntimeError,
            match="^verified_net_reduction_repository_capability_required$",
        ):
            engine.record_verified_net_reduction(
                signal,
                "order-1",
                remaining_remote_quantity=Decimal("0"),
            )

    assert vars(missing_capability) == {}
    engine._determine_side.assert_not_called()


def test_execution_facades_preserve_lookup_failure_identity(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    signal_factory,
):
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )
    signal = signal_factory(signal_type=SignalType.EXIT_LONG)
    failure = RuntimeError("lookup-sentinel")
    mock_order_repo.get_verified_net_reduction_order_by_client_id = MagicMock(
        side_effect=failure
    )
    engine._determine_side = MagicMock(
        side_effect=AssertionError("side resolved after lookup failure")
    )

    with pytest.raises(RuntimeError) as replay_raised:
        engine._completed_verified_net_reduction_replay(signal)

    assert replay_raised.value is failure
    engine._determine_side.assert_not_called()

    mock_order_repo.get_verified_net_reduction_order = MagicMock(side_effect=failure)
    engine._client_order_id_for_signal = MagicMock(
        side_effect=AssertionError("client id resolved after lookup failure")
    )
    with pytest.raises(RuntimeError) as record_raised:
        engine.record_verified_net_reduction(
            signal,
            "order-1",
            remaining_remote_quantity=Decimal("0"),
        )

    assert record_raised.value is failure
    engine._client_order_id_for_signal.assert_not_called()


def test_owner_has_no_submission_or_provider_dependencies():
    source = Path(execution_verified_net_reduction.__file__).read_text()

    for forbidden in (
        "adapter",
        "audit",
        "submission_gate",
        "logger",
        "rithmic",
    ):
        assert forbidden not in source.lower()
