import hashlib
from unittest.mock import MagicMock, call

import pytest

from src.core.strategy_command_idempotency import (
    claim_strategy_command_operation,
    kill_switch_operation_completed,
    mark_kill_switch_operation_completed,
    mark_strategy_command_operation_completed,
)


def _expected_key(namespace: str) -> str:
    digest = hashlib.sha256(b"operator\0request-1").hexdigest()
    return f"live:{namespace}:{digest}"


def _key_builder(value: str) -> str:
    return f"live:{value}"


def test_strategy_command_claim_and_completion_preserve_exact_redis_contract() -> None:
    redis_client = MagicMock()
    redis_client.set.side_effect = [True, None]
    event_logger = MagicMock()

    assert (
        claim_strategy_command_operation(
            redis_client=redis_client,
            key_builder=_key_builder,
            actor="operator",
            idempotency_key="request-1",
            event_logger=event_logger,
        )
        is True
    )
    mark_strategy_command_operation_completed(
        redis_client=redis_client,
        key_builder=_key_builder,
        actor="operator",
        idempotency_key="request-1",
        event_logger=event_logger,
    )

    assert redis_client.set.call_args_list == [
        call(
            _expected_key("strategy-command:idempotency"),
            "claimed",
            nx=True,
            ex=60,
        ),
        call(
            _expected_key("strategy-command:idempotency"),
            "completed",
            ex=86_400,
        ),
    ]
    event_logger.exception.assert_not_called()


def test_strategy_command_redis_failures_preserve_rejection_and_log_envelopes() -> None:
    redis_client = MagicMock()
    redis_client.set.side_effect = RuntimeError("redis unavailable")
    event_logger = MagicMock()

    assert (
        claim_strategy_command_operation(
            redis_client=redis_client,
            key_builder=_key_builder,
            actor="operator",
            idempotency_key="request-1",
            event_logger=event_logger,
        )
        is False
    )
    event_logger.exception.assert_called_once_with(
        "Unable to claim strategy command idempotency key; command rejected"
    )

    event_logger.reset_mock()
    mark_strategy_command_operation_completed(
        redis_client=redis_client,
        key_builder=_key_builder,
        actor="operator",
        idempotency_key="request-1",
        event_logger=event_logger,
    )
    event_logger.exception.assert_called_once_with(
        "Unable to persist completed strategy command marker; "
        "expected_version remains the retry fence"
    )


@pytest.mark.parametrize(
    ("marker", "expected"),
    [
        ("completed", True),
        (b"completed", True),
        (None, False),
        ("claimed", False),
    ],
)
def test_kill_switch_completion_read_preserves_marker_contract(
    marker: object,
    expected: bool,
) -> None:
    redis_client = MagicMock()
    redis_client.get.return_value = marker
    event_logger = MagicMock()

    assert (
        kill_switch_operation_completed(
            redis_client=redis_client,
            key_builder=_key_builder,
            actor="operator",
            idempotency_key="request-1",
            event_logger=event_logger,
        )
        is expected
    )
    redis_client.get.assert_called_once_with(
        _expected_key("ops:kill-switch:idempotency")
    )
    event_logger.exception.assert_not_called()


def test_kill_switch_completion_read_failure_retries_safely() -> None:
    redis_client = MagicMock()
    redis_client.get.side_effect = RuntimeError("redis unavailable")
    event_logger = MagicMock()

    assert (
        kill_switch_operation_completed(
            redis_client=redis_client,
            key_builder=_key_builder,
            actor="operator",
            idempotency_key="request-1",
            event_logger=event_logger,
        )
        is False
    )
    event_logger.exception.assert_called_once_with(
        "Unable to read kill switch idempotency marker; retrying safely"
    )


def test_kill_switch_completion_write_preserves_exact_contract_and_failure_log() -> (
    None
):
    redis_client = MagicMock()
    event_logger = MagicMock()

    mark_kill_switch_operation_completed(
        redis_client=redis_client,
        key_builder=_key_builder,
        actor="operator",
        idempotency_key="request-1",
        event_logger=event_logger,
    )
    redis_client.set.assert_called_once_with(
        _expected_key("ops:kill-switch:idempotency"),
        "completed",
        ex=86_400,
    )

    redis_client.set.side_effect = RuntimeError("redis unavailable")
    event_logger.reset_mock()
    mark_kill_switch_operation_completed(
        redis_client=redis_client,
        key_builder=_key_builder,
        actor="operator",
        idempotency_key="request-1",
        event_logger=event_logger,
    )
    event_logger.exception.assert_called_once_with(
        "Unable to persist kill switch idempotency marker; "
        "future retries will execute safely"
    )
