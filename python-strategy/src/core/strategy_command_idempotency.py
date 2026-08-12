import hashlib
import logging
from collections.abc import Callable
from typing import Any

_KILL_SWITCH_IDEMPOTENCY_TTL_SECONDS = 86_400
_STRATEGY_COMMAND_CLAIM_TTL_SECONDS = 60
_STRATEGY_COMMAND_IDEMPOTENCY_TTL_SECONDS = 86_400


def _operation_key(
    *,
    key_builder: Callable[[str], str],
    namespace: str,
    actor: str,
    idempotency_key: str,
) -> str:
    digest = hashlib.sha256(f"{actor}\0{idempotency_key}".encode()).hexdigest()
    return key_builder(f"{namespace}:{digest}")


def claim_strategy_command_operation(
    *,
    redis_client: Any,
    key_builder: Callable[[str], str],
    actor: str,
    idempotency_key: str,
    event_logger: logging.Logger,
) -> bool:
    key = _operation_key(
        key_builder=key_builder,
        namespace="strategy-command:idempotency",
        actor=actor,
        idempotency_key=idempotency_key,
    )
    try:
        return bool(
            redis_client.set(
                key,
                "claimed",
                nx=True,
                ex=_STRATEGY_COMMAND_CLAIM_TTL_SECONDS,
            )
        )
    except Exception:
        event_logger.exception(
            "Unable to claim strategy command idempotency key; command rejected"
        )
        return False


def mark_strategy_command_operation_completed(
    *,
    redis_client: Any,
    key_builder: Callable[[str], str],
    actor: str,
    idempotency_key: str,
    event_logger: logging.Logger,
) -> None:
    key = _operation_key(
        key_builder=key_builder,
        namespace="strategy-command:idempotency",
        actor=actor,
        idempotency_key=idempotency_key,
    )
    try:
        redis_client.set(
            key,
            "completed",
            ex=_STRATEGY_COMMAND_IDEMPOTENCY_TTL_SECONDS,
        )
    except Exception:
        event_logger.exception(
            "Unable to persist completed strategy command marker; "
            "expected_version remains the retry fence"
        )


def kill_switch_operation_completed(
    *,
    redis_client: Any,
    key_builder: Callable[[str], str],
    actor: str,
    idempotency_key: str,
    event_logger: logging.Logger,
) -> bool:
    key = _operation_key(
        key_builder=key_builder,
        namespace="ops:kill-switch:idempotency",
        actor=actor,
        idempotency_key=idempotency_key,
    )
    try:
        marker = redis_client.get(key)
        return marker in {"completed", b"completed"}
    except Exception:
        event_logger.exception(
            "Unable to read kill switch idempotency marker; retrying safely"
        )
        return False


def mark_kill_switch_operation_completed(
    *,
    redis_client: Any,
    key_builder: Callable[[str], str],
    actor: str,
    idempotency_key: str,
    event_logger: logging.Logger,
) -> None:
    key = _operation_key(
        key_builder=key_builder,
        namespace="ops:kill-switch:idempotency",
        actor=actor,
        idempotency_key=idempotency_key,
    )
    try:
        redis_client.set(
            key,
            "completed",
            ex=_KILL_SWITCH_IDEMPOTENCY_TTL_SECONDS,
        )
    except Exception:
        event_logger.exception(
            "Unable to persist kill switch idempotency marker; "
            "future retries will execute safely"
        )
