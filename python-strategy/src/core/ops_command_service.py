import logging
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from src.core.runtime_capabilities import KillSwitchClearPreparation


class OpsCommandService:
    """Run the existing kill-switch command transactions."""

    def __init__(
        self,
        *,
        operation_lock: Callable[[], AbstractContextManager[object]],
        kill_switch_operation_completed: Callable[..., bool],
        halt_for_kill_switch: Callable[[], None],
        persist_lockdown_database: Callable[..., None],
        persist_lockdown_redis: Callable[[], object],
        run_kill_switch: Callable[..., dict],
        mark_kill_switch_halted: Callable[[], None],
        requires_authoritative_verification: Callable[[], bool],
        kill_switch_result_is_complete: Callable[..., bool],
        mark_kill_switch_operation_completed: Callable[..., None],
        prepare_kill_switch_clear: Callable[[], KillSwitchClearPreparation],
        assert_leadership: Callable[[], None],
        clear_kill_switch: Callable[..., dict],
        persist_clear_database: Callable[..., None],
        persist_clear_redis: Callable[[], object],
        clear_local_halt: Callable[[], None],
        finalize_external_drift_clear: Callable[..., None],
        event_logger: Callable[[], logging.Logger],
    ) -> None:
        self._operation_lock = operation_lock
        self._kill_switch_operation_completed = kill_switch_operation_completed
        self._halt_for_kill_switch = halt_for_kill_switch
        self._persist_lockdown_database = persist_lockdown_database
        self._persist_lockdown_redis = persist_lockdown_redis
        self._run_kill_switch = run_kill_switch
        self._mark_kill_switch_halted = mark_kill_switch_halted
        self._requires_authoritative_verification = requires_authoritative_verification
        self._kill_switch_result_is_complete = kill_switch_result_is_complete
        self._mark_kill_switch_operation_completed = (
            mark_kill_switch_operation_completed
        )
        self._prepare_kill_switch_clear = prepare_kill_switch_clear
        self._assert_leadership = assert_leadership
        self._clear_kill_switch = clear_kill_switch
        self._persist_clear_database = persist_clear_database
        self._persist_clear_redis = persist_clear_redis
        self._clear_local_halt = clear_local_halt
        self._finalize_external_drift_clear = finalize_external_drift_clear
        self._event_logger = event_logger

    def handle_kill_switch(self, params: dict[str, Any]) -> None:
        with self._operation_lock():
            actor = params.get("actor", "operator")
            reason = params.get("reason")
            idempotency_key = params.get("idempotency_key")
            if isinstance(
                idempotency_key, str
            ) and self._kill_switch_operation_completed(
                actor=str(actor),
                idempotency_key=idempotency_key,
            ):
                self._event_logger().info(
                    "Skipping completed kill switch operation for actor %s",
                    actor,
                )
                return

            self._halt_for_kill_switch()
            operation_id = idempotency_key if isinstance(idempotency_key, str) else None
            persisted_database = self._persist_lockdown_in_database(
                actor=actor,
                reason=reason,
                operation_id=operation_id,
            )
            persisted_redis = self._persist_lockdown_in_redis()
            kill_switch_kwargs = {"actor": actor, "reason": reason}
            if operation_id is not None:
                kill_switch_kwargs["operation_id"] = operation_id
            result = self._run_kill_switch(**kill_switch_kwargs)
            self._mark_kill_switch_halted()

            if (
                operation_id is not None
                and persisted_database
                and persisted_redis
                and self._kill_switch_result_is_complete(
                    result,
                    authoritative_required=(
                        self._requires_authoritative_verification()
                    ),
                )
            ):
                self._mark_kill_switch_operation_completed(
                    actor=str(actor),
                    idempotency_key=operation_id,
                )

    def _persist_lockdown_in_database(
        self,
        *,
        actor: object,
        reason: object,
        operation_id: str | None,
    ) -> bool:
        try:
            kwargs = {"actor": actor, "reason": reason}
            if operation_id is not None:
                kwargs["operation_id"] = operation_id
            self._persist_lockdown_database(**kwargs)
            return True
        except Exception:
            self._event_logger().exception(
                "Failed to persist kill switch state to database"
            )
            return False

    def _persist_lockdown_in_redis(self) -> bool:
        try:
            self._persist_lockdown_redis()
            return True
        except Exception:
            self._event_logger().exception(
                "Failed to persist kill switch state; local halt remains active"
            )
            return False

    def handle_clear_kill_switch(self, params: dict[str, Any]) -> None:
        with self._operation_lock():
            actor = params.get("actor", "operator")
            reason = params.get("reason")
            preparation = self._prepare_kill_switch_clear()
            if not preparation.allowed:
                self._event_logger().warning(
                    "Kill switch clear rejected: %s",
                    preparation.blocking_reason,
                )
                return

            generation = preparation.drift_generation
            self._assert_leadership()

            def persist_clear() -> None:
                self._assert_leadership()
                self._persist_clear_database(actor=actor, reason=reason)
                self._assert_leadership()
                self._persist_clear_redis()
                self._assert_leadership()

            clear_succeeded = False
            try:
                result = self._clear_kill_switch(persist_clear=persist_clear)
                clear_succeeded = bool(result["cleared"])
                if generation is None and clear_succeeded:
                    self._assert_leadership()
                    self._clear_local_halt()
                elif not clear_succeeded:
                    self._event_logger().warning(
                        "Kill switch clear rejected: %s",
                        result["reason"],
                    )
            finally:
                if generation is not None:
                    self._finalize_external_drift_clear(
                        prepared_generation=generation,
                        clear_succeeded=clear_succeeded,
                    )
