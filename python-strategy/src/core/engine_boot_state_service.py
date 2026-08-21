"""Durable engine boot-state verification and startup classification."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from src.core.ops_safety import OpsSafetyService


@dataclass(frozen=True)
class EngineBootStateAssessment:
    """One immutable startup disposition derived from both durable stores."""

    locked: bool
    lock_cause: str | None
    auto_recovery_allowed: bool
    read_failed: bool
    boot_marker_persisted: bool
    db_state: str | None
    redis_state: str | None
    db_boot: dict[str, str] | None
    redis_boot: dict[str, str] | None


class EngineBootStateService:
    """Read and persist boot markers without owning submission mutations."""

    def __init__(
        self,
        *,
        ops_safety: OpsSafetyService,
        redis_client: Any,
        system_state_key: str,
        system_boot_state_key: str,
        boot_id: str,
        logger: logging.Logger,
    ) -> None:
        self._ops_safety = ops_safety
        self._redis_client = redis_client
        self._system_state_key = system_state_key
        self._system_boot_state_key = system_boot_state_key
        self._boot_id = boot_id
        self._logger = logger

    def assess_startup(self) -> EngineBootStateAssessment:
        read_failed = False
        try:
            db_state = self._ops_safety.latest_kill_switch_state()
            redis_state = self._redis_client.get(self._system_state_key)
            if isinstance(redis_state, bytes):
                redis_state = redis_state.decode("utf-8")
            if not isinstance(redis_state, str):
                redis_state = None
            db_boot = self._ops_safety.latest_engine_boot_state()
            redis_boot = self.decode_boot_state(
                self._redis_client.get(self._system_boot_state_key)
            )
        except Exception as error:
            self._logger.error(
                "System state unavailable; starting in LOCKDOWN: %s",
                error,
            )
            read_failed = True
            db_state = redis_state = None
            db_boot = redis_boot = None

        boot_marker_persisted = self.persist("UNCLEAN")
        states_disagree = db_state != redis_state
        kill_state_clear = db_state == "OK" and redis_state == "OK"
        previous_boot_clean = (
            db_boot is not None
            and db_boot == redis_boot
            and db_boot.get("state") == "CLEAN"
        )
        previous_boot_unclean = (
            db_boot is not None
            and db_boot == redis_boot
            and db_boot.get("state") == "UNCLEAN"
        )
        auto_recovery_allowed = bool(
            not read_failed
            and boot_marker_persisted
            and not states_disagree
            and kill_state_clear
            and previous_boot_unclean
        )
        if "LOCKDOWN" in {db_state, redis_state}:
            lock_cause = "explicit_lockdown"
        elif auto_recovery_allowed:
            lock_cause = "unclean_boot"
        elif (
            read_failed
            or not boot_marker_persisted
            or states_disagree
            or not kill_state_clear
            or not previous_boot_clean
        ):
            lock_cause = "state_verification_failed"
        else:
            lock_cause = None

        return EngineBootStateAssessment(
            locked=lock_cause is not None,
            lock_cause=lock_cause,
            auto_recovery_allowed=auto_recovery_allowed,
            read_failed=read_failed,
            boot_marker_persisted=boot_marker_persisted,
            db_state=db_state,
            redis_state=redis_state,
            db_boot=db_boot,
            redis_boot=redis_boot,
        )

    @staticmethod
    def decode_boot_state(value: object) -> dict[str, str] | None:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if not isinstance(value, str):
            return None
        try:
            payload = json.loads(value)
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        state = payload.get("state")
        boot_id = payload.get("boot_id")
        if state not in {"CLEAN", "UNCLEAN"} or not isinstance(boot_id, str):
            return None
        return {"state": state, "boot_id": boot_id}

    def persist(self, state: str) -> bool:
        db_persisted = redis_persisted = False
        try:
            self._ops_safety.persist_engine_boot_state(
                state,
                boot_id=self._boot_id,
            )
            db_persisted = True
        except Exception:
            self._logger.exception("Failed to persist engine boot state to database")
        try:
            self._redis_client.set(
                self._system_boot_state_key,
                json.dumps(
                    {"state": state, "boot_id": self._boot_id},
                    separators=(",", ":"),
                ),
            )
            redis_persisted = True
        except Exception:
            self._logger.exception("Failed to persist engine boot state to Redis")
        return db_persisted or redis_persisted
