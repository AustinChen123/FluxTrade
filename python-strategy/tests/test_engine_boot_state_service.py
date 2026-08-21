from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from src.core.engine_boot_state_service import EngineBootStateService


SYSTEM_KEY = "fluxtrade:live:system:state"
BOOT_KEY = "fluxtrade:live:system:engine_boot_state"
BOOT_ID = "boot-current"


def _service(
    *,
    db_state: object = "OK",
    redis_state: object = "OK",
    db_boot: object = None,
    redis_boot: object = None,
) -> tuple[EngineBootStateService, MagicMock, MagicMock, MagicMock]:
    ops_safety = MagicMock()
    ops_safety.latest_kill_switch_state.return_value = db_state
    ops_safety.latest_engine_boot_state.return_value = db_boot
    redis = MagicMock()
    values = {SYSTEM_KEY: redis_state, BOOT_KEY: redis_boot}
    redis.get.side_effect = lambda key: values[key]
    logger = MagicMock()
    service = EngineBootStateService(
        ops_safety=ops_safety,
        redis_client=redis,
        system_state_key=SYSTEM_KEY,
        system_boot_state_key=BOOT_KEY,
        boot_id=BOOT_ID,
        logger=logger,
    )
    return service, ops_safety, redis, logger


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (b"not-json", None),
        (json.dumps([]), None),
        (json.dumps({"state": "OTHER", "boot_id": "old"}), None),
        (json.dumps({"state": "CLEAN", "boot_id": 7}), None),
        (
            json.dumps({"state": "CLEAN", "boot_id": "old"}),
            {"state": "CLEAN", "boot_id": "old"},
        ),
        (
            json.dumps({"state": "UNCLEAN", "boot_id": "old"}).encode(),
            {"state": "UNCLEAN", "boot_id": "old"},
        ),
    ],
)
def test_decode_boot_state_accepts_only_exact_supported_payloads(
    value: object,
    expected: dict[str, str] | None,
) -> None:
    assert EngineBootStateService.decode_boot_state(value) == expected


@pytest.mark.parametrize(
    (
        "db_state",
        "redis_state",
        "db_boot",
        "redis_boot",
        "locked",
        "lock_cause",
        "auto_recovery_allowed",
    ),
    [
        (
            "OK",
            "OK",
            {"state": "CLEAN", "boot_id": "old"},
            {"state": "CLEAN", "boot_id": "old"},
            False,
            None,
            False,
        ),
        (
            "OK",
            "OK",
            {"state": "UNCLEAN", "boot_id": "old"},
            {"state": "UNCLEAN", "boot_id": "old"},
            True,
            "unclean_boot",
            True,
        ),
        (
            "LOCKDOWN",
            "OK",
            {"state": "CLEAN", "boot_id": "old"},
            {"state": "CLEAN", "boot_id": "old"},
            True,
            "explicit_lockdown",
            False,
        ),
        (
            "OK",
            "LOCKDOWN",
            {"state": "CLEAN", "boot_id": "old"},
            {"state": "CLEAN", "boot_id": "old"},
            True,
            "explicit_lockdown",
            False,
        ),
        (
            None,
            "OK",
            {"state": "CLEAN", "boot_id": "old"},
            {"state": "CLEAN", "boot_id": "old"},
            True,
            "state_verification_failed",
            False,
        ),
        (
            "OK",
            "OK",
            None,
            None,
            True,
            "state_verification_failed",
            False,
        ),
        (
            "OK",
            "OK",
            {"state": "CLEAN", "boot_id": "old-a"},
            {"state": "CLEAN", "boot_id": "old-b"},
            True,
            "state_verification_failed",
            False,
        ),
    ],
)
def test_assessment_preserves_fail_closed_state_matrix(
    db_state: str | None,
    redis_state: str | None,
    db_boot: dict[str, str] | None,
    redis_boot: dict[str, str] | None,
    locked: bool,
    lock_cause: str | None,
    auto_recovery_allowed: bool,
) -> None:
    service, ops_safety, redis, _logger = _service(
        db_state=db_state,
        redis_state=redis_state,
        db_boot=db_boot,
        redis_boot=None if redis_boot is None else json.dumps(redis_boot),
    )

    assessment = service.assess_startup()

    assert assessment.locked is locked
    assert assessment.lock_cause == lock_cause
    assert assessment.auto_recovery_allowed is auto_recovery_allowed
    assert assessment.db_state == db_state
    assert assessment.redis_state == redis_state
    assert assessment.db_boot == db_boot
    assert assessment.redis_boot == redis_boot
    ops_safety.persist_engine_boot_state.assert_called_once_with(
        "UNCLEAN",
        boot_id=BOOT_ID,
    )
    redis.set.assert_called_once_with(
        BOOT_KEY,
        '{"state":"UNCLEAN","boot_id":"boot-current"}',
    )


def test_read_failure_still_marks_unclean_and_returns_locked_assessment() -> None:
    service, ops_safety, redis, logger = _service()
    failure = ConnectionError("state stores unavailable")
    ops_safety.latest_kill_switch_state.side_effect = failure

    assessment = service.assess_startup()

    assert assessment.locked is True
    assert assessment.lock_cause == "state_verification_failed"
    assert assessment.auto_recovery_allowed is False
    assert assessment.read_failed is True
    assert assessment.db_state is None
    assert assessment.redis_state is None
    ops_safety.persist_engine_boot_state.assert_called_once_with(
        "UNCLEAN",
        boot_id=BOOT_ID,
    )
    redis.set.assert_called_once()
    logger.error.assert_called_once_with(
        "System state unavailable; starting in LOCKDOWN: %s",
        failure,
    )


@pytest.mark.parametrize(
    ("db_succeeds", "redis_succeeds", "expected"),
    [
        (True, True, True),
        (True, False, True),
        (False, True, True),
        (False, False, False),
    ],
)
def test_boot_marker_persistence_keeps_existing_best_effort_contract(
    db_succeeds: bool,
    redis_succeeds: bool,
    expected: bool,
) -> None:
    service, ops_safety, redis, logger = _service()
    db_failure = RuntimeError("database unavailable")
    redis_failure = RuntimeError("redis unavailable")
    if not db_succeeds:
        ops_safety.persist_engine_boot_state.side_effect = db_failure
    if not redis_succeeds:
        redis.set.side_effect = redis_failure

    assert service.persist("CLEAN") is expected

    ops_safety.persist_engine_boot_state.assert_called_once_with(
        "CLEAN",
        boot_id=BOOT_ID,
    )
    redis.set.assert_called_once_with(
        BOOT_KEY,
        '{"state":"CLEAN","boot_id":"boot-current"}',
    )
    if db_succeeds:
        assert not any(
            call.args == ("Failed to persist engine boot state to database",)
            for call in logger.exception.call_args_list
        )
    if redis_succeeds:
        assert not any(
            call.args == ("Failed to persist engine boot state to Redis",)
            for call in logger.exception.call_args_list
        )
