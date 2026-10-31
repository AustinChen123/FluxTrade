from __future__ import annotations

import pytest

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.spider_management.member_resubmission import (
    MemberResubmissionError,
    MemberResubmissionRequest,
    control_receipt_id,
)
from src.core.spider_management.runtime_owner import RuntimeScopeKey, SpiderRuntimeOwner


def _request(**changes: object) -> dict[str, object]:
    return {
        "schema_version": "spider_member_resubmission_request_v1",
        "environment": "live",
        "execution_scope_id": "account-main",
        "strategy_id": "spider-main",
        "actor": "operator",
        "idempotency_key": "retry-one",
        "expected_version": 7,
        "generation": 1,
        "configuration_sha256": "a" * 64,
        "initial_plan_id": "b" * 64,
        "target_action_id": "c" * 64,
        **changes,
    }


def test_member_resubmission_is_closed_canonical_and_control_receipt_stable() -> None:
    first = MemberResubmissionRequest.from_object(_request())
    replay = MemberResubmissionRequest.from_canonical_bytes(first.canonical_request_bytes)
    changed_version = MemberResubmissionRequest.from_object(_request(expected_version=8))

    assert replay == first
    assert replay.request_id == first.request_id
    assert changed_version.request_id == first.request_id
    assert changed_version.request_sha256 != first.request_sha256
    assert control_receipt_id(first) == control_receipt_id(replay)
    assert first.canonical_request_bytes == canonical_bytes(_request())


@pytest.mark.parametrize(
    "changes",
    [
        {"extra": "not in the closed request"},
        {"expected_version": True},
        {"generation": 0},
        {"target_action_id": "not-a-hash"},
    ],
)
def test_member_resubmission_rejects_invalid_or_open_input(changes: dict[str, object]) -> None:
    with pytest.raises(MemberResubmissionError):
        MemberResubmissionRequest.from_object(_request(**changes))


@pytest.mark.parametrize(
    "attempt_disposition",
    ["PLANNED", "ACCEPTED", "REJECTED", "SEND_UNKNOWN", "TERMINAL"],
)
def test_member_resubmission_uses_parent_command_envelope(
    attempt_disposition: str,
) -> None:
    scope = RuntimeScopeKey("live", "account-main", "spider-main")
    runtime = {
        "schema_version": "spider_runtime_readback_v1",
        "operation_id": "operation-1",
        "phase": "INCOMPLETE",
        "runtime_control": "RUN",
    }
    readback = {
        "schema_version": "spider_member_resubmission_readback_v1",
        "disposition": attempt_disposition,
        "runtime": runtime,
    }

    result = SpiderRuntimeOwner._member_resubmission_result(
        scope, attempt_disposition, "SPIDER_MEMBER_RESUBMISSION_RECORDED", readback
    )

    assert result.success is True
    assert result.data == {
        "schema_version": "spider_management_command_result_v1",
        "scope": {
            "environment": "live",
            "execution_scope_id": "account-main",
            "strategy_id": "spider-main",
        },
        "command": "RESUBMIT",
        "disposition": "ACCEPTED",
        "operation_id": "operation-1",
        "phase": "INCOMPLETE",
        "reason_code": "SPIDER_MEMBER_RESUBMISSION_RECORDED",
        "readback": runtime,
    }


@pytest.mark.parametrize(
    ("attempt_disposition", "parent_disposition", "success"),
    [("REJECTED", "REJECTED", False), ("SEND_UNKNOWN", "UNKNOWN", False)],
)
def test_member_resubmission_maps_no_readback_to_parent_outcome(
    attempt_disposition: str, parent_disposition: str, success: bool
) -> None:
    result = SpiderRuntimeOwner._member_resubmission_result(
        RuntimeScopeKey("live", "account-main", "spider-main"),
        attempt_disposition,
        "SPIDER_MEMBER_RESUBMISSION_NOT_ELIGIBLE",
    )

    assert result.success is success
    assert result.data == {
        "schema_version": "spider_management_command_result_v1",
        "scope": {
            "environment": "live",
            "execution_scope_id": "account-main",
            "strategy_id": "spider-main",
        },
        "command": "RESUBMIT",
        "disposition": parent_disposition,
        "operation_id": None,
        "phase": None,
        "reason_code": "SPIDER_MEMBER_RESUBMISSION_NOT_ELIGIBLE",
        "readback": None,
    }
