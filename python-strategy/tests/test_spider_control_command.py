"""Closed local command identity for durable Spider STOP receipts."""

from copy import deepcopy
from hashlib import sha256

import pytest

from src.core.spider_management.control_command import (
    SpiderControlCommandError,
    SpiderManagementCommand,
    hydrate_control_receipt,
)


def _message(**changes):
    params = {
        "environment": "live",
        "execution_scope_id": "account-main",
        "strategy_id": "spider-main",
        "actor": "api_key",
        "idempotency_key": "stop-key-1",
        "expected_version": 7,
    }
    params.update(changes)
    return {"command": "STOP", "params": params}


def test_stop_command_is_closed_canonical_and_deterministically_identified():
    command = SpiderManagementCommand.from_message(_message(), command="STOP")
    same = SpiderManagementCommand.from_message(_message(), command="STOP")

    assert command == same
    assert command.canonical_request_bytes == (
        b'{"actor":"api_key","command":"STOP","environment":"live",'
        b'"execution_scope_id":"account-main","expected_version":7,'
        b'"idempotency_key":"stop-key-1","schema_version":"SPIDER_MANAGEMENT_COMMAND_V1",'
        b'"strategy_id":"spider-main"}'
    )
    assert command.receipt_id == sha256(
        b"SPIDER_CONTROL_RECEIPT_V1\0" + command.canonical_request_bytes
    ).hexdigest()
    assert SpiderManagementCommand.from_canonical_bytes(
        command.canonical_request_bytes
    ) == command


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_version": True},
        {"expected_version": -1},
        {"expected_version": 2**31},
        {"environment": "bad value"},
        {"actor": "bad actor"},
        {"idempotency_key": ""},
        {"strategy_id": "spider-main", "id": "other"},
    ],
)
def test_stop_command_rejects_invalid_closed_identity_fields(changes):
    with pytest.raises(SpiderControlCommandError):
        SpiderManagementCommand.from_message(_message(**changes), command="STOP")


def test_stop_command_rejects_other_command_and_noncanonical_receipt_bytes():
    with pytest.raises(SpiderControlCommandError):
        SpiderManagementCommand.from_message(_message(), command="RESUME")

    command = SpiderManagementCommand.from_message(_message(), command="STOP")
    with pytest.raises(SpiderControlCommandError):
        SpiderManagementCommand.from_canonical_bytes(
            command.canonical_request_bytes + b" "
        )


def test_receipt_hydration_rejects_mutated_saved_request_identity():
    command = SpiderManagementCommand.from_message(_message(), command="STOP")
    row = {
        "receipt_id": command.receipt_id,
        "environment": command.environment,
        "execution_scope_id": command.execution_scope_id,
        "strategy_id": command.strategy_id,
        "actor": command.actor,
        "idempotency_key": command.idempotency_key,
        "request_payload": command.canonical_request_bytes,
        "request_sha256": command.request_sha256,
        "operation_id": "a" * 64,
        "generation": 1,
        "before_state_version": 7,
        "after_state_version": 8,
        "barrier_sequence": 0,
        "outcome": "COMPLETED",
    }
    hydrated, proof = hydrate_control_receipt(row)
    assert hydrated == command
    assert proof["operation_id"] == "a" * 64

    mutated = deepcopy(row)
    mutated["after_state_version"] = 9
    with pytest.raises(SpiderControlCommandError):
        hydrate_control_receipt(mutated)
