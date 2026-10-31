import pytest

from src.core.backtest.spider_run_artifacts import canonical_bytes
import src.core.spider_management.operation_request as operation_request
from src.core.spider_management.operation_request import (
    SpiderOperationRequest,
    SpiderOperationRequestError,
    _decimal,
)


HASH = "a" * 64


def _body(request_kind="PROPOSAL", proposal_request_id=None):
    selection = {
        "schema_version": "SPIDER_RUNTIME_SELECTION_V1",
        "environment": "test",
        "execution_scope_id": "spider-test",
        "strategy_id": "spider-grid",
        "confirmation_request_id": "b" * 64,
        "configuration_sha256": "c" * 64,
        "host_contract_id": "host-v1",
        "host_contract_sha256": HASH,
        "account_profile": "okx",
        "account_id": "test-account",
        "provider_profile_sha256": HASH,
        "build_policy_id": "SPIDER_BUILD_EXPLICIT_MEMBER_RESUBMIT_V1",
        "time_policy_id": "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1",
        "time_zone": "UTC",
    }
    return {
        "schema_version": "spider_operation_request_v1",
        "request_kind": request_kind,
        "environment": "test",
        "execution_scope_id": "spider-test",
        "strategy_id": "spider-grid",
        "actor": "operator-1",
        "idempotency_key": f"{request_kind.lower()}-key",
        "operation_key": "install-1",
        "operation_core": {
            "operation_kind": "INSTALL",
            "account_profile": "okx",
            "account_id": "test-account",
            "source_generation": 0,
            "expected_state_version": 0,
            "confirmation_request_id": "b" * 64,
            "configuration_sha256": "c" * 64,
            "runtime_selection": selection,
            "position_policy": "RETAIN_ENABLED_V1",
        },
        "authorization": {
            "revision": 1,
            "predecessor_confirmation_request_id": None,
            "basis": {
                "financial_cursor": "model-financial-v1:" + HASH + ":0",
                "tape_cursor": "model-tape-v1:" + HASH + ":0",
                "specification_sha256": HASH,
                "valuation_sha256": HASH,
            },
            "expires_at": "2030-01-01T00:00:00.000Z",
            "effects": [],
        },
        "proposal_request_id": proposal_request_id,
    }


def test_operation_request_proposal_and_confirmation_use_canonical_bytes():
    proposal = SpiderOperationRequest.from_canonical_bytes(canonical_bytes(_body()))
    body = _body("CONFIRMATION", proposal.request_id)
    confirmation = SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))

    assert proposal.request_kind == "PROPOSAL"
    assert confirmation.request_kind == "CONFIRMATION"
    assert proposal.operation_id == confirmation.operation_id
    assert proposal.operation_core_sha256 == confirmation.operation_core_sha256
    assert proposal.request_id != confirmation.request_id


def test_operation_request_rejects_cross_scope_or_wrong_policy():
    body = _body()
    body["operation_core"]["runtime_selection"]["time_policy_id"] = "UNADMITTED"

    try:
        SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))
    except SpiderOperationRequestError:
        pass
    else:
        raise AssertionError("unadmitted time policy was accepted")


def test_operation_request_rejects_noncanonical_bytes():
    raw = b'{ "request_kind": "PROPOSAL" }'

    try:
        SpiderOperationRequest.from_canonical_bytes(raw)
    except SpiderOperationRequestError:
        pass
    else:
        raise AssertionError("noncanonical request was accepted")


def test_operation_request_rejects_revision_above_initial_revision():
    body = _body()
    body["authorization"]["revision"] = 2

    try:
        SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))
    except SpiderOperationRequestError:
        pass
    else:
        raise AssertionError("revision 2 was accepted")


@pytest.mark.parametrize("value", ["1e100000", "NaN", "Infinity", "01.2"])
def test_invalid_decimal_grammar_rejected_before_decimal_or_format(value, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid spelling reached Decimal parsing/formatting")

    monkeypatch.setattr(operation_request, "Decimal", forbidden)
    monkeypatch.setattr(operation_request, "canonical_decimal_text", forbidden)
    with pytest.raises(SpiderOperationRequestError):
        operation_request._decimal(value, positive=True)


def test_decimal_grammar_accepts_canonical_positive_control():
    assert _decimal("1.25", positive=True) == "1.25"
