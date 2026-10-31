from hashlib import sha256
from typing import cast

import pytest

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.spider_management.initial_place_projection import (
    OriginalPlaceCandidate,
    build_missing_projection,
    effective_member_outcome,
    expected_plan_status,
    validate_member_outcomes,
)


def _candidate(action_id: str, *, side: str = "buy") -> OriginalPlaceCandidate:
    payload = canonical_bytes({
        "ordType": "limit",
        "px": "12.50",
        "side": side,
        "sz": "0.25",
    })
    return OriginalPlaceCandidate(
        action_id=action_id,
        source_product_id="BTC-USDT-SWAP",
        original_payload=payload,
        payload_sha256=sha256(payload).hexdigest(),
    )


def test_exact_member_outcomes_project_original_unsent_and_rejected_members() -> None:
    candidates = [_candidate("a" * 64), _candidate("b" * 64, side="sell")]
    outcomes = validate_member_outcomes(
        [candidate.action_id for candidate in candidates],
        {
            "a" * 64: ("REJECTED", "51008", None),
            "b" * 64: ("SEND_UNKNOWN", None, None),
        },
    )
    assert outcomes is not None

    missing = build_missing_projection(candidates, outcomes)
    assert [row["initial_member_id"] for row in missing] == [
        "a" * 64,
        "b" * 64,
    ]
    assert missing[0] == {
        "initial_member_id": "a" * 64,
        "current_frontier_action_id": "a" * 64,
        "original_payload_sha256": candidates[0].payload_sha256,
        "original_terms": {
            "product_id": "BTC-USDT-SWAP",
            "side": "LONG",
            "order_type": "limit",
            "price": "12.50",
            "quantity": "0.25",
            "quantity_unit": "CONTRACTS",
        },
        "last_outcome": "REJECTED",
        "rejection_reason": "51008",
        "resubmit_eligible": False,
        "ineligible_reason": "PROVIDER_PROFILE_UNAVAILABLE",
        "latest_resubmission_request_id": None,
    }
    assert cast(dict[str, object], missing[1]["original_terms"])["side"] == "SHORT"
    assert missing[1]["last_outcome"] == "SEND_UNKNOWN"
    assert expected_plan_status(missing) == "INCOMPLETE"


def test_accepted_member_is_removed_but_does_not_manufacture_readiness() -> None:
    candidate = _candidate("a" * 64)
    outcomes = validate_member_outcomes(
        [candidate.action_id],
        {candidate.action_id: ("ACCEPTED", None, "venue-order-1")},
    )
    assert outcomes is not None
    assert build_missing_projection([candidate], outcomes) == []
    assert expected_plan_status([]) == "COMPLETE"


@pytest.mark.parametrize(
    "outcomes",
    [
        None,
        {},
        {"a" * 64: ("ACCEPTED", None, None)},
        {"a" * 64: ("ACCEPTED", "51008", "venue-order-1")},
        {"a" * 64: ("REJECTED", "50000", None)},
        {"a" * 64: ("REJECTED", "51008", "venue-order-1")},
        {"a" * 64: ("SEND_UNKNOWN", "51008", None)},
        {"a" * 64: ("UNSENT", None, None)},
    ],
)
def test_member_outcome_validation_rejects_unclosed_or_mismatched_classification(
    outcomes: object,
) -> None:
    assert validate_member_outcomes(["a" * 64], outcomes) is None  # type: ignore[arg-type]


def test_financial_acceptance_overrides_a_conflicting_rest_rejection() -> None:
    assert effective_member_outcome(
        ("REJECTED", "51008", None),
        financial_acceptance_proved=True,
        current_provider_order_id="venue-order-1",
    ) == ("SEND_UNKNOWN", None, None)


def test_provider_id_conflict_cannot_cover_an_original_member() -> None:
    assert effective_member_outcome(
        ("ACCEPTED", None, "venue-order-1"),
        financial_acceptance_proved=False,
        current_provider_order_id="different-order",
    ) == ("SEND_UNKNOWN", None, None)


@pytest.mark.parametrize(
    ("operation", "outcome", "expected"),
    [
        ("PLACE", ("REJECTED", "51008", None), True),
        ("PLACE", ("REJECTED", "51400", None), False),
        ("CANCEL", ("ACCEPTED", None, "venue-order-1"), True),
        ("CANCEL", ("REJECTED", "51400", None), True),
        ("CANCEL", ("REJECTED", "51008", None), False),
    ],
)
def test_closed_member_outcome_codes_are_operation_specific(
    operation: str,
    outcome: tuple[str, str | None, str | None],
    expected: bool,
) -> None:
    checked = validate_member_outcomes(
        ["a" * 64], {"a" * 64: outcome}, operation=operation
    )
    assert (checked is not None) is expected
