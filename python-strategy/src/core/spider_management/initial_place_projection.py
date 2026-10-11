"""Pure projection rules for original Spider PLACE candidates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

MemberOutcome = tuple[str, str | None, str | None]


@dataclass(frozen=True, slots=True)
class OriginalPlaceCandidate:
    action_id: str
    source_product_id: str
    original_payload: bytes
    payload_sha256: str


def validate_member_outcomes(
    action_ids: Sequence[str], outcomes: Mapping[str, object] | None
) -> dict[str, MemberOutcome] | None:
    """Validate a provider result's exact action-key and closed tuple shape."""
    expected = set(action_ids)
    if outcomes is None or set(outcomes) != expected:
        return None
    checked: dict[str, MemberOutcome] = {}
    for action_id in action_ids:
        value = outcomes[action_id]
        if type(value) is not tuple or len(value) != 3:
            return None
        state, rejection_reason, provider_order_id = value
        if state == "ACCEPTED":
            if (
                rejection_reason is not None
                or type(provider_order_id) is not str
                or not provider_order_id
            ):
                return None
        elif state == "REJECTED":
            if (
                rejection_reason not in {"51000", "51008"}
                or provider_order_id is not None
            ):
                return None
        elif state == "SEND_UNKNOWN":
            if rejection_reason is not None or provider_order_id is not None:
                return None
        else:
            return None
        checked[action_id] = (state, rejection_reason, provider_order_id)
    return checked


def effective_member_outcome(
    classified: MemberOutcome | None,
    *,
    financial_acceptance_proved: bool,
    current_provider_order_id: object,
) -> MemberOutcome:
    """Apply the current saved financial evidence to one REST classification."""
    unknown: MemberOutcome = ("SEND_UNKNOWN", None, None)
    if classified is None:
        return unknown
    state, rejection_reason, provider_order_id = classified
    if state == "ACCEPTED":
        if (
            rejection_reason is not None
            or type(provider_order_id) is not str
            or not provider_order_id
            or (
                current_provider_order_id is not None
                and current_provider_order_id != provider_order_id
            )
        ):
            return unknown
        return ("ACCEPTED", None, provider_order_id)
    if state == "REJECTED":
        if (
            rejection_reason not in {"51000", "51008"}
            or provider_order_id is not None
        ):
            return unknown
        if financial_acceptance_proved or current_provider_order_id is not None:
            return unknown
        return ("REJECTED", rejection_reason, None)
    if state == "SEND_UNKNOWN" and rejection_reason is None and provider_order_id is None:
        return unknown
    return unknown


def build_missing_projection(
    candidates: Sequence[OriginalPlaceCandidate],
    outcomes: Mapping[str, MemberOutcome],
) -> list[dict[str, object]]:
    """Build the closed missing rows from all original candidates in order."""
    if len({candidate.action_id for candidate in candidates}) != len(candidates):
        raise ValueError("duplicate original Spider action")
    if set(outcomes) != {candidate.action_id for candidate in candidates}:
        raise ValueError("Spider projection does not cover every original action")
    result: list[dict[str, object]] = []
    for candidate in candidates:
        if (
            type(candidate.original_payload) is not bytes
            or type(candidate.payload_sha256) is not str
            or type(candidate.source_product_id) is not str
            or not candidate.source_product_id
        ):
            raise ValueError("invalid original Spider candidate")
        try:
            source_value = decode_canonical(candidate.original_payload)
            if canonical_bytes(source_value) != candidate.original_payload:
                raise ValueError("noncanonical original Spider payload")
        except (TypeError, UnicodeDecodeError):
            raise ValueError("invalid original Spider payload") from None
        if type(source_value) is not dict:
            raise ValueError("invalid original Spider payload")
        source = source_value
        side = source.get("side")
        price = source.get("px")
        quantity = source.get("sz")
        if (
            side not in {"buy", "sell"}
            or type(price) is not str
            or type(quantity) is not str
            or source.get("ordType") != "limit"
        ):
            raise ValueError("invalid original Spider order terms")
        state, rejection_reason, provider_order_id = outcomes[candidate.action_id]
        if state == "ACCEPTED":
            if rejection_reason is not None or not provider_order_id:
                raise ValueError("invalid accepted Spider outcome")
            continue
        if state == "REJECTED":
            if rejection_reason not in {"51000", "51008"} or provider_order_id is not None:
                raise ValueError("invalid rejected Spider outcome")
        elif state == "SEND_UNKNOWN":
            if rejection_reason is not None or provider_order_id is not None:
                raise ValueError("invalid unknown Spider outcome")
        elif state == "UNSENT":
            if rejection_reason is not None or provider_order_id is not None:
                raise ValueError("invalid unsent Spider outcome")
        else:
            raise ValueError("unsupported original Spider outcome")
        result.append({
            "initial_member_id": candidate.action_id,
            "current_frontier_action_id": candidate.action_id,
            "original_payload_sha256": candidate.payload_sha256,
            "original_terms": {
                "product_id": candidate.source_product_id,
                "side": "LONG" if side == "buy" else "SHORT",
                "order_type": "limit",
                "price": price,
                "quantity": quantity,
                "quantity_unit": "CONTRACTS",
            },
            "last_outcome": state,
            "rejection_reason": rejection_reason,
            "resubmit_eligible": False,
            "ineligible_reason": "PROVIDER_PROFILE_UNAVAILABLE",
            "latest_resubmission_request_id": None,
        })
    return result


def expected_plan_status(missing_members: Sequence[object]) -> str:
    return "COMPLETE" if not missing_members else "INCOMPLETE"
