"""Pure classification for validated protective-order terminal observations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Declaration(str, Enum):
    UNDECLARED = "UNDECLARED"
    NONE = "NONE"
    SL = "SL"
    TP = "TP"
    SL_TP = "SL+TP"
    INVALID = "INVALID"


class LegRelation(str, Enum):
    SL = "SL"
    TP = "TP"
    OTHER = "OTHER"
    INVALID = "INVALID"


class FillEvidence(str, Enum):
    ZERO = "ZERO"
    POSITIVE = "POSITIVE"
    PARTIAL = "PARTIAL"
    ACCOUNTING_FAILURE = "ACCOUNTING_FAILURE"
    CONTRADICTORY = "CONTRADICTORY"
    MISSING = "MISSING"
    STALE = "STALE"


class ExposureEvidence(str, Enum):
    OPEN = "OPEN"
    FLAT = "FLAT"
    UNKNOWN = "UNKNOWN"


class TerminalValidity(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"


class Application(str, Enum):
    SAFE_APPLIED = "SAFE_APPLIED"
    PRESERVE_EXISTING = "PRESERVE_EXISTING"


class AlertEligibility(str, Enum):
    LOSS_ALERT = "LOSS_ALERT"
    NO_ALERT = "NO_ALERT"


@dataclass(frozen=True, slots=True)
class ProtectionObservation:
    terminal_validity: TerminalValidity
    application: Application
    alert_eligibility: AlertEligibility
    reason: str
    declared_leg: LegRelation | None = None


def classify_protective_terminal(
    *,
    declaration: Declaration,
    leg: LegRelation,
    fill: FillEvidence,
    exposure: ExposureEvidence,
    identity_valid: bool,
    evidence_valid: bool,
    identity_failure_reason: str | None = None,
) -> ProtectionObservation:
    """Separate terminal application validity from declaration-based alerting.

    This function intentionally has no I/O or repository access. Callers pass
    only immutable declaration and already validated lineage/evidence facts.
    """
    if (
        declaration is Declaration.INVALID
        or leg is LegRelation.INVALID
        or not identity_valid
        or not evidence_valid
        or fill in {
            FillEvidence.CONTRADICTORY,
            FillEvidence.MISSING,
            FillEvidence.STALE,
        }
    ):
        return ProtectionObservation(
            TerminalValidity.INVALID,
            Application.PRESERVE_EXISTING,
            AlertEligibility.NO_ALERT,
            identity_failure_reason or "invalid_or_insufficient_authority",
        )

    if fill is not FillEvidence.ZERO:
        return ProtectionObservation(
            TerminalValidity.VALID,
            Application.PRESERVE_EXISTING,
            AlertEligibility.NO_ALERT,
            "positive_fill_or_existing_accounting_outcome",
        )

    if exposure is ExposureEvidence.UNKNOWN:
        return ProtectionObservation(
            TerminalValidity.INVALID,
            Application.PRESERVE_EXISTING,
            AlertEligibility.NO_ALERT,
            "exposure_authority_unknown",
        )

    if exposure is ExposureEvidence.FLAT:
        return ProtectionObservation(
            TerminalValidity.VALID,
            Application.SAFE_APPLIED,
            AlertEligibility.NO_ALERT,
            "verified_flat_exposure",
        )

    is_declared = (
        declaration is Declaration.SL and leg is LegRelation.SL
    ) or (declaration is Declaration.TP and leg is LegRelation.TP) or (
        declaration is Declaration.SL_TP
        and leg in {LegRelation.SL, LegRelation.TP}
    )
    if is_declared:
        return ProtectionObservation(
            TerminalValidity.VALID,
            Application.SAFE_APPLIED,
            AlertEligibility.LOSS_ALERT,
            "validated_declared_leg_missing_on_open_exposure",
            declared_leg=leg,
        )
    return ProtectionObservation(
        TerminalValidity.VALID,
        Application.SAFE_APPLIED,
        AlertEligibility.NO_ALERT,
        "valid_terminal_without_declared_leg_loss",
    )
