"""Complete factor matrix for protection terminal classification."""

import itertools

import pytest

from src.core.protection_observation import (
    AlertEligibility,
    Application,
    Declaration,
    ExposureEvidence,
    FillEvidence,
    LegRelation,
    TerminalValidity,
    classify_protective_terminal,
)


_DECLARATIONS = tuple(Declaration)
_LEGS = tuple(LegRelation)
_FILLS = tuple(FillEvidence)
_EXPOSURES = tuple(ExposureEvidence)


@pytest.mark.parametrize(
    "declaration,leg,fill,exposure",
    itertools.product(
        _DECLARATIONS,
        _LEGS,
        _FILLS,
        _EXPOSURES,
    ),
    ids=lambda value: getattr(value, "value", str(value)),
)
def test_protection_terminal_factor_matrix(
    declaration, leg, fill, exposure
):
    """Every declaration x leg x fill x authority x exposure cell is explicit."""
    result = classify_protective_terminal(
        declaration=declaration,
        leg=leg,
        fill=fill,
        exposure=exposure,
        identity_valid=True,
        evidence_valid=True,
    )
    invalid = (
        declaration is Declaration.INVALID
        or leg is LegRelation.INVALID
        or fill
        in {
            FillEvidence.CONTRADICTORY,
            FillEvidence.MISSING,
            FillEvidence.STALE,
        }
        or (fill is FillEvidence.ZERO and exposure is ExposureEvidence.UNKNOWN)
    )
    if invalid:
        assert result.terminal_validity is TerminalValidity.INVALID
        assert result.application is Application.PRESERVE_EXISTING
        assert result.alert_eligibility is AlertEligibility.NO_ALERT
        return

    assert result.terminal_validity is TerminalValidity.VALID
    if fill is not FillEvidence.ZERO:
        assert result.application is Application.PRESERVE_EXISTING
        assert result.alert_eligibility is AlertEligibility.NO_ALERT
        return

    assert result.application is Application.SAFE_APPLIED
    is_declared_leg = (
        declaration is Declaration.SL and leg is LegRelation.SL
    ) or (declaration is Declaration.TP and leg is LegRelation.TP) or (
        declaration is Declaration.SL_TP and leg in {LegRelation.SL, LegRelation.TP}
    )
    if exposure is ExposureEvidence.OPEN and is_declared_leg:
        assert result.alert_eligibility is AlertEligibility.LOSS_ALERT
        assert result.declared_leg is leg
    else:
        assert result.alert_eligibility is AlertEligibility.NO_ALERT


@pytest.mark.parametrize(
    "fill",
    [
        FillEvidence.CONTRADICTORY,
        FillEvidence.MISSING,
        FillEvidence.STALE,
        FillEvidence.ACCOUNTING_FAILURE,
    ],
)
def test_invalid_or_failed_fill_evidence_never_becomes_alert(fill):
    result = classify_protective_terminal(
        declaration=Declaration.SL,
        leg=LegRelation.SL,
        fill=fill,
        exposure=ExposureEvidence.OPEN,
        identity_valid=True,
        evidence_valid=fill is FillEvidence.ACCOUNTING_FAILURE,
    )
    assert result.alert_eligibility is AlertEligibility.NO_ALERT
    if fill is FillEvidence.ACCOUNTING_FAILURE:
        assert result.application is Application.PRESERVE_EXISTING


def test_sibling_leg_cannot_assert_declared_leg_is_missing():
    result = classify_protective_terminal(
        declaration=Declaration.SL_TP,
        leg=LegRelation.SL,
        fill=FillEvidence.ZERO,
        exposure=ExposureEvidence.OPEN,
        identity_valid=True,
        evidence_valid=True,
    )
    assert result.alert_eligibility is AlertEligibility.LOSS_ALERT
    assert result.declared_leg is LegRelation.SL


