from dataclasses import fields, replace
from decimal import Decimal
from typing import Any, cast

import pytest

from src.core.backtest.run_evidence import canonical_decision_snapshot
from src.core.strategy_context import StrategyContext
from src.core.market_data.profiles.context_enrichment import (
    enrich_profile_context,
    ProfileContextEnrichmentError,
)
from src.core.market_data.profiles.requirements import ProfileRequirement
from test_profile_decision_context import item, unavailable, B, S, Collection, DAY


def context():
    return StrategyContext(
        "strategy",
        "BINANCE:ETHUSDT-SPOT",
        123,
        Decimal(0),
        Decimal(0),
        Decimal(0),
        Decimal(0),
        Decimal(0),
        Decimal(0),
    )


def requirement(request):
    return ProfileRequirement(
        request.product_id,
        request.base_grid_id,
        request.output_grid_id,
        request.algorithm_version,
        1,
        "utc_complete_strict_v1",
    )


def fixture(basis=B.LIVE_OBSERVED, status=S.FRESH):
    value = (
        item(basis)
        if status is S.FRESH
        else unavailable(
            basis,
            status,
            "INVALID_PROFILE" if status is S.INVALID else "PROFILE_NOT_READY",
        )
    )
    if basis is B.LIVE_OBSERVED:
        value = replace(
            value,
            request=replace(
                value.request, freshness_policy_id="utc_complete_strict_v1"
            ),
        )
    return value, requirement(value.request)


def apply(value, requirements=None, ctx=None):
    return enrich_profile_context(
        ctx or context(),
        requirements or (requirement(value.request),),
        Collection(DAY + 3, (value,)),
        decision_time_ms=DAY + 3,
    )


@pytest.mark.parametrize("basis", list(B))
@pytest.mark.parametrize("status", list(S))
def test_complete_basis_status_matrix_and_account_preservation(basis, status):
    value, req = fixture(basis, status)
    original = context()
    result = apply(value, (req,), original)
    assert result.market_data == Collection(DAY + 3, (value,))
    assert result.market_data is not None
    assert result.market_data.profiles[0].request is value.request
    for field in fields(original):
        if field.name != "market_data":
            assert getattr(result, field.name) is getattr(original, field.name)
    assert original.market_data is None
    assert result.product_id != value.request.product_id  # Auxiliary market is legal.
    repeated = apply(value).market_data
    assert repeated is not None and repeated.digest == result.market_data.digest


def test_empty_preserves_legacy_identity_and_projection():
    original = context()
    before = canonical_decision_snapshot(original)
    assert (
        enrich_profile_context(
            original, (), Collection(DAY + 3, ()), decision_time_ms=DAY + 3
        )
        is original
    )
    assert (
        canonical_decision_snapshot(original) == before and "market_data" not in before
    )
    value, _ = fixture()
    with pytest.raises(ProfileContextEnrichmentError):
        enrich_profile_context(
            original, (), Collection(DAY + 3, (value,)), decision_time_ms=DAY + 3
        )


@pytest.mark.parametrize(
    "change",
    [
        dict(product_id="BINANCE:ETHUSDT-SPOT"),
        dict(base_grid_id="other"),
        dict(output_grid_id="other"),
        dict(algorithm_version="other"),
        dict(window_days=2),
        dict(freshness_policy_id="other"),
    ],
)
def test_requirement_mismatch(change):
    value, req = fixture()
    with pytest.raises(
        ProfileContextEnrichmentError, match="^PROFILE_CONTEXT_ENRICHMENT_INVALID$"
    ):
        apply(value, (replace(req, **change),))


def test_modeled_policy_is_not_freshness_mapping():
    value, req = fixture(B.MODELED)
    alternate = replace(
        value,
        request=replace(value.request, availability_policy_id="another_named_model"),
    )
    data = apply(alternate, (req,)).market_data
    assert data is not None and data.profiles[0] == alternate
    assert alternate.request.freshness_policy_id is None


def test_missing_extra_duplicate_mixed_basis_and_wrong_window():
    value, req = fixture(status=S.MISSING)
    modeled, _ = fixture(B.MODELED, S.MISSING)
    other = replace(value, request=replace(value.request, output_grid_id="other"))
    for values, requirements in (
        ((), (req,)),
        ((value,), (req, req)),
        ((value, other), (req,)),
        ((value, modeled), (req,)),
        (
            (
                replace(
                    value, request=replace(value.request, start_ms=DAY, end_ms=2 * DAY)
                ),
            ),
            (req,),
        ),
    ):
        with pytest.raises(ProfileContextEnrichmentError):
            enrich_profile_context(
                context(),
                requirements,
                Collection(DAY + 3, values),
                decision_time_ms=DAY + 3,
            )


@pytest.mark.parametrize("bad", [True, -1, 2**63, 1.0, DAY + 4])
def test_exact_decision_time(bad):
    value, req = fixture()
    with pytest.raises(ProfileContextEnrichmentError):
        enrich_profile_context(
            context(),
            (req,),
            Collection(DAY + 3, (value,)),
            decision_time_ms=cast(Any, bad),
        )


def test_exact_inputs_and_subclasses():
    value, req = fixture()
    collection = Collection(DAY + 3, (value,))
    for ctx, requirements, data in (
        (None, (req,), collection),
        (context(), [req], collection),
        (context(), (None,), collection),
        (context(), (req,), None),
    ):
        with pytest.raises(ProfileContextEnrichmentError):
            enrich_profile_context(
                cast(Any, ctx),
                cast(Any, requirements),
                cast(Any, data),
                decision_time_ms=DAY + 3,
            )
    subclass = type("Requirement", (ProfileRequirement,), {})(
        **{f.name: getattr(req, f.name) for f in fields(req)}
    )
    with pytest.raises(ProfileContextEnrichmentError):
        apply(value, (subclass,))


def test_mixed_basis_with_otherwise_exact_distinct_coverage():
    live, req = fixture(status=S.MISSING)
    modeled, _ = fixture(B.MODELED, S.MISSING)
    modeled = replace(modeled, request=replace(modeled.request, output_grid_id="other"))
    with pytest.raises(ProfileContextEnrichmentError):
        enrich_profile_context(
            context(),
            (req, requirement(modeled.request)),
            Collection(DAY + 3, (live, modeled)),
            decision_time_ms=DAY + 3,
        )


def test_actual_consumer_finalizer_input():
    from test_profile_observed_snapshot import observed

    request, snapshot = observed()
    decision = snapshot.finalize(decision_time_ms=DAY + 13, current_monotonic_ms=101)
    collection = Collection(DAY + 13, (decision,))
    result = enrich_profile_context(
        context(), (requirement(request),), collection, decision_time_ms=DAY + 13
    )
    assert result.market_data is collection
    assert collection.profiles[0].request is request


def test_enrichment_never_replaces_existing_evidence():
    fresh, req = fixture()
    enriched = apply(fresh)
    assert enriched.market_data is not None
    before = canonical_decision_snapshot(enriched)
    digest = enriched.market_data.digest
    missing, _ = fixture(status=S.MISSING)
    for requirements, data in (
        ((req,), enriched.market_data),
        ((req,), Collection(DAY + 3, (missing,))),
        ((), Collection(DAY + 3, ())),
    ):
        with pytest.raises(
            ProfileContextEnrichmentError, match="^PROFILE_CONTEXT_ENRICHMENT_INVALID$"
        ):
            enrich_profile_context(
                enriched, requirements, data, decision_time_ms=DAY + 3
            )
        assert canonical_decision_snapshot(enriched) == before
        assert enriched.market_data.digest == digest
