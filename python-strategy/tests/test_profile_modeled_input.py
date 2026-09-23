from dataclasses import replace
from typing import Any, cast

import pytest

from src.core.market_data.profiles.decision_context import (
    ProfileDecisionBasis,
    ProfileDecisionStatus,
    StrategyMarketDataContext,
)
from src.core.market_data.profiles.modeled_input import (
    ModeledProfileInput,
    ModeledProfileInputError,
)
from src.core.market_data.profiles.requirements import ProfileRequirement
from test_profile_context_enrichment import fixture

DAY = 86_400_000
DECISION = DAY + 3
POLICY = "utc_day_0020_conservative_v1"


class Provider:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, int, str]] = []

    def context_for(
        self,
        requirements: tuple[ProfileRequirement, ...],
        *,
        decision_time_ms: int,
        availability_policy_id: str,
    ) -> StrategyMarketDataContext:
        self.calls.append((requirements, decision_time_ms, availability_policy_id))
        if isinstance(self.result, BaseException):
            raise self.result
        return cast(StrategyMarketDataContext, self.result)


def modeled(status=ProfileDecisionStatus.FRESH):
    value, requirement = fixture(ProfileDecisionBasis.MODELED, status)
    value = replace(
        value,
        request=replace(
            value.request,
            availability_policy_id=POLICY,
            as_of_ms=DECISION,
        ),
        decision_time_ms=DECISION,
    )
    return value, requirement


@pytest.mark.parametrize("status", list(ProfileDecisionStatus))
def test_resolves_all_modeled_statuses_with_exact_call(status):
    value, requirement = modeled(status)
    result = StrategyMarketDataContext(DECISION, (value,))
    provider = Provider(result)

    assert (
        ModeledProfileInput(provider, POLICY).resolve(
            (requirement,), decision_time_ms=DECISION
        )
        is result
    )
    assert provider.calls == [((requirement,), DECISION, POLICY)]


@pytest.mark.parametrize(
    "error", [RuntimeError("fixture unavailable"), KeyboardInterrupt()]
)
def test_provider_base_exception_propagates_without_fabricating_context(error):
    provider = Provider(error)
    requirement = modeled()[1]

    with pytest.raises(BaseException) as caught:
        ModeledProfileInput(provider, POLICY).resolve(
            (requirement,), decision_time_ms=DECISION
        )

    assert caught.value is error


@pytest.mark.parametrize("decision_time", [True, -1, 1 << 63, 1.0])
def test_invalid_decision_time_never_calls_provider(decision_time):
    provider = Provider(cast(Any, None))
    requirement = modeled()[1]

    with pytest.raises(ModeledProfileInputError):
        ModeledProfileInput(provider, POLICY).resolve(
            (requirement,), decision_time_ms=cast(Any, decision_time)
        )

    assert provider.calls == []


def test_invalid_requirements_never_call_provider():
    provider = Provider(cast(Any, None))
    requirement = modeled()[1]
    child = type("RequirementChild", (ProfileRequirement,), {})(
        requirement.product_id,
        requirement.base_grid_id,
        requirement.output_grid_id,
        requirement.algorithm_version,
        requirement.window_days,
        requirement.freshness_policy_id,
    )

    for requirements in ([requirement], (cast(Any, None),), (child,)):
        with pytest.raises(ModeledProfileInputError):
            ModeledProfileInput(provider, POLICY).resolve(
                cast(Any, requirements), decision_time_ms=DECISION
            )

    assert provider.calls == []


@pytest.mark.parametrize(
    "change",
    [
        {"request": "live"},
        {"request": "policy"},
        {"request": "window"},
    ],
)
def test_rejects_wrong_basis_identity_policy_time_and_window(change):
    value, requirement = modeled(ProfileDecisionStatus.MISSING)
    if change.get("request") == "live":
        value = replace(
            value,
            request=replace(
                value.request,
                purpose="LIVE_QUERY",
                freshness_policy_id="utc_complete_strict_v1",
                availability_policy_id=None,
                as_of_ms=None,
            ),
            basis=ProfileDecisionBasis.LIVE_OBSERVED,
        )
    elif change.get("request") == "policy":
        value = replace(
            value,
            request=replace(value.request, availability_policy_id="other"),
        )
    elif change.get("request") == "window":
        value = replace(
            value,
            request=replace(
                value.request,
                start_ms=DAY,
                end_ms=2 * DAY,
            ),
        )

    with pytest.raises(
        ModeledProfileInputError, match="^MODELED_PROFILE_INPUT_INVALID$"
    ):
        ModeledProfileInput(
            Provider(StrategyMarketDataContext(DECISION, (value,))), POLICY
        ).resolve((requirement,), decision_time_ms=DECISION)


def test_rejects_missing_extra_duplicate_and_wrong_result_type():
    value, requirement = modeled(ProfileDecisionStatus.MISSING)
    other = replace(value, request=replace(value.request, output_grid_id="other"))
    cases = (
        ((requirement,), StrategyMarketDataContext(DECISION, ())),
        ((requirement,), StrategyMarketDataContext(DECISION, (value, other))),
        ((requirement, requirement), StrategyMarketDataContext(DECISION, (value,))),
        ((requirement,), cast(Any, None)),
    )
    for requirements, result in cases:
        with pytest.raises(ModeledProfileInputError):
            ModeledProfileInput(Provider(result), POLICY).resolve(
                requirements, decision_time_ms=DECISION
            )


def test_decision_context_itself_rejects_wrong_modeled_as_of():
    value, _ = modeled(ProfileDecisionStatus.MISSING)
    with pytest.raises(ValueError, match="^PROFILE_DECISION_INVALID$"):
        replace(value, request=replace(value.request, as_of_ms=DECISION + 1))


@pytest.mark.parametrize(
    "provider,policy", [(object(), POLICY), (Provider(None), "bad policy")]
)
def test_configuration_is_explicit_and_safe(provider, policy):
    with pytest.raises(ModeledProfileInputError):
        ModeledProfileInput(cast(Any, provider), policy)
