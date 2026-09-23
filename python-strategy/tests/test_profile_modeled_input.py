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
    completed_candle_decision_time_ms,
)
from src.core.market_data.profiles.modeled_selection import MODELED_AVAILABILITY_POLICY
from src.core.market_data.profiles.requirements import ProfileRequirement
from test_profile_context_enrichment import fixture

DAY = 86_400_000
DECISION = DAY + 3
POLICY = MODELED_AVAILABILITY_POLICY.policy_id


@pytest.mark.parametrize(
    ("timestamp", "timeframe", "expected"),
    [
        (DAY - 60_000, "1m", DAY),
        (DAY, "1m", DAY + 60_000),
        (DAY, "7d", 8 * DAY),
        (DAY, "30d", 31 * DAY),
    ],
)
def test_completed_decision_time_is_exclusive_candle_end(
    timestamp, timeframe, expected
):
    assert completed_candle_decision_time_ms(timestamp, timeframe) == expected


@pytest.mark.parametrize(
    ("timestamp", "timeframe"),
    [
        (True, "1m"),
        (-1, "1m"),
        ((1 << 63) - 59_999, "1m"),
        (0, "0m"),
        (0, "-1m"),
        (0, ""),
        (0, None),
    ],
)
def test_completed_decision_time_rejects_invalid_or_overflowing_input(
    timestamp, timeframe
):
    with pytest.raises(ModeledProfileInputError):
        completed_candle_decision_time_ms(cast(Any, timestamp), cast(Any, timeframe))


class Provider:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, int, str]] = []
        self.availability_policy_id = POLICY
        self.availability_policy_digest = MODELED_AVAILABILITY_POLICY.digest
        self.dataset_digest = "a" * 64

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


def test_provider_identity_is_validated_and_snapshotted() -> None:
    provider = Provider(None)
    configured = ModeledProfileInput(provider, POLICY)
    provider.dataset_digest = "b" * 64
    provider.availability_policy_digest = "c" * 64
    assert configured.dataset_digest == "a" * 64
    assert configured.availability_policy_digest == MODELED_AVAILABILITY_POLICY.digest


def test_pre_call_identity_drift_rejects_without_provider_call() -> None:
    value, requirement = modeled(ProfileDecisionStatus.MISSING)
    provider = Provider(StrategyMarketDataContext(DECISION, (value,)))
    configured = ModeledProfileInput(provider, POLICY)
    provider.dataset_digest = "b" * 64
    with pytest.raises(ModeledProfileInputError):
        configured.resolve((requirement,), decision_time_ms=DECISION)
    assert provider.calls == []


@pytest.mark.parametrize("field,value", [("dataset_digest", "b" * 64),
    ("availability_policy_digest", "c" * 64), ("availability_policy_id", "other")])
def test_in_call_identity_drift_rejects_returned_context(field, value) -> None:
    item, requirement = modeled(ProfileDecisionStatus.MISSING)

    class DriftingProvider(Provider):
        def context_for(self, *args, **kwargs):
            result = super().context_for(*args, **kwargs)
            setattr(self, field, value)
            return result

    provider = DriftingProvider(StrategyMarketDataContext(DECISION, (item,)))
    configured = ModeledProfileInput(provider, POLICY)
    with pytest.raises(ModeledProfileInputError):
        configured.resolve((requirement,), decision_time_ms=DECISION)
    assert len(provider.calls) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("availability_policy_id", "other"),
        ("availability_policy_digest", "short"),
        ("dataset_digest", "g" * 64),
        ("dataset_digest", type("SubStr", (str,), {})("a" * 64)),
    ],
)
def test_provider_identity_rejects_mismatch_or_malformed_digest(field, value) -> None:
    provider = Provider(None)
    setattr(provider, field, value)
    with pytest.raises(ModeledProfileInputError):
        ModeledProfileInput(provider, POLICY)
