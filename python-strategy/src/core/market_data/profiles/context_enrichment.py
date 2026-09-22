"""Attach fixed profile inputs after account context construction, without I/O.

Only utc_complete_strict_v1 completed-window intent is supported. LIVE matches
the request freshness policy; MODELED preserves its named availability policy
without mapping it to freshness. Model selection/evidence belongs to runner config.
"""

from dataclasses import replace

from src.core.strategy_context import StrategyContext

from .decision_context import ProfileDecisionBasis, StrategyMarketDataContext
from .requirements import ProfileRequirement


class ProfileContextEnrichmentError(ValueError):
    def __init__(self) -> None:
        super().__init__("PROFILE_CONTEXT_ENRICHMENT_INVALID")


def enrich_profile_context(
    context: StrategyContext,
    requirements: tuple[ProfileRequirement, ...],
    market_data: StrategyMarketDataContext,
    *,
    decision_time_ms: int,
) -> StrategyContext:
    """Exact coverage, no callback suppression; account timestamp remains untouched."""
    if type(context) is not StrategyContext or context.market_data is not None:
        raise ProfileContextEnrichmentError() from None
    _validate_coverage(requirements, market_data, decision_time_ms)
    return replace(context, market_data=market_data) if requirements else context


def _validate_coverage(
    requirements: tuple[ProfileRequirement, ...],
    market_data: StrategyMarketDataContext,
    decision_time_ms: int,
) -> None:
    if (
        type(requirements) is not tuple
        or any(type(item) is not ProfileRequirement for item in requirements)
        or type(market_data) is not StrategyMarketDataContext
        or type(decision_time_ms) is not int
        or not 0 <= decision_time_ms <= 2**63 - 1
        or market_data.decision_time_ms != decision_time_ms
    ):
        raise ProfileContextEnrichmentError() from None
    if not requirements:
        if market_data.profiles:
            raise ProfileContextEnrichmentError() from None
        return
    end = decision_time_ms // 86400000 * 86400000
    expected = set()
    for requirement in requirements:
        if requirement.freshness_policy_id != "utc_complete_strict_v1":
            raise ProfileContextEnrichmentError() from None
        key = (
            requirement.product_id,
            requirement.base_grid_id,
            requirement.output_grid_id,
            requirement.algorithm_version,
            end - requirement.window_days * 86400000,
            end,
        )
        if key[4] < 0 or key in expected:
            raise ProfileContextEnrichmentError() from None
        expected.add(key)
    actual = set()
    bases = set()
    for item in market_data.profiles:
        request = item.request
        bases.add(item.basis)
        if item.basis is ProfileDecisionBasis.LIVE_OBSERVED:
            valid = (
                request.purpose == "LIVE_QUERY"
                and request.freshness_policy_id == "utc_complete_strict_v1"
            )
        else:
            valid = (
                request.purpose == "MODELED_RESEARCH"
                and request.as_of_ms == decision_time_ms
                and request.availability_policy_id is not None
            )
        key = (
            request.product_id,
            request.base_grid_id,
            request.output_grid_id,
            request.algorithm_version,
            request.start_ms,
            request.end_ms,
        )
        if not valid or key in actual:
            raise ProfileContextEnrichmentError() from None
        actual.add(key)
    if len(bases) != 1 or actual != expected:
        raise ProfileContextEnrichmentError() from None
