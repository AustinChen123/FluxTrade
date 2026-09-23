"""Pure ordered hydration evidence, not database authority or an executable scope.

Recorded descriptors retain terminal outcomes and exact pinned inputs; they never
consult a current modeled view. A later composition owner must bind strategy and
candle identities before executing scopes. Mutable runtime candles are not owned
by this plan. Caller limits bound construction, not semantic evidence identity.
"""

from dataclasses import InitVar, dataclass

from .bootstrap_seed import (
    BootstrapCandle,
    BootstrapKey,
    BootstrapDisposition,
    BootstrapSeed,
    BootstrapSeedError,
    classify_bootstrap,
)
from .decision_application import MarketDataDecisionOutcome
from .decision_input import MarketDataDecisionInput
from .requirements import ProfileRequirement


@dataclass(frozen=True, slots=True)
class BootstrapHydrationPlan:
    seed: BootstrapSeed
    recorded: tuple[
        tuple[MarketDataDecisionOutcome, MarketDataDecisionInput | None], ...
    ]
    completed_recorded_through_ms: int | None
    max_seed_candles: InitVar[int]
    max_recorded_candles: InitVar[int]
    expected_key: InitVar[BootstrapKey]
    requirements: InitVar[tuple[ProfileRequirement, ...]]
    lookback: InitVar[int]

    def __post_init__(
        self,
        max_seed_candles: int,
        max_recorded_candles: int,
        expected_key: BootstrapKey,
        requirements: tuple[ProfileRequirement, ...],
        lookback: int,
    ) -> None:
        if (
            type(self.seed) is not BootstrapSeed
            or type(expected_key) is not BootstrapKey
            or type(requirements) is not tuple
            or any(type(item) is not ProfileRequirement for item in requirements)
            or type(lookback) is not int
            or self.seed.key != expected_key
            or self.seed.requirements != requirements
            or self.seed.lookback != lookback
        ):
            raise BootstrapSeedError()
        disposition = classify_bootstrap(
            requirements,
            proposed=self.seed,
            stored=self.seed,
            history_known_absent=False,
            completed_recorded_through_ms=self.completed_recorded_through_ms,
            recorded=self.recorded,
            max_seed_candles=max_seed_candles,
            max_recorded_candles=max_recorded_candles,
        )
        if disposition is not BootstrapDisposition.REPLAY:
            raise BootstrapSeedError()

    @property
    def seed_candles(self) -> tuple[BootstrapCandle, ...]:
        """Completed seed in callback order; suffix descriptors follow this tuple."""
        return self.seed.candles
