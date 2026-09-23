"""Read existing bootstrap/terminal evidence into a detached executable binding.

No pin, hydration, publication, provider, clock, retry, or modeled fallback. The
caller owns authoritative boundary selection and the later exposure lifecycle.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Literal

from sqlalchemy.orm import Session

from src.core.data_provider import timeframe_to_ms
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.bootstrap_hydration import (
    BootstrapHydrationPlan,
    BoundBootstrapHydration,
)
from src.core.market_data.profiles.bootstrap_seed import BootstrapKey
from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedRecord,
)
from src.core.market_data.profiles.decision_application_store import DecisionBatchRecord
from src.core.market_data.profiles.decision_identity import (
    MarketDataDecisionCompositionIdentity,
)
from src.core.market_data.profiles.decision_owner import (
    MarketDataDecisionOwner,
    IdentityResolver,
)
from src.core.market_data.profiles.read_types import _integer
from src.strategies.base import BaseStrategy


class BootstrapHydrationReaderError(ValueError):
    def __init__(self) -> None:
        super().__init__("BOOTSTRAP_HYDRATION_READER_INVALID")


class BootstrapHydrationReader:
    def __init__(
        self,
        *,
        db_session_factory: Callable[[], AbstractContextManager[Session]],
        seed_store: BootstrapSeedStore,
        application: LiveCandleApplicationService,
        decision_owner: MarketDataDecisionOwner,
        environment: str,
        identity_resolver: IdentityResolver,
        max_seed_candles: int,
        max_recorded_candles: int,
    ) -> None:
        try:
            _integer(max_seed_candles, 1)
            _integer(max_recorded_candles, 1)
            if (
                not callable(db_session_factory)
                or not callable(identity_resolver)
                or type(environment) is not str
            ):
                raise ValueError
        except ValueError:
            raise BootstrapHydrationReaderError() from None
        self._sessions, self._seeds, self._application = (
            db_session_factory,
            seed_store,
            application,
        )
        self._owner, self._environment, self._identity = (
            decision_owner,
            environment,
            identity_resolver,
        )
        self._max_seed, self._max_recorded = max_seed_candles, max_recorded_candles

    def prepare(
        self,
        strategy: BaseStrategy,
        boundary_bar_start_ms: int,
        mode: Literal["BEFORE_PENDING", "THROUGH_APPLIED"] = "BEFORE_PENDING",
    ) -> BoundBootstrapHydration:
        try:
            _integer(boundary_bar_start_ms)
            if (
                not isinstance(strategy, BaseStrategy)
                or type(mode) is not str
                or mode not in ("BEFORE_PENDING", "THROUGH_APPLIED")
            ):
                raise ValueError
        except ValueError:
            raise BootstrapHydrationReaderError() from None
        identity = self._identity(strategy)
        if type(identity) is not MarketDataDecisionCompositionIdentity:
            raise BootstrapHydrationReaderError()
        requirements = strategy.requirements
        key = BootstrapKey(
            self._environment,
            identity.execution_scope_id,
            strategy.strategy_id,
            identity.strategy_version,
            identity.config_hash,
            strategy.product_id,
            requirements.timeframe,
        )
        record = self._seeds.get(key)
        if type(record) is not BootstrapSeedRecord or record.value.key != key:
            raise BootstrapHydrationReaderError()
        seed = record.value
        if (
            seed.requirements != requirements.profile_requirements
            or seed.lookback != requirements.lookback_window
            or seed.lookback > self._max_seed
        ):
            raise BootstrapHydrationReaderError()
        duration = timeframe_to_ms(key.timeframe)
        if boundary_bar_start_ms < seed.cutover_ms or boundary_bar_start_ms % duration:
            raise BootstrapHydrationReaderError()
        count = (boundary_bar_start_ms - seed.cutover_ms) // duration + (
            mode == "THROUGH_APPLIED"
        )
        if count > self._max_recorded:
            raise BootstrapHydrationReaderError()
        through = seed.cutover_ms + (count - 1) * duration if count else None
        recorded, suffix = [], []
        if count:
            with self._sessions() as db:
                for index in range(count):
                    candle, evidence = self._application.read_applied_candle(
                        product_id=key.product_id,
                        timeframe=key.timeframe,
                        bar_start_ms=seed.cutover_ms + index * duration,
                        db=db,
                    )
                    if type(evidence) is not DecisionBatchRecord:
                        raise BootstrapHydrationReaderError()
                    positions = [
                        i
                        for i, outcome in enumerate(evidence.batch.outcomes)
                        if outcome.key.strategy_id == strategy.strategy_id
                    ]
                    if len(positions) != 1:
                        raise BootstrapHydrationReaderError()
                    position = positions[0]
                    prepared = self._owner.prepare_replay_candle(
                        strategy, candle, evidence.batch
                    )
                    recorded.append(
                        (
                            evidence.batch.outcomes[position],
                            evidence.verified_inputs[position],
                        )
                    )
                    suffix.append((candle, prepared))
        plan = BootstrapHydrationPlan(
            seed,
            tuple(recorded),
            through,
            self._max_seed,
            self._max_recorded,
            key,
            requirements.profile_requirements,
            requirements.lookback_window,
        )
        return BoundBootstrapHydration(plan, strategy, identity, tuple(suffix))
