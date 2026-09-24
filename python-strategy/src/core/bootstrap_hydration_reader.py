"""Read existing bootstrap/terminal evidence into a detached executable binding.

No hydration, publication, clock, retry, or modeled fallback. Initial seed pin
requires a trusted history reader and an upper-layer admission fence serializing
ABSENT through pin; DTO typing is not authority. This seam is not activation.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Literal

from sqlalchemy.orm import Session
from sqlalchemy import func, select, text

from src.core.data_provider import timeframe_to_ms
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.bootstrap_hydration import (
    BootstrapHydrationPlan,
    BoundBootstrapHydration,
)
from src.core.market_data.profiles.bootstrap_seed import (
    BootstrapHistoryEvidence as BootstrapHistoryEvidence,
    BootstrapKey,
    BootstrapSeed,
    BootstrapDisposition,
    classify_bootstrap,
)
from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedRecord,
    BootstrapSeedPinResult,
    BootstrapSeedPinStatus,
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
from src.core.market_data.profiles.orm import MarketDataDecisionOutcome
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.orm_models import Candlestick as CandleRow


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

    def prepare_initial_seed(
        self,
        strategy: BaseStrategy,
        boundary_bar_start_ms: int,
        *,
        history_reader: Callable[[BootstrapKey, int], BootstrapHistoryEvidence],
        factory: Callable[[BootstrapKey], BootstrapSeed],
    ) -> BootstrapSeedRecord:
        """Caller must hold the authority/admission fence across this entire call."""
        key, requirements = self._initial_identity(strategy, boundary_bar_start_ms)
        return self._prepare_initial_seed(
            key, requirements, boundary_bar_start_ms, history_reader, factory
        )

    def prepare_initial_seed_under_admission(
        self,
        strategy: BaseStrategy,
        boundary_bar_start_ms: int,
        *,
        factory: Callable[[BootstrapKey], BootstrapSeed],
    ) -> BootstrapSeedRecord:
        """Compose cooperating-caller admission and authority; not activation."""
        key, requirements = self._initial_identity(strategy, boundary_bar_start_ms)
        if not callable(factory):
            raise BootstrapHydrationReaderError()
        with self._seeds.initial_admission(key) as admission:
            return self._prepare_initial_seed(
                key,
                requirements,
                boundary_bar_start_ms,
                admission.read_history,
                factory,
            )

    def _initial_identity(
        self, strategy: BaseStrategy, boundary_bar_start_ms: int
    ) -> tuple[BootstrapKey, StrategyRequirements]:
        try:
            _integer(boundary_bar_start_ms)
            if not isinstance(strategy, BaseStrategy):
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
        if (
            not requirements.profile_requirements
            or requirements.lookback_window > self._max_seed
            or boundary_bar_start_ms % timeframe_to_ms(key.timeframe)
        ):
            raise BootstrapHydrationReaderError()
        return key, requirements

    def _prepare_initial_seed(
        self,
        key: BootstrapKey,
        requirements: StrategyRequirements,
        boundary_bar_start_ms: int,
        history_reader: Callable[[BootstrapKey, int], BootstrapHistoryEvidence],
        factory: Callable[[BootstrapKey], BootstrapSeed],
    ) -> BootstrapSeedRecord:
        record = self._seeds.get(key)
        if record is not None:
            if (
                type(record) is not BootstrapSeedRecord
                or record.value.key != key
                or record.value.requirements != requirements.profile_requirements
                or record.value.lookback != requirements.lookback_window
            ):
                raise BootstrapHydrationReaderError()
            return record
        if not callable(history_reader) or not callable(factory):
            raise BootstrapHydrationReaderError()
        proof = history_reader(key, boundary_bar_start_ms)
        if (
            type(proof) is not BootstrapHistoryEvidence
            or proof.key != key
            or proof.boundary_bar_start_ms != boundary_bar_start_ms
            or proof.state != "ABSENT"
        ):
            raise BootstrapHydrationReaderError()
        candidate = factory(key)
        if (
            type(candidate) is not BootstrapSeed
            or candidate.key != key
            or candidate.cutover_ms != boundary_bar_start_ms
            or candidate.requirements != requirements.profile_requirements
            or candidate.lookback != requirements.lookback_window
        ):
            raise BootstrapHydrationReaderError()
        if (
            classify_bootstrap(
                requirements.profile_requirements,
                proposed=candidate,
                stored=None,
                history_known_absent=True,
                completed_recorded_through_ms=None,
                recorded=(),
                max_seed_candles=self._max_seed,
                max_recorded_candles=self._max_recorded,
            )
            is not BootstrapDisposition.BOOTSTRAP
        ):
            raise BootstrapHydrationReaderError()
        result = self._seeds.pin_confirmed(candidate)
        if (
            type(result) is not BootstrapSeedPinResult
            or result.status is not BootstrapSeedPinStatus.CONFIRMED
            or type(result.record) is not BootstrapSeedRecord
            or result.record.value != candidate
        ):
            raise BootstrapHydrationReaderError()
        return result.record

    def prepare_latest_applied(self, strategy: BaseStrategy) -> BoundBootstrapHydration:
        """Use this strategy's terminal lineage, never the global candle head."""
        key, requirements = self._initial_identity(strategy, 0)
        record = self._seeds.get(key)
        if (
            type(record) is not BootstrapSeedRecord
            or record.value.key != key
            or record.value.requirements != requirements.profile_requirements
            or record.value.lookback != requirements.lookback_window
        ):
            raise BootstrapHydrationReaderError()
        table = MarketDataDecisionOutcome.__table__
        with self._sessions() as db:
            if db.get_bind().dialect.name != "postgresql" or db.in_transaction():
                raise BootstrapHydrationReaderError()
            with db.begin():
                db.execute(
                    text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
                )
                latest = db.execute(
                    select(func.max(table.c.bar_start_ms)).where(
                        *(
                            getattr(table.c, name) == getattr(key, name)
                            for name in (
                                "environment",
                                "execution_scope_id",
                                "strategy_id",
                                "strategy_version",
                                "config_hash",
                                "product_id",
                                "timeframe",
                            )
                        ),
                        table.c.trigger_kind == "CANDLE",
                    )
                ).scalar_one()
        if latest is None:
            return self.prepare(strategy, record.value.cutover_ms, "BEFORE_PENDING")
        try:
            _integer(latest)
            if latest < record.value.cutover_ms or latest % timeframe_to_ms(
                key.timeframe
            ):
                raise ValueError
        except ValueError:
            raise BootstrapHydrationReaderError() from None
        return self.prepare(strategy, latest, "THROUGH_APPLIED")

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
        through = None
        recorded, suffix = [], []
        if count:
            with self._sessions() as db:
                candles = CandleRow.__table__
                starts = db.execute(
                    select(candles.c.timestamp)
                    .where(
                        candles.c.product_id == key.product_id,
                        candles.c.timeframe == key.timeframe,
                        candles.c.timestamp >= seed.cutover_ms,
                        candles.c.timestamp <= boundary_bar_start_ms
                        if mode == "THROUGH_APPLIED"
                        else candles.c.timestamp < boundary_bar_start_ms,
                    )
                    .order_by(candles.c.timestamp)
                    .execution_options(yield_per=128)
                ).scalars()
                previous = seed.cutover_ms - duration
                for start in starts:
                    if (
                        type(start) is not int
                        or start <= previous
                        or start < seed.cutover_ms
                        or start % duration
                        or start > boundary_bar_start_ms
                        or (start == boundary_bar_start_ms and mode == "BEFORE_PENDING")
                    ):
                        raise BootstrapHydrationReaderError()
                    previous = start
                    candle, evidence = self._application.read_applied_candle(
                        product_id=key.product_id,
                        timeframe=key.timeframe,
                        bar_start_ms=start,
                        db=db,
                    )
                    if type(evidence) is not DecisionBatchRecord:
                        raise BootstrapHydrationReaderError()
                    positions = [
                        i
                        for i, outcome in enumerate(evidence.batch.outcomes)
                        if outcome.key.strategy_id == strategy.strategy_id
                    ]
                    if not positions:
                        continue
                    if len(positions) != 1 or len(recorded) >= self._max_recorded:
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
                    through = start
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
