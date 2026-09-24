"""Shared real-store/application fixture for isolated PostgreSQL acceptance."""

from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import os
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker

from src.core.bootstrap_hydration_reader import BootstrapHydrationReader
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.market_data.profiles.bootstrap_seed import BootstrapCandle
from src.core.market_data.profiles.bootstrap_seed_store import BootstrapSeedStore
from src.core.market_data.profiles.decision_application import (
    MarketDataDecisionBatch,
    MarketDataDecisionOutcome,
)
from src.core.market_data.profiles.decision_context import (
    ProfileDecisionBasis as Basis,
    ProfileDecisionStatus as Status,
    StrategyMarketDataContext,
)
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_input_store import DecisionInputStore
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.market_data.profiles.types import canonical_decimal_text
from src.core.orm_models import Candlestick as CandleRow
from src.strategies.base import StrategyRequirements
from test_profile_bootstrap_seed import seed, DAY, MINUTE
from test_profile_context_enrichment import fixture
from test_profile_modeled_input import modeled
from test_profile_recorded_decision_owner import Strategy
from test_signal_processor import make_candle, make_signal

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]
if os.environ.get("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1":
    pytest.skip(
        "isolated PostgreSQL acceptance requires explicit opt-in",
        allow_module_level=True,
    )

PRODUCT = "BINANCE:BTCUSDT-SPOT"
CUTOVER = DAY + 2 * MINUTE


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class Consumer(Strategy):
    def __init__(self):
        super().__init__("s", PRODUCT)
        self.accumulator = Decimal(0)
        self.trace = []

    @property
    def requirements(self):
        return StrategyRequirements(
            PRODUCT, "1m", 2, profile_requirements=seed().requirements
        )

    def on_candle(self, candle, context=None):
        assert context is not None and context.market_data is not None
        evidence = context.market_data
        item = evidence.profiles[0]
        volume = item.profile.base_volume if item.profile else Decimal(0)
        score = {Status.FRESH: 1, Status.MISSING: 2, Status.INVALID: 3}[item.status]
        self.accumulator = self.accumulator * 10 + candle.close + volume + score
        self.trace.append(
            [
                candle.timestamp,
                item.decision_time_ms,
                item.basis.value,
                item.status.value,
                str(volume),
                item.profile.composite_id if item.profile else None,
                evidence.digest,
            ]
        )
        return make_signal("s").model_copy(
            update={"product_id": PRODUCT, "timestamp": candle.timestamp}
        )

    def state(self):
        return {
            "accumulator": canonical_decimal_text(self.accumulator),
            "trace": self.trace,
        }


def evidence(index):
    status = (
        Status.FRESH,
        Status.MISSING,
        Status.FRESH,
        Status.MISSING,
        Status.INVALID,
    )[index]
    item, _ = modeled(status) if index < 2 else fixture(Basis.LIVE_OBSERVED, status)
    decision = DAY + (index + 1) * MINUTE
    request = replace(item.request, as_of_ms=decision) if index < 2 else item.request
    item = replace(item, request=request, decision_time_ms=decision)
    if index == 2:
        assert item.profile is not None
        item = replace(
            item,
            profile=replace(
                item.profile,
                bins=(replace(item.profile.bins[0], base_volume=Decimal(3)),),
                base_volume=Decimal(3),
            ),
        )
    return StrategyMarketDataContext(decision, (item,))


def build(engine, referenced, damage=None):
    strategy = Consumer()
    identity = strategy_decision_composition("deployment", strategy)
    value = seed()
    value = replace(
        value,
        key=replace(
            value.key,
            product_id=PRODUCT,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
        ),
        candles=tuple(
            BootstrapCandle(
                DAY + i * MINUTE,
                Decimal(10 + i * 10),
                Decimal(11 + i * 10),
                Decimal(9 + i * 10),
                Decimal(10 + i * 10),
                Decimal(1),
                evidence(i),
            )
            for i in range(2)
        ),
        max_seed_candles=2,
    )
    factory = sessionmaker(engine)
    active = []

    @contextmanager
    def sessions():
        with factory() as db:
            active.append(db)
            try:
                yield db
            finally:
                active.remove(db)

    seeds, inputs = BootstrapSeedStore(sessions), DecisionInputStore(sessions)
    if damage != "seed":
        seeds.pin(value)
    application = LiveCandleApplicationService(
        environment_identity=lambda: "live", db_session_factory=sessions
    )
    for index in range(2, 5):
        start = DAY + index * MINUTE
        key = value.key.decision_key(start)
        pinned = MarketDataDecisionInput(
            key, value.requirements, start + MINUTE, evidence(index)
        )
        inputs.pin(pinned)  # Null-reference SKIPPED deliberately leaves an orphan.
        skipped = index == 3
        outcome = MarketDataDecisionOutcome(
            key,
            "SKIPPED" if skipped else "APPLIED",
            pinned.input_id if not skipped or referenced else None,
            pinned.input_digest if not skipped or referenced else None,
            "INPUT_STORE_FAILED" if skipped else None,
        )
        candle = make_candle().model_copy(
            update={
                "product_id": PRODUCT,
                "timestamp": start,
                "open": Decimal((index + 1) * 10),
                "high": Decimal((index + 1) * 10 + 1),
                "low": Decimal((index + 1) * 10 - 1),
                "close": Decimal((index + 1) * 10),
                "volume": Decimal(1),
            }
        )
        batch = MarketDataDecisionBatch(
            "live", "deployment", PRODUCT, "1m", start, (key,), (outcome,)
        )
        if damage != "receipt" or index != 4:
            application.apply(
                candle,
                apply_new=lambda _, pending=batch: pending,
                rebuild_applied=lambda _: pytest.fail("rebuild"),
            )
        else:
            # Deliberate incomplete history: canonical candle exists, receipt does not.
            with factory.begin() as db:
                db.add(
                    CandleRow(
                        product_id=PRODUCT,
                        timeframe="1m",
                        timestamp=start,
                        open=candle.open,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        volume=candle.volume,
                    )
                )
    if damage == "ohlcv":
        with factory.begin() as db:
            db.execute(
                update(CandleRow)
                .where(
                    CandleRow.product_id == PRODUCT,
                    CandleRow.timestamp == CUTOVER + 2 * MINUTE,
                )
                .values(close=Decimal(49))
            )
    if damage in ("version", "config"):
        identity = replace(
            identity,
            **(
                {"strategy_version": "different"}
                if damage == "version"
                else {"config_hash": "f" * 64}
            ),
        )
    forbidden = MagicMock(side_effect=AssertionError("external dependency called"))
    forbidden.live_requests.side_effect = AssertionError("cache live_requests called")
    forbidden.decision_many.side_effect = AssertionError("cache decision_many called")
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="deployment",
        identity_resolver=lambda _: identity,
        cache=forbidden,
        input_store=inputs,
        utc_ms=forbidden,
        monotonic_ms=forbidden,
    )
    reader = BootstrapHydrationReader(
        db_session_factory=sessions,
        seed_store=seeds,
        application=application,
        decision_owner=owner,
        environment="live",
        identity_resolver=lambda _: identity,
        max_seed_candles=2,
        max_recorded_candles=3,
    )
    return strategy, reader, seeds, inputs, active, forbidden


@pytest.mark.parametrize("version", ["1.2.0", "1.2.0+profile.1"])
def test_formal_stores_application_and_reader_versions(
    profile_repository_pg, monkeypatch, version
):
    monkeypatch.setattr(Consumer, "__fluxtrade_artifact_version__", version)
    strategy, reader, seeds, inputs, active, _ = build(profile_repository_pg, True)
    bound = reader.prepare(strategy, CUTOVER + 3 * MINUTE)
    assert bound.plan.seed.key.strategy_version == version and not active
    stored_seed = seeds.get(bound.plan.seed.key)
    assert stored_seed is not None and stored_seed.value == bound.plan.seed
    for outcome, pinned in bound.plan.recorded:
        assert outcome.key.strategy_version == version and pinned is not None
        stored_input = inputs.get(outcome.key)
        assert stored_input is not None and stored_input.value == pinned
