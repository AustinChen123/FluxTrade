from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock

import pytest

from src.core.market_data.profiles.decision_identity import strategy_decision_composition
from src.core.market_data.profiles.decision_input_store import (
    DecisionInputRecord,
    DecisionInputPinResult,
    DecisionInputPinStatus,
)
from src.core.market_data.profiles.decision_owner import MarketDataDecisionOwner
from src.core.market_data.profiles.snapshot_cache import ProfileSnapshotCache
from src.core.signal_processor import StrategyDecisionSkipped
from test_profile_context_enrichment import context as example_context
from test_profile_decision_input import sample
from test_signal_processor import DummyStrategy, make_candle

DAY = 86_400_000
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Strategy(DummyStrategy):
    __fluxtrade_artifact_version__ = "1.2.0"

    @property
    def requirements(self):
        return replace(
            super().requirements,
            profile_requirements=sample().requirements,
        )

    def replay_configuration(self):
        return {"profile": True}


def base_context(strategy):
    candle = make_candle()
    return replace(
        example_context(),
        strategy_id=strategy.strategy_id,
        product_id=candle.product_id,
        timestamp=candle.timestamp,
    )


def decision(store):
    cache = ProfileSnapshotCache(Mock(), utc_ms=lambda: DAY + 3, monotonic_ms=lambda: 10)
    owner = MarketDataDecisionOwner(
        environment="live",
        execution_scope_id="live-berlin-1",
        identity_resolver=lambda strategy: strategy_decision_composition(
            "live-berlin-1", strategy
        ),
        cache=cache,
        input_store=store,
        utc_ms=lambda: DAY + 3,
        monotonic_ms=lambda: 10,
    )
    candle = make_candle()
    return candle, owner.begin_candle(candle)


@pytest.mark.parametrize(
    "status,disposition,reason",
    [
        (DecisionInputPinStatus.CONFIRMED, "APPLIED", None),
        (DecisionInputPinStatus.FAILED, "SKIPPED", "INPUT_STORE_FAILED"),
        (
            DecisionInputPinStatus.UNCONFIRMED,
            "SKIPPED",
            "INPUT_COMMIT_UNCONFIRMED",
        ),
    ],
)
def test_pin_state_maps_to_callback_outcome(status, disposition, reason):
    store = MagicMock()

    def result(value):
        record = (
            DecisionInputRecord(value, NOW, False)
            if status is DecisionInputPinStatus.CONFIRMED
            else None
        )
        return DecisionInputPinResult(status, record)

    store.pin_confirmed.side_effect = result
    strategy, (candle, scope) = Strategy("profile_strategy"), decision(store)
    manager = scope(strategy, candle, base_context(strategy))
    if disposition == "APPLIED":
        with manager as enriched:
            assert enriched is not None and enriched.market_data is not None
            assert enriched.market_data.profiles[0].status.value == "MISSING"
    else:
        with pytest.raises(StrategyDecisionSkipped):
            with manager:
                pytest.fail("skipped callback must not start")
    batch = scope.build()
    assert batch is not None
    assert (batch.outcomes[0].disposition, batch.outcomes[0].reason) == (
        disposition,
        reason,
    )
    assert store.pin_confirmed.call_count == 1
