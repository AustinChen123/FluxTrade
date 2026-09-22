from dataclasses import FrozenInstanceError, dataclass, replace
from decimal import Decimal
from enum import Enum
from typing import Any, cast

import pytest

from src.core.market_data.profiles.decision_application import MarketDataDecisionKey
from src.core.market_data.profiles.decision_identity import (
    DecisionCompositionError,
    MarketDataDecisionCompositionIdentity,
    decision_config_hash,
)


def identity(**changes):
    values = {
        "execution_scope_id": "live-berlin-1",
        "strategy_version": "1.2.0+profile.1",
        "configuration": {
            "quantity": Decimal("0.001"),
            "window_days": 7,
        },
        **changes,
    }
    return MarketDataDecisionCompositionIdentity.from_configuration(**values)


def test_identity_is_exact_immutable_and_key_compatible():
    value = identity()
    assert value.execution_scope_id == "live-berlin-1"
    assert value.strategy_version == "1.2.0+profile.1"
    assert len(value.config_hash) == 64
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        cast(Any, value).execution_scope_id = "other"

    key = MarketDataDecisionKey.for_candle(
        environment="live",
        execution_scope_id=value.execution_scope_id,
        strategy_id="strategy_v1",
        strategy_version=value.strategy_version,
        config_hash=value.config_hash,
        product_id="BINANCE:BTCUSDT-SPOT",
        timeframe="1m",
        bar_start_ms=0,
    )
    assert key.strategy_version == "1.2.0+profile.1"


def test_configuration_hash_is_canonical_and_decimal_exact():
    left = identity(configuration={"quantity": Decimal("0.001"), "window_days": 7})
    reordered = identity(configuration={"window_days": 7, "quantity": Decimal("0.001")})
    equivalent = identity(
        configuration={"quantity": Decimal("0.0010"), "window_days": 7}
    )
    changed = identity(configuration={"quantity": Decimal("0.0011"), "window_days": 7})
    assert left.config_hash == reordered.config_hash
    assert left.config_hash == equivalent.config_hash
    assert left.config_hash != changed.config_hash


def test_three_identities_change_independently():
    value = identity()
    assert identity(execution_scope_id="live-berlin-2").config_hash == value.config_hash
    assert identity(strategy_version="1.2.1").config_hash == value.config_hash
    assert identity(configuration={"window_days": 8}).config_hash != value.config_hash


class Mode(Enum):
    LIVE = "live"


@dataclass(frozen=True)
class Nested:
    values: tuple[object, ...]


@pytest.mark.parametrize(
    "configuration",
    [
        0.1,
        {"nested": [0.1]},
        Nested((Decimal("1"), 0.1)),
        {Mode.LIVE: Decimal("1")},
    ],
)
def test_configuration_rejects_float_and_non_string_mapping_keys(configuration):
    with pytest.raises(DecisionCompositionError) as caught:
        decision_config_hash(configuration)
    assert (
        str(caught.value) == "MARKET_DATA_DECISION_COMPOSITION_INVALID"
        and caught.value.__cause__ is None
    )


@pytest.mark.parametrize(
    "field,bad",
    [
        ("execution_scope_id", ""),
        ("execution_scope_id", "live.berlin"),
        ("execution_scope_id", "live/berlin"),
        ("strategy_version", ".1.0.0"),
        ("strategy_version", "1.0.0/secret"),
        ("strategy_version", "版本1"),
        ("config_hash", "A" * 64),
        ("config_hash", "a" * 63),
    ],
)
def test_constructor_rejects_ambiguous_identity_fields(field, bad):
    value = identity()
    with pytest.raises(DecisionCompositionError) as caught:
        replace(value, **{field: bad})
    assert (
        str(caught.value) == "MARKET_DATA_DECISION_COMPOSITION_INVALID"
        and caught.value.__cause__ is None
    )


@pytest.mark.parametrize("field", ["execution_scope_id", "strategy_version"])
def test_constructor_rejects_string_subclasses(field):
    value = identity()
    hostile = type("HostileString", (str,), {})(getattr(value, field))
    with pytest.raises(DecisionCompositionError):
        replace(value, **{field: hostile})
