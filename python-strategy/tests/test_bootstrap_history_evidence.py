from dataclasses import FrozenInstanceError, asdict, replace
from typing import Any, cast

import pytest

from src.core.bootstrap_hydration_reader import (
    BootstrapHistoryEvidence,
)
from src.core.market_data.profiles.bootstrap_seed import (
    BootstrapKey,
    BootstrapSeedError,
    BootstrapHistoryEvidence as DomainEvidence,
)
from test_profile_bootstrap_seed import seed

KEY = seed().key
MAX = (1 << 63) - 1


def test_reader_reexports_pure_domain_type():
    assert BootstrapHistoryEvidence is DomainEvidence


@pytest.mark.parametrize("state", ["ABSENT", "PRESENT", "UNKNOWN"])
@pytest.mark.parametrize("boundary", [0, 60000, MAX // 60000 * 60000])
def test_closed_states_exact_key_and_aligned_boundary(state, boundary):
    value = BootstrapHistoryEvidence(KEY, boundary, state)
    assert value.key is KEY and value.boundary_bar_start_ms == boundary
    assert value.state == state and replace(value) == value
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(value, "state", "ABSENT")


@pytest.mark.parametrize(
    "bad",
    [
        True,
        -60000,
        (MAX // 60000 + 1) * 60000,
        60000.0,
        "60000",
        1,
        MAX,
        type("Int", (int,), {})(60000),
    ],
)
def test_exact_clock_domain_and_alignment(bad):
    with pytest.raises(BootstrapSeedError):
        BootstrapHistoryEvidence(KEY, bad, "ABSENT")


@pytest.mark.parametrize(
    "bad", [None, True, "absent", "", "SECRET", type("Text", (str,), {})("ABSENT")]
)
def test_state_type_and_domain_are_closed(bad):
    with pytest.raises(BootstrapSeedError) as caught:
        replace(BootstrapHistoryEvidence(KEY, 0, "ABSENT"), state=bad)
    assert str(caught.value) == "PROFILE_BOOTSTRAP_HISTORY_EVIDENCE_INVALID"
    assert caught.value.__cause__ is None


def test_exact_key_not_subclass_mapping_or_mutable_object():
    class DerivedKey(BootstrapKey):
        pass

    for bad in (None, asdict(KEY), DerivedKey(**asdict(KEY)), object()):
        with pytest.raises(BootstrapSeedError):
            BootstrapHistoryEvidence(cast(Any, bad), 0, "ABSENT")


def test_boundary_alignment_uses_bound_keys_timeframe():
    hourly = replace(KEY, timeframe="1h")
    with pytest.raises(BootstrapSeedError):
        BootstrapHistoryEvidence(hourly, 60000, "ABSENT")
    assert BootstrapHistoryEvidence(hourly, 3600000, "UNKNOWN").key is hourly
