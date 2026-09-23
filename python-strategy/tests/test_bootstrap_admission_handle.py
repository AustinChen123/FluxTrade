from dataclasses import FrozenInstanceError, replace
from copy import copy, deepcopy
from typing import Any, cast

import pytest

from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedAdmission,
    BootstrapSeedAdmissionError,
)
from test_bootstrap_seed_admission import harness, Halt
from test_profile_bootstrap_seed import seed


@pytest.mark.parametrize("exit_kind", ["normal", "exception", "base_exception"])
def test_handle_closes_before_unlock(exit_kind):
    store, _, connection, _ = harness()
    key = seed().key
    original = connection.execute.side_effect
    handles = []

    def execute(sql, params):
        if "pg_advisory_unlock" in str(sql):
            assert handles[0]._active is False
            with pytest.raises(BootstrapSeedAdmissionError):
                handles[0].read_history(key, 0)
        return original(sql, params)

    connection.execute.side_effect = execute
    error = RuntimeError("body") if exit_kind == "exception" else Halt("body")

    def run():
        with store.initial_admission(key) as handle:
            handles.append(handle)
            assert handle._active is True and handle.key is key
            assert not hasattr(handle, "connection") and not hasattr(handle, "session")
            assert not hasattr(handle, "__dict__")
            with pytest.raises(FrozenInstanceError):
                setattr(handle, "key", replace(key, strategy_id="other"))
            with pytest.raises(BootstrapSeedAdmissionError):
                handle.read_history(key, 60000)
            assert connection.execute.call_count == 1
            if exit_kind != "normal":
                raise error

    if exit_kind == "normal":
        run()
    else:
        with pytest.raises(type(error)) as caught:
            run()
        assert caught.value is error
    for _ in range(2):
        with pytest.raises(BootstrapSeedAdmissionError):
            handles[0].read_history(key, 60000)
    assert connection.execute.call_count == 2


def test_manual_construction_replace_and_deepcopy_cannot_escape_lease():
    key = seed().key
    with pytest.raises(BootstrapSeedAdmissionError):
        BootstrapSeedAdmission(key)
    store, session, connection, _ = harness()
    with store.initial_admission(key) as handle:
        with pytest.raises(BootstrapSeedAdmissionError):
            replace(handle)
        cloned = copy(handle)
        deep = deepcopy(handle)
        assert deep is handle and cloned._lease is handle._lease
    for instance in (handle, cloned, deep):
        assert not instance._active
        with pytest.raises(BootstrapSeedAdmissionError):
            instance.read_history(key, 0)
    assert connection.execute.call_count == 2
    assert session.execute.call_count == 2


@pytest.mark.parametrize("bad", [True, -60000, 1, 2**63, "0"])
def test_invalid_boundary_and_wrong_key_never_issue_history_sql(bad):
    store, _, connection, _ = harness()
    key = seed().key
    with store.initial_admission(key) as handle:
        with pytest.raises(BootstrapSeedAdmissionError):
            handle.read_history(key, cast(Any, bad))
        for wrong in (
            None,
            replace(key, strategy_version="v2"),
            replace(key, config_hash="f" * 64),
        ):
            with pytest.raises(BootstrapSeedAdmissionError):
                handle.read_history(cast(Any, wrong), 60000)
        assert connection.execute.call_count == 1


def test_unlock_failure_leaves_handle_permanently_inactive():
    store, _, connection, _ = harness(released=False)
    key = seed().key
    handle = None
    with pytest.raises(BootstrapSeedAdmissionError):
        with store.initial_admission(key) as handle:
            assert handle._active is True
    assert handle is not None
    assert handle._active is False
    with pytest.raises(BootstrapSeedAdmissionError):
        handle.read_history(key, 0)
    assert connection.execute.call_count == 2
