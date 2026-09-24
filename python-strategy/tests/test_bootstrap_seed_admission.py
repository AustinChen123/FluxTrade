from contextlib import contextmanager
from dataclasses import asdict, replace
import ast
import inspect
import textwrap
from unittest.mock import MagicMock
from typing import Any, cast

import pytest

from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedAdmissionError,
    BootstrapSeedAdmission,
    _admission_lock_key,
)
from test_profile_bootstrap_seed import seed
from src.core.market_data.profiles.repository import TransactionWaitPolicy


def harness(acquired=True, released=True):
    events = []
    connection = MagicMock(closed=False, invalidated=False)
    responses = iter((acquired, released))

    def execute(sql, params):
        events.append((str(sql), params))
        value = next(responses)
        if isinstance(value, BaseException):
            raise value
        return MagicMock(scalar_one=lambda: value)

    connection.execute.side_effect = execute
    connection.invalidate.side_effect = lambda: events.append("invalidate")
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    session.connection.return_value = connection
    session.execute.side_effect = lambda sql, params=None: events.append(
        (str(sql), params)
    )

    @contextmanager
    def transaction():
        events.append("begin")
        try:
            yield
        finally:
            events.append("transaction exit")

    session.begin.side_effect = transaction

    @contextmanager
    def sessions():
        events.append("session")
        try:
            yield session
        finally:
            events.append("session exit")

    return BootstrapSeedStore(sessions), session, connection, events


def test_lineage_stability_and_scope():
    key = seed().key
    lock = _admission_lock_key(key)
    assert type(lock) is int and -(2**63) <= lock < 2**63
    assert lock == _admission_lock_key(
        replace(key, strategy_version="v2", config_hash="f" * 64)
    )
    assert lock == _admission_lock_key(seed(count=1).key)
    for field, value in (
        ("environment", "paper"),
        ("execution_scope_id", "other"),
        ("strategy_id", "other"),
        ("product_id", "BINANCE:ETHUSDT-SPOT"),
        ("timeframe", "5m"),
    ):
        assert lock != _admission_lock_key(replace(key, **{field: value}))


def test_exact_key_and_no_poll_retry_or_sleep():
    key = seed().key

    class Derived(type(key)):
        pass

    store, _, connection, events = harness()
    with pytest.raises(BootstrapSeedAdmissionError):
        with store.initial_admission(Derived(**asdict(key))):
            pytest.fail("body")
    assert events == []
    connection.execute.assert_not_called()
    tree = ast.parse(
        textwrap.dedent(inspect.getsource(BootstrapSeedStore.initial_admission))
    )
    assert not any(isinstance(node, (ast.For, ast.While)) for node in ast.walk(tree))
    assert not any(
        isinstance(node, ast.Attribute) and node.attr in ("sleep", "retry")
        for node in ast.walk(tree)
    )


def test_exact_order_single_connection_no_early_pool_return():
    store, session, connection, events = harness()
    key = seed().key
    with store.initial_admission(key) as token:
        assert type(token) is BootstrapSeedAdmission and token.key is key
        events.append("body")
        session.commit.assert_not_called()
        session.rollback.assert_not_called()
        session.close.assert_not_called()
    params = {"lock_key": _admission_lock_key(key)}
    assert events == [
        "session",
        "begin",
        ("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", None),
        (
            "SELECT set_config('lock_timeout', :lock_timeout, true), "
            "set_config('statement_timeout', :statement_timeout, true)",
            {"lock_timeout": "2000ms", "statement_timeout": "10000ms"},
        ),
        ("SELECT pg_try_advisory_lock(:lock_key)", params),
        "body",
        ("SELECT pg_advisory_unlock(:lock_key)", params),
        "transaction exit",
        "session exit",
    ]
    session.connection.assert_called_once_with()
    assert session.execute.call_count == 2
    assert connection.execute.call_count == 2
    connection.invalidate.assert_not_called()


def test_admission_uses_injected_transaction_wait_policy():
    original, session, _, _ = harness()
    store = BootstrapSeedStore(original._sessions, TransactionWaitPolicy(17, 29))
    with store.initial_admission(seed().key):
        pass
    assert session.execute.call_args_list[1].args[1] == {
        "lock_timeout": "17ms",
        "statement_timeout": "29ms",
    }


@pytest.mark.parametrize(
    "damage", ["key", "dialect", "transaction", "closed", "invalidated", "busy"]
)
def test_invalid_admission_never_enters_body(damage):
    store, session, connection, _ = harness(
        acquired=False if damage == "busy" else True
    )
    key = seed().key
    if damage == "key":
        key = None
    elif damage == "dialect":
        session.get_bind.return_value.dialect.name = "sqlite"
    elif damage == "transaction":
        session.in_transaction.return_value = True
    elif damage in ("closed", "invalidated"):
        setattr(connection, damage, True)
    with pytest.raises(BootstrapSeedAdmissionError, match="^BOOTSTRAP_SEED_ADMISSION$"):
        with store.initial_admission(cast(Any, key)):
            pytest.fail("body")
    assert connection.execute.call_count == (1 if damage == "busy" else 0)


class Halt(BaseException):
    pass


@pytest.mark.parametrize("error_type", [RuntimeError, Halt])
def test_body_failure_unlocks_and_preserves_identity(error_type):
    store, _, connection, events = harness()
    error = error_type("SECRET")
    with pytest.raises(error_type) as caught:
        with store.initial_admission(seed().key):
            raise error
    assert caught.value is error
    assert "pg_advisory_unlock" in events[-3][0]
    assert connection.execute.call_count == 2
    connection.invalidate.assert_not_called()


@pytest.mark.parametrize(
    "released", [False, None, 1, RuntimeError("SECRET"), Halt("SECRET")]
)
@pytest.mark.parametrize("body_error", [False, True])
def test_unlock_failure_invalidates_before_return_to_pool(released, body_error):
    store, _, connection, events = harness(released=released)
    with pytest.raises(BootstrapSeedAdmissionError) as caught:
        with store.initial_admission(seed().key):
            if body_error:
                raise Halt("body")
    assert str(caught.value) == "BOOTSTRAP_SEED_ADMISSION"
    assert caught.value.__cause__ is not None
    connection.invalidate.assert_called_once_with()
    assert events[-3:] == ["invalidate", "transaction exit", "session exit"]
    assert connection.execute.call_count == 2


@pytest.mark.parametrize("error", [RuntimeError("SECRET"), Halt("SECRET")])
def test_acquisition_error_does_not_retry_or_leak_connection(error):
    store, _, connection, _ = harness(acquired=error)
    expected = BootstrapSeedAdmissionError if isinstance(error, Exception) else Halt
    with pytest.raises(expected) as caught:
        with store.initial_admission(seed().key):
            pytest.fail("body")
    if isinstance(error, Halt):
        assert caught.value is error
    else:
        assert caught.value.__cause__ is error
    connection.invalidate.assert_called_once_with()
    assert connection.execute.call_count == 1


def test_connection_acquisition_failure_is_fixed():
    store, session, connection, _ = harness()
    error = RuntimeError("SECRET")
    session.connection.side_effect = error
    with pytest.raises(BootstrapSeedAdmissionError) as caught:
        with store.initial_admission(seed().key):
            pytest.fail("body")
    assert str(caught.value) == "BOOTSTRAP_SEED_ADMISSION"
    assert caught.value.__cause__ is error
    connection.execute.assert_not_called()


@pytest.mark.parametrize("acquired", [None, 1, "true"])
def test_nonboolean_lock_response_invalidates(acquired):
    store, _, connection, _ = harness(acquired=acquired)
    with pytest.raises(BootstrapSeedAdmissionError):
        with store.initial_admission(seed().key):
            pytest.fail("body")
    connection.invalidate.assert_called_once_with()
    assert connection.execute.call_count == 1
