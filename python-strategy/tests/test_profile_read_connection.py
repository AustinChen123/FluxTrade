"""Connection lifecycle contracts with fake engines/sessions; no database I/O."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from threading import Barrier, Event
from typing import Any
from unittest.mock import MagicMock
import subprocess
import sys

import pytest
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from src.core.market_data.profiles import read_connection as connection

Config = connection.ProfileReadConnectionConfig
Owner = connection.ProfileReadConnection
ConfigError = connection.ProfileReadConnectionConfigError
LifecycleError = connection.ProfileReadConnectionLifecycleError


def url() -> URL:
    return URL.create("postgresql", username="reader", password="SECRET@:/%", host="localhost", port=5432, database="profiles")


def harness(monkeypatch: pytest.MonkeyPatch):
    engine = MagicMock()
    create = MagicMock(return_value=engine)
    sessions: list[Any] = []

    def new_session(**_kwargs: Any):
        session = MagicMock()
        session.__enter__.return_value = session
        session.__exit__.side_effect = lambda *_: session.close()
        session.close.return_value = None
        sessions.append(session)
        return session

    constructor = MagicMock(side_effect=new_session)
    monkeypatch.setattr(connection, "create_engine", create)
    monkeypatch.setattr(connection, "Session", constructor)
    return Owner(Config(url())), engine, create, constructor, sessions


def test_url_normalization_exact_credentials_and_repr() -> None:
    config = Config(url())
    assert config.url == url().set(drivername="postgresql+psycopg2")
    assert config.url.password == "SECRET@:/%"
    assert repr(config) == "ProfileReadConnectionConfig()"
    assert Config(config.url) == config
    assert Config(url().set(host="::1")).url.host == "::1"
    with pytest.raises(FrozenInstanceError):
        config.url = url()  # type: ignore[misc]
    with pytest.raises(ConfigError):
        replace(config, url=url().set(drivername="mysql"))


@pytest.mark.parametrize("after_owner", [False, True])
def test_caller_owned_empty_query_is_detached(after_owner: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    query: dict[str, str] = {}
    original = url()._replace(query=query)
    config = Config(original)
    engine = MagicMock()
    create = MagicMock(return_value=engine)
    monkeypatch.setattr(connection, "create_engine", create)
    owner = Owner(config) if after_owner else None
    query.update(host="other-host", dbname="other-database", options="SECRET")
    assert not config.url.query and config.url.query is not query
    args, kwargs = PGDialect_psycopg2().create_connect_args(config.url)
    assert args == [] and kwargs == dict(host="localhost", dbname="profiles", user="reader", password="SECRET@:/%", port=5432)
    if owner is None:
        owner = Owner(config)
    with owner.sessions():
        pass
    assert create.call_args.args[0] == config.url
    assert PGDialect_psycopg2().create_connect_args(create.call_args.args[0]) == (args, kwargs)
    owner.close()


class Text(str):
    pass


@pytest.mark.parametrize("field,value", [
    ("drivername", "sqlite"), ("drivername", "postgresql+asyncpg"), ("drivername", Text("postgresql")),
    ("username", None), ("username", ""), ("username", Text("reader")),
    ("password", None), ("password", ""), ("password", object()), ("password", Text("secret")),
    ("host", None), ("host", ""), ("host", "a,b"), ("host", "/tmp/socket"), ("host", "@socket"),
    ("host", "a b"), ("host", "a\nb"), ("host", "a\\b"),
    ("port", None), ("port", True), ("port", "5432"), ("port", 0), ("port", 65536),
    ("database", None), ("database", ""), ("database", "SECRET\x00"),
    ("query", {"sslmode": "require"}), ("query", {"host": "other"}),
])
def test_reject_invalid_exact_url_fields_without_input_in_error(field: str, value: Any) -> None:
    # _replace preserves deliberately malformed runtime types; URL.create coerces ports.
    invalid = url()._replace(**{field: value})
    with pytest.raises(ConfigError, match="^invalid profile read connection configuration$"):
        Config(invalid)


def test_reject_nonexact_url_and_config(monkeypatch: pytest.MonkeyPatch) -> None:
    create = MagicMock()
    monkeypatch.setattr(connection, "create_engine", create)
    for value in ("postgresql://SECRET", None, MagicMock(spec=URL)):
        with pytest.raises(ConfigError):
            Config(value)  # type: ignore[arg-type]
    for value in (url(), None, MagicMock(spec=Config)):
        with pytest.raises(ConfigError):
            Owner(value)  # type: ignore[arg-type]
    class DerivedURL(URL):
        pass

    class DerivedConfig(Config):
        pass

    with pytest.raises(ConfigError):
        Config(DerivedURL(*url()))
    with pytest.raises(ConfigError):
        Owner(DerivedConfig(url()))
    create.assert_not_called()


def test_lazy_exact_engine_arguments_fresh_sessions_and_close_only(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, constructor, sessions = harness(monkeypatch)
    pending = owner.sessions()
    create.assert_not_called()
    with pending as first:
        with owner.sessions() as second:
            assert first is not second
            with pytest.raises(LifecycleError, match="^profile read connection has active sessions$"):
                owner.close()
    create.assert_called_once_with(Config(url()).url, pool_size=2, max_overflow=0, pool_timeout=0.25,
                                   connect_args={"connect_timeout": 2}, pool_pre_ping=False, pool_recycle=-1)
    assert constructor.call_count == 2
    for call in constructor.call_args_list:
        assert call.kwargs == dict(bind=engine, autoflush=False)
    for session in sessions:
        session.close.assert_called_once()
        session.begin.assert_not_called()
        session.commit.assert_not_called()
        session.rollback.assert_not_called()
    owner.close()
    owner.close()
    engine.dispose.assert_called_once_with()
    with pytest.raises(LifecycleError, match="^profile read connection is closed$"):
        with owner.sessions():
            pytest.fail("closed")


def test_close_before_create_and_previously_obtained_context(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, _, _ = harness(monkeypatch)
    pending = owner.sessions()
    owner.close()
    owner.close()
    with pytest.raises(LifecycleError):
        with pending:
            pytest.fail("closed")
    create.assert_not_called()
    engine.dispose.assert_not_called()


def test_actual_sqlalchemy_session_context_does_not_autobegin(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, _, _, _ = harness(monkeypatch)
    monkeypatch.setattr(connection, "Session", Session)
    with owner.sessions() as first:
        assert type(first) is Session and not first.in_transaction() and not first.autoflush
    with owner.sessions() as second:
        assert second is not first and not second.in_transaction()
    engine.connect.assert_not_called()
    owner.close()


def test_concurrent_first_admission_creates_one_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, _, sessions = harness(monkeypatch)
    start, admitted = Barrier(4), Barrier(4)

    def worker():
        start.wait(timeout=3)
        with owner.sessions() as session:
            admitted.wait(timeout=3)
            return id(session)

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(worker) for _ in range(4)]
        assert len({future.result(timeout=5) for future in futures}) == 4
    assert len(sessions) == 4 and create.call_count == 1
    owner.close()
    engine.dispose.assert_called_once()


def test_enter_wins_close_race_and_user_body_does_not_hold_owner_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, _, _ = harness(monkeypatch)
    creating, release_create, entered, release_body, closing = (Event() for _ in range(5))

    def make_engine(*_args: Any, **_kwargs: Any):
        creating.set()
        assert release_create.wait(timeout=3)
        return engine

    create.side_effect = make_engine

    def enter():
        with owner.sessions():
            entered.set()
            assert release_body.wait(timeout=3)

    def close():
        closing.set()
        owner.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        entry = pool.submit(enter)
        try:
            assert creating.wait(timeout=3)
            closure = pool.submit(close)
            assert closing.wait(timeout=3)
            release_create.set()
            assert entered.wait(timeout=3)
            with pytest.raises(LifecycleError):
                closure.result(timeout=3)
            engine.dispose.assert_not_called()
        finally:
            release_create.set()
            release_body.set()
        entry.result(timeout=3)
    owner.close()
    engine.dispose.assert_called_once()


def test_engine_failure_is_not_retried_but_later_entry_can_try(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, constructor, _ = harness(monkeypatch)
    error = DBAPIError("opaque", None, Exception("opaque"))
    create.side_effect = [error, engine]
    with pytest.raises(DBAPIError) as caught:
        with owner.sessions():
            pytest.fail("creation failed")
    assert caught.value is error and create.call_count == 1
    constructor.assert_not_called()
    with owner.sessions():
        pass
    assert create.call_count == 2
    owner.close()
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("phase", ["construct", "enter", "body", "exit", "close"])
def test_session_failure_releases_active_and_preserves_exception(phase: str, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, constructor, _ = harness(monkeypatch)
    error = DBAPIError("opaque", None, Exception("opaque"))
    session = MagicMock()
    session.__enter__.return_value = session
    session.close.return_value = None
    session.__exit__.side_effect = lambda *_: session.close()
    constructor.side_effect = error if phase == "construct" else None
    constructor.return_value = session
    if phase in ("enter", "exit", "close"):
        method = {"enter": session.__enter__, "exit": session.__exit__, "close": session.close}[phase]
        method.side_effect = error
    with pytest.raises(DBAPIError) as caught:
        with owner.sessions():
            if phase == "body":
                raise error
    assert caught.value is error and create.call_count == constructor.call_count == 1
    if phase == "body":
        assert session.__exit__.call_args.args[1] is error
    owner.close()  # No active-count leak, including failed construction/entry.
    engine.dispose.assert_called_once()


def test_dispose_failure_is_terminal_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, engine, create, _, _ = harness(monkeypatch)
    with owner.sessions():
        pass
    error = RuntimeError("dispose")
    engine.dispose.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        owner.close()
    assert caught.value is error
    owner.close()
    with pytest.raises(LifecycleError):
        with owner.sessions():
            pytest.fail("closed")
    assert create.call_count == engine.dispose.call_count == 1


def test_isolated_import_does_not_read_environment_or_global_engine() -> None:
    code = """
import os, sys, sqlalchemy.orm
from unittest.mock import patch
class NoEnvironment(dict):
    def __getitem__(self, key): raise AssertionError('environment')
    def get(self, *args): raise AssertionError('environment')
    def __iter__(self): raise AssertionError('environment')
with patch('os.getenv', side_effect=AssertionError('environment')), patch('os.environ', NoEnvironment()):
    import src.core.market_data.profiles.read_connection
assert not any(name in sys.modules for name in ('src.core.db', 'src.core.database', 'src.main', 'dotenv'))
assert not any('rithmic' in name.lower() for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
