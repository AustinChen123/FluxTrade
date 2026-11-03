from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from time import monotonic

import pytest
from sqlalchemy.exc import OperationalError

from src.core import db
from src.core.database_url import build_postgres_url


POSTGRES_SETTINGS = {
    "POSTGRES_USER": "fluxtrade",
    "POSTGRES_PASSWORD": "p@ss:word/with%chars",
    "POSTGRES_HOST": "db",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "fluxtrade",
}


def test_postgres_url_preserves_reserved_credential_characters() -> None:
    url = build_postgres_url(POSTGRES_SETTINGS)

    assert url.username == POSTGRES_SETTINGS["POSTGRES_USER"]
    assert url.password == POSTGRES_SETTINGS["POSTGRES_PASSWORD"]
    assert url.host == POSTGRES_SETTINGS["POSTGRES_HOST"]
    assert url.port == 5432
    assert url.database == POSTGRES_SETTINGS["POSTGRES_DB"]


@pytest.mark.parametrize("missing_name", sorted(POSTGRES_SETTINGS))
def test_postgres_url_rejects_each_missing_setting(missing_name: str) -> None:
    settings = {**POSTGRES_SETTINGS, missing_name: ""}

    with pytest.raises(ValueError, match=missing_name):
        build_postgres_url(settings)


@pytest.mark.parametrize("port", ["not-a-port", "0", "65536"])
def test_postgres_url_rejects_invalid_port(port: str) -> None:
    with pytest.raises(ValueError, match="POSTGRES_PORT"):
        build_postgres_url({**POSTGRES_SETTINGS, "POSTGRES_PORT": port})


def test_session_factory_cold_start_initializes_engine_without_deadlock(
    monkeypatch,
) -> None:
    created_engine = object()
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_session_factory", None)
    monkeypatch.setattr(db, "_build_database_url", lambda: "sqlite://")
    monkeypatch.setattr(db, "create_engine", lambda *_args, **_kwargs: created_engine)

    with ThreadPoolExecutor(max_workers=4) as executor:
        factories = list(executor.map(lambda _index: db.get_session_factory(), range(8)))

    assert len({id(factory) for factory in factories}) == 1
    assert factories[0].kw["bind"] is created_engine


def test_production_engine_bounds_pool_wait_and_connect(monkeypatch) -> None:
    created: dict[str, object] = {}
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_build_database_url", lambda: "postgresql://local/db")

    def capture_engine(url: object, **kwargs: object) -> object:
        created.update(kwargs)
        engine = object()
        monkeypatch.setattr(db, "_engine", engine)
        return engine

    monkeypatch.setattr(db, "create_engine", capture_engine)
    db.get_engine()

    assert created["pool_timeout"] == 3
    assert created["connect_args"] == {"connect_timeout": 3}


def test_production_engine_loopback_connect_failure_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(
        db,
        "_build_database_url",
        lambda: "postgresql://fluxtrade:fluxtrade@127.0.0.1:1/postgres",
    )
    started = monotonic()
    with pytest.raises(OperationalError):
        db.get_engine().connect()
    assert monotonic() - started < 4
