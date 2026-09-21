"""Migration URL selection and pre-engine isolation checks; never connect to PG."""

import ast
import os
import runpy
from pathlib import Path

import pytest
import sqlalchemy
from alembic import context
from alembic.config import Config

from src.core.database_url import build_postgres_url, select_migration_url


SETTINGS = {
    "POSTGRES_USER": "env_user",
    "POSTGRES_PASSWORD": "env_secret",
    "POSTGRES_HOST": "env.invalid",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "production",
}
EXPLICIT = "postgresql://isolated:private_secret@isolated.invalid:5432/test_job"
PLACEHOLDER = "driver://user:pass@localhost/dbname"
ENV = Path(__file__).resolve().parents[2] / "database" / "alembic" / "env.py"


@pytest.mark.parametrize("settings", [{}, SETTINGS, {"POSTGRES_USER": "partial"}])
def test_explicit_url_wins_over_missing_partial_or_conflicting_env(settings) -> None:
    url = select_migration_url(EXPLICIT, settings, "test_job")
    assert url.database == "test_job"
    assert url.host == "isolated.invalid"
    assert url.password == "private_secret"


@pytest.mark.parametrize("configured", [PLACEHOLDER, None, ""])
def test_placeholder_uses_complete_env_and_partial_settings_fail_closed(
    configured,
) -> None:
    assert select_migration_url(configured, SETTINGS) == build_postgres_url(SETTINGS)
    for absent in SETTINGS:
        with pytest.raises(ValueError, match="missing PostgreSQL settings"):
            select_migration_url(
                configured, {k: v for k, v in SETTINGS.items() if k != absent}
            )
    with pytest.raises(ValueError, match="missing PostgreSQL settings"):
        select_migration_url(configured, {})


@pytest.mark.parametrize("expected", ["different", "", 123])
def test_guard_error_is_credential_free(expected) -> None:
    with pytest.raises(ValueError) as caught:
        select_migration_url(EXPLICIT, SETTINGS, expected)
    assert str(caught.value) == "migration database isolation mismatch"
    assert caught.value.__cause__ is None


def test_invalid_explicit_url_never_falls_back_or_exposes_input() -> None:
    with pytest.raises(ValueError) as caught:
        select_migration_url("not-a-url-private_secret", SETTINGS)
    assert str(caught.value) == "invalid configured migration URL"


@pytest.mark.parametrize("expected", ["test_job", "different"])
@pytest.mark.parametrize("query", ["", "?dbname=other_db"])
def test_env_guard_runs_before_engine_and_conflicting_env_cannot_redirect(
    monkeypatch, expected, query
) -> None:
    config = Config()
    config.set_main_option("sqlalchemy.url", EXPLICIT + query)
    config.attributes["expected_isolated_database"] = expected
    monkeypatch.setattr(context, "config", config, raising=False)
    monkeypatch.setattr(context, "is_offline_mode", lambda: False)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *args: None)
    for key, value in SETTINGS.items():
        monkeypatch.setenv(key, value)
    engines = []

    class EngineBoundary(Exception):
        pass

    def reject_engine(url, **kwargs):
        engines.append(url)
        raise EngineBoundary

    monkeypatch.setattr(sqlalchemy, "create_engine", reject_engine)
    if expected == "test_job" and not query:
        with pytest.raises(EngineBoundary):
            runpy.run_path(str(ENV))
        assert len(engines) == 1
        assert engines[0] == select_migration_url(EXPLICIT, {})
    else:
        with pytest.raises(ValueError, match="^migration database isolation mismatch$"):
            runpy.run_path(str(ENV))
        assert engines == []


def test_dialect_query_can_override_path_but_isolation_guard_rejects_it() -> None:
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

    raw = EXPLICIT + "?dbname=other_db"
    url = sqlalchemy.engine.make_url(raw)
    assert url.database == "test_job"
    _, parameters = PGDialect_psycopg2().create_connect_args(url)
    assert parameters["dbname"] == "other_db"
    for query in ("dbname=other_db", "host=other_host", "sslmode=require"):
        with pytest.raises(ValueError, match="^migration database isolation mismatch$"):
            select_migration_url(EXPLICIT + "?" + query, SETTINGS, "test_job")


def test_real_lane_helpers_escape_credentials_and_config_interpolation(
    monkeypatch,
) -> None:
    password = "secret@:/%"
    for key, value in {**SETTINGS, "POSTGRES_PASSWORD": password}.items():
        monkeypatch.setenv(key, value)
    path = Path(__file__).with_name("test_migrations.py")
    tree = ast.parse(path.read_text())
    helpers: list[ast.stmt] = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"_admin_url", "_target_url", "_alembic_config"}
    ]
    lane = {
        "os": os,
        "build_postgres_url": build_postgres_url,
        "ALEMBIC_INI": str(ENV.parents[1] / "alembic.ini"),
    }
    exec(compile(ast.Module(body=helpers, type_ignores=[]), str(path), "exec"), lane)
    for function, database in [("_admin_url", "postgres"), ("_target_url", "test_job")]:
        raw = lane[function]() if function == "_admin_url" else lane[function](database)
        url = sqlalchemy.engine.make_url(raw)
        expected = (password, database, "env.invalid")
        assert (url.password, url.database, url.host) == expected
    config = lane["_alembic_config"]("test_job")
    url = select_migration_url(
        config.get_main_option("sqlalchemy.url"),
        SETTINGS,
        config.attributes["expected_isolated_database"],
    )
    expected = (password, "test_job", "env.invalid")
    assert (url.password, url.database, url.host) == expected
