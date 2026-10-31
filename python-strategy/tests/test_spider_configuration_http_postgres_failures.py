"""Isolated PostgreSQL failure-boundary proof for the HTTP configuration store."""

from contextlib import contextmanager
import os

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.spider_configuration_http import (
    SPIDER_CONFIGURATION_REQUEST_PATH,
    SpiderConfigurationHttpService,
)
from test_spider_configuration_http import _api_headers, _app, _request
from test_migrations import _target_url, _upgrade

pytest_plugins = ["test_migrations"]


@pytest.fixture
def spider_http_failure_pg(fresh_pg_db: str):
    from sqlalchemy import create_engine

    _upgrade(fresh_pg_db)
    engine = create_engine(_target_url(fresh_pg_db))
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO strategy(id,name,configuration_json) VALUES ('spider-main','Spider','{}')")
        )
        connection.execute(
            text(
                "INSERT INTO strategy_state(strategy_id,status,config_json,version) "
                "VALUES ('spider-main','ACTIVE','{\"effective\":\"C1\"}',7)"
            )
        )
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
    reason="explicit isolated PostgreSQL opt-in required",
)
def test_http_commit_acknowledgement_loss_uses_one_independent_readback(
    spider_http_failure_pg: Engine,
):
    maker = sessionmaker(spider_http_failure_pg)
    session_count = 0

    @contextmanager
    def lose_first_close_ack():
        nonlocal session_count
        session_count += 1
        with maker() as session:
            yield session
        if session_count == 1:
            raise RuntimeError("simulated response lost after commit")

    service = SpiderConfigurationHttpService(
        lose_first_close_ack,
        environment="research",
        execution_scope_id="account-main",
    )
    app = _app(service)
    try:
        request = _request(key="commit-ack-lost")
        response = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            request.canonical_bytes, headers=_api_headers()
        )
        assert response.status_code == 200
        assert response.body["readback_status"] == "CONFIRMED"
        assert session_count == 2
        with maker() as session:
            count = session.execute(
                text("SELECT count(*) FROM spider_configuration_request")
            ).scalar_one()
        assert count == 1
    finally:
        app.shutdown(timeout=0)


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
    reason="explicit isolated PostgreSQL opt-in required",
)
def test_http_writer_rollback_is_independently_not_found(
    spider_http_failure_pg: Engine, monkeypatch
):
    sessions = sessionmaker(spider_http_failure_pg)
    service = SpiderConfigurationHttpService(
        sessions,
        environment="research",
        execution_scope_id="account-main",
    )
    write = service._store.record_in_transaction

    def rollback_after_insert(session, request):
        write(session, request)
        raise RuntimeError("simulate failure before commit")

    monkeypatch.setattr(service._store, "record_in_transaction", rollback_after_insert)
    app = _app(service)
    try:
        request = _request(key="rollback-before-commit")
        response = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            request.canonical_bytes, headers=_api_headers()
        )
        assert response.status_code == 404
        assert response.body["readback_status"] == "NOT_FOUND"
        with sessions() as session:
            count = session.execute(
                text("SELECT count(*) FROM spider_configuration_request")
            ).scalar_one()
        assert count == 0
    finally:
        app.shutdown(timeout=0)


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
    reason="explicit isolated PostgreSQL opt-in required",
)
def test_http_writer_failure_and_single_failed_readback_are_unknown(
    spider_http_failure_pg: Engine, monkeypatch
):
    maker = sessionmaker(spider_http_failure_pg)
    session_acquisitions = 0

    def sessions():
        nonlocal session_acquisitions
        session_acquisitions += 1
        if session_acquisitions == 2:
            raise RuntimeError("simulated independent reader acquisition failure")
        return maker()

    service = SpiderConfigurationHttpService(
        sessions,
        environment="research",
        execution_scope_id="account-main",
    )
    original_write = service._store.record_in_transaction
    original_readback = service._store.readback
    write_calls = 0
    readback_calls = 0

    def fail_after_real_insert(session, request):
        nonlocal write_calls
        write_calls += 1
        original_write(session, request)
        raise RuntimeError("simulated failure before transaction commit")

    def count_readback(request):
        nonlocal readback_calls
        readback_calls += 1
        return original_readback(request)

    monkeypatch.setattr(service._store, "record_in_transaction", fail_after_real_insert)
    monkeypatch.setattr(service._store, "readback", count_readback)
    app = _app(service)
    try:
        request = _request(key="failed-readback-unknown")
        response = app.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
            request.canonical_bytes, headers=_api_headers()
        )
        assert response.status_code == 503
        assert response.body["readback_status"] == "UNKNOWN"
        assert response.body["record"] is None
        assert session_acquisitions == 2
        assert write_calls == 1
        assert readback_calls == 1
        with maker() as session:
            count = session.execute(
                text("SELECT count(*) FROM spider_configuration_request")
            ).scalar_one()
        assert count == 0
    finally:
        app.shutdown(timeout=0)
