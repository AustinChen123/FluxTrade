"""Causal PostgreSQL tests for operation requests bound to exact accepted C2."""

from contextlib import contextmanager
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
import os
from threading import Event
from typing import cast

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.spider_operation_http import (
    SPIDER_OPERATION_READBACK_PATH,
    SPIDER_OPERATION_REQUEST_PATH,
    SpiderOperationHttpService,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequestKind as ConfigurationKind,
)
from src.core.spider_management.configuration_request_store import (
    SpiderConfigurationRequestStore,
)
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.operation_request_store import (
    SpiderOperationReadbackStatus,
    SpiderOperationRequestStore,
)
from test_migrations import _target_url, _upgrade, fresh_pg_db as _migration_fresh_pg_db
from test_spider_configuration_request import configuration
from test_spider_configuration_request_pg import _request as _configuration_request
from test_spider_operation_request import _body

fresh_pg_db = _migration_fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


@pytest.fixture
def operation_pg(fresh_pg_db: str) -> Iterator[Engine]:
    _upgrade(fresh_pg_db)
    engine = create_engine(_target_url(fresh_pg_db))
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO strategy(id,name,configuration_json) VALUES ('spider-main','Spider','{}')")
        )
        connection.execute(
            text(
                "INSERT INTO strategy_state(strategy_id,status,config_json,version) "
                "VALUES ('spider-main','ACTIVE','{}',7)"
            )
        )
    try:
        yield engine
    finally:
        engine.dispose()


def _confirmed_c2(engine: Engine):
    store = SpiderConfigurationRequestStore(sessionmaker(engine))
    proposal = _configuration_request(
        config=configuration(),
    )
    with Session(engine) as session, session.begin():
        store.record_in_transaction(session, proposal)
    confirmation = _configuration_request(
        kind=ConfigurationKind.CONFIRMATION,
        config=proposal.configuration.value,
        key="confirm-c2",
        proposal_request_id=proposal.request_id,
    )
    with Session(engine) as session, session.begin():
        store.record_in_transaction(session, confirmation)
    c3 = configuration()
    c3["parameters"]["TotalLimit"] = "2"
    with Session(engine) as session, session.begin():
        store.record_in_transaction(session, _configuration_request(config=c3, key="proposal-c3"))
    return store, confirmation


def _operation_request(c2, kind="PROPOSAL", proposal_request_id=None, *, hash_override=None):
    body = _body(kind, proposal_request_id)
    body["environment"] = "live"
    body["execution_scope_id"] = "account-main"
    body["strategy_id"] = "spider-main"
    core = body["operation_core"]
    core["account_id"] = "account-main"
    core["confirmation_request_id"] = c2.request_id
    core["configuration_sha256"] = hash_override or c2.configuration.sha256
    selection = core["runtime_selection"]
    selection["environment"] = "live"
    selection["execution_scope_id"] = "account-main"
    selection["strategy_id"] = "spider-main"
    selection["account_id"] = "account-main"
    selection["confirmation_request_id"] = c2.request_id
    selection["configuration_sha256"] = hash_override or c2.configuration.sha256
    return SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))


def _http_operation_request(c2, kind="PROPOSAL", proposal_request_id=None, *, key=None):
    body = _body(kind, proposal_request_id)
    body["actor"] = "api_key"
    body["idempotency_key"] = key or f"http-{kind.lower()}"
    body["environment"] = "live"
    body["execution_scope_id"] = "account-main"
    body["strategy_id"] = "spider-main"
    core = body["operation_core"]
    core["account_id"] = "account-main"
    core["confirmation_request_id"] = c2.request_id
    core["configuration_sha256"] = c2.configuration.sha256
    selection = core["runtime_selection"]
    selection.update({
        "environment": "live",
        "execution_scope_id": "account-main",
        "strategy_id": "spider-main",
        "account_id": "account-main",
        "confirmation_request_id": c2.request_id,
        "configuration_sha256": c2.configuration.sha256,
    })
    return SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))


def _operation_http_app(sessions):
    service = SpiderOperationHttpService(
        sessions, environment="live", execution_scope_id="account-main"
    )
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="operation-http-pg-test-key",
        spider_operation_http_service=service,
    )
    headers = {"Authorization": "Bearer operation-http-pg-test-key"}
    return app, service, headers


def test_operation_confirmation_uses_exact_persisted_c2_after_later_c3(operation_pg: Engine):
    engine = operation_pg
    _configuration_store, c2 = _confirmed_c2(engine)
    store = SpiderOperationRequestStore(sessionmaker(engine))
    proposal = _operation_request(c2)
    with Session(engine) as session, session.begin():
        store.record_in_transaction(session, proposal)
    confirmation = _operation_request(c2, "CONFIRMATION", proposal.request_id)
    with Session(engine) as session, session.begin():
        store.record_in_transaction(session, confirmation)

    recovered = store.readback(confirmation)
    assert recovered.status is SpiderOperationReadbackStatus.CONFIRMED
    assert recovered.record is not None
    assert recovered.record.request.canonical_request_bytes == confirmation.canonical_request_bytes
    assert recovered.record.authorization_confirmed is True
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT phase FROM spider_runtime_operation WHERE operation_id=:id"),
            {"id": proposal.operation_id},
        ).scalar_one() == "PREVIEW"
        assert connection.execute(
            text("SELECT count(*) FROM spider_operation_request WHERE operation_id=:id"),
            {"id": proposal.operation_id},
        ).scalar_one() == 2


def test_operation_http_to_postgres_proposal_confirmation_retry_conflict_and_readback(
    operation_pg: Engine,
):
    _configuration_store, c2 = _confirmed_c2(operation_pg)
    app, _service, headers = _operation_http_app(sessionmaker(operation_pg))
    proposal = _http_operation_request(c2)
    proposal_response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        proposal.canonical_request_bytes, headers=headers,
    )
    assert proposal_response.status_code == 200
    assert proposal_response.body["record"]["authorization_confirmed"] is False

    exact_retry = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        proposal.canonical_request_bytes, headers=headers,
    )
    assert exact_retry.status_code == 200
    confirmation = _http_operation_request(
        c2, "CONFIRMATION", proposal.request_id
    )
    confirmed = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        confirmation.canonical_request_bytes, headers=headers,
    )
    assert confirmed.status_code == 200
    assert confirmed.body["record"]["authorization_confirmed"] is True

    changed_body = cast(
        dict[str, object], decode_canonical(proposal.canonical_request_bytes)
    )
    core = cast(dict[str, object], changed_body["operation_core"])
    core["expected_state_version"] = 1
    changed = SpiderOperationRequest.from_canonical_bytes(canonical_bytes(changed_body))
    conflict = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        changed.canonical_request_bytes, headers=headers,
    )
    assert conflict.status_code == 409

    readback = app.handle(
        "POST", SPIDER_OPERATION_READBACK_PATH,
        confirmation.canonical_request_bytes, headers=headers,
    )
    assert readback.status_code == 200
    assert readback.body["readback_status"] == "CONFIRMED"
    assert readback.body["runtime"] is None
    with operation_pg.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM spider_operation_request")
        ).scalar_one() == 2


def test_operation_http_commit_response_uncertainty_uses_one_write_and_one_readback(
    operation_pg: Engine,
):
    _configuration_store, c2 = _confirmed_c2(operation_pg)
    request = _http_operation_request(c2)
    writes = 0
    readbacks = 0
    session_calls = 0
    lifecycle = []

    @contextmanager
    def uncertain_sessions():
        nonlocal session_calls
        session_calls += 1
        is_writer = session_calls == 1
        with Session(operation_pg) as session:
            yield session
        if is_writer:
            lifecycle.append("writer_closed")
            raise RuntimeError("response lost after commit")

    app, service, headers = _operation_http_app(uncertain_sessions)
    original_write = service._store.record_in_transaction
    original_readback = service._store.readback

    def write_once(session: Session, request: SpiderOperationRequest):
        nonlocal writes
        writes += 1
        return original_write(session, request)

    def prove_once(request: SpiderOperationRequest):
        nonlocal readbacks
        readbacks += 1
        assert lifecycle == ["writer_closed"]
        lifecycle.append("readback")
        return original_readback(request)

    service._store.record_in_transaction = write_once
    service._store.readback = prove_once
    response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        request.canonical_request_bytes, headers=headers,
    )
    assert response.status_code == 200
    assert response.body["readback_status"] == "CONFIRMED"
    assert writes == 1
    assert readbacks == 1
    assert lifecycle == ["writer_closed", "readback"]
    with operation_pg.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM spider_operation_request")
        ).scalar_one() == 1


def test_revision_two_is_rejected_before_any_operation_writes(operation_pg: Engine):
    _configuration_store, c2 = _confirmed_c2(operation_pg)
    body = _body()
    body["environment"] = "live"
    body["execution_scope_id"] = "account-main"
    body["strategy_id"] = "spider-main"
    body["operation_core"]["account_id"] = "account-main"
    selection = body["operation_core"]["runtime_selection"]
    selection.update({
        "environment": "live",
        "execution_scope_id": "account-main",
        "strategy_id": "spider-main",
        "account_id": "account-main",
        "confirmation_request_id": c2.request_id,
        "configuration_sha256": c2.configuration.sha256,
    })
    body["operation_core"]["confirmation_request_id"] = c2.request_id
    body["operation_core"]["configuration_sha256"] = c2.configuration.sha256
    body["authorization"]["revision"] = 2
    with pytest.raises(ValueError):
        SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))
    with operation_pg.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM spider_runtime_operation")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM spider_operation_request")).scalar_one() == 0


def test_operation_writer_configures_utc_and_bounded_timeouts_before_write(operation_pg: Engine):
    _configuration_store, c2 = _confirmed_c2(operation_pg)
    store = SpiderOperationRequestStore(
        sessionmaker(operation_pg), lock_timeout_ms=321, statement_timeout_ms=1234
    )
    request = _operation_request(c2)
    with Session(operation_pg) as session, session.begin():
        store.record_in_transaction(session, request)
        settings = session.execute(text(
            "SELECT current_setting('TimeZone'), current_setting('lock_timeout'), "
            "current_setting('statement_timeout')"
        )).one()
        assert settings == ("UTC", "321ms", "1234ms")


def test_exact_confirmation_retry_after_head_lock_returns_committed_record(operation_pg: Engine):
    _configuration_store, c2 = _confirmed_c2(operation_pg)
    store = SpiderOperationRequestStore(sessionmaker(operation_pg))
    proposal = _operation_request(c2)
    with Session(operation_pg) as session, session.begin():
        store.record_in_transaction(session, proposal)
    confirmation = _operation_request(c2, "CONFIRMATION", proposal.request_id)
    inserted = Event()
    allow_commit = Event()
    duplicate_checked = Event()
    allow_duplicate = Event()

    def first_insert() -> None:
        with Session(operation_pg) as session, session.begin():
            store.record_in_transaction(session, confirmation)
            inserted.set()
            assert allow_commit.wait(5)

    class CoordinatedRetryStore(SpiderOperationRequestStore):
        @staticmethod
        def _record_existing(session, request):
            record = SpiderOperationRequestStore._record_existing(session, request)
            if request is confirmation and record is None:
                duplicate_checked.set()
                assert allow_duplicate.wait(5)
            return record

    def exact_retry():
        with Session(operation_pg) as session, session.begin():
            return CoordinatedRetryStore(sessionmaker(operation_pg)).record_in_transaction(
                session, confirmation
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(first_insert)
        assert inserted.wait(5)
        retry = pool.submit(exact_retry)
        assert duplicate_checked.wait(5)
        allow_commit.set()
        first.result(timeout=5)
        allow_duplicate.set()
        recovered = retry.result(timeout=5)
    assert recovered.request.canonical_request_bytes == confirmation.canonical_request_bytes
    assert recovered.authorization_confirmed is True
    with operation_pg.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM spider_operation_request")).scalar_one() == 2


def test_operation_head_lock_wait_is_bounded_and_writes_nothing(operation_pg: Engine):
    _configuration_store, c2 = _confirmed_c2(operation_pg)
    store = SpiderOperationRequestStore(
        sessionmaker(operation_pg), lock_timeout_ms=100, statement_timeout_ms=1000
    )
    proposal = _operation_request(c2)
    with Session(operation_pg) as session, session.begin():
        store.record_in_transaction(session, proposal)
    confirmation = _operation_request(c2, "CONFIRMATION", proposal.request_id)

    with Session(operation_pg) as lock_holder, lock_holder.begin():
        lock_holder.execute(
            text(
                "SELECT operation_id FROM spider_runtime_operation "
                "WHERE operation_id=:operation_id FOR UPDATE"
            ),
            {"operation_id": confirmation.operation_id},
        ).one()
        with pytest.raises(DBAPIError):
            with Session(operation_pg) as blocked, blocked.begin():
                store.record_in_transaction(blocked, confirmation)

    with operation_pg.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM spider_operation_request")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT authorization_revision FROM spider_runtime_operation")
        ).scalar_one() is None


def test_operation_request_rejects_configuration_proposal_as_c2(operation_pg: Engine):
    engine = operation_pg
    store = SpiderConfigurationRequestStore(sessionmaker(engine))
    proposal_c2 = _configuration_request(config=configuration())
    with Session(engine) as session, session.begin():
        store.record_in_transaction(session, proposal_c2)
    operation_store = SpiderOperationRequestStore(sessionmaker(engine))
    operation = _operation_request(proposal_c2)
    with Session(engine) as session, session.begin(), pytest.raises(ValueError):
        operation_store.record_in_transaction(session, operation)


def test_operation_request_rejects_wrong_c2_hash_without_writes(operation_pg: Engine):
    engine = operation_pg
    _configuration_store, c2 = _confirmed_c2(engine)
    store = SpiderOperationRequestStore(sessionmaker(engine))
    request = _operation_request(c2, hash_override="d" * 64)
    with Session(engine) as session, session.begin(), pytest.raises(ValueError):
        store.record_in_transaction(session, request)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM spider_runtime_operation")).scalar_one() == 0
        assert connection.execute(text("SELECT count(*) FROM spider_operation_request")).scalar_one() == 0


def test_operation_request_rollback_is_absent_on_independent_readback(operation_pg: Engine):
    engine = operation_pg
    _configuration_store, c2 = _confirmed_c2(engine)
    store = SpiderOperationRequestStore(sessionmaker(engine))
    proposal = _operation_request(c2)
    with pytest.raises(RuntimeError, match="^caller rollback$"):
        with Session(engine) as session:
            with session.begin():
                store.record_in_transaction(session, proposal)
                raise RuntimeError("caller rollback")
    assert store.readback(proposal).status is SpiderOperationReadbackStatus.NOT_FOUND


def test_operation_readback_is_independent_from_writer_connection(operation_pg: Engine):
    engine = operation_pg
    _configuration_store, c2 = _confirmed_c2(engine)
    store = SpiderOperationRequestStore(sessionmaker(engine))
    proposal = _operation_request(c2)
    with pytest.raises(RuntimeError, match="^caller rollback$"):
        with Session(engine) as writer:
            with writer.begin():
                store.record_in_transaction(writer, proposal)
                writer_connection = writer.connection()

                @contextmanager
                def bound_reader():
                    with Session(bind=writer_connection) as reader:
                        yield reader

                result = SpiderOperationRequestStore(bound_reader).readback(proposal)
                assert result.status is SpiderOperationReadbackStatus.UNKNOWN
                assert writer_connection.in_transaction()
                raise RuntimeError("caller rollback")
    assert store.readback(proposal).status is SpiderOperationReadbackStatus.NOT_FOUND
