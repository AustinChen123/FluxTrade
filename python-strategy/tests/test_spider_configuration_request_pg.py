"""Causal PostgreSQL tests for immutable Spider configuration request evidence."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from hashlib import sha256
from threading import Barrier, Event, get_ident
import os
from collections.abc import Iterator
import time

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.spider_management.configuration_request import (
    SpiderConfigurationReadbackStatus as ReadbackStatus,
    SpiderConfigurationRequest,
    SpiderConfigurationRequestConflict,
    SpiderConfigurationRequestError,
    SpiderConfigurationRequestKind,
    SpiderConfigurationRequestStale,
)
from src.core.spider_management.configuration_request_store import (
    SpiderConfigurationRequestStore,
    SpiderConfigurationWriteDisposition,
)
from test_migrations import (
    _alembic_config,
    _target_url,
    _upgrade,
    fresh_pg_db as _migration_fresh_pg_db,
)
from test_spider_configuration_request import configuration

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
def spider_configuration_pg(fresh_pg_db: str) -> Iterator[Engine]:
    _upgrade(fresh_pg_db)
    from sqlalchemy import create_engine

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


def _request(
    *,
    kind=SpiderConfigurationRequestKind.PROPOSAL,
    config=None,
    key="proposal-c2",
    proposal_request_id=None,
):
    return SpiderConfigurationRequest.from_fields(
        environment="live",
        execution_scope_id="account-main",
        strategy_id="spider-main",
        request_kind=kind,
        actor="operator",
        idempotency_key=key,
        expected_state_version=7,
        configuration=configuration() if config is None else config,
        proposal_request_id=proposal_request_id,
    )


def _store(engine: Engine) -> SpiderConfigurationRequestStore:
    return SpiderConfigurationRequestStore(sessionmaker(engine))


def _record(engine: Engine, store: SpiderConfigurationRequestStore, request: SpiderConfigurationRequest):
    with Session(engine) as session, session.begin():
        result = store.record_in_transaction(session, request)
        assert session.in_transaction()
    assert result.disposition is SpiderConfigurationWriteDisposition.RECORDED_IN_TRANSACTION
    assert result.record.result == "NOT_APPLIED"
    return result.record


def _count(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT count(*) FROM spider_configuration_request")
        ).scalar_one()


def test_c2_confirm_survives_c3_and_existing_effective_config_is_unchanged(
    spider_configuration_pg: Engine,
):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    _record(engine, store, proposal)
    confirmation = _request(
        kind=SpiderConfigurationRequestKind.CONFIRMATION,
        config=proposal.configuration.value,
        key="confirm-c2",
        proposal_request_id=proposal.request_id,
    )
    _record(engine, store, confirmation)

    c3 = configuration()
    c3["parameters"]["TotalLimit"] = "2"
    _record(engine, store, _request(config=c3, key="proposal-c3"))

    recovered = store.readback(confirmation)
    assert recovered.status is ReadbackStatus.CONFIRMED
    assert recovered.record is not None
    assert recovered.record.request.configuration.sha256 == proposal.configuration.sha256
    assert recovered.record.result == "NOT_APPLIED"
    with engine.connect() as connection:
        state = connection.execute(
            text("SELECT status, config_json, version FROM strategy_state WHERE strategy_id='spider-main'")
        ).one()
    assert state == ("ACTIVE", '{"effective":"C1"}', 7)
    assert _count(engine) == 3


def test_exact_reentry_conflict_and_stale_version(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    first = _record(engine, store, proposal)
    second = _record(engine, store, proposal)
    assert second == first
    changed = configuration()
    changed["parameters"]["TotalLimit"] = "2"
    with Session(engine) as session, session.begin(), pytest.raises(
        SpiderConfigurationRequestConflict
    ):
        store.record_in_transaction(session, _request(config=changed))
    stale = SpiderConfigurationRequest.from_fields(
        environment="live",
        execution_scope_id="account-main",
        strategy_id="spider-main",
        request_kind=SpiderConfigurationRequestKind.PROPOSAL,
        actor="operator",
        idempotency_key="new-stale",
        expected_state_version=6,
        configuration=configuration(),
    )
    with Session(engine) as session, session.begin(), pytest.raises(
        SpiderConfigurationRequestStale
    ):
        store.record_in_transaction(session, stale)
    assert _count(engine) == 1


def test_confirm_requires_same_scope_and_exact_proposal_payload(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    _record(engine, store, proposal)
    changed = configuration()
    changed["parameters"]["TotalLimit"] = "2"
    confirmation = _request(
        kind=SpiderConfigurationRequestKind.CONFIRMATION,
        config=changed,
        key="confirm-c2",
        proposal_request_id=proposal.request_id,
    )
    with Session(engine) as session, session.begin(), pytest.raises(ValueError):
        store.record_in_transaction(session, confirmation)
    wrong_scope = SpiderConfigurationRequest.from_fields(
        environment="live",
        execution_scope_id="account-other",
        strategy_id="spider-main",
        request_kind=SpiderConfigurationRequestKind.CONFIRMATION,
        actor="operator",
        idempotency_key="confirm-other",
        expected_state_version=7,
        configuration=proposal.configuration.value,
        proposal_request_id=proposal.request_id,
    )
    with Session(engine) as session, session.begin(), pytest.raises(ValueError):
        store.record_in_transaction(session, wrong_scope)
    assert _count(engine) == 1


def test_confirm_rejects_unknown_proposal(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    missing = _request(key="missing-proposal")
    confirmation = _request(
        kind=SpiderConfigurationRequestKind.CONFIRMATION,
        config=missing.configuration.value,
        key="confirm-missing",
        proposal_request_id=missing.request_id,
    )
    with Session(engine) as session, session.begin(), pytest.raises(
        SpiderConfigurationRequestError
    ):
        store.record_in_transaction(session, confirmation)
    assert _count(engine) == 0


def test_rollback_and_commit_ack_loss_are_resolved_only_by_readback(
    spider_configuration_pg: Engine,
):
    engine = spider_configuration_pg
    store = _store(engine)
    rolled_back = _request(key="rollback")
    with pytest.raises(RuntimeError, match="rollback"):
        with Session(engine) as session:
            with session.begin():
                store.record_in_transaction(session, rolled_back)
                assert store.readback(rolled_back).status is ReadbackStatus.NOT_FOUND
                raise RuntimeError("rollback")
    assert store.readback(rolled_back).status is ReadbackStatus.NOT_FOUND

    ack_lost = _request(key="ack-lost")
    try:
        _record(engine, store, ack_lost)
        raise RuntimeError("simulated commit acknowledgement loss")
    except RuntimeError as error:
        assert str(error) == "simulated commit acknowledgement loss"
    result = store.readback(ack_lost)
    assert result.status is ReadbackStatus.CONFIRMED


def test_exact_reentry_precedes_new_version_check(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    first = _record(engine, store, proposal)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE strategy_state SET version=8 WHERE strategy_id='spider-main'")
        )
    second = _record(engine, store, proposal)
    assert second == first


def test_new_reader_recovers_confirmed_c2_after_c3(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    writer_store = _store(engine)
    proposal = _request()
    _record(engine, writer_store, proposal)
    confirmation = _request(
        kind=SpiderConfigurationRequestKind.CONFIRMATION,
        config=proposal.configuration.value,
        key="confirm-c2",
        proposal_request_id=proposal.request_id,
    )
    _record(engine, writer_store, confirmation)
    c3 = configuration()
    c3["parameters"]["TotalLimit"] = "2"
    _record(engine, writer_store, _request(config=c3, key="proposal-c3"))

    restarted_reader = _store(engine)
    recovered = restarted_reader.readback(confirmation)
    assert recovered.status is ReadbackStatus.CONFIRMED
    assert recovered.record is not None
    assert recovered.record.request.configuration.sha256 == proposal.configuration.sha256


def test_readback_rejects_session_bound_to_writer_connection_transaction(
    spider_configuration_pg: Engine,
):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    with pytest.raises(RuntimeError, match="caller rollback"):
        with Session(engine) as writer:
            with writer.begin():
                store.record_in_transaction(writer, proposal)
                writer_connection = writer.connection()

                @contextmanager
                def bound_reader():
                    with Session(bind=writer_connection) as reader:
                        yield reader

                readback = SpiderConfigurationRequestStore(bound_reader).readback(proposal)
                assert readback.status is ReadbackStatus.UNKNOWN
                assert writer_connection.in_transaction()
                assert writer.execute(
                    text(
                        "SELECT count(*) FROM spider_configuration_request "
                        "WHERE request_id=:request_id"
                    ),
                    {"request_id": proposal.request_id},
                ).scalar_one() == 1
                raise RuntimeError("caller rollback")
    assert store.readback(proposal).status is ReadbackStatus.NOT_FOUND


def test_store_paths_use_transaction_local_utc(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    with Session(engine) as writer, writer.begin():
        writer.execute(text("SET LOCAL TIME ZONE 'Pacific/Honolulu'"))
        write_result = store.record_in_transaction(writer, proposal)
        assert write_result.disposition is SpiderConfigurationWriteDisposition.RECORDED_IN_TRANSACTION
        assert writer.execute(text("SHOW TIME ZONE")).scalar_one() == "UTC"

    with engine.connect() as connection:
        connection.execute(text("SET TIME ZONE 'Pacific/Honolulu'"))
        connection.commit()
        with Session(bind=connection) as reader:
            result = SpiderConfigurationRequestStore(lambda: nullcontext(reader)).readback(
                proposal
            )
            assert result.status is ReadbackStatus.CONFIRMED
            assert result.record is not None
            offset = result.record.recorded_at.utcoffset()
            assert offset is not None and offset.total_seconds() == 0


def test_readback_lock_timeout_is_unknown(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    _record(engine, store, proposal)
    with Session(engine) as blocker, blocker.begin():
        blocker.execute(text("LOCK TABLE spider_configuration_request IN ACCESS EXCLUSIVE MODE"))
        result = store.readback(proposal)
        assert result.status is ReadbackStatus.UNKNOWN


def test_readback_failure_is_unknown_not_confirmed(spider_configuration_pg: Engine):
    request = _request()

    @contextmanager
    def unavailable():
        raise OSError("readback unavailable")
        yield  # pragma: no cover

    store = SpiderConfigurationRequestStore(unavailable)
    assert store.readback(request).status is ReadbackStatus.UNKNOWN


def test_same_id_concurrent_writers_serialize_and_reenter(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    expected = _request()
    barrier = Barrier(2)

    def write():
        barrier.wait(timeout=5)
        return _record(engine, store, expected)

    with ThreadPoolExecutor(max_workers=2) as pool:
        left, right = pool.submit(write), pool.submit(write)
        assert left.result(timeout=15) == right.result(timeout=15)
    assert _count(engine) == 1


def test_different_ids_same_scope_and_version_are_separate_proposals(
    spider_configuration_pg: Engine,
):
    engine = spider_configuration_pg
    store = _store(engine)
    first = _request(key="c2-a")
    second = _request(key="c2-b")
    _record(engine, store, first)
    _record(engine, store, second)
    assert _count(engine) == 2


def test_different_id_writer_waits_for_held_strategy_state_lock(
    spider_configuration_pg: Engine,
):
    engine = spider_configuration_pg
    store = _store(engine)
    held, release, second_started, lock_requested = Event(), Event(), Event(), Event()
    second_thread: set[int] = set()
    first = _request(key="held-first")
    second = _request(key="held-second")

    def capture_lock_attempt(conn, cursor, statement, parameters, context, executemany):
        if get_ident() in second_thread and "SELECT strategy_state.version" in statement:
            lock_requested.set()

    def hold_state_row():
        with Session(engine) as session, session.begin():
            session.execute(
                text(
                    "SELECT version FROM strategy_state "
                    "WHERE strategy_id='spider-main' FOR UPDATE"
                )
            )
            held.set()
            if not release.wait(timeout=10):
                raise TimeoutError("state lock release was not signalled")
            return store.record_in_transaction(session, first)

    def compete_with_new_id():
        second_thread.add(get_ident())
        second_started.set()
        return _record(engine, store, second)

    event.listen(engine, "before_cursor_execute", capture_lock_attempt)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            holder = pool.submit(hold_state_row)
            assert held.wait(timeout=5)
            competitor = pool.submit(compete_with_new_id)
            assert second_started.wait(timeout=5)
            assert lock_requested.wait(timeout=5)
            time.sleep(0.1)
            assert not competitor.done()
            release.set()
            holder.result(timeout=10)
            competitor.result(timeout=10)
    finally:
        release.set()
        event.remove(engine, "before_cursor_execute", capture_lock_attempt)
    assert _count(engine) == 2


@pytest.mark.parametrize("corruption", ["scope", "digest", "payload"])
def test_corrupt_scope_hash_or_payload_is_invalid_evidence(
    spider_configuration_pg: Engine, corruption: str
):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    _record(engine, store, proposal)
    if corruption == "scope":
        statement = text(
            "UPDATE spider_configuration_request SET execution_scope_id='account-other' "
            "WHERE request_id=:request_id"
        )
        params = {"request_id": proposal.request_id}
    elif corruption == "digest":
        statement = text(
            "UPDATE spider_configuration_request SET configuration_sha256=:digest "
            "WHERE request_id=:request_id"
        )
        params = {"digest": "0" * 64, "request_id": proposal.request_id}
    else:
        invalid = b"\xff"
        statement = text(
            "UPDATE spider_configuration_request SET configuration_payload=:payload, "
            "configuration_sha256=:digest WHERE request_id=:request_id"
        )
        params = {
            "payload": invalid,
            "digest": sha256(invalid).hexdigest(),
            "request_id": proposal.request_id,
        }
    with engine.begin() as connection:
        connection.execute(statement, params)
    assert store.readback(proposal).status is ReadbackStatus.INVALID_EVIDENCE


def test_confirmation_readback_revalidates_linked_proposal(spider_configuration_pg: Engine):
    engine = spider_configuration_pg
    store = _store(engine)
    proposal = _request()
    _record(engine, store, proposal)
    confirmation = _request(
        kind=SpiderConfigurationRequestKind.CONFIRMATION,
        config=proposal.configuration.value,
        key="confirm-c2",
        proposal_request_id=proposal.request_id,
    )
    _record(engine, store, confirmation)
    changed = configuration()
    changed["parameters"]["TotalLimit"] = "2"
    replacement = SpiderConfigurationRequest.from_fields(
        environment=proposal.environment,
        execution_scope_id=proposal.execution_scope_id,
        strategy_id=proposal.strategy_id,
        request_kind=SpiderConfigurationRequestKind.PROPOSAL,
        actor=proposal.actor,
        idempotency_key=proposal.idempotency_key,
        expected_state_version=proposal.expected_state_version,
        configuration=changed,
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE spider_configuration_request SET configuration_payload=:payload, "
                "configuration_sha256=:digest WHERE request_id=:request_id"
            ),
            {
                "payload": replacement.configuration.canonical_bytes,
                "digest": replacement.configuration.sha256,
                "request_id": proposal.request_id,
            },
        )
    assert store.readback(confirmation).status is ReadbackStatus.INVALID_EVIDENCE


def test_spider_request_migration_round_trip(fresh_pg_db: str):
    from alembic import command
    from sqlalchemy import create_engine

    _upgrade(fresh_pg_db)
    cfg = _alembic_config(fresh_pg_db)
    command.downgrade(cfg, "f27c8d14a9b1")
    engine = create_engine(_target_url(fresh_pg_db))
    try:
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT to_regclass('spider_configuration_request')")
            ).scalar_one() is None
    finally:
        engine.dispose()
    command.upgrade(cfg, "head")
    engine = create_engine(_target_url(fresh_pg_db))
    try:
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT to_regclass('spider_configuration_request')")
            ).scalar_one() == "spider_configuration_request"
    finally:
        engine.dispose()
