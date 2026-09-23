"""Isolated PostgreSQL admission evidence, not runtime activation acceptance."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier
from typing import Any, cast
from unittest.mock import patch
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from src.core.market_data.profiles.repository import TransactionWaitPolicy
from src.core.strategy_activation_request_store import (
    ProfileActivationAdmissionResult,
    ProfileActivationAdmissionStatus as AdmissionStatus,
    ProfileActivationRequestRecord,
    ProfileActivationRequestStore as Store,
    ProfileActivationRequestConflict as Conflict,
    ProfileActivationRequestStale as Stale,
    ProfileActivationRequestStatus as Status,
)
from test_profile_activation_request import request

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def prepare(engine, *values):
    with engine.begin() as connection:
        for strategy_id in sorted({v.intent.key.strategy_id for v in values}):
            connection.execute(
                text(
                    "INSERT INTO strategy(id,name,configuration_json) VALUES (:id,:id,'{}')"
                ),
                {"id": strategy_id},
            )
            connection.execute(
                text(
                    "INSERT INTO strategy_state(strategy_id,status,version) VALUES (:id,'READY',0)"
                ),
                {"id": strategy_id},
            )


def count(engine):
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT count(*) FROM strategy_profile_activation_request")
        ).scalar_one()


@contextmanager
def insert_log(engine, rendezvous=None):
    inserts = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO strategy_profile_activation_request"):
            inserts.append(statement)
            if rendezvous is not None:
                rendezvous.wait(timeout=10)

    event.listen(engine, "before_cursor_execute", capture)
    try:
        yield inserts
    finally:
        event.remove(engine, "before_cursor_execute", capture)


@pytest.mark.parametrize("race", ["same", "slot", "pk"])
def test_concurrent_admission_real_locks_and_unique_arbitration(
    profile_repository_pg, race
):
    engine = profile_repository_pg
    first = request()
    second = (
        replace(first, idempotency_key="other")
        if race == "slot"
        else replace(
            first,
            intent=replace(
                first.intent, key=replace(first.intent.key, strategy_id="other")
            ),
        )
        if race == "pk"
        else first
    )
    prepare(engine, first, second)
    barrier, pids = Barrier(2), []

    @contextmanager
    def sessions():
        # Reserve separate physical backends before either admission transaction.
        with engine.connect() as connection:
            pids.append(
                connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            )
            connection.rollback()
            barrier.wait(timeout=10)
            with Session(bind=connection) as bound:
                yield bound

    store = Store(sessions)

    def admit(value):
        try:
            return store.admit(value, current=value.intent)
        except Conflict:
            return None

    with (
        insert_log(engine, Barrier(2) if race == "pk" else None) as inserts,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        results = [
            future.result(timeout=20)
            for future in [pool.submit(admit, v) for v in (first, second)]
        ]
    assert len(set(pids)) == 2 and count(engine) == 1
    if race == "same":
        assert results[0] == results[1] and results[0] is not None
        assert len(inserts) == 1
    else:
        assert sum(r is None for r in results) == 1
        assert len(inserts) == (2 if race == "pk" else 1)
    winner = next(r for r in results if r is not None)
    assert Store(sessionmaker(engine)).confirm(winner.request) == winner


@pytest.mark.parametrize("terminal", ["CONSUMED", "CANCELLED", "STALE"])
def test_terminal_idempotent_despite_committed_state_drift(
    profile_repository_pg, terminal
):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    store = Store(sessionmaker(engine))
    original = store.admit(value, current=value.intent)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE strategy_profile_activation_request SET status=:status, terminal_at=requested_at, terminal_reason='DONE'"
            ),
            {"status": terminal},
        )
        connection.execute(text("UPDATE strategy_state SET version=1"))
    with insert_log(engine) as inserts:
        result = store.admit(
            value, current=replace(value.intent, expected_state_version=1)
        )
    assert result.status is Status(terminal) and result.request == original.request
    assert (
        result.requested_at == original.requested_at
        and inserts == []
        and count(engine) == 1
    )


def test_state_lock_timeout_and_stale_never_insert(profile_repository_pg):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    store = Store(sessionmaker(engine), TransactionWaitPolicy(25, 250))
    with engine.connect() as holder, insert_log(engine) as inserts:
        transaction = holder.begin()
        holder.execute(
            text(
                "SELECT strategy_id FROM strategy_state WHERE strategy_id=:id FOR UPDATE"
            ),
            {"id": value.intent.key.strategy_id},
        )
        try:
            with pytest.raises(DBAPIError) as caught:
                store.admit(value, current=value.intent)
            assert cast(Any, caught.value.orig).pgcode == "55P03"
            assert count(engine) == 0 and inserts == []
        finally:
            transaction.rollback()
    with engine.begin() as connection:
        connection.execute(text("UPDATE strategy_state SET version=1"))
    with insert_log(engine) as inserts, pytest.raises(Stale):
        store.admit(value, current=value.intent)
    assert count(engine) == 0 and inserts == []


@pytest.mark.parametrize("phase", ["before_commit", "ack_lost"])
def test_commit_fault_identity_and_fresh_confirmation(profile_repository_pg, phase):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    error = RuntimeError("fixed fixture fault")

    class FaultSession(Session):
        pass

    def fail_before_commit(session):
        raise error

    if phase == "before_commit":
        event.listen(FaultSession, "before_commit", fail_before_commit)

    @contextmanager
    def sessions():
        with FaultSession(engine) as session:
            yield session
        if phase == "ack_lost":
            raise error

    try:
        with insert_log(engine) as inserts, pytest.raises(RuntimeError) as caught:
            Store(sessions).admit(value, current=value.intent)
        assert caught.value is error and len(inserts) == 1
    finally:
        if phase == "before_commit":
            event.remove(FaultSession, "before_commit", fail_before_commit)
    record = Store(sessionmaker(engine)).confirm(value)
    if phase == "before_commit":
        assert record is None and count(engine) == 0
    else:
        assert (
            record is not None
            and record.request.canonical_bytes == value.canonical_bytes
        )
        assert (
            record.request.payload_digest == value.payload_digest and count(engine) == 1
        )


@pytest.mark.parametrize(
    ("phase", "expected"),
    [
        ("before_commit", AdmissionStatus.FAILED),
        ("ack_lost", AdmissionStatus.CONFIRMED),
        ("readback_failed", AdmissionStatus.UNCONFIRMED),
    ],
)
def test_admit_confirmed_real_commit_faults(profile_repository_pg, phase, expected):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    admission_error = RuntimeError("SECRET admission fault")
    readback_error = RuntimeError("SECRET readback fault")
    opened, exited, statements = [], [], []

    class FaultSession(Session):
        pass

    def before_commit(session):
        if phase == "before_commit":
            raise admission_error

    def before_execute(state):
        statements.append(state.session)
        if state.session is opened[1] and phase == "readback_failed":
            raise readback_error

    @contextmanager
    def sessions():
        admission = not opened
        session = FaultSession(engine) if admission else Session(engine)
        opened.append(session)
        if not admission:
            assert exited == [opened[0]]
            event.listen(session, "do_orm_execute", before_execute)
        try:
            with session:
                yield session
        finally:
            exited.append(session)
        if admission:
            # The store's transaction has really committed before this ACK loss.
            raise admission_error

    event.listen(FaultSession, "before_commit", before_commit)
    store = Store(sessions)
    try:
        with (
            insert_log(engine) as inserts,
            patch.object(store, "admit", wraps=store.admit) as admit,
            patch.object(store, "confirm", wraps=store.confirm) as confirm,
        ):
            result = store.admit_confirmed(value, current=value.intent)
        admit.assert_called_once_with(value, current=value.intent)
        confirm.assert_called_once_with(value)
        assert len(inserts) == 1
    finally:
        event.remove(FaultSession, "before_commit", before_commit)
    assert len(opened) == 2 and opened[0] is not opened[1]
    assert exited == opened and statements and all(s is opened[1] for s in statements)
    assert type(result) is ProfileActivationAdmissionResult
    assert result.status is expected and "SECRET" not in repr(result)
    # Independent normal session establishes authoritative truth after faults.
    authoritative = Store(sessionmaker(engine)).confirm(value)
    if phase == "before_commit":
        assert authoritative is None and count(engine) == 0
    else:
        assert type(authoritative) is ProfileActivationRequestRecord
        assert authoritative.request.canonical_bytes == value.canonical_bytes
        assert authoritative.request.payload_digest == value.payload_digest
        assert authoritative.status is Status.PENDING and count(engine) == 1
    if expected is AdmissionStatus.CONFIRMED:
        assert type(result.record) is ProfileActivationRequestRecord
        assert result.record == authoritative
    else:
        assert result.record is None
