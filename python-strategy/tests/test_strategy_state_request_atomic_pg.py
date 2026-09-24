"""Caller transaction evidence only; not a runtime/idempotent orchestrator."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import MagicMock
import os

import pytest
from sqlalchemy import event, text
from sqlalchemy.orm import Session, sessionmaker

from src.core.models import StrategyStatus
from src.core.strategy_state_manager import (
    StrategyStateManager,
    StaleStrategyStateVersion,
)
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestStore as Store,
    ProfileActivationRequestStatus as Status,
    terminalize_profile_activation_request,
)
from test_profile_activation_request import request
from test_strategy_activation_request_store_pg import prepare

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def evidence(connection):
    state = connection.execute(text("SELECT status,version FROM strategy_state")).one()
    audit = connection.execute(
        text(
            "SELECT from_status,to_status,actor,reason,transitioned_at FROM strategy_state_transitions"
        )
    ).all()
    status = connection.execute(
        text("SELECT status FROM strategy_profile_activation_request")
    ).scalar_one()
    return tuple(state), [tuple(row) for row in audit], status


@pytest.mark.parametrize("outcome", ["commit", "rollback", "ack_lost"])
def test_state_and_request_share_caller_transaction(profile_repository_pg, outcome):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    store = Store(sessionmaker(engine))
    admitted = store.admit(value, current=value.intent)
    redis = MagicMock()
    factory = MagicMock(side_effect=AssertionError("helper must not open a session"))
    manager = StrategyStateManager(factory, redis)
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    before = (("READY", 0), [], "PENDING")
    after = (
        ("ACTIVE", 1),
        [("READY", "ACTIVE", "operator", "accepted", admitted.requested_at)],
        "CONSUMED",
    )
    error = RuntimeError("fixed transaction fault")
    terminal = None
    event.listen(engine, "before_cursor_execute", capture)
    try:
        with engine.connect() as writer, engine.connect() as observer:
            writer_pid = writer.execute(text("SELECT pg_backend_pid()")).scalar_one()
            writer.rollback()
            assert (
                observer.execute(text("SELECT pg_backend_pid()")).scalar_one()
                != writer_pid
            )
            observer.rollback()
            try:
                with Session(bind=writer) as session:
                    with session.begin():
                        transition = manager.transition_in_transaction(
                            session,
                            value.intent.key.strategy_id,
                            StrategyStatus.ACTIVE,
                            actor="operator",
                            reason="accepted",
                            changed_at=admitted.requested_at,
                            expected_version=0,
                        )
                        terminal = terminalize_profile_activation_request(
                            session,
                            value,
                            status=Status.CONSUMED,
                            terminal_at=admitted.requested_at,
                            terminal_reason="ACTIVATED",
                        )
                        assert (
                            transition.version == 1
                            and terminal.status is Status.CONSUMED
                        )
                        assert evidence(session) == after
                        assert evidence(observer) == before
                        observer.rollback()
                        if outcome == "rollback":
                            raise error
                    if outcome == "ack_lost":
                        raise error
            except RuntimeError as caught:
                assert outcome != "commit" and caught is error
            else:
                assert outcome == "commit"
        locks = [sql for sql in statements if "FOR UPDATE" in sql]
        assert terminal is not None
        assert len(locks) == 2
        assert "FROM strategy_state " in " ".join(locks[0].split())
        assert "FROM strategy_profile_activation_request " in " ".join(locks[1].split())
        updates = sum(sql.startswith("UPDATE") for sql in statements)
        assert updates == 2
        for _ in range(2):
            with engine.connect() as connection:
                assert evidence(connection) == (
                    before if outcome == "rollback" else after
                )
            confirmed = store.confirm(value)
            assert confirmed == (admitted if outcome == "rollback" else terminal)
        assert sum(sql.startswith("UPDATE") for sql in statements) == updates
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert not redis.mock_calls and not factory.mock_calls
    assert manager.get_status(value.intent.key.strategy_id) is None


def test_state_expected_version_serializes_two_backends(profile_repository_pg):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    admitted = Store(sessionmaker(engine)).admit(value, current=value.intent)
    redis, factory = MagicMock(), MagicMock(side_effect=AssertionError("no sessions"))
    manager = StrategyStateManager(factory, redis)
    barrier, pids = Barrier(2), []

    def attempt():
        with engine.connect() as connection:
            pids.append(
                connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
            )
            connection.rollback()
            with Session(bind=connection) as session:
                try:
                    with session.begin():
                        session.execute(
                            text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        )
                        assert (
                            session.execute(
                                text("SHOW transaction_isolation")
                            ).scalar_one()
                            == "read committed"
                        )
                        barrier.wait(timeout=10)
                        result = manager.transition_in_transaction(
                            session,
                            value.intent.key.strategy_id,
                            StrategyStatus.ACTIVE,
                            actor="operator",
                            reason="accepted",
                            changed_at=admitted.requested_at,
                            expected_version=0,
                        )
                    return result
                except StaleStrategyStateVersion:
                    return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(attempt) for _ in range(2)]
        results = [future.result(timeout=15) for future in futures]
    assert len(set(pids)) == 2 and results.count(None) == 1
    winner = next(result for result in results if result is not None)
    assert winner.version == 1
    with engine.connect() as connection:
        state, audit, status = evidence(connection)
    assert state == ("ACTIVE", 1) and len(audit) == 1 and status == "PENDING"
    assert not redis.mock_calls and not factory.mock_calls
    assert manager.get_status(value.intent.key.strategy_id) is None
