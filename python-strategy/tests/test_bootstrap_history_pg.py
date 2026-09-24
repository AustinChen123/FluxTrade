"""Isolated PG authority/cleanup acceptance; no activation or raw-writer fence."""

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import event, insert, text
from sqlalchemy.orm import sessionmaker

from src.core.orm_models import (
    Strategy,
    StrategyState,
    StrategyStateTransition,
    MarketDataApplication,
)
from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedAdmissionError,
)
from src.core.market_data.profiles.decision_input_store import DecisionInputStore
from src.core.market_data.profiles.decision_application import MarketDataDecisionOutcome
from src.core.market_data.profiles.decision_application_store import (
    append_decision_batch,
    _header,
)
from src.core.market_data.profiles.orm import MarketDataDecisionBatch as BatchRow
from src.core.market_data.profiles.repository import TransactionWaitPolicy
from test_profile_bootstrap_seed_store_pg import sample
from test_profile_decision_input import sample as decision_input
from test_profile_decision_application import batch, key

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


def lifecycle(engine, status="DISCOVERED", version=0, transitions=()):
    with engine.begin() as db:
        db.execute(insert(Strategy).values(id="s", name="history acceptance"))
        if status is not None:
            values: dict[str, Any] = dict(
                strategy_id="s", status=status, version=version
            )
            if status == "STOPPED":
                values["stopped_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
            if status == "ERROR":
                values.update(
                    entered_error_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                    last_error_message="fixture",
                )
            db.execute(insert(StrategyState).values(**values))
        for before, after in transitions:
            db.execute(
                insert(StrategyStateTransition).values(
                    strategy_id="s", from_status=before, to_status=after
                )
            )


@pytest.mark.parametrize(
    "status,version,transitions,expected",
    [
        ("DISCOVERED", 0, (), "ABSENT"),
        ("READY", 1, (("DISCOVERED", "READY"),), "ABSENT"),
        ("WARNING", 2, (("DISCOVERED", "READY"), ("READY", "WARNING")), "ABSENT"),
        (None, 0, (), "UNKNOWN"),
        ("READY", 1, (), "UNKNOWN"),
        ("ACTIVE", 0, (), "UNKNOWN"),
        ("STOPPED", 0, (), "UNKNOWN"),
        ("ERROR", 0, (), "UNKNOWN"),
        ("READY", 2, (("READY", "ACTIVE"), ("ACTIVE", "READY")), "UNKNOWN"),
    ],
)
def test_real_lifecycle_audit(
    profile_repository_pg, status, version, transitions, expected
):
    lifecycle(profile_repository_pg, status, version, transitions)
    store = BootstrapSeedStore(sessionmaker(profile_repository_pg))
    wanted = sample().key
    with store.initial_admission(wanted) as admission:
        proof = admission.read_history(wanted, 0)
    assert proof.state == expected and proof.key is wanted


@pytest.mark.parametrize(
    "evidence,expected",
    [
        ("seed", "PRESENT"),
        ("orphan_input", "PRESENT"),
        ("outcome", "PRESENT"),
        ("receipt", "ABSENT"),
        ("empty_batch", "ABSENT"),
        ("target_header_only", "UNKNOWN"),
    ],
)
def test_real_row_precedence_and_ignored_version_config_boundary(
    profile_repository_pg, evidence, expected
):
    engine = profile_repository_pg
    lifecycle(engine, "ACTIVE" if expected == "PRESENT" else "DISCOVERED")
    sessions = sessionmaker(engine)
    store = BootstrapSeedStore(sessions)
    if evidence == "seed":
        store.pin(sample())
    elif evidence == "orphan_input":
        DecisionInputStore(sessions).pin(decision_input())
    else:
        with sessions.begin() as db:
            db.add(
                MarketDataApplication(
                    environment="live",
                    product_id=sample().key.product_id,
                    timeframe="1m",
                    timestamp=0,
                    open=Decimal(10),
                    high=Decimal(10),
                    low=Decimal(10),
                    close=Decimal(10),
                    volume=Decimal(1),
                    decision_contract_version=None if evidence == "receipt" else 1,
                )
            )
            db.flush()
            if evidence != "receipt":
                terminal = batch()
                if evidence in ("outcome", "target_header_only"):
                    outcome = MarketDataDecisionOutcome(
                        key(), "SKIPPED", None, None, "INPUT_STORE_FAILED"
                    )
                    terminal = batch((key(),), (outcome,))
                if evidence == "target_header_only":
                    db.execute(insert(BatchRow).values(**_header(terminal)))
                else:
                    append_decision_batch(db, terminal)
    wanted = replace(sample().key, strategy_version="different", config_hash="f" * 64)
    with store.initial_admission(wanted) as admission:
        # Even C=0 cannot hide later immutable lineage history.
        proof = admission.read_history(wanted, 0)
    assert proof.state == expected


def test_setup_history_unlock_share_backend_and_release(profile_repository_pg):
    engine = profile_repository_pg
    lifecycle(engine)
    seen, isolation = [], []

    def capture(conn, cursor, statement, parameters, context, executemany):
        seen.append((statement, cursor.connection.get_backend_pid()))
        if "pg_try_advisory_lock" in statement:
            with cursor.connection.cursor() as probe:
                probe.execute("SHOW transaction_isolation")
                isolation.append(probe.fetchone()[0])

    event.listen(engine, "after_cursor_execute", capture)
    try:
        with engine.connect() as reserved:
            store = BootstrapSeedStore(sessionmaker(engine))
            with store.initial_admission(sample().key) as admission:
                assert admission.read_history(sample().key, 0).state == "ABSENT"
            holder = seen[0][1]
            assert len(seen) == 7 and {pid for _, pid in seen} == {holder}
            assert seen[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            assert "set_config" in seen[1][0] and "pg_try_advisory_lock" in seen[2][0]
            assert "pg_advisory_unlock" in seen[-1][0]
            with BootstrapSeedStore(sessionmaker(reserved)).initial_admission(
                sample().key
            ):
                assert seen[-1][1] != holder
            assert isolation == ["read committed", "read committed"]
    finally:
        event.remove(engine, "after_cursor_execute", capture)


def test_real_table_timeout_discards_locked_backend_and_releases_advisory_lock(
    profile_repository_pg,
):
    engine = profile_repository_pg
    lifecycle(engine)
    acquired, invalidated, codes = [], [], []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if "pg_try_advisory_lock" in statement:
            acquired.append(cursor.connection.get_backend_pid())

    def invalidate(dbapi_connection, connection_record, exception):
        invalidated.append(dbapi_connection.get_backend_pid())

    def failure(context):
        codes.append(context.original_exception.pgcode)

    event.listen(engine, "after_cursor_execute", capture)
    event.listen(engine.pool, "invalidate", invalidate)
    event.listen(engine, "handle_error", failure)
    try:
        with engine.connect() as reserved, engine.connect() as blocker:
            transaction = blocker.begin()
            try:
                blocker.execute(
                    text(
                        "LOCK TABLE market_data_bootstrap_seed IN ACCESS EXCLUSIVE MODE"
                    )
                )
                store = BootstrapSeedStore(
                    sessionmaker(engine), TransactionWaitPolicy(25, 250)
                )
                with pytest.raises(BootstrapSeedAdmissionError) as caught:
                    with store.initial_admission(sample().key) as admission:
                        admission.read_history(sample().key, 0)
                assert str(caught.value) == "BOOTSTRAP_SEED_ADMISSION"
                assert codes == ["55P03", "25P02"]
                assert len(acquired) == 1 and invalidated == acquired
            finally:
                transaction.rollback()
            holder = acquired[0]
            with BootstrapSeedStore(sessionmaker(reserved)).initial_admission(
                sample().key
            ) as admission:
                assert acquired[-1] != holder
                assert admission.read_history(sample().key, 0).state == "ABSENT"
    finally:
        event.remove(engine, "after_cursor_execute", capture)
        event.remove(engine.pool, "invalidate", invalidate)
        event.remove(engine, "handle_error", failure)
