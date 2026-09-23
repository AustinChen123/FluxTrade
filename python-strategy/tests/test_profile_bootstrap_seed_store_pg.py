"""Explicitly gated isolated PostgreSQL acceptance; no live database access."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from threading import Barrier
from datetime import timedelta

import pytest
from sqlalchemy import event, insert, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedConflict,
    BootstrapSeedIntegrityError,
    BootstrapSeedPinStatus,
    _values,
)
from src.core.market_data.profiles.orm import BootstrapSeed as SeedRow
from test_profile_bootstrap_seed import seed
from test_migrations import (
    _upgrade,
    _downgrade,
    _target_url,
)

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


def sample():
    value = seed()
    return replace(
        value,
        key=replace(value.key, product_id="BINANCE:BTCUSDT-SPOT"),
        max_seed_candles=10,
    )


def test_concurrent_pin_and_content_conflict(profile_repository_pg):
    engine = profile_repository_pg.execution_options(isolation_level="REPEATABLE READ")
    store = BootstrapSeedStore(sessionmaker(engine))
    isolation = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("SELECT market_data_bootstrap_seed"):
            with conn.connection.cursor() as probe:
                probe.execute("SHOW transaction_isolation")
                isolation.append(probe.fetchone()[0])

    event.listen(engine, "after_cursor_execute", capture)
    try:
        for conflict in (False, True):
            first = replace(
                sample(),
                key=replace(sample().key, strategy_id=f"s_{conflict}"),
                max_seed_candles=10,
            )
            second = (
                replace(first, dataset_digest="d" * 64, max_seed_candles=10)
                if conflict
                else first
            )
            barrier = Barrier(2)

            def pin(value):
                barrier.wait(timeout=5)
                try:
                    return store.pin(value)
                except BootstrapSeedConflict:
                    return None

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(pin, value) for value in (first, second)]
                results = [future.result(timeout=10) for future in futures]
            if conflict:
                assert sum(result is None for result in results) == 1
            else:
                assert sorted(
                    result.already_present for result in results if result is not None
                ) == [False, True]
        assert isolation and set(isolation) == {"read committed"}
    finally:
        event.remove(engine, "after_cursor_execute", capture)


def test_rollback_and_real_postcommit_ack_confirmation(profile_repository_pg):
    engine = profile_repository_pg
    value = sample()
    error = DBAPIError(None, None, RuntimeError("SECRET"))
    store = BootstrapSeedStore(sessionmaker(engine))

    def abort(session):
        raise error

    with Session(engine) as session:
        event.listen(session, "before_commit", abort)
        with pytest.raises(DBAPIError) as caught:
            BootstrapSeedStore(lambda: nullcontext(session)).pin(value)
        assert caught.value is error
    assert store.get(value.key) is None
    calls = []

    @contextmanager
    def ack_once():
        calls.append(1)
        with Session(engine) as session:
            yield session
        if len(calls) == 1:
            raise error

    result = BootstrapSeedStore(ack_once).pin_confirmed(value)
    assert result.status is BootstrapSeedPinStatus.CONFIRMED and len(calls) == 2
    assert result.record == store.confirm(value)
    assert (
        result.record is not None
        and result.record.value.canonical_bytes == value.canonical_bytes
    )


@pytest.mark.parametrize(
    "column,bad",
    [
        ("dataset_digest", "d" * 64),
        ("cutover_ms", 0),
        ("canonical_payload", b"{}"),
        ("requirements_digest", "e" * 64),
    ],
)
def test_stored_corruption_is_not_missing(profile_repository_pg, column, bad):
    value = sample()
    with profile_repository_pg.begin() as conn:
        conn.execute(insert(SeedRow).values(**(_values(value) | {column: bad})))
    store = BootstrapSeedStore(sessionmaker(profile_repository_pg))
    for call in (
        lambda: store.get(value.key),
        lambda: store.confirm(value),
        lambda: store.pin(value),
    ):
        with pytest.raises(BootstrapSeedIntegrityError):
            call()


def test_constraints_and_append_only(profile_repository_pg):
    engine = profile_repository_pg
    value = sample()
    record = BootstrapSeedStore(sessionmaker(engine)).pin(value)
    assert record.recorded_at.utcoffset() == timedelta(0)
    assert record.recorded_at.microsecond % 1000 == 0
    for statement in (
        "UPDATE market_data_bootstrap_seed SET lookback=lookback WHERE false",
        "DELETE FROM market_data_bootstrap_seed WHERE false",
        "TRUNCATE market_data_bootstrap_seed",
    ):
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                with pytest.raises(DBAPIError) as caught:
                    conn.execute(text(statement))
                assert getattr(caught.value.orig, "pgcode", None) == "55000"
            finally:
                transaction.rollback()
    for column, bad in (
        ("canonical_payload", b""),
        ("canonical_payload", b"x" * (32 * 1024 * 1024 + 1)),
        ("seed_digest", "X" * 64),
        ("cutover_ms", -1),
        ("lookback", -1),
        ("contract_version", 2),
        ("product_id", "BINANCE:UNKNOWN-SPOT"),
    ):
        values = _values(value) | {
            "seed_id": "f" * 64,
            "strategy_id": "other",
            column: bad,
        }
        with engine.begin() as conn:
            with pytest.raises(DBAPIError):
                with conn.begin_nested():
                    conn.execute(insert(SeedRow).values(**values))
    assert BootstrapSeedStore(sessionmaker(engine)).confirm(value) is not None
    # Independently prove PK and full durable-key uniqueness (C is not in key).
    for change in ({"strategy_id": "pk_other"}, {"seed_id": "f" * 64, "cutover_ms": 0}):
        with engine.begin() as conn:
            with pytest.raises(DBAPIError) as caught:
                with conn.begin_nested():
                    conn.execute(insert(SeedRow).values(**(_values(value) | change)))
            assert getattr(caught.value.orig, "pgcode", None) == "23505"
    for column, bad in (
        *(
            (name, "X" * 64)
            for name in (
                "seed_id",
                "config_hash",
                "requirements_digest",
                "policy_digest",
                "dataset_digest",
            )
        ),
        *(
            (name, "bad:scope")
            for name in (
                "environment",
                "execution_scope_id",
                "strategy_id",
                "strategy_version",
                "timeframe",
            )
        ),
        ("recorded_at", "infinity"),
        ("recorded_at", "0001-01-01 00:00:00.000001+00"),
    ):
        values = _values(value) | {
            "seed_id": "f" * 64,
            "strategy_id": "other",
            column: bad,
        }
        with engine.begin() as conn:
            with pytest.raises(DBAPIError) as caught:
                with conn.begin_nested():
                    conn.execute(insert(SeedRow).values(**values))
            assert getattr(caught.value.orig, "pgcode", None) == "23514"


def test_upgrade_downgrade_artifacts(fresh_pg_db):
    from sqlalchemy import create_engine, inspect

    _upgrade(fresh_pg_db)
    engine = create_engine(_target_url(fresh_pg_db))
    try:
        assert "market_data_bootstrap_seed" in inspect(engine).get_table_names()
        _downgrade(fresh_pg_db, "2f6c8a1e9b04")
        assert "market_data_bootstrap_seed" not in inspect(engine).get_table_names()
        _upgrade(fresh_pg_db)
        assert "market_data_bootstrap_seed" in inspect(engine).get_table_names()
    finally:
        engine.dispose()


def test_unique_wait_timeout_no_retry(profile_repository_pg):
    from src.core.market_data.profiles.repository import TransactionWaitPolicy

    engine = profile_repository_pg
    value = sample()
    attempts = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO market_data_bootstrap_seed"):
            attempts.append(statement)

    store = BootstrapSeedStore(sessionmaker(engine), TransactionWaitPolicy(25, 1000))
    with engine.connect() as holder:
        transaction = holder.begin()
        try:
            holder.execute(insert(SeedRow).values(**_values(value)))
            event.listen(engine, "before_cursor_execute", capture)
            try:
                with pytest.raises(DBAPIError) as caught:
                    store.pin(value)
                assert getattr(caught.value.orig, "pgcode", None) == "55P03"
                assert len(attempts) == 1
            finally:
                event.remove(engine, "before_cursor_execute", capture)
        finally:
            transaction.rollback()
    assert store.get(value.key) is None
    assert not store.pin(value).already_present
