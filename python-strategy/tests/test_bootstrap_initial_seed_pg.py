"""Real admission composition; local concurrency only, no activation or broker."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import sessionmaker

from src.core.bootstrap_hydration_reader import BootstrapHydrationReader
from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedAdmissionError,
)
from src.core.market_data.profiles.decision_identity import (
    strategy_decision_composition,
)
from src.core.market_data.profiles.orm import BootstrapSeed as SeedRow
from test_profile_bootstrap_pg_fixture import Consumer
from test_profile_bootstrap_seed_store_pg import sample
from test_bootstrap_history_pg import lifecycle

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


def test_authority_held_through_factory_pin_and_explicit_retry(profile_repository_pg):
    engine = profile_repository_pg
    lifecycle(engine)
    sessions = sessionmaker(engine)
    store = BootstrapSeedStore(sessions)
    strategy = Consumer()
    identity = strategy_decision_composition("deployment", strategy)
    candidate = replace(
        sample(),
        key=replace(
            sample().key,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
        ),
        max_seed_candles=10,
    )
    forbidden = MagicMock(side_effect=AssertionError("no application or callback"))
    strategy.on_candle = MagicMock(side_effect=AssertionError("callback forbidden"))
    resolver = MagicMock(return_value=identity)
    holder = BootstrapHydrationReader(
        db_session_factory=sessions,
        seed_store=store,
        application=forbidden,
        decision_owner=forbidden,
        environment="live",
        identity_resolver=lambda _: identity,
        max_seed_candles=10,
        max_recorded_candles=3,
    )
    contender = BootstrapHydrationReader(
        db_session_factory=sessions,
        seed_store=store,
        application=forbidden,
        decision_owner=forbidden,
        environment="live",
        identity_resolver=resolver,
        max_seed_candles=10,
        max_recorded_candles=3,
    )
    store.get = MagicMock(wraps=store.get)
    pin_confirmed = store.pin_confirmed
    entered, release = Event(), Event()
    sql, locks, inserts = [], [], []
    ordered = []

    def pin_and_record(value):
        result = pin_confirmed(value)
        ordered.append(("pin_confirmed_done", inserts[-1]))
        return result

    store.pin_confirmed = MagicMock(side_effect=pin_and_record)

    def capture(conn, cursor, statement, parameters, context, executemany):
        sql.append(statement)
        pid = cursor.connection.get_backend_pid()
        ordered.append((statement, pid))
        if "pg_try_advisory_lock" in statement:
            locks.append((pid, parameters["lock_key"]))
        if statement.startswith("INSERT INTO market_data_bootstrap_seed"):
            inserts.append(pid)

    def create(key):
        assert key == candidate.key
        entered.set()
        assert release.wait(timeout=10), "test did not release factory"
        return candidate

    factory = MagicMock(side_effect=create)
    unused_factory = MagicMock(side_effect=AssertionError("contender must not build"))
    event.listen(engine, "after_cursor_execute", capture)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                holder.prepare_initial_seed_under_admission,
                strategy,
                candidate.cutover_ms,
                factory=factory,
            )
            try:
                assert entered.wait(timeout=10), "holder did not reach factory"
                assert len(locks) == 1
                holder_pid, lock_key = locks[0]
                for changed in (
                    identity,
                    replace(identity, strategy_version="other", config_hash="f" * 64),
                ):
                    resolver.return_value = changed
                    with pytest.raises(BootstrapSeedAdmissionError):
                        contender.prepare_initial_seed_under_admission(
                            strategy, candidate.cutover_ms, factory=unused_factory
                        )
                    assert locks[-1][0] != holder_pid
                    assert locks[-1][1] == lock_key
                unused_factory.assert_not_called()
                store.pin_confirmed.assert_not_called()
                assert store.get.call_count == 1
            finally:
                release.set()
            result = future.result(timeout=10)
        assert result.value.canonical_bytes == candidate.canonical_bytes
        assert result.already_present is False
        factory.assert_called_once_with(candidate.key)
        store.pin_confirmed.assert_called_once_with(candidate)
        assert len(inserts) == 1 and inserts[0] != holder_pid
        unlocks = [
            i
            for i, (statement, pid) in enumerate(ordered)
            if "pg_advisory_unlock" in statement and pid == holder_pid
        ]
        insert_positions = [
            i
            for i, (statement, _) in enumerate(ordered)
            if statement.startswith("INSERT INTO market_data_bootstrap_seed")
        ]
        completed = [
            i
            for i, (kind, pid) in enumerate(ordered)
            if kind == "pin_confirmed_done" and pid == inserts[0]
        ]
        assert len(unlocks) == len(insert_positions) == len(completed) == 1
        assert insert_positions[0] < completed[0] < unlocks[0]
        with engine.connect() as db:
            assert db.scalar(select(func.count()).select_from(SeedRow)) == 1

        # Request the original durable identity. A changed version/config shares
        # the lock, but must not reuse another durable key's seed after release.
        resolver.return_value = identity
        checkpoint = len(sql)
        retry = contender.prepare_initial_seed_under_admission(
            strategy, candidate.cutover_ms + 60000, factory=unused_factory
        )
        assert retry.already_present is True
        assert retry.value.canonical_bytes == result.value.canonical_bytes
        assert retry.recorded_at == result.recorded_at
        assert store.get.call_count == 2
        store.pin_confirmed.assert_called_once()
        unused_factory.assert_not_called()
        assert not any(
            statement.startswith("SELECT EXISTS")
            or "strategy_state" in statement
            or "FROM market_data_decision_batch" in statement
            for statement in sql[checkpoint:]
        )
        assert len(locks) == 4 and len(inserts) == 1
        assert not forbidden.mock_calls
        strategy.on_candle.assert_not_called()
        assert strategy.trace == [] and strategy.accumulator == 0
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", capture)
