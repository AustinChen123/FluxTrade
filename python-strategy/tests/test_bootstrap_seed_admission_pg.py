"""Real isolated PG admission, not an authority fence against raw writers.

Unlock fault injection remains a separate acceptance slice; these tests exercise
normal release and competing physical sessions without driver monkeypatching.
"""

from dataclasses import replace

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.orm import sessionmaker

from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedStore,
    BootstrapSeedAdmissionError,
    BootstrapSeedPinStatus,
)
from src.core.market_data.profiles.orm import BootstrapSeed as SeedRow
from test_profile_bootstrap_seed_store_pg import sample
from test_profile_bootstrap_seed import seed

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


def test_distinct_backends_lineage_pool_hold_and_second_connection_pin(
    profile_repository_pg,
):
    engine = profile_repository_pg
    store = BootstrapSeedStore(sessionmaker(engine))
    locks, writes, checkins = [], [], []

    def capture(conn, cursor, statement, parameters, context, executemany):
        pid = cursor.connection.get_backend_pid()
        if "pg_try_advisory_lock" in statement:
            locks.append(pid)
        if statement.startswith("INSERT INTO market_data_bootstrap_seed"):
            writes.append(pid)

    def checked_in(dbapi_connection, connection_record):
        if dbapi_connection is not None:
            checkins.append(dbapi_connection.get_backend_pid())

    event.listen(engine, "after_cursor_execute", capture)
    event.listen(engine.pool, "checkin", checked_in)
    try:
        value = sample()
        with store.initial_admission(value.key):
            holder_pid = locks[-1]
            checkpoint = len(checkins)
            # Explicitly reserve the competing physical backend through release.
            with engine.connect() as competitor_connection:
                contender = BootstrapSeedStore(sessionmaker(competitor_connection))
                earlier = replace(seed(count=1), key=value.key, max_seed_candles=10)
                assert earlier.cutover_ms != value.cutover_ms
                variants = (
                    value.key,
                    replace(value.key, strategy_version="v2"),
                    replace(value.key, config_hash="f" * 64),
                    earlier.key,
                )
                for key in variants:
                    with pytest.raises(BootstrapSeedAdmissionError):
                        with contender.initial_admission(key):
                            pytest.fail("busy admission must not run body or pin")
                    assert locks[-1] != holder_pid
                competitor_pid = locks[-1]
                assert writes == []
                with contender.initial_admission(
                    replace(value.key, strategy_id="other")
                ):
                    assert locks[-1] == competitor_pid
                    assert holder_pid not in checkins[checkpoint:]

                # Pin is deliberately a non-admission writer. The lock does not
                # block raw writers; future startup callers must cooperate.
                result = store.pin_confirmed(value)
                assert result.status is BootstrapSeedPinStatus.CONFIRMED
                assert result.record is not None
                assert result.record.value.canonical_bytes == value.canonical_bytes
                assert writes and all(pid != holder_pid for pid in writes)
                confirmed = store.confirm(value)
                assert confirmed is not None
                assert confirmed.value == value
                assert confirmed.recorded_at == result.record.recorded_at
                assert holder_pid not in checkins[checkpoint:]
            assert holder_pid not in checkins[checkpoint:]
        assert holder_pid in checkins[checkpoint:]
        with engine.connect() as db:
            assert db.scalar(select(func.count()).select_from(SeedRow)) == 1
        with store.initial_admission(value.key):
            pass
    finally:
        event.remove(engine, "after_cursor_execute", capture)
        event.remove(engine.pool, "checkin", checked_in)


class Halt(BaseException):
    pass


@pytest.mark.parametrize("error_type", [None, RuntimeError, Halt])
def test_release_allows_other_reserved_backend_after_every_body_exit(
    profile_repository_pg,
    error_type,
):
    engine = profile_repository_pg
    store = BootstrapSeedStore(sessionmaker(engine))
    pids = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if "pg_try_advisory_lock" in statement:
            pids.append(cursor.connection.get_backend_pid())

    event.listen(engine, "after_cursor_execute", capture)
    try:
        # Reserve one connection before holder acquisition so pool reuse cannot
        # make the release assertion pass via reentrant locking on the holder.
        with engine.connect() as other_connection:
            contender = BootstrapSeedStore(sessionmaker(other_connection))
            error = error_type("body failure") if error_type is not None else None

            def run():
                with store.initial_admission(sample().key):
                    with pytest.raises(BootstrapSeedAdmissionError):
                        with contender.initial_admission(sample().key):
                            pytest.fail("busy body")
                    assert pids[0] != pids[1]
                    if error is not None:
                        raise error

            if error_type is None:
                run()
            else:
                with pytest.raises(error_type) as caught:
                    run()
                assert caught.value is error
            with contender.initial_admission(sample().key):
                assert pids == [pids[0], pids[1], pids[1]]
                assert pids[0] != pids[-1]
    finally:
        event.remove(engine, "after_cursor_execute", capture)
