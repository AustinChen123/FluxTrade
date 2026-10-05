from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import select

from src.core.backtest_result_owner import (
    BacktestResultPersistenceError,
    BacktestResultPersistenceOwner,
)
from src.core.orm_models import BacktestResultSummary
from test_backtest_result_owner_persistence import (
    _database,
    _identity,
    _outcome,
    _row_counts,
)
from test_migrations import fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db


@pytest.mark.parametrize("failure", ["foreign_key", "child_write", "commit"])
def test_writer_rolls_back_database_failure_without_residual_rows(
    fresh_pg_db: str, failure: str
) -> None:
    engine, session_factory = _database(fresh_pg_db)
    trigger = f"p1c_{failure}_failure"
    function = f"p1c_{failure}_failure_fn"
    table = (
        "backtest_equity_sample"
        if failure == "child_write"
        else "backtest_closed_trade"
    )
    try:
        if failure in {"child_write", "commit"}:
            with engine.begin() as connection:
                connection.exec_driver_sql(
                    f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql "
                    "AS $$ BEGIN RAISE EXCEPTION 'p1c injected failure'; END $$"
                )
                if failure == "commit":
                    connection.exec_driver_sql(
                        f"CREATE CONSTRAINT TRIGGER {trigger} AFTER INSERT ON {table} "
                        "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
                        f"EXECUTE FUNCTION {function}()"
                    )
                else:
                    connection.exec_driver_sql(
                        f"CREATE TRIGGER {trigger} BEFORE INSERT ON {table} "
                        f"FOR EACH ROW EXECUTE FUNCTION {function}()"
                    )
        identity = (
            replace(_identity(), dataset_id="missing-dataset")
            if failure == "foreign_key"
            else _identity()
        )
        with pytest.raises(BacktestResultPersistenceError) as error:
            BacktestResultPersistenceOwner(
                session_factory, clock_ns=lambda: 1_700_000_000_000
            ).persist(identity, _outcome())
        assert str(error.value) == "browser_result_persistence_failed"
        assert "p1c injected failure" not in str(error.value)
        assert error.value.__cause__ is None
        assert _row_counts(engine, identity.job_id) == (0, 0, 0, 0, 0)
    finally:
        if failure in {"child_write", "commit"}:
            with engine.begin() as connection:
                connection.exec_driver_sql(
                    f"DROP TRIGGER IF EXISTS {trigger} ON {table}"
                )
                connection.exec_driver_sql(f"DROP FUNCTION IF EXISTS {function}()")
        engine.dispose()


def test_writer_duplicate_job_preserves_existing_result(fresh_pg_db: str) -> None:
    engine, session_factory = _database(fresh_pg_db)
    try:
        owner = BacktestResultPersistenceOwner(
            session_factory, clock_ns=lambda: 1_700_000_000_000
        )
        identity = _identity()
        first = owner.persist(identity, _outcome())
        before = _row_counts(engine, identity.job_id)
        with pytest.raises(BacktestResultPersistenceError) as error:
            owner.persist(identity, replace(_outcome(), total_pnl=Decimal("999")))
        assert str(error.value) == "browser_result_persistence_failed"
        assert _row_counts(engine, identity.job_id) == before
        with session_factory() as session:
            row = session.scalars(
                select(BacktestResultSummary).where(
                    BacktestResultSummary.job_id == identity.job_id
                )
            ).one()
        assert (row.input_digest, row.result_digest) == (
            first.input_digest,
            first.result_digest,
        )
    finally:
        engine.dispose()
