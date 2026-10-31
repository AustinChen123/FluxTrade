"""Real PostgreSQL boundary checks for the approved runtime SQL constraints."""

import os

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError

from test_migrations import (
    _target_url,
    _upgrade,
    fresh_pg_db as _migration_fresh_pg_db,
)

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
def pg_engine(fresh_pg_db: str):
    _upgrade(fresh_pg_db)
    engine = create_engine(_target_url(fresh_pg_db))
    try:
        yield engine
    finally:
        engine.dispose()


def _insert(connection, statement: str, value: object, *, accepted: bool) -> None:
    try:
        with connection.begin_nested():
            connection.execute(text(statement), {"value": value})
    except IntegrityError:
        if accepted:
            raise
    else:
        if not accepted:
            raise AssertionError("PostgreSQL CHECK accepted an out-of-range value")


def _check_expression(connection, table: str, constraint: str) -> str:
    row = connection.execute(
        text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = to_regclass(:table) AND conname = :constraint"
        ),
        {"table": table, "constraint": constraint},
    ).one_or_none()
    if row is None or type(row[0]) is not str:
        raise AssertionError(f"missing migrated constraint {constraint}")
    definition = row[0]
    if not definition.startswith("CHECK (") or not definition.endswith(")"):
        raise AssertionError(f"unexpected PostgreSQL constraint definition: {constraint}")
    return definition[len("CHECK (") : -1]


def test_runtime_sql_check_boundaries(pg_engine: Engine):
    with pg_engine.connect() as connection:
        state_version = _check_expression(
            connection, "spider_runtime_scope", "ck_spider_runtime_scope_state_version"
        )
        action_payload = _check_expression(
            connection, "spider_runtime_action", "ck_spider_runtime_action_payload"
        )
        delivery_payload = _check_expression(
            connection, "spider_runtime_delivery", "ck_spider_runtime_delivery_payload"
        )
        if "2147483647" not in state_version or "1048576" not in action_payload or "1048576" not in delivery_payload:
            raise AssertionError("migrated Spider check differs from the approved bounds")
        connection.execute(text(
            "CREATE TEMP TABLE spider_scope_version_bound "
            f"(state_version BIGINT CHECK ({state_version}))"
        ))
        connection.execute(text(
            "CREATE TEMP TABLE spider_action_payload_bound "
            f"(original_payload BYTEA CHECK ({action_payload}))"
        ))
        connection.execute(text(
            "CREATE TEMP TABLE spider_delivery_payload_bound "
            f"(source_payload BYTEA CHECK ({delivery_payload}))"
        ))

        connection.commit()
        with connection.begin():
            _insert(
                connection,
                "INSERT INTO spider_scope_version_bound(state_version) VALUES (:value)",
                2147483647,
                accepted=True,
            )
            _insert(
                connection,
                "INSERT INTO spider_scope_version_bound(state_version) VALUES (:value)",
                2147483648,
                accepted=False,
            )
            for table in (
                "spider_action_payload_bound",
                "spider_delivery_payload_bound",
            ):
                column = "original_payload" if "action" in table else "source_payload"
                statement = f"INSERT INTO {table}({column}) VALUES (:value)"
                for size in (65537, 1048576):
                    _insert(connection, statement, b"x" * size, accepted=True)
                _insert(connection, statement, b"x" * 1048577, accepted=False)
