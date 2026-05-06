"""End-to-end migration round-trip integration test (Task 0.7).

Verifies the full Alembic revision chain (rev 1 → rev 8) on a real PostgreSQL
database:

1. ``test_full_upgrade_to_head`` — upgrade ``base`` → ``head`` and assert that
   every table, column, index and CHECK constraint introduced by P0 revisions
   exists with the expected shape.
2. ``test_sample_data_insertion_after_upgrade`` — insert representative rows
   into the new tables and verify that CHECK constraints and partial unique
   indexes behave correctly (positive + negative cases).
3. ``test_full_downgrade_to_base`` — downgrade ``head`` → ``base`` and assert
   that every P0-introduced object is gone (tables dropped, ALTERed columns
   removed).
4. ``test_round_trip_idempotent`` — upgrade → downgrade → upgrade twice and
   compare the resulting schema fingerprints to confirm idempotency.

Each test runs against an isolated database created via ``CREATE DATABASE``
on the project's PostgreSQL instance (Fixture Plan B). The database is
dropped at fixture teardown. If PostgreSQL is unreachable the entire module
is skipped, so this file is safe to keep in the default ``pytest`` run.

Marked ``integration`` to keep it out of unit-only runs.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import Engine

# Alembic is imported lazily inside tests so that import-time failures do not
# break collection on environments where alembic is missing.
ALEMBIC_INI = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "database", "alembic.ini")
)

pytestmark = pytest.mark.integration


# --------------------------------------------------------------------------- #
# Connection helpers
# --------------------------------------------------------------------------- #


def _admin_url() -> str:
    user = os.getenv("POSTGRES_USER", "fluxtrade")
    password = os.getenv("POSTGRES_PASSWORD", "fluxtrade")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    # Connect to the maintenance ``postgres`` database for CREATE/DROP DATABASE.
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"


def _target_url(db_name: str) -> str:
    user = os.getenv("POSTGRES_USER", "fluxtrade")
    password = os.getenv("POSTGRES_PASSWORD", "fluxtrade")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"


def _pg_reachable() -> bool:
    try:
        engine = sa.create_engine(_admin_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:  # pragma: no cover - environment dependent
        return False


# --------------------------------------------------------------------------- #
# Module-level skip
# --------------------------------------------------------------------------- #


if not _pg_reachable():  # pragma: no cover - skipped when PG unavailable
    pytest.skip(
        "PostgreSQL unavailable; skipping migration round-trip integration tests",
        allow_module_level=True,
    )


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture()
def fresh_pg_db() -> Iterator[str]:
    """Create an isolated PostgreSQL database for the test, drop on exit.

    Returns the database name. The database is empty (no migrations applied)
    so each test owns the full upgrade/downgrade lifecycle.
    """
    db_name = f"test_migrations_{os.getpid()}_{int(time.time() * 1000)}"
    admin = sa.create_engine(_admin_url(), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    admin.dispose()
    try:
        yield db_name
    finally:
        admin = sa.create_engine(_admin_url(), isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            # Terminate any lingering connections from alembic before dropping.
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :db AND pid <> pg_backend_pid()"
                ),
                {"db": db_name},
            )
            conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
        admin.dispose()


def _alembic_config(db_name: str):
    """Build an Alembic Config pointed at ``db_name`` using the project ini."""
    from alembic.config import Config

    cfg = Config(ALEMBIC_INI)
    # ``script_location`` in alembic.ini uses %(here)s, which Alembic resolves
    # against the .ini file's directory — no extra fix-up needed.
    cfg.set_main_option("sqlalchemy.url", _target_url(db_name))
    return cfg


def _upgrade(db_name: str, target: str = "head") -> None:
    from alembic import command

    command.upgrade(_alembic_config(db_name), target)


def _downgrade(db_name: str, target: str = "base") -> None:
    from alembic import command

    command.downgrade(_alembic_config(db_name), target)


# --------------------------------------------------------------------------- #
# Schema introspection helpers
# --------------------------------------------------------------------------- #


def _table_names(engine: Engine) -> set[str]:
    insp = sa.inspect(engine)
    return set(insp.get_table_names())


def _column_names(engine: Engine, table: str) -> set[str]:
    insp = sa.inspect(engine)
    return {c["name"] for c in insp.get_columns(table)}


def _index_names(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = :t"
            ),
            {"t": table},
        ).fetchall()
    return {r[0] for r in rows}


def _check_constraint_names(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT con.conname
                FROM pg_constraint con
                JOIN pg_class cls ON cls.oid = con.conrelid
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                WHERE ns.nspname = 'public'
                  AND cls.relname = :t
                  AND con.contype = 'c'
                """
            ),
            {"t": table},
        ).fetchall()
    return {r[0] for r in rows}


def _schema_fingerprint(engine: Engine) -> tuple:
    """Return a stable snapshot of the public schema for diff comparison."""
    insp = sa.inspect(engine)
    fp: list[tuple] = []
    for table in sorted(insp.get_table_names()):
        cols = tuple(
            sorted((c["name"], str(c["type"])) for c in insp.get_columns(table))
        )
        idx = tuple(sorted(_index_names(engine, table)))
        chk = tuple(sorted(_check_constraint_names(engine, table)))
        fp.append((table, cols, idx, chk))
    return tuple(fp)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


# Tables introduced by P0 revisions 5–7 that must exist at HEAD and be gone
# after a full downgrade to ``base``.
P0_NEW_TABLES = {
    "system_events",
    "strategy_state_transitions",
    "daily_nav_snapshots",
    "evolution_epochs",
    "gene_records",
}

# Pre-existing tables (created by revs 1–4) that must remain at every stage.
LEGACY_TABLES = {
    "exchange",
    "product",
    "strategy",
    "order",
    "signal_audit",
    "strategy_state",
}


def test_full_upgrade_to_head(fresh_pg_db: str) -> None:
    """Upgrade ``base`` → ``head`` and verify P0 schema artifacts."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        tables = _table_names(engine)
        assert LEGACY_TABLES.issubset(tables), (
            f"Legacy tables missing after upgrade: {LEGACY_TABLES - tables}"
        )
        assert P0_NEW_TABLES.issubset(tables), (
            f"P0 tables missing after upgrade: {P0_NEW_TABLES - tables}"
        )

        # ``order`` must carry the 5 new idempotency / audit columns.
        order_cols = _column_names(engine, "order")
        for col in (
            "client_order_id",
            "intent_payload",
            "submitted_at",
            "acked_at",
            "last_reconciled_at",
        ):
            assert col in order_cols, f"order.{col} missing after upgrade"

        # ``signal_audit`` must carry the 4 new payload/relation columns.
        sa_cols = _column_names(engine, "signal_audit")
        for col in (
            "client_order_id",
            "intent_payload",
            "outcome_payload",
            "signal_batch_id",
        ):
            assert col in sa_cols, f"signal_audit.{col} missing after upgrade"

        # ``strategy_state`` must gain the 5 new error/version columns.
        ss_cols = _column_names(engine, "strategy_state")
        for col in (
            "last_error_message",
            "entered_error_at",
            "recovered_at",
            "stopped_at",
            "version",
        ):
            assert col in ss_cols, f"strategy_state.{col} missing after upgrade"

        # Partial unique index on order.client_order_id.
        order_idx = _index_names(engine, "order")
        assert "uq_order_client_order_id" in order_idx
        assert "idx_order_client_order_id" in order_idx
        assert "idx_order_strategy_status" in order_idx

        # Partial unique champion-per-strategy index.
        gene_idx = _index_names(engine, "gene_records")
        assert "uq_one_champion_per_strategy" in gene_idx

        # CHECK constraints landed by revs 5–7.
        assert "chk_system_events_type" in _check_constraint_names(
            engine, "system_events"
        )
        ss_checks = _check_constraint_names(engine, "strategy_state")
        assert "chk_error_state" in ss_checks
        assert "chk_stopped_state" in ss_checks
        assert "chk_nav_source" in _check_constraint_names(engine, "daily_nav_snapshots")
        assert "chk_gene_role" in _check_constraint_names(engine, "gene_records")
        assert "chk_epoch_status" in _check_constraint_names(engine, "evolution_epochs")
    finally:
        engine.dispose()


def test_sample_data_insertion_after_upgrade(fresh_pg_db: str) -> None:
    """Exercise CHECKs and partial unique indexes with real INSERTs."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.begin() as conn:
            # FK pre-requisites.
            conn.execute(
                text(
                    "INSERT INTO exchange (id, name) VALUES ('binance', 'Binance')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO product (id, exchange_id, symbol) "
                    "VALUES ('binance:BTC/USDT', 'binance', 'BTC/USDT')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO strategy (id, name, configuration_json) "
                    "VALUES ('strat-1', 'Test Strategy', '{}')"
                )
            )

            # Positive system_events insert.
            conn.execute(
                text(
                    "INSERT INTO system_events (event_type, payload) "
                    "VALUES ('reconcile', '{\"note\": \"ok\"}'::jsonb)"
                )
            )

        # Negative CHECK: invalid event_type must be rejected.
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO system_events (event_type, payload) "
                        "VALUES ('invalid_type', '{}'::jsonb)"
                    )
                )

        # Negative CHECK: daily_nav_snapshots.source must be in whitelist.
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO daily_nav_snapshots "
                        "(strategy_id, snapshot_date, nav, source) "
                        "VALUES ('strat-1', DATE '2026-01-01', 1000.0, 'random')"
                    )
                )

        # Positive nav snapshot insert (default source = 'eod_snapshot').
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO daily_nav_snapshots "
                    "(strategy_id, snapshot_date, nav) "
                    "VALUES ('strat-1', DATE '2026-01-02', 1234.56789012)"
                )
            )

        # Evolution epoch + gene records (champion uniqueness).
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO evolution_epochs
                    (id, strategy_id, started_at, pop_size, max_generations,
                     seed, status, eval_pair, eval_start_date, eval_end_date,
                     eval_timeframe)
                    VALUES ('epoch-1', 'strat-1', NOW(), 10, 5, 42, 'running',
                            'BTC/USDT', DATE '2025-01-01', DATE '2025-06-01', '1h')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO gene_records
                    (strategy_id, role, param_pack, score_total, score_breakdown,
                     max_drawdown, epoch_id)
                    VALUES ('strat-1', 'champion', '{}'::jsonb, 1.0, '{}'::jsonb,
                            0.05, 'epoch-1')
                    """
                )
            )

        # Negative CHECK: invalid gene role.
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO gene_records
                        (strategy_id, role, param_pack, score_total,
                         score_breakdown, max_drawdown, epoch_id)
                        VALUES ('strat-1', 'bogus', '{}'::jsonb, 0.0, '{}'::jsonb,
                                0.0, 'epoch-1')
                        """
                    )
                )

        # Partial unique: a second champion for the same strategy must fail.
        with pytest.raises(sa.exc.IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO gene_records
                        (strategy_id, role, param_pack, score_total,
                         score_breakdown, max_drawdown, epoch_id)
                        VALUES ('strat-1', 'champion', '{}'::jsonb, 2.0,
                                '{}'::jsonb, 0.05, 'epoch-1')
                        """
                    )
                )

        # ...but a challenger alongside the champion is allowed.
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO gene_records
                    (strategy_id, role, param_pack, score_total, score_breakdown,
                     max_drawdown, epoch_id)
                    VALUES ('strat-1', 'challenger', '{}'::jsonb, 0.5, '{}'::jsonb,
                            0.1, 'epoch-1')
                    """
                )
            )

        # Read-back sanity: rows are visible.
        with engine.begin() as conn:
            n_events = conn.execute(
                text("SELECT COUNT(*) FROM system_events")
            ).scalar_one()
            n_genes = conn.execute(
                text("SELECT COUNT(*) FROM gene_records")
            ).scalar_one()
        assert n_events == 1
        assert n_genes == 2
    finally:
        engine.dispose()


def test_full_downgrade_to_base(fresh_pg_db: str) -> None:
    """Upgrade then downgrade fully; P0 objects must be removed."""
    _upgrade(fresh_pg_db, "head")
    _downgrade(fresh_pg_db, "base")

    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        tables = _table_names(engine)
        # All P0-introduced tables are gone.
        assert P0_NEW_TABLES.isdisjoint(tables), (
            f"P0 tables still present after downgrade: {P0_NEW_TABLES & tables}"
        )
        # And the original revs 1–4 tables are also gone (full downgrade).
        # alembic_version may or may not remain depending on Alembic version;
        # exclude it from comparison.
        residual = tables - {"alembic_version"}
        assert residual == set(), (
            f"Unexpected residual tables after full downgrade: {residual}"
        )
    finally:
        engine.dispose()


def test_round_trip_idempotent(fresh_pg_db: str) -> None:
    """Upgrade → downgrade → upgrade twice: schema fingerprints must match."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        fp_first = _schema_fingerprint(engine)
    finally:
        engine.dispose()

    _downgrade(fresh_pg_db, "base")
    _upgrade(fresh_pg_db, "head")

    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        fp_second = _schema_fingerprint(engine)
    finally:
        engine.dispose()

    assert fp_first == fp_second, "Schema fingerprint diverged across round-trip"
