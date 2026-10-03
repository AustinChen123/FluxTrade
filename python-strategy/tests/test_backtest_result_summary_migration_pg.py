from __future__ import annotations

from decimal import Decimal

import pytest
import sqlalchemy as sa

from test_migrations import (
    _insert_research_prerequisites,
    _downgrade,
    _target_url,
    _upgrade,
    fresh_pg_db as _fresh_pg_db,
)

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_FORMAL_COLUMNS = (
    "job_id",
    "dataset_id",
    "subject_kind",
    "subject_id",
    "input_digest",
    "result_digest",
    "product_id",
    "timeframe",
    "currency",
    "completed_at",
    "initial_balance",
    "net_pnl",
    "return_pct",
    "max_drawdown",
    "sharpe",
    "sortino",
    "calmar",
)
_NUMERIC_COLUMNS = (
    "initial_balance",
    "net_pnl",
    "return_pct",
    "max_drawdown",
    "sharpe",
    "sortino",
    "calmar",
)


def test_summary_migration_reflection_roundtrip_and_legacy_preservation(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _insert_research_prerequisites(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO strategy (id, name) VALUES ('summary-strategy', 'Fixture')"
            )
        )
    inspector = sa.inspect(engine)
    columns = {
        column["name"]: column
        for column in inspector.get_columns("backtest_result_summary")
    }
    assert set(_FORMAL_COLUMNS) <= columns.keys()
    assert type(columns["completed_at"]["type"]) is sa.BIGINT
    assert type(columns["id"]["type"]) is sa.INTEGER
    table = sa.Table("backtest_result_summary", sa.MetaData(), autoload_with=engine)

    with engine.begin() as conn:
        legacy_id = conn.execute(
            sa.insert(table)
            .values(
                strategy_id="summary-strategy",
                start_time=10,
                end_time=20,
                total_pnl=Decimal("7.25"),
                metrics_json=None,
            )
            .returning(table.c.id)
        ).scalar_one()
    with engine.connect() as conn:
        legacy = (
            conn.execute(sa.select(table).where(table.c.id == legacy_id))
            .mappings()
            .one()
        )
    assert all(legacy[field] is None for field in _FORMAL_COLUMNS)

    engine.dispose()
    _downgrade(fresh_pg_db, "d95a2b73e608")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        assert not set(_FORMAL_COLUMNS) & {
            column["name"]
            for column in sa.inspect(engine).get_columns("backtest_result_summary")
        }
        with engine.connect() as conn:
            legacy_values = conn.execute(
                sa.text(
                    "SELECT total_pnl, metrics_json FROM backtest_result_summary WHERE id=:id"
                ),
                {"id": legacy_id},
            ).one()
        assert legacy_values == (Decimal("7.25"), None)
    finally:
        engine.dispose()
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.connect() as conn:
            legacy_fields = conn.execute(
                sa.text(
                    "SELECT total_pnl, metrics_json, job_id, dataset_id FROM backtest_result_summary WHERE id=:id"
                ),
                {"id": legacy_id},
            ).one()
        assert legacy_fields == (Decimal("7.25"), None, None, None)
    finally:
        engine.dispose()
