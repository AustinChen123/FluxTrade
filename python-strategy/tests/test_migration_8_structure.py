"""Structural tests for Alembic migration 8 (optional performance indexes).

Migration 8 introduces no schema or ORM changes — it only adds two
performance indexes via raw DDL (``op.execute``). Real upgrade/downgrade
behaviour is exercised by Task 0.7's end-to-end round-trip integration
test against a live PostgreSQL.

Here we statically validate:

* The migration file exists and is wired correctly into the Alembic
  revision chain (``revision`` / ``down_revision``).
* It defines ``upgrade`` and ``downgrade`` functions.
* The DDL it emits names the right tables/indexes and uses the partial
  ``WHERE`` clause where required.
* The columns referenced by the DDL really exist on the target ORM
  tables — guards against silent schema drift if anyone renames a column
  on ``Candlestick`` or ``BacktestTradeLog``.

We deliberately avoid ``importlib.import_module`` on the migration file
because Alembic's ``op`` is only importable inside an active migration
context; reading the file as text is sufficient for these structural
checks.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "fb8c6e6098e3_add_optional_performance_indexes.py"
)


@pytest.fixture(scope="module")
def migration_source() -> str:
    assert MIGRATION_PATH.is_file(), f"Migration file missing: {MIGRATION_PATH}"
    return MIGRATION_PATH.read_text(encoding="utf-8")


def test_revision_chain_links_to_migration_7(migration_source: str) -> None:
    """Migration 8 must follow migration 7 (gene registry / evolution epochs)."""
    assert re.search(
        r'^revision\s*:\s*str\s*=\s*"fb8c6e6098e3"', migration_source, re.MULTILINE
    )
    assert re.search(
        r'down_revision\s*:[^=]*=\s*"b34da0d04a78"', migration_source
    )


def test_upgrade_and_downgrade_functions_defined(migration_source: str) -> None:
    """Both direction functions must be defined."""
    assert re.search(r"^def upgrade\(\)\s*->\s*None:", migration_source, re.MULTILINE)
    assert re.search(r"^def downgrade\(\)\s*->\s*None:", migration_source, re.MULTILINE)


def test_upgrade_creates_both_indexes_with_partial_predicate(
    migration_source: str,
) -> None:
    """Upgrade DDL must create both named indexes; the backtest_trade_log
    one must carry the ``WHERE strategy_id IS NOT NULL`` partial predicate."""
    src = migration_source.lower()

    # Candlestick composite index.
    assert "create index if not exists idx_candlestick_product_tf_ts" in src
    assert re.search(
        r"on\s+candlestick\s*\(\s*product_id\s*,\s*timeframe\s*,\s*\"timestamp\"\s+desc\s*\)",
        src,
    )

    # backtest_trade_log partial index.
    assert "create index if not exists idx_backtest_trade_log_strategy" in src
    assert re.search(
        r"on\s+backtest_trade_log\s*\(\s*strategy_id\s*,\s*\"timestamp\"\s+desc\s*\)",
        src,
    )
    assert "where strategy_id is not null" in src


def test_downgrade_drops_both_indexes(migration_source: str) -> None:
    """Downgrade must drop both indexes (idempotent via ``IF EXISTS``)."""
    src = migration_source.lower()
    assert "drop index if exists idx_backtest_trade_log_strategy" in src
    assert "drop index if exists idx_candlestick_product_tf_ts" in src


def test_orm_columns_referenced_by_migration_exist() -> None:
    """The DDL references (product_id, timeframe, timestamp) on
    ``candlestick`` and (strategy_id, timestamp) on ``backtest_trade_log``.
    Make sure those columns really exist on the corresponding ORM tables —
    catches silent schema drift if someone renames a column upstream."""
    from src.core.orm_models import BacktestTradeLog, Candlestick

    candle_cols = {c.name for c in Candlestick.__table__.columns}
    assert {"product_id", "timeframe", "timestamp"}.issubset(candle_cols), (
        f"Candlestick missing expected columns: {candle_cols}"
    )

    btl_cols = {c.name for c in BacktestTradeLog.__table__.columns}
    assert {"strategy_id", "timestamp"}.issubset(btl_cols), (
        f"BacktestTradeLog missing expected columns: {btl_cols}"
    )

    # The partial-index predicate ``WHERE strategy_id IS NOT NULL`` only
    # makes sense if strategy_id is nullable.
    assert BacktestTradeLog.__table__.c.strategy_id.nullable is True
