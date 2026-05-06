"""Structural tests for Migration 6 ORM additions.

These tests inspect SQLAlchemy ``Table`` metadata only — no live
PostgreSQL connection is required. End-to-end migration round-trip
coverage is owned by Task 0.7.

Migration 6 covers:
    * ``strategy_state`` ALTER — 5 audit columns + 2 CHECK constraints
    * ``strategy_state_transitions`` CREATE — append-only transition log
    * ``daily_nav_snapshots`` CREATE — EOD NAV snapshots with CHECK +
      UNIQUE constraints
"""
from __future__ import annotations

from sqlalchemy import CheckConstraint, Date, Integer, Numeric, UniqueConstraint

from src.core.orm_models import (
    DailyNavSnapshot,
    StrategyState,
    StrategyStateTransition,
)


# ---------------------------------------------------------------------------
# strategy_state ALTER columns + CHECK constraints
# ---------------------------------------------------------------------------


def test_strategy_state_has_migration6_columns() -> None:
    cols = StrategyState.__table__.columns
    expected = {
        "last_error_message",
        "entered_error_at",
        "recovered_at",
        "stopped_at",
        "version",
    }
    missing = expected - set(cols.keys())
    assert not missing, f"StrategyState missing migration 6 columns: {missing}"

    # Optimistic-lock counter — NOT NULL, integer, default 0.
    version_col = cols["version"]
    assert not version_col.nullable, "version must be NOT NULL"
    assert isinstance(version_col.type, Integer)
    # server_default is always wrapped in DefaultClause; just check it exists.
    assert version_col.server_default is not None, (
        "version must have a server_default of 0 so existing rows backfill"
    )

    # The four audit timestamps + message column must remain nullable so
    # existing rows do not violate NOT NULL during ALTER.
    for name in ("last_error_message", "entered_error_at", "recovered_at", "stopped_at"):
        assert cols[name].nullable, f"StrategyState.{name} should be nullable"


def test_strategy_state_has_check_constraints() -> None:
    constraints = [
        c
        for c in StrategyState.__table__.constraints
        if isinstance(c, CheckConstraint)
    ]
    names = {c.name for c in constraints}
    assert "chk_error_state" in names, (
        f"Expected chk_error_state CHECK constraint, got {names}"
    )
    assert "chk_stopped_state" in names, (
        f"Expected chk_stopped_state CHECK constraint, got {names}"
    )


# ---------------------------------------------------------------------------
# strategy_state_transitions
# ---------------------------------------------------------------------------


def test_strategy_state_transition_class_has_required_columns() -> None:
    cols = StrategyStateTransition.__table__.columns
    expected = {
        "id",
        "strategy_id",
        "from_status",
        "to_status",
        "transitioned_at",
        "reason",
        "actor",
    }
    missing = expected - set(cols.keys())
    assert not missing, f"StrategyStateTransition missing columns: {missing}"

    # NOT NULL columns.
    for name in ("strategy_id", "from_status", "to_status", "transitioned_at"):
        assert not cols[name].nullable, f"{name} should be NOT NULL"

    # Nullable columns.
    for name in ("reason", "actor"):
        assert cols[name].nullable, f"{name} should be nullable"


def test_strategy_state_transition_strategy_id_is_fk_to_strategy() -> None:
    col = StrategyStateTransition.__table__.columns["strategy_id"]
    fks = list(col.foreign_keys)
    assert len(fks) == 1, "strategy_id must FK to strategy.id"
    assert fks[0].column.table.name == "strategy"


# ---------------------------------------------------------------------------
# daily_nav_snapshots
# ---------------------------------------------------------------------------


def test_daily_nav_snapshot_class_has_required_columns() -> None:
    cols = DailyNavSnapshot.__table__.columns
    expected = {
        "id",
        "strategy_id",
        "snapshot_date",
        "nav",
        "base_currency",
        "drawdown",
        "return_pct",
        "source",
        "recorded_at",
        "notes",
    }
    missing = expected - set(cols.keys())
    assert not missing, f"DailyNavSnapshot missing columns: {missing}"


def test_daily_nav_snapshot_nav_is_numeric_28_8_not_float() -> None:
    """``nav`` must be ``NUMERIC(28, 8)`` — float is forbidden for money."""
    nav_col = DailyNavSnapshot.__table__.columns["nav"]
    assert isinstance(nav_col.type, Numeric), (
        f"nav must be Numeric, got {type(nav_col.type).__name__}"
    )
    assert nav_col.type.precision == 28, (
        f"nav precision should be 28, got {nav_col.type.precision}"
    )
    assert nav_col.type.scale == 8, (
        f"nav scale should be 8, got {nav_col.type.scale}"
    )
    assert not nav_col.nullable, "nav must be NOT NULL"

    # snapshot_date should be a DATE column (not DateTime / BigInt).
    assert isinstance(
        DailyNavSnapshot.__table__.columns["snapshot_date"].type, Date
    )


def test_daily_nav_snapshot_source_has_default_and_check() -> None:
    cols = DailyNavSnapshot.__table__.columns
    source_col = cols["source"]
    assert not source_col.nullable, "source must be NOT NULL"
    assert source_col.server_default is not None, (
        "source must have a server_default of 'eod_snapshot'"
    )

    # CHECK constraint on source values.
    constraints = [
        c
        for c in DailyNavSnapshot.__table__.constraints
        if isinstance(c, CheckConstraint)
    ]
    names = {c.name for c in constraints}
    assert "chk_nav_source" in names, (
        f"Expected chk_nav_source CHECK constraint, got {names}"
    )


def test_daily_nav_snapshot_has_unique_strategy_date() -> None:
    """Only one snapshot per (strategy_id, snapshot_date)."""
    uniques = [
        c
        for c in DailyNavSnapshot.__table__.constraints
        if isinstance(c, UniqueConstraint)
    ]
    matching = [
        c
        for c in uniques
        if {col.name for col in c.columns} == {"strategy_id", "snapshot_date"}
    ]
    assert matching, "Expected UNIQUE(strategy_id, snapshot_date) on daily_nav_snapshots"


def test_daily_nav_snapshot_strategy_id_is_fk_to_strategy() -> None:
    col = DailyNavSnapshot.__table__.columns["strategy_id"]
    fks = list(col.foreign_keys)
    assert len(fks) == 1, "strategy_id must FK to strategy.id"
    assert fks[0].column.table.name == "strategy"
