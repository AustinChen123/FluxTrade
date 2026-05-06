"""add_strategy_state_audit_and_daily_nav

Revision ID: 8d1f3b6c2a4e
Revises: 7c9e4f2a1b3d
Create Date: 2026-05-04 00:00:00.000000

Migration 6 — Phase 0 architecture fixes.

Changes:
    * ``strategy_state`` table: ALTER add 5 audit / lifecycle columns
      (``last_error_message``, ``entered_error_at``, ``recovered_at``,
      ``stopped_at``, ``version``) plus 2 CHECK constraints enforcing
      that ERROR / STOPPED states carry the necessary metadata.
    * ``strategy_state_transitions`` table: new audit log of every
      status change (one row per transition), with FK to ``strategy``
      and a covering index for time-ordered lookups.
    * ``daily_nav_snapshots`` table: end-of-day NAV snapshots, with
      FK to ``strategy``, UNIQUE (strategy_id, snapshot_date), and a
      CHECK constraint on the ``source`` column.

``StrategyStatus`` (see ``src/core/models.py``) uses upper-case enum
values (``ACTIVE`` / ``STOPPED`` / ``ERROR`` / ...). The CHECK
constraints below are written to match those values exactly; legacy
lower-case rows would not satisfy the predicate, so callers must
migrate any historical data to the canonical casing before applying
this revision (no such rows exist in current environments).

The ``version`` column backfills to ``0`` for existing rows because it
is declared ``NOT NULL DEFAULT 0`` — Postgres applies the default to
existing tuples when the column is added in a single ALTER.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "8d1f3b6c2a4e"
down_revision: Union[str, Sequence[str], None] = "7c9e4f2a1b3d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply schema changes for migration 6."""

    # ------------------------------------------------------------------
    # 1. ``strategy_state`` — ALTER add 5 columns.
    #    ``version`` is NOT NULL DEFAULT 0 so existing rows backfill to 0
    #    (optimistic-locking baseline).
    # ------------------------------------------------------------------
    op.add_column(
        "strategy_state",
        sa.Column("last_error_message", sa.Text(), nullable=True),
    )
    op.add_column(
        "strategy_state",
        sa.Column("entered_error_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "strategy_state",
        sa.Column("recovered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "strategy_state",
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "strategy_state",
        sa.Column(
            "version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )

    # 2. CHECK constraints — written as raw DDL to keep enum value list
    #    (canonical upper-case) explicit and self-documenting.
    op.execute(
        "ALTER TABLE strategy_state ADD CONSTRAINT chk_error_state "
        "CHECK (status <> 'ERROR' OR "
        "(entered_error_at IS NOT NULL AND last_error_message IS NOT NULL))"
    )
    op.execute(
        "ALTER TABLE strategy_state ADD CONSTRAINT chk_stopped_state "
        "CHECK (status <> 'STOPPED' OR stopped_at IS NOT NULL)"
    )

    # ------------------------------------------------------------------
    # 3. ``strategy_state_transitions`` — new table.
    # ------------------------------------------------------------------
    op.create_table(
        "strategy_state_transitions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "strategy_id",
            sa.String(),
            sa.ForeignKey("strategy.id"),
            nullable=False,
        ),
        sa.Column("from_status", sa.String(length=32), nullable=False),
        sa.Column("to_status", sa.String(length=32), nullable=False),
        sa.Column(
            "transitioned_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("actor", sa.String(length=64), nullable=True),
    )
    op.execute(
        "CREATE INDEX idx_sst_strategy_ts "
        "ON strategy_state_transitions(strategy_id, transitioned_at DESC)"
    )

    # ------------------------------------------------------------------
    # 4. ``daily_nav_snapshots`` — new table.
    #    NAV / drawdown / return_pct are NUMERIC (Decimal) — float is
    #    forbidden for monetary values per project rules.
    # ------------------------------------------------------------------
    op.create_table(
        "daily_nav_snapshots",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "strategy_id",
            sa.String(),
            sa.ForeignKey("strategy.id"),
            nullable=False,
        ),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("nav", sa.Numeric(28, 8), nullable=False),
        sa.Column("base_currency", sa.String(length=16), nullable=False),
        sa.Column("drawdown", sa.Numeric(10, 8), nullable=True),
        sa.Column("return_pct", sa.Numeric(10, 8), nullable=True),
        sa.Column(
            "source",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'eod_snapshot'"),
        ),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "source IN ('eod_snapshot','startup_reconcile','manual')",
            name="chk_nav_source",
        ),
        sa.UniqueConstraint(
            "strategy_id",
            "snapshot_date",
            name="uq_daily_nav_strategy_date",
        ),
    )
    op.execute(
        "CREATE INDEX idx_daily_nav_strategy_date_desc "
        "ON daily_nav_snapshots(strategy_id, snapshot_date DESC)"
    )
    op.execute(
        "COMMENT ON COLUMN daily_nav_snapshots.snapshot_date "
        "IS 'UTC date for the NAV snapshot'"
    )


def downgrade() -> None:
    """Reverse the migration 6 schema changes."""

    # 1. Drop daily_nav_snapshots (indexes + constraints drop with table).
    op.drop_table("daily_nav_snapshots")

    # 2. Drop strategy_state_transitions (idx_sst_strategy_ts drops with it).
    op.drop_table("strategy_state_transitions")

    # 3. Drop strategy_state CHECK constraints (reverse add order).
    op.execute(
        "ALTER TABLE strategy_state DROP CONSTRAINT IF EXISTS chk_stopped_state"
    )
    op.execute(
        "ALTER TABLE strategy_state DROP CONSTRAINT IF EXISTS chk_error_state"
    )

    # 4. Drop strategy_state columns (reverse add order).
    op.drop_column("strategy_state", "version")
    op.drop_column("strategy_state", "stopped_at")
    op.drop_column("strategy_state", "recovered_at")
    op.drop_column("strategy_state", "entered_error_at")
    op.drop_column("strategy_state", "last_error_message")
