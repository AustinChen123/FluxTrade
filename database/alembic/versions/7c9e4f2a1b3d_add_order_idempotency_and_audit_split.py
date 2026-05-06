"""add_order_idempotency_and_audit_split

Revision ID: 7c9e4f2a1b3d
Revises: 23dad9ecd748
Create Date: 2026-05-04 00:00:00.000000

Migration 5 — Phase 0 architecture fixes.

Changes:
    * ``order`` table: add 5 idempotency / lifecycle columns and supporting
      indexes (incl. partial unique on ``client_order_id``).
    * ``signal_audit`` table: pre-validate ``details_json`` then upgrade to
      JSONB; add 4 columns for Path B audit linkage.
    * ``system_events`` table: new table for cross-cutting system events
      (reconcile / gene_promote / gene_retire / system_error) with FK
      back-references and CHECK constraint.

The ``related_order_id`` column intentionally uses ``VARCHAR`` (not
``BIGINT`` as a stricter reading of the plan implied) because ``order.id``
in this codebase is a string-typed primary key (see rev
``a9c6cd5a3243``); a BIGINT FK would fail to be created.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from src.core.migration_validators import null_invalid_json_in_text_column


# revision identifiers, used by Alembic.
revision: str = "7c9e4f2a1b3d"
down_revision: Union[str, Sequence[str], None] = "23dad9ecd748"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply schema changes for migration 5."""

    bind = op.get_bind()

    # ------------------------------------------------------------------
    # 1. Pre-validate signal_audit.details_json before TEXT -> JSONB cast.
    #    Any non-JSON row is set to NULL so the ALTER COLUMN below cannot
    #    abort the transaction with an "invalid input syntax for jsonb"
    #    error.
    # ------------------------------------------------------------------
    null_invalid_json_in_text_column(bind, "signal_audit", "details_json")

    # ------------------------------------------------------------------
    # 2. ``order`` table — add 5 idempotency / lifecycle columns.
    # ------------------------------------------------------------------
    op.add_column(
        "order",
        sa.Column("client_order_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "order",
        sa.Column("intent_payload", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "order",
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "order",
        sa.Column("acked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "order",
        sa.Column("last_reconciled_at", sa.DateTime(timezone=True), nullable=True),
    )

    # 3. ``order`` indexes (table is a reserved word, hence quoted).
    op.execute(
        'CREATE UNIQUE INDEX uq_order_client_order_id '
        'ON "order"(client_order_id) '
        'WHERE client_order_id IS NOT NULL'
    )
    op.execute(
        'CREATE INDEX idx_order_client_order_id '
        'ON "order"(client_order_id)'
    )
    op.execute(
        'CREATE INDEX idx_order_strategy_status '
        'ON "order"(strategy_id, status)'
    )

    # ------------------------------------------------------------------
    # 4. ``signal_audit`` — TEXT -> JSONB on details_json (data is clean).
    # ------------------------------------------------------------------
    op.alter_column(
        "signal_audit",
        "details_json",
        existing_type=sa.Text(),
        type_=postgresql.JSONB(),
        existing_nullable=True,
        postgresql_using="details_json::jsonb",
    )

    # 5. ``signal_audit`` — add 4 Path B / batch columns.
    op.add_column(
        "signal_audit",
        sa.Column("client_order_id", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "signal_audit",
        sa.Column("intent_payload", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "signal_audit",
        sa.Column("outcome_payload", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "signal_audit",
        sa.Column("signal_batch_id", sa.String(length=64), nullable=True),
    )

    # 6. ``signal_audit`` indexes.
    op.execute(
        "CREATE INDEX idx_signal_audit_client_order_id "
        "ON signal_audit(client_order_id) "
        "WHERE client_order_id IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX idx_signal_audit_strategy_ts "
        "ON signal_audit(strategy_id, timestamp DESC)"
    )
    op.execute(
        "CREATE INDEX idx_signal_audit_batch_id "
        "ON signal_audit(signal_batch_id) "
        "WHERE signal_batch_id IS NOT NULL"
    )

    # ------------------------------------------------------------------
    # 7. ``system_events`` — new table.
    #    related_order_id is VARCHAR because order.id is a string PK in
    #    this codebase (see rev a9c6cd5a3243).
    # ------------------------------------------------------------------
    op.create_table(
        "system_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event_subtype", sa.String(length=64), nullable=True),
        sa.Column(
            "related_strategy_id",
            sa.String(),
            sa.ForeignKey("strategy.id"),
            nullable=True,
        ),
        sa.Column(
            "related_order_id",
            sa.String(),
            sa.ForeignKey("order.id"),
            nullable=True,
        ),
        # No FK — gene_records table is created in rev 7.
        sa.Column("related_gene_id", sa.BigInteger(), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "event_type IN ('reconcile','gene_promote','gene_retire','system_error')",
            name="chk_system_events_type",
        ),
    )

    # 8. ``system_events`` indexes.
    op.execute(
        "CREATE INDEX idx_system_events_type_created "
        "ON system_events(event_type, created_at DESC)"
    )
    op.execute(
        "CREATE INDEX idx_system_events_strategy_created "
        "ON system_events(related_strategy_id, created_at DESC) "
        "WHERE related_strategy_id IS NOT NULL"
    )


def downgrade() -> None:
    """Reverse the migration 5 schema changes."""

    # ------------------------------------------------------------------
    # 1. Drop system_events (indexes drop with the table).
    # ------------------------------------------------------------------
    op.drop_table("system_events")

    # ------------------------------------------------------------------
    # 2. Drop signal_audit indexes added in upgrade().
    # ------------------------------------------------------------------
    op.execute("DROP INDEX IF EXISTS idx_signal_audit_batch_id")
    op.execute("DROP INDEX IF EXISTS idx_signal_audit_strategy_ts")
    op.execute("DROP INDEX IF EXISTS idx_signal_audit_client_order_id")

    # 3. Drop signal_audit columns added in upgrade() (reverse order).
    op.drop_column("signal_audit", "signal_batch_id")
    op.drop_column("signal_audit", "outcome_payload")
    op.drop_column("signal_audit", "intent_payload")
    op.drop_column("signal_audit", "client_order_id")

    # 4. Revert details_json JSONB -> TEXT.
    op.alter_column(
        "signal_audit",
        "details_json",
        existing_type=postgresql.JSONB(),
        type_=sa.Text(),
        existing_nullable=True,
        postgresql_using="details_json::text",
    )

    # ------------------------------------------------------------------
    # 5. Drop order indexes added in upgrade().
    # ------------------------------------------------------------------
    op.execute("DROP INDEX IF EXISTS idx_order_strategy_status")
    op.execute("DROP INDEX IF EXISTS idx_order_client_order_id")
    op.execute("DROP INDEX IF EXISTS uq_order_client_order_id")

    # 6. Drop order columns added in upgrade() (reverse order).
    op.drop_column("order", "last_reconciled_at")
    op.drop_column("order", "acked_at")
    op.drop_column("order", "submitted_at")
    op.drop_column("order", "intent_payload")
    op.drop_column("order", "client_order_id")
