"""add_gene_registry_and_evolution_epochs

Revision ID: b34da0d04a78
Revises: 8d1f3b6c2a4e
Create Date: 2026-05-04 00:00:00.000000

Migration 7 — Phase 0 architecture fixes (GA system schema).

Changes:
    * ``evolution_epochs`` table: append-only ledger of every GA run.
      Carries the four ``eval_*`` columns (``eval_pair``,
      ``eval_start_date``, ``eval_end_date``, ``eval_timeframe``) so a
      ``best_score`` can only be compared across epochs that share an
      evaluation context — without these the score is meaningless.
    * ``gene_records`` table: per-genotype record with role lifecycle
      (``challenger`` / ``champion`` / ``retired``) and FK to
      ``evolution_epochs``. ``score_total`` and ``max_drawdown`` use
      ``NUMERIC`` (Decimal) — float is forbidden for monetary / ratio
      values per project rules.

Indexes are created via ``op.execute()`` raw DDL because partial
indexes (``WHERE role = '...'``) and BRIN indexes are not first-class
SQLAlchemy constructs.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "b34da0d04a78"
down_revision: Union[str, Sequence[str], None] = "8d1f3b6c2a4e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply schema changes for migration 7."""

    # ------------------------------------------------------------------
    # 1. ``evolution_epochs`` — CREATE.
    #    The four ``eval_*`` columns (pair / start / end / timeframe)
    #    are mandatory: without them, ``best_score`` cannot be compared
    #    across epochs (different markets / windows produce different
    #    score distributions).
    # ------------------------------------------------------------------
    op.create_table(
        "evolution_epochs",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column(
            "strategy_id",
            sa.String(),
            sa.ForeignKey("strategy.id"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pop_size", sa.Integer(), nullable=False),
        sa.Column("max_generations", sa.Integer(), nullable=False),
        sa.Column("generations_run", sa.Integer(), nullable=True),
        sa.Column("best_score", sa.Numeric(18, 8), nullable=True),
        sa.Column("seed", sa.BigInteger(), nullable=False),
        sa.Column(
            "config_json",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'running'"),
        ),
        sa.Column("eval_pair", sa.String(length=32), nullable=False),
        sa.Column("eval_start_date", sa.Date(), nullable=False),
        sa.Column("eval_end_date", sa.Date(), nullable=False),
        sa.Column("eval_timeframe", sa.String(length=8), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('running', 'completed', 'aborted')",
            name="chk_epoch_status",
        ),
    )

    op.execute(
        "CREATE INDEX idx_evolution_epochs_strategy "
        "ON evolution_epochs(strategy_id, started_at DESC)"
    )
    # BRIN index for time-series scans — cheap on append-mostly tables.
    op.execute(
        "CREATE INDEX idx_evolution_epochs_started_brin "
        "ON evolution_epochs USING BRIN(started_at)"
    )

    # ------------------------------------------------------------------
    # 2. ``gene_records`` — CREATE.
    #    Role lifecycle: challenger -> champion -> retired. At most one
    #    champion per strategy enforced by partial unique index.
    # ------------------------------------------------------------------
    op.create_table(
        "gene_records",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "strategy_id",
            sa.String(),
            sa.ForeignKey("strategy.id"),
            nullable=False,
        ),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column(
            "param_pack",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("score_total", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "score_breakdown",
            sa.dialects.postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("max_drawdown", sa.Numeric(10, 8), nullable=False),
        sa.Column(
            "epoch_id",
            sa.String(length=64),
            sa.ForeignKey("evolution_epochs.id"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "role IN ('challenger', 'champion', 'retired')",
            name="chk_gene_role",
        ),
    )

    # Partial unique: at most one row with role='champion' per strategy.
    op.execute(
        "CREATE UNIQUE INDEX uq_one_champion_per_strategy "
        "ON gene_records(strategy_id) WHERE role = 'champion'"
    )
    op.execute(
        "CREATE INDEX idx_gene_records_strategy_role "
        "ON gene_records(strategy_id, role)"
    )
    op.execute(
        "CREATE INDEX idx_gene_records_challenger_score "
        "ON gene_records(strategy_id, score_total DESC) "
        "WHERE role = 'challenger'"
    )
    op.execute(
        "CREATE INDEX idx_gene_records_retired_timeline "
        "ON gene_records(strategy_id, retired_at DESC) "
        "WHERE role = 'retired'"
    )
    op.execute(
        "CREATE INDEX idx_gene_records_epoch ON gene_records(epoch_id)"
    )


def downgrade() -> None:
    """Reverse the migration 7 schema changes."""

    # 1. Drop ``gene_records`` first (FK -> evolution_epochs).
    #    Indexes drop with the table.
    op.drop_table("gene_records")

    # 2. Drop ``evolution_epochs``.
    op.drop_table("evolution_epochs")
