"""add_gene_checkpoint_identity

Revision ID: 7a3c9e1b5d2f
Revises: c6d4e2f8a1b0
Create Date: 2026-07-24 00:00:00.000000

Give every persisted GA evaluation an explicit generation and candidate
identity so complete generations can act as atomic resume checkpoints.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "7a3c9e1b5d2f"
down_revision: Union[str, Sequence[str], None] = "c6d4e2f8a1b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "gene_records",
        "max_drawdown",
        existing_type=sa.Numeric(precision=10, scale=8),
        type_=sa.Numeric(precision=18, scale=8),
        existing_nullable=False,
    )
    op.add_column(
        "gene_records",
        sa.Column(
            "generation_index",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "gene_records",
        sa.Column("candidate_id", sa.String(length=64), nullable=True),
    )
    op.execute(
        "UPDATE gene_records "
        "SET candidate_id = 'legacy_' || CAST(id AS VARCHAR) "
        "WHERE candidate_id IS NULL"
    )
    op.alter_column("gene_records", "candidate_id", nullable=False)
    op.alter_column("gene_records", "generation_index", server_default=None)
    op.create_unique_constraint(
        "uq_gene_epoch_generation_candidate",
        "gene_records",
        ["epoch_id", "generation_index", "candidate_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_gene_epoch_generation_candidate",
        "gene_records",
        type_="unique",
    )
    op.drop_column("gene_records", "candidate_id")
    op.drop_column("gene_records", "generation_index")
    op.alter_column(
        "gene_records",
        "max_drawdown",
        existing_type=sa.Numeric(precision=18, scale=8),
        type_=sa.Numeric(precision=10, scale=8),
        existing_nullable=False,
    )
