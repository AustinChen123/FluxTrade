"""add_rithmic_order_account_identity

Revision ID: c6d4e2f8a1b0
Revises: 4f7a2c9d1e6b
Create Date: 2026-07-21 00:00:00.000000

Persist the Rithmic profile and account ID selected before live submission.
Legacy and non-Rithmic rows remain nullable and are never guessed during
recovery.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c6d4e2f8a1b0"
down_revision: Union[str, Sequence[str], None] = "4f7a2c9d1e6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "order",
        sa.Column("account_profile", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "order",
        sa.Column("account_id", sa.String(length=128), nullable=True),
    )

    op.create_check_constraint(
        "chk_order_account_identity_complete",
        "order",
        "(account_profile IS NULL AND account_id IS NULL) OR "
        "(account_profile IS NOT NULL AND account_id IS NOT NULL AND "
        "TRIM(account_profile) <> '' AND TRIM(account_id) <> '')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "chk_order_account_identity_complete",
        "order",
        type_="check",
    )
    op.drop_column("order", "account_id")
    op.drop_column("order", "account_profile")
