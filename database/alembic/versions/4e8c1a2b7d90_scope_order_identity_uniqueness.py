"""scope order identity uniqueness

Revision ID: 4e8c1a2b7d90
Revises: 9b7e2c4d6f10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "4e8c1a2b7d90"
down_revision: Union[str, Sequence[str], None] = "9b7e2c4d6f10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("uq_order_client_order_id", table_name="order")
    op.drop_constraint("uq_order_exchange_id", "order", type_="unique")
    identified = sa.text("account_profile IS NOT NULL AND account_id IS NOT NULL")
    legacy = sa.text("account_profile IS NULL AND account_id IS NULL")
    op.create_index(
        "uq_order_identified_client_order_id",
        "order",
        ["account_profile", "account_id", "client_order_id"],
        unique=True,
        postgresql_where=sa.and_(identified, sa.text("client_order_id IS NOT NULL")),
    )
    op.create_index(
        "uq_order_legacy_client_order_id",
        "order",
        ["client_order_id"],
        unique=True,
        postgresql_where=sa.and_(legacy, sa.text("client_order_id IS NOT NULL")),
    )
    op.create_index(
        "uq_order_identified_exchange_order_id",
        "order",
        ["exchange_id", "account_profile", "account_id", "exchange_order_id"],
        unique=True,
        postgresql_where=sa.and_(
            identified,
            sa.text("exchange_order_id IS NOT NULL"),
        ),
    )
    op.create_index(
        "uq_order_legacy_exchange_order_id",
        "order",
        ["exchange_id", "exchange_order_id"],
        unique=True,
        postgresql_where=sa.and_(legacy, sa.text("exchange_order_id IS NOT NULL")),
    )


def downgrade() -> None:
    connection = op.get_bind()
    client_collision = connection.execute(
        sa.text(
            'SELECT client_order_id FROM "order" '
            "WHERE client_order_id IS NOT NULL GROUP BY client_order_id "
            "HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    exchange_collision = connection.execute(
        sa.text(
            'SELECT exchange_id, exchange_order_id FROM "order" '
            "WHERE exchange_order_id IS NOT NULL "
            "GROUP BY exchange_id, exchange_order_id HAVING COUNT(*) > 1 LIMIT 1"
        )
    ).first()
    if client_collision is not None or exchange_collision is not None:
        raise RuntimeError("order_identity_downgrade_collision")

    for name in (
        "uq_order_identified_client_order_id",
        "uq_order_legacy_client_order_id",
        "uq_order_identified_exchange_order_id",
        "uq_order_legacy_exchange_order_id",
    ):
        op.drop_index(name, table_name="order")
    op.create_unique_constraint(
        "uq_order_exchange_id",
        "order",
        ["exchange_order_id", "exchange_id"],
    )
    op.create_index(
        "uq_order_client_order_id",
        "order",
        ["client_order_id"],
        unique=True,
        postgresql_where=sa.text("client_order_id IS NOT NULL"),
    )
