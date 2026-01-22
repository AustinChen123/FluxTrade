"""add_signal_audit_table

Revision ID: d4b4ccb0bc41
Revises: a9c6cd5a3243
Create Date: 2026-01-22 23:20:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'd4b4ccb0bc41'
down_revision: Union[str, Sequence[str], None] = 'a9c6cd5a3243'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_table(
        'signal_audit',
        sa.Column('id', sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column('timestamp', sa.BigInteger(), nullable=False),
        sa.Column('strategy_id', sa.String(), nullable=False),
        sa.Column('product_id', sa.String(), nullable=False),
        sa.Column('signal_type', sa.String(), nullable=False),
        sa.Column('risk_status', sa.String(), nullable=False),
        sa.Column('risk_message', sa.Text(), nullable=True),
        sa.Column('order_id', sa.String(), nullable=True),
        sa.Column('details_json', sa.Text(), nullable=True)
    )

def downgrade() -> None:
    op.drop_table('signal_audit')