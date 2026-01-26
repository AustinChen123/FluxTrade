"""create strategy_state table

Revision ID: 23dad9ecd748
Revises: 544073f44fb7
Create Date: 2026-01-26 02:05:48.490023

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '23dad9ecd748'
down_revision: Union[str, Sequence[str], None] = '544073f44fb7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'strategy_state',
        sa.Column('strategy_id', sa.String(), primary_key=True),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('config_json', sa.Text(), nullable=True),
        sa.Column('performance_json', sa.Text(), nullable=True),
        sa.Column('last_heartbeat', sa.BigInteger(), nullable=True),
        sa.Column('uptime_start', sa.BigInteger(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table('strategy_state')
