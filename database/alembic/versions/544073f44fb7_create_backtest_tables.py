"""create_backtest_tables

Revision ID: 544073f44fb7
Revises: d4b4ccb0bc41
Create Date: 2026-01-24 16:41:04.748832

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision: str = '544073f44fb7'
down_revision: Union[str, Sequence[str], None] = 'd4b4ccb0bc41'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # backtest_result_summary
    op.create_table(
        'backtest_result_summary',
        sa.Column('id', sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column('strategy_id', sa.String(), sa.ForeignKey('strategy.id'), nullable=False),
        sa.Column('start_time', sa.BigInteger(), nullable=False),
        sa.Column('end_time', sa.BigInteger(), nullable=False),
        sa.Column('total_pnl', sa.Numeric(), nullable=False),
        sa.Column('metrics_json', JSONB(), nullable=True),
    )

    # backtest_trade_log
    op.create_table(
        'backtest_trade_log',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('session_id', sa.Integer(), sa.ForeignKey('backtest_result_summary.id'), nullable=False),
        sa.Column('order_id', sa.String(), nullable=False), # No FK to order table
        sa.Column('exchange_trade_id', sa.String(), nullable=True),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), nullable=False),
        sa.Column('side', sa.String(), nullable=False),
        sa.Column('price', sa.Numeric(), nullable=False),
        sa.Column('quantity', sa.Numeric(), nullable=False),
        sa.Column('fee', sa.Numeric(), nullable=True),
        sa.Column('fee_asset', sa.String(), nullable=True),
        sa.Column('timestamp', sa.BigInteger(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table('backtest_trade_log')
    op.drop_table('backtest_result_summary')