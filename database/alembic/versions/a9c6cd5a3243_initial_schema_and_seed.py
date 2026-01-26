"""initial_schema_and_seed

Revision ID: a9c6cd5a3243
Revises: 
Create Date: 2026-01-22 03:06:08.693469

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from sqlalchemy import String, Integer, BigInteger, Numeric, Text


# revision identifiers, used by Alembic.
revision: str = 'a9c6cd5a3243'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Exchange ---
    op.create_table(
        'exchange',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('name', sa.String(), nullable=False)
    )

    # --- Product ---
    op.create_table(
        'product',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('exchange_id', sa.String(), sa.ForeignKey('exchange.id'), nullable=False),
        sa.Column('base_asset', sa.String(), nullable=False),
        sa.Column('quote_asset', sa.String(), nullable=False)
    )

    # --- Candlestick ---
    op.create_table(
        'candlestick',
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), primary_key=True),
        sa.Column('timeframe', sa.String(), primary_key=True),
        sa.Column('timestamp', sa.BigInteger(), primary_key=True),
        sa.Column('open', sa.Numeric(), nullable=False),
        sa.Column('high', sa.Numeric(), nullable=False),
        sa.Column('low', sa.Numeric(), nullable=False),
        sa.Column('close', sa.Numeric(), nullable=False),
        sa.Column('volume', sa.Numeric(), nullable=False)
    )

    # --- Strategy ---
    op.create_table(
        'strategy',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('configuration_json', sa.Text(), nullable=True)
    )

    # --- Signal ---
    op.create_table(
        'signal',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('strategy_id', sa.String(), sa.ForeignKey('strategy.id'), nullable=False),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), nullable=False),
        sa.Column('timeframe', sa.String(), nullable=False),
        sa.Column('timestamp', sa.BigInteger(), nullable=False),
        sa.Column('type', sa.String(), nullable=False),
        sa.Column('value', sa.Numeric(), nullable=True)
    )

    # --- Order ---
    op.create_table(
        'order',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('exchange_order_id', sa.String(), nullable=True),
        sa.Column('strategy_id', sa.String(), sa.ForeignKey('strategy.id'), nullable=False),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), nullable=False),
        sa.Column('exchange_id', sa.String(), sa.ForeignKey('exchange.id'), nullable=False),
        sa.Column('type', sa.String(), nullable=False),
        sa.Column('side', sa.String(), nullable=False),
        sa.Column('price', sa.Numeric(), nullable=True),
        sa.Column('trigger_price', sa.Numeric(), nullable=True),
        sa.Column('quantity', sa.Numeric(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('timestamp', sa.BigInteger(), nullable=False),
        sa.Column('filled_quantity', sa.Numeric(), nullable=True, server_default='0'),
        sa.Column('filled_price', sa.Numeric(), nullable=True)
    )

    # --- Trade ---
    op.create_table(
        'trade',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('order_id', sa.String(), sa.ForeignKey('order.id'), nullable=False),
        sa.Column('exchange_trade_id', sa.String(), nullable=True),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), nullable=False),
        sa.Column('side', sa.String(), nullable=False),
        sa.Column('price', sa.Numeric(), nullable=False),
        sa.Column('quantity', sa.Numeric(), nullable=False),
        sa.Column('fee', sa.Numeric(), nullable=True),
        sa.Column('fee_asset', sa.String(), nullable=True),
        sa.Column('timestamp', sa.BigInteger(), nullable=False)
    )

    # --- Position ---
    op.create_table(
        'position',
        sa.Column('strategy_id', sa.String(), sa.ForeignKey('strategy.id'), primary_key=True),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), primary_key=True),
        sa.Column('side', sa.String(), primary_key=True),
        sa.Column('quantity', sa.Numeric(), nullable=False),
        sa.Column('entry_price', sa.Numeric(), nullable=False),
        sa.Column('unrealized_pnl', sa.Numeric(), nullable=False),
        sa.Column('last_update_timestamp', sa.BigInteger(), nullable=False)
    )

    # --- Seed Data ---
    exchange_table = table('exchange', column('id', String), column('name', String))
    product_table = table('product',
        column('id', String),
        column('exchange_id', String),
        column('base_asset', String),
        column('quote_asset', String)
    )
    strategy_table = table('strategy',
        column('id', String),
        column('name', String),
        column('configuration_json', String)
    )

    op.bulk_insert(exchange_table, [
        {'id': 'BINANCE', 'name': 'Binance'},
        {'id': 'BYBIT', 'name': 'Bybit'},
        {'id': 'BACKPACK', 'name': 'Backpack'},
    ])

    op.bulk_insert(product_table, [
        {'id': 'BINANCE:BTCUSDT-PERP', 'exchange_id': 'BINANCE', 'base_asset': 'BTC', 'quote_asset': 'USDT'},
        {'id': 'BYBIT:BTCUSDT-PERP', 'exchange_id': 'BYBIT', 'base_asset': 'BTC', 'quote_asset': 'USDT'},
        {'id': 'BACKPACK:BTCUSDT-PERP', 'exchange_id': 'BACKPACK', 'base_asset': 'BTC', 'quote_asset': 'USDT'},
        {'id': 'BINANCE:ETHUSDT-PERP', 'exchange_id': 'BINANCE', 'base_asset': 'ETH', 'quote_asset': 'USDT'},
        {'id': 'BYBIT:ETHUSDT-PERP', 'exchange_id': 'BYBIT', 'base_asset': 'ETH', 'quote_asset': 'USDT'},
    ])

    op.bulk_insert(strategy_table, [
        {'id': 'strategy_1', 'name': 'Trend Following', 'configuration_json': '{}'},
        {'id': 'strategy_2', 'name': 'Mean Reversion', 'configuration_json': '{}'},
    ])


def downgrade() -> None:
    op.drop_table('position')
    op.drop_table('trade')
    op.drop_table('order')
    op.drop_table('signal')
    op.drop_table('candlestick')
    op.drop_table('product')
    op.drop_table('strategy')
    op.drop_table('exchange')