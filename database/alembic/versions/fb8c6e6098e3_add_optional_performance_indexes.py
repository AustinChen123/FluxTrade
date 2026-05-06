"""add_optional_performance_indexes

Revision ID: fb8c6e6098e3
Revises: b34da0d04a78
Create Date: 2026-05-07 00:00:00.000000

Migration 8 — Phase 0 architecture fixes (optional performance indexes).

Changes:
    * ``candlestick``: add composite index ``idx_candlestick_product_tf_ts``
      on ``(product_id, timeframe, timestamp DESC)`` to accelerate the
      common "latest N bars for a product/timeframe" query path used by
      backtest data sources and the live engine warm-up.
    * ``backtest_trade_log``: add partial index
      ``idx_backtest_trade_log_strategy`` on
      ``(strategy_id, timestamp DESC) WHERE strategy_id IS NOT NULL``.
      ``strategy_id`` is nullable (legacy rows may lack it); a partial
      index keeps the index small and avoids indexing NULL keys we never
      query for.

These are pure additive performance indexes — no schema changes, no ORM
changes. Indexes are created via ``op.execute()`` raw DDL because the
DESC ordering hint and the partial ``WHERE`` clause are not first-class
SQLAlchemy constructs.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "fb8c6e6098e3"
down_revision: Union[str, Sequence[str], None] = "b34da0d04a78"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create optional performance indexes."""
    # candlestick: composite index for (product_id, timeframe, ts DESC) lookups.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candlestick_product_tf_ts
            ON candlestick (product_id, timeframe, "timestamp" DESC)
        """
    )

    # backtest_trade_log: partial index — strategy_id is nullable, only index
    # rows that actually carry a strategy_id (the queried subset).
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_trade_log_strategy
            ON backtest_trade_log (strategy_id, "timestamp" DESC)
            WHERE strategy_id IS NOT NULL
        """
    )


def downgrade() -> None:
    """Drop optional performance indexes."""
    op.execute("DROP INDEX IF EXISTS idx_backtest_trade_log_strategy")
    op.execute("DROP INDEX IF EXISTS idx_candlestick_product_tf_ts")
