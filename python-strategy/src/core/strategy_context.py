"""Read-only strategy decision context snapshots.

The context is built after market data has matched existing orders and before
the strategy emits the next intent. Strategies may inspect it, but account
state remains owned by the adapter/matcher/risk layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from src.core.models import OrderSide, PositionSide


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    """Per-strategy/per-product position view for strategy decisions."""

    side: PositionSide
    quantity: Decimal
    average_entry_price: Decimal
    mark_price: Decimal | None
    notional: Decimal | None
    unrealized_pnl: Decimal


@dataclass(frozen=True, slots=True)
class OrderSnapshot:
    """Open order view exposed to strategy decision code."""

    id: str
    product_id: str
    side: OrderSide
    order_type: str
    quantity: Decimal
    timestamp: int
    price: Decimal | None = None
    status: str | None = None


@dataclass(frozen=True, slots=True)
class FillSnapshot:
    """Latest fill view exposed to strategy decision code."""

    order_id: str
    product_id: str
    side: OrderSide
    price: Decimal
    quantity: Decimal
    fee: Decimal
    timestamp: int


@dataclass(frozen=True, slots=True)
class RejectionSnapshot:
    """Latest order/risk rejection view exposed to strategy decision code."""

    reason: str
    timestamp: int
    order_id: str | None = None


@dataclass(frozen=True, slots=True)
class RiskSnapshot:
    """Minimal read-only risk state for strategy decisions."""

    trading_enabled: bool = True
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Read-only account/position/risk snapshot for one strategy decision."""

    strategy_id: str
    product_id: str
    timestamp: int
    available_cash: Decimal
    total_equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    current_drawdown: Decimal
    max_drawdown: Decimal
    position: PositionSnapshot | None = None
    open_orders: tuple[OrderSnapshot, ...] = ()
    latest_fills: tuple[FillSnapshot, ...] = ()
    latest_rejections: tuple[RejectionSnapshot, ...] = ()
    risk: RiskSnapshot = RiskSnapshot()
