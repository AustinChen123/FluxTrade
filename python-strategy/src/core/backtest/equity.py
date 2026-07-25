"""Shared mark-to-market portfolio equity calculation for backtest runners."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Protocol

from src.core.models import Position, PositionSide


class PortfolioEquityAdapter(Protocol):
    @property
    def supports_strategy_positions(self) -> bool: ...

    def get_balance(self, asset: str = "USDT") -> Decimal: ...

    def get_position(
        self,
        product_id: str,
        strategy_id: str | None = None,
    ) -> Position | None: ...


def portfolio_equity(
    adapter: PortfolioEquityAdapter,
    *,
    strategy_ids: Sequence[str],
    product_id: str,
    mark_price: Decimal,
    contract_multiplier: Decimal,
) -> Decimal:
    """Return cash plus unrealized PnL for every strategy-scoped position."""
    unique_strategy_ids = tuple(dict.fromkeys(strategy_ids))
    require_strategy_position_scope(adapter, unique_strategy_ids)
    supports_scoped_positions = adapter.supports_strategy_positions

    if supports_scoped_positions:
        positions = (
            adapter.get_position(product_id, strategy_id=strategy_id)
            for strategy_id in unique_strategy_ids
        )
    else:
        positions = (adapter.get_position(product_id),)

    unrealized_pnl = Decimal("0")
    for position in positions:
        if position is None:
            continue
        if position.side == PositionSide.LONG:
            direction = Decimal("1")
        elif position.side == PositionSide.SHORT:
            direction = Decimal("-1")
        else:
            continue
        unrealized_pnl += (
            (mark_price - position.entry_price)
            * position.quantity
            * contract_multiplier
            * direction
        )
    return adapter.get_balance() + unrealized_pnl


def require_strategy_position_scope(
    adapter: PortfolioEquityAdapter,
    strategy_ids: Sequence[str],
) -> None:
    unique_strategy_ids = tuple(dict.fromkeys(strategy_ids))
    if len(unique_strategy_ids) > 1 and not adapter.supports_strategy_positions:
        raise RuntimeError(
            "multi-strategy portfolio equity requires strategy-scoped positions"
        )
