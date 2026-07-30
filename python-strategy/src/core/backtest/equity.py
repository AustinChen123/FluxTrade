"""Shared mark-to-market portfolio equity calculation for backtest runners."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, cast

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


@dataclass(frozen=True, slots=True)
class PortfolioEquityCalculator:
    """Repeated mark-to-market calculation with a validated strategy scope."""

    adapter: PortfolioEquityAdapter
    strategy_ids: Sequence[str]
    product_id: str
    contract_multiplier: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "strategy_ids",
            require_strategy_position_scope(self.adapter, self.strategy_ids),
        )

    def value(self, mark_price: Decimal) -> Decimal:
        supports_scoped_positions = self.adapter.supports_strategy_positions
        if supports_scoped_positions:
            batch_loader = getattr(
                self.adapter,
                "get_strategy_positions",
                None,
            )
            if callable(batch_loader):
                positions = cast(
                    Iterable[Position | None],
                    batch_loader(
                        self.product_id,
                        self.strategy_ids,
                    ),
                )
            else:
                positions = (
                    self.adapter.get_position(
                        self.product_id,
                        strategy_id=strategy_id,
                    )
                    for strategy_id in self.strategy_ids
                )
        else:
            positions = (self.adapter.get_position(self.product_id),)

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
                * self.contract_multiplier
                * direction
            )
        return self.adapter.get_balance() + unrealized_pnl


def portfolio_equity(
    adapter: PortfolioEquityAdapter,
    *,
    strategy_ids: Sequence[str],
    product_id: str,
    mark_price: Decimal,
    contract_multiplier: Decimal,
) -> Decimal:
    """Return cash plus unrealized PnL for every strategy-scoped position."""
    return PortfolioEquityCalculator(
        adapter=adapter,
        strategy_ids=strategy_ids,
        product_id=product_id,
        contract_multiplier=contract_multiplier,
    ).value(mark_price)


def require_strategy_position_scope(
    adapter: PortfolioEquityAdapter,
    strategy_ids: Sequence[str],
) -> tuple[str, ...]:
    unique_strategy_ids = tuple(dict.fromkeys(strategy_ids))
    if len(unique_strategy_ids) > 1 and not adapter.supports_strategy_positions:
        raise RuntimeError(
            "multi-strategy portfolio equity requires strategy-scoped positions"
        )
    return unique_strategy_ids
