from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
import pandas as pd
from src.core.models import Candlestick, Signal, Trade
from src.core.journal import StrategyJournal
from src.core.strategy_context import StrategyContext


@dataclass
class StrategyRequirements:
    product_id: str
    timeframe: str
    lookback_window: int


class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, product_id: str):
        self.strategy_id = strategy_id
        self.product_id = product_id
        self.journal = StrategyJournal(strategy_id)

    @property
    @abstractmethod
    def requirements(self) -> StrategyRequirements:
        """
        Define data requirements for the strategy.
        """
        pass

    @abstractmethod
    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | list[Signal] | None:
        """
        Process a new candlestick and optionally return a trading signal.

        The long-term event-driven contract is candle + read-only decision
        context. Existing strategies may ignore context during migration.
        """
        pass

    def on_trade(self, trade: Trade) -> Optional[Signal]:
        """Optional: Strategies can override to react to individual trades."""
        return None

    def sync_position_state(self, position_side: str | None) -> bool | None:
        """Optional hook to align internal trade-state after restart warm-up.

        Return True when the position state was consumed, False when the side
        is UNSUPPORTED by this strategy (activation fails closed), or None
        (default) to let the engine try the generic attribute fallback.
        """
        return None

    def fresh_instance_for_replay(self) -> "BaseStrategy":
        """Create an empty instance with the same runtime configuration."""
        raise NotImplementedError(
            "strategy does not define a pending-market replay factory"
        )

    def replay_configuration(self) -> object:
        """Return a stable, equality-comparable recovery configuration."""
        raise NotImplementedError(
            "strategy does not define a pending-market replay configuration"
        )

    def snapshot_walk_forward_trade_state(self) -> object:
        """Capture all state that a warm-up replay must not carry into scoring.

        Walk-forward capable strategies must override this together with
        ``restore_walk_forward_trade_state``. Indicator and feature state is
        intentionally excluded so it remains warmed at the scoring boundary.
        """
        raise NotImplementedError(
            "strategy does not define walk-forward trade-state isolation"
        )

    def restore_walk_forward_trade_state(self, state: object) -> None:
        """Restore the complete trade-state snapshot captured before warm-up."""
        raise NotImplementedError(
            "strategy does not define walk-forward trade-state isolation"
        )

    def run_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run strategy in vectorized mode using Pandas.
        Expected to return DataFrame with 'signal' column.
        """
        raise NotImplementedError("Vectorized execution not implemented")
