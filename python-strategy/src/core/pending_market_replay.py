"""Pending candle replay and strategy-state reconstruction owner."""

from collections.abc import Callable, Sequence
from typing import ContextManager

from sqlalchemy.orm import Session

from src.core.live_candle_application import LiveCandleApplicationService
from src.core.models import Candlestick, Trade
from src.core.strategy_hydration_service import StrategyHydrationService
from src.strategies.base import BaseStrategy


class PendingMarketReplayService:
    """Rebuild strategy memory around one durable live-candle replay."""

    def __init__(
        self,
        *,
        db_session_factory: Callable[[], ContextManager[Session]],
        live_candle_application: LiveCandleApplicationService,
        strategy_hydration: StrategyHydrationService,
        list_active_strategies: Callable[[], Sequence[BaseStrategy]],
        publish_replacement: Callable[[BaseStrategy], None],
    ) -> None:
        self._db_session_factory = db_session_factory
        self._live_candle_application = live_candle_application
        self._strategy_hydration = strategy_hydration
        self._list_active_strategies = list_active_strategies
        self._publish_replacement = publish_replacement

    def rewind_pending(self, models: Sequence[Candlestick | Trade]) -> None:
        """Rebuild affected strategies to immediately before pending candles."""
        if not models:
            return
        candles: list[Candlestick] = []
        for model in models:
            if not isinstance(model, Candlestick):
                raise RuntimeError(
                    "pending trade replay has no durable strategy-state boundary"
                )
            candles.append(model)

        replacements: list[BaseStrategy] = []
        with self._db_session_factory() as db:
            cutoffs: dict[tuple[str, str], int] = {}
            for model in candles:
                if self._live_candle_application.was_applied(model, db=db):
                    continue
                self._live_candle_application.assert_newer(model, db=db)
                key = model.product_id, model.timeframe
                cutoffs[key] = min(cutoffs.get(key, model.timestamp), model.timestamp)
            for current in self._list_active_strategies():
                cutoff = cutoffs.get(
                    (current.product_id, current.requirements.timeframe)
                )
                if cutoff is None:
                    continue
                replacement = self._strategy_hydration.fresh_instance_for_replay(
                    current
                )
                self._strategy_hydration.warm_up(
                    db,
                    replacement,
                    before_timestamp=cutoff,
                )
                replacements.append(replacement)
        for replacement in replacements:
            self._publish_replacement(replacement)

    def rebuild_applied(self, candle: Candlestick) -> None:
        """Synchronize strategy memory without repeating candle side effects."""
        replacements: list[BaseStrategy] = []
        with self._db_session_factory() as db:
            if not self._live_candle_application.was_applied(candle, db=db):
                raise RuntimeError(
                    "cannot rebuild strategy through an unapplied candle"
                )
            for current in self._list_active_strategies():
                if (
                    current.product_id != candle.product_id
                    or current.requirements.timeframe != candle.timeframe
                ):
                    continue
                replacement = self._strategy_hydration.fresh_instance_for_replay(
                    current
                )
                self._strategy_hydration.warm_up(
                    db,
                    replacement,
                    before_timestamp=candle.timestamp + 1,
                )
                replacements.append(replacement)
        for replacement in replacements:
            self._publish_replacement(replacement)

    def replay(
        self,
        data: Candlestick | Trade,
        *,
        apply_new: Callable[[Candlestick], None],
    ) -> None:
        if not isinstance(data, Candlestick):
            raise RuntimeError(
                "pending trade replay has no durable strategy-state boundary"
            )
        self._live_candle_application.replay(
            data,
            rewind_pending=lambda candle: self.rewind_pending((candle,)),
            apply_new=apply_new,
            rebuild_applied=self.rebuild_applied,
        )
