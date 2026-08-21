"""Warm up and validate strategy state before runtime exposure."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

from sqlalchemy.orm import Session

from src.core.models import Candlestick
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.risk_manager import AccountService
from src.core.signal_processor import SignalProcessor
from src.strategies.base import BaseStrategy


class StrategyHydrationService:
    """Rebuild one strategy and synchronize its authoritative position state."""

    def __init__(
        self,
        *,
        signal_processor: SignalProcessor,
        account_service: AccountService,
    ) -> None:
        self._signal_processor = signal_processor
        self._account_service = account_service

    def sync_position_state(self, instance: BaseStrategy) -> None:
        try:
            position = self._account_service.get_position(
                instance.strategy_id,
                instance.product_id,
            )
        except Exception as error:
            raise RuntimeError(
                "position_state_sync_failed: "
                f"strategy_id={instance.strategy_id} error={error}"
            ) from error
        position_side = (
            None if position is None else getattr(position.side, "value", position.side)
        )
        applied = self._signal_processor.set_position_state(instance, position_side)
        if position_side is not None and not applied:
            raise RuntimeError(
                "position_state_sync_unsupported: "
                f"strategy_id={instance.strategy_id} side={position_side}"
            )

    def warm_up(
        self,
        db: Session,
        instance: BaseStrategy,
        *,
        before_timestamp: int | None = None,
    ) -> int:
        """Replay recent candles without emitting signals, then sync position."""
        requirements = instance.requirements
        lookback = max(int(requirements.lookback_window), 0)
        if lookback == 0:
            self.sync_position_state(instance)
            return 0

        query = db.query(ORMCandlestick).filter(
            ORMCandlestick.product_id == requirements.product_id,
            ORMCandlestick.timeframe == requirements.timeframe,
        )
        if before_timestamp is not None:
            query = query.filter(ORMCandlestick.timestamp < before_timestamp)
        rows = query.order_by(ORMCandlestick.timestamp.desc()).limit(lookback).all()
        rows = sorted(rows, key=lambda row: cast(int, row.timestamp))
        if len(rows) < lookback:
            raise RuntimeError(
                "warmup_insufficient_candles: "
                f"strategy_id={instance.strategy_id} "
                f"available={len(rows)} required={lookback}"
            )
        candles = [
            Candlestick(
                product_id=cast(str, row.product_id),
                timeframe=cast(str, row.timeframe),
                timestamp=cast(int, row.timestamp),
                open=cast(Decimal, row.open),
                high=cast(Decimal, row.high),
                low=cast(Decimal, row.low),
                close=cast(Decimal, row.close),
                volume=cast(Decimal, row.volume),
            )
            for row in rows
        ]
        self._signal_processor.warm_up(instance, candles)
        self.sync_position_state(instance)
        return len(candles)

    @staticmethod
    def fresh_instance_for_replay(current: BaseStrategy) -> BaseStrategy:
        current_configuration = current.replay_configuration()
        replacement = current.fresh_instance_for_replay()
        if (
            replacement is current
            or type(replacement) is not type(current)
            or replacement.strategy_id != current.strategy_id
            or replacement.product_id != current.product_id
            or replacement.requirements != current.requirements
            or replacement.replay_configuration() != current_configuration
        ):
            raise RuntimeError(
                "strategy recovery factory did not return a distinct "
                "compatible instance: "
                f"{current.strategy_id}"
            )
        return replacement
