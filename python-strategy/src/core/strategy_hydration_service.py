"""Warm up and validate strategy state before runtime exposure."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

from sqlalchemy.orm import Session

from src.core.data_provider import timeframe_to_ms
from src.core.models import Candlestick
from src.core.product_registry import validate_product_id
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.risk_manager import AccountService
from src.core.signal_processor import SignalProcessor, StrategyDecisionScopeLoader
from src.strategies.base import BaseStrategy, StrategyRequirements


class FixedCandleWindowError(ValueError):
    def __init__(self) -> None:
        super().__init__("FIXED_CANDLE_WINDOW_INVALID")


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
        decision_scope_loader: StrategyDecisionScopeLoader | None = None,
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
        if decision_scope_loader is None:
            self._signal_processor.warm_up(instance, candles)
        else:
            self._signal_processor.warm_up(
                instance,
                candles,
                decision_scope_loader=decision_scope_loader,
            )
        self.sync_position_state(instance)
        return len(candles)

    def hydrate_candles(
        self,
        instance: BaseStrategy,
        candles: tuple[Candlestick, ...],
        *,
        decision_scope_loader: StrategyDecisionScopeLoader,
    ) -> int:
        """Replay supplied candles once; caller exposes the instance only on success.

        Failure may leave this private instance partially hydrated. No publication,
        rollback, retry, or authoritative-history claim is made by this method.
        """
        if (
            not callable(decision_scope_loader)
            or type(candles) is not tuple
            or any(type(c) is not Candlestick for c in candles)
        ):
            raise ValueError("invalid hydration candles")
        self._signal_processor.warm_up(
            instance, list(candles), decision_scope_loader=decision_scope_loader
        )
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

    def read_fixed_candles(
        self,
        db: Session,
        requirements: StrategyRequirements,
        *,
        cutover_ms: int,
        max_seed_candles: int,
    ) -> tuple[Candlestick, ...]:
        """Read only the caller-fixed completed window; never backfill a gap."""
        try:
            if type(requirements) is not StrategyRequirements:
                raise ValueError
            lookback = requirements.lookback_window
            for value, minimum in (
                (lookback, 0),
                (cutover_ms, 0),
                (max_seed_candles, 1),
            ):
                if type(value) is not int or not minimum <= value <= (1 << 63) - 1:
                    raise ValueError
            if (
                lookback > max_seed_candles
                or type(requirements.product_id) is not str
                or validate_product_id(requirements.product_id)
                != requirements.product_id
                or type(requirements.timeframe) is not str
            ):
                raise ValueError
            duration = timeframe_to_ms(requirements.timeframe)
            if type(duration) is not int or not 1 <= duration <= (1 << 63) - 1:
                raise ValueError
            start = cutover_ms - lookback * duration
            if duration <= 0 or cutover_ms % duration or start < 0:
                raise ValueError
        except (ValueError, TypeError, OverflowError, IndexError):
            raise FixedCandleWindowError() from None
        if lookback == 0:
            return ()
        with db.no_autoflush:
            rows = (
                db.query(
                    ORMCandlestick.product_id,
                    ORMCandlestick.timeframe,
                    ORMCandlestick.timestamp,
                    ORMCandlestick.open,
                    ORMCandlestick.high,
                    ORMCandlestick.low,
                    ORMCandlestick.close,
                    ORMCandlestick.volume,
                )
                .filter(
                    ORMCandlestick.product_id == requirements.product_id,
                    ORMCandlestick.timeframe == requirements.timeframe,
                    ORMCandlestick.timestamp >= start,
                    ORMCandlestick.timestamp < cutover_ms,
                )
                .order_by(ORMCandlestick.timestamp.asc())
                .limit(lookback + 1)
                .all()
            )
        if len(rows) != lookback or any(
            type(row.timestamp) is not int or row.timestamp != start + i * duration
            for i, row in enumerate(rows)
        ):
            raise FixedCandleWindowError()
        return tuple(
            Candlestick(
                product_id=row.product_id,
                timeframe=row.timeframe,
                timestamp=row.timestamp,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
            )
            for row in rows
        )
