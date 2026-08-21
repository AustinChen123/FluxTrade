"""Canonical live-candle application and replay persistence owner."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from contextlib import contextmanager
from decimal import Decimal
from typing import ContextManager, Iterator

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.core.models import Candlestick
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.orm_models import MarketDataApplication
from src.core.product_master import ensure_product_registered


LIVE_CANDLE_FENCE_TIMEOUT_SECONDS = 5.0
CandleCallback = Callable[[Candlestick], None]


class LiveCandleApplicationService:
    """Apply each canonical candle once behind its durable receipt fence."""

    def __init__(
        self,
        *,
        environment_identity: Callable[[], str],
        db_session_factory: Callable[[], ContextManager[Session]],
    ) -> None:
        self._environment_identity = environment_identity
        self._db_session_factory = db_session_factory

    @staticmethod
    def _candle_values(candle: Candlestick) -> tuple[Decimal, ...]:
        return candle.open, candle.high, candle.low, candle.close, candle.volume

    @staticmethod
    def _persisted_candle_values(candle: ORMCandlestick) -> tuple[Decimal, ...]:
        return (
            Decimal(str(candle.open)),
            Decimal(str(candle.high)),
            Decimal(str(candle.low)),
            Decimal(str(candle.close)),
            Decimal(str(candle.volume)),
        )

    @staticmethod
    def _application_values(
        application: MarketDataApplication,
    ) -> tuple[Decimal, ...]:
        return (
            Decimal(str(application.open)),
            Decimal(str(application.high)),
            Decimal(str(application.low)),
            Decimal(str(application.close)),
            Decimal(str(application.volume)),
        )

    def _application_identity(self, candle: Candlestick) -> tuple[str, str, str, int]:
        return (
            self._environment_identity(),
            candle.product_id,
            candle.timeframe,
            candle.timestamp,
        )

    def was_applied(
        self,
        candle: Candlestick,
        *,
        db: Session | None = None,
    ) -> bool:
        if self._environment_identity() != "live":
            return False
        if db is None:
            with self._db_session_factory() as owned_db:
                return self.was_applied(candle, db=owned_db)

        application = db.get(
            MarketDataApplication,
            self._application_identity(candle),
        )
        if application is None:
            return False
        if self._application_values(application) != self._candle_values(candle):
            raise RuntimeError(
                "live application receipt conflicts with market payload: "
                f"{candle.product_id}:{candle.timeframe}:{candle.timestamp}"
            )
        persisted = db.get(
            ORMCandlestick,
            (candle.product_id, candle.timeframe, candle.timestamp),
        )
        if persisted is None or self._persisted_candle_values(
            persisted
        ) != self._candle_values(candle):
            raise RuntimeError(
                "live application receipt has no matching canonical candle: "
                f"{candle.product_id}:{candle.timeframe}:{candle.timestamp}"
            )
        return True

    def assert_newer(
        self,
        candle: Candlestick,
        *,
        db: Session | None = None,
    ) -> None:
        if self._environment_identity() != "live":
            return
        if db is None:
            with self._db_session_factory() as owned_db:
                self.assert_newer(candle, db=owned_db)
            return
        latest_timestamp = (
            db.query(func.max(MarketDataApplication.timestamp))
            .filter(
                MarketDataApplication.environment == self._environment_identity(),
                MarketDataApplication.product_id == candle.product_id,
                MarketDataApplication.timeframe == candle.timeframe,
            )
            .scalar()
        )
        if latest_timestamp is not None and candle.timestamp <= int(latest_timestamp):
            raise RuntimeError(
                "live candle application is out of order: "
                f"{candle.product_id}:{candle.timeframe}:"
                f"latest={latest_timestamp}:received={candle.timestamp}"
            )

    def _assert_compatible(self, candle: Candlestick) -> None:
        if self._environment_identity() != "live":
            return
        with self._db_session_factory() as db:
            existing = db.get(
                ORMCandlestick,
                (candle.product_id, candle.timeframe, candle.timestamp),
            )
            if existing is None:
                return
            if self._persisted_candle_values(existing) != self._candle_values(candle):
                raise RuntimeError(
                    "live candle conflicts with canonical history: "
                    f"{candle.product_id}:{candle.timeframe}:{candle.timestamp}"
                )

    def _persist(self, candle: Candlestick) -> None:
        if self._environment_identity() != "live":
            return
        with self._db_session_factory() as db:
            try:
                ensure_product_registered(db, candle.product_id)
                identity = candle.product_id, candle.timeframe, candle.timestamp
                existing = db.get(ORMCandlestick, identity)
                if existing is None:
                    db.add(
                        ORMCandlestick(
                            product_id=candle.product_id,
                            timeframe=candle.timeframe,
                            timestamp=candle.timestamp,
                            open=candle.open,
                            high=candle.high,
                            low=candle.low,
                            close=candle.close,
                            volume=candle.volume,
                        )
                    )
                elif self._persisted_candle_values(existing) != self._candle_values(
                    candle
                ):
                    raise RuntimeError(
                        "live candle conflicts with canonical history: "
                        f"{candle.product_id}:{candle.timeframe}:{candle.timestamp}"
                    )
                application = db.get(
                    MarketDataApplication,
                    self._application_identity(candle),
                )
                if application is not None:
                    raise RuntimeError(
                        "live application receipt appeared concurrently: "
                        f"{candle.product_id}:{candle.timeframe}:{candle.timestamp}"
                    )
                db.add(
                    MarketDataApplication(
                        environment=self._environment_identity(),
                        product_id=candle.product_id,
                        timeframe=candle.timeframe,
                        timestamp=candle.timestamp,
                        open=candle.open,
                        high=candle.high,
                        low=candle.low,
                        close=candle.close,
                        volume=candle.volume,
                    )
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

    @contextmanager
    def application_fence(self, candle: Candlestick) -> Iterator[None]:
        if self._environment_identity() != "live":
            yield
            return
        lock_material = (
            f"{self._environment_identity()}\0{candle.product_id}\0"
            f"{candle.timeframe}\0{candle.timestamp}"
        ).encode()
        lock_key = int.from_bytes(
            hashlib.sha256(lock_material).digest()[:8],
            byteorder="big",
            signed=True,
        )
        with self._db_session_factory() as db:
            dialect = db.get_bind().dialect.name
            if dialect == "sqlite":
                yield
                return
            if dialect != "postgresql":
                raise RuntimeError(
                    "live market recovery requires PostgreSQL advisory locks"
                )
            deadline = time.monotonic() + LIVE_CANDLE_FENCE_TIMEOUT_SECONDS
            while not db.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": lock_key},
            ).scalar():
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        "timed out acquiring live candle application fence"
                    )
                time.sleep(0.05)
            try:
                yield
            finally:
                unlocked = db.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": lock_key},
                ).scalar()
                if unlocked is not True:
                    raise RuntimeError(
                        "failed to release live candle application fence"
                    )

    def apply(
        self,
        candle: Candlestick,
        *,
        apply_new: CandleCallback,
        rebuild_applied: CandleCallback,
    ) -> None:
        if self.was_applied(candle):
            rebuild_applied(candle)
            return
        self.assert_newer(candle)
        self._assert_compatible(candle)
        apply_new(candle)
        self._persist(candle)

    def replay(
        self,
        candle: Candlestick,
        *,
        rewind_pending: CandleCallback,
        apply_new: CandleCallback,
        rebuild_applied: CandleCallback,
    ) -> None:
        with self.application_fence(candle):
            if self.was_applied(candle):
                rebuild_applied(candle)
                return
            rewind_pending(candle)
            self.apply(
                candle,
                apply_new=apply_new,
                rebuild_applied=rebuild_applied,
            )
