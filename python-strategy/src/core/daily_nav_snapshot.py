"""Daily NAV snapshot helpers for risk management."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import date
from decimal import Decimal
from typing import Callable

from sqlalchemy.orm import Session

from src.core.orm_models import DailyNavSnapshot


class DailyNavSnapshotService:
    """Read and initialize per-strategy daily NAV snapshots."""

    def __init__(self, db_session_factory: Callable[[], AbstractContextManager[Session]]) -> None:
        self._db_session_factory = db_session_factory

    def get_start_nav(self, strategy_id: str, snapshot_date: date) -> Decimal | None:
        with self._db_session_factory() as db:
            snapshot = (
                db.query(DailyNavSnapshot)
                .filter_by(strategy_id=strategy_id, snapshot_date=snapshot_date)
                .first()
            )
            if snapshot is None:
                return None
            return Decimal(snapshot.nav)

    def ensure_snapshot(
        self,
        strategy_id: str,
        snapshot_date: date,
        nav: Decimal,
        *,
        base_currency: str = "USDT",
        source: str = "startup_reconcile",
        notes: str | None = None,
    ) -> Decimal:
        with self._db_session_factory() as db:
            snapshot = (
                db.query(DailyNavSnapshot)
                .filter_by(strategy_id=strategy_id, snapshot_date=snapshot_date)
                .first()
            )
            if snapshot is not None:
                return Decimal(snapshot.nav)

            db.add(
                DailyNavSnapshot(
                    strategy_id=strategy_id,
                    snapshot_date=snapshot_date,
                    nav=nav,
                    base_currency=base_currency,
                    source=source,
                    notes=notes,
                )
            )
            db.commit()
            return nav
