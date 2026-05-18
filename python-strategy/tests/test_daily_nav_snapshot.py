"""Tests for daily NAV snapshot service."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date
from decimal import Decimal

from src.core.daily_nav_snapshot import DailyNavSnapshotService
from src.core.orm_models import DailyNavSnapshot


class _FakeQuery:
    def __init__(self, db):
        self.db = db
        self.strategy_id = None
        self.snapshot_date = None

    def filter_by(self, **kwargs):
        self.strategy_id = kwargs["strategy_id"]
        self.snapshot_date = kwargs["snapshot_date"]
        return self

    def first(self):
        return self.db.snapshots.get((self.strategy_id, self.snapshot_date))


class _FakeSession:
    def __init__(self):
        self.snapshots = {}
        self.added = []
        self.commit_count = 0

    def query(self, model):
        assert model is DailyNavSnapshot
        return _FakeQuery(self)

    def add(self, snapshot):
        self.added.append(snapshot)
        self.snapshots[(snapshot.strategy_id, snapshot.snapshot_date)] = snapshot

    def commit(self):
        self.commit_count += 1


def _service(db: _FakeSession) -> DailyNavSnapshotService:
    return DailyNavSnapshotService(lambda: nullcontext(db))


def test_get_start_nav_returns_none_when_missing() -> None:
    db = _FakeSession()

    nav = _service(db).get_start_nav("s1", date(2026, 5, 18))

    assert nav is None


def test_ensure_snapshot_creates_missing_snapshot() -> None:
    db = _FakeSession()

    nav = _service(db).ensure_snapshot(
        "s1",
        date(2026, 5, 18),
        Decimal("100000.12345678"),
        notes="startup",
    )

    assert nav == Decimal("100000.12345678")
    assert db.commit_count == 1
    snapshot = db.added[0]
    assert snapshot.strategy_id == "s1"
    assert snapshot.snapshot_date == date(2026, 5, 18)
    assert snapshot.nav == Decimal("100000.12345678")
    assert snapshot.base_currency == "USDT"
    assert snapshot.source == "startup_reconcile"
    assert snapshot.notes == "startup"


def test_ensure_snapshot_returns_existing_snapshot_without_overwrite() -> None:
    db = _FakeSession()
    existing = DailyNavSnapshot(
        strategy_id="s1",
        snapshot_date=date(2026, 5, 18),
        nav=Decimal("90000"),
        base_currency="USDT",
        source="manual",
    )
    db.snapshots[("s1", date(2026, 5, 18))] = existing

    nav = _service(db).ensure_snapshot(
        "s1",
        date(2026, 5, 18),
        Decimal("100000"),
    )

    assert nav == Decimal("90000")
    assert db.added == []
    assert db.commit_count == 0


def test_get_start_nav_returns_existing_nav_as_decimal() -> None:
    db = _FakeSession()
    db.snapshots[("s1", date(2026, 5, 18))] = DailyNavSnapshot(
        strategy_id="s1",
        snapshot_date=date(2026, 5, 18),
        nav="100000.12345678",
        base_currency="USDT",
    )

    nav = _service(db).get_start_nav("s1", date(2026, 5, 18))

    assert nav == Decimal("100000.12345678")
