"""Deterministic external funding events for cash-spot backtests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, Sequence


@dataclass(frozen=True, slots=True)
class ExternalFundingEvent:
    """One positive quote-asset contribution that becomes usable at a timestamp."""

    event_id: str
    account_id: str
    asset: str
    amount: Decimal
    available_at: int
    source: str

    def __post_init__(self) -> None:
        for field_name in ("event_id", "account_id", "asset", "source"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"external funding {field_name} must be non-empty")
        if not isinstance(self.amount, Decimal):
            raise TypeError("external funding amount must be Decimal")
        if not self.amount.is_finite() or self.amount <= 0:
            raise ValueError("external funding amount must be finite and positive")
        if type(self.available_at) is not int or self.available_at < 0:
            raise ValueError(
                "external funding available_at must be a non-negative integer"
            )


@dataclass(frozen=True, slots=True)
class ExternalFundingApplication:
    """Auditable before/after evidence for one applied contribution."""

    event: ExternalFundingEvent
    applied_at: int
    mark_price: Decimal
    pre_equity: Decimal
    post_equity: Decimal
    quote_total_before: Decimal
    quote_total_after: Decimal


@dataclass(frozen=True, slots=True)
class ExternalFundingCheckpoint:
    """Endpoint evidence sufficient to identify applied and pending events."""

    account_id: str
    contract_hash: str
    applied_event_ids: tuple[str, ...]
    pending_event_ids: tuple[str, ...]


class _CashSpotFundingAdapter(Protocol):
    @property
    def is_cash_spot_settlement(self) -> bool: ...

    def get_cash_spot_account_snapshot(
        self, mark_price: Decimal
    ) -> _CashSpotSnapshot: ...

    def apply_external_funding(self, *, asset: str, amount: Decimal) -> Decimal: ...


class _CashSpotSnapshot(Protocol):
    @property
    def base_total(self) -> Decimal: ...

    @property
    def base_available(self) -> Decimal: ...

    @property
    def base_reserved(self) -> Decimal: ...

    @property
    def quote_total(self) -> Decimal: ...

    @property
    def quote_available(self) -> Decimal: ...

    @property
    def quote_reserved(self) -> Decimal: ...

    @property
    def cost_basis(self) -> Decimal: ...

    @property
    def realized_pnl(self) -> Decimal: ...

    @property
    def unrealized_pnl(self) -> Decimal: ...

    @property
    def total_equity(self) -> Decimal: ...


class ExternalFundingTimeline:
    """Validate, order, apply, and checkpoint a funding event contract.

    A due event is applied on the first replay candle whose timestamp is at or
    after ``available_at``. The runner calls this after matching existing orders
    and settling their fees, but before strategies observe account state.
    """

    def __init__(
        self,
        events: Sequence[ExternalFundingEvent],
        *,
        account_id: str,
        quote_asset: str,
        start_time: int,
    ) -> None:
        for field_name, value in (
            ("account_id", account_id),
            ("quote_asset", quote_asset),
        ):
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"external funding {field_name} must be non-empty")
        if type(start_time) is not int or start_time < 0:
            raise ValueError("external funding start_time must be non-negative")

        unique_by_id: dict[str, ExternalFundingEvent] = {}
        previous_available_at: int | None = None
        for event in events:
            if not isinstance(event, ExternalFundingEvent):
                raise TypeError(
                    "external funding events must be ExternalFundingEvent"
                )
            if (
                previous_available_at is not None
                and event.available_at < previous_available_at
            ):
                raise ValueError(
                    "external funding events must be ordered by available_at"
                )
            previous_available_at = event.available_at
            existing = unique_by_id.get(event.event_id)
            if existing is not None:
                if existing != event:
                    raise ValueError(
                        f"external funding event_id conflict: {event.event_id}"
                    )
                continue
            if event.account_id != account_id:
                raise ValueError(
                    "external funding account mismatch: "
                    f"expected={account_id} actual={event.account_id}"
                )
            if event.asset != quote_asset:
                raise ValueError(
                    "external funding supports configured quote asset only: "
                    f"expected={quote_asset} actual={event.asset}"
                )
            if event.available_at < start_time:
                raise ValueError(
                    "external funding event predates replay start; include it in "
                    "initial_balance instead"
                )
            unique_by_id[event.event_id] = event

        self._account_id = account_id
        self._events = tuple(
            sorted(
                unique_by_id.values(),
                key=lambda event: (event.available_at, event.event_id),
            )
        )
        self._contract_hash = _contract_hash(self._events)
        self._next_index = 0
        self._last_observed_timestamp: int | None = None
        self._applications: list[ExternalFundingApplication] = []

    @property
    def applications(self) -> tuple[ExternalFundingApplication, ...]:
        return tuple(self._applications)

    def apply_due(
        self,
        adapter: _CashSpotFundingAdapter,
        *,
        timestamp: int,
        mark_price: Decimal,
    ) -> tuple[ExternalFundingApplication, ...]:
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("external funding application timestamp is invalid")
        if not isinstance(mark_price, Decimal):
            raise TypeError("external funding mark_price must be Decimal")
        if not mark_price.is_finite() or mark_price <= 0:
            raise ValueError("external funding mark_price must be finite and positive")
        if not adapter.is_cash_spot_settlement:
            raise ValueError("external funding requires cash_spot settlement")
        if (
            self._last_observed_timestamp is not None
            and timestamp < self._last_observed_timestamp
        ):
            raise ValueError(
                "external funding replay timestamps must be non-decreasing"
            )
        self._last_observed_timestamp = timestamp

        applied: list[ExternalFundingApplication] = []
        while self._next_index < len(self._events):
            event = self._events[self._next_index]
            if event.available_at > timestamp:
                break
            application = self._apply_one(
                adapter,
                event=event,
                timestamp=timestamp,
                mark_price=mark_price,
            )
            self._applications.append(application)
            applied.append(application)
            self._next_index += 1
        return tuple(applied)

    def checkpoint(self) -> ExternalFundingCheckpoint:
        return ExternalFundingCheckpoint(
            account_id=self._account_id,
            contract_hash=self._contract_hash,
            applied_event_ids=tuple(
                application.event.event_id for application in self._applications
            ),
            pending_event_ids=tuple(
                event.event_id for event in self._events[self._next_index :]
            ),
        )

    @staticmethod
    def _apply_one(
        adapter: _CashSpotFundingAdapter,
        *,
        event: ExternalFundingEvent,
        timestamp: int,
        mark_price: Decimal,
    ) -> ExternalFundingApplication:
        before = adapter.get_cash_spot_account_snapshot(mark_price)
        returned_total = adapter.apply_external_funding(
            asset=event.asset,
            amount=event.amount,
        )
        after = adapter.get_cash_spot_account_snapshot(mark_price)

        if returned_total != after.quote_total:
            raise RuntimeError("external funding ledger returned inconsistent total")
        if after.quote_total - before.quote_total != event.amount:
            raise RuntimeError("external funding quote total conservation failed")
        if after.quote_available - before.quote_available != event.amount:
            raise RuntimeError("external funding quote available conservation failed")
        for field_name in (
            "base_total",
            "base_available",
            "base_reserved",
            "quote_reserved",
            "cost_basis",
            "realized_pnl",
        ):
            if getattr(after, field_name) != getattr(before, field_name):
                raise RuntimeError(
                    f"external funding unexpectedly changed {field_name}"
                )
        if after.total_equity - before.total_equity != event.amount:
            raise RuntimeError("external funding equity conservation failed")

        return ExternalFundingApplication(
            event=event,
            applied_at=timestamp,
            mark_price=mark_price,
            pre_equity=before.total_equity,
            post_equity=after.total_equity,
            quote_total_before=before.quote_total,
            quote_total_after=after.quote_total,
        )


def build_external_funding_timeline(
    events: Sequence[ExternalFundingEvent],
    *,
    account_id: str | None,
    quote_asset: str | None,
    start_time: int,
) -> ExternalFundingTimeline | None:
    """Build one fresh replay timeline or reject an incomplete account contract."""
    if not events:
        return None
    if account_id is None:
        raise ValueError(
            "external_funding_account_id is required when funding events exist"
        )
    if quote_asset is None:
        raise ValueError(
            "external funding events require an explicit cash_spot instrument"
        )
    return ExternalFundingTimeline(
        events,
        account_id=account_id,
        quote_asset=quote_asset,
        start_time=start_time,
    )


def _contract_hash(events: Sequence[ExternalFundingEvent]) -> str:
    payload = [
        {
            "event_id": event.event_id,
            "account_id": event.account_id,
            "asset": event.asset,
            "amount": str(event.amount),
            "available_at": event.available_at,
            "source": event.source,
        }
        for event in events
    ]
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
