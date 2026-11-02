"""Terminal evidence inside a caller-owned active PostgreSQL transaction.

No session/transaction lifecycle, callback execution or fill guarantee. The caller
must insert the marker=1 receipt and commit atomically with this helper's writes.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, cast
from uuid import UUID

from sqlalchemy import Table, and_, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .decision_application import MarketDataDecisionBatch, _product
from .decision_input_store import _hydrate
from .decision_input import MarketDataDecisionInput
from .orm import MarketDataDecisionBatch as BatchRow
from .orm import MarketDataDecisionOutcome as OutcomeRow
from .orm import MarketDataDecisionInput as InputRow
from src.core.orm_models import MarketExitHandoff as HandoffRow
from .read_types import _safe, _integer

_BATCH = cast(Table, BatchRow.__table__)
_OUTCOME = cast(Table, OutcomeRow.__table__)
_INPUT = cast(Table, InputRow.__table__)
_HANDOFF = cast(Table, HandoffRow.__table__)
_RECEIPT = ("environment", "product_id", "timeframe", "bar_start_ms")
_HANDOFF_RECEIPT = ("environment", "product_id", "timeframe", "timestamp")


class DecisionBatchIntegrityError(ValueError):
    def __init__(self) -> None:
        super().__init__("DECISION_BATCH_INTEGRITY")


class DecisionBatchConflict(ValueError):
    def __init__(self) -> None:
        super().__init__("DECISION_BATCH_CONFLICT")


@dataclass(frozen=True, slots=True)
class MarketExitHandoffInput:
    handoff_id: str
    environment: str
    product_id: str
    timeframe: str
    timestamp: int
    signal_key: str
    emission_ordinal: int
    signal_payload: dict[str, Any]
    signal_digest: str
    execution_scope_id: str | None = None
    strategy_id: str | None = None
    strategy_version: str | None = None
    config_hash: str | None = None
    account_id: str | None = None
    account_profile: str | None = None
    cause_generation: int | None = None
    _canonical_payload: bytes = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            UUID(self.handoff_id)
            canonical = json.dumps(
                self.signal_payload, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False,
            ).encode()
        except (ValueError, TypeError):
            raise DecisionBatchIntegrityError() from None
        if (
            type(self.signal_payload) is not dict
            or self.signal_digest != hashlib.sha256(canonical).hexdigest()
            or type(self.signal_key) is not str or not self.signal_key
            or type(self.emission_ordinal) is not int or self.emission_ordinal < 0
            or (self.strategy_id is None) != (self.strategy_version is None)
            or (self.account_id is None) != (self.account_profile is None)
            or self.cause_generation is not None and self.cause_generation < 0
        ):
            raise DecisionBatchIntegrityError()
        # Own the accepted bytes. The public input may be mutated by its caller,
        # but neither that alias nor a row dict can alter the durable evidence.
        object.__setattr__(self, "signal_payload", json.loads(canonical))
        object.__setattr__(self, "_canonical_payload", canonical)

    @property
    def row(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name in (
            "handoff_id", "environment", "product_id", "timeframe", "timestamp",
            "signal_key", "emission_ordinal", "signal_digest",
            "execution_scope_id", "strategy_id", "strategy_version", "config_hash",
            "account_id", "account_profile", "cause_generation",
        )} | {"signal_payload": json.loads(self._canonical_payload)}

    def verify_payload(self) -> None:
        if hashlib.sha256(self._canonical_payload).hexdigest() != self.signal_digest:
            raise DecisionBatchIntegrityError()


def append_exit_handoffs(
    session: Session, handoffs: tuple[MarketExitHandoffInput, ...]
) -> tuple[MarketExitHandoffInput, ...]:
    """Append immutable handoffs within the caller's receipt transaction."""
    if type(handoffs) is not tuple or any(type(item) is not MarketExitHandoffInput for item in handoffs):
        raise DecisionBatchIntegrityError()
    if not handoffs:
        return ()
    if session.get_bind().dialect.name != "postgresql" or not session.in_transaction():
        raise DecisionBatchIntegrityError()
    first = handoffs[0]
    identity = {name: getattr(first, name) for name in ("environment", "product_id", "timeframe", "timestamp")}
    by_signal_key: dict[str, MarketExitHandoffInput] = {}
    for item in handoffs:
        item.verify_payload()
        if any(getattr(item, name) != value for name, value in identity.items()):
            raise DecisionBatchIntegrityError()
        duplicate = by_signal_key.get(item.signal_key)
        if duplicate is not None:
            if duplicate.row != item.row:
                raise DecisionBatchConflict()
            raise DecisionBatchIntegrityError()
        by_signal_key[item.signal_key] = item
    for item in handoffs:
        existing = session.execute(
            select(_HANDOFF).where(
                and_(*(_HANDOFF.c[name] == getattr(item, name) for name in (*_HANDOFF_RECEIPT, "signal_key")))
            )
        ).mappings().one_or_none()
        if existing is not None:
            if any(existing[name] != value for name, value in item.row.items()):
                raise DecisionBatchConflict()
            continue
        session.execute(insert(_HANDOFF).values(**item.row))
    rows = session.execute(
        select(_HANDOFF).where(and_(*(
            _HANDOFF.c[name] == value for name, value in identity.items()
        )))
    ).mappings().all()
    found = {row["signal_key"]: row for row in rows}
    for item in handoffs:
        row = found.get(item.signal_key)
        if row is None or any(row[name] != value for name, value in item.row.items()):
            raise DecisionBatchIntegrityError()
    return handoffs


def read_exit_handoffs(
    session: Session, *, environment: str, product_id: str, timeframe: str, bar_start_ms: int,
) -> tuple[dict[str, Any], ...]:
    identity = dict(environment=environment, product_id=product_id, timeframe=timeframe, timestamp=bar_start_ms)
    if session.get_bind().dialect.name != "postgresql" or not session.in_transaction():
        raise DecisionBatchIntegrityError()
    rows = session.execute(
        select(_HANDOFF).where(and_(*(_HANDOFF.c[name] == value for name, value in identity.items())))
        .order_by(_HANDOFF.c.emission_ordinal, _HANDOFF.c.signal_key)
    ).mappings().all()
    result = []
    for row in rows:
        record = dict(row)
        try:
            canonical = json.dumps(
                record["signal_payload"], sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False,
            ).encode()
        except (TypeError, ValueError):
            raise DecisionBatchIntegrityError() from None
        if hashlib.sha256(canonical).hexdigest() != record["signal_digest"]:
            raise DecisionBatchIntegrityError()
        result.append(record)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class DecisionBatchRecord:
    batch: MarketDataDecisionBatch
    recorded_at: datetime
    already_present: bool
    verified_inputs: tuple[MarketDataDecisionInput | None, ...]

    def __post_init__(self) -> None:
        if (
            type(self.batch) is not MarketDataDecisionBatch
            or type(self.already_present) is not bool
            or type(self.recorded_at) is not datetime
            or self.recorded_at.tzinfo is not timezone.utc
            or self.recorded_at.microsecond % 1000
            or type(self.verified_inputs) is not tuple
            or len(self.verified_inputs) != len(self.batch.outcomes)
        ):
            raise DecisionBatchIntegrityError()
        for outcome, value in zip(
            self.batch.outcomes, self.verified_inputs, strict=True
        ):
            if outcome.input_id is None:
                if value is not None:
                    raise DecisionBatchIntegrityError()
            elif (
                type(value) is not MarketDataDecisionInput
                or value.key != outcome.key
                or value.input_id != outcome.input_id
                or value.input_digest != outcome.input_digest
            ):
                raise DecisionBatchIntegrityError()


def _guard(session: Session, identity: dict[str, Any]) -> None:
    try:
        _safe(identity["environment"], 64)
        _product(identity["product_id"])
        _safe(identity["timeframe"], 32)
        _integer(identity["bar_start_ms"])
        if (
            session.get_bind().dialect.name != "postgresql"
            or not session.in_transaction()
        ):
            raise ValueError
    except (ValueError, TypeError, KeyError):
        raise DecisionBatchIntegrityError() from None


def _identity(batch: MarketDataDecisionBatch) -> dict[str, Any]:
    return {name: getattr(batch, name) for name in _RECEIPT}


def _header(batch: MarketDataDecisionBatch) -> dict[str, Any]:
    return dict(
        **_identity(batch),
        execution_scope_id=batch.execution_scope_id,
        contract_version=1,
        participant_count=len(batch.participants),
        canonical_payload=batch.canonical_bytes,
        batch_digest=batch.digest,
    )


def _outcomes(batch: MarketDataDecisionBatch) -> list[dict[str, Any]]:
    rows = []
    for outcome in batch.outcomes:
        row = asdict(outcome)
        row.update(row.pop("key"))
        row.update(timeframe=batch.timeframe, bar_start_ms=batch.bar_start_ms)
        rows.append(row)
    return rows


def _inputs(
    session: Session, batch: MarketDataDecisionBatch
) -> tuple[MarketDataDecisionInput | None, ...]:
    verified: list[MarketDataDecisionInput | None] = []
    for outcome in batch.outcomes:
        if outcome.input_id is None:
            verified.append(None)
            continue
        rows = (
            session.execute(
                select(_INPUT)
                .where(
                    or_(
                        _INPUT.c.input_id == outcome.input_id,
                        and_(
                            *(
                                _INPUT.c[name] == value
                                for name, value in asdict(outcome.key).items()
                            )
                        ),
                    )
                )
                .limit(2)
            )
            .mappings()
            .all()
        )
        if len(rows) != 1:
            raise DecisionBatchIntegrityError()
        record = _hydrate(rows[0], outcome.key, True)
        if (
            record.value.key != outcome.key
            or record.value.input_id != outcome.input_id
            or record.value.input_digest != outcome.input_digest
        ):
            raise DecisionBatchIntegrityError()
        verified.append(record.value)
    return tuple(verified)


def read_decision_batch(
    session: Session,
    *,
    environment: str,
    product_id: str,
    timeframe: str,
    bar_start_ms: int,
) -> DecisionBatchRecord | None:
    """None means no header, never an explicit empty batch."""
    identity = dict(
        environment=environment,
        product_id=product_id,
        timeframe=timeframe,
        bar_start_ms=bar_start_ms,
    )
    _guard(session, identity)
    headers = (
        session.execute(
            select(_BATCH)
            .where(and_(*(_BATCH.c[name] == value for name, value in identity.items())))
            .limit(2)
        )
        .mappings()
        .all()
    )
    if not headers:
        return None
    try:
        if len(headers) != 1:
            raise ValueError
        row = headers[0]
        raw = row["canonical_payload"]
        if type(raw) is memoryview:
            raw = raw.tobytes()
        batch = MarketDataDecisionBatch.from_canonical_bytes(raw)
        if _identity(batch) != identity:
            raise ValueError
        for name, expected in _header(batch).items():
            actual = raw if name == "canonical_payload" else row[name]
            if type(actual) is not type(expected) or actual != expected:
                raise ValueError
        rows = (
            session.execute(
                select(_OUTCOME)
                .where(
                    and_(
                        *(_OUTCOME.c[name] == value for name, value in identity.items())
                    )
                )
                .limit(257)
            )
            .mappings()
            .all()
        )
        expected_rows = _outcomes(batch)
        if len(rows) != len(expected_rows):
            raise ValueError
        actual_by_strategy = {row["strategy_id"]: row for row in rows}
        if len(actual_by_strategy) != len(rows):
            raise ValueError
        for expected_row in expected_rows:
            actual_row = actual_by_strategy[expected_row["strategy_id"]]
            if set(actual_row) != set(expected_row) or any(
                type(actual_row[name]) is not type(value) or actual_row[name] != value
                for name, value in expected_row.items()
            ):
                raise ValueError
        verified = _inputs(session, batch)
        return DecisionBatchRecord(batch, row["recorded_at"], True, verified)
    except (ValueError, TypeError, KeyError, OverflowError):
        raise DecisionBatchIntegrityError() from None


def append_decision_batch(
    session: Session, batch: MarketDataDecisionBatch
) -> DecisionBatchRecord:
    """Append/read back only; caller owns receipt, rollback and commit."""
    if type(batch) is not MarketDataDecisionBatch:
        raise DecisionBatchIntegrityError()
    identity = _identity(batch)
    _guard(session, identity)
    existing = read_decision_batch(session, **identity)
    if existing is not None:
        if existing.batch != batch:
            raise DecisionBatchConflict()
        return existing
    try:
        _inputs(session, batch)
    except ValueError:
        raise DecisionBatchIntegrityError() from None
    inserted = session.execute(
        insert(_BATCH)
        .values(**_header(batch))
        .on_conflict_do_nothing()
        .returning(_BATCH.c.batch_digest)
    ).scalar_one_or_none()
    if inserted is not None:
        rows = _outcomes(batch)
        if rows:
            session.execute(insert(_OUTCOME), rows)
    record = read_decision_batch(session, **identity)
    if record is None:
        raise DecisionBatchIntegrityError()
    if record.batch != batch:
        raise DecisionBatchConflict()
    return DecisionBatchRecord(
        record.batch, record.recorded_at, inserted is None, record.verified_inputs
    )
