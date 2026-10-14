"""Pure validation and clock conversion for admitted Spider MODEL time input."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.decimal_math import canonical_decimal_text

SCHEMA_VERSION = "SPIDER_MODEL_TIME_DELIVERY_V1"
TIME_POLICY_ID = "SPIDER_COUNT_DOWNTIME_NO_CATCHUP_V1"
_INT64_MAX = 2**63 - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MIN_UTC_MS = (datetime(1, 1, 1, tzinfo=timezone.utc) - _EPOCH).days * 86_400_000
_MAX_UTC_DELTA = datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc) - _EPOCH
_MAX_UTC_MS = (
    _MAX_UTC_DELTA.days * 86_400_000
    + _MAX_UTC_DELTA.seconds * 1000
    + _MAX_UTC_DELTA.microseconds // 1000
)


class ModelTimeUnavailable(ValueError):
    """The closed MODEL time envelope cannot safely map to source time."""


@dataclass(frozen=True, slots=True)
class ModelTimeRecord:
    epoch_id: str
    observed_utc_ms: int
    elapsed_ms: int
    source_now_ms: int
    observation_id: str
    time_zone: str
    checkpoint_sha256: str


@dataclass(frozen=True, slots=True)
class ModelTimeTransition:
    kind: str
    source_now_ms: int
    record: ModelTimeRecord
    should_invoke_timer: bool
    snapshots: dict[str, object] | None
    local_hour: int | None
    local_minute: int | None


def decode_model_time_transition(
    raw: bytes,
    *,
    scope: dict[str, str],
    effective_identity: dict[str, object],
    evidence_sha256: str,
    ordered_products: tuple[str, ...],
    checkpoint_now_ms: int,
    checkpoint_sha256: str,
    previous: ModelTimeRecord | None,
) -> ModelTimeTransition:
    """Validate one canonical envelope and map it to source time without side effects."""
    try:
        _require(type(raw) is bytes and 1 <= len(raw) <= 1_048_576)
        _require(_valid_scope(scope))
        _require(_valid_effective_identity(effective_identity))
        _require(_is_sha256(evidence_sha256) and _is_sha256(checkpoint_sha256))
        _require(type(ordered_products) is tuple and bool(ordered_products))
        _require(
            all(type(product) is str and 0 < len(product) <= 128 for product in ordered_products)
            and len(set(ordered_products)) == len(ordered_products)
        )
        _require(_is_int64(checkpoint_now_ms) and checkpoint_now_ms >= 0)

        value = decode_canonical(raw)
        _require(canonical_bytes(value) == raw)
        envelope = _closed(value, {
            "schema_version", "scope", "effective_identity", "observation_id",
            "predecessor_observation_id", "checkpoint_before_sha256", "provenance",
            "event", "clock", "snapshots",
        })
        _require(envelope["schema_version"] == SCHEMA_VERSION)
        supplied_scope = envelope["scope"]
        supplied_identity = envelope["effective_identity"]
        _require(
            _valid_scope(supplied_scope)
            and supplied_scope == scope
            and _valid_effective_identity(supplied_identity)
            and supplied_identity == effective_identity
        )
        observation_id = envelope["observation_id"]
        predecessor_id = envelope["predecessor_observation_id"]
        checkpoint_before = envelope["checkpoint_before_sha256"]
        _require(_is_sha256(observation_id) and _is_sha256(checkpoint_before))
        provenance = _closed(envelope["provenance"], {"kind", "evidence_sha256"})
        _require(provenance == {"kind": "MODEL", "evidence_sha256": evidence_sha256})

        event = envelope["event"]
        _require(event in {"ANCHOR", "TIMER", "RESTART"})
        clock = _closed(envelope["clock"], {
            "epoch_id", "observed_utc_ms", "elapsed_ms", "time_zone", "time_policy_id",
        })
        epoch_id = clock["epoch_id"]
        observed_utc_ms = clock["observed_utc_ms"]
        elapsed_ms = clock["elapsed_ms"]
        time_zone = clock["time_zone"]
        _require(type(epoch_id) is str and 0 < len(epoch_id) <= 128)
        _require(_valid_utc_ms(observed_utc_ms))
        _require(_is_int64(elapsed_ms) and cast(int, elapsed_ms) >= 0)
        elapsed_ms = cast(int, elapsed_ms)
        observed_utc_ms = cast(int, observed_utc_ms)
        epoch_id = cast(str, epoch_id)
        time_zone = cast(str, time_zone)
        _require(time_zone == "UTC" and clock["time_policy_id"] == TIME_POLICY_ID)

        snapshots = envelope["snapshots"]
        local_hour: int | None = None
        local_minute: int | None = None
        if event == "ANCHOR":
            _require(previous is None and predecessor_id is None)
            _require(checkpoint_before == checkpoint_sha256 and snapshots is None)
            new_now_ms = checkpoint_now_ms
        else:
            _require(previous is not None and predecessor_id == previous.observation_id)
            _require(checkpoint_before == checkpoint_sha256)
            _validate_previous(cast(ModelTimeRecord, previous))
            prior = cast(ModelTimeRecord, previous)
            _require(prior.source_now_ms == checkpoint_now_ms)
            _require(observed_utc_ms >= prior.observed_utc_ms)
            if event == "TIMER":
                _require(epoch_id == prior.epoch_id and time_zone == prior.time_zone)
                _require(elapsed_ms >= prior.elapsed_ms)
                _validate_snapshots(snapshots, observed_utc_ms, ordered_products)
                delta_ms = elapsed_ms - prior.elapsed_ms
                local_hour = (observed_utc_ms // 3_600_000) % 24
                local_minute = (observed_utc_ms // 60_000) % 60
            else:
                _require(epoch_id != prior.epoch_id)
                _require(snapshots is None)
                delta_ms = observed_utc_ms - prior.observed_utc_ms
            new_now_ms = _checked_add(prior.source_now_ms, delta_ms)

        record = ModelTimeRecord(
            epoch_id=cast(str, epoch_id),
            observed_utc_ms=cast(int, observed_utc_ms),
            elapsed_ms=cast(int, elapsed_ms),
            source_now_ms=new_now_ms,
            observation_id=cast(str, observation_id),
            time_zone=cast(str, time_zone),
            checkpoint_sha256=cast(str, checkpoint_sha256),
        )
        return ModelTimeTransition(
            kind=cast(str, event),
            source_now_ms=new_now_ms,
            record=record,
            should_invoke_timer=event == "TIMER",
            snapshots=cast(dict[str, object] | None, snapshots),
            local_hour=local_hour,
            local_minute=local_minute,
        )
    except (KeyError, TypeError, ValueError, InvalidOperation, UnicodeDecodeError, RecursionError) as exc:
        raise ModelTimeUnavailable("SPIDER_MODEL_TIME_UNAVAILABLE") from exc


def _validate_snapshots(value: object, observed_utc_ms: int, products: tuple[str, ...]) -> None:
    row = _closed(value, {"earn", "trading", "positions", "open_orders", "as_of_ms"})
    as_of_ms = row["as_of_ms"]
    _require(_valid_utc_ms(as_of_ms) and cast(int, as_of_ms) <= observed_utc_ms)
    as_of_ms = cast(int, as_of_ms)
    earn = row["earn"]
    _require(earn is None or _canonical_decimal(earn))
    trading = row["trading"]
    if trading is not None:
        trading_row = _closed(trading, {"eq", "availEq"})
        _require(_canonical_decimal(trading_row["eq"]) and _canonical_decimal(trading_row["availEq"]))
    positions = row["positions"]
    if positions is not None:
        _require(type(positions) is dict and set(positions) == set(products))
        position_map = cast(dict[str, object], positions)
        _require(all(_canonical_decimal(position_map[product]) for product in products))
    orders = row["open_orders"]
    if orders is not None:
        _require(type(orders) is list)
        for item in cast(list[object], orders):
            order = _closed(item, {
                "local_order_id", "state", "price", "quantity_contracts",
                "cumulative_contracts", "created_at_ms",
            })
            _require(type(order["local_order_id"]) is str and 0 < len(order["local_order_id"]) <= 128)
            _require(order["state"] in {"live", "partially_filled"})
            _require(_canonical_decimal(order["price"]))
            _require(_canonical_decimal(order["quantity_contracts"]))
            _require(_canonical_decimal(order["cumulative_contracts"]))
            _require(_valid_utc_ms(order["created_at_ms"]))


def _canonical_decimal(value: object) -> bool:
    if type(value) is not str or "e" in value.lower():
        return False
    try:
        decimal = Decimal(value)
        return decimal.is_finite() and canonical_decimal_text(decimal) == value
    except (InvalidOperation, ValueError):
        return False


def _valid_scope(value: object) -> bool:
    return (
        type(value) is dict
        and set(value) == {"environment", "execution_scope_id", "strategy_id"}
        and all(type(item) is str and 0 < len(item) <= 128 for item in value.values())
    )


def _valid_effective_identity(value: object) -> bool:
    return (
        type(value) is dict
        and set(value) == {"generation", "confirmation_request_id", "configuration_sha256"}
        and _is_int64(cast(dict[str, object], value)["generation"])
        and 1 <= cast(int, cast(dict[str, object], value)["generation"]) <= _INT64_MAX
        and _is_sha256(value["confirmation_request_id"])
        and _is_sha256(value["configuration_sha256"])
    )


def _validate_previous(previous: ModelTimeRecord) -> None:
    _require(type(previous) is ModelTimeRecord)
    _require(type(previous.epoch_id) is str and 0 < len(previous.epoch_id) <= 128)
    _require(_valid_utc_ms(previous.observed_utc_ms))
    _require(_is_int64(previous.elapsed_ms) and previous.elapsed_ms >= 0)
    _require(_is_int64(previous.source_now_ms) and previous.source_now_ms >= 0)
    _require(_is_sha256(previous.observation_id) and _is_sha256(previous.checkpoint_sha256))
    _require(previous.time_zone == "UTC")


def _valid_utc_ms(value: object) -> bool:
    return _is_signed_int64(value) and _MIN_UTC_MS <= cast(int, value) <= _MAX_UTC_MS


def _is_int64(value: object) -> bool:
    return type(value) is int and 0 <= value <= _INT64_MAX


def _is_signed_int64(value: object) -> bool:
    return type(value) is int and -(2**63) <= value <= _INT64_MAX


def _is_sha256(value: object) -> bool:
    return (
        type(value) is str and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def _closed(value: object, keys: set[str]) -> dict[str, object]:
    _require(type(value) is dict and set(value) == keys)
    return cast(dict[str, object], value)


def _checked_add(left: int, right: int) -> int:
    result = left + right
    _require(0 <= result <= _INT64_MAX)
    return result


def _require(condition: object) -> None:
    if not condition:
        raise ValueError("invalid model time input")
