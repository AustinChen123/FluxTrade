"""Reusable scaled execution candles with Rust-derived decision markers."""

from __future__ import annotations

from array import array
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal, DecimalException
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from zipfile import BadZipFile

import numpy as np

from fluxtrade_core import (
    CandleAggregator,  # pyright: ignore[reportAttributeAccessIssue]
    Candlestick as RustCandlestick,  # pyright: ignore[reportAttributeAccessIssue]
    ScaledCandlestick as RustScaledCandlestick,  # pyright: ignore[reportAttributeAccessIssue]
)

from src.core.data_provider import timeframe_to_ms
from src.core.models import Candlestick
from src.core.precision import PrecisionCodec, PrecisionSpec
from src.core.product_registry import validate_product_id


# Bump whenever the artifact layout or Rust aggregation semantics change.
_CONTRACT_VERSION = 1
_EXECUTION_WIDTH = 6
_DECISION_WIDTH = 7
_TIMESTAMP = 0
_OPEN = 1
_HIGH = 2
_LOW = 3
_CLOSE = 4
_VOLUME = 5
_READY_INDEX = 0
_DECISION_TIMESTAMP = 1


class PreparedReplayFormatError(ValueError):
    """Raised when a prepared replay artifact is malformed."""


class PreparedReplaySpecMismatch(ValueError):
    """Raised when an artifact does not match the requested replay contract."""


@dataclass(frozen=True, slots=True)
class PreparedReplaySpec:
    """Inputs that determine the meaning of a prepared replay artifact."""

    source_fingerprint: str
    product_id: str
    source_timeframe: str
    decision_timeframe: str
    precision: PrecisionSpec

    def __post_init__(self) -> None:
        for name in (
            "source_fingerprint",
            "product_id",
            "source_timeframe",
            "decision_timeframe",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        validate_product_id(self.product_id)


@dataclass(frozen=True, slots=True)
class PreparedReplayEvent:
    """One execution candle and an optional completed decision candle."""

    timestamp: int
    mark_price: Decimal
    scaled_execution_candle: Any
    decision_candle: Candlestick | None


@dataclass(frozen=True, slots=True)
class PreparedReplayTape:
    """Immutable integer-backed replay data prepared once and sliced repeatedly."""

    spec: PreparedReplaySpec
    _execution: np.ndarray
    _decisions: np.ndarray

    def __post_init__(self) -> None:
        execution = _validated_matrix(
            self._execution,
            width=_EXECUTION_WIDTH,
            name="execution",
        )
        decisions = _validated_matrix(
            self._decisions,
            width=_DECISION_WIDTH,
            name="decisions",
        )
        _validate_replay_rows(execution, decisions, spec=self.spec)
        execution.setflags(write=False)
        decisions.setflags(write=False)
        object.__setattr__(self, "_execution", execution)
        object.__setattr__(self, "_decisions", decisions)

    def __len__(self) -> int:
        return len(self._execution)

    @property
    def decision_count(self) -> int:
        return len(self._decisions)

    @classmethod
    def from_candles(
        cls,
        candles: Iterable[Candlestick],
        *,
        spec: PreparedReplaySpec,
    ) -> "PreparedReplayTape":
        """Prepare trusted scaled candles and Rust aggregation output."""
        if not CandleAggregator.can_aggregate(
            spec.source_timeframe,
            spec.decision_timeframe,
        ):
            raise ValueError(
                "source timeframe cannot be aggregated to decision timeframe"
            )

        codec = PrecisionCodec(spec.precision)
        aggregator = CandleAggregator()
        execution_values = array("q")
        decision_values = array("q")
        previous_timestamp: int | None = None
        row_index = 0

        for candle in candles:
            if candle.product_id != spec.product_id:
                raise ValueError(
                    "prepared replay product mismatch: "
                    f"expected={spec.product_id} actual={candle.product_id}"
                )
            if candle.timeframe != spec.source_timeframe:
                raise ValueError(
                    "prepared replay source timeframe mismatch: "
                    f"expected={spec.source_timeframe} actual={candle.timeframe}"
                )
            if (
                previous_timestamp is not None
                and candle.timestamp <= previous_timestamp
            ):
                raise ValueError(
                    "prepared replay candle timestamps must be strictly increasing"
                )
            previous_timestamp = candle.timestamp

            execution_values.extend(
                (
                    candle.timestamp,
                    _encode_price_exact(codec, candle.open, "execution open"),
                    _encode_price_exact(codec, candle.high, "execution high"),
                    _encode_price_exact(codec, candle.low, "execution low"),
                    _encode_price_exact(codec, candle.close, "execution close"),
                    _encode_quantity_exact(
                        codec,
                        candle.volume,
                        "execution volume",
                    ),
                )
            )
            completed = aggregator.add_candle(
                RustCandlestick(
                    product_id=candle.product_id,
                    timeframe=candle.timeframe,
                    timestamp=candle.timestamp,
                    open=str(candle.open),
                    high=str(candle.high),
                    low=str(candle.low),
                    close=str(candle.close),
                    volume=str(candle.volume),
                ),
                spec.decision_timeframe,
            )
            if completed is not None:
                decision_values.extend(
                    (
                        row_index,
                        int(completed.timestamp),
                        _encode_price_exact(
                            codec,
                            Decimal(str(completed.open)),
                            "decision open",
                        ),
                        _encode_price_exact(
                            codec,
                            Decimal(str(completed.high)),
                            "decision high",
                        ),
                        _encode_price_exact(
                            codec,
                            Decimal(str(completed.low)),
                            "decision low",
                        ),
                        _encode_price_exact(
                            codec,
                            Decimal(str(completed.close)),
                            "decision close",
                        ),
                        _encode_quantity_exact(
                            codec,
                            Decimal(str(completed.volume)),
                            "decision volume",
                        ),
                    )
                )
            row_index += 1

        return cls(
            spec=spec,
            _execution=_matrix_from_array(execution_values, _EXECUTION_WIDTH),
            _decisions=_matrix_from_array(decision_values, _DECISION_WIDTH),
        )

    def iter_events(
        self,
        *,
        start_time: int,
        end_time: int,
    ) -> Iterator[PreparedReplayEvent]:
        """Yield inclusive execution rows and causally available decisions."""
        if end_time < start_time:
            raise ValueError("end_time must be greater than or equal to start_time")

        timestamps = self._execution[:, _TIMESTAMP]
        start_index = int(np.searchsorted(timestamps, start_time, side="left"))
        end_index = int(np.searchsorted(timestamps, end_time, side="right"))
        ready_indexes = self._decisions[:, _READY_INDEX]
        decision_index = int(np.searchsorted(ready_indexes, start_index, side="left"))
        codec = PrecisionCodec(self.spec.precision)

        for execution_index in range(start_index, end_index):
            decision_candle = None
            if (
                decision_index < len(self._decisions)
                and int(ready_indexes[decision_index]) == execution_index
            ):
                decision_row = self._decisions[decision_index]
                decision_timestamp = int(decision_row[_DECISION_TIMESTAMP])
                if start_time <= decision_timestamp <= end_time:
                    decision_candle = _decision_candle(
                        decision_row,
                        spec=self.spec,
                        codec=codec,
                    )
                decision_index += 1

            row = self._execution[execution_index]
            yield PreparedReplayEvent(
                timestamp=int(row[_TIMESTAMP]),
                mark_price=codec.decode_price(int(row[_CLOSE])),
                scaled_execution_candle=RustScaledCandlestick(
                    product_id=self.spec.product_id,
                    timeframe=self.spec.source_timeframe,
                    timestamp=int(row[_TIMESTAMP]),
                    open_units=int(row[_OPEN]),
                    high_units=int(row[_HIGH]),
                    low_units=int(row[_LOW]),
                    close_units=int(row[_CLOSE]),
                    volume_units=int(row[_VOLUME]),
                ),
                decision_candle=decision_candle,
            )

    def save(self, path: str | Path) -> None:
        """Atomically persist the replay without pickle-backed data."""
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        metadata = json.dumps(
            _metadata_for(self.spec),
            sort_keys=True,
            separators=(",", ":"),
        )
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w+b",
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                np.savez(
                    temporary,
                    metadata=np.array(metadata),
                    execution=self._execution,
                    decisions=self._decisions,
                )
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, destination)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        expected_spec: PreparedReplaySpec | None = None,
    ) -> "PreparedReplayTape":
        """Load and validate a prepared replay artifact."""
        try:
            with np.load(Path(path), allow_pickle=False) as archive:
                required = {"metadata", "execution", "decisions"}
                missing = required - set(archive.files)
                if missing:
                    raise PreparedReplayFormatError(
                        f"prepared replay is missing arrays: {sorted(missing)}"
                    )
                try:
                    metadata = json.loads(str(archive["metadata"].item()))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise PreparedReplayFormatError(
                        "prepared replay metadata is invalid"
                    ) from exc
                spec = _spec_from_metadata(metadata)
                execution = archive["execution"]
                decisions = archive["decisions"]
        except (
            FileNotFoundError,
            PreparedReplayFormatError,
            PreparedReplaySpecMismatch,
        ):
            raise
        except (OSError, ValueError, EOFError, BadZipFile) as exc:
            raise PreparedReplayFormatError(
                "prepared replay artifact cannot be read"
            ) from exc

        if expected_spec is not None and spec != expected_spec:
            raise PreparedReplaySpecMismatch(
                "prepared replay specification does not match requested data"
            )
        return cls(spec=spec, _execution=execution, _decisions=decisions)


def file_source_fingerprint(path: str | Path) -> str:
    """Return an authoritative content digest without storing the source path."""
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def load_or_prepare_replay(
    path: str | Path,
    *,
    spec: PreparedReplaySpec,
    candle_factory: Callable[[], Iterable[Candlestick]],
) -> PreparedReplayTape:
    """Reuse a matching artifact or rebuild it when its contract changed."""
    try:
        return PreparedReplayTape.load(path, expected_spec=spec)
    except FileNotFoundError:
        pass
    except PreparedReplaySpecMismatch:
        pass

    tape = PreparedReplayTape.from_candles(candle_factory(), spec=spec)
    tape.save(path)
    return tape


def _validated_matrix(
    value: np.ndarray,
    *,
    width: int,
    name: str,
) -> np.ndarray:
    matrix = np.asarray(value)
    if matrix.dtype != np.int64:
        raise PreparedReplayFormatError(f"{name} array must use int64")
    if matrix.ndim != 2 or matrix.shape[1] != width:
        raise PreparedReplayFormatError(f"{name} array must have shape (n, {width})")
    return np.array(matrix, dtype=np.int64, order="C", copy=True)


def _validate_replay_rows(
    execution: np.ndarray,
    decisions: np.ndarray,
    *,
    spec: PreparedReplaySpec,
) -> None:
    execution_timestamps = execution[:, _TIMESTAMP]
    if np.any(execution_timestamps < 0) or (
        len(execution) > 1
        and np.any(execution_timestamps[1:] <= execution_timestamps[:-1])
    ):
        raise PreparedReplayFormatError(
            "execution timestamps must be non-negative and strictly increasing"
        )
    _validate_ohlcv(execution, offset=0, name="execution")
    if not len(decisions):
        _validate_marker_schedule(execution, decisions, spec=spec)
        return
    ready_indexes = decisions[:, _READY_INDEX]
    decision_timestamps = decisions[:, _DECISION_TIMESTAMP]
    if (
        len(execution) == 0
        or np.any(ready_indexes < 0)
        or np.any(ready_indexes >= len(execution))
        or (len(decisions) > 1 and np.any(ready_indexes[1:] <= ready_indexes[:-1]))
        or np.any(decision_timestamps < 0)
        or (
            len(decisions) > 1
            and np.any(decision_timestamps[1:] <= decision_timestamps[:-1])
        )
    ):
        raise PreparedReplayFormatError(
            "decisions must be ordered, unique, and within execution data"
        )
    _validate_ohlcv(decisions, offset=1, name="decision")
    ready_timestamps = execution[ready_indexes, _TIMESTAMP]
    if np.any(decision_timestamps > ready_timestamps):
        raise PreparedReplayFormatError(
            "decision candle cannot become ready before its timestamp"
        )
    _validate_marker_schedule(execution, decisions, spec=spec)


def _validate_ohlcv(
    rows: np.ndarray,
    *,
    offset: int,
    name: str,
) -> None:
    if not len(rows):
        return
    opens = rows[:, _OPEN + offset]
    highs = rows[:, _HIGH + offset]
    lows = rows[:, _LOW + offset]
    closes = rows[:, _CLOSE + offset]
    volumes = rows[:, _VOLUME + offset]
    if (
        np.any(opens <= 0)
        or np.any(highs <= 0)
        or np.any(lows <= 0)
        or np.any(closes <= 0)
        or np.any(volumes < 0)
        or np.any(highs < np.maximum(opens, closes))
        or np.any(lows > np.minimum(opens, closes))
    ):
        raise PreparedReplayFormatError(
            f"{name} OHLCV values violate candle invariants"
        )


def _validate_marker_schedule(
    execution: np.ndarray,
    decisions: np.ndarray,
    *,
    spec: PreparedReplaySpec,
) -> None:
    try:
        can_aggregate = CandleAggregator.can_aggregate(
            spec.source_timeframe,
            spec.decision_timeframe,
        )
        source_ms = timeframe_to_ms(spec.source_timeframe)
        decision_ms = timeframe_to_ms(spec.decision_timeframe)
    except (IndexError, ValueError) as exc:
        raise PreparedReplayFormatError(
            "prepared replay timeframes are invalid"
        ) from exc
    if not can_aggregate:
        raise PreparedReplayFormatError(
            "prepared replay timeframes cannot be aggregated"
        )

    timestamps = execution[:, _TIMESTAMP]
    if len(timestamps) and np.any(timestamps % source_ms != 0):
        raise PreparedReplayFormatError(
            "execution timestamps must align to the source timeframe"
        )
    if source_ms == decision_ms:
        expected_ready_indexes = np.arange(len(execution), dtype=np.int64)
        expected_decision_timestamps = timestamps
    elif len(execution) == 0:
        expected_ready_indexes = np.empty(0, dtype=np.int64)
        expected_decision_timestamps = np.empty(0, dtype=np.int64)
    else:
        buckets = (timestamps // decision_ms) * decision_ms
        transitions = (
            np.flatnonzero(buckets[1:] > buckets[:-1]).astype(
                np.int64,
                copy=False,
            )
            + 1
        )
        bucket_start_indexes = np.concatenate(
            (np.array([0], dtype=np.int64), transitions)
        )
        aligned_starts = (
            timestamps[bucket_start_indexes] == buckets[bucket_start_indexes]
        )
        eligible_buckets = np.logical_or.accumulate(aligned_starts)
        emitted_buckets = eligible_buckets[:-1]
        expected_ready_indexes = transitions[emitted_buckets]
        expected_decision_timestamps = buckets[bucket_start_indexes[:-1]][
            emitted_buckets
        ]

    if not (
        np.array_equal(
            decisions[:, _READY_INDEX],
            expected_ready_indexes,
        )
        and np.array_equal(
            decisions[:, _DECISION_TIMESTAMP],
            expected_decision_timestamps,
        )
    ):
        raise PreparedReplayFormatError(
            "decision markers do not match the Rust aggregation schedule"
        )


def _encode_price_exact(
    codec: PrecisionCodec,
    value: Decimal,
    field_name: str,
) -> int:
    units = codec.encode_price(value)
    if codec.decode_price(units) != value:
        raise ValueError(f"{field_name} must align to price_tick")
    return units


def _encode_quantity_exact(
    codec: PrecisionCodec,
    value: Decimal,
    field_name: str,
) -> int:
    units = codec.encode_quantity(value)
    if codec.decode_quantity(units) != value:
        raise ValueError(f"{field_name} must align to quantity_step")
    return units


def _matrix_from_array(values: array[int], width: int) -> np.ndarray:
    if not values:
        return np.empty((0, width), dtype=np.int64)
    return np.asarray(values, dtype=np.int64).reshape((-1, width))


def _decision_candle(
    row: np.ndarray,
    *,
    spec: PreparedReplaySpec,
    codec: PrecisionCodec,
) -> Candlestick:
    return Candlestick(
        product_id=spec.product_id,
        timeframe=spec.decision_timeframe,
        timestamp=int(row[_DECISION_TIMESTAMP]),
        open=codec.decode_price(int(row[_OPEN + 1])),
        high=codec.decode_price(int(row[_HIGH + 1])),
        low=codec.decode_price(int(row[_LOW + 1])),
        close=codec.decode_price(int(row[_CLOSE + 1])),
        volume=codec.decode_quantity(int(row[_VOLUME + 1])),
    )


def _metadata_for(spec: PreparedReplaySpec) -> dict[str, str | int]:
    return {
        "contract_version": _CONTRACT_VERSION,
        "source_fingerprint": spec.source_fingerprint,
        "product_id": spec.product_id,
        "source_timeframe": spec.source_timeframe,
        "decision_timeframe": spec.decision_timeframe,
        "price_tick": str(spec.precision.price_tick),
        "quantity_step": str(spec.precision.quantity_step),
        "fee_rate_step": str(spec.precision.fee_rate_step),
    }


def _spec_from_metadata(metadata: Any) -> PreparedReplaySpec:
    if not isinstance(metadata, dict):
        raise PreparedReplayFormatError("prepared replay metadata must be an object")
    if metadata.get("contract_version") != _CONTRACT_VERSION:
        raise PreparedReplaySpecMismatch(
            "prepared replay contract version does not match"
        )
    required = {
        "source_fingerprint",
        "product_id",
        "source_timeframe",
        "decision_timeframe",
        "price_tick",
        "quantity_step",
        "fee_rate_step",
    }
    missing = required - set(metadata)
    if missing:
        raise PreparedReplayFormatError(
            f"prepared replay metadata is missing fields: {sorted(missing)}"
        )
    try:
        return PreparedReplaySpec(
            source_fingerprint=str(metadata["source_fingerprint"]),
            product_id=str(metadata["product_id"]),
            source_timeframe=str(metadata["source_timeframe"]),
            decision_timeframe=str(metadata["decision_timeframe"]),
            precision=PrecisionSpec(
                price_tick=Decimal(str(metadata["price_tick"])),
                quantity_step=Decimal(str(metadata["quantity_step"])),
                fee_rate_step=Decimal(str(metadata["fee_rate_step"])),
            ),
        )
    except (TypeError, ValueError, DecimalException) as exc:
        raise PreparedReplayFormatError(
            "prepared replay metadata values are invalid"
        ) from exc
