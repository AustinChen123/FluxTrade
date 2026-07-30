from decimal import Decimal
import json
import os

import numpy as np
import pytest

pytest.importorskip("fluxtrade_core")

from src.core.backtest.prepared_replay import (
    PreparedReplayFormatError,
    PreparedReplaySpec,
    PreparedReplayTape,
    file_source_fingerprint,
    load_or_prepare_replay,
)
from src.core.models import Candlestick
from src.core.precision import PrecisionSpec


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
MINUTE_MS = 60_000


def _spec(source_fingerprint: str = "source-v1") -> PreparedReplaySpec:
    return PreparedReplaySpec(
        source_fingerprint=source_fingerprint,
        product_id=PRODUCT_ID,
        source_timeframe="1m",
        decision_timeframe="5m",
        precision=PrecisionSpec(
            price_tick=Decimal("0.25"),
            quantity_step=Decimal("1"),
        ),
    )


def _candles(count: int = 11) -> list[Candlestick]:
    return [
        Candlestick(
            product_id=PRODUCT_ID,
            timeframe="1m",
            timestamp=index * MINUTE_MS,
            open=Decimal(100 + index),
            high=Decimal(101 + index),
            low=Decimal(99 + index),
            close=Decimal("100.25") + index,
            volume=Decimal(index + 1),
        )
        for index in range(count)
    ]


@pytest.mark.rust
def test_prepared_replay_preserves_scaled_execution_and_closed_decisions():
    tape = PreparedReplayTape.from_candles(_candles(), spec=_spec())

    events = list(tape.iter_events(start_time=0, end_time=10 * MINUTE_MS))

    assert len(tape) == 11
    assert tape.decision_count == 2
    assert events[0].decision_candle is None
    first_decision = events[5].decision_candle
    assert first_decision is not None
    assert first_decision.timestamp == 0
    assert first_decision.open == Decimal("100")
    assert first_decision.high == Decimal("105")
    assert first_decision.low == Decimal("99")
    assert first_decision.close == Decimal("104.25")
    assert first_decision.volume == Decimal("15")
    assert events[5].scaled_execution_candle.close_units == 421
    assert events[5].mark_price == Decimal("105.25")


@pytest.mark.rust
def test_prepared_replay_slice_drops_partial_leading_bucket():
    tape = PreparedReplayTape.from_candles(_candles(), spec=_spec())

    events = list(
        tape.iter_events(
            start_time=2 * MINUTE_MS,
            end_time=10 * MINUTE_MS,
        )
    )

    assert [event.timestamp for event in events] == list(
        range(2 * MINUTE_MS, 11 * MINUTE_MS, MINUTE_MS)
    )
    decisions = [
        event.decision_candle for event in events if event.decision_candle is not None
    ]
    assert [decision.timestamp for decision in decisions] == [5 * MINUTE_MS]


@pytest.mark.rust
def test_prepared_replay_cache_reuses_match_and_rebuilds_stale_spec(tmp_path):
    path = tmp_path / "prepared-replay.npz"
    calls = 0

    def candle_factory():
        nonlocal calls
        calls += 1
        return _candles()

    first = load_or_prepare_replay(
        path,
        spec=_spec("source-v1"),
        candle_factory=candle_factory,
    )
    reused = load_or_prepare_replay(
        path,
        spec=_spec("source-v1"),
        candle_factory=candle_factory,
    )
    rebuilt = load_or_prepare_replay(
        path,
        spec=_spec("source-v2"),
        candle_factory=candle_factory,
    )

    assert calls == 2
    assert len(first) == len(reused) == len(rebuilt) == 11
    assert reused.spec.source_fingerprint == "source-v1"
    assert rebuilt.spec.source_fingerprint == "source-v2"


@pytest.mark.rust
def test_prepared_replay_rejects_wrong_product_and_non_monotonic_rows():
    wrong_product = _candles()
    wrong_product[0] = wrong_product[0].model_copy(
        update={"product_id": "BINANCE:ETHUSDT-PERP"}
    )
    with pytest.raises(ValueError, match="product mismatch"):
        PreparedReplayTape.from_candles(wrong_product, spec=_spec())

    duplicate = _candles()
    duplicate[1] = duplicate[1].model_copy(update={"timestamp": 0})
    with pytest.raises(ValueError, match="strictly increasing"):
        PreparedReplayTape.from_candles(duplicate, spec=_spec())

    wrong_timeframe = _candles()
    wrong_timeframe[0] = wrong_timeframe[0].model_copy(update={"timeframe": "5m"})
    with pytest.raises(ValueError, match="source timeframe mismatch"):
        PreparedReplayTape.from_candles(wrong_timeframe, spec=_spec())


@pytest.mark.rust
@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("close", Decimal("100.13"), "price_tick"),
        ("volume", Decimal("1.5"), "quantity_step"),
    ],
)
def test_prepared_replay_rejects_off_grid_values(
    field_name,
    value,
    message,
):
    candle = _candles(1)[0].model_copy(update={field_name: value})

    with pytest.raises(ValueError, match=message):
        PreparedReplayTape.from_candles([candle], spec=_spec())


@pytest.mark.rust
def test_prepared_replay_supports_empty_and_equal_timeframe_sources():
    empty = PreparedReplayTape.from_candles([], spec=_spec())
    assert list(empty.iter_events(start_time=0, end_time=MINUTE_MS)) == []

    equal_spec = PreparedReplaySpec(
        source_fingerprint="equal-v1",
        product_id=PRODUCT_ID,
        source_timeframe="1m",
        decision_timeframe="1m",
        precision=_spec().precision,
    )
    equal = PreparedReplayTape.from_candles(_candles(2), spec=equal_spec)

    events = list(equal.iter_events(start_time=0, end_time=MINUTE_MS))
    decisions = [
        event.decision_candle for event in events if event.decision_candle is not None
    ]
    assert [decision.timestamp for decision in decisions] == [
        0,
        MINUTE_MS,
    ]


@pytest.mark.rust
def test_prepared_replay_supports_rust_second_timeframes():
    second_spec = PreparedReplaySpec(
        source_fingerprint="seconds-v1",
        product_id=PRODUCT_ID,
        source_timeframe="30s",
        decision_timeframe="1m",
        precision=_spec().precision,
    )
    candles = [
        candle.model_copy(
            update={
                "timeframe": "30s",
                "timestamp": index * 30_000,
            }
        )
        for index, candle in enumerate(_candles(3))
    ]

    tape = PreparedReplayTape.from_candles(candles, spec=second_spec)
    events = list(tape.iter_events(start_time=0, end_time=MINUTE_MS))

    assert tape.decision_count == 1
    assert events[2].decision_candle is not None
    assert events[2].decision_candle.timestamp == 0


@pytest.mark.rust
def test_prepared_replay_rejects_unsupported_aggregation():
    unsupported = PreparedReplaySpec(
        source_fingerprint="unsupported-v1",
        product_id=PRODUCT_ID,
        source_timeframe="15m",
        decision_timeframe="1h",
        precision=_spec().precision,
    )

    with pytest.raises(ValueError, match="cannot be aggregated"):
        PreparedReplayTape.from_candles([], spec=unsupported)


def test_prepared_replay_rejects_mutable_or_malformed_arrays():
    with pytest.raises(PreparedReplayFormatError, match="shape"):
        PreparedReplayTape(
            spec=_spec(),
            _execution=np.empty((1, 5), dtype=np.int64),
            _decisions=np.empty((0, 7), dtype=np.int64),
        )
    with pytest.raises(PreparedReplayFormatError, match="int64"):
        PreparedReplayTape(
            spec=_spec(),
            _execution=np.empty((0, 6), dtype=np.float64),
            _decisions=np.empty((0, 7), dtype=np.int64),
        )

    invalid_ohlc = np.array(
        [[0, 400, 399, 396, 401, 1]],
        dtype=np.int64,
    )
    with pytest.raises(PreparedReplayFormatError, match="candle invariants"):
        PreparedReplayTape(
            spec=_spec(),
            _execution=invalid_ohlc,
            _decisions=np.empty((0, 7), dtype=np.int64),
        )

    large_aligned_timestamp = (np.iinfo(np.int64).max // MINUTE_MS) * MINUTE_MS
    descending = np.array(
        [
            [large_aligned_timestamp, 400, 404, 396, 401, 1],
            [0, 400, 404, 396, 401, 1],
        ],
        dtype=np.int64,
    )
    with pytest.raises(PreparedReplayFormatError, match="strictly increasing"):
        PreparedReplayTape(
            spec=_spec(),
            _execution=descending,
            _decisions=np.empty((0, 7), dtype=np.int64),
        )


@pytest.mark.rust
@pytest.mark.parametrize("ready_index", [4, 10])
def test_prepared_replay_rejects_early_or_delayed_decision_marker(
    ready_index,
):
    tape = PreparedReplayTape.from_candles(_candles(), spec=_spec())
    decisions = tape._decisions[:1].copy()
    decisions[0, 0] = ready_index

    with pytest.raises(PreparedReplayFormatError, match="aggregation schedule"):
        PreparedReplayTape(
            spec=_spec(),
            _execution=tape._execution,
            _decisions=decisions,
        )


@pytest.mark.rust
def test_prepared_replay_marker_schedule_preserves_source_gaps():
    source = _candles()
    gapped = [*source[:5], source[10]]

    tape = PreparedReplayTape.from_candles(gapped, spec=_spec())
    events = list(tape.iter_events(start_time=0, end_time=10 * MINUTE_MS))

    decisions = [
        event.decision_candle for event in events if event.decision_candle is not None
    ]
    assert [decision.timestamp for decision in decisions] == [0]
    assert events[-1].timestamp == 10 * MINUTE_MS


@pytest.mark.rust
def test_prepared_replay_owns_immutable_array_copies():
    original = PreparedReplayTape.from_candles(_candles(), spec=_spec())
    execution = original._execution.copy()
    decisions = original._decisions.copy()

    copied = PreparedReplayTape(
        spec=_spec(),
        _execution=execution,
        _decisions=decisions,
    )
    execution[0, 0] = MINUTE_MS
    decisions[0, 0] = 6

    assert copied._execution.flags.writeable is False
    assert copied._decisions.flags.writeable is False
    assert next(copied.iter_events(start_time=0, end_time=0)).timestamp == 0


def test_prepared_replay_does_not_replace_corrupt_cache(tmp_path):
    path = tmp_path / "corrupt.npz"
    np.savez(path, metadata=np.array("{}"))

    with pytest.raises(PreparedReplayFormatError, match="missing arrays"):
        load_or_prepare_replay(
            path,
            spec=_spec(),
            candle_factory=_candles,
        )


@pytest.mark.rust
@pytest.mark.parametrize(
    "payload",
    [
        b"not-an-npz",
        None,
    ],
)
def test_prepared_replay_normalizes_corrupt_artifact_errors(
    tmp_path,
    payload,
):
    path = tmp_path / "corrupt-artifact.npz"
    if payload is not None:
        path.write_bytes(payload)
    else:
        tape = PreparedReplayTape.from_candles(_candles(), spec=_spec())
        metadata = {
            "contract_version": 1,
            "source_fingerprint": "source-v1",
            "product_id": PRODUCT_ID,
            "source_timeframe": "1m",
            "decision_timeframe": "5m",
            "price_tick": "not-a-decimal",
            "quantity_step": "1",
            "fee_rate_step": "0.00000001",
        }
        np.savez(
            path,
            metadata=np.array(json.dumps(metadata)),
            execution=tape._execution,
            decisions=tape._decisions,
        )

    with pytest.raises(PreparedReplayFormatError):
        PreparedReplayTape.load(path)


def test_file_source_fingerprint_changes_with_file_metadata(tmp_path):
    source = tmp_path / "candles.csv"
    source.write_bytes(b"same-size-a")
    original_stat = source.stat()
    before = file_source_fingerprint(source)

    source.write_bytes(b"same-size-b")
    os.utime(
        source,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )

    assert file_source_fingerprint(source) != before
