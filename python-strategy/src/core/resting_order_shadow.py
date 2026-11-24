import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, cast

from src.core.resting_order_sim import iter_simulate
from src.core.resting_order_strategy import MinuteBar, RestingOrderStrategy
from src.core.strategy_loader import StrategyLoader


def _load_strategy(
    pack: str, catalog_id: str
) -> type[RestingOrderStrategy]:
    loaded = StrategyLoader.scan_resting_order_sources(pack)
    strategy_class = loaded.get(catalog_id)
    if not isinstance(strategy_class, type):
        raise ValueError(f"resting-order catalog id not found: {catalog_id}")
    return strategy_class


def _validate_params(params: object) -> dict[str, object]:
    if type(params) is not dict or any(
        type(value) not in (int, float, str, bool) for value in params.values()
    ):
        raise ValueError("params must be an object of JSON scalar values")
    return params


def _write(stream, record: dict[str, object]) -> None:
    stream.write(json.dumps(record, separators=(",", ":")) + "\n")
    stream.flush()


def _type_exact_equal(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        actual_dict = cast(dict[object, object], actual)
        expected_dict = cast(dict[object, object], expected)
        return actual_dict.keys() == expected_dict.keys() and all(
            _type_exact_equal(actual_dict[key], expected_dict[key])
            for key in actual_dict
        )
    if isinstance(actual, list):
        actual_list = cast(list[object], actual)
        expected_list = cast(list[object], expected)
        return len(actual_list) == len(expected_list) and all(
            _type_exact_equal(left, right)
            for left, right in zip(actual_list, expected_list)
        )
    return bool(actual == expected)


def run_shadow(
    *,
    pack: str,
    catalog_id: str,
    params: dict[str, object],
    inst: str,
    maker_fee: float,
    taker_fee: float,
    start_ms: int,
    minutes: int,
    out: str,
    fetch_window: Callable[[str, int, int], Any],
    now_ms: Callable[[], int] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    params = _validate_params(params)
    if type(start_ms) is not int or start_ms % 60_000:
        raise ValueError("start_ms must be minute-aligned")
    if type(minutes) is not int or minutes <= 0:
        raise ValueError("minutes must be positive")
    strategy_class = _load_strategy(pack, catalog_id)
    strategy = strategy_class(**params)
    clock = now_ms or (lambda: int(time.time() * 1000))
    target_end = start_ms + minutes * 60_000
    next_start = start_ms
    last_bar: int | None = None
    with open(out, "x", encoding="utf-8") as stream:
        _write(
            stream,
            {
                "kind": "header",
                "inst": inst,
                "catalog_id": catalog_id,
                "catalog_sha256": getattr(
                    strategy_class, "__fluxtrade_catalog_sha256__"
                ),
                "readiness": getattr(strategy_class, "__fluxtrade_readiness__"),
                "params": params,
                "maker_fee": maker_fee,
                "taker_fee": taker_fee,
                "start_ms": start_ms,
            },
        )
        def minute_bars():
            nonlocal next_start, last_bar
            while next_start < target_end:
                closed_end = min((clock() // 60_000) * 60_000, target_end)
                if closed_end > next_start:
                    try:
                        window = fetch_window(inst, next_start, closed_end)
                    except Exception as error:
                        _write(
                            stream,
                            {
                                "kind": "fetch_error",
                                "ts_ms": next_start,
                                "error": str(error),
                            },
                        )
                    else:
                        missing = iter(window.missing)
                        next_missing = next(missing, None)
                        for bar in window.bars:
                            while next_missing is not None and next_missing < bar.ts_ms:
                                _write(
                                    stream,
                                    {"kind": "missing", "ts_ms": next_missing},
                                )
                                next_missing = next(missing, None)
                            if last_bar is not None and bar.ts_ms <= last_bar:
                                continue
                            _write(
                                stream,
                                {"record_type": "bar", "kind": "bar", **asdict(bar)},
                            )
                            last_bar = bar.ts_ms
                            yield MinuteBar(
                                bar.ts_ms, bar.open, bar.high, bar.low, bar.close
                            )
                        while next_missing is not None:
                            _write(stream, {"kind": "missing", "ts_ms": next_missing})
                            next_missing = next(missing, None)
                        next_start = closed_end
                if next_start < target_end:
                    current = clock()
                    next_minute = (current // 60_000 + 1) * 60_000
                    sleep((next_minute - current) / 1000 + 0.05)

        for record in iter_simulate(
            strategy, minute_bars(), maker_fee, taker_fee
        ):
            _write(stream, {"record_type": "sim", **asdict(record)})


def replay_shadow(*, replay: str, pack: str) -> tuple[int, str]:
    records = [
        json.loads(line)
        for line in Path(replay).read_text(encoding="utf-8").splitlines()
    ]
    if not records or records[0].get("kind") != "header":
        raise ValueError("JSONL must begin with a header record")
    header = records[0]
    loaded = StrategyLoader.scan_resting_order_sources(pack)
    strategy_class = loaded.get(header.get("catalog_id"))
    if not isinstance(strategy_class, type):
        return 1, "catalog id not found in pack"
    if getattr(strategy_class, "__fluxtrade_catalog_sha256__", None) != header.get(
        "catalog_sha256"
    ):
        return 1, "catalog sha256 differs"
    if getattr(strategy_class, "__fluxtrade_readiness__", None) != header.get(
        "readiness"
    ):
        return 1, "catalog readiness differs"
    params = _validate_params(header.get("params"))
    strategy = strategy_class(**params)
    bars = [
        MinuteBar(row["ts_ms"], row["open"], row["high"], row["low"], row["close"])
        for row in records[1:]
        if row.get("record_type") == "bar"
    ]
    actual = [
        asdict(record)
        for record in iter_simulate(
            strategy, bars, header["maker_fee"], header["taker_fee"]
        )
    ]
    expected = [
        {key: value for key, value in row.items() if key not in ("kind", "record_type")}
        | {"kind": row["kind"]}
        for row in records[1:]
        if row.get("record_type") == "sim"
    ]
    if not _type_exact_equal(actual, expected):
        index = next(
            (
                i
                for i, pair in enumerate(zip(actual, expected))
                if not _type_exact_equal(pair[0], pair[1])
            ),
            min(len(actual), len(expected)),
        )
        actual_record = actual[index] if index < len(actual) else None
        expected_record = expected[index] if index < len(expected) else None
        return (
            1,
            f"simulator record {index} differs: expected={expected_record!r} actual={actual_record!r}",
        )
    return 0, "replay matches"
