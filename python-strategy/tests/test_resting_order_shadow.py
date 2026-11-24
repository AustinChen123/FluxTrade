import ast
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from test_strategy_loader import _read_only_tree, _write_rest_catalog
from src.core.adapters.okx_public_candles import fetch_closed_1m
from src.core.resting_order_shadow import replay_shadow, run_shadow
from src.core.strategy_loader import StrategyLoader


TOY_CODE = """
from src.core.resting_order_strategy import RestingOrderStrategy, RestingIntent, DesiredOrder

class ToyRestingStrategy(RestingOrderStrategy):
    def on_bar(self, bar):
        return RestingIntent((DesiredOrder("buy", 99.0, 1.0),))
    def on_fill(self, fill):
        return RestingIntent((), flatten=True)
"""


def _fetcher(http_get: Any) -> Any:
    return lambda inst, start, end: fetch_closed_1m(inst, start, end, http_get)


def make_pack(root: Path, code: str = TOY_CODE) -> None:
    _write_rest_catalog(root, code=code)


def payload(ts: int, close: str) -> bytes:
    return json.dumps(
        {
            "code": "0",
            "data": [[str(ts), close, close, close, close, "1", "1", "1", "1"]],
        }
    ).encode()


def payload_rows() -> bytes:
    rows = [
        [str(ts), price, price, price, price, "1", "1", "1", "1"]
        for ts, price in ((0, "100"), (60_000, "98"))
    ]
    return json.dumps({"code": "0", "data": rows}).encode()


def run(
    tmp_path: Path, fetch, *, now=None, start=0, minutes=1, output="run.jsonl"
) -> Path:
    pack = tmp_path / "pack"
    pack.mkdir(exist_ok=True)
    if not (pack / StrategyLoader.CATALOG_NAME).exists():
        make_pack(pack)
    for item in [pack, *pack.rglob("*")]:
        item.chmod(0o555 if item.is_dir() else 0o444)
    result = tmp_path / output
    clock = iter(now or [120_000, 120_000])
    try:
        run_shadow(
            pack=str(pack),
            catalog_id="toy_resting_v1",
            params={},
            inst="BTC-USDT",
            maker_fee=0.001,
            taker_fee=0.002,
            start_ms=start,
            minutes=minutes,
            out=str(result),
            fetch_window=_fetcher(fetch),
            now_ms=lambda: next(clock),
            sleep=lambda _: None,
        )
    finally:
        for item in [pack, *pack.rglob("*")]:
            item.chmod(0o755 if item.is_dir() else 0o644)
    return result


def test_live_oracle_and_replay(tmp_path: Path) -> None:
    path = run(tmp_path, lambda _: payload_rows(), now=[120_000, 120_000], minutes=2)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows[0] == {
        "kind": "header",
        "inst": "BTC-USDT",
        "catalog_id": "toy_resting_v1",
        "catalog_sha256": hashlib.sha256(
            (tmp_path / "pack" / StrategyLoader.CATALOG_NAME).read_bytes()
        ).hexdigest(),
        "readiness": "RESEARCH_VALIDATED",
        "params": {},
        "maker_fee": 0.001,
        "taker_fee": 0.002,
        "start_ms": 0,
    }
    assert [
        (
            row["record_type"],
            row["kind"],
            row.get("ts_ms"),
            row.get("price"),
            row.get("gross"),
            row.get("fee"),
        )
        for row in rows[1:]
    ] == [
        ("bar", "bar", 0, None, None, None),
        ("sim", "bar", 0, None, None, None),
        ("bar", "bar", 60_000, None, None, None),
        ("sim", "bar", 60_000, None, None, None),
        ("sim", "fill", 60_000, 99.0, None, 0.099),
        ("sim", "flatten", 60_000, 98.0, None, 0.196),
        ("sim", "settle", 60_000, None, -1.0, 0.29500000000000004),
    ]
    with _read_only_tree(tmp_path / "pack"):
        assert replay_shadow(replay=str(path), pack=str(tmp_path / "pack")) == (
            0,
            "replay matches",
        )


def test_records_flushed_before_next_fetch_and_overlapping_bar_skipped(
    tmp_path: Path,
) -> None:
    pack = tmp_path / "pack"
    pack.mkdir()
    make_pack(pack)
    for item in [pack, *pack.rglob("*")]:
        item.chmod(0o555 if item.is_dir() else 0o444)
    result = tmp_path / "live.jsonl"
    now_values = iter([60_000, 60_000, 120_000, 120_000, 120_000])
    calls = 0
    callback_results: list[bool] = []

    def fetch(url: str) -> bytes:
        nonlocal calls
        calls += 1
        if calls == 2:
            lines = result.read_text().splitlines()
            callback_results.append(
                any(json.loads(line).get("record_type") == "sim" for line in lines)
            )
        ts = 0 if calls == 1 else 60_000
        return payload(ts, "98")

    try:
        run_shadow(
            pack=str(pack),
            catalog_id="toy_resting_v1",
            params={},
            inst="BTC-USDT",
            maker_fee=0.001,
            taker_fee=0.002,
            start_ms=0,
            minutes=2,
            out=str(result),
            fetch_window=_fetcher(fetch),
            now_ms=lambda: next(now_values),
            sleep=lambda _: None,
        )
    finally:
        for item in [pack, *pack.rglob("*")]:
            item.chmod(0o755 if item.is_dir() else 0o644)
    bars = [
        json.loads(line)["ts_ms"]
        for line in result.read_text().splitlines()
        if json.loads(line).get("record_type") == "bar"
    ]
    assert bars == [0, 60_000]
    assert callback_results == [True]
    assert not any(
        json.loads(line).get("kind") == "fetch_error"
        for line in result.read_text().splitlines()
    )


def test_failed_fetch_waits_for_next_current_minute_boundary(tmp_path: Path) -> None:
    pack = tmp_path / "pack"
    pack.mkdir()
    make_pack(pack)
    for item in [pack, *pack.rglob("*")]:
        item.chmod(0o555 if item.is_dir() else 0o444)
    sleeps: list[float] = []
    calls = 0

    class StopAfterTwoRetries(Exception):
        pass

    def fail(_: str) -> bytes:
        nonlocal calls
        calls += 1
        raise ValueError("offline")

    def stop_on_third_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) == 2:
            raise StopAfterTwoRetries

    try:
        with pytest.raises(StopAfterTwoRetries):
            run_shadow(
                pack=str(pack), catalog_id="toy_resting_v1", params={}, inst="BTC-USDT",
                maker_fee=0.001, taker_fee=0.002, start_ms=0, minutes=1,
                out=str(tmp_path / "failed.jsonl"), fetch_window=_fetcher(fail),
                now_ms=lambda: 120_000, sleep=stop_on_third_sleep,
            )
    finally:
        for item in [pack, *pack.rglob("*")]:
            item.chmod(0o755 if item.is_dir() else 0o644)
    assert calls == 2
    assert sleeps == [60.05, 60.05]


def test_multicycle_shadow_keeps_orders_across_polling_cycles(tmp_path: Path) -> None:
    cycle_times = iter(
        [
            60_000, 60_000,
            120_000, 120_000,
            180_000, 180_000,
            240_000, 240_000,
        ]
    )
    closes = iter(["100", "100", "100", "98"])
    path = run(
        tmp_path,
        lambda url: payload(int(url.rsplit("after=", 1)[1]) - 60_000, next(closes)),
        now=cycle_times,
        minutes=4,
    )
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    fills = [row for row in rows if row.get("record_type") == "sim" and row["kind"] == "fill"]
    assert fills == [
        {
            "record_type": "sim",
            "kind": "fill",
            "ts_ms": 180_000,
            "touch_buy": None,
            "touch_sell": None,
            "side": "buy",
            "price": 99.0,
            "quantity": 1.0,
            "fee": 0.099,
            "gross": None,
        }
    ]
    with _read_only_tree(tmp_path / "pack"):
        assert replay_shadow(replay=str(path), pack=str(tmp_path / "pack")) == (
            0,
            "replay matches",
        )


def test_fetch_error_recovers_and_missing_is_recorded(tmp_path: Path) -> None:
    calls = 0

    def fetch(url: str) -> bytes:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("offline")
        return json.dumps({"code": "0", "data": []}).encode()

    path = run(tmp_path, fetch, now=[60_000, 120_000, 120_000], minutes=1)
    kinds = [json.loads(line)["kind"] for line in path.read_text().splitlines()]
    assert kinds == ["header", "fetch_error", "missing"]
    assert calls == 2


def test_replay_rejects_catalog_digest_and_record_edits(tmp_path: Path) -> None:
    path = run(tmp_path, lambda _: payload_rows(), now=[120_000, 120_000], minutes=2)
    pack = tmp_path / "pack"
    with _read_only_tree(pack):
        assert replay_shadow(replay=str(path), pack=str(pack))[0] == 0
    changed = tmp_path / "changed.jsonl"
    lines = path.read_text().splitlines()
    simulator = json.loads(lines[2])
    simulator["touch_buy"] = True
    lines[2] = json.dumps(simulator)
    changed.write_text("\n".join(lines) + "\n")
    with _read_only_tree(pack):
        code, message = replay_shadow(replay=str(changed), pack=str(pack))
    assert code == 1 and "record 0" in message
    catalog_path = pack / StrategyLoader.CATALOG_NAME
    catalog = json.loads(catalog_path.read_text())
    catalog["resting_order_strategies"][0]["display_name"] = "Changed"
    catalog_path.chmod(0o644)
    catalog_path.write_text(json.dumps(catalog))
    catalog_path.chmod(0o444)
    with _read_only_tree(pack):
        assert "sha256" in replay_shadow(replay=str(path), pack=str(pack))[1]


@pytest.mark.parametrize(
    ("field", "replacement"),
    [("touch_buy", 0), ("touch_sell", 0), ("touch_buy", 1), ("touch_sell", 1)],
)
def test_replay_rejects_bool_fields_replaced_by_int(
    tmp_path: Path, field: str, replacement: int
) -> None:
    path = run(tmp_path, lambda _: payload_rows(), now=[120_000, 120_000], minutes=2)
    lines = path.read_text().splitlines()
    sim_index = next(
        i for i, line in enumerate(lines) if json.loads(line).get("record_type") == "sim"
    )
    record = json.loads(lines[sim_index])
    record[field] = replacement
    lines[sim_index] = json.dumps(record)
    changed = tmp_path / f"{field}-{replacement}.jsonl"
    changed.write_text("\n".join(lines) + "\n")
    with _read_only_tree(tmp_path / "pack"):
        code, message = replay_shadow(replay=str(changed), pack=str(tmp_path / "pack"))
    assert code == 1 and "record 0" in message


def test_replay_rejects_int_field_changed_to_float(tmp_path: Path) -> None:
    path = run(tmp_path, lambda _: payload_rows(), now=[120_000, 120_000], minutes=2)
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    sim_index = next(
        i for i, record in enumerate(lines) if record.get("record_type") == "sim"
    )
    assert type(lines[sim_index]["ts_ms"]) is int
    lines[sim_index]["ts_ms"] = float(lines[sim_index]["ts_ms"])
    changed = tmp_path / "int-to-float.jsonl"
    changed.write_text("\n".join(json.dumps(row) for row in lines) + "\n")
    with _read_only_tree(tmp_path / "pack"):
        result = replay_shadow(replay=str(changed), pack=str(tmp_path / "pack"))
    assert result[0] == 1 and "record 0" in result[1]


@pytest.mark.parametrize("params", [[1], {"bad": []}, None])
def test_invalid_params_rejected_before_load(tmp_path: Path, params) -> None:
    pack = tmp_path / "missing"
    with pytest.raises(ValueError, match="params"):
        run_shadow(
            pack=str(pack),
            catalog_id="missing",
            params=params,
            inst="X",
            maker_fee=0,
            taker_fee=0,
            start_ms=0,
            minutes=1,
            out=str(tmp_path / "out"),
            fetch_window=_fetcher(lambda _: b""),
        )


def test_exclusive_output_and_unaligned_start(tmp_path: Path) -> None:
    pack = tmp_path / "pack"
    pack.mkdir()
    make_pack(pack)
    out = tmp_path / "exists"
    out.write_text("keep")
    kwargs: dict[str, Any] = dict(
        pack=str(pack),
        catalog_id="toy_resting_v1",
        params={},
        inst="X",
        maker_fee=0,
        taker_fee=0,
        minutes=1,
        out=str(out),
        fetch_window=_fetcher(lambda _: b""),
    )
    with _read_only_tree(pack):
        with pytest.raises(FileExistsError):
            run_shadow(**kwargs, start_ms=0)
    with _read_only_tree(pack):
        with pytest.raises(ValueError, match="aligned"):
            run_shadow(**{**kwargs, "out": str(tmp_path / "new")}, start_ms=1)


def test_isolation_imports() -> None:
    path = Path(__file__).parents[1] / "src/core/resting_order_shadow.py"
    tree = ast.parse(path.read_text())
    imports = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    imports.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert not any(
        name.startswith(("ccxt", "src.core.execution", "src.core.order", "sqlalchemy"))
        for name in imports
        if name
    )
    assert all(
        name.startswith(
            (
                "src.core.",
                "collections",
                "dataclasses",
                "json",
                "pathlib",
                "time",
                "typing",
            )
        )
        for name in imports
        if name
    )
