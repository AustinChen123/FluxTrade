from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.adapters import okx_resting_order_step as step
from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingState,
    save_okx_resting_state,
)
from src.core.adapters.okx_resting_order_plan import IssuedOrder, OkxPlanScope
from src.core.adapters.okx_spider_transport import OkxBatchCallResult, _decode_batch_request
from src.core.resting_order_strategy import (
    DesiredOrder,
    MinuteBar,
    RestingFill,
    RestingIntent,
    RestingOrderStrategy,
)


INST = "TOY:SWAP"
SCOPE = OkxPlanScope("demo", "toy-account", "toy-strategy", "toy-ref")
NOW = 1_800_000_000_000
BAR = MinuteBar(NOW - 60_000, 10.0, 11.0, 9.0, 10.0)


class FakeClient:
    def __init__(self, events: list[str], fills: list[dict[str, str]] | None = None) -> None:
        self.events = events
        self.fills = fills or []
        self.fill_code = "0"
        self.fail_instrument = False
        self.positions = ["0"]

    def privateGetTradeFills(self, params: dict[str, str]) -> object:
        self.events.append("fills_read")
        return {"code": self.fill_code, "data": self.fills}

    def publicGetPublicInstruments(self, params: dict[str, str]) -> object:
        self.events.append("instrument_read")
        if self.fail_instrument:
            raise RuntimeError("instrument failure")
        return {"code": "0", "data": [{"instId": INST, "instType": "SWAP", "ctType": "linear", "ctMult": "1", "state": "live", "ctVal": "1", "lotSz": "1", "minSz": "1", "tickSz": "1"}]}

    def privateGetAccountPositions(self, params: dict[str, str]) -> object:
        self.events.append("position_read")
        value = self.positions[0]
        rows = [] if value == "0" else [{"instId": INST, "instType": "SWAP", "mgnMode": "cross", "posSide": "net", "pos": value}]
        return {"code": "0", "data": rows}

    def privateGetTradeOrdersPending(self, params: dict[str, str]) -> object:
        self.events.append("orders_read")
        return {"code": "0", "data": []}


class FakeTransport:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.payloads: list[bytes] = []

    def submit(self, payload: bytes) -> OkxBatchCallResult:
        self.events.append("submit")
        self.payloads.append(payload)
        request = _decode_batch_request(payload)
        assert request is not None
        members = cast(list[dict[str, object]], request["members"])
        outcomes = tuple((cast(str, row.get("action_id", row.get("cancel_action_id"))), ("ACCEPTED", "0", "toy-order")) for row in members)
        return OkxBatchCallResult(b"{}", None, outcomes)


class RecordingStrategy(RestingOrderStrategy):
    def __init__(self, events: list[str], on_bar_intent: object = None, fill_intents: list[RestingIntent] | None = None, fail_fill: bool = False) -> None:
        self.events = events
        self.on_bar_intent = on_bar_intent if on_bar_intent is not None else RestingIntent(())
        self.fill_intents = fill_intents or []
        self.fail_fill = fail_fill
        self.fills: list[RestingFill] = []

    def on_fill(self, fill: RestingFill) -> RestingIntent:
        self.events.append("on_fill")
        self.fills.append(fill)
        if self.fail_fill:
            raise RuntimeError("fill handler failure")
        return self.fill_intents[min(len(self.fills) - 1, len(self.fill_intents) - 1)] if self.fill_intents else RestingIntent(())

    def on_bar(self, bar: MinuteBar) -> RestingIntent:
        self.events.append("on_bar")
        return cast(RestingIntent, self.on_bar_intent)


def fill(bill_id: str, *, wire: str = "wire", exec_type: str = "M") -> dict[str, str]:
    return {"instId": INST, "instType": "SWAP", "billId": bill_id, "tradeId": bill_id, "ordId": bill_id,
            "clOrdId": wire, "side": "buy", "fillPx": "10", "fillSz": "1", "ts": str(NOW), "execType": exec_type}


def setup_paths(tmp_path: Path, *, run_id: str = "run_1", cursor: str | None = "10", polled_ms: int = NOW - 60_000) -> tuple[Path, Path]:
    state_path, cursor_path = tmp_path / "state.json", tmp_path / "cursor.json"
    save_okx_resting_state(state_path, OkxRestingState(run_id, 0, {"wire": IssuedOrder("a" * 64, "b" * 64, "local:1")}))
    step.save_okx_fill_cursor(cursor_path, step.OkxFillCursor(run_id, cursor, polled_ms))
    return state_path, cursor_path


def run(tmp_path: Path, events: list[str], *, client: FakeClient | None = None, strategy: RecordingStrategy | None = None, intent: RestingIntent | None = None, now_ms: int = NOW):
    state_path, cursor_path = setup_paths(tmp_path)
    read_client = client or FakeClient(events)
    read_client.events = events
    actor = strategy or RecordingStrategy(events, intent)
    transport = FakeTransport(events)
    result = step.run_okx_resting_step(read_client=read_client, transport=cast(Any, transport), state_path=state_path,
        cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=actor, bar=BAR, now_ms=now_ms)
    return result, read_client, actor, transport, state_path, cursor_path


def test_cursor_exact_bytes_round_trip_and_missing(tmp_path: Path) -> None:
    path = tmp_path / "cursor.json"
    assert step.load_okx_fill_cursor(path) is None
    for cursor in (None, "123"):
        value = step.OkxFillCursor("run_1", cursor, 123)
        step.save_okx_fill_cursor(path, value)
        assert path.read_bytes() == (json.dumps({"schema": "OKX_FILL_CURSOR_V1", "run_id": "run_1", "cursor": cursor, "polled_ms": 123}, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        assert step.load_okx_fill_cursor(path) == value
        assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("raw", [b"{", b"\xff", b"[]", b'{"schema":"OKX_FILL_CURSOR_V2","run_id":"run_1","cursor":null,"polled_ms":1}',
    b'{"schema":"OKX_FILL_CURSOR_V1","run_id":"run_1","cursor":null,"polled_ms":true}',
    b'{"schema":"OKX_FILL_CURSOR_V1","run_id":"run_1","cursor":null,"polled_ms":1.0}',
    b'{"schema":"OKX_FILL_CURSOR_V1","run_id":"run_1","cursor":"","polled_ms":1}',
    b'{"schema":"OKX_FILL_CURSOR_V1","run_id":"run_1","cursor":null,"polled_ms":1,"extra":1}',
    b'{"schema":"OKX_FILL_CURSOR_V1","run_id":"bad:id","cursor":null,"polled_ms":1}'])
def test_cursor_rejects_malformed_files(tmp_path: Path, raw: bytes) -> None:
    path = tmp_path / "cursor.json"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="invalid OKX fill cursor file"):
        step.load_okx_fill_cursor(path)


def test_cursor_constructor_type_bounds_and_atomic_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for args in [("bad:id", None, 1), ("run_1", "", 1), ("run_1", "١", 1), ("run_1", "1a", 1), ("run_1", None, True), ("run_1", None, 0)]:
        with pytest.raises(ValueError):
            step.OkxFillCursor(*args)
    path = tmp_path / "cursor.json"
    step.save_okx_fill_cursor(path, step.OkxFillCursor("run_1", "12", 123))
    previous = path.read_bytes()
    monkeypatch.setattr(step.os, "replace", lambda *_: (_ for _ in ()).throw(OSError("replace failed")))
    with pytest.raises(OSError, match="replace failed"):
        step.save_okx_fill_cursor(path, step.OkxFillCursor("run_1", "13", 124))
    assert path.read_bytes() == previous and list(tmp_path.iterdir()) == [path]


def test_main_order_and_same_cursor_is_saved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    state_path, cursor_path = setup_paths(tmp_path)
    real_save = step.save_okx_fill_cursor

    def save(path: Path, value: step.OkxFillCursor) -> None:
        events.append("cursor_save")
        real_save(path, value)

    monkeypatch.setattr(step, "save_okx_fill_cursor", save)
    client, actor, transport = FakeClient(events), RecordingStrategy(events), FakeTransport(events)
    result = step.run_okx_resting_step(read_client=cast(Any, client), transport=cast(Any, transport), state_path=state_path,
        cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=actor, bar=BAR, now_ms=NOW)
    assert result.cycle is not None and result.flatten is None
    assert events == ["fills_read", "instrument_read", "cursor_save", "on_bar", "instrument_read", "orders_read"]
    assert step.load_okx_fill_cursor(cursor_path) == step.OkxFillCursor("run_1", "10", NOW)


def test_maker_fills_sorted_and_taker_close_fill_not_delivered(tmp_path: Path) -> None:
    events: list[str] = []
    client = FakeClient(events, [fill("12", exec_type="T"), fill("11"), fill("10")])
    state_path, cursor_path = setup_paths(tmp_path, cursor="9")
    transport = FakeTransport(events)
    actor = RecordingStrategy(events)
    result = step.run_okx_resting_step(read_client=cast(Any, client), transport=cast(Any, transport), state_path=state_path,
        cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=actor, bar=BAR, now_ms=NOW)
    assert result.delivered == 2 and result.close_fills == 1 and result.cursor == "12"
    assert len(actor.fills) == 2
    assert step.load_okx_fill_cursor(cursor_path) == step.OkxFillCursor("run_1", "12", NOW)


def test_foreign_own_id_mismatch_is_counted_and_not_delivered(tmp_path: Path) -> None:
    events: list[str] = []
    client = FakeClient(events, [fill("11", wire="not-owned")])
    result, _, actor, _, _, _ = run(tmp_path, events, client=client)
    assert result.foreign == 1 and result.delivered == 0 and actor.fills == []


def test_flatten_from_fill_or_bar_skips_cycle_and_keeps_fill_delivery_sticky(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    fill_flatten = RestingIntent((), flatten=True)
    actor = RecordingStrategy(events, RestingIntent((DesiredOrder("buy", 9.0, 1.0),)), [fill_flatten])
    client = FakeClient(events, [fill("11")])
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(step, "run_okx_flatten", lambda **kwargs: calls.append(kwargs) or cast(Any, "flatten"))
    monkeypatch.setattr(step, "run_okx_resting_cycle", lambda **kwargs: pytest.fail("cycle called"))
    result, _, _, _, _, _ = run(tmp_path, events, client=client, strategy=actor)
    assert result.flatten == "flatten" and result.cycle is None
    assert calls[0]["fallback_run_id"] == "run_1-s"


def test_bar_flatten_with_nonempty_orders_calls_only_flatten(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    intent = RestingIntent((DesiredOrder("buy", 9.0, 1.0),), flatten=True)
    calls: list[str] = []
    monkeypatch.setattr(step, "run_okx_flatten", lambda **_kwargs: calls.append("flatten") or cast(Any, "flatten"))
    monkeypatch.setattr(step, "run_okx_resting_cycle", lambda **_kwargs: pytest.fail("cycle called"))
    result, _, _, _, _, _ = run(tmp_path, events, intent=intent)
    assert result.flatten == "flatten" and result.cycle is None and calls == ["flatten"]


def test_real_cycle_submits_post_only_place(tmp_path: Path) -> None:
    events: list[str] = []
    state_path, cursor_path = setup_paths(tmp_path)
    transport = FakeTransport(events)
    step.run_okx_resting_step(read_client=cast(Any, FakeClient(events)), transport=cast(Any, transport), state_path=state_path,
        cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events, RestingIntent((DesiredOrder("buy", 9.0, 1.0),))), bar=BAR, now_ms=NOW)
    places = [_decode_batch_request(payload) for payload in transport.payloads]
    assert len(places) == 1 and places[0] is not None
    assert places[0]["operation"] == "PLACE"
    member = cast(list[dict[str, object]], places[0]["members"])[0]
    assert cast(dict[str, object], member["request_payload"])["ordType"] == "post_only"


def test_real_flatten_submits_reduce_only_market_close(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    state_path, cursor_path = setup_paths(tmp_path, run_id="x" * 36)
    client = FakeClient(events)
    client.positions = ["2"]
    transport = FakeTransport(events)
    result = step.run_okx_resting_step(read_client=cast(Any, client), transport=cast(Any, transport), state_path=state_path,
        cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events, RestingIntent((), flatten=True)), bar=BAR, now_ms=NOW)
    request = _decode_batch_request(transport.payloads[0])
    assert result.flatten is not None and request is not None and request["operation"] == "PLACE"
    member = cast(list[dict[str, object]], request["members"])[0]
    payload = cast(dict[str, object], member["request_payload"])
    assert payload["ordType"] == "market" and payload["reduceOnly"] is True


@pytest.mark.parametrize("failure", ["on_fill", "instrument", "page", "envelope"])
def test_pre_cursor_failures_preserve_cursor_and_skip_bar_or_submit(tmp_path: Path, failure: str) -> None:
    events: list[str] = []
    client = FakeClient(events)
    actor = RecordingStrategy(events, fail_fill=failure == "on_fill")
    if failure == "on_fill":
        client.fills = [fill("11")]
    elif failure == "instrument":
        client.fail_instrument = True
    elif failure == "page":
        client.fills = [fill(str(index + 11)) for index in range(100)]
    elif failure == "envelope":
        client.fill_code = "1"
    state_path, cursor_path = setup_paths(tmp_path)
    before = cursor_path.read_bytes()
    transport = FakeTransport(events)
    with pytest.raises((ValueError, RuntimeError)):
        step.run_okx_resting_step(read_client=cast(Any, client), transport=cast(Any, transport), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=actor, bar=BAR, now_ms=NOW)
    assert cursor_path.read_bytes() == before
    assert "on_bar" not in events and transport.payloads == []


def test_save_failure_after_fill_does_not_call_bar_or_submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    client = FakeClient(events, [fill("11")])
    actor = RecordingStrategy(events)
    state_path, cursor_path = setup_paths(tmp_path)
    monkeypatch.setattr(step, "save_okx_fill_cursor", lambda *_: (_ for _ in ()).throw(OSError("save")))
    transport = FakeTransport(events)
    with pytest.raises(OSError, match="save"):
        step.run_okx_resting_step(read_client=cast(Any, client), transport=cast(Any, transport), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=actor, bar=BAR, now_ms=NOW)
    assert len(actor.fills) == 1 and "on_bar" not in events and not transport.payloads


def test_on_bar_invalid_type_occurs_after_cursor_save_and_before_submit(tmp_path: Path) -> None:
    events: list[str] = []
    state_path, cursor_path = setup_paths(tmp_path)
    actor = RecordingStrategy(events, cast(Any, object()))
    transport = FakeTransport(events)
    with pytest.raises(ValueError, match="on_bar"):
        step.run_okx_resting_step(read_client=cast(Any, FakeClient(events)), transport=cast(Any, transport), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=actor, bar=BAR, now_ms=NOW)
    assert step.load_okx_fill_cursor(cursor_path) == step.OkxFillCursor("run_1", "10", NOW)
    assert not transport.payloads


@pytest.mark.parametrize("age_delta,valid", [(172_800_000, True), (172_800_001, False), (0, True), (-1, False)])
def test_cursor_time_gap_boundary(tmp_path: Path, age_delta: int, valid: bool) -> None:
    events: list[str] = []
    state_path, cursor_path = setup_paths(tmp_path, polled_ms=NOW - age_delta)
    client, transport = FakeClient(events), FakeTransport(events)
    def call() -> object:
        return step.run_okx_resting_step(read_client=cast(Any, client), transport=cast(Any, transport), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events), bar=BAR, now_ms=NOW)
    if valid:
        call()
        assert "fills_read" in events
    else:
        with pytest.raises(ValueError):
            call()
        assert events == []


def test_state_cursor_identity_and_missing_or_malformed_fail_before_reads(tmp_path: Path) -> None:
    state_path, cursor_path = setup_paths(tmp_path, run_id="other")
    step.save_okx_fill_cursor(cursor_path, step.OkxFillCursor("run_1", "10", NOW - 60_000))
    events: list[str] = []
    with pytest.raises(ValueError, match="differs"):
        step.run_okx_resting_step(read_client=cast(Any, FakeClient(events)), transport=cast(Any, FakeTransport(events)), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events), bar=BAR, now_ms=NOW)
    assert events == []


@pytest.mark.parametrize("state_mode", ["missing", "malformed"])
def test_state_failure_is_before_cursor_or_exchange_reads(tmp_path: Path, state_mode: str) -> None:
    state_path, cursor_path = setup_paths(tmp_path)
    if state_mode == "missing":
        state_path.unlink()
    else:
        state_path.write_text("{")
    events: list[str] = []
    with pytest.raises(ValueError):
        step.run_okx_resting_step(read_client=cast(Any, FakeClient(events)), transport=cast(Any, FakeTransport(events)), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events), bar=BAR, now_ms=NOW)
    assert events == []


@pytest.mark.parametrize("cursor_mode", ["missing", "malformed"])
def test_cursor_file_failure_is_before_exchange_reads(tmp_path: Path, cursor_mode: str) -> None:
    state_path, cursor_path = setup_paths(tmp_path)
    if cursor_mode == "missing":
        cursor_path.unlink()
    else:
        cursor_path.write_text("{")
    events: list[str] = []
    with pytest.raises(ValueError):
        step.run_okx_resting_step(read_client=cast(Any, FakeClient(events)), transport=cast(Any, FakeTransport(events)), state_path=state_path,
            cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events), bar=BAR, now_ms=NOW)
    assert events == []


@pytest.mark.parametrize("field,value", [("inst_id", "I" * 65), ("scope", object()), ("strategy", object()), ("bar", object()), ("now_ms", True), ("now_ms", 0)])
def test_entry_validation_has_zero_io(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, value: object) -> None:
    state_path, cursor_path = setup_paths(tmp_path)
    state_before, cursor_before = state_path.read_bytes(), cursor_path.read_bytes()
    events: list[str] = []
    kwargs: dict[str, object] = {"read_client": cast(Any, FakeClient(events)), "transport": cast(Any, FakeTransport(events)),
        "state_path": state_path, "cursor_path": cursor_path, "inst_id": INST, "scope": SCOPE,
        "strategy": RecordingStrategy(events), "bar": BAR, "now_ms": NOW}
    kwargs[field] = value
    monkeypatch.setattr(step, "load_okx_resting_state", lambda path: events.append("load_state"))
    monkeypatch.setattr(step, "load_okx_fill_cursor", lambda path: events.append("load_cursor"))
    with pytest.raises(ValueError):
        step.run_okx_resting_step(**cast(Any, kwargs))
    assert events == [] and state_path.read_bytes() == state_before and cursor_path.read_bytes() == cursor_before


def test_now_ms_one_is_accepted(tmp_path: Path) -> None:
    events: list[str] = []
    state_path, cursor_path = setup_paths(tmp_path, polled_ms=1)
    result = step.run_okx_resting_step(read_client=FakeClient(events), transport=cast(Any, FakeTransport(events)),
        state_path=state_path, cursor_path=cursor_path, inst_id=INST, scope=SCOPE, strategy=RecordingStrategy(events), bar=BAR, now_ms=1)
    assert result.cycle is not None and step.load_okx_fill_cursor(cursor_path) == step.OkxFillCursor("run_1", "10", 1)


def test_cursor_save_fsyncs_file_and_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    synced: list[int] = []
    real_fsync = step.os.fsync
    monkeypatch.setattr(step.os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd))[1])
    step.save_okx_fill_cursor(tmp_path / "cursor.json", step.OkxFillCursor("run_1", "12", 123))
    assert len(synced) == 2
