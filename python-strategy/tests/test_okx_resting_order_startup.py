from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.adapters import okx_resting_order_startup as startup
from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingState,
    load_okx_resting_state,
    save_okx_resting_state,
)
from src.core.adapters.okx_resting_order_plan import IssuedOrder, OkxOwnRestingOrder, OkxPlanScope
from src.core.adapters.okx_resting_order_reads import OkxRestingSnapshot
from src.core.adapters.okx_spider_transport import OkxBatchCallResult


SCOPE = OkxPlanScope("demo", "toy-account", "toy-strategy", "toy-ref")
INST = "TOY:SWAP"


class FakeClient:
    def __init__(self, positions: list[Decimal], orders: list[tuple[OkxOwnRestingOrder, ...]]) -> None:
        self.positions = positions
        self.orders = orders
        self.calls: list[str] = []

    def privateGetAccountPositions(self, params: dict[str, str]) -> object:
        self.calls.append("position")
        value = self.positions.pop(0) if len(self.positions) > 1 else self.positions[0]
        if isinstance(value, Exception):
            raise value
        if value == 0:
            return {"code": "0", "data": []}
        return {"code": "0", "data": [{"instId": INST, "instType": "SWAP", "mgnMode": "cross", "posSide": "net", "pos": str(value)}]}

    def publicGetPublicInstruments(self, params: dict[str, str]) -> object:
        self.calls.append("instrument")
        return {"code": "0", "data": [{"instId": INST, "instType": "SWAP", "ctType": "linear", "ctMult": "1", "state": "live", "ctVal": "1", "lotSz": "1", "minSz": "1", "tickSz": "1"}]}

    def privateGetTradeOrdersPending(self, params: dict[str, str]) -> object:
        self.calls.append("orders")
        rows = self.orders.pop(0) if len(self.orders) > 1 else self.orders[0]
        return {"code": "0", "data": [{"instId": INST, "state": "live", "side": order.side, "ordId": order.ord_id, "clOrdId": order.cl_ord_id, "px": str(order.px), "sz": str(order.remaining_sz), "accFillSz": "0"} for order in rows]}

    def privateGetTradeFills(self, params: dict[str, str]) -> object:
        self.calls.append("cursor")
        return {"code": "0", "data": []}


class FakeTransport:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.submits = 0

    def submit(self, payload: bytes) -> OkxBatchCallResult:
        self.events.append("submit")
        self.submits += 1
        return OkxBatchCallResult(b"{}", None, ())


def order(wire: str) -> OkxOwnRestingOrder:
    return OkxOwnRestingOrder("123", wire, "buy", Decimal("1"), Decimal("1"))


def run(path: Path, client: FakeClient, events: list[str], *, run_id: str = "new-run", attempts: int = 2):
    transport = FakeTransport(events)
    return startup.run_okx_resting_startup(
        read_client=client,
        transport=transport,
        state_path=path,
        inst_id=INST,
        scope=SCOPE,
        run_id=run_id,
        max_attempts=attempts,
        settle=lambda: events.append("settle"),
    )


def test_flat_start_is_one_attempt_and_writes_fresh_state_after_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    events: list[str] = []
    client = FakeClient([Decimal("0")], [()])
    real_save = startup.save_okx_resting_state

    def save(target: Path, state: OkxRestingState) -> None:
        events.append("save")
        real_save(target, state)

    monkeypatch.setattr(startup, "save_okx_resting_state", save)
    result = run(path, client, events)
    assert result.clean and result.attempts == 1 and result.position == 0 and result.cursor is None
    assert client.calls == ["position", "instrument", "orders", "position", "orders", "cursor"]
    assert events == ["settle", "save"]
    assert load_okx_resting_state(path) == OkxRestingState("new-run", 0, {})
    assert path.read_bytes() == b'{"issued":{},"next_seq":0,"run_id":"new-run","schema":"OKX_RESTING_STATE_V1"}\n'


def test_position_re_read_zero_becomes_clean_after_first_attempt(tmp_path: Path) -> None:
    events: list[str] = []
    client = FakeClient([Decimal("2"), Decimal("0")], [(), ()])
    result = run(tmp_path / "state.json", client, events, attempts=3)
    assert result.clean and result.attempts == 1 and result.position == 0
    assert client.calls.count("position") == 2
    assert events == ["submit", "settle"]


def test_exhausted_position_retries_do_not_change_state_or_read_cursor(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_bytes(b"preserve exactly")
    before = path.read_bytes()
    events: list[str] = []
    client = FakeClient([Decimal("1"), Decimal("1"), Decimal("1")], [(), (), ()])
    result = run(path, client, events, attempts=2)
    assert not result.clean and result.attempts == 2 and result.position == 1 and result.cursor is None
    assert path.read_bytes() == before
    assert "cursor" not in client.calls
    assert events.count("submit") == 2 and events.count("settle") == 2


@pytest.mark.parametrize("wire,clean,stray", [("owned", False, 0), ("foreign", True, 0), ("raunlisted", False, 1)])
def test_order_ownership_and_ra_prefix_determine_cleanliness(tmp_path: Path, wire: str, clean: bool, stray: int) -> None:
    path = tmp_path / "state.json"
    if wire == "owned":
        save_okx_resting_state(path, OkxRestingState("previous", 0, {wire: IssuedOrder("1" * 64, "2" * 64, "old:1")}))
    events: list[str] = []
    client = FakeClient([Decimal("0")], [(order(wire),)])
    result = run(path, client, events, attempts=1)
    assert result.clean is clean and result.stray_ra_orders == stray
    assert ("cursor" in client.calls) is clean


def test_corrupt_state_uses_fallback_ids_without_rethrowing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    path.write_text("{")
    ids: list[str] = []

    def flatten(**kwargs: object) -> object:
        ids.append(cast(str, kwargs["fallback_run_id"]))
        return object()

    positions = iter([Decimal("1"), Decimal("1")])
    monkeypatch.setattr(startup, "run_okx_flatten", flatten)
    monkeypatch.setattr(startup, "read_okx_net_position", lambda *_args: next(positions))
    monkeypatch.setattr(startup, "read_okx_own_resting_orders", lambda *_args: OkxRestingSnapshot((), 0))
    result = run(path, FakeClient([Decimal("0")], [()]), [], attempts=2)
    assert not result.clean and ids == ["new-run-f0", "new-run-f1"]


def test_same_run_id_rejected_before_exchange_reads(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("same", 0, {}))
    client = FakeClient([Decimal("0")], [()])
    with pytest.raises(ValueError, match="differ"):
        run(path, client, [], run_id="same")
    assert client.calls == []


@pytest.mark.parametrize("run_id,attempts,valid", [("x" * 36, 1, True), ("x" * 37, 1, False), ("valid", 0, False), ("valid", 1, True), ("valid", 10, True), ("valid", 11, False), ("valid", True, False)])
def test_entry_bounds(run_id: str, attempts: object, valid: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[str] = []
    monkeypatch.setattr(startup, "load_okx_resting_state", lambda *_args: reads.append("state") or None)
    if valid:
        monkeypatch.setattr(startup, "run_okx_flatten", lambda **_kwargs: object())
        monkeypatch.setattr(startup, "read_okx_net_position", lambda *_args: Decimal("0"))
        monkeypatch.setattr(startup, "read_okx_own_resting_orders", lambda *_args: OkxRestingSnapshot((), 0))
        monkeypatch.setattr(startup, "init_okx_fill_cursor", lambda *_args: None)
        monkeypatch.setattr(startup, "save_okx_resting_state", lambda *_args: None)
        startup.run_okx_resting_startup(
            read_client=cast(Any, object()), transport=cast(Any, object()), state_path=tmp_path / "state.json",
            inst_id=INST, scope=SCOPE, run_id=run_id, max_attempts=cast(int, attempts), settle=lambda: None,
        )
        assert reads == ["state"]
    else:
        with pytest.raises(ValueError):
            startup.run_okx_resting_startup(
                read_client=cast(Any, object()), transport=cast(Any, object()), state_path=tmp_path / "state.json",
                inst_id=INST, scope=SCOPE, run_id=run_id, max_attempts=cast(int, attempts), settle=lambda: None,
            )
        assert reads == []


def test_read_failure_after_flatten_propagates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    monkeypatch.setattr(startup, "run_okx_flatten", lambda **_kwargs: events.append("flatten") or object())
    monkeypatch.setattr(startup, "read_okx_net_position", lambda *_args: (_ for _ in ()).throw(RuntimeError("read")))
    with pytest.raises(RuntimeError, match="read"):
        run(tmp_path / "state.json", FakeClient([Decimal("0")], [()]), events)
    assert events == ["flatten", "settle"]


def test_save_failure_propagates_after_cursor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    client = FakeClient([Decimal("0")], [()])
    monkeypatch.setattr(startup, "save_okx_resting_state", lambda *_args: (_ for _ in ()).throw(OSError("save")))
    with pytest.raises(OSError, match="save"):
        run(path, client, [])
    assert client.calls[-1] == "cursor"


@pytest.mark.parametrize("field,value", [("run_id", "bad:id"), ("inst_id", "I" * 65), ("scope", "not-a-scope")])
def test_invalid_config_is_rejected_before_the_state_file_or_exchange_is_touched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, value: str) -> None:
    touched: list[str] = []
    monkeypatch.setattr(startup, "load_okx_resting_state", lambda path: touched.append("load"))
    client = FakeClient([Decimal("0")], [()])
    kwargs: dict[str, Any] = {"read_client": client, "transport": FakeTransport(touched), "state_path": tmp_path / "state.json", "inst_id": INST, "scope": SCOPE, "run_id": "new-run", "max_attempts": 1, "settle": lambda: touched.append("settle")}
    kwargs[field] = value
    with pytest.raises(ValueError):
        startup.run_okx_resting_startup(**kwargs)
    assert touched == [] and client.calls == []


def test_clean_start_returns_the_cursor_it_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(startup, "init_okx_fill_cursor", lambda client, inst_id: "777")
    result = run(tmp_path / "state.json", FakeClient([Decimal("0")], [()]), [])
    assert result.clean and result.cursor == "777"


@pytest.mark.parametrize("unreadable", [True, False])
def test_unreadable_state_still_flattens_but_never_reissues_an_identity(tmp_path: Path, unreadable: bool) -> None:
    path = tmp_path / "state.json"
    if unreadable:
        path.write_text('{"issued":{},"next_seq":-1,"run_id":"same","schema":"OKX_RESTING_STATE_V1"}\n')
    else:
        save_okx_resting_state(path, OkxRestingState("old", 3, {}))
    original = path.read_bytes()
    events: list[str] = []
    client = FakeClient([Decimal("1"), Decimal("0")], [()])
    result = run(path, client, events, run_id="same" if unreadable else "new-run")
    assert events.count("submit") == 1 and events[0] == "submit"
    assert result.attempts == 1 and result.position == 0 and result.state_unreadable is unreadable
    assert result.clean is not unreadable
    assert ("cursor" in client.calls) is not unreadable
    if unreadable:
        assert result.cursor is None and path.read_bytes() == original
    else:
        assert load_okx_resting_state(path) == OkxRestingState("new-run", 0, {})
