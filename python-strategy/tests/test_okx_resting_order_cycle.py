from __future__ import annotations

import json
import stat
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Mapping, cast

import pytest

from src.core.adapters import okx_resting_order_cycle as cycle
from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingState,
    load_okx_resting_state,
    run_okx_resting_cycle,
    save_okx_resting_state,
)
from src.core.adapters.okx_resting_order_plan import IssuedOrder, OkxPlanScope, OkxRestingPlan, OkxSwapInstrument
from src.core.adapters.okx_resting_order_reads import OkxOwnRestingOrder
from src.core.adapters.okx_spider_transport import OkxBatchCallResult
from src.core.resting_order_strategy import DesiredOrder


SCOPE = OkxPlanScope("demo", "toy-account", "toy-strategy", "toy-ref")
ISSUED = IssuedOrder("a" * 64, "b" * 64, "run:0")


class FakeReadClient:
    def __init__(self, orders: list[OkxOwnRestingOrder] | None = None) -> None:
        self.public_calls: list[dict[str, str]] = []
        self.private_calls: list[dict[str, str]] = []
        self.orders = orders or []

    def publicGetPublicInstruments(self, params: dict[str, str]) -> object:
        self.public_calls.append(params)
        return {"code": "0", "data": [{"instId": "TOY:SWAP", "instType": "SWAP", "ctType": "linear", "ctMult": "1", "state": "live", "ctVal": "1", "lotSz": "1", "minSz": "1", "tickSz": "1"}]}

    def privateGetTradeOrdersPending(self, params: dict[str, str]) -> object:
        self.private_calls.append(params)
        return {"code": "0", "data": [{"instId": "TOY:SWAP", "state": "live", "side": row.side, "ordId": row.ord_id, "clOrdId": row.cl_ord_id, "px": str(row.px), "sz": str(row.remaining_sz), "accFillSz": "0"} for row in self.orders]}


class FakeTransport:
    def __init__(self, scripted: list[OkxBatchCallResult] | None = None, on_submit=None) -> None:
        self.payloads: list[bytes] = []
        self.scripted = scripted or []
        self.on_submit = on_submit

    def submit(self, payload: bytes) -> OkxBatchCallResult:
        self.payloads.append(payload)
        if self.on_submit is not None:
            self.on_submit(payload)
        if self.scripted:
            return self.scripted.pop(0)
        import src.core.adapters.okx_spider_transport as transport_module
        request = cast(dict[str, object], transport_module._decode_batch_request(payload))
        members = cast(list[dict[str, object]], request["members"])
        return OkxBatchCallResult(b"{}", None, tuple((cast(str, row.get("cancel_action_id", row.get("action_id"))), ("ACCEPTED", "0", "ord")) for row in members))


def result(status: str = "ACCEPTED", failure: str | None = None) -> OkxBatchCallResult:
    return OkxBatchCallResult(b"{}" if failure is None else None, failure, (("a" * 64, (status, "0", "ord")),))


def test_state_round_trip_exact_and_missing(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    assert load_okx_resting_state(path) is None
    state = OkxRestingState("run_1", 3, {"wire1": ISSUED})
    save_okx_resting_state(path, state)
    assert load_okx_resting_state(path) == state
    assert json.loads(path.read_text()) == {"schema": "OKX_RESTING_STATE_V1", "run_id": "run_1", "next_seq": 3, "issued": {"wire1": {"action_id": "a" * 64, "binding_sha256": "b" * 64, "local_order_id": "run:0"}}}
    assert list(tmp_path.iterdir()) == [path]
    with pytest.raises(TypeError):
        cast(dict[str, IssuedOrder], state.issued)["wire2"] = ISSUED


def test_state_constructor_rejects_bool_next_seq() -> None:
    with pytest.raises(ValueError):
        OkxRestingState("run_1", True, {})


@pytest.mark.parametrize("edit", [
    lambda doc: doc.update(extra=1), lambda doc: doc.update(schema="V2"),
    lambda doc: doc.pop("issued"), lambda doc: doc.update(next_seq=True),
    lambda doc: doc.update(next_seq=-1), lambda doc: doc.update(next_seq="3"),
    lambda doc: doc["issued"].update({"wire": {"action_id": "bad", "binding_sha256": "b" * 64, "local_order_id": "x"}}),
    lambda doc: doc["issued"].update({"wire": {"action_id": "a" * 64, "binding_sha256": "b" * 64, "local_order_id": "x", "extra": 1}}),
    lambda doc: doc["issued"].update({"x" * 33: {"action_id": "a" * 64, "binding_sha256": "b" * 64, "local_order_id": "x"}}),
])
def test_state_rejects_malformed_documents_with_legal_twin(tmp_path: Path, edit) -> None:
    path = tmp_path / "state.json"
    valid = {"schema": "OKX_RESTING_STATE_V1", "run_id": "run_1", "next_seq": 0, "issued": {}}
    path.write_text("{")
    with pytest.raises(ValueError):
        load_okx_resting_state(path)
    path.write_bytes(b"\xff")
    with pytest.raises(ValueError):
        load_okx_resting_state(path)
    path.write_text(json.dumps(valid))
    assert load_okx_resting_state(path) == OkxRestingState("run_1", 0, {})
    malformed = json.loads(json.dumps(valid))
    edit(malformed)
    path.write_text(json.dumps(malformed))
    with pytest.raises(ValueError):
        load_okx_resting_state(path)


def test_replace_failure_preserves_old_state_and_removes_temp(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("old", 0, {}))
    original = path.read_bytes()
    def fail_replace(source, target):
        raise OSError("replace failed")
    monkeypatch.setattr(cycle.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        save_okx_resting_state(path, OkxRestingState("new", 1, {}))
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


def test_write_ahead_and_cancel_then_place_order(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("run_1", 0, {"wireold": ISSUED}))
    reader = FakeReadClient([OkxOwnRestingOrder("old-ord", "wireold", "sell", Decimal("8"), Decimal("1"))])
    holder: dict[str, OkxRestingPlan] = {}
    real_planner = cycle.plan_okx_resting_orders
    def capture_plan(
        *, desired: tuple[DesiredOrder, ...], resting: tuple[OkxOwnRestingOrder, ...],
        instrument: OkxSwapInstrument, scope: OkxPlanScope, run_id: str, next_seq: int,
        issued: Mapping[str, IssuedOrder],
    ) -> OkxRestingPlan:
        plan = real_planner(desired=desired, resting=resting, instrument=instrument, scope=scope, run_id=run_id, next_seq=next_seq, issued=issued)
        holder["plan"] = plan
        return plan
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(cycle, "plan_okx_resting_orders", capture_plan)
    def inspect_first(payload: bytes) -> None:
        plan = holder["plan"]
        saved = load_okx_resting_state(path)
        assert saved is not None and saved.next_seq == plan.next_seq
        assert set(plan.new_issued.items()) <= set(saved.issued.items())
    transport = FakeTransport(on_submit=inspect_first)
    output = run_okx_resting_cycle(read_client=reader, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=(DesiredOrder("buy", 10.0, 1.0),))
    monkeypatch.undo()
    assert len(transport.payloads) == 2
    assert len(output.cancel_results) == 1 and len(output.place_results) == 1
    from src.core.adapters.okx_spider_transport import _decode_batch_request
    operations = []
    for payload in transport.payloads:
        request = _decode_batch_request(payload)
        assert request is not None
        operations.append(request["operation"])
    assert operations == ["CANCEL", "PLACE"]
    assert reader.public_calls == [{"instType": "SWAP", "instId": "TOY:SWAP"}]
    assert reader.private_calls == [{"instType": "SWAP", "instId": "TOY:SWAP", "limit": "100"}]


@pytest.mark.parametrize(
    "failed",
    [result("REJECTED"), result("SEND_UNKNOWN"), result("ACCEPTED", "RESPONSE_INVALID"), OkxBatchCallResult(b"{}", None, ())],
)
def test_cancel_failure_holds_all_places(tmp_path: Path, failed: OkxBatchCallResult) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("run_1", 0, {"wireold": ISSUED}))
    old = OkxOwnRestingOrder("old-ord", "wireold", "sell", Decimal("8"), Decimal("1"))
    reader = FakeReadClient([old])
    transport = FakeTransport([failed])
    output = run_okx_resting_cycle(read_client=reader, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=(DesiredOrder("buy", 10.0, 1.0),))
    assert len(output.cancel_results) == 1 and output.places_held
    assert len(transport.payloads) == 1 and output.place_results == ()


def test_cancel_accepted_allows_place_and_empty_plan_does_not_submit(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("run_1", 0, {"wireold": ISSUED}))
    reader = FakeReadClient([OkxOwnRestingOrder("old-ord", "wireold", "sell", Decimal("8"), Decimal("1"))])
    transport = FakeTransport()
    output = run_okx_resting_cycle(read_client=reader, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=(DesiredOrder("buy", 10.0, 1.0),))
    assert len(output.cancel_results) == 1 and len(output.place_results) == 1 and not output.places_held
    save_okx_resting_state(path, OkxRestingState("run_1", 0, {}))
    empty_transport = FakeTransport()
    run_okx_resting_cycle(read_client=FakeReadClient(), transport=empty_transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=())
    assert empty_transport.payloads == []


def test_place_exception_is_recoverable_without_reusing_sequence(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("run_1", 0, {}))
    desired = (DesiredOrder("buy", 10.0, 1.0),)
    class RaisingTransport(FakeTransport):
        def submit(self, payload: bytes) -> OkxBatchCallResult:
            self.payloads.append(payload)
            raise RuntimeError("ambiguous place")
    first = cycle.plan_okx_resting_orders
    expected = first(desired=desired, resting=(), instrument=cycle.read_okx_swap_instrument(FakeReadClient(), "TOY:SWAP"), scope=SCOPE, run_id="run_1", next_seq=0, issued={})
    with pytest.raises(RuntimeError, match="ambiguous place"):
        run_okx_resting_cycle(read_client=FakeReadClient(), transport=RaisingTransport(), state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=desired)
    saved = load_okx_resting_state(path)
    assert saved is not None and saved.next_seq == expected.next_seq and saved.issued == expected.new_issued
    wire, issued = next(iter(expected.new_issued.items()))
    request = cast(dict[str, object], __import__("src.core.backtest.spider_run_artifacts", fromlist=["decode_canonical"]).decode_canonical(expected.place_payloads[0]))
    member = cast(dict[str, object], cast(list[object], request["members"])[0])
    request_order = cast(dict[str, str], member["request_payload"])
    resting = OkxOwnRestingOrder("returned-ord", wire, "buy", Decimal(request_order["px"]), Decimal(request_order["sz"]))
    recovered = run_okx_resting_cycle(read_client=FakeReadClient([resting]), transport=FakeTransport(), state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=desired)
    assert recovered.plan.place_payloads == ()
    assert recovered.plan.next_seq == expected.next_seq
    assert issued == saved.issued[wire]


def test_merge_conflict_fails_before_write_or_submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    original = OkxRestingState("run_1", 0, {"wire1": ISSUED})
    save_okx_resting_state(path, original)
    prior_bytes = path.read_bytes()
    real_planner = cycle.plan_okx_resting_orders
    def conflicting_planner(**kwargs):
        plan = real_planner(**kwargs)
        conflicting = IssuedOrder("c" * 64, "d" * 64, "different")
        return replace(plan, new_issued=MappingProxyType({"wire1": conflicting}))
    monkeypatch.setattr(cycle, "plan_okx_resting_orders", conflicting_planner)
    transport = FakeTransport()
    with pytest.raises(ValueError, match="issued identity conflict"):
        run_okx_resting_cycle(read_client=FakeReadClient(), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=(DesiredOrder("buy", 10.0, 1.0),))
    assert path.read_bytes() == prior_bytes
    assert transport.payloads == []


def test_missing_saved_state_reads_nothing(tmp_path: Path) -> None:
    reader = FakeReadClient()
    transport = FakeTransport()
    with pytest.raises(ValueError, match="no saved state"):
        run_okx_resting_cycle(read_client=reader, transport=transport, state_path=tmp_path / "missing.json", inst_id="TOY:SWAP", scope=SCOPE, desired=())
    assert reader.public_calls == reader.private_calls == []
    assert transport.payloads == []


def test_save_fsync_replace_fsync_order_and_file_failure_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("old", 0, {}))
    old_bytes = path.read_bytes()
    real_fsync = cycle.os.fsync
    real_replace = cycle.os.replace
    events: list[tuple[str, int, object]] = []
    temp_identity: list[tuple[int, int, int]] = []
    old_identity = (cycle.os.stat(path).st_dev, cycle.os.stat(path).st_ino)

    def record_fsync(fd: int) -> None:
        mode = cycle.os.fstat(fd).st_mode
        identity = (mode, cycle.os.fstat(fd).st_dev, cycle.os.fstat(fd).st_ino)
        events.append(("fsync", fd, identity))
        if (identity[1], identity[2]) != old_identity:
            temp_identity.append(identity)
        real_fsync(fd)

    def record_replace(source: str, target: Path) -> None:
        temp_stat = cycle.os.stat(source)
        temp_identity.append((temp_stat.st_mode, temp_stat.st_dev, temp_stat.st_ino))
        events.append(("replace", -1, (Path(source), Path(target))))
        real_replace(source, target)

    monkeypatch.setattr(cycle.os, "fsync", record_fsync)
    monkeypatch.setattr(cycle.os, "replace", record_replace)
    save_okx_resting_state(path, OkxRestingState("new", 1, {}))
    assert [event[0] for event in events] == ["fsync", "replace", "fsync"]
    temp_path = cast(tuple[Path, Path], events[1][2])[0]
    assert events[0][2] == temp_identity[0]
    assert events[1][2] == (temp_path, path)
    assert events[2][2] == (cycle.os.stat(tmp_path).st_mode, cycle.os.stat(tmp_path).st_dev, cycle.os.stat(tmp_path).st_ino)
    assert list(tmp_path.iterdir()) == [path]

    save_okx_resting_state(path, OkxRestingState("old", 0, {}))
    old_bytes = path.read_bytes()
    calls = 0

    def fail_file_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if (cycle.os.fstat(fd).st_dev, cycle.os.fstat(fd).st_ino) != (cycle.os.stat(path).st_dev, cycle.os.stat(path).st_ino):
            raise OSError("file fsync failed")
        real_fsync(fd)

    monkeypatch.setattr(cycle.os, "fsync", fail_file_fsync)
    with pytest.raises(OSError, match="file fsync failed"):
        save_okx_resting_state(path, OkxRestingState("new", 1, {}))
    assert calls == 1
    assert path.read_bytes() == old_bytes
    assert list(tmp_path.iterdir()) == [path]


def test_directory_fsync_failure_keeps_new_state_and_skips_submits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("run_1", 5, {}))
    real_fsync = cycle.os.fsync

    def fail_directory_fsync(fd: int) -> None:
        if stat.S_ISDIR(cycle.os.fstat(fd).st_mode):
            raise OSError("directory fsync failed")
        real_fsync(fd)

    monkeypatch.setattr(cycle.os, "fsync", fail_directory_fsync)
    transport = FakeTransport()
    with pytest.raises(OSError, match="directory fsync failed"):
        run_okx_resting_cycle(read_client=FakeReadClient(), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=(DesiredOrder("buy", 10.0, 1.0),))
    saved = load_okx_resting_state(path)
    assert saved is not None and saved.next_seq == 6
    assert transport.payloads == []
    assert list(tmp_path.iterdir()) == [path]


def _many_orders(count: int, side: Literal["buy", "sell"] = "sell") -> tuple[DesiredOrder, ...]:
    return tuple(DesiredOrder(side, float(100 + index), 1.0) for index in range(count))


def _payload_members(payload: bytes) -> list[dict[str, object]]:
    from src.core.adapters.okx_spider_transport import _decode_batch_request
    request = _decode_batch_request(payload)
    assert request is not None
    return cast(list[dict[str, object]], request["members"])


def test_multi_batch_submits_all_payloads_in_order_and_holds_places_on_late_cancel_failure(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    issued = {f"wire{index}": IssuedOrder(f"{index:064x}", f"{index + 100:064x}", f"old:{index}") for index in range(21)}
    save_okx_resting_state(path, OkxRestingState("run_1", 0, issued))
    resting = [OkxOwnRestingOrder(f"ord{index}", f"wire{index}", "sell", Decimal("8"), Decimal("1")) for index in range(21)]
    reader = FakeReadClient(resting)
    script = [result("ACCEPTED"), result("REJECTED")]
    transport = FakeTransport(scripted=script)
    output = run_okx_resting_cycle(read_client=reader, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=_many_orders(21, "buy"))
    plan = output.plan
    assert len(plan.cancel_payloads) == len(plan.place_payloads) == 2
    assert transport.payloads == list(plan.cancel_payloads)
    assert len(output.cancel_results) == 2 and output.place_results == () and output.places_held

    save_okx_resting_state(path, OkxRestingState("run_1", 0, issued))
    transport = FakeTransport(scripted=[result(), result(), result("ACCEPTED", "RESPONSE_INVALID"), result()])
    output = run_okx_resting_cycle(read_client=FakeReadClient(resting), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=_many_orders(21, "buy"))
    assert len(output.cancel_results) == 2 and len(output.place_results) == 2 and not output.places_held
    assert transport.payloads == list(output.plan.cancel_payloads + output.plan.place_payloads)
    from src.core.adapters.okx_spider_transport import _decode_batch_request
    operations = [cast(dict[str, object], _decode_batch_request(payload))["operation"] for payload in transport.payloads]
    assert operations == ["CANCEL", "CANCEL", "PLACE", "PLACE"]

    save_okx_resting_state(path, OkxRestingState("run_1", 0, issued))
    class RaiseSecondPlace(FakeTransport):
        def submit(self, payload: bytes) -> OkxBatchCallResult:
            self.payloads.append(payload)
            if len(self.payloads) == 4:
                raise RuntimeError("second place failed")
            return result()
    transport = RaiseSecondPlace()
    with pytest.raises(RuntimeError, match="second place failed"):
        run_okx_resting_cycle(read_client=FakeReadClient(resting), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=_many_orders(21, "buy"))
    saved = load_okx_resting_state(path)
    assert saved is not None and saved.next_seq == 42
    assert transport.payloads == list(output.plan.cancel_payloads + output.plan.place_payloads)


def test_recovery_sequence_oracle_uses_contract_and_received_place_requests(tmp_path: Path) -> None:
    start = 37
    count = 3
    run_id = "run_1"
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState(run_id, start, {}))
    desired = _many_orders(count, "buy")
    first_transport = FakeTransport()
    run_okx_resting_cycle(read_client=FakeReadClient(), transport=first_transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=desired)
    saved = load_okx_resting_state(path)
    expected_local_ids = {f"{run_id}:{seq}" for seq in range(start, start + count)}
    assert saved is not None and saved.next_seq == start + count
    assert {item.local_order_id for item in saved.issued.values()} == expected_local_ids
    first_wires: set[str] = set()
    exchange_orders: list[OkxOwnRestingOrder] = []
    for payload in first_transport.payloads:
        for member in _payload_members(payload):
            request = cast(dict[str, str], member["request_payload"])
            wire = request["clOrdId"]
            first_wires.add(wire)
            exchange_orders.append(OkxOwnRestingOrder(f"ord-{wire}", wire, request["side"], Decimal(request["px"]), Decimal(request["sz"])))
    assert len(first_wires) == count
    replay_transport = FakeTransport()
    replay = run_okx_resting_cycle(read_client=FakeReadClient(exchange_orders), transport=replay_transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=desired)
    assert replay.plan.foreign == () and replay_transport.payloads == []
    expanded_transport = FakeTransport()
    run_okx_resting_cycle(read_client=FakeReadClient(exchange_orders), transport=expanded_transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, desired=desired + _many_orders(1, "buy"))
    new_members = [member for payload in expanded_transport.payloads for member in _payload_members(payload)]
    assert len(new_members) == 1
    new_member = new_members[0]
    assert new_member["local_order_id"] == f"{run_id}:{start + count}"
    assert cast(dict[str, str], new_member["request_payload"])["clOrdId"] not in first_wires
