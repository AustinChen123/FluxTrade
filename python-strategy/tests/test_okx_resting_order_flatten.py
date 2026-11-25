from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from src.core.adapters import okx_resting_order_flatten as flatten
from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingState,
    load_okx_resting_state,
    save_okx_resting_state,
)
from src.core.adapters.okx_resting_order_plan import IssuedOrder, OkxPlanScope, OkxRestingPlan
from src.core.adapters.okx_spider_transport import OkxBatchCallResult, _decode_batch_request


SCOPE = OkxPlanScope("demo", "toy-account", "toy-strategy", "toy-ref")


def issued(seq: int) -> IssuedOrder:
    return IssuedOrder(f"{seq:064x}", f"{seq + 100:064x}", f"saved:{seq}")


class FakeReadClient:
    def __init__(self, positions: object | Exception | None = None, orders: list[tuple[str, str]] | None = None) -> None:
        self.calls: list[str] = []
        self.positions = positions if positions is not None else {"code": "0", "data": [{"instId": "TOY:SWAP", "instType": "SWAP", "mgnMode": "cross", "posSide": "net", "pos": "3"}]}
        self.orders = orders or []

    def privateGetAccountPositions(self, params: dict[str, str]) -> object:
        self.calls.append("positions")
        if isinstance(self.positions, Exception):
            raise self.positions
        return self.positions

    def publicGetPublicInstruments(self, params: dict[str, str]) -> object:
        self.calls.append("instrument")
        return {"code": "0", "data": [{"instId": "TOY:SWAP", "instType": "SWAP", "ctType": "linear", "ctMult": "1", "state": "live", "ctVal": "1", "lotSz": "1", "minSz": "1", "tickSz": "1"}]}

    def privateGetTradeOrdersPending(self, params: dict[str, str]) -> object:
        self.calls.append("pending")
        rows = []
        for index, (wire, order_id) in enumerate(self.orders):
            rows.append({"instId": "TOY:SWAP", "state": "live", "side": "buy", "ordId": order_id, "clOrdId": wire, "px": "1", "sz": "1", "accFillSz": "0"})
        return {"code": "0", "data": rows}


class FakeTransport:
    def __init__(self, results: list[OkxBatchCallResult | Exception] | None = None, before_submit=None) -> None:
        self.payloads: list[bytes] = []
        self.results = results or []
        self.before_submit = before_submit

    def submit(self, payload: bytes) -> OkxBatchCallResult:
        self.payloads.append(payload)
        if self.before_submit is not None:
            self.before_submit(payload)
        value = self.results.pop(0) if self.results else OkxBatchCallResult(b"{}", None, ())
        if isinstance(value, Exception):
            raise value
        return value


def request(payload: bytes) -> dict[str, object]:
    return cast(dict[str, object], _decode_batch_request(payload))


def test_close_precedes_state_save_and_cancels_only_owned(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    original = OkxRestingState("saved", 5, {"ownwire": issued(1)})
    save_okx_resting_state(path, original)
    old_bytes = path.read_bytes()
    client = FakeReadClient(orders=[("ownwire", "ord-own"), ("foreign", "ord-foreign")])
    seen: list[tuple[str, bool]] = []
    def observe(payload: bytes) -> None:
        operation = cast(str, request(payload)["operation"])
        seen.append((operation, path.read_bytes() == old_bytes))
    transport = FakeTransport(before_submit=observe)
    result = flatten.run_okx_flatten(read_client=client, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert seen == [("PLACE", True), ("CANCEL", False)]
    assert len(result.cancel_outcomes) == 1 and all(isinstance(outcome, OkxBatchCallResult) for outcome in result.cancel_outcomes)
    assert client.calls == ["positions", "instrument", "pending"]
    assert [request(payload)["operation"] for payload in transport.payloads] == ["PLACE", "CANCEL"]
    close = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    close_payload = cast(dict[str, object], close["request_payload"])
    wire = cast(str, close["wire_clOrdId"])
    assert close_payload == {"instId": "TOY:SWAP", "tdMode": "cross", "side": "sell", "posSide": "net", "ordType": "market", "sz": "3", "clOrdId": wire, "reduceOnly": True}
    cancel_members = cast(list[dict[str, object]], request(transport.payloads[1])["members"])
    assert [cast(dict[str, object], member["request_payload"])["ordId"] for member in cancel_members] == ["ord-own"]
    state = load_okx_resting_state(path)
    assert state is not None and state.next_seq == result.plan.next_seq
    assert close["local_order_id"] == "saved:5"
    assert state.issued[wire] == result.plan.new_issued[wire]
    assert result.close_result is not None and result.save_error is None and result.state_used


def test_save_failure_does_not_block_close_or_cancels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("saved", 0, {"wire": issued(1)}))
    old_bytes = path.read_bytes()
    client = FakeReadClient(orders=[("wire", "ord")])
    transport = FakeTransport()
    monkeypatch.setattr(flatten, "save_okx_resting_state", lambda *args: (_ for _ in ()).throw(OSError("save")))
    result = flatten.run_okx_flatten(read_client=client, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert len(transport.payloads) == 2 and isinstance(result.save_error, OSError)
    assert path.read_bytes() == old_bytes


@pytest.mark.parametrize("corrupt", [False, True])
def test_missing_or_corrupt_state_closes_without_writing_or_cancelling(tmp_path: Path, corrupt: bool) -> None:
    path = tmp_path / "state.json"
    if corrupt:
        path.write_text("{")
    original = path.read_bytes() if corrupt else None
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=[("foreign", "ord")]), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert len(transport.payloads) == 1 and request(transport.payloads[0])["operation"] == "PLACE"
    member = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    assert member["local_order_id"] == "fallback:0"
    assert result.state_used is False and result.save_error is None
    assert (path.read_bytes() if path.exists() else None) == original


def test_close_and_cancel_exceptions_do_not_block_later_cancels(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    orders = [(f"wire{i}", f"ord{i}") for i in range(21)]
    state = OkxRestingState("saved", 0, {wire: issued(i) for i, (wire, _) in enumerate(orders)})
    save_okx_resting_state(path, state)
    outcomes = [RuntimeError("close"), RuntimeError("first cancel"), OkxBatchCallResult(b"{}", None, ())]
    transport = FakeTransport(outcomes)
    result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=orders), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert isinstance(result.close_error, RuntimeError) and result.close_result is None
    assert len(transport.payloads) == 3
    assert isinstance(result.cancel_outcomes[0], RuntimeError)
    assert isinstance(result.cancel_outcomes[1], OkxBatchCallResult)


@pytest.mark.parametrize("position,own,expected", [("0", True, ["CANCEL"]), ("0", False, [])])
def test_zero_position_submits_only_planned_cancels(tmp_path: Path, position: str, own: bool, expected: list[str]) -> None:
    path = tmp_path / "state.json"
    orders = [("wire", "ord")] if own else []
    save_okx_resting_state(path, OkxRestingState("saved", 0, {"wire": issued(1)}))
    client = FakeReadClient({"code": "0", "data": [{"instId": "TOY:SWAP", "instType": "SWAP", "mgnMode": "cross", "posSide": "net", "pos": position}]}, orders)
    transport = FakeTransport()
    flatten.run_okx_flatten(read_client=client, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert [request(payload)["operation"] for payload in transport.payloads] == expected


def test_read_exception_propagates_before_state_or_submit(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("unchanged")
    before = path.read_bytes()
    transport = FakeTransport()
    with pytest.raises(RuntimeError, match="read"):
        flatten.run_okx_flatten(read_client=FakeReadClient(RuntimeError("read")), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert transport.payloads == [] and path.read_bytes() == before


def test_merge_conflict_is_reported_after_close_and_before_cancels_state_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("saved", 0, {"wire": issued(1)}))
    before = path.read_bytes()
    real_plan = flatten.plan_okx_flatten
    def conflicting_plan(**kwargs) -> OkxRestingPlan:
        plan = real_plan(**kwargs)
        return OkxRestingPlan(plan.cancel_payloads, plan.place_payloads, plan.skipped, plan.foreign, {"wire": issued(2)}, plan.next_seq)
    monkeypatch.setattr(flatten, "plan_okx_flatten", conflicting_plan)
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=[("wire", "ord")]), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert isinstance(result.save_error, ValueError) and len(transport.payloads) == 2
    assert path.read_bytes() == before


def test_unreadable_state_file_still_closes_with_fallback_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("saved", 4, {"wire": issued(1)}))
    original = path.read_bytes()
    monkeypatch.setattr(flatten, "load_okx_resting_state", lambda _path: (_ for _ in ()).throw(PermissionError("denied")))
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=[("wire", "ord")]), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert [request(payload)["operation"] for payload in transport.payloads] == ["PLACE"]
    member = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    assert member["local_order_id"] == "fallback:0"
    assert result.state_used is False and path.read_bytes() == original


def test_state_that_breaks_the_plan_still_closes_with_fallback_identity(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    shared = IssuedOrder("1" * 64, "2" * 64, "saved:1")
    save_okx_resting_state(path, OkxRestingState("saved", 4, {"wirea": shared, "wireb": shared}))
    original = path.read_bytes()
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=[("wirea", "orda"), ("wireb", "ordb")]), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert [request(payload)["operation"] for payload in transport.payloads] == ["PLACE"]
    member = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    assert member["local_order_id"] == "fallback:0"
    assert result.state_used is False and result.save_error is None and path.read_bytes() == original
    twin_path = tmp_path / "twin.json"
    save_okx_resting_state(twin_path, OkxRestingState("saved", 4, {"wirea": shared}))
    twin = FakeTransport()
    twin_result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=[("wirea", "orda")]), transport=twin, state_path=twin_path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert [request(payload)["operation"] for payload in twin.payloads] == ["PLACE", "CANCEL"] and twin_result.state_used is True


@pytest.mark.parametrize("with_state", [False, True])
def test_unplannable_resting_snapshot_still_closes_without_cancels(tmp_path: Path, with_state: bool) -> None:
    path = tmp_path / "state.json"
    if with_state:
        save_okx_resting_state(path, OkxRestingState("saved", 0, {"wire": issued(1)}))
    original = path.read_bytes() if with_state else None
    client = FakeReadClient(orders=[("wire", "ord-a"), ("wire", "ord-b")])
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=client, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert client.calls == ["positions", "instrument", "pending"]
    assert [request(payload)["operation"] for payload in transport.payloads] == ["PLACE"]
    member = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    assert member["local_order_id"] == "fallback:0"
    assert cast(dict[str, object], member["request_payload"])["sz"] == "3"
    assert result.state_used is False and result.cancel_outcomes == () and result.save_error is None
    assert (path.read_bytes() if path.exists() else None) == original


def test_unique_resting_snapshot_twin_still_cancels(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_okx_resting_state(path, OkxRestingState("saved", 0, {"wire": issued(1)}))
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=FakeReadClient(orders=[("wire", "ord-a"), ("other", "ord-b")]), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert [request(payload)["operation"] for payload in transport.payloads] == ["PLACE", "CANCEL"]
    assert result.state_used is True


@pytest.mark.parametrize("fallback", ["fallback:2026-11-25", "", "x" * 41])
@pytest.mark.parametrize("corrupt", [False, True])
def test_invalid_fallback_run_id_is_rejected_before_any_read_or_submit(tmp_path: Path, fallback: str, corrupt: bool) -> None:
    path = tmp_path / "state.json"
    if corrupt:
        path.write_text("{")
    else:
        save_okx_resting_state(path, OkxRestingState("saved", 0, {"wire": issued(1)}))
    original = path.read_bytes()
    client = FakeReadClient(orders=[("wire", "ord")])
    transport = FakeTransport()
    with pytest.raises(ValueError, match="invalid run_id"):
        flatten.run_okx_flatten(read_client=client, transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id=fallback)
    assert client.calls == [] and transport.payloads == [] and path.read_bytes() == original


@pytest.mark.parametrize("fallback", ["fallback_2026-11-25", "x" * 40])
def test_valid_fallback_run_id_twin_closes_with_corrupt_state(tmp_path: Path, fallback: str) -> None:
    path = tmp_path / "state.json"
    path.write_text("{")
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=FakeReadClient(), transport=transport, state_path=path, inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id=fallback)
    member = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    assert member["local_order_id"] == f"{fallback}:0" and result.close_result is not None


def test_invalid_scope_is_rejected_before_any_read(tmp_path: Path) -> None:
    client = FakeReadClient()
    transport = FakeTransport()
    with pytest.raises(ValueError, match="invalid scope"):
        flatten.run_okx_flatten(read_client=client, transport=transport, state_path=tmp_path / "state.json", inst_id="TOY:SWAP", scope=cast(OkxPlanScope, object()), fallback_run_id="fallback")
    assert client.calls == [] and transport.payloads == []


MAX_POS = "-" + "9" * 20 + "." + "9" * 20


class WideReadClient(FakeReadClient):
    def __init__(self, pos: str, inst_id: str) -> None:
        super().__init__({"code": "0", "data": [{"instId": inst_id, "instType": "SWAP", "mgnMode": "cross", "posSide": "net", "pos": pos}]})
        self.inst_id = inst_id

    def publicGetPublicInstruments(self, params: dict[str, str]) -> object:
        self.calls.append("instrument")
        return {"code": "0", "data": [{"instId": self.inst_id, "instType": "SWAP", "ctType": "linear", "ctMult": "1", "state": "live", "ctVal": "1", "lotSz": "1", "minSz": "1", "tickSz": "1"}]}


def test_largest_accepted_inputs_still_produce_a_transport_accepted_close(tmp_path: Path) -> None:
    inst_id = "I" * 64
    scope = OkxPlanScope("d" * 64, "e" * 64, "s" * 64, "a" * 64)
    path = tmp_path / "state.json"
    path.write_text("{")
    transport = FakeTransport()
    result = flatten.run_okx_flatten(read_client=WideReadClient(MAX_POS, inst_id), transport=transport, state_path=path, inst_id=inst_id, scope=scope, fallback_run_id="f" * 40)
    assert result.close_result is not None and len(transport.payloads) == 1
    assert len(transport.payloads[0]) < 65_536
    member = cast(list[dict[str, object]], request(transport.payloads[0])["members"])[0]
    assert cast(dict[str, object], member["request_payload"])["sz"] == MAX_POS[1:]


@pytest.mark.parametrize("pos", ["1e-1048576", "1E+2", "9" * 21, "0." + "1" * 21, " 3", "3.", ".5", "+3", "NaN"])
def test_out_of_domain_position_text_is_a_read_failure_before_any_submit(tmp_path: Path, pos: str) -> None:
    client = FakeReadClient({"code": "0", "data": [{"instId": "TOY:SWAP", "instType": "SWAP", "mgnMode": "cross", "posSide": "net", "pos": pos}]})
    transport = FakeTransport()
    with pytest.raises(ValueError, match="invalid OKX resting order read"):
        flatten.run_okx_flatten(read_client=client, transport=transport, state_path=tmp_path / "state.json", inst_id="TOY:SWAP", scope=SCOPE, fallback_run_id="fallback")
    assert client.calls == ["positions"] and transport.payloads == []


@pytest.mark.parametrize("inst_id", ["", "I" * 65, "TOY SWAP\n"])
def test_invalid_inst_id_is_rejected_before_any_read(tmp_path: Path, inst_id: str) -> None:
    client = FakeReadClient()
    transport = FakeTransport()
    with pytest.raises(ValueError, match="printable ASCII"):
        flatten.run_okx_flatten(read_client=client, transport=transport, state_path=tmp_path / "state.json", inst_id=inst_id, scope=SCOPE, fallback_run_id="fallback")
    assert client.calls == [] and transport.payloads == []
