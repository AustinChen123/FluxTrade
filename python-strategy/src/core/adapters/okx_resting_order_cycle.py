from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from src.core.adapters.okx_resting_order_plan import (
    IssuedOrder,
    OkxPlanScope,
    OkxRestingPlan,
    OkxSwapInstrument,
    plan_okx_resting_orders,
)
from src.core.adapters.okx_resting_order_reads import (
    OkxRestingSnapshot,
    _OkxReadClient,
    read_okx_own_resting_orders,
    read_okx_swap_instrument,
)
from src.core.adapters.okx_spider_demo_trading import OkxSpiderDemoBatchTransport
from src.core.adapters.okx_spider_transport import OkxBatchCallResult
from src.core.resting_order_strategy import DesiredOrder


_SCHEMA = "OKX_RESTING_STATE_V1"
_WIRE_ID = re.compile(r"[A-Za-z0-9]{1,32}\Z")


@dataclass(frozen=True, slots=True)
class OkxRestingState:
    run_id: str
    next_seq: int
    issued: Mapping[str, IssuedOrder]

    def __post_init__(self) -> None:
        if type(self.run_id) is not str or not self.run_id:
            raise ValueError("run_id must be non-empty str")
        if type(self.next_seq) is not int or self.next_seq < 0:
            raise ValueError("next_seq must be non-negative int")
        if not isinstance(self.issued, Mapping):
            raise ValueError("issued must be a mapping")
        copied: dict[str, IssuedOrder] = {}
        for wire, order in self.issued.items():
            if type(wire) is not str or _WIRE_ID.fullmatch(wire) is None or type(order) is not IssuedOrder:
                raise ValueError("invalid issued entry")
            copied[wire] = order
        object.__setattr__(self, "issued", MappingProxyType(copied))


@dataclass(frozen=True, slots=True)
class OkxRestingCycleResult:
    plan: OkxRestingPlan
    cancel_results: tuple[OkxBatchCallResult, ...]
    place_results: tuple[OkxBatchCallResult, ...]
    places_held: bool


def _state_document(state: OkxRestingState) -> dict[str, object]:
    return {
        "schema": _SCHEMA,
        "run_id": state.run_id,
        "next_seq": state.next_seq,
        "issued": {
            wire: {
                "action_id": order.action_id,
                "binding_sha256": order.binding_sha256,
                "local_order_id": order.local_order_id,
            }
            for wire, order in sorted(state.issued.items())
        },
    }


def save_okx_resting_state(path: Path, state: OkxRestingState) -> None:
    if not isinstance(path, Path) or type(state) is not OkxRestingState:
        raise ValueError("invalid state path or value")
    payload = (json.dumps(_state_document(state), sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as stream:
            temporary = stream.name
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def load_okx_resting_state(path: Path) -> OkxRestingState | None:
    if not isinstance(path, Path):
        raise ValueError("path must be Path")
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    try:
        document = json.loads(raw)
        if type(document) is not dict or set(document) != {"schema", "run_id", "next_seq", "issued"}:
            raise ValueError("invalid state document")
        if type(document["schema"]) is not str or document["schema"] != _SCHEMA:
            raise ValueError("invalid state schema")
        run_id, next_seq, entries = document["run_id"], document["next_seq"], document["issued"]
        if type(run_id) is not str or not run_id or type(next_seq) is not int or next_seq < 0 or type(entries) is not dict:
            raise ValueError("invalid state fields")
        issued: dict[str, IssuedOrder] = {}
        for wire, row in entries.items():
            if type(wire) is not str or _WIRE_ID.fullmatch(wire) is None or type(row) is not dict or set(row) != {"action_id", "binding_sha256", "local_order_id"}:
                raise ValueError("invalid issued row")
            issued[wire] = IssuedOrder(row["action_id"], row["binding_sha256"], row["local_order_id"])
        return OkxRestingState(run_id, next_seq, issued)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, KeyError) as error:
        raise ValueError("invalid OKX resting state") from error


class _BatchTransport(Protocol):
    def submit(self, payload: bytes) -> OkxBatchCallResult: ...


def _cancel_accepted(result: OkxBatchCallResult) -> bool:
    return (
        result.response_failure is None
        and bool(result.member_outcomes)
        and all(outcome[0] == "ACCEPTED" for _, outcome in result.member_outcomes)
    )


def run_okx_resting_cycle(
    *, read_client: _OkxReadClient, transport: OkxSpiderDemoBatchTransport | _BatchTransport,
    state_path: Path, inst_id: str, scope: OkxPlanScope, desired: Sequence[DesiredOrder],
) -> OkxRestingCycleResult:
    state = load_okx_resting_state(state_path)
    if state is None:
        raise ValueError("no saved state")
    instrument: OkxSwapInstrument = read_okx_swap_instrument(read_client, inst_id)
    resting: OkxRestingSnapshot = read_okx_own_resting_orders(read_client, inst_id)
    plan = plan_okx_resting_orders(
        desired=tuple(desired), resting=resting.orders, instrument=instrument,
        scope=scope, run_id=state.run_id, next_seq=state.next_seq, issued=state.issued,
    )
    merged = dict(state.issued)
    for wire, issued in plan.new_issued.items():
        if wire in merged and merged[wire] != issued:
            raise ValueError("issued identity conflict")
        merged[wire] = issued
    save_okx_resting_state(state_path, OkxRestingState(state.run_id, plan.next_seq, merged))
    cancels = tuple(transport.submit(payload) for payload in plan.cancel_payloads)
    if not all(_cancel_accepted(result) for result in cancels):
        return OkxRestingCycleResult(plan, cancels, (), True)
    places = tuple(transport.submit(payload) for payload in plan.place_payloads)
    return OkxRestingCycleResult(plan, cancels, places, False)
