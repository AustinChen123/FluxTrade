from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingState,
    load_okx_resting_state,
    save_okx_resting_state,
)
from src.core.adapters.okx_resting_order_plan import (
    OkxPlanScope,
    OkxRestingPlan,
    OkxSwapInstrument,
    plan_okx_flatten,
    validate_okx_run_id,
    validate_okx_text,
)
from src.core.adapters.okx_resting_order_reads import (
    OkxRestingSnapshot,
    read_okx_net_position,
    read_okx_own_resting_orders,
    read_okx_swap_instrument,
)
from src.core.adapters.okx_spider_transport import OkxBatchCallResult


class _FlattenReadClient(Protocol):
    def privateGetAccountPositions(self, params: dict[str, str], /) -> object: ...

    def publicGetPublicInstruments(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeOrdersPending(self, params: dict[str, str], /) -> object: ...


class _FlattenTransport(Protocol):
    def submit(self, payload: bytes) -> OkxBatchCallResult: ...


@dataclass(frozen=True, slots=True)
class OkxFlattenResult:
    plan: OkxRestingPlan
    state_used: bool
    save_error: Exception | None
    close_result: OkxBatchCallResult | None
    close_error: Exception | None
    cancel_outcomes: tuple[OkxBatchCallResult | Exception, ...]


def run_okx_flatten(
    *,
    read_client: _FlattenReadClient,
    transport: _FlattenTransport,
    state_path: Path,
    inst_id: str,
    scope: OkxPlanScope,
    fallback_run_id: str,
) -> OkxFlattenResult:
    validate_okx_run_id(fallback_run_id)
    validate_okx_text("inst_id", inst_id)
    if type(scope) is not OkxPlanScope:
        raise ValueError("invalid scope")
    position: Decimal = read_okx_net_position(read_client, inst_id)
    instrument: OkxSwapInstrument = read_okx_swap_instrument(read_client, inst_id)
    snapshot: OkxRestingSnapshot = read_okx_own_resting_orders(read_client, inst_id)

    try:
        state = load_okx_resting_state(state_path)
    except Exception:
        state = None
    plan: OkxRestingPlan | None = None
    if state is not None:
        try:
            plan = plan_okx_flatten(
                position=position, resting=snapshot.orders, instrument=instrument, scope=scope,
                run_id=state.run_id, next_seq=state.next_seq, issued=state.issued,
            )
        except Exception:
            state = None
    if plan is None or state is None:
        plan = plan_okx_flatten(
            position=position, resting=(), instrument=instrument, scope=scope,
            run_id=fallback_run_id, next_seq=0, issued={},
        )
    state_used = state is not None

    close_result: OkxBatchCallResult | None = None
    close_error: Exception | None = None
    if plan.place_payloads:
        try:
            close_result = transport.submit(plan.place_payloads[0])
        except Exception as error:
            close_error = error

    save_error: Exception | None = None
    if state is not None:
        try:
            merged = dict(state.issued)
            for wire, order in plan.new_issued.items():
                if wire in merged and merged[wire] != order:
                    raise ValueError("issued identity conflict")
                merged[wire] = order
            save_okx_resting_state(
                state_path,
                OkxRestingState(state.run_id, plan.next_seq, merged),
            )
        except Exception as error:
            save_error = error

    cancel_outcomes: list[OkxBatchCallResult | Exception] = []
    for payload in plan.cancel_payloads:
        try:
            cancel_outcomes.append(transport.submit(payload))
        except Exception as error:
            cancel_outcomes.append(error)

    return OkxFlattenResult(
        plan,
        state_used,
        save_error,
        close_result,
        close_error,
        tuple(cancel_outcomes),
    )
