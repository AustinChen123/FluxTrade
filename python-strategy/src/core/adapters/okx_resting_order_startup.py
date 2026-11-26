from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable, Protocol

from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingState,
    load_okx_resting_state,
    save_okx_resting_state,
)
from src.core.adapters.okx_resting_order_fills import init_okx_fill_cursor
from src.core.adapters.okx_resting_order_flatten import OkxFlattenResult, run_okx_flatten
from src.core.adapters.okx_resting_order_plan import (
    OkxPlanScope,
    validate_okx_run_id,
    validate_okx_text,
)
from src.core.adapters.okx_resting_order_reads import (
    read_okx_net_position,
    read_okx_own_resting_orders,
)
from src.core.adapters.okx_spider_transport import OkxBatchCallResult


class _StartupReadClient(Protocol):
    def privateGetAccountPositions(self, params: dict[str, str], /) -> object: ...

    def publicGetPublicInstruments(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeOrdersPending(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeFills(self, params: dict[str, str], /) -> object: ...


class _StartupTransport(Protocol):
    def submit(self, payload: bytes) -> OkxBatchCallResult: ...


@dataclass(frozen=True, slots=True)
class OkxStartupResult:
    clean: bool
    attempts: int
    position: Decimal
    stray_ra_orders: int
    state_unreadable: bool
    flattens: tuple[OkxFlattenResult, ...]
    cursor: str | None


def run_okx_resting_startup(
    *,
    read_client: _StartupReadClient,
    transport: _StartupTransport,
    state_path: Path,
    inst_id: str,
    scope: OkxPlanScope,
    run_id: str,
    max_attempts: int,
    settle: Callable[[], None],
) -> OkxStartupResult:
    validate_okx_run_id(run_id)
    if len(run_id) > 36:
        raise ValueError("run_id must be at most 36 characters")
    validate_okx_text("inst_id", inst_id)
    if type(scope) is not OkxPlanScope:
        raise ValueError("invalid scope")
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
        raise ValueError("max_attempts must be an int from 1 to 10")

    state_unreadable = False
    try:
        previous = load_okx_resting_state(state_path)
    except Exception:
        previous = None
        state_unreadable = True
    if previous is not None and previous.run_id == run_id:
        raise ValueError("run_id must differ from saved state")

    flattens: list[OkxFlattenResult] = []
    position = Decimal("0")
    stray_ra_orders = 0
    clean = False
    for attempt in range(max_attempts):
        flattens.append(
            run_okx_flatten(
                read_client=read_client,
                transport=transport,
                state_path=state_path,
                inst_id=inst_id,
                scope=scope,
                fallback_run_id=f"{run_id}-f{attempt}",
            )
        )
        settle()
        position = read_okx_net_position(read_client, inst_id)
        snapshot = read_okx_own_resting_orders(read_client, inst_id)
        stray_ra_orders = sum(order.cl_ord_id.startswith("ra") for order in snapshot.orders)
        prior_issued = previous.issued if previous is not None else {}
        clean = position == 0 and not any(
            order.cl_ord_id in prior_issued or order.cl_ord_id.startswith("ra")
            for order in snapshot.orders
        )
        if clean:
            break

    if not clean or state_unreadable:
        return OkxStartupResult(False, len(flattens), position, stray_ra_orders, state_unreadable, tuple(flattens), None)

    cursor = init_okx_fill_cursor(read_client, inst_id)
    save_okx_resting_state(state_path, OkxRestingState(run_id, 0, {}))
    return OkxStartupResult(True, len(flattens), position, 0, False, tuple(flattens), cursor)
