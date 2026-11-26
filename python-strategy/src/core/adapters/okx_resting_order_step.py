from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from src.core.adapters.okx_resting_order_cycle import (
    OkxRestingCycleResult,
    load_okx_resting_state,
    run_okx_resting_cycle,
)
from src.core.adapters.okx_resting_order_fill_delivery import deliver_okx_fills
from src.core.adapters.okx_resting_order_fills import (
    OkxFillRead,
    read_okx_new_fills,
)
from src.core.adapters.okx_resting_order_flatten import (
    OkxFlattenResult,
    run_okx_flatten,
)
from src.core.adapters.okx_resting_order_plan import (
    OkxPlanScope,
    validate_okx_run_id,
    validate_okx_text,
)
from src.core.adapters.okx_resting_order_reads import (
    read_okx_swap_instrument,
)
from src.core.adapters.okx_spider_transport import OkxBatchCallResult
from src.core.resting_order_strategy import (
    MinuteBar,
    RestingIntent,
    RestingOrderStrategy,
)


_CURSOR_SCHEMA = "OKX_FILL_CURSOR_V1"
_MAX_CURSOR_AGE_MS = 172_800_000


class _StepReadClient(Protocol):
    def privateGetTradeFills(self, params: dict[str, str], /) -> object: ...

    def publicGetPublicInstruments(self, params: dict[str, str], /) -> object: ...

    def privateGetAccountPositions(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeOrdersPending(self, params: dict[str, str], /) -> object: ...


class _StepTransport(Protocol):
    def submit(self, payload: bytes) -> OkxBatchCallResult: ...


@dataclass(frozen=True, slots=True)
class OkxFillCursor:
    run_id: str
    cursor: str | None
    polled_ms: int

    def __post_init__(self) -> None:
        validate_okx_run_id(self.run_id)
        if self.cursor is not None and (
            type(self.cursor) is not str
            or not self.cursor.isascii()
            or not self.cursor.isdigit()
        ):
            raise ValueError("invalid OKX fill cursor")
        if type(self.polled_ms) is not int or self.polled_ms <= 0:
            raise ValueError("invalid OKX fill cursor")


def save_okx_fill_cursor(path: Path, value: OkxFillCursor) -> None:
    if not isinstance(path, Path) or type(value) is not OkxFillCursor:
        raise ValueError("invalid cursor path or value")
    document = {
        "schema": _CURSOR_SCHEMA,
        "run_id": value.run_id,
        "cursor": value.cursor,
        "polled_ms": value.polled_ms,
    }
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
        ) as stream:
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


def load_okx_fill_cursor(path: Path) -> OkxFillCursor | None:
    if not isinstance(path, Path):
        raise ValueError("invalid OKX fill cursor file")
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    try:
        document = json.loads(raw.decode("utf-8"))
        if type(document) is not dict or set(document) != {"schema", "run_id", "cursor", "polled_ms"}:
            raise ValueError("invalid document")
        if type(document["schema"]) is not str or document["schema"] != _CURSOR_SCHEMA:
            raise ValueError("invalid schema")
        return OkxFillCursor(document["run_id"], document["cursor"], document["polled_ms"])
    except (ValueError, TypeError, KeyError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid OKX fill cursor file") from error


@dataclass(frozen=True, slots=True)
class OkxStepResult:
    delivered: int
    close_fills: int
    foreign: int
    cursor: str | None
    intent: RestingIntent
    flatten: OkxFlattenResult | None
    cycle: OkxRestingCycleResult | None


def run_okx_resting_step(
    *,
    read_client: _StepReadClient,
    transport: _StepTransport,
    state_path: Path,
    cursor_path: Path,
    inst_id: str,
    scope: OkxPlanScope,
    strategy: RestingOrderStrategy,
    bar: MinuteBar,
    now_ms: int,
) -> OkxStepResult:
    validate_okx_text("inst_id", inst_id)
    if type(scope) is not OkxPlanScope:
        raise ValueError("invalid scope")
    if not isinstance(strategy, RestingOrderStrategy):
        raise ValueError("invalid strategy")
    if type(bar) is not MinuteBar:
        raise ValueError("invalid bar")
    if type(now_ms) is not int or now_ms <= 0:
        raise ValueError("invalid now_ms")

    state = load_okx_resting_state(state_path)
    if state is None:
        raise ValueError("no saved state")
    cursor_state = load_okx_fill_cursor(cursor_path)
    if cursor_state is None:
        raise ValueError("no fill cursor")
    if cursor_state.run_id != state.run_id:
        raise ValueError("fill cursor run_id differs from saved state")
    if now_ms < cursor_state.polled_ms:
        raise ValueError("fill cursor time is in the future")
    if now_ms - cursor_state.polled_ms > _MAX_CURSOR_AGE_MS:
        raise ValueError("fill cursor is too old")

    read = read_okx_new_fills(read_client, inst_id, cursor_state.cursor, frozenset(state.issued))
    maker_fills = tuple(fill for fill in read.fills if fill.exec_type == "M")
    close_fills = sum(fill.exec_type == "T" for fill in read.fills)
    maker_read = OkxFillRead(maker_fills, read.foreign, read.cursor)
    instrument = read_okx_swap_instrument(read_client, inst_id)
    delivery = deliver_okx_fills(strategy, maker_read, instrument)
    save_okx_fill_cursor(cursor_path, OkxFillCursor(state.run_id, delivery.cursor, now_ms))

    intent = strategy.on_bar(bar)
    if type(intent) is not RestingIntent:
        raise ValueError("on_bar must return RestingIntent")
    flatten_now = delivery.flatten or intent.flatten
    if flatten_now:
        flatten_result = run_okx_flatten(
            read_client=read_client,
            transport=transport,
            state_path=state_path,
            inst_id=inst_id,
            scope=scope,
            fallback_run_id=f"{state.run_id}-s",
        )
        return OkxStepResult(delivery.delivered, close_fills, read.foreign, delivery.cursor, intent, flatten_result, None)

    cycle_result = run_okx_resting_cycle(
        read_client=read_client,
        transport=transport,
        state_path=state_path,
        inst_id=inst_id,
        scope=scope,
        desired=intent.orders,
    )
    return OkxStepResult(delivery.delivered, close_fills, read.foreign, delivery.cursor, intent, None, cycle_result)
