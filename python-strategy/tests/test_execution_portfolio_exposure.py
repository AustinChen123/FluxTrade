from __future__ import annotations

from contextlib import AbstractContextManager
from decimal import Decimal
import threading
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.core import execution_portfolio_exposure
from src.core.execution import ExecutionEngine
from src.core.interfaces.repository import IOrderRepository
from src.core.models import OrderStatus, PositionSide
from src.core.portfolio_runtime import PortfolioExposureSnapshot


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
ACTIVE_STATUSES = {
    OrderStatus.NEW.value,
    OrderStatus.SUBMITTED_UNCONFIRMED.value,
    OrderStatus.SUBMITTED.value,
    OrderStatus.PARTIALLY_FILLED.value,
}


class _RecordingLock(AbstractContextManager[None]):
    def __init__(self) -> None:
        self.depth = 0
        self.entries = 0

    def __enter__(self) -> None:
        self.depth += 1
        self.entries += 1

    def __exit__(self, *_args: object) -> None:
        self.depth -= 1


class _Repository:
    def __init__(
        self,
        lock: _RecordingLock,
        *,
        intents: dict[str, object] | None = None,
        orders: list[object] | None = None,
    ) -> None:
        self.lock = lock
        self.intents = intents or {}
        self.orders = orders or []
        self.intent_reads: list[str] = []
        self.status_reads: list[set[str]] = []

    def get_order_by_client_order_id(self, client_order_id: str) -> object | None:
        assert self.lock.depth == 1
        self.intent_reads.append(client_order_id)
        return self.intents.get(client_order_id)

    def list_orders_by_statuses(self, statuses: set[str]) -> list[object]:
        assert self.lock.depth == 1
        self.status_reads.append(statuses)
        return self.orders


def _position(side: object, quantity: Decimal) -> object:
    return SimpleNamespace(side=side, quantity=quantity)


def _order(
    order_id: str,
    *,
    strategy_id: str = "long",
    product_id: str = PRODUCT_ID,
    side: str = "buy",
    quantity: Decimal = Decimal("2"),
    filled_quantity: Decimal = Decimal("0.5"),
    intent_payload: object = None,
) -> object:
    return SimpleNamespace(
        id=order_id,
        strategy_id=strategy_id,
        product_id=product_id,
        side=side,
        quantity=quantity,
        filled_quantity=filled_quantity,
        intent_payload={} if intent_payload is None else intent_payload,
    )


def _project(
    *,
    positions: dict[str, object | None] | None = None,
    intents: dict[str, object] | None = None,
    orders: list[object] | None = None,
    strategy_ids: tuple[str, ...] = ("long", "short", "flat"),
    requested_intents: dict[str, str] | None = None,
) -> tuple[PortfolioExposureSnapshot, _RecordingLock, _Repository]:
    lock = _RecordingLock()
    repository = _Repository(lock, intents=intents, orders=orders)
    position_values = positions or {}

    def load_position(strategy_id: str, product_id: str) -> object | None:
        assert lock.depth == 1
        assert product_id == PRODUCT_ID
        return position_values.get(strategy_id)

    snapshot = execution_portfolio_exposure.project_portfolio_exposure(
        position_loader=load_position,
        order_repository=repository,
        order_event_lock=lock,
        strategy_ids=strategy_ids,
        product_id=PRODUCT_ID,
        requested_intents=requested_intents or {},
    )
    return snapshot, lock, repository


def test_projection_is_atomic_and_preserves_signed_decimal_exposure() -> None:
    persisted = SimpleNamespace(strategy_id="long", product_id=PRODUCT_ID)
    snapshot, lock, repository = _project(
        positions={
            "long": _position(PositionSide.LONG, Decimal("1.25")),
            "short": _position(PositionSide.SHORT, Decimal("0.75")),
        },
        intents={"intent-long": persisted},
        requested_intents={"intent-long": "long"},
        orders=[
            _order("buy", strategy_id="long"),
            _order(
                "sell",
                strategy_id="short",
                side="sell",
                quantity=Decimal("1"),
                filled_quantity=Decimal("0.25"),
            ),
        ],
    )

    assert snapshot == PortfolioExposureSnapshot(
        quantities={
            "long": Decimal("2.75"),
            "short": Decimal("-1.50"),
            "flat": Decimal("0"),
        },
        existing_client_order_ids=frozenset({"intent-long"}),
    )
    assert lock.entries == 1
    assert lock.depth == 0
    assert repository.intent_reads == ["intent-long"]
    assert repository.status_reads == [ACTIVE_STATUSES]


def test_projection_excludes_non_entry_or_foreign_orders() -> None:
    included = _order("included")
    snapshot, _, _ = _project(
        strategy_ids=("long",),
        orders=[
            included,
            _order(
                "protection",
                intent_payload={"pending_entry_order_id": "included"},
            ),
            _order("reduce", intent_payload={"reduce_only": True}),
            _order("nested-reduce", intent_payload={"order": {"reduce_only": True}}),
            _order("foreign-owner", strategy_id="other"),
            _order("foreign-product", product_id="BINANCE:ETHUSDT-PERP"),
            _order("raw-payload", intent_payload="not-a-mapping"),
            _order(
                "complete",
                quantity=Decimal("1"),
                filled_quantity=Decimal("1"),
            ),
        ],
    )

    assert snapshot.quantities == {"long": Decimal("3")}


def test_falsey_filled_quantity_preserves_existing_zero_fallback_scale() -> None:
    snapshot, _, _ = _project(
        strategy_ids=("long",),
        orders=[
            _order(
                "zero-scale",
                quantity=Decimal("2"),
                filled_quantity=Decimal("0.00"),
            )
        ],
    )

    assert snapshot.quantities["long"].as_tuple() == Decimal("2").as_tuple()


@pytest.mark.parametrize(
    ("strategy_ids", "requested_intents", "error"),
    [
        (
            ("long", "long"),
            {},
            "portfolio_exposure_strategy_ids_must_be_unique",
        ),
        (
            ("long",),
            {"intent": "other"},
            "portfolio_exposure_intent_owner_unknown",
        ),
    ],
)
def test_input_identity_validation_precedes_external_reads(
    strategy_ids: tuple[str, ...],
    requested_intents: dict[str, str],
    error: str,
) -> None:
    lock = _RecordingLock()
    repository = _Repository(lock)

    with pytest.raises(ValueError, match=f"^{error}$"):
        execution_portfolio_exposure.project_portfolio_exposure(
            position_loader=lambda *_args: None,
            order_repository=repository,
            order_event_lock=lock,
            strategy_ids=strategy_ids,
            product_id=PRODUCT_ID,
            requested_intents=requested_intents,
        )

    assert lock.entries == 0
    assert repository.intent_reads == []
    assert repository.status_reads == []


def test_missing_position_loader_precedes_repository_access() -> None:
    lock = _RecordingLock()
    repository = _Repository(lock)

    with pytest.raises(RuntimeError, match="^portfolio_position_loader_missing$"):
        execution_portfolio_exposure.project_portfolio_exposure(
            position_loader=None,
            order_repository=repository,
            order_event_lock=lock,
            strategy_ids=("long",),
            product_id=PRODUCT_ID,
            requested_intents={},
        )

    assert lock.entries == 0
    assert repository.status_reads == []


def test_missing_loader_precedes_compound_input_identity_errors() -> None:
    lock = _RecordingLock()
    repository = _Repository(lock)

    with pytest.raises(RuntimeError, match="^portfolio_position_loader_missing$"):
        execution_portfolio_exposure.project_portfolio_exposure(
            position_loader=None,
            order_repository=repository,
            order_event_lock=lock,
            strategy_ids=("long", "long"),
            product_id=PRODUCT_ID,
            requested_intents={"intent": "unknown"},
        )

    assert lock.entries == 0


def test_duplicate_sleeve_precedes_unknown_intent_owner() -> None:
    lock = _RecordingLock()
    repository = _Repository(lock)

    with pytest.raises(
        ValueError,
        match="^portfolio_exposure_strategy_ids_must_be_unique$",
    ):
        execution_portfolio_exposure.project_portfolio_exposure(
            position_loader=lambda *_args: None,
            order_repository=repository,
            order_event_lock=lock,
            strategy_ids=("long", "long"),
            product_id=PRODUCT_ID,
            requested_intents={"intent": "unknown"},
        )

    assert lock.entries == 0


@pytest.mark.parametrize(
    ("persisted_strategy_id", "persisted_product_id"),
    [
        ("other", PRODUCT_ID),
        ("long", "BINANCE:ETHUSDT-PERP"),
    ],
)
def test_replay_intent_identity_mismatch_fails_closed(
    persisted_strategy_id: str,
    persisted_product_id: str,
) -> None:
    persisted = SimpleNamespace(
        strategy_id=persisted_strategy_id,
        product_id=persisted_product_id,
    )

    with pytest.raises(
        RuntimeError,
        match="^portfolio_replay_intent_identity_mismatch:client_order_id=intent$",
    ):
        _project(
            strategy_ids=("long", "other"),
            intents={"intent": persisted},
            requested_intents={"intent": "long"},
        )


@pytest.mark.parametrize(
    ("position", "strategy_id"),
    [
        (_position(PositionSide.LONG, Decimal("0")), "long"),
        (_position(PositionSide.SHORT, Decimal("NaN")), "short"),
        (_position("FLAT", Decimal("1")), "flat"),
    ],
)
def test_invalid_position_fails_closed(position: object, strategy_id: str) -> None:
    error = (
        "portfolio_position_side_invalid"
        if strategy_id == "flat"
        else "portfolio_position_invalid"
    )

    with pytest.raises(RuntimeError, match=f"^{error}:{strategy_id}$"):
        _project(
            positions={strategy_id: position},
            strategy_ids=(strategy_id,),
        )


@pytest.mark.parametrize(
    ("order", "error"),
    [
        (
            _order("zero", quantity=Decimal("0")),
            "portfolio_pending_entry_quantity_invalid:order_id=zero",
        ),
        (
            _order("overfill", quantity=Decimal("1"), filled_quantity=Decimal("2")),
            "portfolio_pending_entry_quantity_invalid:order_id=overfill",
        ),
        (
            _order("side", side="hold"),
            "portfolio_pending_entry_side_invalid:order_id=side",
        ),
    ],
)
def test_invalid_pending_entry_fails_closed(order: object, error: str) -> None:
    with pytest.raises(RuntimeError, match=f"^{error}$"):
        _project(strategy_ids=("long",), orders=[order])


def test_pending_entry_cannot_cross_existing_sleeve_position() -> None:
    with pytest.raises(
        RuntimeError,
        match="^portfolio_pending_entry_crosses_sleeve_position:long$",
    ):
        _project(
            positions={"long": _position(PositionSide.LONG, Decimal("1"))},
            strategy_ids=("long",),
            orders=[_order("cross", side="sell")],
        )


def test_execution_engine_facade_passes_current_dependencies(
    mock_db_session: Any,
    mock_clock: Any,
    mock_exchange_adapter: Any,
    mock_order_repo: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
    )
    loader = MagicMock()
    repository = MagicMock(spec=IOrderRepository)
    lock = threading.RLock()
    expected = PortfolioExposureSnapshot({"long": Decimal("3")})
    captured: dict[str, object] = {}

    execution_engine._position_loader = loader
    execution_engine.order_manager.repo = repository
    execution_engine._order_event_apply_lock = lock

    def project(**values: object) -> PortfolioExposureSnapshot:
        captured.update(values)
        return expected

    monkeypatch.setattr(
        execution_portfolio_exposure,
        "project_portfolio_exposure",
        project,
    )

    result = execution_engine.portfolio_exposure_snapshot(
        ("long",),
        PRODUCT_ID,
        {"intent": "long"},
    )

    assert result is expected
    assert captured == {
        "position_loader": loader,
        "order_repository": repository,
        "order_event_lock": lock,
        "strategy_ids": ("long",),
        "product_id": PRODUCT_ID,
        "requested_intents": {"intent": "long"},
    }
