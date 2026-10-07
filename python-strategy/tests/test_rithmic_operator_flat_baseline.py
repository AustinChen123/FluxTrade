import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.adapters.rithmic_operator_flat_baseline import (
    _order_cut_row,
    _trade_cut_row,
    _decimal_text,
    compute_rithmic_operator_flat_cut,
    read_latest_rithmic_operator_flat_baseline,
    write_rithmic_operator_flat_baseline,
)
from src.core.models import Position, PositionSide
from src.core.orm_models import (
    Base,
    Exchange,
    Order,
    Product,
    Strategy,
    SystemEvent,
    Trade,
)


_PROFILE = "paper"
_ACCOUNT_ID = "account-1"
_PRODUCT_ID = "RITHMIC:NQ-202609"


def _prepare_database(session_factory) -> None:
    engine = session_factory.kw["bind"]
    Base.metadata.create_all(engine, tables=[SystemEvent.__table__])
    with session_factory() as session:
        session.add_all(
            [
                Exchange(id="RITHMIC", name="Rithmic"),
                Exchange(id="BINANCE", name="Binance"),
                Product(
                    id=_PRODUCT_ID,
                    exchange_id="RITHMIC",
                    base_asset="NQ",
                    quote_asset="USD",
                ),
                Product(
                    id="BINANCE:BTCUSDT-PERP",
                    exchange_id="BINANCE",
                    base_asset="BTC",
                    quote_asset="USDT",
                ),
                Strategy(id="other_strategy", name="Other Strategy"),
            ]
        )
        session.commit()


def _scoped_order(**overrides) -> Order:
    values = {
        "id": "order-1",
        "exchange_order_id": "basket-1",
        "strategy_id": "test_strategy",
        "product_id": _PRODUCT_ID,
        "exchange_id": "RITHMIC",
        "account_profile": _PROFILE,
        "account_id": _ACCOUNT_ID,
        "type": "stop_loss",
        "side": "sell",
        "price": Decimal("20001.00"),
        "trigger_price": Decimal("19999.00"),
        "quantity": Decimal("2.0000"),
        "status": "SUBMITTED",
        "timestamp": 1_700_000_000_000,
        "filled_quantity": None,
        "filled_price": None,
        "client_order_id": "client-1",
        "intent_payload": {
            "placement_mode": "attach-at-entry",
            "pending_entry_order_id": None,
            "native_parent_client_order_id": "parent-client",
            "native_parent_basket_id": "parent-basket",
            "native_leg_type": "stop_loss",
        },
    }
    values.update(overrides)
    return Order(**values)


def _scoped_trade(**overrides) -> Trade:
    values = {
        "id": "trade-1",
        "order_id": "order-1",
        "exchange_trade_id": None,
        "product_id": _PRODUCT_ID,
        "side": "sell",
        "price": Decimal("20000.2500"),
        "quantity": Decimal("1.000"),
        "fee": Decimal("-0.000"),
        "fee_asset": None,
        "timestamp": 1_700_000_001_000,
    }
    values.update(overrides)
    return Trade(**values)


def _seed_scoped_rows(session_factory) -> None:
    _prepare_database(session_factory)
    with session_factory() as session:
        session.add(_scoped_order())
        session.flush()
        session.add(_scoped_trade())
        session.commit()


def _cut(session_factory) -> str:
    with session_factory() as session:
        return compute_rithmic_operator_flat_cut(
            session,
            profile=_PROFILE,
            account_id=_ACCOUNT_ID,
            product_ids=[_PRODUCT_ID],
        )


def _prior_position(quantity: str = "1.2300") -> Position:
    return Position(
        strategy_id="test_strategy",
        product_id=_PRODUCT_ID,
        side=PositionSide.LONG,
        quantity=Decimal(quantity),
        entry_price=Decimal("20000.2500"),
        unrealized_pnl=Decimal("0"),
    )


def _write_baseline(session_factory, **overrides):
    values = {
        "operation_id": "operation-1",
        "actor": "operator-a",
        "profile": " paper ",
        "account_id": " account-1 ",
        "product_ids": [_PRODUCT_ID],
        "observed_at_ms": 1_700_000_002_000,
        "prior_positions": [_prior_position()],
    }
    values.update(overrides)
    return write_rithmic_operator_flat_baseline(session_factory, **values)


def _order_trade_history(session_factory):
    with session_factory() as session:
        orders = session.execute(select(Order.__table__)).all()
        trades = session.execute(select(Trade.__table__)).all()
    return orders, trades


def _baseline_event_count(session_factory) -> int:
    with session_factory() as session:
        return len(session.scalars(select(SystemEvent)).all())


def _projection_digest(orders, trades) -> str:
    encoded = json.dumps(
        {"orders": orders, "trades": trades},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def test_cut_uses_exact_scoped_rows_and_canonical_decimal_document(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    expected_document = {
        "orders": [
            {
                "id": "order-1",
                "exchange_id": "rithmic",
                "account_profile": _PROFILE,
                "account_id": _ACCOUNT_ID,
                "strategy_id": "test_strategy",
                "product_id": _PRODUCT_ID,
                "client_order_id": "client-1",
                "exchange_order_id": "basket-1",
                "type": "stop_loss",
                "side": "sell",
                "quantity": "2",
                "status": "SUBMITTED",
                "filled_quantity": "0",
                "filled_price": None,
                "placement_mode": "attach-at-entry",
                "pending_entry_order_id": None,
                "native_parent_client_order_id": "parent-client",
                "native_parent_basket_id": "parent-basket",
                "native_leg_type": "stop_loss",
            }
        ],
        "trades": [
            {
                "id": "trade-1",
                "order_id": "order-1",
                "exchange_trade_id": None,
                "product_id": _PRODUCT_ID,
                "side": "sell",
                "price": "20000.25",
                "quantity": "1",
                "fee": "0",
                "fee_asset": None,
            }
        ],
    }
    canonical_bytes = json.dumps(
        expected_document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    initial = _cut(sqlite_order_session_factory)
    assert initial == hashlib.sha256(canonical_bytes).hexdigest()

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None
        order.filled_quantity = None
        session.commit()
    assert _cut(sqlite_order_session_factory) != initial

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None
        order.filled_quantity = Decimal("0")
        session.commit()
    assert _cut(sqlite_order_session_factory) == initial

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None
        order.timestamp += 500
        order.submitted_at = datetime(2026, 10, 7, tzinfo=timezone.utc)
        order.acked_at = datetime(2026, 10, 7, tzinfo=timezone.utc)
        order.last_reconciled_at = datetime(2026, 10, 7, tzinfo=timezone.utc)
        session.add_all(
            [
                _scoped_order(
                    id="other-profile",
                    account_profile="other-profile",
                    exchange_order_id="other-profile-basket",
                ),
                _scoped_order(
                    id="other-account",
                    account_id="other-account",
                    exchange_order_id="other-account-basket",
                ),
                _scoped_order(
                    id="other-product",
                    product_id="BINANCE:BTCUSDT-PERP",
                    exchange_order_id="other-product-basket",
                    client_order_id="other-product-client",
                ),
                _scoped_order(
                    id="other-exchange",
                    exchange_id="BINANCE",
                    exchange_order_id="other-exchange-basket",
                    client_order_id="other-exchange-client",
                ),
            ]
        )
        session.commit()
    assert _cut(sqlite_order_session_factory) == initial

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None
        order.status = "CANCELLED"
        session.commit()
    assert _cut(sqlite_order_session_factory) != initial

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        trade = session.get(Trade, "trade-1")
        assert order is not None and trade is not None
        order.status = "SUBMITTED"
        trade.fee = Decimal("0.01")
        session.commit()
    assert _cut(sqlite_order_session_factory) != initial


def test_cut_rejects_trade_product_mismatch(sqlite_order_session_factory):
    _seed_scoped_rows(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        trade = session.get(Trade, "trade-1")
        assert trade is not None
        trade.product_id = "BINANCE:BTCUSDT-PERP"
        session.commit()

    with sqlite_order_session_factory() as session:
        with pytest.raises(
            ValueError,
            match="rithmic_operator_flat_baseline_trade_product_mismatch",
        ):
            compute_rithmic_operator_flat_cut(
                session,
                profile=_PROFILE,
                account_id=_ACCOUNT_ID,
                product_ids=[_PRODUCT_ID],
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exchange_id", "BINANCE"),
        ("account_profile", "other-profile"),
        ("account_id", "other-account"),
        ("strategy_id", "other_strategy"),
        ("product_id", "BINANCE:BTCUSDT-PERP"),
        ("client_order_id", "client-2"),
        ("exchange_order_id", "basket-2"),
        ("type", "take_profit"),
        ("side", "buy"),
        ("quantity", Decimal("2.0001")),
        ("status", "CANCELLED"),
        ("filled_quantity", Decimal("0.125")),
        ("filled_price", Decimal("20000.25")),
    ],
)
def test_every_order_cut_field_change_invalidates_cut(
    sqlite_order_session_factory,
    field,
    value,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    initial = _cut(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None
        setattr(order, field, value)
        session.commit()
    assert _cut(sqlite_order_session_factory) != initial


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("placement_mode", "manual"),
        ("pending_entry_order_id", "pending-entry-2"),
        ("native_parent_client_order_id", "parent-client-2"),
        ("native_parent_basket_id", "parent-basket-2"),
        ("native_leg_type", "take_profit"),
    ],
)
def test_every_order_intent_cut_field_change_invalidates_cut(
    sqlite_order_session_factory,
    field,
    value,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    initial = _cut(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None and order.intent_payload is not None
        order.intent_payload = {**order.intent_payload, field: value}
        session.commit()
    assert _cut(sqlite_order_session_factory) != initial


@pytest.mark.parametrize(
    ("field", "value", "unscoped"),
    [
        ("order_id", "order-2", False),
        ("exchange_trade_id", "exchange-trade-2", False),
        ("product_id", "BINANCE:BTCUSDT-PERP", True),
        ("side", "buy", False),
        ("price", Decimal("20000.26"), False),
        ("quantity", Decimal("1.001"), False),
        ("fee", Decimal("0.001"), False),
        ("fee_asset", "USD", False),
    ],
)
def test_every_trade_cut_field_change_invalidates_or_rejects_cut(
    sqlite_order_session_factory,
    field,
    value,
    unscoped,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        session.add(
            _scoped_order(
                id="order-2",
                exchange_order_id="basket-2",
                client_order_id="client-2",
            )
        )
        session.commit()
    initial = _cut(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        trade = session.get(Trade, "trade-1")
        assert trade is not None
        if field == "order_id":
            value = "order-2"
        setattr(trade, field, value)
        session.commit()
    if unscoped:
        with sqlite_order_session_factory() as session:
            with pytest.raises(
                ValueError,
                match="rithmic_operator_flat_baseline_trade_product_mismatch",
            ):
                compute_rithmic_operator_flat_cut(
                    session,
                    profile=_PROFILE,
                    account_id=_ACCOUNT_ID,
                    product_ids=[_PRODUCT_ID],
                )
    else:
        assert _cut(sqlite_order_session_factory) != initial


def test_order_and_trade_primary_key_changes_change_canonical_rows():
    order = _order_cut_row(_scoped_order())
    changed_order = _order_cut_row(_scoped_order(id="order-2"))
    trade = _trade_cut_row(_scoped_trade())
    changed_trade = _trade_cut_row(_scoped_trade(id="trade-2"))
    assert _projection_digest([changed_order], [trade]) != _projection_digest(
        [order], [trade]
    )
    assert _projection_digest([order], [changed_trade]) != _projection_digest(
        [order], [trade]
    )


def test_joined_trades_outside_exact_account_product_or_venue_are_excluded(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    initial = _cut(sqlite_order_session_factory)
    variants = [
        {"account_profile": "other-profile"},
        {"account_id": "other-account"},
        {"product_id": "BINANCE:BTCUSDT-PERP"},
        {"exchange_id": "BINANCE", "product_id": "BINANCE:BTCUSDT-PERP"},
    ]
    with sqlite_order_session_factory() as session:
        for index, overrides in enumerate(variants):
            order_id = f"out-of-scope-order-{index}"
            session.add(
                _scoped_order(
                    id=order_id,
                    client_order_id=f"out-of-scope-client-{index}",
                    exchange_order_id=f"out-of-scope-basket-{index}",
                    **overrides,
                )
            )
            session.add(
                _scoped_trade(
                    id=f"out-of-scope-trade-{index}",
                    order_id=order_id,
                    product_id=overrides.get("product_id", _PRODUCT_ID),
                )
            )
        session.commit()
    assert _cut(sqlite_order_session_factory) == initial


def test_cut_orders_ids_by_exact_text_not_database_collation(
    sqlite_order_session_factory,
):
    _prepare_database(sqlite_order_session_factory)
    identifiers = ["a", "A", "a-10", "a-2"]
    with sqlite_order_session_factory() as session:
        session.add_all(
            [
                _scoped_order(
                    id=identifier,
                    exchange_order_id=f"basket-{identifier}",
                    client_order_id=f"client-{identifier}",
                )
                for identifier in reversed(identifiers)
            ]
        )
        session.add_all(
            [
                _scoped_trade(
                    id=f"trade-{identifier}",
                    order_id="a",
                    exchange_trade_id=f"exchange-{identifier}",
                )
                for identifier in reversed(identifiers)
            ]
        )
        session.commit()

    with sqlite_order_session_factory() as session:
        rows = [_order_cut_row(order) for order in session.scalars(select(Order)).all()]
        trade_rows = [
            _trade_cut_row(trade) for trade in session.scalars(select(Trade)).all()
        ]
    expected_orders = sorted(rows, key=lambda row: cast(str, row["id"]))
    expected_trades = sorted(trade_rows, key=lambda row: cast(str, row["id"]))
    expected_bytes = json.dumps(
        {"orders": expected_orders, "trades": expected_trades},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert (
        _cut(sqlite_order_session_factory) == hashlib.sha256(expected_bytes).hexdigest()
    )


def test_baseline_sql_write_read_duplicate_and_conflict(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    original_history = _order_trade_history(sqlite_order_session_factory)
    first = _write_baseline(sqlite_order_session_factory)
    assert first.event_id > 0
    assert first.payload == {
        "version": 1,
        "operation_id": "operation-1",
        "actor": "operator-a",
        "profile": _PROFILE,
        "account_id": _ACCOUNT_ID,
        "product_ids": [_PRODUCT_ID],
        "observed_at_ms": 1_700_000_002_000,
        "prior_positions": [
            {
                "strategy_id": "test_strategy",
                "product_id": _PRODUCT_ID,
                "side": "LONG",
                "quantity": "1.23",
                "entry_price": "20000.25",
            }
        ],
        "target": "FLAT",
        "cut_sha256": _cut(sqlite_order_session_factory),
        "economics_unresolved": True,
        "economics_reason": "provider_exit_fill_unattributed",
    }
    with sqlite_order_session_factory() as session:
        events = session.scalars(select(SystemEvent)).all()
    assert len(events) == 1
    assert events[0].event_type == "ops"
    assert events[0].event_subtype == "rithmic_operator_flat_baseline"
    assert _order_trade_history(sqlite_order_session_factory) == original_history

    loaded = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=_PROFILE,
        account_id=_ACCOUNT_ID,
        product_ids=[_PRODUCT_ID],
    )
    assert loaded.reason == "operator_flat_baseline"
    assert loaded.record == first
    assert _order_trade_history(sqlite_order_session_factory) == original_history

    replay = _write_baseline(
        sqlite_order_session_factory,
        actor="operator-b",
        observed_at_ms=1_700_000_009_000,
        prior_positions=[_prior_position("9.5")],
    )
    assert replay == first
    with sqlite_order_session_factory() as session:
        assert len(session.scalars(select(SystemEvent)).all()) == 1

    with pytest.raises(
        ValueError,
        match="rithmic_operator_flat_baseline_operation_conflict",
    ):
        _write_baseline(sqlite_order_session_factory, profile="other-profile")
    with pytest.raises(
        ValueError,
        match="rithmic_operator_flat_baseline_operation_conflict",
    ):
        _write_baseline(sqlite_order_session_factory, account_id="other-account")
    with pytest.raises(
        ValueError,
        match="rithmic_operator_flat_baseline_operation_conflict",
    ):
        _write_baseline(
            sqlite_order_session_factory,
            product_ids=["BINANCE:BTCUSDT-PERP"],
            prior_positions=[],
        )
    assert _order_trade_history(sqlite_order_session_factory) == original_history

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        assert order is not None
        order.status = "CANCELLED"
        session.commit()
    changed_history = _order_trade_history(sqlite_order_session_factory)
    with pytest.raises(
        ValueError,
        match="rithmic_operator_flat_baseline_operation_conflict",
    ):
        _write_baseline(sqlite_order_session_factory)
    assert _order_trade_history(sqlite_order_session_factory) == changed_history


@pytest.mark.parametrize(
    ("failure_point", "expected_event_count"),
    [
        ("write_lookup", 0),
        ("flush", 0),
        ("commit", 0),
        ("refresh", 1),
    ],
)
def test_sql_write_failures_propagate_without_losing_durable_replay(
    sqlite_order_session_factory,
    monkeypatch,
    failure_point,
    expected_event_count,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    baseline_flush_completed = False

    def fail(*_args, **_kwargs):
        raise RuntimeError(f"injected_{failure_point}")

    with monkeypatch.context() as patcher:
        if failure_point == "write_lookup":
            original_scalars = Session.scalars

            def fail_event_lookup(self, statement, *args, **kwargs):
                if "system_events" in str(statement).lower():
                    fail()
                return original_scalars(self, statement, *args, **kwargs)

            patcher.setattr(Session, "scalars", fail_event_lookup)
        elif failure_point == "flush":
            original_flush = Session.flush

            def flush_baseline_then_fail(self, *args, **kwargs):
                nonlocal baseline_flush_completed
                has_baseline_event = any(
                    isinstance(instance, SystemEvent)
                    and instance.event_subtype == "rithmic_operator_flat_baseline"
                    for instance in self.new
                )
                original_flush(self, *args, **kwargs)
                if has_baseline_event:
                    baseline_flush_completed = True
                    fail()

            patcher.setattr(Session, "flush", flush_baseline_then_fail)
        else:
            patcher.setattr(Session, failure_point, fail)
        with pytest.raises(RuntimeError, match=f"injected_{failure_point}"):
            _write_baseline(sqlite_order_session_factory)

    if failure_point == "flush":
        assert baseline_flush_completed
    assert _baseline_event_count(sqlite_order_session_factory) == expected_event_count
    if failure_point == "refresh":
        with sqlite_order_session_factory() as session:
            persisted = session.scalars(select(SystemEvent)).one()
        original_event_id = int(getattr(persisted, "id"))
        replay = _write_baseline(sqlite_order_session_factory)
        assert replay.event_id == original_event_id
        assert replay.payload["actor"] == "operator-a"
        assert replay.payload["observed_at_ms"] == 1_700_000_002_000
        assert _baseline_event_count(sqlite_order_session_factory) == 1


@pytest.mark.parametrize("failure_point", ["event_lookup", "cut_trade_query"])
def test_sql_read_failures_propagate(
    sqlite_order_session_factory,
    monkeypatch,
    failure_point,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    _write_baseline(sqlite_order_session_factory)

    def fail(*_args, **_kwargs):
        raise RuntimeError(f"injected_{failure_point}")

    with monkeypatch.context() as patcher:
        if failure_point == "event_lookup":
            original_scalars = Session.scalars

            def fail_event_lookup(self, statement, *args, **kwargs):
                if "system_events" in str(statement).lower():
                    fail()
                return original_scalars(self, statement, *args, **kwargs)

            patcher.setattr(Session, "scalars", fail_event_lookup)
        else:
            patcher.setattr(Session, "execute", fail)
        with pytest.raises(RuntimeError, match=f"injected_{failure_point}"):
            read_latest_rithmic_operator_flat_baseline(
                sqlite_order_session_factory,
                profile=_PROFILE,
                account_id=_ACCOUNT_ID,
                product_ids=[_PRODUCT_ID],
            )
    assert _baseline_event_count(sqlite_order_session_factory) == 1


@pytest.mark.parametrize(
    "invalidity",
    [
        "unknown_version",
        "extra_field",
        "missing_field",
        "noncanonical_decimal",
        "wrong_target",
        "economics_resolved",
        "bad_cut_digest",
        "invalid_observation_time",
    ],
)
def test_latest_unknown_or_invalid_schema_is_not_a_baseline(
    sqlite_order_session_factory,
    invalidity,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    first = _write_baseline(sqlite_order_session_factory)
    invalid_payload = dict(first.payload)
    prior_positions = cast(list[dict[str, object]], first.payload["prior_positions"])
    if invalidity == "unknown_version":
        invalid_payload["version"] = 2
    else:
        if invalidity == "extra_field":
            invalid_payload["unexpected"] = "not-v1"
        elif invalidity == "missing_field":
            del invalid_payload["actor"]
        elif invalidity == "noncanonical_decimal":
            prior_positions[0]["quantity"] = "1.2300"
        elif invalidity == "wrong_target":
            invalid_payload["target"] = "OPEN"
        elif invalidity == "economics_resolved":
            invalid_payload["economics_unresolved"] = False
        elif invalidity == "bad_cut_digest":
            invalid_payload["cut_sha256"] = "not-a-digest"
        else:
            invalid_payload["observed_at_ms"] = True
    with sqlite_order_session_factory() as session:
        session.add(
            SystemEvent(
                event_type="ops",
                event_subtype="rithmic_operator_flat_baseline",
                payload=invalid_payload,
            )
        )
        session.commit()

    result = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=_PROFILE,
        account_id=_ACCOUNT_ID,
        product_ids=[_PRODUCT_ID],
    )
    assert result.record is None
    assert result.reason == "scope_or_version_inapplicable"


def test_stored_target_conflict_for_same_operation_rejects_write(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    first = _write_baseline(sqlite_order_session_factory)
    history = _order_trade_history(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        event = session.get(SystemEvent, first.event_id)
        assert event is not None and isinstance(event.payload, dict)
        event.payload = {**event.payload, "target": "OPEN"}
        session.commit()

    with pytest.raises(
        ValueError,
        match="rithmic_operator_flat_baseline_operation_conflict",
    ):
        _write_baseline(sqlite_order_session_factory)
    assert _baseline_event_count(sqlite_order_session_factory) == 1
    assert _order_trade_history(sqlite_order_session_factory) == history


def test_invalid_prior_position_product_is_not_a_baseline(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    baseline = _write_baseline(sqlite_order_session_factory)
    invalid_payload = dict(baseline.payload)
    prior_positions = cast(list[dict[str, object]], baseline.payload["prior_positions"])
    invalid_payload["prior_positions"] = [
        {
            **prior_positions[0],
            "product_id": "RITHMIC:OTHER",
        }
    ]
    with sqlite_order_session_factory() as session:
        session.add(
            SystemEvent(
                event_type="ops",
                event_subtype="rithmic_operator_flat_baseline",
                payload=invalid_payload,
            )
        )
        session.commit()

    result = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=_PROFILE,
        account_id=_ACCOUNT_ID,
        product_ids=[_PRODUCT_ID],
    )
    assert result.record is None
    assert result.reason == "scope_or_version_inapplicable"


def test_missing_baseline_is_not_applicable(sqlite_order_session_factory):
    _seed_scoped_rows(sqlite_order_session_factory)
    result = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=_PROFILE,
        account_id=_ACCOUNT_ID,
        product_ids=[_PRODUCT_ID],
    )
    assert result.record is None
    assert result.reason == "no_baseline"


def test_new_scoped_cut_invalidates_baseline_but_other_scope_does_not(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    baseline = _write_baseline(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        session.add(
            SystemEvent(
                event_type="ops",
                event_subtype="rithmic_operator_flat_baseline",
                payload={**baseline.payload, "account_id": "other-account"},
            )
        )
        session.commit()

    unchanged = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=_PROFILE,
        account_id=_ACCOUNT_ID,
        product_ids=[_PRODUCT_ID],
    )
    assert unchanged.record == baseline

    with sqlite_order_session_factory() as session:
        order = session.get(Order, "order-1")
        trade = session.get(Trade, "trade-1")
        assert order is not None and trade is not None
        order.status = "CANCELLED"
        trade.fee = Decimal("0.02")
        session.commit()
    changed = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=_PROFILE,
        account_id=_ACCOUNT_ID,
        product_ids=[_PRODUCT_ID],
    )
    assert changed.record is None
    assert changed.reason == "durable_cut_changed"


def test_empty_scope_and_non_decimal_money_are_rejected(
    sqlite_order_session_factory,
):
    _seed_scoped_rows(sqlite_order_session_factory)
    with sqlite_order_session_factory() as session:
        with pytest.raises(ValueError, match="product_scope_invalid"):
            compute_rithmic_operator_flat_cut(
                session,
                profile=_PROFILE,
                account_id=_ACCOUNT_ID,
                product_ids=[],
            )
    for value in (0.1, "1.2", Decimal("NaN"), Decimal("Infinity")):
        with pytest.raises(ValueError, match="rithmic_operator_flat_baseline"):
            _decimal_text(value, "test")
    assert _decimal_text(Decimal("-0.000000"), "test") == "0"
    assert (
        _decimal_text(
            Decimal("123456789012345678901234567890.12345678901234567890"),
            "test",
        )
        == "123456789012345678901234567890.1234567890123456789"
    )
