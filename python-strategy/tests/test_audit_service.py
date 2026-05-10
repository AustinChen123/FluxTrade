from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.core.audit_service import (
    build_signal_audit,
    build_signal_intent_audit,
    commit_signal_audit,
    write_signal_audit_intent,
    write_signal_audit_outcome,
    write_system_event,
)
from src.core.models import Candlestick, Signal, SignalType


def _make_signal() -> Signal:
    return Signal(
        strategy_id="strat-1",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1704067200000,
        type=SignalType.LONG,
        value=Decimal("42000.12"),
        quantity=Decimal("0.25"),
        metadata={
            "threshold": Decimal("0.75"),
            "levels": [Decimal("41900.1"), {"target": Decimal("43000.2")}],
        },
    )


def _make_candle() -> Candlestick:
    return Candlestick(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1704067200000,
        open=Decimal("41000.1"),
        high=Decimal("43000.2"),
        low=Decimal("40500.3"),
        close=Decimal("42000.4"),
        volume=Decimal("123.45"),
    )


def test_build_signal_audit_uses_jsonb_native_payload() -> None:
    clock = MagicMock()
    clock.now.return_value = 1704067200.123

    audit = build_signal_audit(
        clock=clock,
        signal=_make_signal(),
        candle=_make_candle(),
        risk_passed=True,
        risk_message="PASS",
        order_id="order-1",
    )

    assert audit.timestamp == 1704067200123
    assert audit.strategy_id == "strat-1"
    assert audit.product_id == "BINANCE:BTCUSDT-PERP"
    assert audit.signal_type == "LONG"
    assert audit.risk_status == "PASS"
    assert audit.risk_message == "PASS"
    assert audit.order_id == "order-1"
    assert audit.details_json["candle"]["close"] == "42000.4"
    assert audit.details_json["signal_metadata"]["threshold"] == "0.75"
    assert audit.details_json["signal_metadata"]["levels"] == [
        "41900.1",
        {"target": "43000.2"},
    ]


def test_build_signal_audit_records_reject_without_order() -> None:
    audit = build_signal_audit(
        clock=MagicMock(now=MagicMock(return_value=1704067200.0)),
        signal=_make_signal(),
        candle=None,
        risk_passed=False,
        risk_message="REJECT: no balance",
        order_id=None,
    )

    assert audit.risk_status == "REJECT"
    assert audit.risk_message == "REJECT: no balance"
    assert audit.order_id is None
    assert audit.details_json["candle"] is None


def test_commit_signal_audit_rolls_back_and_raises_on_failure() -> None:
    session = MagicMock()
    audit = MagicMock()
    session.commit.side_effect = RuntimeError("audit write failed")

    with pytest.raises(RuntimeError, match="audit write failed"):
        commit_signal_audit(session, audit)

    session.add.assert_called_once_with(audit)
    session.rollback.assert_called_once()


def test_write_system_event_adds_decimal_safe_payload() -> None:
    session = MagicMock()

    event = write_system_event(
        session,
        event_type="reconcile",
        event_subtype="balance",
        payload={
            "balance": Decimal("1000.25"),
            "positions": [{"size": Decimal("0.5")}],
        },
        related_strategy_id="strat-1",
        related_order_id="order-1",
        related_gene_id=42,
    )

    assert event.event_type == "reconcile"
    assert event.event_subtype == "balance"
    assert event.related_strategy_id == "strat-1"
    assert event.related_order_id == "order-1"
    assert event.related_gene_id == 42
    assert event.payload == {
        "balance": "1000.25",
        "positions": [{"size": "0.5"}],
    }
    session.add.assert_called_once_with(event)
    session.commit.assert_not_called()


def test_write_system_event_supports_gene_promote() -> None:
    session = MagicMock()

    event = write_system_event(
        session,
        event_type="gene_promote",
        payload={"fitness": Decimal("1.2345")},
        related_gene_id=7,
    )

    assert event.event_type == "gene_promote"
    assert event.related_gene_id == 7
    assert event.payload == {"fitness": "1.2345"}
    session.add.assert_called_once_with(event)


def test_write_system_event_rejects_unknown_type() -> None:
    session = MagicMock()

    with pytest.raises(ValueError, match="unsupported system event type"):
        write_system_event(
            session,
            event_type="unknown",
            payload={},
        )

    session.add.assert_not_called()


def test_build_signal_intent_audit_serializes_payload() -> None:
    clock = MagicMock()
    clock.now.return_value = 1704067200.5

    audit = build_signal_intent_audit(
        clock=clock,
        signal=_make_signal(),
        client_order_id="strat-1-abc",
        signal_batch_id="batch-1",
        intent_payload={
            "quantity": Decimal("0.25"),
            "nested": {"price": Decimal("42000.12")},
        },
    )

    assert audit.timestamp == 1704067200500
    assert audit.strategy_id == "strat-1"
    assert audit.signal_type == "LONG"
    assert audit.risk_status == "PASS"
    assert audit.risk_message == "PASS"
    assert audit.client_order_id == "strat-1-abc"
    assert audit.signal_batch_id == "batch-1"
    assert audit.intent_payload == {
        "quantity": "0.25",
        "nested": {"price": "42000.12"},
    }


def test_write_signal_audit_intent_flushes_and_commits() -> None:
    session = MagicMock()
    audit = MagicMock()

    result = write_signal_audit_intent(session, audit)

    assert result is audit
    session.add.assert_called_once_with(audit)
    session.flush.assert_called_once()
    session.commit.assert_called_once()
    session.rollback.assert_not_called()


def test_write_signal_audit_intent_rolls_back_and_raises_on_failure() -> None:
    session = MagicMock()
    audit = MagicMock()
    session.flush.side_effect = RuntimeError("intent failed")

    with pytest.raises(RuntimeError, match="intent failed"):
        write_signal_audit_intent(session, audit)

    session.rollback.assert_called_once()


def test_write_signal_audit_outcome_updates_payload_and_commits() -> None:
    session = MagicMock()
    audit = MagicMock()

    result = write_signal_audit_outcome(
        session,
        audit,
        order_id="order-1",
        risk_message="placed",
        outcome_payload={
            "exchange_order_id": "ex-1",
            "fee": Decimal("1.23"),
        },
    )

    assert result is audit
    assert audit.order_id == "order-1"
    assert audit.risk_message == "placed"
    assert audit.outcome_payload == {
        "exchange_order_id": "ex-1",
        "fee": "1.23",
    }
    session.add.assert_called_once_with(audit)
    session.commit.assert_called_once()
    session.rollback.assert_not_called()


def test_write_signal_audit_outcome_rolls_back_and_raises_on_failure() -> None:
    session = MagicMock()
    audit = MagicMock()
    session.commit.side_effect = RuntimeError("outcome failed")

    with pytest.raises(RuntimeError, match="outcome failed"):
        write_signal_audit_outcome(
            session,
            audit,
            outcome_payload={"error": "network"},
        )

    session.rollback.assert_called_once()
