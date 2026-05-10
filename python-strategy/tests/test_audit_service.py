from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.core.audit_service import build_signal_audit, commit_signal_audit
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
