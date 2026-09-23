from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import MagicMock
from typing import Any
from decimal import Decimal

import pytest

from src.core import live_candle_application as owner
from test_signal_processor import make_candle
from test_profile_decision_application import batch


def setup(monkeypatch):
    candle = make_candle()
    row = SimpleNamespace(**candle.model_dump())
    receipt = SimpleNamespace(**candle.model_dump(), decision_contract_version=1)
    db = MagicMock()
    state: dict[str, Any] = {"row": row, "receipt": receipt}
    db.get.side_effect = (
        lambda model, identity: state["row"]
        if model is owner.ORMCandlestick
        else state["receipt"]
    )
    terminal = batch()
    read = MagicMock(
        return_value=owner.DecisionBatchRecord(
            terminal, datetime(2026, 1, 1, tzinfo=timezone.utc), True, ()
        )
    )
    monkeypatch.setattr(owner, "read_decision_batch", read)
    factory = MagicMock(side_effect=AssertionError("no new session"))
    service = owner.LiveCandleApplicationService(
        environment_identity=lambda: "live", db_session_factory=factory
    )
    args: dict[str, Any] = dict(
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        bar_start_ms=candle.timestamp,
        db=db,
    )
    return service, args, candle, state, read, terminal


def test_reads_exact_canonical_candle_and_reuses_receipt_classifier(monkeypatch):
    service, args, candle, _, read, terminal = setup(monkeypatch)
    result, evidence = service.read_applied_candle(**args)
    assert result == candle and result is not candle
    assert evidence is read.return_value and evidence.batch is terminal
    assert evidence.verified_inputs == ()
    db = args["db"]
    assert db.get.call_args_list[0].args == (
        owner.ORMCandlestick,
        (candle.product_id, candle.timeframe, candle.timestamp),
    )
    read.assert_called_once_with(
        db,
        environment="live",
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        bar_start_ms=candle.timestamp,
    )
    db.query.assert_not_called()
    db.commit.assert_not_called()


@pytest.mark.parametrize(
    "damage",
    [
        "candle",
        "receipt",
        "legacy",
        "batch",
        "receipt_value",
        "canonical_value",
        "identity",
        "nonlive",
    ],
)
def test_missing_and_conflicting_evidence_rejected(monkeypatch, damage):
    service, args, _, state, read, _ = setup(monkeypatch)
    if damage in ("candle", "receipt"):
        state["row" if damage == "candle" else "receipt"] = None
    elif damage == "legacy":
        state["receipt"].decision_contract_version = None
    elif damage == "batch":
        read.return_value = None
    elif damage == "receipt_value":
        state["receipt"].close += 1
    elif damage == "canonical_value":
        original = state["row"]
        changed = SimpleNamespace(**vars(original))
        changed.close += 1
        args["db"].get.side_effect = [original, state["receipt"], changed]
    elif damage == "identity":
        state["row"].timestamp += 1
    else:
        service._environment_identity = lambda: "backtest"
    with pytest.raises((owner.DecisionBatchIntegrityError, RuntimeError)):
        service.read_applied_candle(**args)


@pytest.mark.parametrize(
    "field,value",
    [
        ("product_id", True),
        ("product_id", "bad"),
        ("timeframe", "bad:frame"),
        ("timeframe", ""),
        ("bar_start_ms", True),
        ("bar_start_ms", -1),
        ("bar_start_ms", 2**63),
    ],
)
def test_invalid_before_database(monkeypatch, field, value):
    service, args, _, _, _, _ = setup(monkeypatch)
    with pytest.raises(owner.DecisionBatchIntegrityError):
        service.read_applied_candle(**(args | {field: value}))
    args["db"].get.assert_not_called()


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
@pytest.mark.parametrize(
    "value", [1, Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")]
)
def test_non_finite_or_non_decimal_canonical_value_rejected_before_receipt(
    monkeypatch, field, value
):
    service, args, _, state, read, _ = setup(monkeypatch)
    setattr(state["row"], field, value)

    with pytest.raises(owner.DecisionBatchIntegrityError):
        service.read_applied_candle(**args)

    assert args["db"].get.call_count == 1
    read.assert_not_called()
