from __future__ import annotations

import json
from decimal import Decimal

from src.control_plane.backtest_jobs import _json_safe
from src.core.backtest.run_evidence import (
    BacktestFillRecord,
    BacktestRunProvenance,
    canonical_decision_snapshot,
)
from src.core.models import PositionSide
from src.core.strategy_context import (
    CapitalSnapshot,
    PositionSnapshot,
    RiskSnapshot,
    StrategyContext,
)


def test_json_safe_recursively_projects_real_backtest_evidence_dataclasses():
    fill = BacktestFillRecord(
        fill_sequence=3,
        strategy_id="golden_cross",
        product_id="BINANCE:BTCUSDT-PERP",
        side="LONG",
        price=Decimal("123.4500"),
        quantity=Decimal("0.25"),
        fee=Decimal("0.012345"),
        fee_asset="USDT",
        timestamp=1_704_067_200_000,
    )
    provenance = BacktestRunProvenance(
        schema_version="fluxtrade.backtest.v1",
        runner_kind="research",
        dataset_identity_version="v1",
        dataset_sha256="a" * 64,
        dataset_candle_count=12,
        dataset_first_timestamp=1_704_067_200_000,
        dataset_last_timestamp=1_704_067_860_000,
        program_version="test",
        program_sha256="b" * 64,
        extension_version="test",
        extension_sha256="c" * 64,
        configuration_identity_version="v1",
        configuration_sha256="d" * 64,
        runner_configuration_sha256="e" * 64,
        matching_model="rust",
    )
    decision_context = StrategyContext(
        strategy_id="golden_cross",
        product_id="BINANCE:BTCUSDT-PERP",
        timestamp=1_704_067_200_000,
        available_cash=Decimal("950.00"),
        total_equity=Decimal("1000.00"),
        realized_pnl=Decimal("0.00"),
        unrealized_pnl=Decimal("50.00"),
        current_drawdown=Decimal("0.00"),
        max_drawdown=Decimal("0.00"),
        position=PositionSnapshot(
            side=PositionSide.LONG,
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("100.00"),
            mark_price=Decimal("101.00"),
            notional=Decimal("50.50"),
            unrealized_pnl=Decimal("0.50"),
        ),
        risk=RiskSnapshot(trading_enabled=True),
        capital=CapitalSnapshot(
            allocated=Decimal("1000.00"),
            used=Decimal("50.00"),
            available=Decimal("950.00"),
            unallocated=Decimal("0.00"),
        ),
    )

    projected = _json_safe(
        {
            "fills": (fill,),
            "provenance": provenance,
            "decision": canonical_decision_snapshot(decision_context),
        }
    )

    assert projected["fills"] == [
        {
            "fill_sequence": 3,
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "side": "LONG",
            "price": "123.4500",
            "quantity": "0.25",
            "fee": "0.012345",
            "fee_asset": "USDT",
            "timestamp": 1_704_067_200_000,
        }
    ]
    assert projected["provenance"] == {
        "schema_version": "fluxtrade.backtest.v1",
        "runner_kind": "research",
        "dataset_identity_version": "v1",
        "dataset_sha256": "a" * 64,
        "dataset_candle_count": 12,
        "dataset_first_timestamp": 1_704_067_200_000,
        "dataset_last_timestamp": 1_704_067_860_000,
        "program_version": "test",
        "program_sha256": "b" * 64,
        "extension_version": "test",
        "extension_sha256": "c" * 64,
        "configuration_identity_version": "v1",
        "configuration_sha256": "d" * 64,
        "runner_configuration_sha256": "e" * 64,
        "matching_model": "rust",
    }
    assert projected["decision"]["position"]["quantity"] == "0.5"
    assert projected["decision"]["position"]["side"] == "LONG"
    assert projected["decision"]["risk"] == {
        "trading_enabled": True,
        "reason": None,
    }
    assert projected["decision"]["capital"]["available"] == "950.00"
    assert json.loads(json.dumps(projected)) == projected


def test_json_safe_does_not_treat_dataclass_class_as_an_instance():
    assert _json_safe(BacktestFillRecord) is BacktestFillRecord


def test_json_safe_keeps_primitive_mapping_and_tuple_projection():
    value = {"integer": 2, "decimal": Decimal("1.250"), "nested": (True, "ok")}

    assert _json_safe(value) == {
        "integer": 2,
        "decimal": "1.250",
        "nested": [True, "ok"],
    }
