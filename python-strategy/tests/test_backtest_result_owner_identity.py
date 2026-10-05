from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from typing import cast

import pytest

from src.core.backtest_result_owner import (
    BacktestResultRunIdentity,
    canonical_input_digest,
    canonical_input_payload,
)
from src.core.product_registry import FeeModel, InstrumentSpec, MarketType


def _identity(**overrides) -> BacktestResultRunIdentity:
    values = {
        "job_id": "job-1",
        "strategy_id": "golden-cross",
        "artifact_version": "2.1.0",
        "catalog_sha256": "a" * 64,
        "dataset_id": "dataset-1",
        "dataset_checksum_sha256": "b" * 64,
        "product_id": "BINANCE:BTCUSDT-SPOT",
        "timeframe": "1m",
        "currency": "USDT",
        "start": 1_767_225_600_000,
        "end": 1_767_398_400_000,
        "initial_balance": Decimal("1000.00"),
        "instrument": InstrumentSpec(
            "BINANCE:BTCUSDT-SPOT",
            "binance",
            "BTC/USDT",
            "BTC",
            "USDT",
            quantity_step=Decimal("0.001"),
            price_tick=Decimal("0.01"),
            market_type=MarketType.SPOT,
        ),
        "maker_fee": Decimal("0.001"),
        "taker_fee": Decimal("0.002"),
        "drawdown_limit": Decimal("0.25"),
        "execution_timeframe": None,
    }
    return BacktestResultRunIdentity(**(values | overrides))


def test_canonical_input_binds_identity_and_uses_canonical_decimal_text():
    identity = _identity()
    payload = canonical_input_payload(identity)
    assert payload["subject"] == {
        "kind": "STRATEGY_ARTIFACT",
        "id": "golden-cross:2.1.0",
        "digest": "a" * 64,
    }
    assert payload["dataset"] == {"id": "dataset-1", "checksum_sha256": "b" * 64}
    assert payload["initial_balance"] == "1000"
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    assert canonical_input_digest(identity) == hashlib.sha256(encoded).hexdigest()
    assert canonical_input_digest(
        _identity(dataset_id="dataset-2")
    ) != canonical_input_digest(identity)


@pytest.mark.parametrize(
    "field,bad",
    [
        (field, bad)
        for field in (
            "quantity_step",
            "price_tick",
            "min_notional",
            "min_quantity",
            "multiplier",
            "tick_value",
            "capital_per_contract",
        )
        for bad in (
            "0.1",
            0.1,
            True,
            Decimal("NaN"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
        )
    ],
)
def test_instrument_numeric_fields_require_finite_exact_decimals(field, bad):
    instrument = replace(_identity().instrument, **{field: cast(Decimal, bad)})
    with pytest.raises(ValueError):
        canonical_input_payload(_identity(instrument=instrument))


@pytest.mark.parametrize(
    "instrument",
    [
        replace(_identity().instrument, fee_model=cast(FeeModel, MarketType.SPOT)),
        replace(
            _identity().instrument,
            capital_model=cast(MarketType, FeeModel.PER_CONTRACT),
        ),
    ],
)
def test_instrument_declared_enums_reject_other_enum_types(instrument):
    with pytest.raises(ValueError):
        canonical_input_payload(_identity(instrument=instrument))
