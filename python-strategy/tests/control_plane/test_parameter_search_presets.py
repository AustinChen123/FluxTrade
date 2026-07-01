from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.control_plane import GoldenCrossParameterSearchPreset
from src.control_plane.models import ParameterSearchJobRequest
from src.control_plane.search_space import generate_parameter_candidates


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"


def _preset_payload() -> dict:
    return {
        "strategy_id": "golden_cross_easy",
        "product_id": PRODUCT_ID,
        "timeframe": "15m",
        "start_time": 1_700_000_000_000,
        "end_time": 1_700_001_800_000,
        "short_window": {
            "min": 5,
            "max": 10,
            "step": 5,
        },
        "long_window": {
            "min": 20,
            "max": 30,
            "step": 10,
        },
        "quantity": {
            "min": "0.01",
            "max": "0.02",
            "step": "0.01",
        },
        "candidate_sample_count": 8,
        "seed": 17,
        "backtest": {
            "candles_csv_path": "data/BTCUSDT_15m.csv",
            "initial_balance": "10000",
            "maker_fee": "0.0002",
            "taker_fee": "0.0006",
        },
        "research_runner": {
            "capital_allocation": "1000",
        },
    }


def test_golden_cross_preset_builds_parameter_search_request():
    preset = GoldenCrossParameterSearchPreset.model_validate(_preset_payload())

    request = preset.to_parameter_search_request()

    assert isinstance(request, ParameterSearchJobRequest)
    assert request.strategy_id == "golden_cross_easy"
    assert request.product_id == PRODUCT_ID
    assert request.timeframe == "15m"
    assert request.candidates is None
    assert request.search_space is not None
    assert request.candidate_sample_count == 8
    assert request.seed == 17
    assert request.backtest is not None
    assert request.backtest.candles_csv_path == "data/BTCUSDT_15m.csv"
    assert request.research_runner is not None
    assert request.research_runner.capital_allocation == Decimal("1000")
    assert request.search_space.model_dump(mode="json") == {
        "parameters": {
            "short_window": {
                "type": "integer",
                "min": 5,
                "max": 10,
                "step": 5,
                "choices": None,
            },
            "long_window": {
                "type": "integer",
                "min": 20,
                "max": 30,
                "step": 10,
                "choices": None,
            },
            "quantity": {
                "type": "decimal",
                "min": "0.01",
                "max": "0.02",
                "step": "0.01",
                "choices": None,
            },
        }
    }


def test_golden_cross_preset_generates_same_candidates_as_manual_search_space():
    preset = GoldenCrossParameterSearchPreset.model_validate(_preset_payload())
    request = preset.to_parameter_search_request()
    manual_request = ParameterSearchJobRequest.model_validate(
        {
            **{
                key: value
                for key, value in _preset_payload().items()
                if key
                not in {
                    "short_window",
                    "long_window",
                    "quantity",
                    "research_runner",
                }
            },
            "search_space": {
                "parameters": {
                    "short_window": {
                        "type": "integer",
                        "min": 5,
                        "max": 10,
                        "step": 5,
                    },
                    "long_window": {
                        "type": "integer",
                        "min": 20,
                        "max": 30,
                        "step": 10,
                    },
                    "quantity": {
                        "type": "decimal",
                        "min": "0.01",
                        "max": "0.02",
                        "step": "0.01",
                    },
                }
            },
        }
    )
    assert request.search_space is not None
    assert manual_request.search_space is not None

    preset_candidates = generate_parameter_candidates(
        request.search_space,
        sample_count=request.candidate_sample_count or 0,
        seed=request.seed,
    )
    manual_candidates = generate_parameter_candidates(
        manual_request.search_space,
        sample_count=manual_request.candidate_sample_count or 0,
        seed=manual_request.seed,
    )

    assert [candidate.param_pack for candidate in preset_candidates] == [
        candidate.param_pack for candidate in manual_candidates
    ]


def test_golden_cross_preset_rejects_window_ranges_that_can_generate_invalid_pairs():
    payload = _preset_payload()
    payload["short_window"] = {
        "min": 5,
        "max": 20,
        "step": 5,
    }
    payload["long_window"] = {
        "min": 20,
        "max": 40,
        "step": 10,
    }

    with pytest.raises(ValidationError, match="short_window.max < long_window.min"):
        GoldenCrossParameterSearchPreset.model_validate(payload)


def test_golden_cross_preset_rejects_boolean_integer_ranges():
    payload = _preset_payload()
    payload["short_window"]["min"] = True

    with pytest.raises(ValidationError, match="cannot be boolean"):
        GoldenCrossParameterSearchPreset.model_validate(payload)


def test_golden_cross_preset_rejects_non_positive_quantity():
    payload = _preset_payload()
    payload["quantity"] = {
        "min": "0",
        "max": "0.02",
        "step": "0.01",
    }

    with pytest.raises(ValidationError, match="quantity range min must be positive"):
        GoldenCrossParameterSearchPreset.model_validate(payload)
