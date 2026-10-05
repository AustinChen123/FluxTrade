from decimal import Decimal

import pytest

from test_control_plane import PRODUCT_ID, TIMEFRAME, _write_research_candles
from src.control_plane.models import ParameterSearchJobRequest
import src.control_plane.parameter_evaluation as parameter_evaluation
from src.control_plane.parameter_evaluation import GoldenCrossResearchParameterEvaluator
from src.core.data_sources.csv_source import CsvDataSource
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.strategies.golden_cross import GoldenCrossStrategy


@pytest.mark.parametrize(
    ("accounting", "expected_balance", "expected_maker", "expected_taker"),
    [
        pytest.param(
            {
                "initial_balance": "10000.123456789012345678901234",
                "maker_fee": "0.000001234567890123456789012345",
                "taker_fee": "0.0001234567890123456789012345",
            },
            Decimal("10000.123456789012345678901234"),
            Decimal("0.000001234567890123456789012345"),
            Decimal("0.0001234567890123456789012345"),
            id="high-precision-accounting",
        ),
        pytest.param(
            {},
            Decimal("10000"),
            Decimal("0"),
            Decimal("0"),
            id="existing-accounting-defaults",
        ),
    ],
)
def test_research_evaluator_matches_direct_decimal_runner(
    tmp_path,
    monkeypatch,
    accounting,
    expected_balance,
    expected_maker,
    expected_taker,
):
    candle_path = tmp_path / "research-candles.csv"
    rows = _write_research_candles(candle_path)
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_type": "golden_cross",
            "strategy_id": "precision-golden-cross",
            "product_id": PRODUCT_ID,
            "timeframe": TIMEFRAME,
            "start_time": rows[0][0],
            "end_time": rows[-1][0],
            "backtest": {"candles_csv_path": str(candle_path), **accounting},
            "candidates": [
                {
                    "candidate_id": "candidate",
                    "param_pack": {
                        "short_window": 1,
                        "long_window": 3,
                        "quantity": "0.01",
                    },
                }
            ],
        }
    )
    assert request.backtest is not None
    assert request.candidates is not None
    candidate = request.candidates[0]
    assert request.backtest.initial_balance == expected_balance
    assert request.backtest.maker_fee == expected_maker
    assert request.backtest.taker_fee == expected_taker

    real_runner = parameter_evaluation.ResearchBacktestRunner
    evaluator_results = []
    evaluator_inputs = []

    class CapturingRunner(real_runner):
        def run(self):
            evaluator_inputs.append((self.initial_balance, dict(self.fee_config)))
            result = super().run()
            evaluator_results.append(result)
            return result

    monkeypatch.setattr(parameter_evaluation, "ResearchBacktestRunner", CapturingRunner)
    evaluation = GoldenCrossResearchParameterEvaluator().evaluate(request, candidate)
    assert len(evaluator_results) == len(evaluator_inputs) == 1
    actual_runner_result = evaluator_results[0]
    actual_initial, actual_fees = evaluator_inputs[0]
    direct = ResearchBacktestRunner(
        start_time=request.start_time,
        end_time=request.end_time,
        product_id=request.product_id,
        timeframe=request.timeframe,
        initial_balance=request.backtest.initial_balance,
        fee_config={
            "maker": request.backtest.maker_fee,
            "taker": request.backtest.taker_fee,
        },
        data_source=CsvDataSource(
            file_path=str(candle_path),
            product_id=request.product_id,
            timeframe=request.timeframe,
        ),
    )
    direct.add_strategy(
        GoldenCrossStrategy(
            f"{request.strategy_id}_{candidate.candidate_id}",
            request.product_id,
            short_window=1,
            long_window=3,
            timeframe=request.timeframe,
            quantity=Decimal("0.01"),
        )
    )
    direct_result = direct.run()

    assert Decimal(str(actual_initial)) == expected_balance
    assert actual_fees == {"maker": expected_maker, "taker": expected_taker}
    assert evaluation.metrics["provenance"]["configuration_sha256"] == (
        direct_result["provenance"].configuration_sha256
    )
    assert evaluation.score_total == actual_runner_result["mark_to_market_pnl"]
    assert actual_runner_result["total_pnl"] == direct_result["total_pnl"]
    assert (
        actual_runner_result["mark_to_market_pnl"]
        == direct_result["mark_to_market_pnl"]
    )
    assert Decimal(evaluation.metrics["total_pnl"]) == actual_runner_result["total_pnl"]
    assert direct_result["raw_trade_count"] > 0
    evaluator_trade_fees = sum(
        (trade.fee for trade in actual_runner_result["raw_trades"]), Decimal("0")
    )
    direct_trade_fees = sum(
        (trade.fee for trade in direct_result["raw_trades"]), Decimal("0")
    )
    assert evaluator_trade_fees == direct_trade_fees
    assert evaluator_trade_fees > 0 if expected_taker > 0 else evaluator_trade_fees == 0
    evaluator_final_equity = (
        Decimal(str(actual_initial)) + actual_runner_result["total_pnl"]
    )
    direct_final_equity = request.backtest.initial_balance + direct_result["total_pnl"]
    assert evaluator_final_equity == direct_final_equity
