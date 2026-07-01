from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.control_plane.models import (
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_search import (
    ParameterSearchJobExecutor,
    ResearchBacktestParameterEvaluator,
)
import src.control_plane.parameter_search as parameter_search


class _RecordingRunner:
    instances = []

    def __init__(self, *args, capital_allocator=None, **kwargs):
        self.capital_allocator = capital_allocator
        self.strategies = []
        _RecordingRunner.instances.append(self)

    def add_strategy(self, strategy):
        self.strategies.append(strategy)

    def run(self):
        return {
            "total_pnl": Decimal("1"),
            "max_drawdown": Decimal("0"),
            "raw_trades": [],
            "closed_trades": [],
            "raw_trade_count": 0,
            "candle_count": 0,
        }


class _NoopEvaluator:
    def evaluate(self, request, candidate):
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal("1"),
            max_drawdown=Decimal("0"),
            metrics={},
        )


def _strategy_factory(strategy_id, product_id, timeframe, param_pack):
    return SimpleNamespace(
        strategy_id=strategy_id,
        product_id=product_id,
        timeframe=timeframe,
        param_pack=param_pack,
    )


def _request_payload(tmp_path):
    return {
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "5m",
        "start_time": 1_700_000_000_000,
        "end_time": 1_700_000_060_000,
        "backtest": {
            "candles_csv_path": str(tmp_path / "candles.csv"),
            "initial_balance": "1000",
            "maker_fee": "0",
            "taker_fee": "0",
        },
        "research_runner": {
            "capital_allocation": "100",
        },
        "candidates": [
            {"candidate_id": "a", "param_pack": {"score": 1}},
        ],
    }


def test_research_parameter_search_creates_isolated_capital_allocators(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(parameter_search, "ResearchBacktestRunner", _RecordingRunner)
    _RecordingRunner.instances = []
    evaluator = ResearchBacktestParameterEvaluator(
        _strategy_factory,
        preload_candles=False,
    )
    request = ParameterSearchJobRequest.model_validate(_request_payload(tmp_path))

    evaluator.evaluate(
        request,
        ParameterCandidate(candidate_id="a", param_pack={}),
    )
    evaluator.evaluate(
        request,
        ParameterCandidate(candidate_id="b", param_pack={}),
    )

    assert len(_RecordingRunner.instances) == 2
    first_allocator = _RecordingRunner.instances[0].capital_allocator
    second_allocator = _RecordingRunner.instances[1].capital_allocator
    assert first_allocator is not second_allocator
    assert first_allocator.get_allocation("golden_cross_a") == Decimal("100")
    assert first_allocator.get_allocation("golden_cross_b") == Decimal("0")
    assert second_allocator.get_allocation("golden_cross_a") == Decimal("0")
    assert second_allocator.get_allocation("golden_cross_b") == Decimal("100")


def test_research_runner_capital_allocation_rejects_non_positive_values(tmp_path):
    payload = _request_payload(tmp_path)
    payload["research_runner"]["capital_allocation"] = "0"

    with pytest.raises(ValidationError, match="capital_allocation must be positive"):
        ParameterSearchJobRequest.model_validate(payload)


def test_research_evaluator_rejects_allocation_above_initial_balance(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(parameter_search, "ResearchBacktestRunner", _RecordingRunner)
    evaluator = ResearchBacktestParameterEvaluator(
        _strategy_factory,
        preload_candles=False,
    )
    payload = _request_payload(tmp_path)
    payload["research_runner"]["capital_allocation"] = "1001"
    request = ParameterSearchJobRequest.model_validate(payload)

    with pytest.raises(ValueError, match="cannot exceed backtest.initial_balance"):
        evaluator.evaluate(
            request,
            ParameterCandidate(candidate_id="a", param_pack={}),
        )


def test_parameter_search_result_includes_research_runner_config(tmp_path):
    executor = ParameterSearchJobExecutor(
        _NoopEvaluator(),
        run_inline=True,
    )
    request = ParameterSearchJobRequest.model_validate(_request_payload(tmp_path))

    job = executor.submit_search(request)

    assert job.result["research_runner"] == {"capital_allocation": "100"}
