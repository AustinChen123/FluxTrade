from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.control_plane.models import (
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_search import (
    CsvSignalBacktestParameterEvaluator,
    GoldenCrossFastFitnessParameterEvaluator,
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchJobExecutor,
    ResearchBacktestParameterEvaluator,
)
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy, SystemEvent
import src.control_plane.parameter_search as parameter_search


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


class _RecordingRunner:
    instances = []

    def __init__(self, *args, capital_allocator=None, **kwargs):
        self.capital_allocator = capital_allocator
        self.kwargs = kwargs
        self.strategies = []
        _RecordingRunner.instances.append(self)

    def add_strategy(self, strategy):
        self.strategies.append(strategy)

    def run(self):
        return {
            "annualized_sharpe": Decimal("0"),
            "daily_return_moments": {
                "count": 0,
                "sum": Decimal("0"),
                "sum_squares": Decimal("0"),
                "sum_cubes": Decimal("0"),
                "sum_fourth": Decimal("0"),
            },
            "closed_trade_count": 0,
            "equity_sample_count": 1,
            "total_pnl": Decimal("1"),
            "mark_to_market_pnl": Decimal("1"),
            "max_drawdown": Decimal("0"),
            "raw_trades": [],
            "closed_trades": [],
            "raw_trade_count": 0,
            "yearly_mark_to_market_returns": {},
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


class _StaticDataSourceProvider:
    def __init__(self, source):
        self.source = source
        self.requests = []

    def create(self, request):
        self.requests.append(request)
        return self.source

    def cache_key(self, request):
        return "static", request.product_id, request.timeframe


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


def _sqlite_gene_registry_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'research_runner_gene_registry.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Strategy.__table__,
        SystemEvent.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Strategy(id="golden_cross", name="Golden Cross"))
        session.commit()
    return session_factory


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


def test_research_parameter_search_propagates_instrument_spec(tmp_path, monkeypatch):
    monkeypatch.setattr(parameter_search, "ResearchBacktestRunner", _RecordingRunner)
    _RecordingRunner.instances = []
    payload = _request_payload(tmp_path)
    payload["backtest"]["instrument"] = {
        "multiplier": "2",
        "fee_model": "per_contract",
        "capital_model": "per_contract",
        "capital_per_contract": "2500",
    }
    request = ParameterSearchJobRequest.model_validate(payload)

    ResearchBacktestParameterEvaluator(
        _strategy_factory,
        preload_candles=False,
    ).evaluate(request, ParameterCandidate(candidate_id="a", param_pack={}))

    spec = _RecordingRunner.instances[0].kwargs["instrument_spec"]
    assert spec.multiplier == Decimal("2")
    assert spec.fee_model.value == "per_contract"
    assert spec.capital_model.value == "per_contract"
    assert spec.capital_per_contract == Decimal("2500")


def test_research_evaluator_uses_injected_data_source_provider(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(parameter_search, "ResearchBacktestRunner", _RecordingRunner)
    _RecordingRunner.instances = []
    source = object()
    provider = _StaticDataSourceProvider(source)
    payload = _request_payload(tmp_path)
    payload["backtest"].pop("candles_csv_path")
    request = ParameterSearchJobRequest.model_validate(payload)

    ResearchBacktestParameterEvaluator(
        _strategy_factory,
        preload_candles=False,
        data_source_provider=provider,
    ).evaluate(request, ParameterCandidate(candidate_id="a", param_pack={}))

    assert _RecordingRunner.instances[0].kwargs["data_source"] is source
    assert provider.requests == [request]


def test_research_evaluator_scores_fold_endpoint_equity(tmp_path, monkeypatch):
    class _OpenPositionRunner(_RecordingRunner):
        def run(self):
            return {
                **super().run(),
                "total_pnl": Decimal("1"),
                "mark_to_market_pnl": Decimal("-7"),
            }

    monkeypatch.setattr(parameter_search, "ResearchBacktestRunner", _OpenPositionRunner)
    evaluator = ResearchBacktestParameterEvaluator(
        _strategy_factory,
        preload_candles=False,
    )
    request = ParameterSearchJobRequest.model_validate(_request_payload(tmp_path))

    result = evaluator.evaluate(
        request,
        ParameterCandidate(candidate_id="a", param_pack={}),
    )

    assert result.score_total == Decimal("-7")
    assert result.metrics["total_pnl"] == "1"
    assert result.metrics["mark_to_market_pnl"] == "-7"


def test_csv_signal_evaluator_uses_injected_source_and_endpoint_equity(
    tmp_path,
    monkeypatch,
):
    payload = _request_payload(tmp_path)
    payload.pop("research_runner")
    payload["backtest"].pop("candles_csv_path")
    request = ParameterSearchJobRequest.model_validate(payload)
    source = object()
    provider = _StaticDataSourceProvider(source)
    evaluator = CsvSignalBacktestParameterEvaluator(
        data_source_provider=provider,
    )
    captured = {}

    def run_backtest_request(backtest_request, **kwargs):
        captured["request"] = backtest_request
        captured.update(kwargs)
        return {
            "total_pnl": "1",
            "mark_to_market_pnl": "-7",
            "max_drawdown": "2",
        }

    monkeypatch.setattr(
        evaluator._backtest_executor,
        "run_backtest_request",
        run_backtest_request,
    )

    result = evaluator.evaluate(
        request,
        ParameterCandidate(
            candidate_id="a",
            param_pack={"signals_csv_path": str(tmp_path / "signals.csv")},
        ),
    )

    assert result.score_total == Decimal("-7")
    assert captured["data_source"] is source
    assert provider.requests == [request]


def test_default_csv_provider_rejects_missing_candle_path(tmp_path):
    payload = _request_payload(tmp_path)
    payload["backtest"].pop("candles_csv_path")
    request = ParameterSearchJobRequest.model_validate(payload)

    with pytest.raises(ValueError, match="CSV market data requires"):
        ResearchBacktestParameterEvaluator(
            _strategy_factory,
            preload_candles=False,
        ).evaluate(
            request,
            ParameterCandidate(candidate_id="a", param_pack={}),
        )


def test_research_parameter_search_isolates_capital_allocators_per_dataset(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(parameter_search, "ResearchBacktestRunner", _RecordingRunner)
    _RecordingRunner.instances = []
    evaluator = ResearchBacktestParameterEvaluator(
        _strategy_factory,
        preload_candles=False,
    )
    request = ParameterSearchJobRequest.model_validate(
        {
            **_request_payload(tmp_path),
            "evaluation_set": {
                "datasets": [
                    {
                        "dataset_id": "trend",
                        "product_id": "BINANCE:BTCUSDT-PERP",
                        "timeframe": "5m",
                        "start_time": 1_700_000_000_000,
                        "end_time": 1_700_000_060_000,
                    },
                    {
                        "dataset_id": "chop",
                        "product_id": "BINANCE:BTCUSDT-PERP",
                        "timeframe": "5m",
                        "start_time": 1_700_000_060_000,
                        "end_time": 1_700_000_120_000,
                    },
                ],
            },
        }
    )
    executor = ParameterSearchJobExecutor(
        evaluator,
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert len(_RecordingRunner.instances) == 2
    first_allocator = _RecordingRunner.instances[0].capital_allocator
    second_allocator = _RecordingRunner.instances[1].capital_allocator
    assert first_allocator is not second_allocator
    assert first_allocator.get_allocation("golden_cross_a") == Decimal("100")
    assert second_allocator.get_allocation("golden_cross_a") == Decimal("100")


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


def test_parameter_search_persists_research_runner_config_in_epoch(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    executor = ParameterSearchJobExecutor(
        _NoopEvaluator(),
        run_inline=True,
        db_session_factory=session_factory,
    )
    request = ParameterSearchJobRequest.model_validate(_request_payload(tmp_path))

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, job.result["epoch_id"])
    assert epoch is not None
    assert epoch.config_json["research_runner"] == {"capital_allocation": "100"}


def test_csv_signal_evaluator_rejects_research_runner_config(tmp_path):
    request = ParameterSearchJobRequest.model_validate(_request_payload(tmp_path))
    evaluator = CsvSignalBacktestParameterEvaluator()

    with pytest.raises(
        ValueError,
        match="research_runner settings require ResearchBacktestParameterEvaluator",
    ):
        evaluator.evaluate(
            request,
            ParameterCandidate(candidate_id="a", param_pack={}),
        )


def test_fast_fitness_evaluator_rejects_research_runner_config(tmp_path):
    request = ParameterSearchJobRequest.model_validate(_request_payload(tmp_path))
    evaluator = GoldenCrossFastFitnessParameterEvaluator()

    with pytest.raises(
        ValueError,
        match="research_runner settings require ResearchBacktestParameterEvaluator",
    ):
        evaluator.evaluate(
            request,
            ParameterCandidate(
                candidate_id="a",
                param_pack={"short_window": 1, "long_window": 2},
            ),
        )


def test_fast_fitness_propagates_instrument_spec(tmp_path):
    payload = _request_payload(tmp_path)
    payload.pop("research_runner")
    payload["backtest"]["instrument"] = {
        "multiplier": "2",
        "fee_model": "per_contract",
        "capital_model": "per_contract",
        "capital_per_contract": "2500",
    }
    candle_path = tmp_path / "candles.csv"
    candle_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "1700000000000,100,101,99,100,10\n"
        "1700000060000,101,102,100,101,10\n"
    )
    request = ParameterSearchJobRequest.model_validate(payload)

    evaluator = GoldenCrossFastFitnessParameterEvaluator()._evaluator_for(request)

    assert evaluator.contract_multiplier == 2.0
    assert evaluator.fee_model.value == "per_contract"


def test_fast_fitness_cache_separates_instrument_accounting(tmp_path):
    payload = _request_payload(tmp_path)
    payload.pop("research_runner")
    candle_path = tmp_path / "candles.csv"
    candle_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "1700000000000,100,101,99,100,10\n"
        "1700000060000,101,102,100,101,10\n"
    )
    evaluator = GoldenCrossFastFitnessParameterEvaluator()
    first = evaluator._evaluator_for(ParameterSearchJobRequest.model_validate(payload))
    payload["backtest"]["instrument"] = {
        "multiplier": "2",
        "fee_model": "per_contract",
        "capital_model": "per_contract",
        "capital_per_contract": "2500",
    }
    second = evaluator._evaluator_for(ParameterSearchJobRequest.model_validate(payload))

    assert first is not second
    assert first.contract_multiplier == 1.0
    assert second.contract_multiplier == 2.0


def test_generated_walk_forward_folds_run_through_research_evaluator(tmp_path):
    candle_path = tmp_path / "walk_forward.csv"
    start = 1_700_000_000_000
    rows = ["timestamp,open,high,low,close,volume"]
    for index in range(10):
        timestamp = start + index * 300_000
        price = 100 + (index % 4)
        rows.append(f"{timestamp},{price},{price + 1},{price - 1},{price},10")
    candle_path.write_text("\n".join(rows) + "\n")

    scoring_start = start + 600_000
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "5m",
            "start_time": scoring_start,
            "end_time": scoring_start + 2_400_000 - 1,
            "backtest": {
                "candles_csv_path": str(candle_path),
                "initial_balance": "1000",
                "maker_fee": "0",
                "taker_fee": "0",
            },
            "candidates": [
                {
                    "candidate_id": "baseline",
                    "param_pack": {
                        "short_window": 1,
                        "long_window": 2,
                        "quantity": "0.01",
                    },
                }
            ],
            "evaluation_set": {
                "walk_forward": {
                    "product_id": "BINANCE:BTCUSDT-PERP",
                    "timeframe": "5m",
                    "start_time": scoring_start,
                    "end_time": scoring_start + 2_400_000 - 1,
                    "fold_duration_ms": 1_200_000,
                    "warmup_duration_ms": 600_000,
                }
            },
            "fitness": {},
        }
    )
    executor = ParameterSearchJobExecutor(
        GoldenCrossResearchParameterEvaluator(),
        run_inline=True,
    )
    restored_request = ParameterSearchJobRequest.model_validate(
        request.model_dump(mode="json")
    )

    job = executor.submit_search(restored_request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert [
        dataset["dataset_id"]
        for dataset in job.result["evaluation_set"]["datasets"]
    ] == ["wf_0000", "wf_0001"]
    evaluation = job.result["evaluations"][0]
    assert evaluation["metrics"]["aggregation"] == "registered_walk_forward_fitness"
    assert evaluation["metrics"]["fitness"]["independent_trials"] == 1
