"""Parameter evaluation contracts, implementations, and result normalization."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Callable, Protocol, runtime_checkable

from src.control_plane.backtest_jobs import (
    BacktestJobExecutor,
    BacktestRunSpec,
    SessionFactory,
    _json_safe,
)
from src.control_plane.evaluation_data import (
    CsvEvaluationDataSourceProvider,
    EvaluationDataSourceProvider,
)
from src.control_plane.models import (
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.core.capital_allocator import CapitalAllocator
from src.core.data_sources.memory import MemoryDataSource
from src.core.golden_cross_fast_fitness import GoldenCrossFastFitnessEvaluator
from src.core.precision import PrecisionCodec
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy
from src.strategies.golden_cross import GoldenCrossStrategy


class ParameterSearchEvaluator(Protocol):
    """Evaluation boundary for candidate parameter packs."""

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult: ...


@runtime_checkable
class ParameterSearchRequestValidator(Protocol):
    """Optional evaluator capability for rejecting unsupported jobs pre-submit."""

    def validate_request(self, request: ParameterSearchJobRequest) -> None: ...


class UnsupportedParameterSearchError(ValueError):
    """Raised before job creation when no evaluator owns the strategy type."""


@runtime_checkable
class WalkForwardWarmupEvaluator(Protocol):
    """Optional evaluator capability for state-only pre-fold replay."""

    def evaluate_with_warmup(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
        *,
        warmup_start_time: int,
    ) -> ParameterEvaluationResult: ...


class ParameterSearchEvaluatorRegistry:
    """Dispatch parameter jobs to an explicitly registered strategy evaluator."""

    def __init__(
        self,
        evaluators: Mapping[str, ParameterSearchEvaluator],
    ) -> None:
        if not evaluators:
            raise ValueError("at least one parameter-search evaluator is required")
        self._evaluators = dict(evaluators)

    def validate_request(self, request: ParameterSearchJobRequest) -> None:
        evaluator = self._resolve(request)
        if isinstance(evaluator, ParameterSearchRequestValidator):
            evaluator.validate_request(request)
        if request.evaluation_set is None:
            return
        requires_warmup = any(
            dataset.warmup_start_time is not None
            for dataset in request.evaluation_set.resolved_datasets
        )
        if requires_warmup and not isinstance(
            evaluator,
            WalkForwardWarmupEvaluator,
        ):
            raise UnsupportedParameterSearchError(
                f"strategy_type does not support walk-forward warmup: "
                f"{request.strategy_type}"
            )

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        return self._resolve(request).evaluate(request, candidate)

    def evaluate_with_warmup(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
        *,
        warmup_start_time: int,
    ) -> ParameterEvaluationResult:
        evaluator = self._resolve(request)
        if not isinstance(evaluator, WalkForwardWarmupEvaluator):
            raise ValueError(
                f"strategy_type does not support walk-forward warmup: "
                f"{request.strategy_type}"
            )
        return evaluator.evaluate_with_warmup(
            request,
            candidate,
            warmup_start_time=warmup_start_time,
        )

    def _resolve(
        self,
        request: ParameterSearchJobRequest,
    ) -> ParameterSearchEvaluator:
        strategy_type = request.strategy_type
        if strategy_type is None:
            raise UnsupportedParameterSearchError(
                "strategy_type is required for parameter search"
            )
        evaluator = self._evaluators.get(strategy_type)
        if evaluator is None:
            raise UnsupportedParameterSearchError(
                f"unsupported strategy_type: {strategy_type}"
            )
        return evaluator


class CsvSignalBacktestParameterEvaluator:
    """Evaluate candidates by running CSV-signal backtests.

    Each candidate must include ``signals_csv_path`` in ``param_pack``. The shared
    candle CSV and fees live in ``ParameterSearchJobRequest.backtest``.
    """

    def __init__(
        self,
        db_session_factory: SessionFactory | None = None,
        *,
        data_source_provider: EvaluationDataSourceProvider | None = None,
    ) -> None:
        self._backtest_executor = BacktestJobExecutor(
            db_session_factory=db_session_factory,
            run_inline=True,
        )
        self._data_source_provider = (
            data_source_provider
            if data_source_provider is not None
            else CsvEvaluationDataSourceProvider()
        )

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        _reject_research_runner_settings(request, "CSV-signal evaluation")
        if request.backtest is None:
            raise ValueError("backtest settings are required for CSV-signal evaluation")

        signals_csv_path = candidate.param_pack.get("signals_csv_path")
        if not isinstance(signals_csv_path, str) or not signals_csv_path.strip():
            raise ValueError("candidate param_pack.signals_csv_path is required")

        backtest_request = BacktestRunSpec(
            strategy_id=f"{request.strategy_id}_{candidate.candidate_id}",
            product_id=request.product_id,
            timeframe=request.timeframe,
            candles_csv_path=request.backtest.candles_csv_path,
            signals_csv_path=signals_csv_path,
            start_time=request.start_time,
            end_time=request.end_time,
            initial_balance=request.backtest.initial_balance,
            maker_fee=request.backtest.maker_fee,
            taker_fee=request.backtest.taker_fee,
            instrument=request.backtest.instrument,
            write_reports=request.backtest.write_reports,
        )
        result = self._backtest_executor.run_backtest_request(
            backtest_request,
            max_drawdown_limit=None if request.evolution is not None else 0.20,
            data_source=self._data_source_provider.create(request),
        )
        score = _result_decimal(result, "mark_to_market_pnl")
        max_drawdown = _result_decimal(result, "max_drawdown")
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=_drawdown_loss_magnitude(max_drawdown),
            metrics=_normalize_metrics_drawdown(result),
        )


ResearchStrategyFactory = Callable[[str, str, str, dict[str, Any]], BaseStrategy]


class ResearchBacktestParameterEvaluator:
    """Evaluate candidates with the in-memory research backtest runner."""

    def __init__(
        self,
        strategy_factory: ResearchStrategyFactory,
        *,
        preload_candles: bool = True,
        precision_codec: PrecisionCodec | None = None,
        data_source_provider: EvaluationDataSourceProvider | None = None,
    ) -> None:
        self._strategy_factory = strategy_factory
        self._preload_candles = preload_candles
        self._precision_codec = precision_codec
        self._data_source_provider = (
            data_source_provider
            if data_source_provider is not None
            else CsvEvaluationDataSourceProvider()
        )
        self._candle_cache: dict[tuple, list] = {}
        self._prepared_scaled_cache: dict[tuple, list] = {}

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        return self._evaluate(request, candidate, warmup_start_time=None)

    def evaluate_with_warmup(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
        *,
        warmup_start_time: int,
    ) -> ParameterEvaluationResult:
        return self._evaluate(
            request,
            candidate,
            warmup_start_time=warmup_start_time,
        )

    def _evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
        *,
        warmup_start_time: int | None,
    ) -> ParameterEvaluationResult:
        if request.backtest is None:
            raise ValueError("backtest settings are required for research evaluation")

        data_source, prepared_scaled_candles = self._replay_inputs_for(request)
        strategy = self._strategy_factory(
            f"{request.strategy_id}_{candidate.candidate_id}",
            request.product_id,
            request.timeframe,
            candidate.param_pack,
        )
        if warmup_start_time is not None:
            self._warm_up_strategy(
                request,
                strategy,
                warmup_start_time=warmup_start_time,
            )
        capital_allocator = _capital_allocator_for(request, strategy.strategy_id)
        runner = ResearchBacktestRunner(
            start_time=request.start_time,
            end_time=request.end_time,
            product_id=request.product_id,
            timeframe=request.timeframe,
            initial_balance=float(request.backtest.initial_balance),
            data_source=data_source,
            fee_config={
                "maker": float(request.backtest.maker_fee),
                "taker": float(request.backtest.taker_fee),
            },
            precision_codec=self._precision_codec,
            prepared_scaled_candles=prepared_scaled_candles,
            capital_allocator=capital_allocator,
            instrument_spec=(
                request.backtest.instrument.to_instrument_spec(request.product_id)
                if request.backtest.instrument is not None
                else None
            ),
        )
        runner.add_strategy(strategy)

        result = runner.run()
        invalid_intent_count = int(result.get("invalid_order_intent_count", 0))
        if invalid_intent_count:
            raise ValueError(
                "research_backtest_invalid_order_intent: "
                f"count={invalid_intent_count}"
            )
        metrics = {
            key: value
            for key, value in result.items()
            if key
            not in {
                "closed_trades",
                "raw_trades",
                "invalid_order_intent_rejections",
            }
        }
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=_result_decimal(result, "mark_to_market_pnl"),
            max_drawdown=_drawdown_loss_magnitude(
                _result_decimal(result, "max_drawdown")
            ),
            metrics=_normalize_metrics_drawdown(metrics),
        )

    def _warm_up_strategy(
        self,
        request: ParameterSearchJobRequest,
        strategy: BaseStrategy,
        *,
        warmup_start_time: int,
    ) -> None:
        assert request.backtest is not None
        source = self._data_source_provider.create(request)
        candles = list(
            source.get_candles(
                request.product_id,
                request.timeframe,
                warmup_start_time,
                request.start_time - 1,
            )
        )
        required = strategy.requirements.lookback_window
        if len(candles) < required:
            raise ValueError(
                "walk-forward warmup has insufficient candles: "
                f"required={required} actual={len(candles)}"
            )
        SignalProcessor(StrategyRegistry(), execution_engine=None).warm_up(
            strategy,
            candles,
            require_complete_trade_state=True,
        )

    def _replay_inputs_for(self, request: ParameterSearchJobRequest):
        assert request.backtest is not None
        data_source = self._data_source_provider.create(request)
        if not self._preload_candles:
            return data_source, None

        cache_key = _candle_cache_key(request, self._data_source_provider)
        candles = self._candle_cache.get(cache_key)
        if candles is None:
            candles = list(
                data_source.get_candles(
                    request.product_id,
                    request.timeframe,
                    request.start_time,
                    request.end_time,
                )
            )
            self._candle_cache[cache_key] = candles

        prepared_scaled_candles = None
        if self._precision_codec is not None:
            prepared_scaled_candles = self._prepared_scaled_cache.get(cache_key)
            if prepared_scaled_candles is None:
                prepared_scaled_candles = ResearchBacktestRunner.prepare_scaled_candles(
                    candles,
                    self._precision_codec,
                )
                self._prepared_scaled_cache[cache_key] = prepared_scaled_candles
        return MemoryDataSource(candles), prepared_scaled_candles


class GoldenCrossResearchParameterEvaluator(ResearchBacktestParameterEvaluator):
    """Research evaluator for GoldenCrossStrategy parameter packs."""

    def __init__(
        self,
        precision_codec: PrecisionCodec | None = None,
        *,
        data_source_provider: EvaluationDataSourceProvider | None = None,
    ) -> None:
        super().__init__(
            _golden_cross_strategy_factory,
            precision_codec=precision_codec,
            data_source_provider=data_source_provider,
        )


class GoldenCrossFastFitnessParameterEvaluator:
    """Evaluate GoldenCross candidates through the numeric fitness path."""

    def __init__(
        self,
        *,
        data_source_provider: EvaluationDataSourceProvider | None = None,
    ) -> None:
        self._data_source_provider = (
            data_source_provider
            if data_source_provider is not None
            else CsvEvaluationDataSourceProvider()
        )
        self._fitness_cache: dict[tuple, GoldenCrossFastFitnessEvaluator] = {}

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        _reject_research_runner_settings(request, "fast fitness evaluation")
        if request.backtest is None:
            raise ValueError("backtest settings are required for fast fitness evaluation")

        evaluator = self._evaluator_for(request)
        result = evaluator.evaluate(
            short_window=int(candidate.param_pack["short_window"]),
            long_window=int(candidate.param_pack["long_window"]),
            quantity=Decimal(str(candidate.param_pack.get("quantity", "0.01"))),
        )
        metrics = {
            "total_pnl": result.total_pnl,
            "max_drawdown": result.max_drawdown,
            "total_trades": result.total_trades,
            "raw_trade_count": result.raw_trade_count,
            "win_rate": result.win_rate,
            "profit_factor": result.profit_factor,
            "gross_profit": result.gross_profit,
            "gross_loss": result.gross_loss,
            "fitness_mode": "golden_cross_fast",
        }
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=result.total_pnl,
            max_drawdown=_drawdown_loss_magnitude(result.max_drawdown),
            metrics=_normalize_metrics_drawdown(metrics),
        )

    def _evaluator_for(
        self,
        request: ParameterSearchJobRequest,
    ) -> GoldenCrossFastFitnessEvaluator:
        assert request.backtest is not None
        cache_key = _fitness_cache_key(request, self._data_source_provider)
        evaluator = self._fitness_cache.get(cache_key)
        if evaluator is None:
            data_source = self._data_source_provider.create(request)
            df = data_source.get_candles_df(
                request.product_id,
                request.timeframe,
                request.start_time,
                request.end_time,
            )
            evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(
                df,
                initial_balance=request.backtest.initial_balance,
                taker_fee=request.backtest.taker_fee,
                instrument_spec=(
                    request.backtest.instrument.to_instrument_spec(request.product_id)
                    if request.backtest.instrument is not None
                    else None
                ),
            )
            self._fitness_cache[cache_key] = evaluator
        return evaluator


def _golden_cross_strategy_factory(
    strategy_id: str,
    product_id: str,
    timeframe: str,
    param_pack: dict[str, Any],
) -> BaseStrategy:
    try:
        short_window = int(param_pack["short_window"])
        long_window = int(param_pack["long_window"])
    except KeyError as exc:
        raise ValueError("candidate param_pack requires short_window and long_window") from exc

    return GoldenCrossStrategy(
        strategy_id,
        product_id,
        short_window=short_window,
        long_window=long_window,
        timeframe=timeframe,
        quantity=Decimal(str(param_pack.get("quantity", "0.01"))),
    )


def _candle_cache_key(
    request: ParameterSearchJobRequest,
    data_source_provider: EvaluationDataSourceProvider,
) -> tuple:
    return (
        data_source_provider.cache_key(request),
        request.product_id,
        request.timeframe,
        request.start_time,
        request.end_time,
    )


def _fitness_cache_key(
    request: ParameterSearchJobRequest,
    data_source_provider: EvaluationDataSourceProvider,
) -> tuple:
    assert request.backtest is not None
    instrument = request.backtest.instrument
    return (
        _candle_cache_key(request, data_source_provider),
        request.backtest.initial_balance,
        request.backtest.taker_fee,
        instrument.multiplier if instrument is not None else None,
        instrument.fee_model if instrument is not None else None,
        instrument.capital_model if instrument is not None else None,
        instrument.capital_per_contract if instrument is not None else None,
    )


def _capital_allocator_for(
    request: ParameterSearchJobRequest,
    strategy_id: str,
) -> CapitalAllocator | None:
    if request.research_runner is None:
        return None
    capital_allocation = request.research_runner.capital_allocation
    if capital_allocation is None:
        return None
    if request.backtest is None:
        raise ValueError("backtest settings are required for capital allocation")
    initial_balance = request.backtest.initial_balance
    if capital_allocation > initial_balance:
        raise ValueError(
            "research_runner.capital_allocation cannot exceed backtest.initial_balance"
        )

    allocator = CapitalAllocator(total_balance=initial_balance)
    allocator.allocate(strategy_id, capital_allocation)
    return allocator


def _reject_research_runner_settings(
    request: ParameterSearchJobRequest,
    evaluator_name: str,
) -> None:
    if request.research_runner is None:
        return
    raise ValueError(
        "research_runner settings require ResearchBacktestParameterEvaluator; "
        f"{evaluator_name} does not support them"
    )


def _normalize_evaluation_result(
    evaluation: ParameterEvaluationResult,
) -> ParameterEvaluationResult:
    max_drawdown = _drawdown_loss_magnitude(evaluation.max_drawdown)
    metrics = _normalize_metrics_drawdown(evaluation.metrics)
    if max_drawdown == evaluation.max_drawdown and metrics == evaluation.metrics:
        return evaluation
    return evaluation.model_copy(
        update={
            "max_drawdown": max_drawdown,
            "metrics": metrics,
        }
    )


def _normalize_metrics_drawdown(metrics: dict[str, Any]) -> dict[str, Any]:
    if "max_drawdown" not in metrics:
        return _json_safe(metrics)

    normalized = dict(metrics)
    normalized["max_drawdown"] = _drawdown_loss_magnitude(
        Decimal(str(normalized["max_drawdown"]))
    )
    return _json_safe(normalized)


def _drawdown_loss_magnitude(drawdown: Decimal) -> Decimal:
    return abs(drawdown)


def _result_decimal(result: dict[str, Any], key: str) -> Decimal:
    value = result.get(key, "0")
    return Decimal(str(value))
