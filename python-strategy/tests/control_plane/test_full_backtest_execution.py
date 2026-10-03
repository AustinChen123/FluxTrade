from __future__ import annotations

from decimal import Decimal
import inspect
from pathlib import Path

import pytest
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session

from src.control_plane.full_backtest import (
    FullBacktestExecutionError,
    ResolvedFullBacktest,
    resolve_full_backtest,
    run_full_backtest,
)
from src.control_plane.full_backtest_request import FullStrategyBacktestRequest
from src.core.backtest_result_persistence import FullBacktestOutcome
from src.core.data_sources.research_database import ResearchDatasetMetadata
from src.core.models import Candlestick, Signal, SignalType
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    SignalAudit,
    Strategy,
)
from src.strategies.base import BaseStrategy, StrategyRequirements
from test_database_evaluation_data import stored_dataset as stored_dataset
from test_full_backtest_request import _payload


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kwargs):
    return "JSON"


def _outcome() -> FullBacktestOutcome:
    return FullBacktestOutcome(
        initial_balance=Decimal("1"),
        total_pnl=Decimal("0"),
        mark_to_market_pnl=Decimal("0"),
        max_drawdown=Decimal("0"),
        trade_sharpe=Decimal("0"),
        sortino_ratio=Decimal("0"),
        calmar_ratio=Decimal("0"),
        equity_samples=(),
        closed_trades=(),
    )


class _StubStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1h", 1)

    def on_candle(self, candle: Candlestick, context=None):
        return None


def _request() -> FullStrategyBacktestRequest:
    payload = _payload() | {
        "dataset_id": "execution-stub-dataset",
        "strategy_id": "catalog::ExecutionProbe",
        "start": 1000,
        "end": 2000,
        "drawdown_limit": "0.1234567890123456789",
        "execution_timeframe": "1m",
    }
    return FullStrategyBacktestRequest.model_validate(payload)


def _resolved() -> ResolvedFullBacktest:
    request = _request()
    dataset = ResearchDatasetMetadata(
        id=request.dataset_id,
        product_id=request.instrument.product_id,
        timeframe="1m",
        checksum_sha256="c" * 64,
        start_time=request.start,
        end_time=request.end,
    )
    return ResolvedFullBacktest(
        request=request,
        strategy=_StubStrategy(request.strategy_id, request.instrument.product_id),
        dataset=dataset,
        decision_timeframe="1h",
        execution_timeframe=request.execution_timeframe,
        source_range=(request.start, request.end),
        catalog_sha256="a" * 64,
    )


def test_runner_receives_exact_request_and_disabled_report_settings(monkeypatch):
    resolved = _resolved()
    outcome = _outcome()
    captured = {}

    class StubRunner:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            self.completed_outcome = outcome

        def add_strategy(self, strategy):
            captured["strategy"] = strategy

        def run(self):
            captured["ran"] = True

    from src.control_plane import full_backtest

    monkeypatch.setattr(full_backtest, "BacktestRunner", StubRunner)

    def factory() -> Session:
        pytest.fail("runner setup should not open a database session")

    assert run_full_backtest(resolved, session_factory=factory) is outcome
    request = resolved.request
    assert captured == {
        "start_time": request.start,
        "end_time": request.end,
        "product_id": request.instrument.product_id,
        "timeframe": resolved.decision_timeframe,
        "initial_balance": Decimal("10000.0000000000000000000001"),
        "max_drawdown_limit": Decimal("0.1234567890123456789"),
        "data_source": captured["data_source"],
        "fee_config": {"maker": Decimal("0.0002"), "taker": Decimal("0.0006")},
        "report_config": {
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        "db_session_factory": factory,
        "instrument_spec": request.instrument.to_instrument_spec(),
        "execution_timeframe": "1m",
        "capture_completed_outcome": True,
        "strategy": resolved.strategy,
        "ran": True,
    }
    assert captured["data_source"]._dataset_id == resolved.dataset.id
    assert captured["data_source"]._session_factory is factory


@pytest.mark.parametrize("failure", ["construct", "add", "run", "missing", "wrong"])
def test_runner_failures_are_fixed_and_detail_free(monkeypatch, failure):
    class StubRunner:
        def __init__(self, **kwargs):
            if failure == "construct":
                raise RuntimeError("private report path and database detail")
            self.completed_outcome = (
                None
                if failure == "missing"
                else object()
                if failure == "wrong"
                else _outcome()
            )

        def add_strategy(self, strategy):
            if failure == "add":
                raise RuntimeError("private constructor detail")

        def run(self):
            if failure == "run":
                raise RuntimeError("private runner traceback")

    from src.control_plane import full_backtest

    monkeypatch.setattr(full_backtest, "BacktestRunner", StubRunner)
    with pytest.raises(FullBacktestExecutionError) as captured:

        def unused_session_factory() -> Session:
            pytest.fail("stub failures should not open a database session")

        run_full_backtest(_resolved(), session_factory=unused_session_factory)
    assert captured.value.code == "browser_result_execution_failed"
    assert str(captured.value) == "browser_result_execution_failed"


class _NativeSignalStrategy(BaseStrategy):
    __fluxtrade_artifact_version__ = "v1.2.3"
    __fluxtrade_catalog_sha256__ = "d" * 64

    def __init__(self, strategy_id: str, product_id: str) -> None:
        super().__init__(strategy_id, product_id)
        self._index = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle: Candlestick, context=None):
        self._index += 1
        signal_type = {
            1: SignalType.LONG,
            4: SignalType.EXIT_LONG,
        }.get(self._index)
        if signal_type is None:
            return None
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="1m",
            timestamp=candle.timestamp,
            type=signal_type,
            quantity=Decimal("1"),
        )


def test_real_sealed_data_uses_native_runner_and_returns_fee_bearing_outcome(
    stored_dataset,  # noqa: F811
    monkeypatch,
):
    from src.core.backtest_runner import BacktestRunner

    assert (
        BacktestRunner.__module__ == "src.core.backtest_runner"
        and Path(inspect.getsourcefile(BacktestRunner) or "").resolve()
        == (Path(__file__).parents[2] / "src/core/backtest_runner.py").resolve()
    )
    factory, _ = stored_dataset
    engine = factory.kw["bind"]
    for table in (
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ):
        table.create(engine, checkfirst=True)

    from test_full_backtest_resolution import _request

    request = _request()
    resolved = resolve_full_backtest(
        request,
        strategy_loader=lambda: {request.strategy_id: _NativeSignalStrategy},
        session_factory=factory,
    )

    def fail_if_report_directory_created(*_args, **_kwargs):
        raise AssertionError("disabled reports must not touch the filesystem")

    monkeypatch.setattr(Path, "mkdir", fail_if_report_directory_created)
    outcome = run_full_backtest(resolved, session_factory=factory)

    assert type(outcome) is FullBacktestOutcome
    assert any(trade.fee > 0 for trade in outcome.closed_trades)
