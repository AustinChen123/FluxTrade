from __future__ import annotations

from dataclasses import replace

import pytest
from sqlalchemy.orm import Session

from src.control_plane import backtest_jobs
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.full_backtest import (
    FullBacktestExecutionError,
    FullBacktestResolutionError,
)
from src.core.backtest_result_owner import (
    BacktestResultPersistenceError,
    BacktestResultPersistenceReceipt,
    BacktestResultRunIdentity,
)
from test_full_backtest_execution import _outcome, _request, _resolved


def _session_factory() -> Session:
    pytest.fail("private full-run tests stub persistence")


def test_private_full_run_persists_exact_identity_and_returns_receipt(monkeypatch):
    request = _request()
    resolved = _resolved()
    outcome = replace(_outcome(), initial_balance=request.initial_balance)
    events = []

    def loader():
        return {}

    def resolve(actual_request, *, strategy_loader, session_factory):
        events.append("resolve")
        assert actual_request == request
        assert strategy_loader is loader
        assert session_factory is _session_factory
        return resolved

    def run(actual_resolved, *, session_factory):
        events.append("run")
        assert actual_resolved is resolved
        assert session_factory is _session_factory
        return outcome

    class Owner:
        def __init__(self, session_factory):
            assert session_factory is _session_factory

        def persist(self, identity, actual_outcome):
            events.append("persist")
            assert actual_outcome is outcome
            events.append(identity)
            return BacktestResultPersistenceReceipt(identity.job_id, "c" * 64, "d" * 64)

    monkeypatch.setattr(backtest_jobs, "resolve_full_backtest", resolve)
    monkeypatch.setattr(backtest_jobs, "run_full_backtest", run)
    monkeypatch.setattr(backtest_jobs, "BacktestResultPersistenceOwner", Owner)
    executor = BacktestJobExecutor(
        db_session_factory=_session_factory,
        strategy_loader=loader,
        run_inline=True,
    )

    result = executor._run_full_strategy_request("job-1", request)

    assert events[:3] == ["resolve", "run", "persist"]
    assert events[3] == BacktestResultRunIdentity(
        job_id="job-1",
        strategy_id=request.strategy_id,
        artifact_version=request.artifact_version,
        catalog_sha256=resolved.catalog_sha256,
        dataset_id=resolved.dataset.id,
        dataset_checksum_sha256=resolved.dataset.checksum_sha256,
        product_id=resolved.dataset.product_id,
        timeframe=resolved.decision_timeframe,
        currency=request.instrument.quote,
        start=request.start,
        end=request.end,
        initial_balance=request.initial_balance,
        instrument=request.instrument.to_instrument_spec(),
        maker_fee=request.fees.maker,
        taker_fee=request.fees.taker,
        drawdown_limit=request.drawdown_limit,
        execution_timeframe=resolved.execution_timeframe,
    )
    assert result == {
        "job_id": "job-1",
        "input_digest": "c" * 64,
        "result_digest": "d" * 64,
    }


def test_private_full_run_requires_subject_and_database_dependencies():
    request = _request()
    with pytest.raises(FullBacktestResolutionError) as no_loader:
        BacktestJobExecutor(
            db_session_factory=_session_factory
        )._run_full_strategy_request("job-1", request)
    assert no_loader.value.code == "browser_result_subject_unavailable"

    with pytest.raises(FullBacktestResolutionError) as no_database:
        BacktestJobExecutor(strategy_loader=lambda: {})._run_full_strategy_request(
            "job-1", request
        )
    assert no_database.value.code == "browser_result_backend_unavailable"


@pytest.mark.parametrize(
    "failure",
    [
        FullBacktestResolutionError("browser_result_dataset_unavailable"),
        FullBacktestExecutionError(),
        BacktestResultPersistenceError("browser_result_persistence_failed"),
    ],
)
def test_private_full_run_propagates_typed_boundary_errors(monkeypatch, failure):
    request = _request()
    events = []
    resolved = _resolved()

    def loader():
        return {}

    def resolve(*_args, **_kwargs):
        events.append("resolve")
        if isinstance(failure, FullBacktestResolutionError):
            raise failure
        return resolved

    def run(*_args, **_kwargs):
        events.append("run")
        if isinstance(failure, FullBacktestExecutionError):
            raise failure
        return replace(_outcome(), initial_balance=request.initial_balance)

    class Owner:
        def __init__(self, _factory):
            pass

        def persist(self, identity, _outcome):
            if isinstance(failure, BacktestResultPersistenceError):
                raise failure
            return BacktestResultPersistenceReceipt(identity.job_id, "c" * 64, "d" * 64)

    monkeypatch.setattr(backtest_jobs, "resolve_full_backtest", resolve)
    monkeypatch.setattr(backtest_jobs, "run_full_backtest", run)
    monkeypatch.setattr(backtest_jobs, "BacktestResultPersistenceOwner", Owner)
    executor = BacktestJobExecutor(
        db_session_factory=_session_factory,
        strategy_loader=loader,
        run_inline=True,
    )

    with pytest.raises(type(failure)) as captured:
        executor._run_full_strategy_request("job-1", request)
    assert captured.value is failure
