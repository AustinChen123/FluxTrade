from __future__ import annotations

from dataclasses import replace

import pytest

from src.control_plane import backtest_jobs
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.full_backtest import (
    FullBacktestExecutionError,
    FullBacktestResolutionError,
    ResolutionErrorCode,
)
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.models import BacktestJobRequest, JobStatus
from src.core.backtest_result_owner import (
    BacktestResultPersistenceError,
    BacktestResultPersistenceReceipt,
    BacktestResultRunIdentity,
)
from test_full_backtest_execution import _outcome, _request, _resolved


def _session_factory():
    raise AssertionError("test boundary must stub database persistence")


def _wire_full_executor(monkeypatch, events, request, factory, store=None):
    resolved = _resolved()
    outcome = replace(_outcome(), initial_balance=request.initial_balance)

    def resolve(actual_request, *, strategy_loader, session_factory):
        events.append("resolve")
        assert actual_request == request
        assert strategy_loader is loader
        assert session_factory is factory
        return resolved

    def run(actual_resolved, *, session_factory):
        events.append("run")
        assert actual_resolved is resolved
        assert session_factory is factory
        return outcome

    class Owner:
        def __init__(self, actual_factory):
            assert actual_factory is factory

        def persist(self, identity, actual_outcome):
            if store is not None:
                running = store.get(identity.job_id)
                assert running is not None
                assert running.status is JobStatus.RUNNING
            events.append("persist")
            assert actual_outcome is outcome
            events.append(identity)
            return BacktestResultPersistenceReceipt(
                job_id=identity.job_id,
                input_digest="c" * 64,
                result_digest="d" * 64,
            )

    def loader():
        return {}

    monkeypatch.setattr(backtest_jobs, "resolve_full_backtest", resolve)
    monkeypatch.setattr(backtest_jobs, "run_full_backtest", run)
    monkeypatch.setattr(backtest_jobs, "BacktestResultPersistenceOwner", Owner)
    return resolved, loader


def test_full_executor_resolves_runs_persists_then_succeeds(monkeypatch):
    events = []
    request = _request()
    factory = _session_factory
    store = InMemoryJobStore()
    resolved, loader = _wire_full_executor(monkeypatch, events, request, factory, store)
    executor = BacktestJobExecutor(
        store=store,
        db_session_factory=factory,
        strategy_loader=loader,
        run_inline=True,
    )

    job = executor.submit_backtest(request)

    assert job.status is JobStatus.SUCCEEDED
    assert job.result == {
        "job_id": job.id,
        "input_digest": "c" * 64,
        "result_digest": "d" * 64,
    }
    assert events[:3] == ["resolve", "run", "persist"]
    identity = events[3]
    assert isinstance(identity, BacktestResultRunIdentity)
    assert identity == BacktestResultRunIdentity(
        job_id=job.id,
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
    stored = store.get(job.id)
    assert stored is not None
    assert stored.status is JobStatus.SUCCEEDED


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        ("subject", "browser_result_subject_unavailable"),
        ("dataset", "browser_result_dataset_unavailable"),
        ("backend", "browser_result_backend_unavailable"),
        ("execution", "browser_result_execution_failed"),
        ("unexpected", "browser_result_execution_failed"),
        ("persistence", "browser_result_persistence_failed"),
    ],
)
def test_full_executor_failures_are_fixed_and_never_succeed(
    monkeypatch, failure, expected
):
    request = _request()
    factory = _session_factory
    store = InMemoryJobStore()
    events = []
    resolved, loader = _wire_full_executor(monkeypatch, events, request, factory, store)

    def fail_resolve(*_args, **_kwargs):
        codes: dict[str, ResolutionErrorCode] = {
            "subject": "browser_result_subject_unavailable",
            "dataset": "browser_result_dataset_unavailable",
            "backend": "browser_result_backend_unavailable",
        }
        code = codes.get(failure)
        if code:
            raise FullBacktestResolutionError(code)
        return resolved

    def fail_run(*_args, **_kwargs):
        if failure == "execution":
            raise FullBacktestExecutionError()
        if failure == "unexpected":
            raise RuntimeError("private runner detail")
        return _outcome()

    class Owner:
        def __init__(self, _factory):
            pass

        def persist(self, identity, _outcome):
            if failure == "persistence":
                raise BacktestResultPersistenceError("private database detail")
            return BacktestResultPersistenceReceipt(identity.job_id, "c" * 64, "d" * 64)

    monkeypatch.setattr(backtest_jobs, "resolve_full_backtest", fail_resolve)
    monkeypatch.setattr(backtest_jobs, "run_full_backtest", fail_run)
    monkeypatch.setattr(backtest_jobs, "BacktestResultPersistenceOwner", Owner)
    executor = BacktestJobExecutor(
        store=store,
        db_session_factory=factory,
        strategy_loader=loader,
        run_inline=True,
    )

    job = executor.submit_backtest(request)

    assert job.status is JobStatus.FAILED
    assert job.error == expected
    assert job.result is None


def test_full_executor_missing_dependencies_fail_closed():
    request = _request()
    no_loader = BacktestJobExecutor(
        db_session_factory=_session_factory, run_inline=True
    ).submit_backtest(request)
    no_backend = BacktestJobExecutor(
        strategy_loader=lambda: {}, run_inline=True
    ).submit_backtest(request)
    assert no_loader.status is JobStatus.FAILED
    assert no_loader.error == "browser_result_subject_unavailable"
    assert no_backend.status is JobStatus.FAILED
    assert no_backend.error == "browser_result_backend_unavailable"


def test_full_retry_reparses_decimal_request_and_uses_a_new_job_identity(monkeypatch):
    request = _request()
    store = InMemoryJobStore()
    original = store.create(kind=request.kind, request=request)
    store.mark_failed(original.id, "earlier execution failed")
    events = []
    factory = _session_factory
    resolved, loader = _wire_full_executor(monkeypatch, events, request, factory, store)
    executor = BacktestJobExecutor(
        store=store,
        db_session_factory=factory,
        strategy_loader=loader,
        run_inline=True,
    )

    retried = executor.retry_backtest(original.id)

    identity = events[3]
    assert retried.id != original.id
    assert retried.status is JobStatus.SUCCEEDED
    assert retried.request["initial_balance"] == str(request.initial_balance)
    assert retried.request["fees"] == request.fees.model_dump(mode="json")
    assert isinstance(identity, BacktestResultRunIdentity)
    assert identity.job_id == retried.id
    assert identity.initial_balance == request.initial_balance
    assert events.count("resolve") == 1


def test_csv_executor_path_never_invokes_full_result_owner(monkeypatch):
    request = BacktestJobRequest(
        strategy_id="csv",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        candles_csv_path="/tmp/candles.csv",
        signals_csv_path="/tmp/signals.csv",
        start_time=1,
        end_time=2,
    )
    executor = BacktestJobExecutor(run_inline=True)
    monkeypatch.setattr(
        executor, "run_backtest_request", lambda _request: {"legacy": True}
    )

    def unexpected_owner(*_args, **_kwargs):
        raise AssertionError("CSV jobs do not use full-result persistence")

    monkeypatch.setattr(
        backtest_jobs, "BacktestResultPersistenceOwner", unexpected_owner
    )
    job = executor.submit_backtest(request)
    assert job.status is JobStatus.SUCCEEDED
    assert job.result == {"legacy": True}
