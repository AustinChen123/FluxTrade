from __future__ import annotations

import json
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier
from unittest.mock import MagicMock
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.app import ControlPlaneApp, HttpResponse
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.ga_profile import get_golden_cross_profile
from src.control_plane.ga_commands import (
    GaCommandBackendError,
    GaCommandService,
    GaCommandValidationError,
)
from src.control_plane.ga_http_contract import (
    GaCommandRequest,
    parse_ga_command_request,
)
from src.control_plane.ga_lifecycle import GaJobRecord, GaJobStatus, GaJobStoreError
from src.control_plane.jobs import InMemoryJobStore, SqliteJobStore
from src.control_plane.parameter_evaluation import ParameterSearchEvaluatorRegistry
from src.control_plane.main import build_control_plane_app
from src.control_plane.invalidation import ControlPlaneInvalidationHub
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.control_plane.models import (
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)

ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR = "example.com/cap/fluxtrade-operator"


def _app() -> ControlPlaneApp:
    app = ControlPlaneApp(
        BacktestJobExecutor(store=InMemoryJobStore(), run_inline=True),
        api_key="operator-key",
        browser_auth=BrowserSessionAuth(
            allowed_origin=ORIGIN,
            operator_capability=OPERATOR,
            step_up_capability="example.com/cap/step-up",
        ),
    )
    return app


def _auth_headers(app: ControlPlaneApp) -> dict[str, str]:
    identity = {
        "Origin": ORIGIN,
        "Tailscale-User-Login": "operator@example.com",
        "Tailscale-App-Capabilities": json.dumps({OPERATOR: [{}]}),
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    return {
        **identity,
        "Cookie": dict(issued.headers)["Set-Cookie"].split(";", 1)[0],
        "X-CSRF-Token": issued.body["csrf_token"],
    }


def _valid_profile_input() -> dict[str, object]:
    profile = get_golden_cross_profile()
    return {
        "parameter_search_profile_id": profile["parameter_search_profile_id"],
        "strategy_subject": profile["strategy_subject"],
        "fitness_profile_id": profile["fitness_profile_id"],
        "cost_profile_id": profile["cost_profile_id"],
        "profile_revision": profile["profile_revision"],
        "strategy_version": profile["strategy_version"],
        "dataset_id": "sealed-dataset",
        "start_time": 1_704_067_200_000,
        "end_time": 1_704_067_800_000,
        "initial_balance": "10000.123456789012345678901234",
        "fees": {"maker": "0.000001", "taker": "0.000123"},
        "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        "parameters": {
            "short_window": {"min": 1, "max": 2, "step": 1},
            "long_window": {"min": 3, "max": 4, "step": 1},
            "quantity": "0.01",
        },
        "population_size": 2,
        "max_generations": 3,
        "seed": 17,
    }


def test_default_in_memory_app_rejects_ga_command_without_durable_service():
    app = _app()
    response = app.handle(
        "POST",
        "/api/v1/ga-jobs",
        body=json.dumps(_valid_profile_input()),
        headers={**_auth_headers(app), "Idempotency-Key": "submit-1"},
    )

    assert (response.status_code, response.body) == (
        503,
        {"error": "ga_controls_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_http_submit_receipt_binds_original_intent_and_returns_snapshot(
    store_kind, tmp_path
):
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SqliteJobStore(tmp_path / "http-receipts.sqlite3")
    )
    request = {
        "kind": "parameter_search",
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "start_time": 1_704_067_200_000,
        "end_time": 1_704_067_800_000,
        "market_data": {"kind": "sealed_dataset", "dataset_id": "sealed-a"},
        "objective": "maximize_score",
        "seed": 17,
        "backtest": {
            "initial_balance": "10000.123456789012345678901234",
            "maker_fee": "0.000001",
            "taker_fee": "0.000123",
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        },
        "search_space": {"parameters": {"quantity": {"min": "0.01"}}},
        "evolution": {"epoch_id": None, "max_generations": 2, "population_size": 2},
        "ga_binding": {
            "parameter_search_profile_id": "golden_cross_research_v1",
            "profile_revision": "a" * 64,
            "strategy_subject": "builtin:golden_cross",
            "strategy_version": "b" * 64,
            "dataset_id": "sealed-a",
            "dataset_checksum": "c" * 64,
            "fitness_profile_id": "mark_to_market_pnl_v1",
            "cost_profile_id": "explicit_accounting_v1",
            "input_digest": "d" * 64,
        },
    }
    wire_intent = _valid_profile_input()
    first, replayed = store.submit_ga_job_command(
        actor="operator@example.com",
        idempotency_key="receipt-key",
        request=request,
        http_wire_intent=wire_intent,
    )
    assert not replayed
    original = first.detached_copy()
    store.transition_ga_job(first.id, "claim", first.version)

    reordered = dict(reversed(tuple(wire_intent.items())))
    receipt = store.get_ga_command_receipt(
        actor="operator@example.com",
        idempotency_key="receipt-key",
        operation="submit",
        http_wire_intent=reordered,
    )
    assert receipt == original
    assert receipt is not None
    receipt.request["ga_binding"]["input_digest"] = "mutated"
    assert (
        store.get_ga_command_receipt(
            actor="operator@example.com",
            idempotency_key="receipt-key",
            operation="submit",
            http_wire_intent=wire_intent,
        )
        == original
    )

    changed = deepcopy(wire_intent)
    changed["seed"] = 18
    with pytest.raises(GaJobStoreError) as error:
        store.get_ga_command_receipt(
            actor="operator@example.com",
            idempotency_key="receipt-key",
            operation="submit",
            http_wire_intent=changed,
        )
    assert error.value.code == "idempotency_conflict"
    assert len(store.list_ga_jobs()) == 1


def test_command_replay_precedes_preflight_and_never_dispatches():
    store = InMemoryJobStore()
    request = {
        "kind": "parameter_search",
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "start_time": 1_704_067_200_000,
        "end_time": 1_704_067_800_000,
        "market_data": {"kind": "sealed_dataset", "dataset_id": "sealed-a"},
        "objective": "maximize_score",
        "seed": 17,
        "backtest": {
            "initial_balance": "10000",
            "maker_fee": "0",
            "taker_fee": "0",
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        },
        "search_space": {"parameters": {"quantity": {"min": "0.01"}}},
        "evolution": {"epoch_id": None, "max_generations": 2, "population_size": 2},
        "ga_binding": {
            "parameter_search_profile_id": "golden_cross_research_v1",
            "profile_revision": "a" * 64,
            "strategy_subject": "builtin:golden_cross",
            "strategy_version": "b" * 64,
            "dataset_id": "sealed-a",
            "dataset_checksum": "c" * 64,
            "fitness_profile_id": "mark_to_market_pnl_v1",
            "cost_profile_id": "explicit_accounting_v1",
            "input_digest": "d" * 64,
        },
    }
    wire = _valid_profile_input()
    original, _ = store.submit_ga_job_command(
        actor="operator@example.com",
        idempotency_key="replay-first",
        request=request,
        http_wire_intent=wire,
    )

    class Executor:
        evaluator = None

        def dispatch_ga_job(self, *_args):
            raise AssertionError("receipt replay must not dispatch")

    service = GaCommandService(
        store=store,
        executor=cast(ParameterSearchJobExecutor, Executor()),
        session_factory=None,
    )
    command = GaCommandRequest(
        operation="submit",
        idempotency_key="replay-first",
        body=wire,
    )
    status, replay = service.execute(command, actor="operator@example.com")
    assert status == 202
    assert replay == original


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
@pytest.mark.parametrize(
    ("operation", "source_status", "expected_status", "http_status"),
    [
        ("submit", None, GaJobStatus.QUEUED, 202),
        ("pause", GaJobStatus.QUEUED, GaJobStatus.PAUSED, 200),
        ("resume", GaJobStatus.PAUSED, GaJobStatus.QUEUED, 200),
        ("cancel", GaJobStatus.QUEUED, GaJobStatus.CANCELLED, 200),
        ("retry", GaJobStatus.CANCELLED, GaJobStatus.QUEUED, 202),
    ],
)
def test_service_commands_commit_once_and_replay_original_snapshot(
    store_kind, operation, source_status, expected_status, http_status, tmp_path
):
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SqliteJobStore(tmp_path / "service-commands.sqlite3")
    )
    request = {
        "kind": "parameter_search",
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "start_time": 1_704_067_200_000,
        "end_time": 1_704_067_800_000,
        "market_data": {"kind": "sealed_dataset", "dataset_id": "sealed-a"},
        "objective": "maximize_score",
        "seed": 17,
        "backtest": {
            "initial_balance": "10000",
            "maker_fee": "0.000001",
            "taker_fee": "0.000123",
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        },
        "search_space": {"parameters": {"quantity": {"min": "0.01"}}},
        "evolution": {
            "epoch_id": "source-epoch",
            "max_generations": 2,
            "population_size": 2,
        },
        "ga_binding": {
            "parameter_search_profile_id": "golden_cross_research_v1",
            "profile_revision": "a" * 64,
            "strategy_subject": "builtin:golden_cross",
            "strategy_version": "b" * 64,
            "dataset_id": "sealed-a",
            "dataset_checksum": "c" * 64,
            "fitness_profile_id": "mark_to_market_pnl_v1",
            "cost_profile_id": "explicit_accounting_v1",
            "input_digest": "d" * 64,
        },
    }
    original_job = None
    if operation != "submit":
        original_job = store.create_ga_job(request=request, epoch_id="source-epoch")
        if source_status == GaJobStatus.PAUSED:
            original_job = store.transition_ga_job(
                original_job.id, "pause", original_job.version
            )
        elif source_status == GaJobStatus.CANCELLED:
            original_job = store.transition_ga_job(
                original_job.id, "cancel", original_job.version
            )

    dispatched = []

    class Executor:
        evaluator = None

        def dispatch_ga_job(self, job_id, version):
            dispatched.append((job_id, version))

    class Service(GaCommandService):
        def _compile(self, payload):
            del payload
            return deepcopy(request)

        def _validate_existing_binding(self, source):
            del source
            return None

    service = Service(
        store=store,
        executor=cast(ParameterSearchJobExecutor, Executor()),
        session_factory=None,
    )
    if operation == "submit":
        command_body = _valid_profile_input()
    else:
        assert original_job is not None
        command_body = {"expected_version": original_job.version}
    command = GaCommandRequest(
        operation=operation,
        idempotency_key=f"service-{operation}",
        job_id=None if original_job is None else original_job.id,
        body=command_body,
    )
    actor = "operator@example.com"
    first_status, first = service.execute(command, actor=actor)
    assert first_status == http_status
    assert first.status == expected_status
    if original_job is not None:
        if operation == "retry":
            assert store.get_ga_job(original_job.id) == original_job
            assert first.retry_of_job_id == original_job.id
        else:
            assert store.get_ga_job(original_job.id) == first
    expected_dispatches = 1 if operation in {"submit", "resume", "retry"} else 0
    assert len(dispatched) == expected_dispatches

    replay_status, replay = service.execute(command, actor=actor)
    assert replay_status == http_status
    assert replay == first
    assert len(dispatched) == expected_dispatches


def test_dispatch_failure_keeps_accepted_receipt_and_replay_does_not_reschedule(caplog):
    store = InMemoryJobStore()
    request = {
        "kind": "parameter_search",
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "start_time": 1_704_067_200_000,
        "end_time": 1_704_067_800_000,
        "market_data": {"kind": "sealed_dataset", "dataset_id": "sealed-a"},
        "objective": "maximize_score",
        "seed": 17,
        "backtest": {
            "initial_balance": "10000",
            "maker_fee": "0.000001",
            "taker_fee": "0.000123",
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        },
        "search_space": {"parameters": {"quantity": {"min": "0.01"}}},
        "evolution": {
            "epoch_id": "source-epoch",
            "max_generations": 2,
            "population_size": 2,
        },
        "ga_binding": {
            "parameter_search_profile_id": "golden_cross_research_v1",
            "profile_revision": "a" * 64,
            "strategy_subject": "builtin:golden_cross",
            "strategy_version": "b" * 64,
            "dataset_id": "sealed-a",
            "dataset_checksum": "c" * 64,
            "fitness_profile_id": "mark_to_market_pnl_v1",
            "cost_profile_id": "explicit_accounting_v1",
            "input_digest": "d" * 64,
        },
    }

    class Executor:
        evaluator = None
        calls = 0

        def dispatch_ga_job(self, *_args):
            self.calls += 1
            raise RuntimeError("private dispatch detail")

    class Service(GaCommandService):
        def _compile(self, payload):
            del payload
            return deepcopy(request)

    executor = Executor()
    service = Service(
        store=store,
        executor=cast(ParameterSearchJobExecutor, executor),
        session_factory=None,
    )
    command = GaCommandRequest(
        operation="submit",
        idempotency_key="dispatch-failure",
        body=_valid_profile_input(),
    )
    status, receipt = service.execute(command, actor="operator@example.com")
    assert status == 202
    assert receipt.status == GaJobStatus.QUEUED
    assert store.get_ga_job(receipt.id) == receipt
    replay_status, replay = service.execute(command, actor="operator@example.com")
    assert replay_status == 202
    assert replay == receipt
    assert executor.calls == 1
    assert "ga_dispatch_failed" in caplog.text
    assert "private dispatch detail" not in caplog.text


@pytest.mark.parametrize(
    ("method", "path", "body", "expected"),
    [
        ("POST", "/api/v1/ga-jobs", "{}", "submit"),
        ("POST", "/api/v1/ga-jobs/job-1/pause", '{"expected_version":1}', "pause"),
    ],
)
def test_ga_command_parser_accepts_only_closed_command_shapes(
    method, path, body, expected
):
    parsed = parse_ga_command_request(
        method,
        path,
        "",
        body,
        {"Idempotency-Key": "valid-key"},
        raw_target=path,
    )
    assert parsed is not None and parsed.operation == expected


@pytest.mark.parametrize(
    ("method", "path", "body", "headers", "raw_query", "raw_target"),
    [
        (
            "POST",
            "/api/v1/ga-jobs",
            '{"a":1,"a":2}',
            {"Idempotency-Key": "k"},
            "",
            "/api/v1/ga-jobs",
        ),
        (
            "POST",
            "/api/v1/ga-jobs",
            '{"a":NaN}',
            {"Idempotency-Key": "k"},
            "",
            "/api/v1/ga-jobs",
        ),
        (
            "POST",
            "/api/v1/ga-jobs/x/pause",
            '{"expected_version":true}',
            {"Idempotency-Key": "k"},
            "",
            "/api/v1/ga-jobs/x/pause",
        ),
        (
            "POST",
            "/api/v1/ga-jobs/x/pause",
            '{"expected_version":1,"extra":0}',
            {"Idempotency-Key": "k"},
            "",
            "/api/v1/ga-jobs/x/pause",
        ),
        (
            "POST",
            "/api/v1/ga-jobs",
            "{}",
            {"Idempotency-Key": "bad key"},
            "",
            "/api/v1/ga-jobs",
        ),
        (
            "POST",
            "/api/v1/ga-jobs",
            "{}",
            {"Idempotency-Key": "k"},
            "x=1",
            "/api/v1/ga-jobs?x=1",
        ),
        (
            "POST",
            "/api/v1/ga-jobs",
            "{}",
            {"Idempotency-Key": "k"},
            "",
            "/api/v1/ga-jobs\t?x=1",
        ),
    ],
)
def test_ga_command_parser_rejects_malformed_wire_before_io(
    method, path, body, headers, raw_query, raw_target
):
    from src.control_plane.ga_http_contract import GaHttpRequestError

    with pytest.raises(GaHttpRequestError):
        parse_ga_command_request(
            method, path, raw_query, body, headers, raw_target=raw_target
        )


@pytest.mark.parametrize(
    ("path", "operation", "status"),
    [
        ("/api/v1/ga-jobs", "submit", 202),
        ("/api/v1/ga-jobs/job-1/pause", "pause", 200),
        ("/api/v1/ga-jobs/job-1/resume", "resume", 200),
        ("/api/v1/ga-jobs/job-1/cancel", "cancel", 200),
        ("/api/v1/ga-jobs/job-1/retry", "retry", 202),
    ],
)
def test_all_ga_command_routes_use_one_operator_service(path, operation, status):
    app = _app()
    record = GaJobRecord(
        id="job-1",
        status=GaJobStatus.QUEUED,
        version=1,
        request={},
        epoch_id="epoch-1",
    )

    class Service:
        calls = []

        def execute(self, command, *, actor):
            self.calls.append((command, actor))
            return status, record

    service = Service()
    app.ga_command_service = cast(GaCommandService, service)
    body = (
        json.dumps(_valid_profile_input())
        if operation == "submit"
        else '{"expected_version":1}'
    )
    response = app.handle(
        "POST",
        path,
        body=body,
        headers={**_auth_headers(app), "Idempotency-Key": "route-test"},
    )
    assert response.status_code == status
    assert set(response.body) == {"schema_version", "job"}
    assert set(response.body["job"]) == {
        "id",
        "kind",
        "status",
        "version",
        "epoch_id",
        "completed_generation",
        "checkpoint",
        "retry_of_job_id",
        "request",
        "error",
    }
    assert response.body["job"]["id"] == "job-1"
    assert response.body["job"]["kind"] == "ga"
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    command, actor = service.calls[0]
    assert command.operation == operation
    assert actor == "operator@example.com"


@pytest.mark.parametrize(
    "path,body",
    [
        ("/api/v1/ga-jobs", json.dumps(_valid_profile_input())),
        ("/api/v1/ga-jobs/job-1/pause", '{"expected_version":1}'),
        ("/api/v1/ga-jobs/job-1/resume", '{"expected_version":1}'),
        ("/api/v1/ga-jobs/job-1/cancel", '{"expected_version":1}'),
        ("/api/v1/ga-jobs/job-1/retry", '{"expected_version":1}'),
    ],
)
def test_command_auth_and_missing_service_fail_closed_for_every_route(path, body):
    app = _app()
    browser_headers = _auth_headers(app)
    response = app.handle("POST", path, body=body, headers={})
    assert (response.status_code, response.body) == (401, {"error": "unauthorized"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}

    response = app.handle(
        "POST", path, body=body, headers={"x-api-key": "operator-key"}
    )
    assert (response.status_code, response.body) == (403, {"error": "forbidden"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}

    response = app.handle(
        "POST",
        path,
        body=body,
        headers={**browser_headers, "Idempotency-Key": "missing-service"},
    )
    assert (response.status_code, response.body) == (
        503,
        {"error": "ga_controls_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


def test_invalid_command_wire_is_rejected_before_missing_service():
    app = _app()
    response = app.handle(
        "POST",
        "/api/v1/ga-jobs/job-1/pause",
        body='{"expected_version":true}',
        headers={**_auth_headers(app), "Idempotency-Key": "invalid-version"},
    )
    assert (response.status_code, response.body) == (
        422,
        {"error": "validation_error"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


@pytest.mark.parametrize(
    ("failure", "status", "body"),
    [
        (GaJobStoreError("ga_job_not_found"), 404, {"error": "ga_job_not_found"}),
        (
            GaJobStoreError("job_version_conflict"),
            409,
            {"error": "job_version_conflict"},
        ),
        (GaJobStoreError("validation_error"), 422, {"error": "validation_error"}),
        (
            GaCommandValidationError("private details"),
            422,
            {"error": "validation_error"},
        ),
        (
            GaCommandBackendError("private details"),
            503,
            {"error": "ga_backend_unavailable"},
        ),
        (RuntimeError("private details"), 503, {"error": "ga_backend_unavailable"}),
    ],
)
def test_command_errors_are_mapped_to_fixed_responses(failure, status, body):
    app = _app()

    class Service:
        def execute(self, *_args, **_kwargs):
            raise failure

    app.ga_command_service = cast(GaCommandService, Service())
    response = app.handle(
        "POST",
        "/api/v1/ga-jobs/job-1/cancel",
        body='{"expected_version":1}',
        headers={**_auth_headers(app), "Idempotency-Key": "mapped-error"},
    )
    assert (response.status_code, response.body) == (status, body)
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert "private details" not in json.dumps(response.body)


def test_nonoperator_session_cannot_reach_ga_command_service():
    app = _app()

    class ExplodingService:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("authorization must precede command service access")

    app.ga_command_service = cast(GaCommandService, ExplodingService())
    identity = {
        "Origin": ORIGIN,
        "Tailscale-User-Login": "reader@example.com",
        "Tailscale-App-Capabilities": "{}",
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    response = app.handle(
        "POST",
        "/api/v1/ga-jobs/job-1/cancel",
        body='{"expected_version":1}',
        headers={
            **identity,
            "Cookie": dict(issued.headers)["Set-Cookie"].split(";", 1)[0],
            "X-CSRF-Token": issued.body["csrf_token"],
            "Idempotency-Key": "nonoperator",
        },
    )
    assert (response.status_code, response.body) == (
        403,
        {"error": "operator_capability_required"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


def test_sqlite_composition_interrupts_active_once_and_dispatches_only_queued(
    tmp_path, monkeypatch
):
    store = SqliteJobStore(tmp_path / "startup-ga.sqlite3")

    def create_job(epoch_id):
        return store.create_ga_job(
            request={
                "strategy": {"id": "golden_cross"},
                "evolution": {"epoch_id": epoch_id, "max_generations": 2},
            },
            epoch_id=epoch_id,
        )

    active = create_job("active-epoch")
    active = store.transition_ga_job(active.id, "claim", active.version)
    queued = create_job("queued-epoch")
    queued_again = create_job("queued-again-epoch")
    paused = create_job("paused-epoch")
    paused = store.transition_ga_job(paused.id, "pause", paused.version)
    terminal = create_job("terminal-epoch")
    terminal = store.transition_ga_job(terminal.id, "cancel", terminal.version)

    dispatched = []

    def capture_dispatch(executor, job_id, version):
        dispatched.append((job_id, version))
        if len(dispatched) == 1:
            raise RuntimeError("private dispatch failure")
        return store.get_ga_job(job_id)

    monkeypatch.setattr(ParameterSearchJobExecutor, "dispatch_ga_job", capture_dispatch)

    class Evaluator:
        def evaluate(
            self,
            request: ParameterSearchJobRequest,
            candidate: ParameterCandidate,
        ) -> ParameterEvaluationResult:
            del request, candidate
            raise AssertionError("startup test must not evaluate")

    engine = create_engine(f"sqlite:///{tmp_path / 'control-plane.sqlite3'}")
    sessions = sessionmaker(bind=engine)
    app = None
    try:
        app = build_control_plane_app(
            redis_client=MagicMock(),
            db_session_factory=sessions,
            job_store=store,
            parameter_search_evaluator=ParameterSearchEvaluatorRegistry(
                {"golden_cross": Evaluator()}
            ),
            readiness_probe=lambda: None,
        )
        interrupted = store.get_ga_job(active.id)
        queued_after = store.get_ga_job(queued.id)
        queued_again_after = store.get_ga_job(queued_again.id)
        paused_after = store.get_ga_job(paused.id)
        terminal_after = store.get_ga_job(terminal.id)
        assert interrupted is not None and interrupted.status == GaJobStatus.FAILED
        assert interrupted.error == "control_plane_interrupted"
        assert queued_after is not None and queued_after.status == GaJobStatus.QUEUED
        assert (
            queued_again_after is not None
            and queued_again_after.status == GaJobStatus.QUEUED
        )
        assert paused_after is not None and paused_after.status == GaJobStatus.PAUSED
        assert (
            terminal_after is not None
            and terminal_after.status == GaJobStatus.CANCELLED
        )
        assert dispatched == sorted(
            [(queued.id, queued.version), (queued_again.id, queued_again.version)]
        )
    finally:
        if app is not None:
            assert app.shutdown(timeout=10)
        engine.dispose()


def test_200_control_serializer_failure_is_sanitized_and_no_store(monkeypatch):
    app = _app()
    record = GaJobRecord(
        id="serializer-control-job",
        status=GaJobStatus.PAUSED,
        version=2,
        request={"evolution": {"max_generations": 1}},
        epoch_id="serializer-control-epoch",
    )

    class Service:
        def execute(self, *_args, **_kwargs):
            return 200, record

    def fail_serialization(_response):
        raise TypeError("private serializer failure")

    app.ga_command_service = cast(GaCommandService, Service())
    monkeypatch.setattr(HttpResponse, "json", fail_serialization)
    response = app.handle(
        "POST",
        f"/api/v1/ga-jobs/{record.id}/pause",
        body='{"expected_version":1}',
        headers={**_auth_headers(app), "Idempotency-Key": "control-serialize"},
    )
    assert (response.status_code, response.body) == (
        503,
        {"error": "ga_backend_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


def test_http_control_then_real_stale_claim_never_evaluates(tmp_path, monkeypatch):
    class CountingStore(SqliteJobStore):
        interrupt_calls = 0

        def interrupt_ga_jobs(self, error="control_plane_interrupted"):
            self.interrupt_calls += 1
            return super().interrupt_ga_jobs(error)

    class Evaluator:
        calls = 0

        def evaluate(self, request, candidate):
            del request, candidate
            self.calls += 1
            raise AssertionError("a paused or cancelled snapshot must not evaluate")

    store = CountingStore(tmp_path / "http-worker-claim.sqlite3")
    sources = [
        store.create_ga_job(
            request={"evolution": {"max_generations": 1}},
            epoch_id=f"claim-{action}-epoch",
        )
        for action in ("pause", "cancel")
    ]
    evaluator = Evaluator()
    engine = create_engine(f"sqlite:///{tmp_path / 'claim-control-plane.sqlite3'}")
    sessions = sessionmaker(bind=engine)
    startup_dispatches = []
    original_dispatch = ParameterSearchJobExecutor.dispatch_ga_job

    def hold_startup_dispatch(executor, job_id, version):
        startup_dispatches.append((executor, job_id, version))
        return None

    monkeypatch.setattr(
        ParameterSearchJobExecutor, "dispatch_ga_job", hold_startup_dispatch
    )
    app = build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=sessions,
        job_store=store,
        parameter_search_evaluator=ParameterSearchEvaluatorRegistry(
            {"golden_cross": evaluator}
        ),
        browser_auth=BrowserSessionAuth(
            allowed_origin=ORIGIN,
            operator_capability=OPERATOR,
            step_up_capability="example.com/cap/step-up",
        ),
        readiness_probe=lambda: None,
    )
    assert store.interrupt_calls == 1
    assert {(job_id, version) for _, job_id, version in startup_dispatches} == {
        (source.id, source.version) for source in sources
    }
    headers = _auth_headers(app)
    records = []
    for action, source in zip(("pause", "cancel"), sources, strict=True):
        response = app.handle(
            "POST",
            f"/api/v1/ga-jobs/{source.id}/{action}",
            body=json.dumps({"expected_version": source.version}),
            headers={**headers, "Idempotency-Key": f"claim-{action}"},
        )
        expected = GaJobStatus.PAUSED if action == "pause" else GaJobStatus.CANCELLED
        assert response.status_code == 200
        assert response.body["job"]["status"] == expected.value
        records.append((source, expected))

    expected_by_id = {source.id: expected for source, expected in records}
    for executor, job_id, snapshot_version in startup_dispatches:
        future = original_dispatch(executor, job_id, snapshot_version)
        assert isinstance(future, Future)
        with pytest.raises(GaJobStoreError) as error:
            future.result(timeout=5)
        assert error.value.code == "job_version_conflict"
        current = store.get_ga_job(job_id)
        assert current is not None and current.status == expected_by_id[job_id]
    assert evaluator.calls == 0
    assert store.interrupt_calls == 1
    assert app.shutdown(timeout=5)
    engine.dispose()


@pytest.mark.parametrize("failure_point", ["interrupt", "list"])
def test_startup_recovery_failure_closes_both_executors_and_hub(
    failure_point, tmp_path, monkeypatch
):
    class FailingStore(SqliteJobStore):
        def interrupt_ga_jobs(self, error="control_plane_interrupted"):
            if failure_point == "interrupt":
                raise RuntimeError("private startup interrupt failure")
            return super().interrupt_ga_jobs(error)

        def list_ga_jobs(self):
            if failure_point == "list":
                raise RuntimeError("private startup list failure")
            return super().list_ga_jobs()

    class Evaluator:
        def evaluate(self, request, candidate):
            del request, candidate
            raise AssertionError("startup failure must not evaluate")

    closed = []
    original_backtest_shutdown = BacktestJobExecutor.shutdown
    original_search_shutdown = ParameterSearchJobExecutor.shutdown
    original_hub_close = ControlPlaneInvalidationHub.close

    def close_backtest(executor, *args, **kwargs):
        closed.append("backtest")
        return original_backtest_shutdown(executor, *args, **kwargs)

    def close_search(executor, *args, **kwargs):
        closed.append("search")
        return original_search_shutdown(executor, *args, **kwargs)

    def close_hub(hub):
        closed.append("hub")
        return original_hub_close(hub)

    monkeypatch.setattr(BacktestJobExecutor, "shutdown", close_backtest)
    monkeypatch.setattr(ParameterSearchJobExecutor, "shutdown", close_search)
    monkeypatch.setattr(ControlPlaneInvalidationHub, "close", close_hub)
    engine = create_engine(f"sqlite:///{tmp_path / 'failed-startup.sqlite3'}")
    try:
        failure = "interrupt" if failure_point == "interrupt" else "list"
        with pytest.raises(RuntimeError, match=f"private startup {failure} failure"):
            build_control_plane_app(
                redis_client=MagicMock(),
                db_session_factory=sessionmaker(bind=engine),
                job_store=FailingStore(tmp_path / "failed-startup-jobs.sqlite3"),
                parameter_search_evaluator=ParameterSearchEvaluatorRegistry(
                    {"golden_cross": Evaluator()}
                ),
                readiness_probe=lambda: None,
            )
        assert sorted(closed) == ["backtest", "hub", "search"]
    finally:
        engine.dispose()


def test_simultaneous_http_submit_misses_commit_one_sqlite_receipt(
    tmp_path, monkeypatch
):
    store = SqliteJobStore(tmp_path / "simultaneous-http-submit.sqlite3")
    app = _app()
    preflight_gate = Barrier(2)
    misses = []
    dispatches = []
    original_receipt_lookup = store.get_ga_command_receipt
    request = {"evolution": {"max_generations": 1}}

    def observe_receipt_miss(*args, **kwargs):
        receipt = original_receipt_lookup(*args, **kwargs)
        if kwargs.get("idempotency_key") == "simultaneous-submit":
            misses.append(receipt)
        return receipt

    class Service(GaCommandService):
        def _compile(self, payload):
            del payload
            preflight_gate.wait(timeout=5)
            return deepcopy(request)

    class Executor:
        evaluator = None

        def dispatch_ga_job(self, job_id, version):
            dispatches.append((job_id, version))

    monkeypatch.setattr(store, "get_ga_command_receipt", observe_receipt_miss)
    app.ga_command_service = Service(
        store=store,
        executor=cast(ParameterSearchJobExecutor, Executor()),
        session_factory=None,
    )
    headers = {
        **_auth_headers(app),
        "Idempotency-Key": "simultaneous-submit",
    }
    body = json.dumps(_valid_profile_input())
    with ThreadPoolExecutor(max_workers=2) as callers:
        first = callers.submit(
            app.handle,
            "POST",
            "/api/v1/ga-jobs",
            body,
            headers,
        )
        second = callers.submit(
            app.handle,
            "POST",
            "/api/v1/ga-jobs",
            body,
            headers,
        )
        responses = (first.result(timeout=10), second.result(timeout=10))

    assert [response.status_code for response in responses] == [202, 202]
    assert responses[0].body == responses[1].body
    assert misses == [None, None]
    records = store.list_ga_jobs()
    assert len(records) == 1
    assert dispatches == [(records[0].id, records[0].version)]
