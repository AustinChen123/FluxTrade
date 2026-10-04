from __future__ import annotations

import ast
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from src.control_plane.app import ControlPlaneApp, HttpResponse
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.ga_lifecycle import GaJobStatus
from src.control_plane.jobs import InMemoryJobStore, JobStore, SqliteJobStore
from src.control_plane.ga_profile import get_golden_cross_profile

ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR = "example.com/cap/fluxtrade-operator"


def _auth_headers(app: ControlPlaneApp, *, operator: bool = True) -> dict[str, str]:
    identity = {
        "Origin": ORIGIN,
        "Tailscale-User-Login": "operator@example.com",
        "Tailscale-App-Capabilities": json.dumps({OPERATOR: [{}]} if operator else {}),
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    return {
        **identity,
        "Cookie": dict(issued.headers)["Set-Cookie"].split(";", 1)[0],
        "X-CSRF-Token": issued.body["csrf_token"],
    }


def _app(store: JobStore | None = None) -> ControlPlaneApp:
    executor = Mock(spec=BacktestJobExecutor)
    executor.store = store or InMemoryJobStore()
    return ControlPlaneApp(
        executor,
        browser_auth=BrowserSessionAuth(
            allowed_origin=ORIGIN,
            operator_capability=OPERATOR,
            step_up_capability="example.com/cap/step-up",
        ),
        api_key="operator-key",
    )


@pytest.fixture(params=["memory", "sqlite"])
def store(request: pytest.FixtureRequest, tmp_path) -> JobStore:
    if request.param == "memory":
        return InMemoryJobStore()
    return SqliteJobStore(tmp_path / "ga-jobs.sqlite")


def _request() -> dict[str, object]:
    return {
        "strategy": {"id": "golden_cross", "version": "sha256:source"},
        "evolution": {"epoch_id": "epoch-1", "max_generations": 2},
        "seed": 17,
        "amount": "0.010000",
    }


def _transition(store, record, action: str, payload=None):
    return store.transition_ga_job(record.id, action, record.version, payload)


def _record_in_state(store, state: GaJobStatus):
    record = store.create_ga_job(request=_request(), epoch_id="epoch-1")
    if state == GaJobStatus.QUEUED:
        return record
    if state == GaJobStatus.PAUSED:
        return _transition(store, record, "pause")
    if state == GaJobStatus.CANCELLED:
        return _transition(store, record, "cancel")
    record = _transition(store, record, "claim")
    if state == GaJobStatus.RUNNING:
        return record
    if state == GaJobStatus.PAUSING:
        return _transition(store, record, "pause")
    if state == GaJobStatus.CANCELLING:
        return _transition(store, record, "cancel")
    if state == GaJobStatus.FAILED:
        return _transition(store, record, "fail", {"error": "private /tmp/path"})
    if state == GaJobStatus.SUCCEEDED:
        record = _transition(
            store,
            record,
            "ack_generation",
            {"completed_generation": 0, "checkpoint_epoch_id": "epoch-1"},
        )
        return _transition(
            store,
            record,
            "ack_generation",
            {"completed_generation": 1, "checkpoint_epoch_id": "epoch-1"},
        )
    raise AssertionError(f"unhandled GA status {state}")


def test_ga_profile_http_route_is_available_to_operator_session() -> None:
    app = _app()
    response = app.handle(
        "GET",
        "/api/v1/ga-profiles/golden_cross_research_v1",
        headers=_auth_headers(app),
    )

    assert response.status_code == 200
    assert response.body["schema_version"] == 1
    assert response.body["profile"]["parameter_search_profile_id"] == (
        "golden_cross_research_v1"
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


def test_profile_is_exact_detached_compiler_description(monkeypatch) -> None:
    app = _app()
    expected = get_golden_cross_profile()
    response = app.handle(
        "GET",
        "/api/v1/ga-profiles/golden_cross_research_v1",
        headers=_auth_headers(app),
    )
    assert response.body == {"schema_version": 1, "profile": expected}
    response.body["profile"]["accepted_fields"].clear()
    again = app.handle(
        "GET",
        "/api/v1/ga-profiles/golden_cross_research_v1",
        headers=_auth_headers(app),
    )
    assert again.body["profile"] == expected


def test_unknown_profile_and_source_failure_are_fixed(monkeypatch) -> None:
    import src.control_plane.app as app_module

    app = _app()
    headers = _auth_headers(app)
    unknown = app.handle("GET", "/api/v1/ga-profiles/another_profile", headers=headers)
    assert (unknown.status_code, unknown.body) == (
        404,
        {"error": "ga_profile_not_found"},
    )
    assert dict(unknown.headers) == {"Cache-Control": "no-store"}
    monkeypatch.setattr(
        app_module,
        "get_golden_cross_profile",
        Mock(side_effect=RuntimeError("private source detail")),
    )
    failed = app.handle(
        "GET", "/api/v1/ga-profiles/golden_cross_research_v1", headers=headers
    )
    assert (failed.status_code, failed.body) == (
        503,
        {"error": "ga_backend_unavailable"},
    )
    assert dict(failed.headers) == {"Cache-Control": "no-store"}
    assert "private source detail" not in failed.json()


@pytest.mark.parametrize("state", list(GaJobStatus))
def test_detail_projects_every_durable_state_without_mutating_record(store, state):
    record = _record_in_state(store, state)
    app = _app(store)
    response = app.handle(
        "GET", f"/api/v1/ga-jobs/{record.id}", headers=_auth_headers(app)
    )
    assert response.status_code == 200
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert set(response.body) == {"schema_version", "job"}
    projection = response.body["job"]
    assert set(projection) == {
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
    assert projection["status"] == state.value
    assert projection["request"] == _request()
    assert projection["error"] == (
        "ga_execution_failed" if state == GaJobStatus.FAILED else None
    )
    if state == GaJobStatus.SUCCEEDED:
        assert projection["checkpoint"] == {
            "epoch_id": "epoch-1",
            "completed_generation": 1,
        }
    before = store.get_ga_job(record.id)
    response.body["job"]["request"]["amount"] = "changed"
    assert store.get_ga_job(record.id) == before


def test_interrupted_error_is_preserved_but_other_error_text_is_hidden(store):
    active = _record_in_state(store, GaJobStatus.RUNNING)
    interrupted = store.transition_ga_job(
        active.id, "interrupt", active.version, {"error": "control_plane_interrupted"}
    )
    app = _app(store)
    headers = _auth_headers(app)
    result = app.handle("GET", f"/api/v1/ga-jobs/{interrupted.id}", headers=headers)
    assert result.body["job"]["error"] == "control_plane_interrupted"
    secret = store.create_ga_job(request=_request(), epoch_id="epoch-1")
    failed = _transition(
        store, _transition(store, secret, "claim"), "fail", {"error": "/private/sql"}
    )
    response = app.handle("GET", f"/api/v1/ga-jobs/{failed.id}", headers=headers)
    assert response.body["job"]["error"] == "ga_execution_failed"
    assert "/private/sql" not in response.json()


def test_index_orders_counts_then_offsets_and_excludes_legacy_rows(store):
    records = [_record_in_state(store, GaJobStatus.QUEUED) for _ in range(4)]
    store.create(kind="backtest", request=_LegacyRequest())
    app = _app(store)
    headers = _auth_headers(app)
    first = app.handle("GET", "/api/v1/ga-jobs?limit=2", headers=headers)
    second = app.handle("GET", "/api/v1/ga-jobs?limit=2&offset=2", headers=headers)
    assert first.status_code == second.status_code == 200
    assert first.body["schema_version"] == second.body["schema_version"] == 1
    assert first.body["total_count"] == second.body["total_count"] == 4
    ids = [item["id"] for item in first.body["items"] + second.body["items"]]
    assert ids == sorted(record.id for record in records)
    assert first.body["limit"] == 2 and first.body["offset"] == 0
    assert second.body["offset"] == 2
    assert all(
        item["kind"] == "ga" for item in first.body["items"] + second.body["items"]
    )
    empty = app.handle("GET", "/api/v1/ga-jobs?offset=2147483647", headers=headers)
    assert empty.body["items"] == [] and empty.body["total_count"] == 4


def test_index_default_and_numeric_boundaries_are_exact():
    app = _app()
    headers = _auth_headers(app)
    default = app.handle("GET", "/api/v1/ga-jobs", headers=headers)
    assert (default.status_code, default.body["limit"], default.body["offset"]) == (
        200,
        50,
        0,
    )
    bounded = app.handle(
        "GET", "/api/v1/ga-jobs?%6cimit=001&offset=000", headers=headers
    )
    assert (bounded.status_code, bounded.body["limit"], bounded.body["offset"]) == (
        200,
        1,
        0,
    )
    maximum = app.handle("GET", "/api/v1/ga-jobs?limit=100", headers=headers)
    assert maximum.status_code == 200 and maximum.body["limit"] == 100


def test_detail_missing_and_legacy_job_are_not_found(store):
    legacy = store.create(kind="backtest", request=_LegacyRequest())
    app = _app(store)
    headers = _auth_headers(app)
    for job_id in ("missing", legacy.id):
        response = app.handle("GET", f"/api/v1/ga-jobs/{job_id}", headers=headers)
        assert (response.status_code, response.body) == (
            404,
            {"error": "ga_job_not_found"},
        )
        assert dict(response.headers) == {"Cache-Control": "no-store"}


def test_detail_projects_retry_source_identity(store):
    source = _record_in_state(store, GaJobStatus.FAILED)
    retry, replayed = store.retry_ga_job_command(
        actor="operator@example.com",
        idempotency_key="retry-source",
        job_id=source.id,
        expected_version=source.version,
    )
    assert replayed is False
    app = _app(store)
    response = app.handle(
        "GET", f"/api/v1/ga-jobs/{retry.id}", headers=_auth_headers(app)
    )
    assert response.status_code == 200
    assert response.body["job"]["retry_of_job_id"] == source.id


class _LegacyRequest(BaseModel):
    kind: str = "backtest"


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/ga-jobs?limit=0",
        "/api/v1/ga-jobs?limit=101",
        "/api/v1/ga-jobs?offset=2147483648",
        "/api/v1/ga-jobs?limit=1&limit=2",
        "/api/v1/ga-jobs?offset=0&offset=1",
        "/api/v1/ga-jobs?limit=",
        "/api/v1/ga-jobs?limit=%ZZ",
        "/api/v1/ga-jobs?unknown=1",
        "/api/v1/ga-jobs?limit=１",
        "/api/v1/ga-jobs/a%2Fb",
        "/api/v1/ga-jobs/a%5Cb",
        "/api/v1/ga-jobs/%2E%2E",
        "/api/v1/ga-jobs/%C2%85",
        "/api/v1/ga-jobs/id?offset=0",
        "/api/v1/ga-profiles/golden_cross_research_v1?limit=1",
        "/api/v1/ga-jobs/\u0085",
    ],
)
def test_closed_route_query_and_identity_rejections_are_no_store(path):
    app = _app()
    response = app.handle("GET", path, headers=_auth_headers(app))
    assert (response.status_code, response.body) == (422, {"error": "validation_error"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}


@pytest.mark.parametrize("body", ["not empty", b"x"])
def test_ga_get_rejects_nonempty_body(body):
    app = _app()
    response = app.handle(
        "GET", "/api/v1/ga-jobs", body=body, headers=_auth_headers(app)
    )
    assert (response.status_code, response.body) == (422, {"error": "validation_error"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}


@pytest.mark.parametrize("body", [None, "", b""])
def test_ga_get_accepts_empty_body_forms(body):
    app = _app()
    response = app.handle(
        "GET", "/api/v1/ga-jobs", body=body, headers=_auth_headers(app)
    )
    assert (response.status_code, response.body["items"]) == (200, [])
    assert dict(response.headers) == {"Cache-Control": "no-store"}


@pytest.mark.parametrize("identity", ["missing", "api_key", "local", "nonoperator"])
def test_ga_read_authentication_is_browser_operator_only(identity):
    class CountingStore(InMemoryJobStore):
        reads = 0

        def get_ga_job(self, job_id: str):
            self.reads += 1
            return super().get_ga_job(job_id)

        def list_ga_jobs(self):
            self.reads += 1
            return super().list_ga_jobs()

    backing = CountingStore()
    app = _app(backing)
    if identity == "missing":
        headers = None
        expected = (401, {"error": "unauthorized"})
    elif identity == "api_key":
        headers = {"X-API-Key": "operator-key"}
        expected = (403, {"error": "forbidden"})
    elif identity == "local":
        app = ControlPlaneApp(Mock(spec=BacktestJobExecutor))
        headers = {}
        expected = (403, {"error": "forbidden"})
    else:
        headers = _auth_headers(app, operator=False)
        expected = (403, {"error": "forbidden"})
    for path in (
        "/api/v1/ga-profiles/golden_cross_research_v1",
        "/api/v1/ga-jobs",
        "/api/v1/ga-jobs/missing",
        "/api/v1/ga-jobs?limit=bad",
        "/api/v1/ga-jobs/%2F",
    ):
        response = app.handle("GET", path, headers=headers)
        assert (response.status_code, response.body) == expected
        assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert backing.reads == 0


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/api/v1/ga-jobs"),
        ("GET", "/api/v1/ga-jobs/id/extra"),
        ("GET", "/api/v1/ga-jobs/"),
        ("GET", "/api/v1/ga-profiles/"),
    ],
)
def test_unsupported_ga_route_is_fixed_not_found(method, path):
    app = _app()
    response = app.handle(method, path, headers=_auth_headers(app))
    assert (response.status_code, response.body) == (404, {"error": "not_found"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}


def test_sqlite_read_uses_reopened_store_and_preserves_command_receipts(tmp_path):
    path = tmp_path / "durable-ga.sqlite"
    store = SqliteJobStore(path)
    record, replayed = store.submit_ga_job_command(
        actor="operator@example.com",
        idempotency_key="read-only-key",
        request=_request(),
    )
    assert replayed is False
    with closing(sqlite3.connect(path)) as connection:
        before_receipts = connection.execute(
            "SELECT COUNT(*) FROM control_plane_ga_command_receipts"
        ).fetchone()[0]
    reopened = SqliteJobStore(path)
    app = _app(reopened)
    headers = _auth_headers(app)
    before = reopened.get_ga_job(record.id)
    for _ in range(2):
        response = app.handle("GET", f"/api/v1/ga-jobs/{record.id}", headers=headers)
        assert response.status_code == 200
        assert response.body["job"]["version"] == 1
    with closing(sqlite3.connect(path)) as connection:
        after_receipts = connection.execute(
            "SELECT COUNT(*) FROM control_plane_ga_command_receipts"
        ).fetchone()[0]
    assert reopened.get_ga_job(record.id) == before
    assert after_receipts == before_receipts == 1


def test_store_failure_is_sanitized_and_has_no_partial_index():
    class BrokenStore(InMemoryJobStore):
        def list_ga_jobs(self):
            raise RuntimeError("/private/path and credentials")

    app = _app(BrokenStore())
    response = app.handle("GET", "/api/v1/ga-jobs", headers=_auth_headers(app))
    assert (response.status_code, response.body) == (
        503,
        {"error": "ga_backend_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert "private/path" not in response.json()


@pytest.mark.parametrize(
    "method_name,path",
    [
        ("list_ga_jobs", "/api/v1/ga-jobs"),
        ("get_ga_job", "/api/v1/ga-jobs/job"),
    ],
)
def test_store_read_failures_are_sanitized(method_name, path):
    class BrokenStore(InMemoryJobStore):
        called: str | None = None

        def list_ga_jobs(self):
            self.called = "list_ga_jobs"
            raise RuntimeError("database secret")

        def get_ga_job(self, job_id: str):
            self.called = "get_ga_job"
            raise RuntimeError("database secret")

    store = BrokenStore()
    app = _app(store)
    response = app.handle("GET", path, headers=_auth_headers(app))
    assert (response.status_code, response.body) == (
        503,
        {"error": "ga_backend_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert "database secret" not in response.json()
    assert store.called == method_name


def test_passive_http_contract_has_no_inward_runtime_dependencies():
    source_path = Path(__file__).parents[2] / "src/control_plane/ga_http_contract.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)
    forbidden_prefixes = (
        "src.control_plane.app",
        "src.control_plane.main",
        "src.control_plane.backtest_jobs",
        "src.control_plane.jobs",
        "src.control_plane.parameter_search",
        "src.control_plane.evolution_persistence",
        "src.control_plane.orm_models",
        "src.strategies",
        "sqlalchemy",
        "psycopg",
    )
    assert all(
        not any(
            name == prefix or name.startswith(prefix + ".")
            for prefix in forbidden_prefixes
        )
        for name in imports
    )


@pytest.mark.parametrize(
    "target",
    [
        "/api/v1/ga-jobs?limit=1\t0",
        "/api/v1/ga-jobs?limit=1\r0",
        "/api/v1/ga-profiles/golden_cross_research_v1\n",
    ],
)
def test_original_ga_target_controls_reject_after_auth_without_reads(
    target, monkeypatch
):
    import src.control_plane.app as app_module

    class CountingStore(InMemoryJobStore):
        reads = 0

        def get_ga_job(self, job_id: str):
            self.reads += 1
            return super().get_ga_job(job_id)

        def list_ga_jobs(self):
            self.reads += 1
            return super().list_ga_jobs()

    store = CountingStore()
    app = _app(store)
    profile_source = Mock(wraps=app_module.get_golden_cross_profile)
    monkeypatch.setattr(app_module, "get_golden_cross_profile", profile_source)

    rejected = app.handle("GET", target, headers=_auth_headers(app))
    assert (rejected.status_code, rejected.body) == (
        422,
        {"error": "validation_error"},
    )
    assert dict(rejected.headers) == {"Cache-Control": "no-store"}
    for headers, expected in (
        (None, (401, {"error": "unauthorized"})),
        ({"X-API-Key": "operator-key"}, (403, {"error": "forbidden"})),
    ):
        response = app.handle("GET", target, headers=headers)
        assert (response.status_code, response.body) == expected
        assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert store.reads == 0
    profile_source.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/ga-profiles/golden_cross_research_v1",
        "/api/v1/ga-jobs/job",
        "/api/v1/ga-jobs",
    ],
)
def test_success_serialization_failure_becomes_fixed_no_store_503(path, monkeypatch):
    app = _app()
    if path.endswith("/job"):
        record = app.backtest_executor.store.create_ga_job(
            request=_request(), epoch_id="epoch-1"
        )
        path = f"/api/v1/ga-jobs/{record.id}"
    real_json = HttpResponse.json
    serialization_attempts = 0

    def fail_success_serialization(response: HttpResponse) -> str:
        nonlocal serialization_attempts
        if response.status_code == 200 and response.body.get("schema_version") == 1:
            serialization_attempts += 1
            raise RuntimeError("injected serializer failure")
        return real_json(response)

    monkeypatch.setattr(HttpResponse, "json", fail_success_serialization)
    response = app.handle("GET", path, headers=_auth_headers(app))
    assert serialization_attempts == 1
    assert (response.status_code, response.body) == (
        503,
        {"error": "ga_backend_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert response.json() == '{"error":"ga_backend_unavailable"}'
