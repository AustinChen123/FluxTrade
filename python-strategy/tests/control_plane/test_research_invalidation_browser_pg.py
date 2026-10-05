from __future__ import annotations

import json
import http.client
import os
import shutil
import socket
import ssl
import subprocess
import time
from concurrent.futures import Future
from collections.abc import Callable
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.jobs import SqliteJobStore
from src.control_plane.main import build_control_plane_app
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy, SystemEvent
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from test_control_plane import PRODUCT_ID, TIMEFRAME, _write_research_candles
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_OPERATOR = "example.com/cap/fluxtrade-operator"
_STEP_UP = "example.com/cap/step-up"


def _wait_for(predicate, description: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError(f"timed out waiting for {description}")


def _https_json(
    port: int,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes | str | None = None,
) -> tuple[int, dict[str, str], object | None]:
    import http.client

    connection = http.client.HTTPSConnection(
        "127.0.0.1",
        port,
        context=ssl._create_unverified_context(),
        timeout=15,
    )
    connection.request(method, path, body=body, headers=headers)
    response = connection.getresponse()
    raw = response.read()
    result = (
        response.status,
        dict(response.getheaders()),
        json.loads(raw) if raw else None,
    )
    connection.close()
    return result


def _profile_input(dataset_id: str, start: int, end: int) -> dict[str, object]:
    from src.control_plane.ga_profile import get_golden_cross_profile

    profile = get_golden_cross_profile()
    return {
        "parameter_search_profile_id": profile["parameter_search_profile_id"],
        "strategy_subject": profile["strategy_subject"],
        "fitness_profile_id": profile["fitness_profile_id"],
        "cost_profile_id": profile["cost_profile_id"],
        "profile_revision": profile["profile_revision"],
        "strategy_version": profile["strategy_version"],
        "dataset_id": dataset_id,
        "start_time": start,
        "end_time": end,
        "initial_balance": "10000.123456789012345678901234",
        "fees": {"maker": "0.000001", "taker": "0.000123"},
        "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        "parameters": {
            "short_window": {"min": 1, "max": 2, "step": 1},
            "long_window": {"min": 3, "max": 4, "step": 1},
            "quantity": "0.01",
        },
        "population_size": 2,
        "max_generations": 1,
        "seed": 17,
    }


def test_real_https_browser_reconnect_refreshes_missed_pg_epoch(
    fresh_pg_db: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.control_plane import server as server_module

    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db), pool_pre_ping=True)
    sessions = sessionmaker(bind=engine)
    with sessions() as session:
        session.add(Strategy(id="golden_cross", name="Golden Cross"))
        session.commit()

    csv_path = tmp_path / "sealed-research.csv"
    candles = _write_research_candles(csv_path)
    dataset_id = "research-invalidation-browser"
    imported = ResearchDatasetImporter(sessions).import_csv(
        csv_path,
        ResearchDatasetSpec(
            dataset_id=dataset_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            source="research-invalidation-browser-test",
            revision="v1",
        ),
    )
    assert imported.checksum_sha256

    frontend = Path(__file__).resolve().parents[3] / "frontend"
    assert (frontend / "dist" / "index.html").is_file(), "run frontend build first"
    node = shutil.which("node")
    npm = shutil.which("npm")
    openssl = shutil.which("openssl")
    assert node is not None and npm is not None and openssl is not None

    store = SqliteJobStore(tmp_path / "browser-ga.sqlite3")
    futures: dict[str, Future] = {}
    original_dispatch = ParameterSearchJobExecutor.dispatch_ga_job

    def capture_dispatch(executor, job_id, expected_version):
        future = original_dispatch(executor, job_id, expected_version)
        assert isinstance(future, Future)
        futures[job_id] = future
        return future

    monkeypatch.setattr(ParameterSearchJobExecutor, "dispatch_ga_job", capture_dispatch)
    reservation = socket.socket()
    reservation.bind(("127.0.0.1", 0))
    port = reservation.getsockname()[1]
    reservation.close()
    origin = f"https://127.0.0.1:{port}"
    app = build_control_plane_app(
        redis_client=MagicMock(),
        db_session_factory=sessions,
        job_store=store,
        browser_auth=BrowserSessionAuth(
            allowed_origin=origin,
            operator_capability=_OPERATOR,
            step_up_capability=_STEP_UP,
        ),
        readiness_probe=lambda: None,
    )

    handlers: list[Any] = []
    reconnect_attempted = Event()
    release_reconnect = Event()
    base_handler: type[BaseHTTPRequestHandler] = server_module.make_handler(
        app, static_dir=frontend / "dist"
    )

    class TrackedBackendHandler(base_handler):
        def do_GET(self):
            if self.path == "/api/v1/events":
                handlers.append(self)
            cast(
                Callable[[BaseHTTPRequestHandler], None],
                getattr(base_handler, "do_GET"),
            )(self)

    backend = ThreadingHTTPServer(("127.0.0.1", 0), TrackedBackendHandler)
    backend.daemon_threads = True
    backend_thread = Thread(target=backend.serve_forever, daemon=True)
    backend_thread.start()
    proxy_handlers: list[BaseHTTPRequestHandler] = []
    event_statuses: list[int] = []
    stream_exits: list[str] = []
    forwarded_ga_bodies: list[dict[str, object]] = []
    forwarded_ga_keys: list[str] = []
    event_requests = 0

    class HttpsProxyHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def do_GET(self) -> None:
            self._forward()

        def do_POST(self) -> None:
            self._forward()

        def _forward(self) -> None:
            nonlocal event_requests
            is_event_stream = self.path == "/api/v1/events"
            if is_event_stream:
                event_requests += 1
                if event_requests > 1:
                    reconnect_attempted.set()
                    if not release_reconnect.wait(timeout=30):
                        self.close_connection = True
                        return
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else None
            if self.command == "POST" and self.path == "/api/v1/ga-jobs" and body:
                parsed_body = json.loads(body)
                assert isinstance(parsed_body, dict)
                forwarded_ga_bodies.append(parsed_body)
                forwarded_ga_keys.append(self.headers.get("Idempotency-Key", ""))
            headers = {
                name: value
                for name, value in self.headers.items()
                if name.lower()
                not in {
                    "connection",
                    "keep-alive",
                    "tailscale-user-login",
                    "tailscale-app-capabilities",
                    "upgrade",
                }
            }
            headers["Tailscale-User-Login"] = "research-browser@example.invalid"
            headers["Tailscale-App-Capabilities"] = json.dumps(
                {_OPERATOR: [{}], _STEP_UP: [{}]}
            )
            upstream = http.client.HTTPConnection(
                "127.0.0.1", backend.server_port, timeout=5
            )
            response = None
            try:
                upstream.request(self.command, self.path, body=body, headers=headers)
                response = upstream.getresponse()
                if is_event_stream:
                    event_statuses.append(response.status)
                self.send_response(response.status, response.reason)
                for name, value in response.getheaders():
                    if name.lower() not in {
                        "connection",
                        "keep-alive",
                        "transfer-encoding",
                        "upgrade",
                    }:
                        self.send_header(name, value)
                self.end_headers()
                self.close_connection = True
                if is_event_stream:
                    proxy_handlers.append(self)
                    while True:
                        chunk = response.read1(4096)
                        if not chunk:
                            stream_exits.append("backend_eof")
                            return
                        self.wfile.write(chunk)
                        self.wfile.flush()
                else:
                    payload = response.read()
                    if payload:
                        self.wfile.write(payload)
            except (OSError, http.client.HTTPException) as exc:
                if is_event_stream:
                    stream_exits.append(type(exc).__name__)
                self.close_connection = True
            finally:
                if response is not None:
                    response.close()
                upstream.close()

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", port), HttpsProxyHandler)
    server.daemon_threads = True
    cert_path = tmp_path / "localhost.crt"
    key_path = tmp_path / "localhost.key"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-nodes",
            "-days",
            "1",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(certfile=cert_path, keyfile=key_path)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    assert server.server_address[1] == port
    monkeypatch.setattr(server_module, "_EVENT_HEARTBEAT_SECONDS", 0.1)

    browser_process: subprocess.Popen[str] | None = None
    try:
        identity_headers = {
            "Origin": origin,
            "Tailscale-User-Login": "research-browser@example.invalid",
            "Tailscale-App-Capabilities": json.dumps({_OPERATOR: [{}], _STEP_UP: [{}]}),
        }
        status, response_headers, session_body = _https_json(
            port,
            "POST",
            "/api/v1/auth/session",
            identity_headers,
            body=b"",
        )
        assert status == 201 and isinstance(session_body, dict)
        csrf_token = session_body.get("csrf_token")
        assert isinstance(csrf_token, str) and csrf_token
        cookie_header = response_headers.get("Set-Cookie")
        assert isinstance(cookie_header, str)
        assert "Secure" in cookie_header and "HttpOnly" in cookie_header
        assert "SameSite=Strict" in cookie_header
        cookies = SimpleCookie()
        cookies.load(cookie_header)
        cookie = next(iter(cookies.values()))
        cookie_pair = f"{cookie.key}={cookie.value}"
        command_headers = {
            "Origin": origin,
            "Cookie": cookie_pair,
            "Tailscale-User-Login": "research-browser@example.invalid",
            "Tailscale-App-Capabilities": json.dumps({_OPERATOR: [{}], _STEP_UP: [{}]}),
            "X-CSRF-Token": csrf_token,
            "Content-Type": "application/json",
            "Idempotency-Key": "browser-reconnect-initial",
        }
        profile = _profile_input(dataset_id, candles[0][0], candles[-1][0])

        def submit(idempotency_key: str) -> tuple[str, dict[str, object]]:
            headers = {**command_headers, "Idempotency-Key": idempotency_key}
            result_status, _headers, body = _https_json(
                port,
                "POST",
                "/api/v1/ga-jobs",
                headers,
                body=json.dumps(profile),
            )
            assert result_status == 202 and isinstance(body, dict)
            job = body.get("job")
            assert isinstance(job, dict)
            return str(job["id"]), job

        first_job_id, first_job = submit("browser-reconnect-initial")
        first_completed = futures[first_job_id].result(timeout=60)
        assert first_completed.status.value == "SUCCEEDED"
        first_request = first_job.get("request")
        assert isinstance(first_request, dict)
        first_evolution = first_request.get("evolution")
        assert isinstance(first_evolution, dict)
        first_epoch_id = first_evolution.get("epoch_id")
        assert isinstance(first_epoch_id, str) and first_epoch_id

        # Establish a known prior champion through the existing authenticated
        # lifecycle route, using a gene produced by the real native worker.
        with sessions() as session:
            first_genes = list(
                session.scalars(
                    select(GeneRecord)
                    .where(GeneRecord.epoch_id == first_epoch_id)
                    .order_by(GeneRecord.id)
                ).all()
            )
        assert len(first_genes) >= 2, "native fixture job must persist challengers"
        prior_gene_id = first_genes[0].id
        status, _headers, prior_receipt = _https_json(
            port,
            "POST",
            f"/genes/{prior_gene_id}/promote",
            command_headers,
            body=json.dumps({"reason": "fixture establishes prior champion"}),
        )
        assert status == 200 and isinstance(prior_receipt, dict)
        assert prior_receipt.get("scope") == "RESEARCH_CANDIDATE_ONLY"
        prior_gene = prior_receipt.get("gene")
        assert isinstance(prior_gene, dict)
        assert prior_gene.get("role") == "champion"
        assert prior_gene.get("retired_gene_ids") == []

        ready_path = tmp_path / "browser-ready.json"
        continue_path = tmp_path / "browser-continue.json"
        env = {
            **os.environ,
            "RESEARCH_REAL_ORIGIN": origin,
            "RESEARCH_REAL_COOKIE": cookie_pair,
            "RESEARCH_REAL_READY_FILE": str(ready_path),
            "RESEARCH_REAL_CONTINUE_FILE": str(continue_path),
            "RESEARCH_REAL_INITIAL_EPOCH": first_epoch_id,
            "RESEARCH_REAL_DATASET_ID": dataset_id,
            "RESEARCH_REAL_START": str(candles[0][0]),
            "RESEARCH_REAL_END": str(candles[-1][0]),
            "RESEARCH_REAL_RESULT_FILE": str(tmp_path / "browser-result.json"),
        }
        browser_process = subprocess.Popen(
            [
                npm,
                "run",
                "test:browser",
                "--",
                "--config=playwright.integration.config.ts",
                "e2e/research-invalidation-real.e2e.ts",
            ],
            cwd=frontend,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        _wait_for(lambda: ready_path.is_file(), "real browser initial selection", 90)
        assert browser_process is not None
        try:
            _wait_for(
                lambda: app.invalidation_hub.active_count == 1,
                "live browser SSE",
                15,
            )
        except AssertionError as exc:
            browser_process.terminate()
            browser_output, _ = browser_process.communicate(timeout=5)
            raise AssertionError(
                f"{exc}; event_requests={event_requests}; "
                f"backend_event_handlers={len(handlers)}; "
                f"event_statuses={event_statuses}; stream_exits={stream_exits}\n"
                f"{browser_output[-6000:]}"
            ) from exc
        assert handlers
        assert proxy_handlers
        stream_handler = proxy_handlers[-1]
        stream_handler.connection.shutdown(socket.SHUT_RDWR)
        stream_handler.connection.close()
        _wait_for(lambda: app.invalidation_hub.active_count == 0, "SSE disconnect", 10)
        _wait_for(reconnect_attempted.is_set, "browser reconnect barrier", 15)
        assert app.invalidation_hub.active_count == 0

        second_job_id, second_job = submit("browser-reconnect-second")
        second_completed = futures[second_job_id].result(timeout=60)
        assert second_completed.status.value == "SUCCEEDED"
        second_request = second_job.get("request")
        assert isinstance(second_request, dict)
        second_evolution = second_request.get("evolution")
        assert isinstance(second_evolution, dict)
        second_epoch_id = second_evolution.get("epoch_id")
        assert isinstance(second_epoch_id, str) and second_epoch_id != first_epoch_id
        with sessions() as session:
            durable_epoch = session.scalar(
                select(EvolutionEpoch).where(EvolutionEpoch.id == second_epoch_id)
            )
            assert durable_epoch is not None and durable_epoch.generations_run == 1
        assert app.invalidation_hub.active_count == 0
        release_reconnect.set()
        continue_path.write_text(
            json.dumps({"epochId": second_epoch_id}), encoding="utf-8"
        )
        _wait_for(
            lambda: app.invalidation_hub.active_count == 1, "browser SSE reconnect", 20
        )

        output, _ = browser_process.communicate(timeout=90)
        assert browser_process.returncode == 0, output[-8000:]
        result_path = tmp_path / "browser-result.json"
        assert result_path.is_file(), "browser did not report its UI-submitted job"
        browser_result = json.loads(result_path.read_text(encoding="utf-8"))
        assert isinstance(browser_result, dict)
        ui_job_id = browser_result.get("jobId")
        assert isinstance(ui_job_id, str) and ui_job_id in futures
        ui_completion = futures[ui_job_id].result(timeout=60)
        assert ui_completion.status.value == "SUCCEEDED"
        ui_record = store.get_ga_job(ui_job_id)
        assert ui_record is not None and ui_record.status.value == "SUCCEEDED"
        assert ui_record.epoch_id
        assert ui_record.completed_generation == 0
        assert len(forwarded_ga_bodies) == 3
        assert len(forwarded_ga_keys) == 3 and forwarded_ga_keys[-1]
        assert forwarded_ga_keys[:2] == [
            "browser-reconnect-initial",
            "browser-reconnect-second",
        ]
        ui_payload = forwarded_ga_bodies[-1]
        assert ui_payload["dataset_id"] == dataset_id
        assert ui_payload["initial_balance"] == "10000.123456789012345678901234"
        assert ui_payload["fees"] == {"maker": "0.000001", "taker": "0.000123"}
        stored_backtest = ui_record.request["backtest"]
        assert isinstance(stored_backtest, dict)
        assert stored_backtest["initial_balance"] == "10000.123456789012345678901234"
        assert stored_backtest["maker_fee"] == "0.000001"
        assert stored_backtest["taker_fee"] == "0.000123"
        persisted_receipt = store.get_ga_command_receipt(
            actor="research-browser@example.invalid",
            idempotency_key=forwarded_ga_keys[-1],
            operation="submit",
            http_wire_intent=ui_payload,
        )
        assert persisted_receipt is not None and persisted_receipt.id == ui_job_id
        with sessions() as session:
            ui_epoch = session.get(EvolutionEpoch, ui_record.epoch_id)
            assert ui_epoch is not None and ui_epoch.generations_run == 1
            ui_genes = list(
                session.scalars(
                    select(GeneRecord).where(GeneRecord.epoch_id == ui_record.epoch_id)
                ).all()
            )
            assert len(ui_genes) == 2
            promoted_gene_id = next(
                gene.id for gene in ui_genes if gene.role == "champion"
            )
            retired_prior = session.get(GeneRecord, prior_gene_id)
            assert retired_prior is not None and retired_prior.role == "retired"
            events = list(
                session.scalars(
                    select(SystemEvent).where(
                        SystemEvent.event_type.in_(["gene_promote", "gene_retire"]),
                        SystemEvent.related_gene_id.in_(
                            [prior_gene_id, promoted_gene_id]
                        ),
                    )
                ).all()
            )
            assert {(event.event_type, event.related_gene_id) for event in events} == {
                ("gene_promote", prior_gene_id),
                ("gene_retire", prior_gene_id),
                ("gene_promote", promoted_gene_id),
            }
    finally:
        release_reconnect.set()
        if browser_process is not None and browser_process.poll() is None:
            browser_process.terminate()
            try:
                browser_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                browser_process.kill()
                browser_process.wait(timeout=5)
        app.close_event_streams()
        for handler in proxy_handlers:
            try:
                handler.connection.shutdown(socket.SHUT_RDWR)
                handler.connection.close()
            except OSError:
                pass
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=3)
        assert not server_thread.is_alive()
        backend.shutdown()
        backend.server_close()
        backend_thread.join(timeout=3)
        assert not backend_thread.is_alive()
        app.shutdown(timeout=5)
        engine.dispose()
