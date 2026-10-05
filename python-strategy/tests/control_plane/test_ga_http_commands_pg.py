from __future__ import annotations

import json
import logging
import sqlite3
from copy import deepcopy
from concurrent.futures import Future
from contextlib import closing
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError

from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.app import HttpResponse
from src.control_plane.ga_lifecycle import GaJobStatus
from src.control_plane.invalidation import InvalidationSubscription
from src.control_plane.ga_profile import get_golden_cross_profile
from src.control_plane.jobs import SqliteJobStore
from src.control_plane.main import build_control_plane_app
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy
from src.core.research_datasets import (
    ResearchDatasetImporter,
    ResearchDatasetIntegrityError,
    ResearchDatasetSpec,
)
from test_control_plane import PRODUCT_ID, TIMEFRAME, _write_research_candles
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_ORIGIN = "https://fluxtrade.example.ts.net"
_OPERATOR = "example.com/cap/fluxtrade-operator"


def _profile_input(dataset_id: str, start: int, end: int) -> dict[str, object]:
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


def _session_headers(app) -> dict[str, str]:
    identity = {
        "Origin": _ORIGIN,
        "Tailscale-User-Login": "operator@example.com",
        "Tailscale-App-Capabilities": json.dumps({_OPERATOR: [{}]}),
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    return {
        **identity,
        "Cookie": dict(issued.headers)["Set-Cookie"].split(";", 1)[0],
        "X-CSRF-Token": issued.body["csrf_token"],
    }


def _drain_invalidation_events(
    stream: InvalidationSubscription,
) -> list[dict[str, object]]:
    events = []
    while (frame := stream.next_frame(timeout=0)) is not None:
        events.append(json.loads(frame.split(b"data: ", 1)[1].strip()))
    return events


def test_composed_http_submit_runs_native_and_replays_original_receipt(
    fresh_pg_db: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    sessions = sessionmaker(bind=engine)
    store_path = tmp_path / "ga-http.sqlite3"
    store = SqliteJobStore(store_path)
    dataset_id = "ga-http-sealed-native"
    candle_path = tmp_path / "candles.csv"
    rows = _write_research_candles(candle_path)
    imported = ResearchDatasetImporter(sessions).import_csv(
        candle_path,
        ResearchDatasetSpec(
            dataset_id=dataset_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            source="ga-http-native-test",
            revision="v1",
        ),
    )
    futures: list[Future] = []
    original_dispatch = ParameterSearchJobExecutor.dispatch_ga_job

    def capture_dispatch(executor, job_id, expected_version):
        future = original_dispatch(executor, job_id, expected_version)
        assert isinstance(future, Future)
        futures.append(future)
        return future

    monkeypatch.setattr(ParameterSearchJobExecutor, "dispatch_ga_job", capture_dispatch)
    app = None
    try:
        app = build_control_plane_app(
            redis_client=MagicMock(),
            db_session_factory=sessions,
            job_store=store,
            browser_auth=BrowserSessionAuth(
                allowed_origin=_ORIGIN,
                operator_capability=_OPERATOR,
                step_up_capability="example.com/cap/step-up",
            ),
            readiness_probe=lambda: None,
        )
        event_stream = app.invalidation_hub.subscribe()
        headers = _session_headers(app)
        profile_input = _profile_input(dataset_id, rows[0][0], rows[-1][0])
        original_publish = app.invalidation_hub.publish_committed
        publication_checks: list[tuple[str, str, int]] = []
        receipt_key_for_publish: str | None = None

        def verify_durable_state_before_publish(
            resource: str, identity: str, revision: int
        ) -> None:
            if resource == "ga_job":
                with closing(sqlite3.connect(store_path, timeout=0.1)) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    job_row = connection.execute(
                        "SELECT ga_version FROM control_plane_jobs "
                        "WHERE id = ? AND kind = 'ga'",
                        (identity,),
                    ).fetchone()
                    assert job_row == (revision,)
                    if revision == 1 and receipt_key_for_publish is not None:
                        receipt = connection.execute(
                            "SELECT record_json FROM control_plane_ga_command_receipts "
                            "WHERE actor = ? AND idempotency_key = ?",
                            ("operator@example.com", receipt_key_for_publish),
                        ).fetchone()
                        assert receipt is not None
                        receipt_record = json.loads(receipt[0])
                        assert receipt_record["id"] == identity
                        assert receipt_record["version"] == revision
                    connection.rollback()
            else:
                assert resource == "evolution_epoch"
                assert cast(QueuePool, engine.pool).checkedout() == 0
                with sessions() as session:
                    committed = session.scalar(
                        select(EvolutionEpoch)
                        .where(EvolutionEpoch.id == identity)
                        .with_for_update(nowait=True)
                    )
                    assert committed is not None
                    assert committed.revision == revision
            publication_checks.append((resource, identity, revision))
            original_publish(resource, identity, revision)

        monkeypatch.setattr(
            app.invalidation_hub,
            "publish_committed",
            verify_durable_state_before_publish,
        )
        invalid_input = deepcopy(profile_input)
        invalid_input["dataset_id"] = "missing-sealed-dataset"
        rejected = app.handle(
            "POST",
            "/api/v1/ga-jobs",
            body=json.dumps(invalid_input),
            headers={**headers, "Idempotency-Key": "native-submit-1"},
        )
        assert (rejected.status_code, rejected.body) == (
            422,
            {"error": "validation_error"},
        )
        assert dict(rejected.headers) == {"Cache-Control": "no-store"}
        assert store.list_ga_jobs() == []
        with sessions() as session:
            assert session.scalars(select(EvolutionEpoch)).all() == []

        strategy_rejected = app.handle(
            "POST",
            "/api/v1/ga-jobs",
            body=json.dumps(profile_input),
            headers={**headers, "Idempotency-Key": "native-submit-1"},
        )
        assert (strategy_rejected.status_code, strategy_rejected.body) == (
            422,
            {"error": "validation_error"},
        )
        assert store.list_ga_jobs() == []
        with sessions() as session:
            assert session.scalars(select(EvolutionEpoch)).all() == []

        with monkeypatch.context() as scoped:

            def unavailable(_source):
                raise OperationalError("SELECT", {}, RuntimeError("private"))

            scoped.setattr(
                ResearchDatabaseDataSource, "get_dataset_metadata", unavailable
            )
            unavailable_response = app.handle(
                "POST",
                "/api/v1/ga-jobs",
                body=json.dumps(profile_input),
                headers={**headers, "Idempotency-Key": "backend-unavailable"},
            )
            assert (unavailable_response.status_code, unavailable_response.body) == (
                503,
                {"error": "ga_backend_unavailable"},
            )
            assert dict(unavailable_response.headers) == {"Cache-Control": "no-store"}
            assert store.list_ga_jobs() == []

        with sessions() as session:
            session.add(Strategy(id="golden_cross", name="Golden Cross"))
            session.commit()

        def fail_success_serialization(self):
            if self.status_code in {200, 202}:
                raise TypeError("private serializer failure")
            return HttpResponse.json(self)

        receipt_key_for_publish = "native-submit-1"
        with monkeypatch.context() as scoped:
            scoped.setattr(HttpResponse, "json", fail_success_serialization)
            response = app.handle(
                "POST",
                "/api/v1/ga-jobs",
                body=json.dumps(profile_input),
                headers={**headers, "Idempotency-Key": "native-submit-1"},
            )
        receipt_key_for_publish = None
        assert (response.status_code, response.body) == (
            503,
            {"error": "ga_backend_unavailable"},
        )
        assert dict(response.headers) == {"Cache-Control": "no-store"}
        original_receipt = store.get_ga_command_receipt(
            actor="operator@example.com",
            idempotency_key="native-submit-1",
            operation="submit",
            http_wire_intent=profile_input,
        )
        assert original_receipt is not None
        assert original_receipt.status == GaJobStatus.QUEUED
        assert len(futures) == 1
        replay = app.handle(
            "POST",
            "/api/v1/ga-jobs",
            body=json.dumps(profile_input),
            headers={**headers, "Idempotency-Key": "native-submit-1"},
        )
        assert replay.status_code == 202
        assert replay.body["job"]["id"] == original_receipt.id
        assert len(futures) == 1
        assert replay.body["schema_version"] == 1
        original_receipt_body = replay.body["job"]
        assert original_receipt_body["status"] == "QUEUED"
        assert original_receipt_body["version"] == 1
        binding = original_receipt_body["request"]["ga_binding"]
        assert binding["dataset_id"] == dataset_id
        assert binding["dataset_checksum"] == imported.checksum_sha256
        assert binding["strategy_version"] == profile_input["strategy_version"]
        assert len(binding["input_digest"]) == 64
        assert original_receipt_body["request"]["backtest"]["initial_balance"] == (
            "10000.123456789012345678901234"
        )
        assert len(futures) == 1
        completed = futures[0].result(timeout=60)
        assert completed.status.value == "SUCCEEDED"
        assert completed.completed_generation == 0
        with sessions() as session:
            epoch = session.get(EvolutionEpoch, completed.epoch_id)
            assert epoch is not None and epoch.generations_run == 1
            assert epoch.revision == 3
            genes = session.scalars(
                select(GeneRecord).where(GeneRecord.epoch_id == completed.epoch_id)
            ).all()
            assert len(genes) == 2

        events = _drain_invalidation_events(event_stream)
        ga_versions = [
            event["revision"]
            for event in events
            if event["resource"] == "ga_job" and event["identity"] == completed.id
        ]
        epoch_revisions = [
            event["revision"]
            for event in events
            if event["resource"] == "evolution_epoch"
            and event["identity"] == completed.epoch_id
        ]
        assert completed.version == 3
        assert ga_versions == [1, 2, 3]
        assert epoch_revisions == [1, 2, 3]
        assert publication_checks[:6] == [
            ("ga_job", completed.id, 1),
            ("ga_job", completed.id, 2),
            ("evolution_epoch", completed.epoch_id, 1),
            ("evolution_epoch", completed.epoch_id, 2),
            ("evolution_epoch", completed.epoch_id, 3),
            ("ga_job", completed.id, 3),
        ]

        retry_headers = {
            **headers,
            "Idempotency-Key": "native-retry-serializer-failure",
        }
        retry_body = json.dumps({"expected_version": completed.version})
        with monkeypatch.context() as scoped:
            scoped.setattr(HttpResponse, "json", fail_success_serialization)
            retry_response = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{completed.id}/retry",
                body=retry_body,
                headers=retry_headers,
            )
        assert (retry_response.status_code, retry_response.body) == (
            503,
            {"error": "ga_backend_unavailable"},
        )
        assert dict(retry_response.headers) == {"Cache-Control": "no-store"}
        assert len(futures) == 2
        retry_receipt = store.get_ga_command_receipt(
            actor="operator@example.com",
            idempotency_key="native-retry-serializer-failure",
            operation="retry",
            job_id=completed.id,
            expected_version=completed.version,
        )
        assert retry_receipt is not None and retry_receipt.status == GaJobStatus.QUEUED
        retry_replay = app.handle(
            "POST",
            f"/api/v1/ga-jobs/{completed.id}/retry",
            body=retry_body,
            headers=retry_headers,
        )
        assert retry_replay.status_code == 202
        assert retry_replay.body["job"]["id"] == retry_receipt.id
        assert len(futures) == 2
        assert futures[1].result(timeout=60).status.value == "SUCCEEDED"

        def source_for(operation, *, invalid_binding=False):
            request = deepcopy(completed.request)
            if invalid_binding:
                request["ga_binding"]["input_digest"] = "0" * 64
            source = store.create_ga_job(
                request=request,
                epoch_id=completed.epoch_id,
            )
            if operation == "resume":
                return store.transition_ga_job(source.id, "pause", source.version)
            running = store.transition_ga_job(source.id, "claim", source.version)
            return store.transition_ga_job(
                running.id, "fail", running.version, {"error": "fixture failure"}
            )

        def pg_counts():
            with sessions() as session:
                return (
                    len(session.scalars(select(EvolutionEpoch)).all()),
                    len(session.scalars(select(GeneRecord)).all()),
                )

        original_session_get = Session.get
        for operation in ("resume", "retry"):
            for failure in ("binding", "dataset", "strategy", "backend"):
                source = source_for(operation, invalid_binding=failure == "binding")
                before = store.get_ga_job(source.id)
                assert before == source
                job_ids_before = {record.id for record in store.list_ga_jobs()}
                counts_before = pg_counts()
                calls_before = len(futures)
                key = f"{operation}-{failure}-preflight"

                with monkeypatch.context() as scoped:
                    if failure in {"dataset", "backend"}:

                        def fail_dataset(_source):
                            if failure == "dataset":
                                raise ResearchDatasetIntegrityError()
                            raise OperationalError(
                                "SELECT", {}, RuntimeError("private database detail")
                            )

                        scoped.setattr(
                            ResearchDatabaseDataSource,
                            "get_dataset_metadata",
                            fail_dataset,
                        )
                    elif failure == "strategy":

                        def hide_strategy(session, entity, identity, *args, **kwargs):
                            if entity is Strategy and identity == "golden_cross":
                                return None
                            return original_session_get(
                                session, entity, identity, *args, **kwargs
                            )

                        scoped.setattr(Session, "get", hide_strategy)

                    response = app.handle(
                        "POST",
                        f"/api/v1/ga-jobs/{source.id}/{operation}",
                        body=json.dumps({"expected_version": source.version}),
                        headers={
                            **headers,
                            "Idempotency-Key": key,
                        },
                    )

                expected = (
                    (503, {"error": "ga_backend_unavailable"})
                    if failure == "backend"
                    else (422, {"error": "validation_error"})
                )
                assert (response.status_code, response.body) == expected
                assert dict(response.headers) == {"Cache-Control": "no-store"}
                assert store.get_ga_job(source.id) == before
                assert {record.id for record in store.list_ga_jobs()} == job_ids_before
                assert (
                    store.get_ga_command_receipt(
                        actor="operator@example.com",
                        idempotency_key=key,
                        operation=operation,
                        job_id=source.id,
                        expected_version=source.version,
                    )
                    is None
                )
                assert pg_counts() == counts_before
                assert len(futures) == calls_before

        unavailable_calls = []

        def unavailable_metadata(_source):
            unavailable_calls.append(True)
            raise OperationalError("SELECT", {}, RuntimeError("private"))

        queued_resume = store.create_ga_job(
            request=deepcopy(completed.request), epoch_id=completed.epoch_id
        )
        queued_retry = store.create_ga_job(
            request=deepcopy(completed.request), epoch_id=completed.epoch_id
        )
        paused_resume = source_for("resume")
        with monkeypatch.context() as scoped:
            scoped.setattr(
                ResearchDatabaseDataSource,
                "get_dataset_metadata",
                unavailable_metadata,
            )
            invalid_state = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{queued_resume.id}/resume",
                body=json.dumps({"expected_version": queued_resume.version}),
                headers={**headers, "Idempotency-Key": "resume-invalid-state"},
            )
            stale_version = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{completed.id}/retry",
                body=json.dumps({"expected_version": completed.version + 1}),
                headers={**headers, "Idempotency-Key": "retry-stale-version"},
            )
            retry_invalid_state = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{queued_retry.id}/retry",
                body=json.dumps({"expected_version": queued_retry.version}),
                headers={**headers, "Idempotency-Key": "retry-invalid-state"},
            )
            resume_stale_version = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{paused_resume.id}/resume",
                body=json.dumps({"expected_version": paused_resume.version + 1}),
                headers={**headers, "Idempotency-Key": "resume-stale-version"},
            )
            missing_job = app.handle(
                "POST",
                "/api/v1/ga-jobs/missing-job/resume",
                body='{"expected_version":1}',
                headers={**headers, "Idempotency-Key": "resume-missing-job"},
            )
            pause_source = store.create_ga_job(
                request=deepcopy(completed.request), epoch_id=completed.epoch_id
            )
            pause_response = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{pause_source.id}/pause",
                body=json.dumps({"expected_version": pause_source.version}),
                headers={**headers, "Idempotency-Key": "pause-without-pg"},
            )
            cancel_source = store.create_ga_job(
                request=deepcopy(completed.request), epoch_id=completed.epoch_id
            )
            cancel_response = app.handle(
                "POST",
                f"/api/v1/ga-jobs/{cancel_source.id}/cancel",
                body=json.dumps({"expected_version": cancel_source.version}),
                headers={**headers, "Idempotency-Key": "cancel-without-pg"},
            )
        assert (invalid_state.status_code, invalid_state.body) == (
            409,
            {"error": "job_transition_invalid"},
        )
        assert (stale_version.status_code, stale_version.body) == (
            409,
            {"error": "job_version_conflict"},
        )
        assert (retry_invalid_state.status_code, retry_invalid_state.body) == (
            409,
            {"error": "job_transition_invalid"},
        )
        assert (resume_stale_version.status_code, resume_stale_version.body) == (
            409,
            {"error": "job_version_conflict"},
        )
        assert (missing_job.status_code, missing_job.body) == (
            404,
            {"error": "ga_job_not_found"},
        )
        assert pause_response.status_code == 200
        assert cancel_response.status_code == 200
        assert not unavailable_calls
        paused_after_command = store.get_ga_job(pause_source.id)
        cancelled_after_command = store.get_ga_job(cancel_source.id)
        assert paused_after_command is not None
        assert cancelled_after_command is not None
        assert paused_after_command.status == GaJobStatus.PAUSED
        assert cancelled_after_command.status == GaJobStatus.CANCELLED
        store.transition_ga_job(queued_resume.id, "cancel", queued_resume.version)
        store.transition_ga_job(queued_retry.id, "cancel", queued_retry.version)

        def forbidden_preflight(_source):
            raise AssertionError("matching receipt must precede mutable preflight")

        valid_get_dataset_metadata = ResearchDatabaseDataSource.get_dataset_metadata
        monkeypatch.setattr(
            ResearchDatabaseDataSource, "get_dataset_metadata", forbidden_preflight
        )

        jobs_before_receipt_replay = store.list_ga_jobs()
        replay = app.handle(
            "POST",
            "/api/v1/ga-jobs",
            body=json.dumps(profile_input),
            headers={**headers, "Idempotency-Key": "native-submit-1"},
        )
        assert replay.status_code == 202
        assert replay.body["job"] == original_receipt_body
        assert len(futures) == 2
        assert store.list_ga_jobs() == jobs_before_receipt_replay
        monkeypatch.setattr(
            ResearchDatabaseDataSource,
            "get_dataset_metadata",
            valid_get_dataset_metadata,
        )

        original_hub = app.invalidation_hub
        assert app.shutdown(timeout=30)
        assert original_hub.active_count == 0
        app = None
        reopened_store = SqliteJobStore(tmp_path / "ga-http.sqlite3")
        app = build_control_plane_app(
            redis_client=MagicMock(),
            db_session_factory=sessions,
            job_store=reopened_store,
            browser_auth=BrowserSessionAuth(
                allowed_origin=_ORIGIN,
                operator_capability=_OPERATOR,
                step_up_capability="example.com/cap/step-up",
            ),
            readiness_probe=lambda: None,
        )
        reopened_headers = _session_headers(app)
        reopened_replay = app.handle(
            "POST",
            "/api/v1/ga-jobs",
            body=json.dumps(profile_input),
            headers={**reopened_headers, "Idempotency-Key": "native-submit-1"},
        )
        assert reopened_replay.status_code == 202
        assert reopened_replay.body["job"] == original_receipt_body
        assert len(futures) == 2

        changed_input = {**profile_input, "seed": 18}
        conflict = app.handle(
            "POST",
            "/api/v1/ga-jobs",
            body=json.dumps(changed_input),
            headers={**reopened_headers, "Idempotency-Key": "native-submit-1"},
        )
        assert (conflict.status_code, conflict.body) == (
            409,
            {"error": "idempotency_conflict"},
        )
        assert len(futures) == 2
        assert store.list_ga_jobs() == jobs_before_receipt_replay

        warning_calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []
        invalidation_logger = logging.getLogger("src.control_plane.invalidation")
        original_warning = invalidation_logger.warning

        def capture_fixed_warning(message, *args, **kwargs):
            warning_calls.append((message, args, kwargs))
            original_warning(message, *args, **kwargs)

        monkeypatch.setattr(invalidation_logger, "warning", capture_fixed_warning)
        app.invalidation_hub.close()
        closed_hub_headers = {
            **reopened_headers,
            "Idempotency-Key": "native-retry-closed-hub",
        }
        closed_hub_body = json.dumps({"expected_version": completed.version})
        closed_hub_retry = app.handle(
            "POST",
            f"/api/v1/ga-jobs/{completed.id}/retry",
            body=closed_hub_body,
            headers=closed_hub_headers,
        )
        assert closed_hub_retry.status_code == 202
        assert dict(closed_hub_retry.headers) == {"Cache-Control": "no-store"}
        assert len(futures) == 3
        closed_hub_result = futures[2].result(timeout=60)
        assert closed_hub_result.status.value == "SUCCEEDED"
        assert closed_hub_result.completed_generation == 0
        closed_hub_receipt = reopened_store.get_ga_command_receipt(
            actor="operator@example.com",
            idempotency_key="native-retry-closed-hub",
            operation="retry",
            job_id=completed.id,
            expected_version=completed.version,
        )
        assert closed_hub_receipt is not None
        assert closed_hub_receipt.status == GaJobStatus.QUEUED
        assert closed_hub_receipt.id == closed_hub_retry.body["job"]["id"]
        closed_hub_replay = app.handle(
            "POST",
            f"/api/v1/ga-jobs/{completed.id}/retry",
            body=closed_hub_body,
            headers=closed_hub_headers,
        )
        assert closed_hub_replay.status_code == 202
        assert closed_hub_replay.body == closed_hub_retry.body
        assert warning_calls
        assert all(
            call == ("invalidation_publish_failed", (), {}) for call in warning_calls
        )
        assert not any(
            completed.id in str(message) or closed_hub_receipt.id in str(message)
            for message, _args, _kwargs in warning_calls
        )
        assert len(futures) == 3
    finally:
        if app is not None:
            assert app.shutdown(timeout=30)
        assert cast(QueuePool, engine.pool).checkedout() == 0
        engine.dispose()
