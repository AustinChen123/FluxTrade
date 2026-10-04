from __future__ import annotations

import json
import logging
import sqlite3
import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from pydantic import BaseModel

import src.control_plane.jobs as jobs_module
from src.control_plane.ga_lifecycle import GaJobStatus, GaJobStoreError
from src.control_plane.invalidation import ControlPlaneInvalidationHub
from src.control_plane.jobs import InMemoryJobStore, SqliteJobStore


class LegacyRequest(BaseModel):
    value: int = 1


@pytest.fixture(params=["memory", "sqlite"])
def ga_store(request, tmp_path):
    return (
        InMemoryJobStore()
        if request.param == "memory"
        else SqliteJobStore(tmp_path / "jobs.sqlite")
    )


def _request(max_generations: int = 2) -> dict[str, Any]:
    return {
        "strategy": {"id": "golden_cross", "version": "1"},
        "evolution": {"epoch_id": "epoch-a", "max_generations": max_generations},
        "seed": 17,
    }


def _create(store, max_generations: int = 2):
    return store.create_ga_job(
        request=_request(max_generations),
        epoch_id="epoch-a",
    )


def _transition(store, job, action: str, payload=None):
    return store.transition_ga_job(job.id, action, job.version, payload)


def _job_in_state(store, state: GaJobStatus):
    job = _create(store)
    if state == GaJobStatus.QUEUED:
        return job
    if state == GaJobStatus.PAUSED:
        return _transition(store, job, "pause")
    if state == GaJobStatus.CANCELLED:
        return _transition(store, job, "cancel")
    job = _transition(store, job, "claim")
    if state == GaJobStatus.RUNNING:
        return job
    if state == GaJobStatus.PAUSING:
        return _transition(store, job, "pause")
    if state == GaJobStatus.CANCELLING:
        return _transition(store, job, "cancel")
    if state == GaJobStatus.FAILED:
        return _transition(store, job, "fail", {"error": "test_failure"})
    if state == GaJobStatus.SUCCEEDED:
        job = _transition(
            store,
            job,
            "ack_generation",
            {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
        )
        return _transition(
            store,
            job,
            "ack_generation",
            {"completed_generation": 1, "checkpoint_epoch_id": "epoch-a"},
        )
    raise AssertionError(f"unhandled state {state}")


def _assert_code(error: pytest.ExceptionInfo, code: str) -> None:
    assert isinstance(error.value, GaJobStoreError)
    assert error.value.code == code


def _next_event(stream):
    frame = stream.next_frame(timeout=0)
    assert frame is not None
    return json.loads(frame.split(b"data: ", 1)[1].strip())


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_ga_store_publishes_only_committed_mutations(store_kind, tmp_path):
    hub = ControlPlaneInvalidationHub()
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SqliteJobStore(tmp_path / "events.sqlite")
    )
    store.bind_invalidation_hub(hub)
    stream = hub.subscribe()

    request = _request()
    job, replayed = store.submit_ga_job_command(
        actor="operator",
        idempotency_key="submit-once",
        request=request,
    )
    assert not replayed
    assert _next_event(stream) == {
        "schema_version": 1,
        "resource": "ga_job",
        "identity": job.id,
        "revision": 1,
    }
    assert store.get_ga_job(job.id) == job
    assert store.list_ga_jobs() == [job]
    replay, replayed = store.submit_ga_job_command(
        actor="operator",
        idempotency_key="submit-once",
        request=request,
    )
    assert replayed and replay == job
    assert stream.next_frame(timeout=0) is None

    running = _transition(store, job, "claim")
    assert _next_event(stream)["revision"] == running.version == 2
    acknowledged = store.apply_ga_worker_envelope(
        running.id,
        epoch_id=running.epoch_id,
        expected_completed_generation=-1,
        action="ack_generation",
        payload={
            "completed_generation": 0,
            "checkpoint_epoch_id": running.epoch_id,
        },
    )
    assert _next_event(stream)["revision"] == acknowledged.version == 3

    paused, replayed = store.transition_ga_job_command(
        actor="operator",
        idempotency_key="pause-once",
        job_id=job.id,
        action="pause",
        expected_version=acknowledged.version,
    )
    assert not replayed and paused.status == GaJobStatus.PAUSING
    assert _next_event(stream)["revision"] == paused.version == 4
    replay, replayed = store.transition_ga_job_command(
        actor="operator",
        idempotency_key="pause-once",
        job_id=job.id,
        action="pause",
        expected_version=acknowledged.version,
    )
    assert replayed and replay == paused
    with pytest.raises(GaJobStoreError) as conflict:
        store.transition_ga_job(job.id, "pause", paused.version + 1)
    _assert_code(conflict, "job_version_conflict")
    assert stream.next_frame(timeout=0) is None

    checkpointed = store.apply_ga_worker_envelope(
        job.id,
        epoch_id=paused.epoch_id,
        expected_completed_generation=0,
        action="ack_generation",
        payload={
            "completed_generation": 1,
            "checkpoint_epoch_id": paused.epoch_id,
        },
    )
    assert checkpointed.status == GaJobStatus.PAUSED
    assert _next_event(stream)["revision"] == checkpointed.version == 5
    resumed, replayed = store.transition_ga_job_command(
        actor="operator",
        idempotency_key="resume-once",
        job_id=job.id,
        action="resume",
        expected_version=checkpointed.version,
    )
    assert not replayed and resumed.status == GaJobStatus.QUEUED
    assert _next_event(stream)["revision"] == resumed.version == 6
    cancelled, replayed = store.transition_ga_job_command(
        actor="operator",
        idempotency_key="cancel-once",
        job_id=job.id,
        action="cancel",
        expected_version=resumed.version,
    )
    assert not replayed and cancelled.status == GaJobStatus.CANCELLED
    assert _next_event(stream)["revision"] == cancelled.version == 7
    retried, replayed = store.retry_ga_job_command(
        actor="operator",
        idempotency_key="retry-once",
        job_id=job.id,
        expected_version=cancelled.version,
    )
    assert not replayed and retried.retry_of_job_id == job.id
    assert _next_event(stream) == {
        "schema_version": 1,
        "resource": "ga_job",
        "identity": retried.id,
        "revision": 1,
    }
    assert store.retry_ga_job_command(
        actor="operator",
        idempotency_key="retry-once",
        job_id=job.id,
        expected_version=cancelled.version,
    ) == (retried, True)
    assert stream.next_frame(timeout=0) is None

    hub.close()


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_closed_invalidation_hub_cannot_fail_committed_ga_create(
    store_kind, tmp_path, monkeypatch: pytest.MonkeyPatch
):
    hub = ControlPlaneInvalidationHub()
    hub.close()
    store = (
        InMemoryJobStore(hub)
        if store_kind == "memory"
        else SqliteJobStore(tmp_path / "closed-hub.sqlite", hub)
    )

    warning_calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []
    invalidation_logger = logging.getLogger("src.control_plane.invalidation")
    original_warning = invalidation_logger.warning

    def capture_fixed_warning(message, *args, **kwargs):
        warning_calls.append((message, args, kwargs))
        original_warning(message, *args, **kwargs)

    monkeypatch.setattr(invalidation_logger, "warning", capture_fixed_warning)
    request = _request()
    job, replayed = store.submit_ga_job_command(
        actor="operator",
        idempotency_key="closed-hub-submit",
        request=request,
    )

    assert not replayed
    assert store.get_ga_job(job.id) == job
    assert (
        store.get_ga_command_receipt(
            actor="operator",
            idempotency_key="closed-hub-submit",
            operation="submit",
            request=request,
        )
        == job
    )
    replay, replayed = store.submit_ga_job_command(
        actor="operator",
        idempotency_key="closed-hub-submit",
        request=request,
    )
    assert replayed and replay == job
    assert warning_calls == [("invalidation_publish_failed", (), {})]


def test_sqlite_command_receipt_rollback_publishes_nothing_and_reopens(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "receipt-rollback.sqlite"
    hub = ControlPlaneInvalidationHub()
    stream = hub.subscribe()
    store = SqliteJobStore(path, hub)
    request = _request()
    original_insert = jobs_module._insert_ga_command_receipt
    receipts_written: list[str] = []

    def insert_receipt_then_fail(conn, actor, idempotency_key, fingerprint, record):
        original_insert(conn, actor, idempotency_key, fingerprint, record)
        receipts_written.append(idempotency_key)
        raise RuntimeError("fault after receipt insert before commit")

    monkeypatch.setattr(
        jobs_module, "_insert_ga_command_receipt", insert_receipt_then_fail
    )
    with pytest.raises(RuntimeError, match="after receipt insert"):
        store.submit_ga_job_command(
            actor="operator",
            idempotency_key="atomic-submit",
            request=request,
        )
    assert receipts_written == ["atomic-submit"]
    assert store.list_ga_jobs() == []
    assert (
        store.get_ga_command_receipt(
            actor="operator",
            idempotency_key="atomic-submit",
            operation="submit",
            request=request,
        )
        is None
    )
    assert stream.next_frame(timeout=0) is None

    monkeypatch.setattr(jobs_module, "_insert_ga_command_receipt", original_insert)
    reopened = SqliteJobStore(path, hub)
    original_publish = hub.publish_committed
    publication_checks: list[tuple[str, str, int]] = []

    def verify_commit_before_publish(
        resource: str, identity: str, revision: int
    ) -> None:
        assert resource == "ga_job"
        with closing(sqlite3.connect(path, timeout=0.1)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            job_row = connection.execute(
                "SELECT ga_version FROM control_plane_jobs WHERE id = ? AND kind = 'ga'",
                (identity,),
            ).fetchone()
            receipt_row = connection.execute(
                "SELECT record_json FROM control_plane_ga_command_receipts "
                "WHERE actor = ? AND idempotency_key = ?",
                ("operator", "atomic-submit"),
            ).fetchone()
            assert job_row == (revision,)
            assert receipt_row is not None
            receipt = json.loads(receipt_row[0])
            assert (receipt["id"], receipt["version"]) == (identity, revision)
            connection.rollback()
        publication_checks.append((resource, identity, revision))
        original_publish(resource, identity, revision)

    monkeypatch.setattr(hub, "publish_committed", verify_commit_before_publish)
    committed, replayed = reopened.submit_ga_job_command(
        actor="operator",
        idempotency_key="atomic-submit",
        request=request,
    )
    assert not replayed
    assert reopened.get_ga_job(committed.id) == committed
    assert (
        reopened.get_ga_command_receipt(
            actor="operator",
            idempotency_key="atomic-submit",
            operation="submit",
            request=request,
        )
        == committed
    )
    assert publication_checks == [("ga_job", committed.id, committed.version)]
    assert _next_event(stream)["revision"] == committed.version == 1
    assert stream.next_frame(timeout=0) is None
    hub.close()


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_startup_interrupt_publishes_each_changed_ga_version(store_kind, tmp_path):
    hub = ControlPlaneInvalidationHub()
    store = (
        InMemoryJobStore(hub)
        if store_kind == "memory"
        else SqliteJobStore(tmp_path / "interrupt-events.sqlite", hub)
    )
    stream = hub.subscribe()
    first = _create(store)
    second = _create(store)
    assert _next_event(stream)["identity"] == first.id
    assert _next_event(stream)["identity"] == second.id
    first_running = _transition(store, first, "claim")
    second_running = _transition(store, second, "claim")
    assert _next_event(stream)["revision"] == first_running.version
    assert _next_event(stream)["revision"] == second_running.version

    interrupted = store.interrupt_ga_jobs()

    assert {record.id for record in interrupted} == {first.id, second.id}
    events = [_next_event(stream), _next_event(stream)]
    assert {event["identity"] for event in events} == {first.id, second.id}
    assert {event["revision"] for event in events} == {3}
    first_after = store.get_ga_job(first.id)
    second_after = store.get_ga_job(second.id)
    assert first_after is not None and first_after.status == GaJobStatus.FAILED
    assert second_after is not None and second_after.status == GaJobStatus.FAILED
    assert stream.next_frame(timeout=0) is None
    hub.close()


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_legacy_create_cannot_reserve_ga_kind(store_kind: str, tmp_path) -> None:
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SqliteJobStore(tmp_path / "jobs.sqlite")
    )
    with pytest.raises(ValueError, match="GA jobs require the GA store API"):
        store.create(kind="ga", request=LegacyRequest())

    assert store.list() == []


def test_ga_records_are_isolated_detached_and_legacy_visible_only_to_legacy_api(
    ga_store,
) -> None:
    request = _request()
    ga = ga_store.create_ga_job(request=request, epoch_id="epoch-a")
    request["seed"] = 99
    request["evolution"]["max_generations"] = 8
    ga.request["seed"] = 88
    ga.request["strategy"]["version"] = "mutated"

    assert ga.status == GaJobStatus.QUEUED
    assert ga.version == 1
    assert ga.completed_generation == -1
    assert ga.checkpoint is None
    assert ga_store.get(ga.id) is None
    assert ga_store.list() == []
    stored = ga_store.get_ga_job(ga.id)
    assert stored.request == _request()

    legacy = ga_store.create(kind="backtest", request=LegacyRequest())
    assert ga_store.get(legacy.id) == legacy
    assert ga_store.list() == [legacy]
    assert [record.id for record in ga_store.list_ga_jobs()] == [ga.id]
    with pytest.raises(KeyError):
        ga_store.mark_running(ga.id)


def test_ga_store_rejects_non_json_or_missing_generation_request(ga_store) -> None:
    for request in (
        {"evolution": {"max_generations": True}},
        {"evolution": {"max_generations": 0}},
        {"evolution": {"max_generations": 1}, "bad": float("nan")},
        {"evolution": {"max_generations": 1, "epoch_id": "other"}},
    ):
        with pytest.raises(GaJobStoreError) as error:
            ga_store.create_ga_job(request=request, epoch_id="epoch-a")
        _assert_code(error, "validation_error")
    assert ga_store.list_ga_jobs() == []


def test_ga_transition_error_precedence_and_strict_versions(ga_store) -> None:
    job = _create(ga_store)
    for version in (True, 1.0, 0, "1"):
        with pytest.raises(GaJobStoreError) as error:
            ga_store.transition_ga_job(job.id, "claim", version)
        _assert_code(error, "validation_error")
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job(job.id, "not-an-action", 99)
    _assert_code(error, "validation_error")
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job(job.id, ["claim"], 1)
    _assert_code(error, "validation_error")
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job(job.id, "claim", 99, {"unexpected": True})
    _assert_code(error, "validation_error")
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job(job.id, "claim", 2)
    _assert_code(error, "job_version_conflict")
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job("missing", "claim", True)
    _assert_code(error, "ga_job_not_found")
    legacy = ga_store.create(kind="backtest", request=LegacyRequest())
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job(legacy.id, "claim", True)
    _assert_code(error, "ga_job_not_found")
    assert ga_store.get_ga_job(job.id) == job


def test_ga_interrupt_requires_positive_current_version(ga_store) -> None:
    job = _transition(ga_store, _create(ga_store), "claim")
    payload = {"error": "control_plane_interrupted"}
    for version in (None, True, 99):
        with pytest.raises(GaJobStoreError) as error:
            ga_store.transition_ga_job(job.id, "interrupt", version, payload)
        _assert_code(
            error,
            "job_version_conflict" if version == 99 else "validation_error",
        )
        assert ga_store.get_ga_job(job.id) == job


def test_ga_state_action_matrix_for_both_stores(ga_store) -> None:
    expected = {
        GaJobStatus.QUEUED: {
            "claim": GaJobStatus.RUNNING,
            "pause": GaJobStatus.PAUSED,
            "cancel": GaJobStatus.CANCELLED,
        },
        GaJobStatus.RUNNING: {
            "pause": GaJobStatus.PAUSING,
            "cancel": GaJobStatus.CANCELLING,
            "ack_generation": GaJobStatus.RUNNING,
            "fail": GaJobStatus.FAILED,
            "interrupt": GaJobStatus.FAILED,
        },
        GaJobStatus.PAUSING: {
            "cancel": GaJobStatus.CANCELLING,
            "ack_generation": GaJobStatus.PAUSED,
            "fail": GaJobStatus.FAILED,
            "interrupt": GaJobStatus.FAILED,
        },
        GaJobStatus.PAUSED: {
            "resume": GaJobStatus.QUEUED,
            "cancel": GaJobStatus.CANCELLED,
        },
        GaJobStatus.CANCELLING: {
            "ack_generation": GaJobStatus.CANCELLED,
            "fail": GaJobStatus.FAILED,
            "interrupt": GaJobStatus.FAILED,
        },
        GaJobStatus.CANCELLED: {},
        GaJobStatus.SUCCEEDED: {},
        GaJobStatus.FAILED: {},
    }
    actions = (
        "claim",
        "pause",
        "resume",
        "cancel",
        "ack_generation",
        "finalize",
        "fail",
        "interrupt",
    )
    for state, legal_actions in expected.items():
        for action in actions:
            job = _job_in_state(ga_store, state)
            payload = None
            if action == "ack_generation":
                payload = {
                    "completed_generation": 0,
                    "checkpoint_epoch_id": "epoch-a",
                }
            elif action in {"fail", "interrupt"}:
                payload = {"error": "matrix_failure"}
            if action not in legal_actions:
                with pytest.raises(GaJobStoreError) as error:
                    _transition(ga_store, job, action, payload)
                _assert_code(error, "job_transition_invalid")
                assert ga_store.get_ga_job(job.id) == job
                continue
            updated = _transition(ga_store, job, action, payload)
            assert updated.status == legal_actions[action]
            assert updated.version == job.version + 1


def test_ga_claim_pause_resume_and_cancel_generation_boundary(ga_store) -> None:
    job = _create(ga_store)
    job = _transition(ga_store, job, "claim")
    assert (job.status, job.version) == (GaJobStatus.RUNNING, 2)
    job = _transition(ga_store, job, "pause")
    assert (job.status, job.version) == (GaJobStatus.PAUSING, 3)
    job = _transition(ga_store, job, "cancel")
    assert (job.status, job.version) == (GaJobStatus.CANCELLING, 4)
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert (job.status, job.version, job.completed_generation) == (
        GaJobStatus.CANCELLED,
        5,
        0,
    )
    assert job.checkpoint.epoch_id == "epoch-a"
    assert job.checkpoint.completed_generation == 0
    with pytest.raises(GaJobStoreError) as error:
        _transition(ga_store, job, "resume")
    _assert_code(error, "job_transition_invalid")


def test_ga_queued_pause_resume_preserves_checkpoint_and_completes_once(
    ga_store,
) -> None:
    job = _create(ga_store)
    job = _transition(ga_store, job, "pause")
    assert (job.status, job.version, job.completed_generation, job.checkpoint) == (
        GaJobStatus.PAUSED,
        2,
        -1,
        None,
    )
    job = _transition(ga_store, job, "resume")
    assert (job.status, job.version, job.request) == (
        GaJobStatus.QUEUED,
        3,
        _request(),
    )
    job = _transition(ga_store, job, "claim")
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert (job.status, job.completed_generation) == (GaJobStatus.RUNNING, 0)
    generation_zero = job
    with pytest.raises(GaJobStoreError) as error:
        _transition(
            ga_store,
            job,
            "ack_generation",
            {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
        )
    _assert_code(error, "validation_error")
    assert ga_store.get_ga_job(job.id) == generation_zero
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 1, "checkpoint_epoch_id": "epoch-a"},
    )
    assert (job.status, job.completed_generation, job.version) == (
        GaJobStatus.SUCCEEDED,
        1,
        6,
    )
    terminal_version = job.version
    assert job.checkpoint.completed_generation == 1
    terminal = job
    with pytest.raises(GaJobStoreError) as error:
        _transition(
            ga_store,
            job,
            "ack_generation",
            {"completed_generation": 2, "checkpoint_epoch_id": "epoch-a"},
        )
    _assert_code(error, "job_transition_invalid")
    with pytest.raises(GaJobStoreError) as error:
        _transition(ga_store, job, "finalize")
    _assert_code(error, "job_transition_invalid")
    assert ga_store.get_ga_job(job.id) == terminal
    with pytest.raises(GaJobStoreError) as error:
        ga_store.transition_ga_job(job.id, "finalize", terminal_version - 1)
    _assert_code(error, "job_version_conflict")
    assert ga_store.get_ga_job(job.id) == terminal


@pytest.mark.parametrize(
    ("pending_action", "expected_status"),
    [("pause", GaJobStatus.PAUSED), ("cancel", GaJobStatus.CANCELLED)],
)
def test_ga_pending_action_wins_at_final_generation(
    ga_store,
    pending_action: str,
    expected_status: GaJobStatus,
) -> None:
    job = _create(ga_store, max_generations=1)
    job = _transition(ga_store, job, "claim")
    job = _transition(ga_store, job, pending_action)
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert (
        job.status,
        job.completed_generation,
        job.checkpoint.completed_generation,
    ) == (
        expected_status,
        0,
        0,
    )


def test_ga_final_resume_requires_committed_terminal_generation(ga_store) -> None:
    job = _create(ga_store, max_generations=1)
    job = _transition(ga_store, job, "pause")
    job = _transition(ga_store, job, "resume")
    job = _transition(ga_store, job, "claim")
    with pytest.raises(GaJobStoreError) as error:
        _transition(ga_store, job, "finalize")
    _assert_code(error, "job_transition_invalid")
    unchanged = job

    job = _transition(ga_store, job, "pause")
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert job.status == GaJobStatus.PAUSED
    job = _transition(ga_store, job, "resume")
    job = _transition(ga_store, job, "claim")
    job = _transition(ga_store, job, "finalize")
    assert (job.status, job.completed_generation, job.version) == (
        GaJobStatus.SUCCEEDED,
        0,
        9,
    )
    with pytest.raises(GaJobStoreError) as error:
        _transition(ga_store, job, "finalize")
    _assert_code(error, "job_transition_invalid")
    assert ga_store.get_ga_job(job.id).version == job.version
    assert unchanged.status == GaJobStatus.RUNNING


@pytest.mark.parametrize(
    ("pending_action", "expected_status"),
    [
        (None, GaJobStatus.SUCCEEDED),
        ("pause", GaJobStatus.PAUSED),
        ("cancel", GaJobStatus.CANCELLED),
    ],
)
def test_ga_final_resume_preserves_last_boundary_action(
    ga_store,
    pending_action: str | None,
    expected_status: GaJobStatus,
) -> None:
    job = _transition(ga_store, _create(ga_store, max_generations=1), "claim")
    job = _transition(ga_store, job, "pause")
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    job = _transition(ga_store, job, "resume")
    job = _transition(ga_store, job, "claim")
    if pending_action is not None:
        job = _transition(ga_store, job, pending_action)
    finalized = _transition(ga_store, job, "finalize")
    assert finalized.status == expected_status
    assert finalized.completed_generation == 0
    assert finalized.checkpoint == job.checkpoint


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        (
            {"completed_generation": True, "checkpoint_epoch_id": "epoch-a"},
            "validation_error",
        ),
        (
            {"completed_generation": 1, "checkpoint_epoch_id": "epoch-a"},
            "validation_error",
        ),
        (
            {"completed_generation": 0, "checkpoint_epoch_id": "epoch-b"},
            "validation_error",
        ),
        (
            {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a", "extra": 1},
            "validation_error",
        ),
    ],
)
def test_ga_generation_acknowledgement_requires_exact_next_epoch_checkpoint(
    ga_store,
    payload: dict[str, object],
    code: str,
) -> None:
    job = _transition(ga_store, _create(ga_store), "claim")
    with pytest.raises(GaJobStoreError) as error:
        _transition(ga_store, job, "ack_generation", payload)
    _assert_code(error, code)
    assert ga_store.get_ga_job(job.id).version == job.version


def test_ga_execution_failure_and_startup_interrupt_are_scoped(ga_store) -> None:
    queued = _create(ga_store)
    paused = _transition(ga_store, _create(ga_store), "pause")
    running = _transition(ga_store, _create(ga_store), "claim")
    pausing = _transition(
        ga_store,
        _transition(ga_store, _create(ga_store), "claim"),
        "pause",
    )
    cancelling = _transition(ga_store, _create(ga_store), "claim")
    cancelling = _transition(ga_store, cancelling, "cancel")
    legacy = ga_store.create(kind="parameter_search", request=LegacyRequest())

    interrupted = ga_store.interrupt_ga_jobs()
    assert {job.id for job in interrupted} == {running.id, pausing.id, cancelling.id}
    assert all(job.status == GaJobStatus.FAILED for job in interrupted)
    assert {job.version for job in interrupted} == {3, 4}
    assert ga_store.get_ga_job(queued.id).status == GaJobStatus.QUEUED
    assert ga_store.get_ga_job(paused.id).status == GaJobStatus.PAUSED
    assert ga_store.get(legacy.id) == legacy
    assert {job.error for job in interrupted} == {"control_plane_interrupted"}


def test_ga_generation_zero_resume_then_pause_preserves_checkpoint(ga_store) -> None:
    job = _transition(ga_store, _create(ga_store, max_generations=2), "claim")
    job = _transition(ga_store, job, "pause")
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert job.status == GaJobStatus.PAUSED
    preserved = job.detached_copy()
    job = _transition(ga_store, job, "resume")
    assert job.status == GaJobStatus.QUEUED
    job = _transition(ga_store, job, "pause")
    assert job.status == GaJobStatus.PAUSED
    assert job.request == preserved.request
    assert job.completed_generation == preserved.completed_generation
    assert job.checkpoint == preserved.checkpoint


def test_sqlite_startup_interrupts_persisted_active_ga_only(tmp_path) -> None:
    path = tmp_path / "restart.sqlite"
    before = SqliteJobStore(path)
    queued = _create(before)
    paused = _transition(before, _create(before), "pause")
    running = _transition(before, _create(before), "claim")
    pausing = _transition(before, _create(before), "claim")
    pausing = _transition(before, pausing, "pause")
    cancelling = _transition(before, _create(before), "claim")
    cancelling = _transition(before, cancelling, "cancel")
    failed = _job_in_state(before, GaJobStatus.FAILED)
    cancelled = _job_in_state(before, GaJobStatus.CANCELLED)
    succeeded = _job_in_state(before, GaJobStatus.SUCCEEDED)
    legacy = before.create(kind="parameter_search", request=LegacyRequest())
    legacy = before.mark_running(legacy.id)
    active_before = {
        job.id: before.get_ga_job(job.id) for job in (running, pausing, cancelling)
    }

    after_restart = SqliteJobStore(path)
    interrupted = after_restart.interrupt_ga_jobs()
    assert {job.id for job in interrupted} == {running.id, pausing.id, cancelling.id}
    for job in interrupted:
        prior = active_before[job.id]
        assert prior is not None
        assert job.status == GaJobStatus.FAILED
        assert job.version == prior.version + 1
    assert after_restart.get_ga_job(queued.id) == queued
    assert after_restart.get_ga_job(paused.id) == paused
    assert after_restart.get_ga_job(failed.id) == failed
    assert after_restart.get_ga_job(cancelled.id) == cancelled
    assert after_restart.get_ga_job(succeeded.id) == succeeded
    assert after_restart.get(legacy.id) == legacy


def test_ga_policy_imports_no_storage_or_higher_layer() -> None:
    policy_path = Path(__file__).parents[2] / "src/control_plane/ga_lifecycle.py"
    tree = ast.parse(policy_path.read_text())
    imports = {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    imports.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    forbidden = (
        "sqlite",
        "sqlalchemy",
        "control_plane",
        "strategy",
        "strategies",
        "evaluator",
        "provider",
    )

    def is_forbidden(name: str) -> bool:
        return any(term in name.lower() for term in forbidden)

    assert is_forbidden("src.strategies.base")
    assert all(not is_forbidden(name) for name in imports)


def test_ga_explicit_failure_preserves_checkpoint_and_increments_once(ga_store) -> None:
    job = _transition(ga_store, _create(ga_store), "claim")
    job = _transition(
        ga_store,
        job,
        "ack_generation",
        {"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    failed = _transition(ga_store, job, "fail", {"error": "evaluation_failed"})
    assert (failed.status, failed.version, failed.completed_generation) == (
        GaJobStatus.FAILED,
        job.version + 1,
        0,
    )
    assert failed.checkpoint == job.checkpoint
    assert failed.error == "evaluation_failed"


def test_sqlite_ga_claim_is_atomic_across_independent_store_instances(tmp_path) -> None:
    path = tmp_path / "jobs.sqlite"
    first = SqliteJobStore(path)
    second = SqliteJobStore(path)
    job = _create(first)
    barrier = Barrier(2)

    def claim(store):
        barrier.wait()
        try:
            return store.transition_ga_job(job.id, "claim", 1).status
        except GaJobStoreError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(claim, (first, second)))
    assert outcomes.count(GaJobStatus.RUNNING) == 1
    assert outcomes.count("job_version_conflict") == 1
    stored = first.get_ga_job(job.id)
    assert stored is not None
    assert (stored.status, stored.version) == (GaJobStatus.RUNNING, 2)


def test_sqlite_ga_update_failure_after_write_rolls_back_and_reopens(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "jobs.sqlite"
    hub = ControlPlaneInvalidationHub()
    stream = hub.subscribe()
    store = SqliteJobStore(path, hub)
    job = _create(store)
    assert _next_event(stream)["revision"] == job.version
    original_write = jobs_module._write_ga_record
    writes: list[str] = []

    def write_then_fail(conn, updated, expected_version):
        original_write(conn, updated, expected_version)
        writes.append(updated.id)
        raise RuntimeError("fault after UPDATE before commit")

    monkeypatch.setattr(jobs_module, "_write_ga_record", write_then_fail)
    with pytest.raises(RuntimeError, match="after UPDATE"):
        store.transition_ga_job(job.id, "claim", 1)
    assert writes == [job.id]
    assert stream.next_frame(timeout=0) is None
    monkeypatch.setattr(jobs_module, "_write_ga_record", original_write)
    reopened = SqliteJobStore(path, hub)
    assert reopened.get_ga_job(job.id) == job
    claimed = reopened.transition_ga_job(job.id, "claim", 1)
    assert (claimed.status, claimed.version) == (GaJobStatus.RUNNING, 2)
    assert _next_event(stream)["revision"] == claimed.version
    hub.close()


def test_sqlite_bulk_interrupt_failure_rolls_back_prior_updates(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "jobs.sqlite"
    store = SqliteJobStore(path)
    running = _transition(store, _create(store), "claim")
    pausing = _transition(store, _create(store), "claim")
    pausing = _transition(store, pausing, "pause")
    before = {job.id: store.get_ga_job(job.id) for job in (running, pausing)}
    original_write = jobs_module._write_ga_record
    writes: list[str] = []

    def write_twice_then_fail(conn, updated, expected_version):
        original_write(conn, updated, expected_version)
        writes.append(updated.id)
        if len(writes) == 2:
            raise RuntimeError("fault after second UPDATE before commit")

    monkeypatch.setattr(jobs_module, "_write_ga_record", write_twice_then_fail)
    with pytest.raises(RuntimeError, match="second UPDATE"):
        store.interrupt_ga_jobs()
    assert set(writes) == {running.id, pausing.id}
    reopened = SqliteJobStore(path)
    assert {job.id: reopened.get_ga_job(job.id) for job in (running, pausing)} == before


def test_sqlite_old_schema_upgrades_additively_and_legacy_row_survives(
    tmp_path,
) -> None:
    path = tmp_path / "old.sqlite"
    request_json = '{"value":1}'
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            """CREATE TABLE control_plane_jobs (
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT,
                finished_at TEXT, request_json TEXT NOT NULL, result_json TEXT,
                error TEXT)"""
        )
        conn.execute(
            """INSERT INTO control_plane_jobs
               (id,kind,status,created_at,updated_at,request_json)
               VALUES ('legacy-id','backtest','QUEUED','2026-01-01','2026-01-01',?)""",
            (request_json,),
        )
        conn.commit()
    first = SqliteJobStore(path)
    legacy = first.get("legacy-id")
    assert legacy is not None
    assert legacy.request == {"value": 1}
    ga = _create(first)
    second = SqliteJobStore(path)
    assert second.get("legacy-id") == legacy
    assert second.get_ga_job(ga.id) == ga
    with closing(sqlite3.connect(path)) as conn:
        names = {
            row[1] for row in conn.execute("PRAGMA table_info(control_plane_jobs)")
        }
    assert {
        "ga_version",
        "ga_epoch_id",
        "ga_completed_generation",
        "ga_checkpoint_generation",
        "ga_retry_of_job_id",
    } <= names
