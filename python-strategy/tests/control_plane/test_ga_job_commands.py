from __future__ import annotations

import copy
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from threading import Barrier
from typing import Any

import pytest

import src.control_plane.jobs as jobs_module
from src.control_plane.ga_lifecycle import GaJobStatus, GaJobStoreError
from src.control_plane.jobs import InMemoryJobStore, SqliteJobStore


@pytest.fixture(params=["memory", "sqlite"])
def command_store(request, tmp_path):
    return (
        InMemoryJobStore()
        if request.param == "memory"
        else SqliteJobStore(tmp_path / "ga-commands.sqlite")
    )


def _compiled_request(seed: int = 17, epoch_id: str | None = None) -> dict[str, Any]:
    return {
        "kind": "parameter_search",
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "start_time": 1_704_067_200_000,
        "end_time": 1_704_067_800_000,
        "market_data": {"kind": "sealed_dataset", "dataset_id": "sealed-a"},
        "objective": "maximize_score",
        "seed": seed,
        "backtest": {
            "initial_balance": "10000.123456789012345678901234",
            "maker_fee": "0.000001",
            "taker_fee": "0.000123",
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        },
        "search_space": {"parameters": {"quantity": {"min": "0.01"}}},
        "evolution": {
            "epoch_id": epoch_id,
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


def _submit(store, *, actor="operator-a", key="key-1", request=None):
    return store.submit_ga_job_command(
        actor=actor,
        idempotency_key=key,
        request=_compiled_request() if request is None else request,
    )


def _transition(store, job_id, action, version, *, actor="operator-a", key="key-1"):
    return store.transition_ga_job_command(
        actor=actor,
        idempotency_key=key,
        job_id=job_id,
        action=action,
        expected_version=version,
    )


def _assert_code(error: pytest.ExceptionInfo, code: str) -> None:
    assert isinstance(error.value, GaJobStoreError)
    assert error.value.code == code


def test_submit_replay_returns_original_detached_receipt(command_store):
    request = _compiled_request()
    first, replayed = _submit(command_store, request=request)
    assert not replayed
    assert first.status == GaJobStatus.QUEUED and first.version == 1
    assert first.epoch_id and first.request["evolution"]["epoch_id"] == first.epoch_id
    assert first.request["ga_binding"] == request["ga_binding"]
    assert request["evolution"]["epoch_id"] is None
    expected_receipt = first.detached_copy()

    progressed = command_store.transition_ga_job(first.id, "claim", first.version)
    assert progressed.status == GaJobStatus.RUNNING
    first.request["ga_binding"]["input_digest"] = "mutated"
    replay, was_replayed = _submit(command_store, request=_compiled_request())
    assert was_replayed
    assert replay == expected_receipt
    assert replay.request["ga_binding"] == request["ga_binding"]
    assert command_store.get_ga_job(first.id) == progressed
    assert len(command_store.list_ga_jobs()) == 1


def test_submit_fingerprint_ignores_only_generated_epoch_and_conflicts_on_intent(
    command_store,
):
    first, _ = _submit(command_store, request=_compiled_request(epoch_id="generated-a"))
    replay, was_replayed = _submit(
        command_store, request=_compiled_request(epoch_id="generated-b")
    )
    assert was_replayed and replay.id == first.id

    changed = _compiled_request(epoch_id="generated-c")
    changed["backtest"]["initial_balance"] = "10000.123456789012345678901235"
    with pytest.raises(GaJobStoreError) as error:
        _submit(command_store, request=changed)
    _assert_code(error, "idempotency_conflict")
    assert len(command_store.list_ga_jobs()) == 1


def test_actor_key_namespace_covers_operations_and_allows_other_actors(command_store):
    submitted, _ = _submit(command_store)
    other_actor, replayed = _submit(command_store, actor="operator-b")
    assert not replayed and other_actor.id != submitted.id

    with pytest.raises(GaJobStoreError) as error:
        _transition(command_store, submitted.id, "pause", 1)
    _assert_code(error, "idempotency_conflict")
    assert command_store.get_ga_job(submitted.id) == submitted


def test_action_replay_returns_original_snapshot_and_conflicts_before_state_checks(
    command_store,
):
    job, _ = _submit(command_store, key="submit")
    paused, replayed = _transition(
        command_store, job.id, "pause", job.version, key="action"
    )
    assert not replayed and paused.status == GaJobStatus.PAUSED
    resumed = command_store.transition_ga_job(job.id, "resume", paused.version)
    replay, was_replayed = _transition(
        command_store, job.id, "pause", job.version, key="action"
    )
    assert was_replayed and replay == paused
    assert command_store.get_ga_job(job.id) == resumed

    with pytest.raises(GaJobStoreError) as error:
        _transition(command_store, job.id, "cancel", 99, key="action")
    _assert_code(error, "idempotency_conflict")


@pytest.mark.parametrize(
    ("changed_target", "changed_version"),
    [(True, False), (False, True)],
)
def test_action_receipt_conflicts_on_target_or_version(
    command_store, changed_target, changed_version
):
    original, _ = _submit(command_store, key="submit-original")
    other, _ = _submit(command_store, key="submit-other")
    accepted, replayed = _transition(
        command_store, original.id, "pause", original.version, key="one-action"
    )
    assert not replayed and accepted.status == GaJobStatus.PAUSED
    with pytest.raises(GaJobStoreError) as error:
        _transition(
            command_store,
            other.id if changed_target else original.id,
            "pause",
            original.version + 1 if changed_version else original.version,
            key="one-action",
        )
    _assert_code(error, "idempotency_conflict")
    assert command_store.get_ga_job(original.id) == accepted


@pytest.mark.parametrize(
    ("action", "start_action", "expected_status"),
    [
        ("pause", None, GaJobStatus.PAUSED),
        ("resume", "pause", GaJobStatus.QUEUED),
        ("cancel", None, GaJobStatus.CANCELLED),
        ("cancel", "claim", GaJobStatus.CANCELLING),
    ],
)
def test_command_transition_actions_use_existing_state_policy(
    command_store, action, start_action, expected_status
):
    job, _ = _submit(command_store)
    if start_action is not None:
        job = command_store.transition_ga_job(job.id, start_action, job.version)
    updated, replayed = _transition(
        command_store,
        job.id,
        action,
        job.version,
        key=f"action-{action}-{start_action}",
    )
    assert not replayed
    assert updated.status == expected_status
    assert updated.version == job.version + 1


@pytest.mark.parametrize(
    ("actor", "key"),
    [
        ("", "key"),
        ("  ", "key"),
        ("operator", ""),
        ("operator", "has space"),
        ("operator", "x" * 129),
        ("operator", "non-ascii-☃"),
    ],
)
def test_command_identity_rejects_invalid_actor_or_idempotency_key(
    command_store, actor, key
):
    with pytest.raises(GaJobStoreError) as error:
        _submit(command_store, actor=actor, key=key)
    _assert_code(error, "validation_error")
    assert command_store.list_ga_jobs() == []


def test_fresh_action_error_precedence_and_rejected_key_reuse(command_store):
    with pytest.raises(GaJobStoreError) as error:
        _transition(command_store, "missing", "pause", True, key="reusable")
    _assert_code(error, "ga_job_not_found")

    job, _ = _submit(command_store, key="submit")
    with pytest.raises(GaJobStoreError) as error:
        _transition(command_store, job.id, "pause", True, key="reusable")
    _assert_code(error, "validation_error")
    with pytest.raises(GaJobStoreError) as error:
        _transition(command_store, job.id, "pause", 99, key="reusable")
    _assert_code(error, "job_version_conflict")

    paused, replayed = _transition(
        command_store, job.id, "pause", job.version, key="reusable"
    )
    assert not replayed and paused.status == GaJobStatus.PAUSED


@pytest.mark.parametrize(
    ("terminal_action", "terminal_status"),
    [
        ("cancel", GaJobStatus.CANCELLED),
        ("fail", GaJobStatus.FAILED),
        ("success", GaJobStatus.SUCCEEDED),
    ],
)
def test_terminal_retry_preserves_source_and_only_rebinds_epoch(
    command_store, terminal_action, terminal_status
):
    source, _ = _submit(command_store, key="submit")
    if terminal_action == "cancel":
        source = command_store.transition_ga_job(source.id, "cancel", source.version)
    elif terminal_action == "fail":
        running = command_store.transition_ga_job(source.id, "claim", source.version)
        source = command_store.transition_ga_job(
            source.id, "fail", running.version, {"error": "fixture-failure"}
        )
    else:
        running = command_store.transition_ga_job(source.id, "claim", source.version)
        acknowledged = command_store.transition_ga_job(
            source.id,
            "ack_generation",
            running.version,
            {"completed_generation": 0, "checkpoint_epoch_id": source.epoch_id},
        )
        source = command_store.transition_ga_job(
            source.id,
            "ack_generation",
            acknowledged.version,
            {"completed_generation": 1, "checkpoint_epoch_id": source.epoch_id},
        )
    assert source.status == terminal_status
    unchanged = source.detached_copy()

    retried, replayed = command_store.retry_ga_job_command(
        actor="operator-a",
        idempotency_key="retry-1",
        job_id=source.id,
        expected_version=source.version,
    )
    assert not replayed
    assert retried.id != source.id and retried.retry_of_job_id == source.id
    assert retried.epoch_id != source.epoch_id
    assert retried.status == GaJobStatus.QUEUED
    assert retried.version == 1 and retried.completed_generation == -1
    assert retried.checkpoint is None
    expected_request = copy.deepcopy(unchanged.request)
    expected_request["evolution"]["epoch_id"] = retried.epoch_id
    assert retried.request == expected_request
    assert command_store.get_ga_job(source.id) == unchanged

    advanced = command_store.transition_ga_job(retried.id, "claim", retried.version)
    replay, was_replayed = command_store.retry_ga_job_command(
        actor="operator-a",
        idempotency_key="retry-1",
        job_id=source.id,
        expected_version=source.version,
    )
    assert was_replayed and replay == retried
    assert command_store.get_ga_job(retried.id) == advanced
    assert command_store.get_ga_job(source.id) == unchanged


def test_retry_rejection_does_not_consume_key_and_version_precedes_state(command_store):
    job, _ = _submit(command_store, key="submit")
    with pytest.raises(GaJobStoreError) as error:
        command_store.retry_ga_job_command(
            actor="operator-a",
            idempotency_key="retry-reuse",
            job_id=job.id,
            expected_version=job.version + 1,
        )
    _assert_code(error, "job_version_conflict")
    with pytest.raises(GaJobStoreError) as error:
        command_store.retry_ga_job_command(
            actor="operator-a",
            idempotency_key="retry-reuse",
            job_id=job.id,
            expected_version=job.version,
        )
    _assert_code(error, "job_transition_invalid")
    with pytest.raises(GaJobStoreError) as error:
        command_store.retry_ga_job_command(
            actor="operator-a",
            idempotency_key="retry-reuse",
            job_id=job.id,
            expected_version="bad",
        )
    _assert_code(error, "validation_error")
    failed = command_store.transition_ga_job(job.id, "claim", job.version)
    failed = command_store.transition_ga_job(
        job.id, "fail", failed.version, {"error": "terminal"}
    )
    retried, replayed = command_store.retry_ga_job_command(
        actor="operator-a",
        idempotency_key="retry-reuse",
        job_id=job.id,
        expected_version=failed.version,
    )
    assert not replayed and retried.retry_of_job_id == failed.id


@pytest.mark.parametrize(
    "state",
    [
        GaJobStatus.RUNNING,
        GaJobStatus.PAUSING,
        GaJobStatus.PAUSED,
        GaJobStatus.CANCELLING,
    ],
)
def test_terminal_retry_rejects_additional_nonterminal_states_without_consuming_key(
    command_store, state
):
    job, _ = _submit(command_store)
    job = command_store.transition_ga_job(job.id, "claim", job.version)
    if state == GaJobStatus.PAUSING:
        job = command_store.transition_ga_job(job.id, "pause", job.version)
    elif state == GaJobStatus.PAUSED:
        job = command_store.transition_ga_job(job.id, "pause", job.version)
        job = command_store.transition_ga_job(
            job.id,
            "ack_generation",
            job.version,
            {"completed_generation": 0, "checkpoint_epoch_id": job.epoch_id},
        )
    elif state == GaJobStatus.CANCELLING:
        job = command_store.transition_ga_job(job.id, "cancel", job.version)
    elif state == GaJobStatus.RUNNING:
        job = command_store.transition_ga_job(
            job.id,
            "ack_generation",
            job.version,
            {"completed_generation": 0, "checkpoint_epoch_id": job.epoch_id},
        )
    assert job.status == state
    before = command_store.get_ga_job(job.id)
    assert before == job

    with pytest.raises(GaJobStoreError) as error:
        command_store.retry_ga_job_command(
            actor="operator-a",
            idempotency_key="retry-nonterminal",
            job_id=job.id,
            expected_version=job.version,
        )
    _assert_code(error, "job_transition_invalid")
    assert command_store.get_ga_job(job.id) == before
    assert len(command_store.list_ga_jobs()) == 1

    if state == GaJobStatus.PAUSED:
        terminal = command_store.transition_ga_job(job.id, "cancel", job.version)
    else:
        terminal = command_store.transition_ga_job(
            job.id, "fail", job.version, {"error": "terminal-test"}
        )
    retried, replayed = command_store.retry_ga_job_command(
        actor="operator-a",
        idempotency_key="retry-nonterminal",
        job_id=job.id,
        expected_version=terminal.version,
    )
    assert not replayed and retried.retry_of_job_id == job.id
    assert len(command_store.list_ga_jobs()) == 2


def test_sqlite_receipts_survive_reopen_and_replay_original_response(tmp_path):
    path = tmp_path / "durable-ga-commands.sqlite"
    first_store = SqliteJobStore(path)
    original, replayed = _submit(first_store)
    assert not replayed
    advanced = first_store.transition_ga_job(original.id, "claim", original.version)

    reopened = SqliteJobStore(path)
    replay, was_replayed = _submit(reopened)
    assert was_replayed and replay == original
    assert reopened.get_ga_job(original.id) == advanced


def test_sqlite_same_key_race_has_one_submit_and_one_original_receipt(tmp_path):
    path = tmp_path / "same-key-race.sqlite"
    stores = (SqliteJobStore(path), SqliteJobStore(path))
    barrier = Barrier(2)

    def submit(store):
        barrier.wait()
        return _submit(store, key="racing-key")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, stores))
    assert results[0][0] == results[1][0]
    assert sorted(replayed for _, replayed in results) == [False, True]
    assert len(stores[0].list_ga_jobs()) == 1


def test_sqlite_conflicting_intent_race_has_one_winner_and_one_conflict(tmp_path):
    path = tmp_path / "conflicting-key-race.sqlite"
    stores = (SqliteJobStore(path), SqliteJobStore(path))
    barrier = Barrier(2)

    def submit(pair):
        store, seed = pair
        barrier.wait()
        try:
            record, replayed = _submit(
                store, key="one-key", request=_compiled_request(seed=seed)
            )
            return record.id, replayed
        except GaJobStoreError as error:
            return error.code, False

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(submit, ((stores[0], 17), (stores[1], 18))))
    assert sum(value == "idempotency_conflict" for value, _ in outcomes) == 1
    assert sum(value != "idempotency_conflict" for value, _ in outcomes) == 1
    assert len(stores[0].list_ga_jobs()) == 1


def test_sqlite_failure_after_job_and_receipt_writes_rolls_back_and_reuses_key(
    tmp_path, monkeypatch: pytest.MonkeyPatch
):
    path = tmp_path / "receipt-rollback.sqlite"
    store = SqliteJobStore(path)
    original_insert = jobs_module._insert_ga_command_receipt
    writes: list[str] = []

    def insert_then_fail(conn, actor, key, fingerprint, record):
        original_insert(conn, actor, key, fingerprint, record)
        writes.append(key)
        raise RuntimeError("fault after job and receipt writes before commit")

    monkeypatch.setattr(jobs_module, "_insert_ga_command_receipt", insert_then_fail)
    with pytest.raises(RuntimeError, match="after job and receipt writes"):
        _submit(store, key="rollback-key")
    assert writes == ["rollback-key"]

    with closing(sqlite3.connect(path)) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM control_plane_jobs WHERE kind='ga'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM control_plane_ga_command_receipts"
            ).fetchone()[0]
            == 0
        )

    monkeypatch.setattr(jobs_module, "_insert_ga_command_receipt", original_insert)
    reopened = SqliteJobStore(path)
    created, replayed = _submit(reopened, key="rollback-key")
    assert not replayed and created.status == GaJobStatus.QUEUED
    assert len(reopened.list_ga_jobs()) == 1
