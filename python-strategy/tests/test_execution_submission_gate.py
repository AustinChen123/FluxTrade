from __future__ import annotations

import threading
import time
import sys
from unittest.mock import MagicMock

import pytest

from src.core.execution_submission_gate import ExecutionSubmissionGate


def _gate() -> tuple[ExecutionSubmissionGate, MagicMock]:
    logger = MagicMock()
    return (
        ExecutionSubmissionGate(
            lambda: logger.exception("Submission drain callback failed")
        ),
        logger,
    )


@pytest.mark.parametrize(
    ("kill_halted", "reconcile_halted", "expected"),
    [
        (False, False, None),
        (True, False, "kill_switch_halted"),
        (False, True, "reconcile_halted"),
        (True, True, "kill_switch_halted"),
    ],
)
def test_admission_matrix_preserves_independent_halts_and_reason_precedence(
    kill_halted: bool,
    reconcile_halted: bool,
    expected: str | None,
) -> None:
    gate, _logger = _gate()
    if reconcile_halted:
        gate.claim_reconcile_halt()
    if kill_halted:
        assert gate.halt_and_drain(timeout=0) is True

    assert gate.try_begin_submission() == expected

    assert gate.submissions_halted is kill_halted
    assert gate.reconcile_halted is reconcile_halted
    assert gate.in_flight == (1 if expected is None else 0)
    if expected is None:
        gate.finish_submission()
        assert gate.in_flight == 0


def test_resuming_each_halt_never_clears_the_other() -> None:
    gate, _logger = _gate()
    gate.halt_for_reconcile(timeout=0)
    gate.halt_and_drain(timeout=0)

    gate.resume_after_reconcile()
    assert gate.reconcile_halted is False
    assert gate.submissions_halted is True

    gate.halt_for_reconcile(timeout=0)
    gate.resume_submissions()
    assert gate.reconcile_halted is True
    assert gate.submissions_halted is False


def test_queued_callbacks_detach_only_on_zero_and_run_fifo_exactly_once() -> None:
    gate, _logger = _gate()
    assert gate.try_begin_submission() is None
    assert gate.try_begin_submission() is None
    calls: list[str] = []
    gate.run_when_submissions_drained(lambda: calls.append("first"))
    gate.run_when_submissions_drained(lambda: calls.append("second"))

    gate.finish_submission()
    assert calls == []

    gate.finish_submission()
    assert calls == ["first", "second"]
    assert gate.in_flight == 0


def test_immediate_callback_runs_outside_lock_and_propagates_original_error() -> None:
    gate, logger = _gate()
    observed: list[bool] = []

    def prove_lock_released() -> None:
        completed = threading.Event()
        thread = threading.Thread(
            target=lambda: (observed.append(gate.reconcile_halted), completed.set())
        )
        thread.start()
        assert completed.wait(timeout=1)
        thread.join(timeout=1)
        assert not thread.is_alive()

    gate.run_when_submissions_drained(prove_lock_released)
    assert observed == [False]

    sentinel = RuntimeError("immediate callback sentinel")
    with pytest.raises(RuntimeError) as raised:
        gate.run_when_submissions_drained(lambda: (_ for _ in ()).throw(sentinel))

    assert raised.value is sentinel
    logger.exception.assert_not_called()


def test_queued_callback_error_is_logged_and_does_not_skip_later_callbacks() -> None:
    logger = MagicMock()
    error_contexts: list[BaseException | None] = []
    logger.exception.side_effect = lambda *_args: error_contexts.append(sys.exception())
    gate = ExecutionSubmissionGate(
        lambda: logger.exception("Submission drain callback failed")
    )
    assert gate.try_begin_submission() is None
    sentinel = RuntimeError("queued callback sentinel")
    calls: list[str] = []
    gate.run_when_submissions_drained(lambda: (_ for _ in ()).throw(sentinel))
    gate.run_when_submissions_drained(lambda: calls.append("after-error"))

    gate.finish_submission()

    assert calls == ["after-error"]
    logger.exception.assert_called_once_with("Submission drain callback failed")
    assert error_contexts == [sentinel]


def test_queued_callback_runs_outside_lock_and_can_reenter_gate() -> None:
    gate, _logger = _gate()
    assert gate.try_begin_submission() is None
    completed = threading.Event()

    def callback() -> None:
        thread = threading.Thread(
            target=lambda: (gate.claim_reconcile_halt(), completed.set())
        )
        thread.start()
        assert completed.wait(timeout=1)
        thread.join(timeout=1)
        assert not thread.is_alive()

    gate.run_when_submissions_drained(callback)
    gate.finish_submission()

    assert gate.reconcile_halted is True


def test_matching_and_stale_tracked_reconcile_claims_are_consumed() -> None:
    gate, _logger = _gate()
    assert gate.halt_for_reconcile(timeout=0) is True
    first_generation = gate.generation

    gate.resume_after_reconcile()
    assert gate.reconcile_halted is False

    assert gate.halt_for_reconcile(timeout=0) is True
    tracked_generation = gate.generation
    raw_generation = gate.claim_reconcile_halt()
    assert raw_generation == tracked_generation + 1

    gate.resume_after_reconcile()
    assert gate.reconcile_halted is True
    assert gate.generation == raw_generation

    gate.resume_after_reconcile()
    assert gate.reconcile_halted is False
    assert first_generation < tracked_generation < raw_generation


def test_reconcile_claims_are_thread_local_and_newer_cross_thread_claim_wins() -> None:
    gate, _logger = _gate()
    first_claimed = threading.Event()
    second_claimed = threading.Event()
    resume_first = threading.Event()
    resume_second = threading.Event()
    first_resumed = threading.Event()
    second_resumed = threading.Event()

    def first_owner() -> None:
        gate.halt_for_reconcile(timeout=0)
        first_claimed.set()
        assert resume_first.wait(timeout=1)
        gate.resume_after_reconcile()
        first_resumed.set()

    def second_owner() -> None:
        assert first_claimed.wait(timeout=1)
        gate.halt_for_reconcile(timeout=0)
        second_claimed.set()
        assert resume_second.wait(timeout=1)
        gate.resume_after_reconcile()
        second_resumed.set()

    first_thread = threading.Thread(target=first_owner)
    second_thread = threading.Thread(target=second_owner)
    first_thread.start()
    second_thread.start()
    assert second_claimed.wait(timeout=1)
    newer_generation = gate.generation

    resume_first.set()
    assert first_resumed.wait(timeout=1)
    assert gate.reconcile_halted is True
    assert gate.generation == newer_generation

    resume_second.set()
    assert second_resumed.wait(timeout=1)
    first_thread.join(timeout=1)
    second_thread.join(timeout=1)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert gate.reconcile_halted is False


def test_tracked_then_raw_safety_claim_advances_two_generations() -> None:
    gate, _logger = _gate()
    initial_generation = gate.generation

    gate.halt_for_reconcile(timeout=0)
    gate.claim_reconcile_halt()

    assert gate.generation == initial_generation + 2
    gate.resume_after_reconcile()
    assert gate.reconcile_halted is True


def test_authoritative_begin_rejects_existing_halts_without_mutation() -> None:
    kill_gate, _logger = _gate()
    kill_gate.halt_and_drain(timeout=0)
    kill_generation = kill_gate.generation
    assert kill_gate.begin_authoritative_exit(timeout=0) is None
    assert kill_gate.generation == kill_generation
    assert kill_gate.in_flight == 0

    reconcile_gate, _logger = _gate()
    reconcile_gate.claim_reconcile_halt()
    reconcile_generation = reconcile_gate.generation
    assert reconcile_gate.begin_authoritative_exit(timeout=0) is None
    assert reconcile_gate.generation == reconcile_generation
    assert reconcile_gate.in_flight == 0


def test_authoritative_timeout_preserves_reconcile_halt() -> None:
    gate, _logger = _gate()
    assert gate.try_begin_submission() is None

    assert gate.begin_authoritative_exit(timeout=0) is None

    assert gate.reconcile_halted is True
    assert gate.in_flight == 1
    gate.finish_submission()


def test_kill_raised_while_authoritative_begin_waits_preserves_both_halts() -> None:
    gate, _logger = _gate()
    assert gate.try_begin_submission() is None
    result: list[int | None] = []
    started_waiting = threading.Event()

    def begin() -> None:
        started_waiting.set()
        result.append(gate.begin_authoritative_exit(timeout=1))

    thread = threading.Thread(target=begin)
    thread.start()
    assert started_waiting.wait(timeout=1)
    deadline = time.monotonic() + 1
    while not gate.reconcile_halted and time.monotonic() < deadline:
        time.sleep(0.001)
    assert gate.reconcile_halted is True
    assert gate.halt_and_drain(timeout=0) is False
    gate.finish_submission()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert result == [None]
    assert gate.submissions_halted is True
    assert gate.reconcile_halted is True
    assert gate.in_flight == 0


@pytest.mark.parametrize("resume_after_reconcile", [False, True])
def test_authoritative_finish_obeys_explicit_resume_disposition(
    resume_after_reconcile: bool,
) -> None:
    gate, _logger = _gate()
    generation = gate.begin_authoritative_exit(timeout=0)
    assert generation is not None
    assert gate.in_flight == 1
    assert gate.authoritative_exit_active is True
    assert gate.begin_authoritative_exit(timeout=0) is None

    gate.finish_authoritative_exit(
        resume_after_reconcile=resume_after_reconcile,
        reconcile_generation=generation,
    )

    assert gate.in_flight == 0
    assert gate.reconcile_halted is (not resume_after_reconcile)


@pytest.mark.parametrize("resume_after_reconcile", [False, True])
def test_authoritative_finish_drains_queued_callbacks_with_error_context(
    resume_after_reconcile: bool,
) -> None:
    logger = MagicMock()
    error_contexts: list[BaseException | None] = []
    logger.exception.side_effect = lambda *_args: error_contexts.append(sys.exception())
    gate = ExecutionSubmissionGate(
        lambda: logger.exception("Submission drain callback failed")
    )
    generation = gate.begin_authoritative_exit(timeout=0)
    assert generation is not None
    sentinel = RuntimeError("authoritative callback sentinel")
    calls: list[str] = []
    gate.run_when_submissions_drained(lambda: (_ for _ in ()).throw(sentinel))
    gate.run_when_submissions_drained(lambda: calls.append("after-error"))

    gate.finish_authoritative_exit(
        resume_after_reconcile=resume_after_reconcile,
        reconcile_generation=generation,
    )

    assert calls == ["after-error"]
    assert error_contexts == [sentinel]
    logger.exception.assert_called_once_with("Submission drain callback failed")
    assert gate.in_flight == 0
    assert gate.reconcile_halted is (not resume_after_reconcile)


def test_authoritative_finish_never_clears_newer_generation_or_kill_halt() -> None:
    gate, _logger = _gate()
    generation = gate.begin_authoritative_exit(timeout=0)
    assert generation is not None
    newer_generation = gate.claim_reconcile_halt()
    assert gate.halt_and_drain(timeout=0) is False

    gate.finish_authoritative_exit(
        resume_after_reconcile=True,
        reconcile_generation=generation,
    )

    assert gate.in_flight == 0
    assert gate.submissions_halted is True
    assert gate.reconcile_halted is True
    assert gate.generation == newer_generation
