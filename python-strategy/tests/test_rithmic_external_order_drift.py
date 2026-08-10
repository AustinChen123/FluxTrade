from unittest.mock import MagicMock

import pytest

from src.core.adapters.rithmic_external_order_drift import (
    RithmicExternalOrderDriftService,
)


def _service(**overrides):
    dependencies = {
        "halt_submissions": MagicMock(),
        "clear_local_halt": MagicMock(),
        "persist_lockdown_state": MagicMock(),
        "persist_redis_lockdown": MagicMock(),
        "assert_runtime_leadership": MagicMock(),
        "resume_after_reconcile": MagicMock(),
        "logger": MagicMock(),
    }
    dependencies.update(overrides)
    return RithmicExternalOrderDriftService(**dependencies), dependencies


def test_detection_publishes_generation_and_halt_atomically() -> None:
    owner_ref: list[RithmicExternalOrderDriftService] = []

    def halt_while_locked() -> None:
        owner = owner_ref[0]
        acquired = owner._lock.acquire(blocking=False)
        if acquired:
            owner._lock.release()
            pytest.fail("drift generation published outside the submission halt lock")

    owner, dependencies = _service(
        halt_submissions=MagicMock(side_effect=halt_while_locked)
    )
    owner_ref.append(owner)

    owner.detect("first")
    owner.detect("second")

    assert owner.pending is True
    assert owner.current_generation() == 2
    assert dependencies["halt_submissions"].call_count == 2
    dependencies["persist_lockdown_state"].assert_called_once_with("first")
    dependencies["persist_redis_lockdown"].assert_called_once_with()


def test_persistence_failure_never_releases_local_halt() -> None:
    owner, dependencies = _service(
        persist_lockdown_state=MagicMock(side_effect=RuntimeError("database down")),
        persist_redis_lockdown=MagicMock(side_effect=RuntimeError("redis down")),
    )

    owner.detect("external order")

    assert owner.pending is True
    assert owner.current_generation() == 1
    dependencies["halt_submissions"].assert_called_once_with()
    dependencies["clear_local_halt"].assert_not_called()
    assert dependencies["logger"].exception.call_count == 2


@pytest.mark.parametrize("clear_succeeded", [False, True])
def test_generation_advance_during_clear_reasserts_lockdown(
    clear_succeeded: bool,
) -> None:
    owner, dependencies = _service()
    owner.detect("before clear")
    prepared_generation = owner.current_generation()
    for dependency in (
        "halt_submissions",
        "persist_lockdown_state",
        "persist_redis_lockdown",
    ):
        dependencies[dependency].reset_mock()

    owner.detect("during clear")
    dependencies["halt_submissions"].reset_mock()
    owner.finalize_clear(
        prepared_generation=prepared_generation,
        clear_succeeded=clear_succeeded,
    )

    assert owner.pending is True
    dependencies["clear_local_halt"].assert_not_called()
    dependencies["halt_submissions"].assert_called_once_with()
    dependencies["persist_lockdown_state"].assert_called_once_with(
        "rithmic_external_order_detected_during_clear"
    )
    dependencies["persist_redis_lockdown"].assert_called_once_with()
    dependencies["assert_runtime_leadership"].assert_called_once_with()
    dependencies["resume_after_reconcile"].assert_called_once_with()


@pytest.mark.parametrize(
    ("clear_succeeded", "expected_pending", "expected_clear_calls"),
    [(False, True, 0), (True, False, 1)],
)
def test_unchanged_generation_preserves_exact_clear_decision(
    clear_succeeded: bool,
    expected_pending: bool,
    expected_clear_calls: int,
) -> None:
    owner, dependencies = _service()
    owner.detect("before clear")
    prepared_generation = owner.current_generation()
    for dependency in (
        "halt_submissions",
        "persist_lockdown_state",
        "persist_redis_lockdown",
    ):
        dependencies[dependency].reset_mock()

    owner.finalize_clear(
        prepared_generation=prepared_generation,
        clear_succeeded=clear_succeeded,
    )

    assert owner.pending is expected_pending
    assert dependencies["clear_local_halt"].call_count == expected_clear_calls
    dependencies["halt_submissions"].assert_not_called()
    dependencies["persist_lockdown_state"].assert_not_called()
    dependencies["persist_redis_lockdown"].assert_not_called()
    assert dependencies["assert_runtime_leadership"].call_count == (
        2 if clear_succeeded else 1
    )
    dependencies["resume_after_reconcile"].assert_called_once_with()


def test_leadership_loss_cannot_clear_pending_or_release_reconcile_gate() -> None:
    owner, dependencies = _service()
    owner.detect("before clear")
    prepared_generation = owner.current_generation()
    dependencies["assert_runtime_leadership"].side_effect = RuntimeError(
        "leadership lost"
    )

    with pytest.raises(RuntimeError, match="leadership lost"):
        owner.finalize_clear(
            prepared_generation=prepared_generation,
            clear_succeeded=True,
        )

    assert owner.pending is True
    dependencies["assert_runtime_leadership"].assert_called_once_with()
    dependencies["clear_local_halt"].assert_not_called()
    dependencies["resume_after_reconcile"].assert_not_called()
