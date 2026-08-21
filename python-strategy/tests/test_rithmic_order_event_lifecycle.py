import threading

import pytest

from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)


def test_operation_gate_releases_after_failure() -> None:
    gate = RithmicOrderEventLifecycleGate()
    primary = RuntimeError("primary")

    def fail() -> None:
        raise primary

    with pytest.raises(RuntimeError) as caught:
        gate.run(fail)
    assert caught.value is primary

    assert gate.run(lambda: "next-operation") == "next-operation"


def test_operation_gate_allows_same_thread_reentry_while_blocking_other_threads() -> (
    None
):
    gate = RithmicOrderEventLifecycleGate()
    contender_attempting = threading.Event()
    contender_entered = threading.Event()
    contenders: list[threading.Thread] = []

    def contend() -> None:
        contender_attempting.set()
        gate.run(contender_entered.set)

    def outer_operation() -> None:
        contender = threading.Thread(target=contend)
        contenders.append(contender)
        contender.start()
        assert contender_attempting.wait(timeout=1.0)
        assert not contender_entered.wait(timeout=0.05)
        assert gate.run(lambda: "nested-operation") == "nested-operation"

    gate.run(outer_operation)

    assert contender_entered.wait(timeout=1.0)
    contenders[0].join(timeout=1.0)
    assert not contenders[0].is_alive()
