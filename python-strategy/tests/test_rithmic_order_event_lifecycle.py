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
