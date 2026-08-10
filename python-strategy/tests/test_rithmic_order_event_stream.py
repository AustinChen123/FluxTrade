from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.core.adapters.rithmic_order_event_stream import (
    RithmicOrderEventStreamService,
)
from src.core.adapters.rithmic_adapter import RithmicUnmappedOrderEvent


def _service(adapter, **overrides):
    dependencies = {
        "adapter": adapter,
        "stop_event": MagicMock(),
        "is_running": MagicMock(side_effect=[True, False]),
        "publish_worker": MagicMock(),
        "reconcile_if_needed": MagicMock(return_value=True),
        "process_event": MagicMock(return_value={"action": "updated"}),
        "lockdown": MagicMock(),
        "assert_runtime_leadership": MagicMock(),
        "halt_submissions": MagicMock(),
        "on_runtime_started": MagicMock(),
        "logger": MagicMock(),
    }
    dependencies["stop_event"].is_set.return_value = False
    dependencies.update(overrides)
    return RithmicOrderEventStreamService(**dependencies), dependencies


def test_start_failure_halts_and_preserves_original_exception() -> None:
    error = RuntimeError("start failed")
    adapter = MagicMock()
    adapter.start_order_event_stream.side_effect = error
    owner, dependencies = _service(adapter)

    with pytest.raises(RuntimeError) as raised:
        owner.start()

    assert raised.value is error
    dependencies["halt_submissions"].assert_called_once_with()
    dependencies["on_runtime_started"].assert_not_called()
    dependencies["publish_worker"].assert_not_called()


def test_start_publishes_current_worker_before_starting_it() -> None:
    calls: list[str] = []
    adapter = MagicMock()
    adapter.start_order_event_stream.side_effect = lambda: calls.append("adapter_start")
    stop_event = MagicMock()
    stop_event.clear.side_effect = lambda: calls.append("clear_stop")

    class Worker:
        def __init__(self, *, target, name, daemon):
            calls.append("construct_worker")

        def start(self):
            calls.append("start_worker")

    owner, _ = _service(
        adapter,
        stop_event=stop_event,
        on_runtime_started=MagicMock(
            side_effect=lambda: calls.append("runtime_started")
        ),
        publish_worker=MagicMock(
            side_effect=lambda worker: calls.append("publish_worker")
        ),
    )

    with patch(
        "src.core.adapters.rithmic_order_event_stream.threading.Thread",
        Worker,
    ):
        owner.start()

    assert calls == [
        "adapter_start",
        "runtime_started",
        "clear_stop",
        "construct_worker",
        "publish_worker",
        "start_worker",
    ]


def test_unresolved_reconnect_waits_without_polling() -> None:
    adapter = MagicMock()
    owner, dependencies = _service(
        adapter,
        reconcile_if_needed=MagicMock(return_value=False),
    )

    owner._run()

    adapter.poll_order_event.assert_not_called()
    dependencies["process_event"].assert_not_called()
    dependencies["stop_event"].wait.assert_called_once_with(1.0)


def test_empty_poll_waits_without_processing() -> None:
    adapter = MagicMock()
    adapter.poll_order_event.return_value = None
    owner, dependencies = _service(adapter)

    owner._run()

    dependencies["process_event"].assert_not_called()
    dependencies["stop_event"].wait.assert_called_once_with(0.05)


def test_applied_event_completes_without_lockdown() -> None:
    event = SimpleNamespace(product_id="RITHMIC:NQ-202609")
    adapter = MagicMock()
    adapter.poll_order_event.return_value = event
    owner, dependencies = _service(
        adapter,
        process_event=MagicMock(return_value={"action": "applied"}),
    )

    owner._run()

    dependencies["process_event"].assert_called_once_with(event)
    dependencies["lockdown"].assert_not_called()
    dependencies["halt_submissions"].assert_not_called()


@pytest.mark.parametrize(
    ("leadership", "expected_poll_calls", "expected_process_calls"),
    [
        ([RuntimeError("before reconnect")], 0, 0),
        ([None, RuntimeError("after poll")], 1, 0),
        ([None, None, RuntimeError("after process")], 1, 1),
    ],
    ids=["before-reconnect", "after-poll", "after-process"],
)
def test_leadership_fences_stop_before_the_next_side_effect(
    leadership,
    expected_poll_calls,
    expected_process_calls,
) -> None:
    event = SimpleNamespace(product_id="RITHMIC:NQ-202609")
    adapter = MagicMock()
    adapter.poll_order_event.return_value = event
    owner, dependencies = _service(
        adapter,
        assert_runtime_leadership=MagicMock(side_effect=leadership),
        process_event=MagicMock(return_value={"action": "unresolved"}),
    )

    owner._run()

    assert adapter.poll_order_event.call_count == expected_poll_calls
    assert dependencies["process_event"].call_count == expected_process_calls
    dependencies["lockdown"].assert_not_called()
    dependencies["halt_submissions"].assert_called_once_with()


def test_unknown_order_enters_drift_owner_and_worker_continues() -> None:
    event = SimpleNamespace(
        product_id="RITHMIC:NQ-202609",
        client_order_id="manual-order",
        exchange_order_id="basket-manual",
        raw={"account_id": "ACCOUNT", "exchange": "CME", "symbol": "NQU6"},
    )
    adapter = MagicMock()
    adapter.poll_order_event.return_value = event
    owner, dependencies = _service(
        adapter,
        process_event=MagicMock(return_value={"action": "unknown_order"}),
    )

    owner._run()

    dependencies["lockdown"].assert_called_once_with(
        "rithmic_external_order_detected: account_id=ACCOUNT exchange=CME "
        "symbol=NQU6 client_order_id=manual-order "
        "exchange_order_id=basket-manual"
    )
    dependencies["halt_submissions"].assert_not_called()


def test_unsafe_action_enters_drift_owner_with_exact_identity() -> None:
    event = SimpleNamespace(
        product_id="RITHMIC:NQ-202609",
        client_order_id="owned-order",
        exchange_order_id="basket-1",
        raw=None,
    )
    adapter = MagicMock()
    adapter.poll_order_event.return_value = event
    owner, dependencies = _service(
        adapter,
        process_event=MagicMock(
            return_value={"action": "unresolved_missing_fill_price"}
        ),
    )

    owner._run()

    dependencies["lockdown"].assert_called_once_with(
        "rithmic_order_event_requires_reconciliation: "
        "action=unresolved_missing_fill_price product_id=RITHMIC:NQ-202609 "
        "client_order_id=owned-order exchange_order_id=basket-1"
    )


def test_unmapped_event_enters_external_order_drift_and_continues() -> None:
    adapter = MagicMock()
    adapter.poll_order_event.side_effect = RithmicUnmappedOrderEvent(
        account_id="ACCOUNT",
        exchange="CME",
        symbol="ESZ6",
    )
    owner, dependencies = _service(adapter)

    owner._run()

    dependencies["lockdown"].assert_called_once_with(
        "rithmic_external_order_detected: account_id=ACCOUNT exchange=CME "
        "symbol=ESZ6 client_order_id=unknown exchange_order_id=unknown"
    )
    dependencies["halt_submissions"].assert_not_called()


def test_leadership_loss_during_unmapped_handling_stops_without_lockdown() -> None:
    adapter = MagicMock()
    adapter.poll_order_event.side_effect = RithmicUnmappedOrderEvent(
        account_id="ACCOUNT",
        exchange="CME",
        symbol="ESZ6",
    )
    leadership = MagicMock(side_effect=[None, RuntimeError("leadership lost")])
    owner, dependencies = _service(
        adapter,
        assert_runtime_leadership=leadership,
    )

    owner._run()

    dependencies["lockdown"].assert_not_called()
    dependencies["halt_submissions"].assert_not_called()


def test_terminal_loop_failure_logs_once_halts_and_stops_worker() -> None:
    adapter = MagicMock()
    adapter.poll_order_event.side_effect = RuntimeError("invalid event")
    owner, dependencies = _service(adapter)

    owner._run()

    dependencies["logger"].exception.assert_called_once_with(
        "Exchange order event stream failed; submissions remain halted"
    )
    dependencies["halt_submissions"].assert_called_once_with()
