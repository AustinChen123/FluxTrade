import threading
from unittest.mock import MagicMock, call

from src.core.strategy_command_listener import build_strategy_command_listener


class _FakePubSub:
    def __init__(
        self,
        messages: list[dict[str, object] | None],
        running: threading.Event,
    ) -> None:
        self.messages = messages
        self.running = running
        self.calls: list[tuple[str, object]] = []

    def subscribe(self, channel: str) -> None:
        self.calls.append(("subscribe", channel))

    def get_message(self, *, timeout: float) -> dict[str, object] | None:
        self.calls.append(("get_message", timeout))
        message = self.messages.pop(0)
        if not self.messages:
            self.running.clear()
        return message

    def close(self) -> None:
        self.calls.append(("close", None))


def _start_with_messages(
    messages: list[dict[str, object] | None],
    *,
    leadership: MagicMock | None = None,
    submit: MagicMock | None = None,
    event_logger: MagicMock | None = None,
) -> tuple[_FakePubSub, MagicMock, MagicMock, MagicMock, threading.Thread]:
    running = threading.Event()
    running.set()
    pubsub = _FakePubSub(messages, running)
    if leadership is None:
        leadership = MagicMock()
    if submit is None:
        submit = MagicMock()
    if event_logger is None:
        event_logger = MagicMock()
    worker_thread_ids: list[int] = []

    def pubsub_factory():
        worker_thread_ids.append(threading.get_ident())
        return pubsub

    thread = build_strategy_command_listener(
        pubsub_factory=pubsub_factory,
        is_running=running.is_set,
        assert_leadership=leadership,
        submit_command=submit,
        event_logger=event_logger,
    )
    assert thread.ident is None
    thread.start()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert thread.daemon is True
    assert worker_thread_ids == [thread.ident]
    return pubsub, leadership, submit, event_logger, thread


def test_listener_preserves_filter_fence_parse_submit_and_cleanup_order() -> None:
    events = MagicMock()
    leadership = MagicMock()
    submit = MagicMock()
    events.attach_mock(leadership, "leadership")
    events.attach_mock(submit, "submit")

    pubsub, _leadership, _submit, event_logger, _thread = _start_with_messages(
        [
            None,
            {"type": "subscribe", "data": object()},
            {"type": "message", "data": '{"command":"SCAN"}'},
        ],
        leadership=leadership,
        submit=submit,
    )

    assert events.mock_calls == [
        call.leadership(),
        call.submit({"command": "SCAN"}),
    ]
    assert pubsub.calls == [
        ("subscribe", "cmd:strategy:control"),
        ("get_message", 1.0),
        ("get_message", 1.0),
        ("get_message", 1.0),
        ("close", None),
    ]
    event_logger.info.assert_called_once_with(
        "📡 Command Listener Started. Subscribed to 'cmd:strategy:control'"
    )


def test_parse_failure_logs_existing_envelope_and_continues() -> None:
    event_logger = MagicMock()

    _pubsub, leadership, submit, _logger, _thread = _start_with_messages(
        [
            {"type": "message", "data": "not-json"},
            {"type": "message", "data": '{"command":"SCAN"}'},
        ],
        event_logger=event_logger,
    )

    assert leadership.call_count == 2
    submit.assert_called_once_with({"command": "SCAN"})
    assert event_logger.error.call_count == 1
    assert event_logger.error.call_args.args[0] == "Error parsing command: %s"


def test_submission_failure_logs_existing_envelope_and_continues() -> None:
    error = RuntimeError("executor unavailable")
    submit = MagicMock(side_effect=[error, None])
    event_logger = MagicMock()

    _pubsub, leadership, _submit, _logger, _thread = _start_with_messages(
        [
            {"type": "message", "data": '{"command":"FIRST"}'},
            {"type": "message", "data": '{"command":"SECOND"}'},
        ],
        submit=submit,
        event_logger=event_logger,
    )

    assert leadership.call_count == 2
    assert submit.call_args_list == [
        call({"command": "FIRST"}),
        call({"command": "SECOND"}),
    ]
    event_logger.error.assert_called_once_with(
        "Error parsing command: %s",
        error,
    )


def test_leadership_failure_stops_before_parse_or_submit_and_closes() -> None:
    error = RuntimeError("lease lost")
    leadership = MagicMock(side_effect=[error, None])
    later_message: dict[str, object] = {
        "type": "message",
        "data": '{"command":"LATER"}',
    }

    pubsub, _leadership, submit, event_logger, _thread = _start_with_messages(
        [
            {"type": "message", "data": object()},
            later_message,
        ],
        leadership=leadership,
    )

    leadership.assert_called_once_with()
    submit.assert_not_called()
    event_logger.error.assert_not_called()
    assert pubsub.messages == [later_message]
    assert pubsub.calls[-1] == ("close", None)
