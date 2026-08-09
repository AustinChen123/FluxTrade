import json
import logging
import threading
from unittest.mock import MagicMock

import pytest

from src.core.rithmic_publisher_liveness_gate import (
    RithmicPublisherLivenessGate,
    RithmicPublisherLivenessState,
)


class _RedisReader:
    def __init__(self, value: object = None):
        self.value = value
        self.error: Exception | None = None
        self.get_calls = 0
        self.close_calls = 0

    def get(self, key: str) -> object:
        self.get_calls += 1
        if self.error is not None:
            raise self.error
        return self.value

    def close(self) -> None:
        self.close_calls += 1


def _gate(
    reader: _RedisReader,
    *,
    logger: logging.Logger | None = None,
) -> RithmicPublisherLivenessGate:
    return RithmicPublisherLivenessGate(
        redis_client=reader,
        key="fluxtrade:live:heartbeat:data-publisher",
        logger=logger or logging.getLogger("test.publisher_liveness"),
    )


def _structured_record(record: logging.LogRecord) -> dict[str, object]:
    return {
        "level": record.levelname,
        "message": record.getMessage(),
        "component": vars(record).get("component"),
        "event_code": vars(record).get("event_code"),
        "liveness_state": vars(record).get("liveness_state"),
        "reason_code": vars(record).get("reason_code"),
    }


def _serialized_record(record: logging.LogRecord) -> str:
    return json.dumps(vars(record), default=str, sort_keys=True)


def test_unarmed_gate_is_closed_without_redis_io() -> None:
    reader = _RedisReader("alive")
    gate = _gate(reader)

    assert gate.state is RithmicPublisherLivenessState.UNARMED
    assert gate.observe() is False
    assert reader.get_calls == 0


@pytest.mark.parametrize("unhealthy_value", [None, "stale", b"stale"])
def test_unconfirmed_gate_waits_for_first_exact_alive(
    unhealthy_value: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = _RedisReader(unhealthy_value)
    gate = _gate(reader)
    gate.arm()

    with caplog.at_level(logging.INFO, logger="test.publisher_liveness"):
        assert gate.observe() is False
        assert gate.observe() is False
        reader.value = "alive"
        assert gate.observe() is True
        assert gate.observe() is True

    assert gate.state is RithmicPublisherLivenessState.CONFIRMED
    records = [
        record
        for record in caplog.records
        if getattr(record, "component", None) == "rithmic_data_publisher"
    ]
    assert [_structured_record(record) for record in records] == [
        {
            "level": "WARNING",
            "message": "Rithmic data publisher liveness transition",
            "component": "rithmic_data_publisher",
            "event_code": "publisher_liveness_unconfirmed",
            "liveness_state": "unconfirmed",
            "reason_code": ("missing" if unhealthy_value is None else "invalid_value"),
        },
        {
            "level": "INFO",
            "message": "Rithmic data publisher liveness transition",
            "component": "rithmic_data_publisher",
            "event_code": "publisher_liveness_confirmed",
            "liveness_state": "confirmed",
            "reason_code": "alive",
        },
    ]
    assert all(
        str(unhealthy_value) not in _serialized_record(record) for record in records
    )


def test_unconfirmed_read_error_is_retryable_and_sanitized(
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = _RedisReader()
    reader.error = RuntimeError("REDIS_URL_SECRET")
    gate = _gate(reader)
    gate.arm()

    with caplog.at_level(logging.INFO, logger="test.publisher_liveness"):
        assert gate.observe() is False
        reader.error = None
        reader.value = "alive"
        assert gate.observe() is True

    assert "REDIS_URL_SECRET" not in caplog.text
    transition = next(
        record
        for record in caplog.records
        if getattr(record, "event_code", None) == "publisher_liveness_unconfirmed"
    )
    assert vars(transition)["reason_code"] == "read_error"
    assert "REDIS_URL_SECRET" not in _serialized_record(transition)


@pytest.mark.parametrize(
    ("unhealthy_value", "error", "reason_code"),
    [
        (None, None, "missing"),
        ("wrong", None, "invalid_value"),
        (None, RuntimeError("PRIVATE_CONNECTION_DETAIL"), "read_error"),
    ],
)
def test_confirmed_failure_latches_once_and_never_reads_again(
    unhealthy_value: object,
    error: Exception | None,
    reason_code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    reader = _RedisReader("alive")
    gate = _gate(reader)
    gate.arm()

    with caplog.at_level(logging.INFO, logger="test.publisher_liveness"):
        assert gate.observe() is True
        reader.value = unhealthy_value
        reader.error = error
        assert gate.observe() is False
        calls_after_latch = reader.get_calls
        reader.value = "alive"
        reader.error = None
        gate.arm()
        assert gate.observe() is False

    assert gate.state is RithmicPublisherLivenessState.LATCHED
    assert reader.get_calls == calls_after_latch
    latch_records = [
        record
        for record in caplog.records
        if getattr(record, "event_code", None) == "publisher_liveness_latched"
    ]
    assert len(latch_records) == 1
    assert _structured_record(latch_records[0]) == {
        "level": "WARNING",
        "message": "Rithmic data publisher liveness transition",
        "component": "rithmic_data_publisher",
        "event_code": "publisher_liveness_latched",
        "liveness_state": "latched",
        "reason_code": reason_code,
    }
    assert "PRIVATE_CONNECTION_DETAIL" not in caplog.text
    assert "wrong" not in caplog.text
    serialized = "\n".join(_serialized_record(record) for record in caplog.records)
    assert "PRIVATE_CONNECTION_DETAIL" not in serialized
    assert "wrong" not in serialized


def test_concurrent_failure_observation_has_one_transition_and_one_log() -> None:
    reader = _RedisReader("alive")
    logger = MagicMock(spec=logging.Logger)
    gate = _gate(reader, logger=logger)
    gate.arm()
    assert gate.observe() is True
    reader.value = None

    barrier = threading.Barrier(9)
    results: list[bool] = []

    def observe() -> None:
        barrier.wait()
        results.append(gate.observe())

    threads = [threading.Thread(target=observe) for _ in range(8)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=1.0)

    assert results == [False] * 8
    assert gate.state is RithmicPublisherLivenessState.LATCHED
    assert [call.args[0] for call in logger.log.call_args_list] == [
        logging.INFO,
        logging.WARNING,
    ]
    assert reader.get_calls == 2


def test_close_owns_the_dedicated_redis_client() -> None:
    reader = _RedisReader()
    gate = _gate(reader)

    gate.close()

    assert reader.close_calls == 1
