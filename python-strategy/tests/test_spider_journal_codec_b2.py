# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""D5.1 B2 ordering, context stripping, and unsupported P3 rows."""

from copy import deepcopy

import pytest

from src.core.backtest import spider_journal_codec_v2 as codec
from src.core.backtest.spider_run_envelope_schema import journal_record
from src.core.backtest.spider_run_errors import ReconciliationProjectionError
from src.core.backtest.spider_run_evidence import _context_chain
from test_spider_run_artifacts import historical_attempt
from test_spider_run_envelope_schema import record


def _attempt_and_row(kind="CALLBACK_RESULT"):
    attempt = historical_attempt()
    row = record(kind)
    row["configuration_context"] = deepcopy(attempt["configuration_context"])
    row["historical_context"] = deepcopy(attempt["historical_context"])
    return attempt, row


def _validator(row):
    journal_record(row)


def _context_check(attempt):
    return lambda row: _context_chain(attempt, row)


def test_b2_success_runs_existing_checks_then_b1_and_strips_only_the_copy(monkeypatch):
    attempt, row = _attempt_and_row()
    original = deepcopy(row)
    prior = (b"previous-definition",)
    events = []
    b1 = codec.encode_p3_v2_row

    def validate(candidate):
        events.append("v1")
        journal_record(candidate)

    def context_check(candidate):
        events.append("context")
        _context_chain(attempt, candidate)

    def intern(candidate, state):
        events.append("b1")
        return b1(candidate, state)

    monkeypatch.setattr(codec, "encode_p3_v2_row", intern)
    encoded, next_state = codec.encode_p3_v2_row_with_context_gate(
        row,
        prior,
        validate_v1=validate,
        check_context_chain=context_check,
    )

    assert events == ["v1", "context", "b1"]
    assert "configuration_context" not in encoded
    assert "historical_context" not in encoded
    assert row == original
    assert next_state == prior
    assert type(next_state) is tuple


@pytest.mark.parametrize(
    "field,mutate",
    [
        ("configuration_context", lambda value: value.update(config_id="other")),
        (
            "historical_context",
            lambda value: value.update(historical_input_sha256="f" * 64),
        ),
    ],
)
def test_b2_context_mismatch_propagates_before_b1(monkeypatch, field, mutate):
    attempt, row = _attempt_and_row()
    mutate(row[field])
    source = deepcopy(row)
    calls = []
    monkeypatch.setattr(codec, "encode_p3_v2_row", lambda *args: calls.append(args))

    with pytest.raises(ReconciliationProjectionError) as observed:
        codec.encode_p3_v2_row_with_context_gate(
            row,
            validate_v1=_validator,
            check_context_chain=_context_check(attempt),
        )

    assert calls == []
    assert row == source
    with pytest.raises(ReconciliationProjectionError) as expected:
        _context_chain(attempt, row)
    assert type(observed.value) is type(expected.value)
    assert observed.value.args == expected.value.args
    assert type(observed.value.__cause__) is type(expected.value.__cause__)
    assert observed.value.__suppress_context__ is expected.value.__suppress_context__


@pytest.mark.parametrize(
    "field,value,expected_events",
    [
        ("configuration_context", "missing", ["v1"]),
        ("historical_context", "missing", ["v1", "context"]),
        ("configuration_context", None, ["v1"]),
        ("historical_context", None, ["v1"]),
    ],
)
def test_b2_missing_or_null_context_follows_v1_then_context_gate_precedence(
    field, value, expected_events
):
    attempt, row = _attempt_and_row()
    if value == "missing":
        row.pop(field)
    else:
        row[field] = value
    events = []

    def validate(candidate):
        events.append("v1")
        journal_record(candidate)

    def context_check(candidate):
        events.append("context")
        _context_chain(attempt, candidate)

    with pytest.raises((ValueError, TypeError)):
        codec.encode_p3_v2_row_with_context_gate(
            row, validate_v1=validate, check_context_chain=context_check
        )
    assert events == expected_events


def test_b2_validation_error_wins_over_a_simultaneous_context_mismatch():
    attempt, row = _attempt_and_row()
    row["configuration_context"]["config_id"] = "different"
    row["scheduler_key"]["queue_class"] = "wrong"
    events = []

    def validate(candidate):
        events.append("v1")
        journal_record(candidate)

    def context_check(candidate):
        events.append("context")
        _context_chain(attempt, candidate)

    with pytest.raises(ValueError) as expected:
        journal_record(row)
    with pytest.raises(ValueError) as observed:
        codec.encode_p3_v2_row_with_context_gate(
            row, validate_v1=validate, check_context_chain=context_check
        )
    assert events == ["v1"]
    assert type(observed.value) is type(expected.value)
    assert observed.value.args == expected.value.args
    assert type(observed.value.__cause__) is type(expected.value.__cause__)
    assert observed.value.__suppress_context__ is expected.value.__suppress_context__


def test_delivery_attempt_is_unsupported_before_any_encoded_row_is_returned():
    attempt, row = _attempt_and_row("DELIVERY_ATTEMPT")
    source = deepcopy(row)
    emitted = []

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW") as caught:
        encoded, state = codec.encode_p3_v2_row_with_context_gate(
            row,
            validate_v1=_validator,
            check_context_chain=_context_check(attempt),
        )
        emitted.append((encoded, state))

    assert caught.value.args == ("CODEC_UNSUPPORTED_ROW",)
    assert emitted == []
    assert row == source
