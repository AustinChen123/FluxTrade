# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""D5.1 context attachment and canonical v2 wire-form tests."""

import hashlib

import pytest

from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_artifacts import canonical_bytes, encode_artifact
from spider_journal_reader_test_support import valid_inputs


def test_reader_reattaches_contexts_from_verified_retained_attempt_bytes(tmp_path):
    attempt, attempt_bytes, original, encoded, raw, metadata, budget = valid_inputs()
    assert "configuration_context" not in encoded
    assert "historical_context" not in encoded
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)

    result = reader.read_p3_v2_journal_from_attempt_bytes(
        path,
        expected_metadata=metadata,
        retained_attempt_bytes=attempt_bytes,
        expected_attempt_sha256=hashlib.sha256(attempt_bytes).hexdigest(),
        budget=budget,
    )

    assert result.rows == [original]
    assert result.rows[0]["configuration_context"] == attempt["configuration_context"]
    assert result.rows[0]["historical_context"] == attempt["historical_context"]
    assert canonical_bytes(result.rows[0]) == canonical_bytes(original)


@pytest.mark.parametrize(
    "attempt_bytes,expected_digest",
    [
        (None, "0" * 64),
        ("not-bytes", "0" * 64),
        (b"", hashlib.sha256(b"").hexdigest()),
        (b"{\n", hashlib.sha256(b"{\n").hexdigest()),
        (
            canonical_bytes([]) + b"\n",
            hashlib.sha256(canonical_bytes([]) + b"\n").hexdigest(),
        ),
        (
            canonical_bytes({}) + b"\n",
            hashlib.sha256(canonical_bytes({}) + b"\n").hexdigest(),
        ),
        (
            canonical_bytes({"configuration_context": {}}) + b"\n",
            hashlib.sha256(
                canonical_bytes({"configuration_context": {}}) + b"\n"
            ).hexdigest(),
        ),
        (
            canonical_bytes({"historical_context": {}}) + b"\n",
            hashlib.sha256(
                canonical_bytes({"historical_context": {}}) + b"\n"
            ).hexdigest(),
        ),
        (
            canonical_bytes({"configuration_context": [], "historical_context": {}})
            + b"\n",
            hashlib.sha256(
                canonical_bytes({"configuration_context": [], "historical_context": {}})
                + b"\n"
            ).hexdigest(),
        ),
        (
            canonical_bytes({"configuration_context": {}, "historical_context": None})
            + b"\n",
            hashlib.sha256(
                canonical_bytes(
                    {"configuration_context": {}, "historical_context": None}
                )
                + b"\n"
            ).hexdigest(),
        ),
        (
            canonical_bytes({"configuration_context": {}, "historical_context": []})
            + b"\n",
            hashlib.sha256(
                canonical_bytes({"configuration_context": {}, "historical_context": []})
                + b"\n"
            ).hexdigest(),
        ),
        (
            canonical_bytes({"configuration_context": {}, "historical_context": {}})
            + b"\n",
            "f" * 64,
        ),
        (b'{"x": 1}\n', hashlib.sha256(b'{"x": 1}\n').hexdigest()),
        (
            canonical_bytes({"configuration_context": {}}),
            hashlib.sha256(canonical_bytes({"configuration_context": {}})).hexdigest(),
        ),
    ],
)
def test_reader_rejects_invalid_or_wrong_identity_attempt_bytes_before_journal_io(
    tmp_path, monkeypatch, attempt_bytes, expected_digest
):
    _, _, _, _, _, metadata, budget = valid_inputs()
    io_calls = []
    monkeypatch.setattr(
        reader.journal_buffer,
        "capture_owned_p3_v2_buffer",
        lambda *args, **kwargs: io_calls.append((args, kwargs)),
    )

    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        reader.read_p3_v2_journal_from_attempt_bytes(
            tmp_path / "must-not-be-read.jsonl",
            expected_metadata=metadata,
            retained_attempt_bytes=attempt_bytes,
            expected_attempt_sha256=expected_digest,
            budget=budget,
        )
    assert io_calls == []


def test_reader_requires_the_persisted_attempt_jsonl_terminator(tmp_path, monkeypatch):
    _, attempt_bytes, _, _, _, metadata, budget = valid_inputs()
    unframed = attempt_bytes[:-1]
    io_calls = []
    monkeypatch.setattr(
        reader.journal_buffer,
        "capture_owned_p3_v2_buffer",
        lambda *args, **kwargs: io_calls.append((args, kwargs)),
    )

    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        reader.read_p3_v2_journal_from_attempt_bytes(
            tmp_path / "must-not-be-read.jsonl",
            expected_metadata=metadata,
            retained_attempt_bytes=unframed,
            expected_attempt_sha256=hashlib.sha256(unframed).hexdigest(),
            budget=budget,
        )
    assert io_calls == []


def test_reader_rejects_multiple_retained_attempt_rows_before_journal_io(
    tmp_path, monkeypatch
):
    attempt, _, _, _, _, metadata, budget = valid_inputs()
    raw = encode_artifact(attempt) + encode_artifact(attempt)
    io_calls = []
    monkeypatch.setattr(
        reader.journal_buffer,
        "capture_owned_p3_v2_buffer",
        lambda *args, **kwargs: io_calls.append((args, kwargs)),
    )

    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        reader.read_p3_v2_journal_from_attempt_bytes(
            tmp_path / "must-not-be-read.jsonl",
            expected_metadata=metadata,
            retained_attempt_bytes=raw,
            expected_attempt_sha256=hashlib.sha256(raw).hexdigest(),
            budget=budget,
        )
    assert io_calls == []


def test_reader_requires_a_string_attempt_digest_without_running_user_equality(
    tmp_path, monkeypatch
):
    _, attempt_bytes, _, _, _, metadata, budget = valid_inputs()
    io_calls = []

    class EqualDigest:
        def __eq__(self, other):
            return True

    monkeypatch.setattr(
        reader.journal_buffer,
        "capture_owned_p3_v2_buffer",
        lambda *args, **kwargs: io_calls.append((args, kwargs)),
    )
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        reader.read_p3_v2_journal_from_attempt_bytes(
            tmp_path / "must-not-be-read.jsonl",
            expected_metadata=metadata,
            retained_attempt_bytes=attempt_bytes,
            expected_attempt_sha256=EqualDigest(),
            budget=budget,
        )
    assert io_calls == []
