from dataclasses import replace
import json
import hashlib
from collections import UserList
from unittest.mock import Mock
from typing import Any

import pytest

from src.core.market_data.profiles import bootstrap_history as owner
from src.core.market_data.profiles.decision_application import (
    MarketDataDecisionBatch,
    MarketDataDecisionOutcome,
)
from test_profile_decision_application import batch, key
from test_profile_bootstrap_seed import seed

KEY = replace(seed().key, product_id="BINANCE:BTCUSDT-SPOT")


def row(start=0, strategy="other") -> dict[str, Any]:
    participant = replace(key(), strategy_id=strategy, trigger_id=f"1m:{start}")
    outcome = MarketDataDecisionOutcome(
        participant, "SKIPPED", None, None, "INPUT_STORE_FAILED"
    )
    value = replace(
        batch(), bar_start_ms=start, participants=(participant,), outcomes=(outcome,)
    )
    return dict(
        environment=value.environment,
        execution_scope_id=value.execution_scope_id,
        product_id=value.product_id,
        timeframe=value.timeframe,
        bar_start_ms=start,
        contract_version=1,
        participant_count=1,
        canonical_payload=value.canonical_bytes,
        batch_digest=value.digest,
    )


@pytest.mark.parametrize("count", [0, 1, 32])
def test_clear_exact_count_one_decode_per_valid_row(count, monkeypatch):
    rows = tuple(row(i * 60000) for i in range(count))
    decoder = Mock(wraps=MarketDataDecisionBatch.from_canonical_bytes)
    monkeypatch.setattr(MarketDataDecisionBatch, "from_canonical_bytes", decoder)
    assert owner.classify_batch_history(rows, KEY) == "CLEAR"
    assert decoder.call_count == count
    assert [call.args[0] for call in decoder.call_args_list] == [
        r["canonical_payload"] for r in rows
    ]
    assert owner.MAX_HISTORY_BATCH_ROWS == 32


def test_thirty_third_row_admitted_before_any_decode(monkeypatch):
    rows = tuple(row(i * 60000) for i in range(33))
    decoder = Mock(side_effect=AssertionError("must not decode"))
    monkeypatch.setattr(MarketDataDecisionBatch, "from_canonical_bytes", decoder)
    assert owner.classify_batch_history(rows, KEY) == "UNKNOWN"
    decoder.assert_not_called()


@pytest.mark.parametrize("position", [0, 1, 2])
def test_target_anywhere_ignores_version_and_config(position):
    rows = tuple(row(i * 60000, "s" if i == position else "other") for i in range(3))
    wanted = replace(KEY, strategy_version="new", config_hash="f" * 64)
    assert owner.classify_batch_history(rows, wanted) == "UNKNOWN"


def test_driver_memoryview_and_empty_participant_batch():
    record = row()
    record["canonical_payload"] = memoryview(record["canonical_payload"])
    assert owner.classify_batch_history((record,), KEY) == "CLEAR"
    empty = batch()
    record.update(
        canonical_payload=empty.canonical_bytes,
        batch_digest=empty.digest,
        participant_count=0,
    )
    assert owner.classify_batch_history((record,), KEY) == "CLEAR"


@pytest.mark.parametrize(
    "field,bad",
    [
        ("environment", "paper"),
        ("execution_scope_id", "other"),
        ("product_id", "BINANCE:ETHUSDT-SPOT"),
        ("timeframe", "5m"),
        ("bar_start_ms", -1),
        ("bar_start_ms", True),
        ("bar_start_ms", 2**63),
        ("bar_start_ms", 60000),
        ("contract_version", True),
        ("contract_version", 2),
        ("participant_count", True),
        ("participant_count", -1),
        ("participant_count", 2),
        ("batch_digest", "f" * 64),
        ("batch_digest", None),
        ("canonical_payload", b"{}"),
        ("canonical_payload", "SECRET"),
        ("canonical_payload", bytearray(b"{}")),
        ("canonical_payload", b""),
    ],
)
def test_exact_row_claims_and_payload_rejected(field, bad):
    record = row()
    record[field] = bad
    assert owner.classify_batch_history((record,), KEY) == "UNKNOWN"


@pytest.mark.parametrize("field", list(row()))
def test_missing_and_unknown_fields(field):
    record = row()
    record.pop(field)
    assert owner.classify_batch_history((record,), KEY) == "UNKNOWN"
    assert (
        owner.classify_batch_history((row() | {"unexpected": "SECRET"},), KEY)
        == "UNKNOWN"
    )


def test_duplicate_and_reversed_order_are_not_clear():
    first, second = row(), row(60000)
    assert owner.classify_batch_history((first, first), KEY) == "UNKNOWN"
    assert owner.classify_batch_history((second, first), KEY) == "UNKNOWN"


@pytest.mark.parametrize("binary", [bytes, memoryview])
def test_payload_size_preflight_and_exact_control(binary, monkeypatch):
    assert owner.MAX_DECISION_BATCH_BYTES == 1024 * 1024
    record = row()
    raw = record["canonical_payload"]
    monkeypatch.setattr(owner, "MAX_DECISION_BATCH_BYTES", len(raw))
    decoder = Mock(wraps=MarketDataDecisionBatch.from_canonical_bytes)
    monkeypatch.setattr(MarketDataDecisionBatch, "from_canonical_bytes", decoder)
    assert (
        owner.classify_batch_history(
            (record | {"canonical_payload": binary(raw)},), KEY
        )
        == "CLEAR"
    )
    decoder.assert_called_once()
    decoder.reset_mock()
    assert (
        owner.classify_batch_history(
            (record | {"canonical_payload": binary(raw + b" ")},), KEY
        )
        == "UNKNOWN"
    )
    decoder.assert_not_called()


@pytest.mark.parametrize(
    "field,bad", [("strategy_version", ".invalid"), ("config_hash", "SECRET")]
)
def test_nested_identity_corruption_uses_existing_codec(field, bad, monkeypatch):
    record = row()
    value = json.loads(record["canonical_payload"])
    value["participants"][0][field] = bad
    value["outcomes"][0]["key"][field] = bad
    record["canonical_payload"] = json.dumps(
        value, sort_keys=True, separators=(",", ":")
    ).encode()
    record["batch_digest"] = hashlib.sha256(record["canonical_payload"]).hexdigest()
    decoder = Mock(wraps=MarketDataDecisionBatch.from_canonical_bytes)
    monkeypatch.setattr(MarketDataDecisionBatch, "from_canonical_bytes", decoder)
    with pytest.raises(ValueError):
        decoder(record["canonical_payload"])
    decoder.reset_mock()
    assert owner.classify_batch_history((record,), KEY) == "UNKNOWN"
    decoder.assert_called_once_with(record["canonical_payload"])


def test_exact_string_types_not_subclasses():
    class Text(str):
        pass

    for field in (
        "environment",
        "execution_scope_id",
        "product_id",
        "timeframe",
        "batch_digest",
    ):
        record = row()
        record[field] = Text(record[field])
        assert owner.classify_batch_history((record,), KEY) == "UNKNOWN"


@pytest.mark.parametrize(
    "field",
    ["bar_start_ms", "contract_version", "participant_count", "canonical_payload"],
)
def test_integer_and_bytes_subclasses_rejected(field):
    record = row()
    original = record[field]
    record[field] = type("Derived", (type(original),), {})(original)
    assert owner.classify_batch_history((record,), KEY) == "UNKNOWN"


class HostileSequence(UserList):
    def __len__(self):
        raise AssertionError("must reject before len or iteration")


@pytest.mark.parametrize(
    "rows",
    [
        [],
        {},
        "",
        b"",
        memoryview(b""),
        [row()],
        {"row": row()},
        HostileSequence(),
        type("Tuple", (tuple,), {})(),
    ],
)
def test_outer_container_requires_exact_tuple(rows):
    assert owner.classify_batch_history(rows, KEY) == "UNKNOWN"
    assert owner.classify_batch_history((), KEY) == "CLEAR"
