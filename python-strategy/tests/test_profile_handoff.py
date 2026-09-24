import json
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from src.core.market_data.profiles.handoff import ParsedHandoff, parse_handoff
from src.core.market_data.profiles.publication import MAX_JSON_BYTES

FIXTURE = json.loads((Path(__file__).parent / "fixtures/profile_handoff_v1.json").read_text())
RAW = FIXTURE["wire_bytes"].encode("utf-8")


def encode(wire: dict[str, Any]) -> bytes:
    return json.dumps(wire, sort_keys=True, separators=(",", ":")).encode()


def test_shared_rust_fixture_exact_content_digest_and_metadata_mapping() -> None:
    result = parse_handoff(RAW)
    wire = json.loads(RAW)
    assert encode(wire) == RAW
    assert result.publication.content.content_bytes == FIXTURE["content_bytes"].encode()
    assert result.publication.content_sha256 == FIXTURE["content_sha256"]
    assert result.spec.id == "daily-job" and result.spec.config_sha256 == "c" * 64
    assert result.publication.content.aggregate_count == 2
    assert result.publication.reconciliation.thaw() == wire["reconciliation"]
    assert result.publication.source_manifest.thaw() == {
        "schema_version": 1, "job_id": "daily-job", "config_sha256": "c" * 64, "hours": wire["hours"]}
    assert result.publication.source_available_at is not None
    assert result.publication.source_available_at.isoformat() == "1970-01-02T00:00:00+00:00"
    with pytest.raises(FrozenInstanceError):
        setattr(result, "spec", result.spec)
    with pytest.raises(ValueError):
        ParsedHandoff(replace(result.spec, grid_id="other"), result.publication)


@pytest.mark.parametrize("path,value", [
    (("schema_version",), 2), (("schema_version",), True), (("config_sha256",), "A" * 64),
    (("content_sha256",), "d" * 64), (("content", "bin_origin"), "-0"),
    (("content", "bin_step"), "10.0"), (("content", "bins", 0, "base_volume"), "2e0"),
    (("content", "bins", 0, "base_volume"), "NaN"), (("content", "bins", 0, "base_volume"), "1E+4300"),
    (("content", "bins", 1, "bin_index"), 0), (("content", "window_start_ms"), False),
    (("hours", 1, "start_ms"), 0), (("hours", 1, "end_ms"), 1),
    (("hours", 2, "first_aggregate_id"), 100), (("hours", 2, "last_aggregate_id"), 100),
    (("hours", 2, "last_aggregate_id"), 102),
    (("hours", 0, "aggregate_count"), 2), (("hours", 1, "first_aggregate_id"), 100),
    (("hours", 0, "page_count"), 0), (("hours", 0, "page_count"), True),
    (("hours", 0, "manifest_sha256"), "invalid"),
    (("reconciliation", "expected_base_volume"), "4"), (("reconciliation", "actual_quote_volume"), "36"),
    (("reconciliation", "actual_aggregate_trade_count"), 3),
    (("reconciliation", "official_constituent_trade_count"), 0),
    (("reconciliation", "official_constituent_trade_count"), 1 << 64),
    (("reconciliation", "response_sha256"), "invalid"), (("reconciliation", "result"), "PARTIAL"),
    (("reconciliation", "product_id"), "BINANCE:BTCUSDT-PERP"),
    (("availability_basis",), "MODELED"), (("raw_retention_state",), "DELETED"),
    (("source_available_at_ms",), 86399999), (("source_available_at_ms",), 253402300800000),
    (("job_id",), "secret\x00"), (("job_id",), "../job"),
])
def test_wire_mutation_matrix(path: tuple[Any, ...], value: Any) -> None:
    wire = json.loads(RAW)
    target = wire
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError, match="^invalid profile handoff$"):
        parse_handoff(encode(wire))


def test_duplicate_unknown_missing_fields_order_and_input_limits() -> None:
    cases = [RAW.replace(b'"schema_version":1', b'"schema_version":1,"schema_version":1', 1),
             RAW.replace(b'"bin_index":0', b'"bin_index":0,"bin_index":0', 1),
             RAW.replace(b'"page_count":1', b'"page_count":1.0', 1), b'\xff', b' ' * (MAX_JSON_BYTES + 1)]
    for scope in ((), ("content",), ("reconciliation",), ("hours", 0), ("content", "bins", 0)):
        for missing in (False, True):
            wire = json.loads(RAW)
            target = wire
            for key in scope:
                target = target[key]
            if missing:
                target.pop(next(iter(target)))
            else:
                target["unexpected"] = "not accepted"
            cases.append(encode(wire))
    wire = json.loads(RAW)
    wire["hours"][0], wire["hours"][1] = wire["hours"][1], wire["hours"][0]
    cases.append(encode(wire))
    wire = json.loads(RAW)
    nested: Any = "deep"
    for _ in range(17):
        nested = [nested]
    wire["hours"][0]["manifest_sha256"] = nested
    cases.append(encode(wire))
    for raw in cases:
        with pytest.raises(ValueError, match="^invalid profile handoff$"):
            parse_handoff(raw)


def test_empty_day_requires_official_zero_and_counts_are_distinct() -> None:
    wire = json.loads(RAW)
    wire["reconciliation"]["official_constituent_trade_count"] = 99
    assert parse_handoff(encode(wire)).publication.content.aggregate_count == 2
    content = replace(parse_handoff(RAW).publication.content, bins=())
    wire["content"], wire["content_sha256"] = json.loads(content.content_bytes), content.content_sha256
    for hour in wire["hours"]:
        hour.update(aggregate_count=0, first_aggregate_id=None, last_aggregate_id=None)
    for field in ("expected_base_volume", "expected_quote_volume", "actual_base_volume", "actual_quote_volume"):
        wire["reconciliation"][field] = "0"
    wire["reconciliation"].update(actual_aggregate_trade_count=0, official_constituent_trade_count=0)
    assert parse_handoff(encode(wire)).publication.content == content
    wire["reconciliation"]["official_constituent_trade_count"] = 1
    with pytest.raises(ValueError):
        parse_handoff(encode(wire))


@pytest.mark.parametrize("change", [{"grid_id": "g1"}, {"bin_origin": Decimal(1)}, {"bin_step": Decimal(1)}])
def test_mvp_grid_rejection_even_with_a_matching_content_digest(change: dict[str, Any]) -> None:
    wire = json.loads(RAW)
    content = replace(parse_handoff(RAW).publication.content, **change)
    wire["content"] = json.loads(content.content_bytes)
    wire["content_sha256"] = content.content_sha256
    with pytest.raises(ValueError, match="^invalid profile handoff$"):
        parse_handoff(encode(wire))
    # Unit is fixed to USDT by the MVP identity, never a caller-selected field.
    wire = json.loads(RAW)
    wire["content"]["unit"] = "USDC"
    with pytest.raises(ValueError):
        parse_handoff(encode(wire))
