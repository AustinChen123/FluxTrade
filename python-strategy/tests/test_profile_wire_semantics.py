from dataclasses import fields, replace
import json
from typing import Any, cast

import pytest

from src.core.market_data.profiles import wire
from test_profile_wire import sample, DAY

from test_profile_wire_structure import reject, _at


@pytest.mark.parametrize(
    "value",
    [
        "NaN",
        "Infinity",
        "1e0",
        "1.0",
        "-0",
        " 1",
        "0." + "0" * 28 + "1",
        str(1 << 96),
        "\ud800",
        "1" * 65,
        "é" * 33,
        1,
        True,
        None,
    ],
)
@pytest.mark.parametrize(
    "path",
    [
        ("base_volume",),
        ("quote_volume",),
        ("bins", 0, "base_volume"),
        ("bins", 0, "quote_volume"),
        ("grid", "origin"),
        ("grid", "step"),
        ("poc", "low"),
        ("poc", "high_exclusive"),
    ],
)
def test_every_decimal_field(path, value):
    _, _, raw = sample()
    body = json.loads(raw)
    _at(body, path[:-1])[path[-1]] = value
    reject(body)


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), True),
        (("schema_version",), 1),
        (("data_kind",), "OTHER"),
        (("validation_basis",), "FRESH"),
        (("profile_kind",), "COMPOSITE"),
        (("product_id",), "BINANCE:ETHUSDT-SPOT"),
        (("output_grid_id",), "other"),
        (("base_grid_id",), "other"),
        (("algorithm_version",), "other"),
        (("grid", "unit"), "BTC"),
        (("grid", "step"), "20"),
        (("window", "start_ms"), True),
        (("window", "end_ms"), 2 * DAY),
        (("coverage", "expected_days"), True),
        (("coverage", "complete_days"), 0),
        (("manifest_digest",), "a" * 64),
        (("revision",), True),
        (("manifest", 0, "revision"), 0),
        (("manifest", 0, "start_ms"), -1),
        (("aggregate_count",), True),
        (("aggregate_count",), 4),
        (("base_volume",), "3"),
        (("quote_volume",), "25"),
        (("bins", 0, "bin_index"), True),
        (("bins", 0, "aggregate_count"), 0),
        (("bins", 0, "base_volume"), "0"),
        (("poc", "bin_index"), 1),
        (("poc", "low"), "-11"),
        (("poc",), None),
        (("validation_max_age_ms",), 300001),
        (("validation_remaining_ms",), 1),
        (("source_available_at_ms",), DAY - 1),
        (("validation_started_at_ms",), DAY + 11),
        (("validation_completed_at_ms",), DAY - 1),
        (("served_at_ms",), DAY),
        (("validation_expires_at_ms",), DAY + 11),
    ],
)
def test_content_identity_and_time_mutations(path, value):
    _, _, raw = sample()
    body = json.loads(raw)
    _at(body, path[:-1])[path[-1]] = value
    reject(body)


def test_manifests_bins_order_empty_contract_and_composite_version():
    for days in (1, 7):
        request, _, raw = sample(days)
        baseline = json.loads(raw)
        for key, value in (
            ("manifest", []),
            ("bins", list(reversed(baseline["bins"]))),
            ("bins", baseline["bins"] * 2),
            ("bins", []),
        ):
            reject({**baseline, key: value}, request)
        if days > 1:
            reject(
                {**baseline, "manifest": list(reversed(baseline["manifest"]))}, request
            )
            reject({**baseline, "merge_algorithm_version": "aligned-sum-v2"}, request)
        reject(
            {**baseline, "manifest": baseline["manifest"] * 91},
            request,
            "QUERY_TOO_LARGE",
        )
    request, _, raw = sample(empty=True)
    body = json.loads(raw)
    body["poc"] = {"bin_index": 0, "low": "0", "high_exclusive": "10"}
    reject(body, request)


def test_exact_inputs_and_evidence_constructor():
    request, evidence, raw = sample()
    child = type("RequestChild", (type(request),), {})(
        **{f.name: getattr(request, f.name) for f in fields(request)}
    )
    for invalid in (
        None,
        child,
        replace(request, freshness_policy_id="other"),
        replace(
            request,
            purpose="MODELED_RESEARCH",
            freshness_policy_id=None,
            availability_policy_id="m",
            as_of_ms=DAY,
        ),
    ):
        with pytest.raises(wire.ProfileWireError):
            wire.decode_live_profile_response(cast(Any, invalid), raw)
    for invalid in (bytearray(raw), type("Bytes", (bytes,), {})(raw), "SECRET"):
        with pytest.raises(wire.ProfileWireError):
            wire.decode_live_profile_response(request, cast(Any, invalid))
    for stamp in (
        True,
        -1,
        1 << 63,
        evidence.validation_completed_at_ms - 1,
        evidence.validation_expires_at_ms,
    ):
        with pytest.raises(wire.ProfileWireError):
            wire.ProfileWireEvidence(evidence, stamp)
    with pytest.raises(wire.ProfileWireError):
        wire.ProfileWireEvidence(cast(Any, None), DAY)


@pytest.mark.parametrize(
    "field",
    [
        "source_available_at_ms",
        "validation_started_at_ms",
        "validation_completed_at_ms",
        "served_at_ms",
        "validation_expires_at_ms",
        "validation_max_age_ms",
        "validation_remaining_ms",
    ],
)
@pytest.mark.parametrize("bad", [True, -1, 1 << 63, None, "1"])
def test_all_time_domains(field, bad):
    _, _, raw = sample()
    body = json.loads(raw)
    body[field] = bad
    reject(body)


def test_lease_algebra_and_daily_bins_digest_not_just_top_level():
    request, _, raw = sample()
    body = json.loads(raw)
    body["validation_expires_at_ms"] = DAY + 300011
    body["validation_remaining_ms"] = 300000
    reject(body, request)  # Derived elapsed would be negative.
    body = json.loads(raw)
    body["served_at_ms"] = body["validation_expires_at_ms"]
    body["validation_remaining_ms"] = 0
    reject(body, request)
    body = json.loads(raw)
    body["bins"][0]["quote_volume"] = "13"
    body["quote_volume"] = "25"
    reject(
        body, request
    )  # Totals still exact, but daily content identity no longer matches.


@pytest.mark.parametrize(
    "change",
    [
        {"product_id": "BINANCE:ETHUSDT-SPOT"},
        {"base_grid_id": "other"},
        {"output_grid_id": "other"},
        {"algorithm_version": "v2"},
        {"start_ms": DAY, "end_ms": 2 * DAY},
        {"revision": 2},
    ],
)
def test_request_binding(change):
    request, _, raw = sample()
    reject(raw, replace(request, **change))


def test_evidence_exact_type_and_closed_error_reason():
    _, validated, _ = sample()
    child = type("Child", (type(validated),), {})(
        **{f.name: getattr(validated, f.name) for f in fields(validated)}
    )
    with pytest.raises(wire.ProfileWireError):
        wire.ProfileWireEvidence(child, DAY + 11)
    for reason in (True, "SECRET", type("Text", (str,), {})("INVALID_PROFILE")):
        with pytest.raises(ValueError, match="^PROFILE_WIRE_INVALID_PROFILE$"):
            wire.ProfileWireError(cast(Any, reason))
