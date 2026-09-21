from dataclasses import replace
from decimal import Decimal
import json
from typing import Callable, cast

import pytest

from src.control_plane import profile_http_success as encoder
from src.core.market_data.profiles.composite_types import CompositeProfilePoc
from src.core.market_data.profiles.types import ProfileBin
from test_profile_live_validation import fixture


def evidence(monkeypatch: pytest.MonkeyPatch, *, daily: bool = False):
    _, _, query, _, start = fixture(monkeypatch)
    if daily:
        manifest = replace(query.profile.manifest, days=query.profile.manifest.days[1:])
        query = replace(
            query,
            profile=replace(query.profile, manifest=manifest),
            selection=replace(query.selection, manifest=manifest),
        )
    return encoder.ValidatedLiveProfileQuery(query, start, start + 10, 10)


@pytest.mark.parametrize("daily", [False, True])
def test_identity_empty_and_deterministic_encoding(
    monkeypatch: pytest.MonkeyPatch, daily: bool
) -> None:
    value = evidence(monkeypatch, daily=daily)
    served = value.validation_completed_at_ms
    raw = encoder.encode_profile_success(value, served_at_ms=served)
    assert type(raw) is bytes and raw == encoder.encode_profile_success(
        value, served_at_ms=served
    )
    body = json.loads(raw)
    assert body["schema_version"] == 1 and body["data_kind"] == "VOLUME_PROFILE"
    assert body["validation_basis"] == "SERVER_PINNED_READ" and "status" not in body
    assert body["profile_kind"] == ("DAILY" if daily else "COMPOSITE")
    assert (
        body["bins"] == []
        and body["poc"] is None
        and body["base_volume"] == body["quote_volume"] == "0"
    )
    assert body["manifest"] == json.loads(value.query.profile.manifest.canonical_bytes)
    assert body["manifest_digest"] == value.query.profile.manifest_digest
    assert body["coverage"] == dict(
        expected_days=1 if daily else 2, complete_days=1 if daily else 2
    )
    if daily:
        ref = value.query.profile.manifest.days[0]
        assert (body["snapshot_id"], body["revision"], body["content_sha256"]) == (
            ref.snapshot_id,
            ref.revision,
            ref.content_sha256,
        )
        assert "composite_id" not in body and "merge_algorithm_version" not in body
    else:
        assert body["composite_id"] == value.query.profile.composite_id
        assert body["merge_algorithm_version"] == "aligned-sum-v1"
        assert not {"snapshot_id", "revision", "content_sha256"} & body.keys()
    assert (
        raw
        == json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    )
    assert body["validation_remaining_ms"] == value.validation_expires_at_ms - served


def test_canonical_decimal_and_poc(monkeypatch: pytest.MonkeyPatch) -> None:
    value = evidence(monkeypatch)
    profile = replace(
        value.query.profile,
        bins=(ProfileBin(-1, Decimal("1.20"), Decimal("12.00"), 1),),
        base_volume=Decimal("1.20"),
        quote_volume=Decimal("12.00"),
        aggregate_count=1,
        poc=CompositeProfilePoc(-1, Decimal("-10.00"), Decimal("-0.00")),
    )
    value = replace(value, query=replace(value.query, profile=profile))
    raw = encoder.encode_profile_success(
        value, served_at_ms=value.validation_completed_at_ms
    )
    assert type(raw) is bytes
    body = json.loads(raw)
    assert body["bins"] == [
        dict(bin_index=-1, base_volume="1.2", quote_volume="12", aggregate_count=1)
    ]
    assert body["poc"] == dict(bin_index=-1, low="-10", high_exclusive="0")


def test_bin_and_actual_byte_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    value = evidence(monkeypatch)
    bins = tuple(ProfileBin(i, Decimal(1), Decimal(1), 1) for i in range(5001))
    profile = replace(
        value.query.profile,
        bins=bins[:5000],
        base_volume=Decimal(5000),
        quote_volume=Decimal(5000),
        aggregate_count=5000,
        poc=CompositeProfilePoc(0, Decimal(0), Decimal(10)),
    )
    value = replace(value, query=replace(value.query, profile=profile))
    raw = encoder.encode_profile_success(
        value, served_at_ms=value.validation_completed_at_ms
    )
    assert type(raw) is bytes and len(json.loads(raw)["bins"]) == 5000
    excess = replace(
        value, query=replace(value.query, profile=replace(profile, bins=bins))
    )
    assert encoder.encode_profile_success(
        excess, served_at_ms=value.validation_completed_at_ms
    ) == encoder.ProfileHttpError(400, "QUERY_TOO_LARGE")
    monkeypatch.setattr(encoder, "_MAX_BYTES", len(raw))
    assert (
        encoder.encode_profile_success(
            value, served_at_ms=value.validation_completed_at_ms
        )
        == raw
    )
    monkeypatch.setattr(encoder, "_MAX_BYTES", len(raw) - 1)
    assert encoder.encode_profile_success(
        value, served_at_ms=value.validation_completed_at_ms
    ) == encoder.ProfileHttpError(400, "QUERY_TOO_LARGE")


def test_served_bounds_and_exact_types(monkeypatch: pytest.MonkeyPatch) -> None:
    value = evidence(monkeypatch)
    invoke = cast(Callable[..., object], encoder.encode_profile_success)
    for served in (
        value.validation_completed_at_ms - 1,
        value.validation_expires_at_ms,
        value.validation_expires_at_ms + 1,
        True,
        -1,
        1 << 63,
        type("Integer", (int,), {})(1),
    ):
        assert invoke(value, served_at_ms=served) == encoder.ProfileHttpError(
            503, "BACKEND_UNAVAILABLE"
        )
    assert invoke(
        True, served_at_ms=value.validation_completed_at_ms
    ) == encoder.ProfileHttpError(503, "BACKEND_UNAVAILABLE")
    assert (
        type(
            encoder.encode_profile_success(
                value, served_at_ms=value.validation_expires_at_ms - 1
            )
        )
        is bytes
    )
