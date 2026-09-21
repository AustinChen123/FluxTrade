from dataclasses import replace
from decimal import Decimal, Inexact, Rounded, localcontext
import json

import pytest

from src.control_plane.profile_http_success import encode_profile_success
from src.core.market_data.profiles import wire
from src.core.market_data.profiles.composite_types import (
    CompositeProfile,
    CompositeProfilePoc,
)
from src.core.market_data.profiles.grid import resolve_profile_grid
from src.core.market_data.profiles.live_query import LiveProfileQueryResult
from src.core.market_data.profiles.live_selection import LiveProfileSelection
from src.core.market_data.profiles.live_validation import ValidatedLiveProfileQuery
from src.core.market_data.profiles.read_types import (
    DailyProfileRef,
    OrderedProfileManifest,
    ProfileQueryRequest,
)
from src.core.market_data.profiles.types import ProfileBin, VolumeProfileContent

DAY = 86400000


def sample(days=1, empty=False):
    grid = resolve_profile_grid("BINANCE:BTCUSDT-SPOT", "btc_spot_usdt_10_v1", "vp-v1")
    bins = (
        ()
        if empty
        else (
            ProfileBin(-1, Decimal("1.2"), Decimal("12"), 2),
            ProfileBin(1, Decimal("1.2"), Decimal("12"), 1),
        )
    )
    content = VolumeProfileContent(
        grid.product_id,
        0,
        DAY,
        grid.grid_id,
        grid.origin,
        grid.step,
        grid.algorithm_version,
        bins,
    )
    refs = tuple(
        DailyProfileRef(
            replace(
                content, window_start_ms=i * DAY, window_end_ms=(i + 1) * DAY
            ).content_sha256,
            1,
            replace(
                content, window_start_ms=i * DAY, window_end_ms=(i + 1) * DAY
            ).content_sha256,
            i * DAY,
            (i + 1) * DAY,
        )
        for i in range(days)
    )
    manifest = OrderedProfileManifest(
        grid.product_id, grid.grid_id, grid.algorithm_version, refs
    )
    profile = CompositeProfile(
        manifest,
        grid,
        bins,
        content.base_volume,
        content.quote_volume,
        content.aggregate_count,
        None if empty else CompositeProfilePoc(-1, Decimal(-10), Decimal(0)),
    )
    request = ProfileQueryRequest(
        grid.product_id,
        grid.grid_id,
        grid.grid_id,
        grid.algorithm_version,
        0,
        days * DAY,
        "LIVE_QUERY",
        "utc_complete_strict_v1",
    )
    selection = LiveProfileSelection(manifest, days * DAY, days * DAY + 100)
    evidence = ValidatedLiveProfileQuery(
        LiveProfileQueryResult(request, selection, profile),
        days * DAY,
        days * DAY + 10,
        20,
    )
    raw = encode_profile_success(evidence, served_at_ms=days * DAY + 11)
    assert type(raw) is bytes
    return request, evidence, raw


@pytest.mark.parametrize("days", [1, 7, 30, 90])
@pytest.mark.parametrize("empty", [False, True])
def test_encoder_roundtrip(days, empty):
    request, evidence, raw = sample(days, empty)
    decoded = wire.decode_live_profile_response(request, raw)
    assert decoded.validated == evidence and decoded.validated.query.request is request
    assert decoded.served_at_ms == days * DAY + 11
    assert (
        decoded.validated.query.selection.available_at_ms
        > evidence.validation_completed_at_ms
    )
    assert (
        encode_profile_success(decoded.validated, served_at_ms=decoded.served_at_ms)
        == raw
    )


def test_low_decimal_context_is_irrelevant():
    request, _, raw = sample()
    expected = wire.decode_live_profile_response(request, raw)
    with localcontext() as context:
        context.prec = 1
        context.traps[Inexact] = context.traps[Rounded] = True
        assert wire.decode_live_profile_response(request, raw) == expected


def test_daily_content_hash_and_composite_identity_are_checked():
    for days, field in ((1, "content_sha256"), (7, "composite_id")):
        request, _, raw = sample(days)
        body = json.loads(raw)
        body[field] = "f" * 64
        with pytest.raises(wire.ProfileWireError, match="INVALID_PROFILE"):
            wire.decode_live_profile_response(request, json.dumps(body).encode())


def test_exact_5000_bins_and_lease_boundaries():
    request, evidence, _ = sample(7)
    bins = tuple(ProfileBin(i, Decimal(1), Decimal(10), 1) for i in range(5000))
    profile = replace(
        evidence.query.profile,
        bins=bins,
        base_volume=Decimal(5000),
        quote_volume=Decimal(50000),
        aggregate_count=5000,
        poc=CompositeProfilePoc(0, Decimal(0), Decimal(10)),
    )
    evidence = replace(evidence, query=replace(evidence.query, profile=profile))
    for served in (
        evidence.validation_completed_at_ms,
        evidence.validation_expires_at_ms - 1,
    ):
        raw = encode_profile_success(evidence, served_at_ms=served)
        assert type(raw) is bytes
        decoded = wire.decode_live_profile_response(request, raw)
        assert (
            decoded.validated == evidence
            and len(decoded.validated.query.profile.bins) == 5000
        )


def test_composite_poc_requires_unique_volume_maximum():
    request, evidence, _ = sample(7)
    profile = replace(
        evidence.query.profile,
        bins=(
            ProfileBin(-1, Decimal(1), Decimal(10), 1),
            ProfileBin(2, Decimal(3), Decimal(60), 1),
        ),
        base_volume=Decimal(4),
        quote_volume=Decimal(70),
        aggregate_count=2,
        poc=CompositeProfilePoc(2, Decimal(20), Decimal(30)),
    )
    evidence = replace(evidence, query=replace(evidence.query, profile=profile))
    raw = encode_profile_success(
        evidence, served_at_ms=evidence.validation_completed_at_ms
    )
    assert type(raw) is bytes
    assert wire.decode_live_profile_response(request, raw).validated == evidence
    body = json.loads(raw)
    assert body["profile_kind"] == "COMPOSITE"
    # Valid edges and existing index isolate winner selection, not daily digest/edge checks.
    body["poc"] = {"bin_index": -1, "low": "-10", "high_exclusive": "0"}
    with pytest.raises(wire.ProfileWireError, match="^PROFILE_WIRE_INVALID_PROFILE$"):
        wire.decode_live_profile_response(request, json.dumps(body).encode())
