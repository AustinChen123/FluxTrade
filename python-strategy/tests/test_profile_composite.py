from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import composite as owner
from src.core.market_data.profiles.read_results import VerifiedManifestRead
from src.core.market_data.profiles.read_types import (
    DailyProfileRef,
    OrderedProfileManifest,
    ProfileInvalidation,
)
from test_profile_read_results import daily, NOW


def reading(*, empty: bool = False, drift: bool = False) -> VerifiedManifestRead:
    days = []
    for i in range(2):
        original = daily(i).publication
        content = replace(
            original.content,
            grid_id="btc_spot_usdt_10_v1",
            bin_origin=Decimal(0),
            bin_step=Decimal(20 if drift else 10),
            bins=() if empty else original.content.bins,
        )
        digest = content.content_sha256
        ref = DailyProfileRef(digest, 1, digest, i * 86400000, (i + 1) * 86400000)
        days.append(
            replace(daily(i), ref=ref, publication=replace(original, content=content))
        )
    manifest = OrderedProfileManifest(
        "BINANCE:BTCUSDT-SPOT",
        "btc_spot_usdt_10_v1",
        "vp-v1",
        tuple(d.ref for d in days),
    )
    return VerifiedManifestRead(manifest, tuple(days))


def result(empty: bool) -> SimpleNamespace:
    return SimpleNamespace(
        product_id="BINANCE:BTCUSDT-SPOT",
        window_start_ms=0,
        window_end_ms=172800000,
        bin_origin="0",
        bin_step="10",
        unit="USDT",
        bins=[] if empty else [(0, "1", "1", 1)],
        base_volume="0" if empty else "1",
        quote_volume="0" if empty else "1",
        aggregate_count=0 if empty else 1,
        poc_index=None if empty else 0,
        poc_low=None if empty else "0",
        poc_high_exclusive=None if empty else "10",
    )


@pytest.mark.parametrize("empty", [False, True])
def test_one_native_call_payload_and_identity(
    monkeypatch: pytest.MonkeyPatch, empty: bool
) -> None:
    read = reading(empty=empty)
    before = tuple(d.publication.content.content_bytes for d in read.days)
    native = Mock(return_value=result(empty))
    monkeypatch.setattr(owner, "_load_native", lambda: (SimpleNamespace, native))
    composed = owner.compose_profile(read)
    assert composed.manifest is read.manifest
    native.assert_called_once()
    args = native.call_args.args
    assert args[:4] == ("BINANCE:BTCUSDT-SPOT", "0", "10", "USDT") and args[5] == "10"
    expected_bins = [] if empty else [(-1, "1.2", "12", 2), (2, "0.3", "3", 1)]
    assert args[4] == [
        (0, 86400000, expected_bins),
        (86400000, 172800000, expected_bins),
    ]
    args[4][0][2].clear()
    assert tuple(d.publication.content.content_bytes for d in read.days) == before
    assert composed.aggregate_count == (0 if empty else 1)
    assert (composed.poc is None) == empty


@pytest.mark.parametrize(
    "mode,reason",
    [
        ("revoked", "REVOKED"),
        ("drift", "INTEGRITY"),
        ("cap", "TOO_LARGE"),
        ("type", "INVALID_INPUT"),
    ],
)
def test_admission_precedes_native(
    monkeypatch: pytest.MonkeyPatch, mode: str, reason: str
) -> None:
    read = reading(drift=mode == "drift")
    if mode == "revoked":
        event = ProfileInvalidation(
            "e",
            read.days[0].ref.snapshot_id,
            "bad",
            NOW.replace(microsecond=0),
            None,
            "test",
        )
        read = replace(
            read, days=(replace(read.days[0], invalidations=(event,)), read.days[1])
        )
    if mode == "cap":
        monkeypatch.setattr(owner, "_BIN_CAP", 0)
    canonical = Mock(side_effect=AssertionError("payload admission violated"))
    monkeypatch.setattr(owner, "canonical_decimal_text", canonical)
    loader = Mock(side_effect=AssertionError("native admission violated"))
    monkeypatch.setattr(owner, "_load_native", loader)
    with pytest.raises(
        owner.ProfileCompositionError, match="^PROFILE_COMPOSITION_" + reason + "$"
    ):
        owner.compose_profile(Mock() if mode == "type" else read)
    loader.assert_not_called()
    canonical.assert_not_called()
