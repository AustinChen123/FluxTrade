import json
from dataclasses import replace
from decimal import Decimal
from typing import cast

import pytest

from src.core.market_data.profiles.handoff import encode_handoff, parse_handoff
from src.core.market_data.profiles.publication import (
    CanonicalJsonObject,
    MAX_JSON_NODES,
    _canonical,
)
from src.core.market_data.profiles.types import ProfileBin
from test_profile_handoff import RAW


def test_510_bins_full_wire_roundtrip_retains_metadata_ratchet():
    parsed = parse_handoff(RAW)
    content = replace(
        parsed.publication.content,
        bins=tuple(ProfileBin(i, Decimal(1), Decimal(1), 1) for i in range(510)),
    )
    manifest = parsed.publication.source_manifest.thaw()
    hours = cast(list[dict[str, object]], manifest["hours"])
    for hour in hours:
        hour.update(aggregate_count=0, first_aggregate_id=None, last_aggregate_id=None)
    hours[0].update(
        aggregate_count=510, first_aggregate_id=1, last_aggregate_id=510
    )
    recon = parsed.publication.reconciliation.thaw()
    for key in (
        "expected_base_volume",
        "actual_base_volume",
        "expected_quote_volume",
        "actual_quote_volume",
    ):
        recon[key] = "510"
    recon.update(actual_aggregate_trade_count=510, official_constituent_trade_count=510)
    publication = replace(
        parsed.publication,
        content=content,
        source_manifest=CanonicalJsonObject(manifest),
        reconciliation=CanonicalJsonObject(recon),
    )
    raw = encode_handoff(parsed.spec, publication)
    assert len(raw) <= 65537
    assert parse_handoff(raw).publication == publication
    assert MAX_JSON_NODES == 4096
    with pytest.raises(ValueError, match="node"):
        CanonicalJsonObject(json.loads(raw))
    with pytest.raises(ValueError):
        parse_handoff(raw + b" " * 65537)


def test_full_wire_byte_depth_limits_unchanged():
    with pytest.raises(ValueError, match="byte"):
        _canonical({"value": "x" * 65536}, max_nodes=65536)
    deep: dict[str, object] = {}
    for _ in range(17):
        deep = {"value": deep}
    with pytest.raises(ValueError, match="depth"):
        _canonical(deep, max_nodes=65536)
