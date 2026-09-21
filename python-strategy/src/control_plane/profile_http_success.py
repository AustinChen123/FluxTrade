"""Bounded full-profile success encoding; no route, pagination or freshness claim."""

import json

from src.core.decimal_math import canonical_decimal_text
from src.core.market_data.profiles.live_validation import ValidatedLiveProfileQuery
from .profile_http_contract import ProfileHttpError

_MAX_BINS = 5000
_MAX_BYTES = 2 * 1024 * 1024


def encode_profile_success(
    evidence: ValidatedLiveProfileQuery, *, served_at_ms: int
) -> bytes | ProfileHttpError:
    if (
        type(evidence) is not ValidatedLiveProfileQuery
        or type(served_at_ms) is not int
        or not 0 <= served_at_ms <= (1 << 63) - 1
    ):
        return ProfileHttpError(503, "BACKEND_UNAVAILABLE")
    expires = evidence.validation_expires_at_ms
    if not evidence.validation_completed_at_ms <= served_at_ms < expires:
        return ProfileHttpError(503, "BACKEND_UNAVAILABLE")
    profile = evidence.query.profile
    if len(profile.bins) > _MAX_BINS:
        return ProfileHttpError(400, "QUERY_TOO_LARGE")
    days = profile.manifest.days
    poc = profile.poc
    payload = dict(
        schema_version=1,
        data_kind="VOLUME_PROFILE",
        profile_kind="DAILY" if len(days) == 1 else "COMPOSITE",
        validation_basis="SERVER_PINNED_READ",
        product_id=profile.product_id,
        base_grid_id=profile.base_grid_id,
        output_grid_id=profile.output_grid.grid_id,
        grid=dict(
            origin=canonical_decimal_text(profile.output_grid.origin),
            step=canonical_decimal_text(profile.output_grid.step),
            unit=profile.output_grid.unit,
        ),
        algorithm_version=profile.algorithm_version,
        window=dict(start_ms=profile.window_start_ms, end_ms=profile.window_end_ms),
        coverage=dict(expected_days=len(days), complete_days=len(days)),
        manifest=json.loads(profile.manifest.canonical_bytes),
        manifest_digest=profile.manifest_digest,
        bins=[
            dict(
                bin_index=b.bin_index,
                base_volume=canonical_decimal_text(b.base_volume),
                quote_volume=canonical_decimal_text(b.quote_volume),
                aggregate_count=b.aggregate_count,
            )
            for b in profile.bins
        ],
        base_volume=canonical_decimal_text(profile.base_volume),
        quote_volume=canonical_decimal_text(profile.quote_volume),
        aggregate_count=profile.aggregate_count,
        poc=None
        if poc is None
        else dict(
            bin_index=poc.index,
            low=canonical_decimal_text(poc.low),
            high_exclusive=canonical_decimal_text(poc.high_exclusive),
        ),
        validation_started_at_ms=evidence.validation_started_at_ms,
        validation_completed_at_ms=evidence.validation_completed_at_ms,
        served_at_ms=served_at_ms,
        validation_expires_at_ms=expires,
        validation_max_age_ms=evidence.validation_max_age_ms,
        validation_remaining_ms=expires - served_at_ms,
    )
    if len(days) == 1:
        ref = days[0]
        payload.update(
            snapshot_id=ref.snapshot_id,
            revision=ref.revision,
            content_sha256=ref.content_sha256,
        )
    else:
        payload.update(
            composite_id=profile.composite_id,
            merge_algorithm_version=profile.merge_algorithm_version,
        )
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > _MAX_BYTES:
        return ProfileHttpError(400, "QUERY_TOO_LARGE")
    return encoded
