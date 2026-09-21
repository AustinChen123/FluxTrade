from dataclasses import replace
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import Mock, call

import pytest

from src.core.market_data.profiles import live_query as query
from src.core.market_data.profiles.composite_types import CompositeProfile
from src.core.market_data.profiles.grid import resolve_profile_grid
from src.core.market_data.profiles.read_results import ProfileCandidate
from src.core.market_data.profiles.read_types import ProfileQueryRequest
from test_profile_composite import reading
from test_profile_read_results import NOW

if TYPE_CHECKING:
    from src.core.market_data.profiles.read_repository import ProfileReadRepository

    def structural(provider: ProfileReadRepository) -> query.ProfileLiveProvider:
        return provider


def setup(monkeypatch: pytest.MonkeyPatch):
    read = reading(empty=True)
    m = read.manifest
    request = ProfileQueryRequest(
        m.product_id,
        m.base_grid_id,
        m.base_grid_id,
        m.algorithm_version,
        0,
        172800000,
        "LIVE_QUERY",
        freshness_policy_id="utc_complete_strict_v1",
    )
    provider = Mock()
    provider.list_candidates.return_value = tuple(
        ProfileCandidate(d.ref, NOW, NOW, NOW, "OBSERVED", False) for d in read.days
    )
    provider.get_manifest.return_value = read
    profile = CompositeProfile(
        m,
        resolve_profile_grid(m.product_id, m.base_grid_id, m.algorithm_version),
        (),
        Decimal(0),
        Decimal(0),
        0,
        None,
    )
    compose = Mock(return_value=profile)
    monkeypatch.setattr(query, "compose_profile", compose)
    return request, provider, compose, query.LiveSelectionContext(172800000)


def test_happy_pins_once_and_exact_args(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, compose, context = setup(monkeypatch)
    result = query.query_live_profile(provider, request, context)
    assert isinstance(result, query.LiveProfileQueryResult)
    assert provider.mock_calls == [
        call.list_candidates(
            product_id=request.product_id,
            base_grid_id=request.base_grid_id,
            algorithm_version=request.algorithm_version,
            start_ms=0,
            end_ms=172800000,
            revision=None,
        ),
        call.get_manifest(result.selection.manifest),
    ]
    compose.assert_called_once_with(provider.get_manifest.return_value)
    assert result.profile is compose.return_value


@pytest.mark.parametrize(
    "mode,reason",
    [
        ("future", "NOT_READY"),
        ("expired", "PROFILE_EXPIRED"),
        ("revoked", "SNAPSHOT_REVOKED"),
    ],
)
def test_selector_unavailable_stops(
    monkeypatch: pytest.MonkeyPatch, mode: str, reason: str
) -> None:
    request, provider, compose, context = setup(monkeypatch)
    if mode == "revoked":
        provider.list_candidates.return_value = tuple(
            replace(c, revoked=True) for c in provider.list_candidates.return_value
        )
    else:
        context = query.LiveSelectionContext(0 if mode == "future" else 259200000)
    assert query.query_live_profile(
        provider, request, context
    ) == query.LiveProfileQueryUnavailable(reason)
    provider.get_manifest.assert_not_called()
    compose.assert_not_called()


def test_bad_grid_is_pre_io(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, compose, context = setup(monkeypatch)
    with pytest.raises(query.ProfileQueryError, match="^PROFILE_QUERY_INVALID$"):
        query.query_live_profile(
            provider, replace(request, output_grid_id="SECRET"), context
        )
    assert provider.mock_calls == [] and compose.call_count == 0


@pytest.mark.parametrize("value", [None, True, "wrong_manifest"])
def test_second_read_classification(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    request, provider, compose, context = setup(monkeypatch)
    if value == "wrong_manifest":
        read = provider.get_manifest.return_value
        days = tuple(replace(d, ref=replace(d.ref, revision=2)) for d in read.days)
        value = replace(
            read,
            days=days,
            manifest=replace(read.manifest, days=tuple(d.ref for d in days)),
        )
    provider.get_manifest.return_value = value
    if value is None:
        assert query.query_live_profile(
            provider, request, context
        ) == query.LiveProfileQueryUnavailable("NOT_READY")
    else:
        with pytest.raises(query.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
            query.query_live_profile(provider, request, context)
    compose.assert_not_called()
    provider.list_candidates.assert_called_once()
