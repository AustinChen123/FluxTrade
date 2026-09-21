from dataclasses import fields, replace
from decimal import Decimal
from typing import TYPE_CHECKING, Callable, cast
from unittest.mock import Mock, call

import pytest

from src.core.market_data.profiles import live_query as query
from src.core.market_data.profiles.composite_types import CompositeProfile
from src.core.market_data.profiles.grid import resolve_profile_grid
from src.core.market_data.profiles.read_results import ProfileCandidate
from src.core.market_data.profiles.read_types import ProfileQueryRequest
from src.core.market_data.profiles.read_types import ProfileInvalidation
from test_profile_composite import reading
from test_profile_read_results import NOW

if TYPE_CHECKING:
    from src.core.market_data.profiles.read_repository import ProfileReadRepository

    def structural(provider: ProfileReadRepository) -> query.ProfileLiveProvider:
        return provider


@pytest.mark.parametrize(
    "change",
    [
        {"product_id": "BINANCE:ETHUSDT-SPOT"},
        {"base_grid_id": "other"},
        {"output_grid_id": "other"},
        {"algorithm_version": "v2"},
        {"start_ms": 86400000},
        {"end_ms": 259200000},
        {"freshness_policy_id": "other"},
        {
            "purpose": "MODELED_RESEARCH",
            "freshness_policy_id": None,
            "availability_policy_id": "modeled",
            "as_of_ms": 172800000,
        },
    ],
)
def test_result_request_identity_binding(monkeypatch, change):
    request, provider, _, context = setup(monkeypatch)
    result = query.query_live_profile(provider, request, context)
    assert isinstance(result, query.LiveProfileQueryResult)
    assert result.request is request
    with pytest.raises(query.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        replace(result, request=replace(request, **change))


def test_result_request_exact_and_single_day_revision(monkeypatch):
    request, provider, _, context = setup(monkeypatch)
    result = query.query_live_profile(provider, request, context)
    assert isinstance(result, query.LiveProfileQueryResult)
    subclass = type("RequestSubclass", (ProfileQueryRequest,), {})
    child = subclass(**{f.name: getattr(request, f.name) for f in fields(request)})
    for bad in (None, child, True):
        with pytest.raises(query.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
            replace(result, request=cast(ProfileQueryRequest, bad))
    manifest = replace(result.profile.manifest, days=result.profile.manifest.days[:1])
    profile = replace(result.profile, manifest=manifest)
    selection = replace(result.selection, manifest=manifest, decision_time_ms=86400000)
    single = replace(request, end_ms=86400000, revision=manifest.days[0].revision)
    valid = query.LiveProfileQueryResult(single, selection, profile)
    assert valid.request is single
    with pytest.raises(query.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        replace(
            valid,
            request=replace(
                single, revision=single.revision + 1 if single.revision else 2
            ),
        )


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


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("REVOKED", "SNAPSHOT_REVOKED"),
        ("TOO_LARGE", "QUERY_TOO_LARGE"),
        ("ARITHMETIC", "INVALID_PROFILE"),
        ("INTEGRITY", "INVALID_PROFILE"),
        ("NATIVE_UNAVAILABLE", "BACKEND_UNAVAILABLE"),
        ("NATIVE_FAILURE", "BACKEND_UNAVAILABLE"),
        ("SECRET", None),
    ],
)
def test_composition_mapping(
    monkeypatch: pytest.MonkeyPatch, reason: str, expected: str | None
) -> None:
    request, provider, compose, context = setup(monkeypatch)
    compose.side_effect = query.ProfileCompositionError(reason)
    if expected:
        assert query.query_live_profile(
            provider, request, context
        ) == query.LiveProfileQueryUnavailable(expected)
    else:
        with pytest.raises(query.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
            query.query_live_profile(provider, request, context)
    compose.assert_called_once()


@pytest.mark.parametrize("phase", ["list_candidates", "get_manifest"])
def test_provider_errors_propagate(monkeypatch: pytest.MonkeyPatch, phase: str) -> None:
    request, provider, compose, context = setup(monkeypatch)
    error = RuntimeError("SECRET")
    getattr(provider, phase).side_effect = error
    with pytest.raises(RuntimeError) as caught:
        query.query_live_profile(provider, request, context)
    assert caught.value is error
    compose.assert_not_called()
    if phase == "list_candidates":
        provider.get_manifest.assert_not_called()


def test_success_dto_rejects_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, compose, context = setup(monkeypatch)
    result = query.query_live_profile(provider, request, context)
    assert isinstance(result, query.LiveProfileQueryResult)
    with pytest.raises(query.ProfileQueryError):
        query.LiveProfileQueryResult(
            result.request, result.selection, cast(CompositeProfile, True)
        )
    changed = replace(
        result.profile.manifest,
        days=tuple(replace(d, revision=2) for d in result.profile.manifest.days),
    )
    with pytest.raises(query.ProfileQueryError):
        replace(result, profile=replace(result.profile, manifest=changed))


def test_explicit_single_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, compose, _ = setup(monkeypatch)
    read = provider.get_manifest.return_value
    manifest = replace(read.manifest, days=read.manifest.days[:1])
    read = replace(read, manifest=manifest, days=read.days[:1])
    provider.get_manifest.return_value = read
    provider.list_candidates.return_value = provider.list_candidates.return_value[:1]
    compose.return_value = replace(compose.return_value, manifest=manifest)
    request = replace(request, end_ms=86400000, revision=1)
    result = query.query_live_profile(
        provider, request, query.LiveSelectionContext(86400000)
    )
    assert isinstance(result, query.LiveProfileQueryResult)
    assert provider.list_candidates.call_args.kwargs["revision"] == 1
    assert result.selection.manifest == manifest
    provider.get_manifest.assert_called_once_with(manifest)


def test_new_revision_does_not_reselect(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, _, context = setup(monkeypatch)
    original = provider.get_manifest.return_value

    def race(manifest):
        assert manifest == original.manifest
        provider.list_candidates.return_value += tuple(
            replace(c, ref=replace(c.ref, snapshot_id=f"{i:064x}", revision=2))
            for i, c in enumerate(provider.list_candidates.return_value)
        )
        return original

    provider.get_manifest.side_effect = race
    result = query.query_live_profile(provider, request, context)
    assert isinstance(result, query.LiveProfileQueryResult)
    assert result.selection.manifest == original.manifest
    assert all(ref.revision == 1 for ref in result.profile.manifest.days)
    provider.list_candidates.assert_called_once()
    provider.get_manifest.assert_called_once()


def test_revocation_race_uses_real_composer(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.core.market_data.profiles import composite

    request, provider, _, context = setup(monkeypatch)
    read = provider.get_manifest.return_value
    event = ProfileInvalidation(
        "revoked",
        read.days[0].ref.snapshot_id,
        "BAD",
        NOW.replace(microsecond=0),
        None,
        "test",
    )
    provider.get_manifest.return_value = replace(
        read, days=(replace(read.days[0], invalidations=(event,)), read.days[1])
    )
    loader = Mock(side_effect=AssertionError("native not allowed"))
    monkeypatch.setattr(composite, "_load_native", loader)
    monkeypatch.setattr(query, "compose_profile", composite.compose_profile)
    assert query.query_live_profile(
        provider, request, context
    ) == query.LiveProfileQueryUnavailable("SNAPSHOT_REVOKED")
    loader.assert_not_called()
    provider.list_candidates.assert_called_once()
    provider.get_manifest.assert_called_once()


def test_pre_io_validation_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, compose, context = setup(monkeypatch)
    invoke = cast(Callable[..., object], query.query_live_profile)
    recorded = replace(
        request,
        purpose="MODELED_RESEARCH",
        freshness_policy_id=None,
        availability_policy_id="a",
        as_of_ms=0,
    )
    cases = [(True, context), (request, True), (recorded, context)]
    for field in (
        "freshness_policy_id",
        "base_grid_id",
        "algorithm_version",
        "output_grid_id",
    ):
        cases.append((replace(request, **{field: "SECRET"}), context))
    for bad_request, bad_context in cases:
        with pytest.raises(query.ProfileQueryError) as caught:
            invoke(provider, bad_request, bad_context)
        assert (
            str(caught.value) == "PROFILE_QUERY_INVALID"
            and caught.value.__cause__ is None
        )
        assert provider.mock_calls == [] and compose.call_count == 0


def test_dto_exact_contracts(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, _, context = setup(monkeypatch)
    result = query.query_live_profile(provider, request, context)
    assert isinstance(result, query.LiveProfileQueryResult)
    selected = result.selection
    derived = type("SelectionSubclass", (type(selected),), {})(
        selected.manifest,
        selected.decision_time_ms,
        selected.available_at_ms,
        selected.policy,
    )
    constructor = cast(Callable[..., object], query.LiveProfileQueryResult)
    for invalid in (True, derived):
        with pytest.raises(query.ProfileQueryError):
            constructor(result.request, invalid, result.profile)
    unavailable = cast(Callable[..., object], query.LiveProfileQueryUnavailable)
    for invalid in (True, "SECRET", type("Text", (str,), {})("NOT_READY")):
        with pytest.raises(query.ProfileQueryError):
            unavailable(invalid)
    for reason in (
        "NOT_READY",
        "PROFILE_EXPIRED",
        "SNAPSHOT_REVOKED",
        "QUERY_TOO_LARGE",
        "INVALID_PROFILE",
        "BACKEND_UNAVAILABLE",
    ):
        assert query.LiveProfileQueryUnavailable(reason).reason == reason
