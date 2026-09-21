from unittest.mock import Mock
import subprocess
import sys

import pytest

from src.control_plane.profile_http_contract import ProfileHttpError, profile_http_error
from src.core.market_data.profiles import live_query, live_validation
from src.core.market_data.profiles.read_results import ProfileReadTooLarge
from test_profile_live_query import setup


@pytest.mark.parametrize("phase", ["list_candidates", "get_manifest"])
def test_exact_limit_maps_through_validation_and_http(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    request, provider, compose, context = setup(monkeypatch)
    getattr(provider, phase).side_effect = ProfileReadTooLarge("SECRET")
    utc, mono = Mock(side_effect=[context.decision_time_ms]), Mock(side_effect=[0])
    result = live_validation.validate_live_profile(
        provider, request, utc_ms=utc, monotonic_ms=mono
    )
    assert result == live_query.LiveProfileQueryUnavailable(request, "QUERY_TOO_LARGE")
    assert isinstance(result, live_query.LiveProfileQueryUnavailable)
    assert profile_http_error(result) == ProfileHttpError(400, "QUERY_TOO_LARGE")
    assert "SECRET" not in repr(result)
    assert utc.call_count == mono.call_count == 1
    compose.assert_not_called()
    provider.list_candidates.assert_called_once()
    if phase == "list_candidates":
        provider.get_manifest.assert_not_called()
    else:
        provider.get_manifest.assert_called_once()


@pytest.mark.parametrize("phase", ["list_candidates", "get_manifest"])
@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("SECRET"),
        type("DerivedLimit", (ProfileReadTooLarge,), {})("SECRET"),
    ],
)
def test_other_provider_failures_keep_identity(
    monkeypatch: pytest.MonkeyPatch, phase: str, error: Exception
) -> None:
    request, provider, compose, context = setup(monkeypatch)
    getattr(provider, phase).side_effect = error
    with pytest.raises(type(error)) as caught:
        live_query.query_live_profile(provider, request, context)
    assert caught.value is error
    compose.assert_not_called()
    provider.list_candidates.assert_called_once()
    if phase == "list_candidates":
        provider.get_manifest.assert_not_called()
    else:
        provider.get_manifest.assert_called_once()


def test_reader_reexports_same_exception() -> None:
    from src.core.market_data.profiles.read_repository import (
        ProfileReadTooLarge as exported,
    )

    assert exported is ProfileReadTooLarge
    assert str(exported("unchanged")) == "unchanged"


def test_query_import_does_not_load_orm() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import src.core.market_data.profiles.live_query; "
            "assert not any(n.startswith('sqlalchemy') or n.endswith('.orm') for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
