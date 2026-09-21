"""Injected synchronous profile query service; no retries or application wiring."""

from typing import Callable

from src.core.market_data.profiles.live_query import (
    LiveProfileQueryUnavailable,
    ProfileLiveProvider,
)
from src.core.market_data.profiles.live_validation import (
    LiveProfileValidationUnavailable,
    validate_live_profile,
)
from .profile_http_contract import (
    ProfileHttpError,
    parse_profile_query,
    profile_http_error,
)
from .profile_http_success import encode_profile_success


class ProfileQueryService:
    def __init__(
        self,
        provider: ProfileLiveProvider,
        *,
        utc_ms: Callable[[], int],
        monotonic_ms: Callable[[], int],
    ) -> None:
        if not all(
            callable(value)
            for value in (
                getattr(provider, "list_candidates", None),
                getattr(provider, "get_manifest", None),
                utc_ms,
                monotonic_ms,
            )
        ):
            raise ValueError("invalid profile query service") from None
        self._provider, self._utc, self._monotonic = provider, utc_ms, monotonic_ms

    def query(self, raw_query: bytes) -> bytes | ProfileHttpError:
        try:
            request = parse_profile_query(raw_query)
            evidence = validate_live_profile(
                self._provider, request, utc_ms=self._utc, monotonic_ms=self._monotonic
            )
            if isinstance(evidence, LiveProfileQueryUnavailable):
                return profile_http_error(evidence)
            if isinstance(evidence, LiveProfileValidationUnavailable):
                return ProfileHttpError(503, "BACKEND_UNAVAILABLE")
            return encode_profile_success(evidence, served_at_ms=self._utc())
        except Exception as error:
            return profile_http_error(error)
