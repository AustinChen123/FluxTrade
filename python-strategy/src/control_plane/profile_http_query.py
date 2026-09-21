"""Injected synchronous profile query service; no retries or application wiring."""

import logging
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

logger = logging.getLogger(__name__)


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
        phase = "parse"
        try:
            request = parse_profile_query(raw_query)
            phase = "validate"
            evidence = validate_live_profile(
                self._provider, request, utc_ms=self._utc, monotonic_ms=self._monotonic
            )
            if isinstance(evidence, LiveProfileQueryUnavailable):
                result = profile_http_error(evidence)
                if result.status >= 500:
                    logger.error(
                        "profile_backend_failure phase=validate reason=%s",
                        evidence.reason,
                    )
                return result
            if isinstance(evidence, LiveProfileValidationUnavailable):
                logger.error(
                    "profile_backend_failure phase=validate reason=%s", evidence.reason
                )
                return ProfileHttpError(503, "BACKEND_UNAVAILABLE")
            phase = "served_clock"
            served_at_ms = self._utc()
            phase = "encode"
            result = encode_profile_success(evidence, served_at_ms=served_at_ms)
            if isinstance(result, ProfileHttpError) and result.status >= 500:
                logger.error(
                    "profile_backend_failure phase=encode code=%s", result.code
                )
            return result
        except Exception as error:
            result = profile_http_error(error)
            if result.status >= 500:
                logger.error(
                    "profile_backend_failure phase=%s exception_type=%s",
                    phase,
                    type(error).__name__,
                )
            return result
