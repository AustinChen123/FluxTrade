"""One-shot cross-service snapshot boundary; transport owns bounded HTTP reads.

Malformed success is INVALID_PROFILE; transport/non-contract failure is
BACKEND_UNAVAILABLE. Neither is a trading or global-lockdown instruction.
"""

from dataclasses import dataclass, field
import json
import re
from typing import Protocol
from urllib.parse import urlencode

from .live_query import LiveProfileQueryUnavailable
from .read_types import ProfileQueryRequest
from .wire import ProfileWireError, ProfileWireEvidence, decode_live_profile_response

_PATH = "/api/v1/market-data/volume-profiles"
_MAX_BODY = 2 * 1024 * 1024
_ERROR_BODY = 1024
_MAX_AUTH_TOKEN_BYTES = 4096  # ASCII credentials: character and byte counts agree.
_OUTCOMES = {
    (404, "PROFILE_NOT_READY"): "NOT_READY",
    (404, "PROFILE_EXPIRED"): "PROFILE_EXPIRED",
    (409, "SNAPSHOT_REVOKED"): "SNAPSHOT_REVOKED",
    (400, "QUERY_TOO_LARGE"): "QUERY_TOO_LARGE",
    (503, "BACKEND_UNAVAILABLE"): "BACKEND_UNAVAILABLE",
}


class ProfileSnapshotClientError(ValueError):
    """Local programming/configuration error; never a retry instruction."""

    def __init__(self) -> None:
        super().__init__("PROFILE_SNAPSHOT_INVALID_REQUEST")


@dataclass(frozen=True, slots=True)
class ProfileSnapshotResponse:
    status: int
    body: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.status) is not int
            or not 100 <= self.status <= 599
            or type(self.body) is not bytes
        ):
            raise ProfileSnapshotClientError()


class ProfileSnapshotTransport(Protocol):
    def get(
        self,
        target: bytes,
        *,
        headers: tuple[tuple[str, str], ...],
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> ProfileSnapshotResponse:
        """Bound the delivered body during I/O; enforce timeout; do not log headers."""
        ...


def _error_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    if len(pairs) != 1 or pairs[0][0] != "error" or type(pairs[0][1]) is not str:
        raise ValueError
    return dict(pairs)


def _reject(value: str) -> object:
    raise ValueError


class ProfileSnapshotClient:
    """No retry/cache/observation; credentials are excluded from representations."""

    def __init__(
        self, transport: ProfileSnapshotTransport, *, auth_header: str, auth_token: str
    ) -> None:
        if (
            not callable(getattr(transport, "get", None))
            or type(auth_header) is not str
            or re.fullmatch(r"[A-Za-z0-9-]{1,64}", auth_header) is None
            or type(auth_token) is not str
            or not auth_token
            or len(auth_token) > _MAX_AUTH_TOKEN_BYTES
            or not auth_token.isascii()
            or any(ord(char) < 32 or ord(char) == 127 for char in auth_token)
        ):
            raise ProfileSnapshotClientError()
        self._transport = transport
        self._headers = ((auth_header, auth_token),)

    def fetch(
        self, request: ProfileQueryRequest
    ) -> ProfileWireEvidence | LiveProfileQueryUnavailable:
        if (
            type(request) is not ProfileQueryRequest
            or request.purpose != "LIVE_QUERY"
            or request.freshness_policy_id != "utc_complete_strict_v1"
        ):
            raise ProfileSnapshotClientError()
        params = [
            (name, str(getattr(request, name)))
            for name in (
                "product_id",
                "base_grid_id",
                "output_grid_id",
                "algorithm_version",
                "start_ms",
                "end_ms",
                "purpose",
                "freshness_policy_id",
            )
        ]
        if request.revision is not None:
            params.append(("revision", str(request.revision)))
        target = (_PATH + "?" + urlencode(params)).encode("ascii")
        try:
            response = self._transport.get(
                target,
                headers=self._headers,
                timeout_seconds=3.0,
                max_response_bytes=_MAX_BODY,
            )
        except Exception:
            return LiveProfileQueryUnavailable(request, "BACKEND_UNAVAILABLE")
        if type(response) is not ProfileSnapshotResponse:
            return LiveProfileQueryUnavailable(request, "BACKEND_UNAVAILABLE")
        if len(response.body) > _MAX_BODY:
            return LiveProfileQueryUnavailable(
                request,
                "QUERY_TOO_LARGE" if response.status == 200 else "BACKEND_UNAVAILABLE",
            )
        if response.status == 200:
            try:
                return decode_live_profile_response(request, response.body)
            except ProfileWireError as error:
                return LiveProfileQueryUnavailable(request, error.reason)
        try:
            if len(response.body) > _ERROR_BODY:
                raise ValueError
            envelope = json.loads(
                response.body.decode("utf-8"),
                object_pairs_hook=_error_object,
                parse_float=_reject,
                parse_constant=_reject,
            )
            if type(envelope) is not dict or set(envelope) != {"error"}:
                raise ValueError
            code = envelope["error"]
        except (ValueError, RecursionError):
            return LiveProfileQueryUnavailable(request, "BACKEND_UNAVAILABLE")
        if (response.status, code) == (400, "INVALID_REQUEST"):
            raise ProfileSnapshotClientError()
        return LiveProfileQueryUnavailable(
            request, _OUTCOMES.get((response.status, code), "BACKEND_UNAVAILABLE")
        )
