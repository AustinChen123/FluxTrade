"""One-request internal HTTP transport; no redirects, retries, or decompression."""

import http.client
import math
import re
from urllib.parse import urlsplit

from .snapshot_client import ProfileSnapshotResponse


def _invalid() -> ValueError:
    return ValueError("PROFILE_HTTP_TRANSPORT_INVALID")


class ProfileSnapshotHttpTransport:
    """Fresh connections with socket timeouts, not an end-to-end deadline.

    The caller owns trusted internal origin selection and authentication headers.
    No credentials are stored here; HTTP proxies from the environment are unused.
    """

    def __init__(self, origin: str) -> None:
        if type(origin) is not str or any(
            ord(c) <= 32 or ord(c) >= 127 for c in origin
        ):
            raise _invalid()
        try:
            parsed = urlsplit(origin)
            host, port = parsed.hostname, parsed.port
            if (
                parsed.scheme not in ("http", "https")
                or not host
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path not in ("", "/")
                or "?" in origin
                or "#" in origin
                or parsed.netloc.endswith(":")
                or (port is not None and not 1 <= port <= 65535)
                or re.fullmatch(r"[A-Za-z0-9.:-]+", host) is None
            ):
                raise _invalid()
        except ValueError:
            raise _invalid() from None
        self._https = parsed.scheme == "https"
        self._host = host
        self._port = port

    def get(
        self,
        target: bytes,
        *,
        headers: tuple[tuple[str, str], ...],
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> ProfileSnapshotResponse:
        if (
            type(target) is not bytes
            or not target.startswith(b"/")
            or target.startswith(b"//")
            or any(c <= 32 or c >= 127 for c in target)
            or b"#" in target
            or type(timeout_seconds) not in (int, float)
            or not 0 < timeout_seconds <= 30
            or not math.isfinite(timeout_seconds)
            or type(max_response_bytes) is not int
            or not 1 <= max_response_bytes <= 2 * 1024 * 1024
            or type(headers) is not tuple
        ):
            raise _invalid()
        names: set[str] = set()
        for pair in headers:
            if (
                type(pair) is not tuple
                or len(pair) != 2
                or type(pair[0]) is not str
                or re.fullmatch(r"[A-Za-z0-9-]{1,64}", pair[0]) is None
                or type(pair[1]) is not str
                or len(pair[1]) > 4096
                or any(ord(c) < 32 or ord(c) >= 127 for c in pair[1])
                or pair[0].lower() in names
                or pair[0].lower() in ("host", "content-length", "transfer-encoding")
            ):
                raise _invalid()
            names.add(pair[0].lower())
        factory = (
            http.client.HTTPSConnection if self._https else http.client.HTTPConnection
        )
        connection = factory(self._host, port=self._port, timeout=timeout_seconds)
        try:
            connection.request("GET", target.decode("ascii"), headers=dict(headers))
            response = connection.getresponse()
            try:
                return ProfileSnapshotResponse(
                    response.status, response.read(max_response_bytes + 1)
                )
            finally:
                response.close()
        finally:
            connection.close()
