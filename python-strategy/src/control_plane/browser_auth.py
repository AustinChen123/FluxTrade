from __future__ import annotations

import json
import secrets
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from email.header import decode_header
from hmac import compare_digest
from http.cookies import SimpleCookie
from threading import Lock
from typing import Protocol
from urllib.parse import urlsplit


TAILSCALE_LOGIN_HEADER = "tailscale-user-login"
TAILSCALE_CAPABILITIES_HEADER = "tailscale-app-capabilities"
SESSION_COOKIE_NAME = "__Host-fluxtrade_session"
MAX_ACTOR_LENGTH = 64


class BrowserAuthRejected(ValueError):
    """Raised when trusted-proxy identity or browser proof is invalid."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class BrowserPrincipal:
    actor: str
    capabilities: frozenset[str]
    csrf_token: str
    session_token: str
    expires_at: float
    step_up_expires_at: float | None

    def has_capability(self, capability: str) -> bool:
        return capability in self.capabilities

    def has_step_up(self, capability: str, *, now: float) -> bool:
        return (
            capability in self.capabilities
            and self.step_up_expires_at is not None
            and now < self.step_up_expires_at
        )


class BrowserSessionStore(Protocol):
    def put(self, principal: BrowserPrincipal, *, now: float) -> None:
        ...

    def get(self, token: str, *, now: float) -> BrowserPrincipal | None:
        ...

    def delete(self, token: str) -> None:
        ...


class InMemoryBrowserSessionStore:
    def __init__(
        self,
        *,
        max_sessions: int = 1_024,
        max_sessions_per_actor: int = 8,
    ) -> None:
        if max_sessions <= 0:
            raise ValueError("max_sessions must be positive")
        if max_sessions_per_actor <= 0:
            raise ValueError("max_sessions_per_actor must be positive")
        self._sessions: dict[str, BrowserPrincipal] = {}
        self._lock = Lock()
        self._max_sessions = max_sessions
        self._max_sessions_per_actor = max_sessions_per_actor

    def put(self, principal: BrowserPrincipal, *, now: float) -> None:
        with self._lock:
            self._discard_expired_locked(now)
            self._sessions.pop(principal.session_token, None)
            actor_sessions = [
                session
                for session in self._sessions.values()
                if session.actor == principal.actor
            ]
            if len(actor_sessions) >= self._max_sessions_per_actor:
                self._evict_locked(actor_sessions)
            if len(self._sessions) >= self._max_sessions:
                self._evict_locked(list(self._sessions.values()))
            self._sessions[principal.session_token] = principal

    def get(self, token: str, *, now: float) -> BrowserPrincipal | None:
        with self._lock:
            self._discard_expired_locked(now)
            return self._sessions.get(token)

    def delete(self, token: str) -> None:
        with self._lock:
            self._sessions.pop(token, None)

    def _discard_expired_locked(self, now: float) -> None:
        expired = [
            token
            for token, principal in self._sessions.items()
            if now >= principal.expires_at
        ]
        for token in expired:
            self._sessions.pop(token, None)

    def _evict_locked(self, candidates: list[BrowserPrincipal]) -> None:
        oldest = min(
            candidates,
            key=lambda principal: (
                principal.expires_at,
                principal.session_token,
            ),
        )
        self._sessions.pop(oldest.session_token, None)


class BrowserAuthProvider(Protocol):
    operator_capability: str

    def issue(self, headers: Mapping[str, str] | None) -> BrowserPrincipal:
        ...

    def authenticate(
        self,
        headers: Mapping[str, str] | None,
    ) -> BrowserPrincipal | None:
        ...

    def revoke(self, principal: BrowserPrincipal) -> None:
        ...

    def require_same_origin(self, headers: Mapping[str, str] | None) -> None:
        ...

    def require_csrf(
        self,
        principal: BrowserPrincipal,
        headers: Mapping[str, str] | None,
    ) -> None:
        ...

    def has_step_up(self, principal: BrowserPrincipal) -> bool:
        ...

    def session_cookie(self, principal: BrowserPrincipal) -> str:
        ...

    def expired_cookie(self) -> str:
        ...


class BrowserSessionAuth:
    """In-process browser sessions issued from trusted proxy identity headers."""

    def __init__(
        self,
        *,
        allowed_origin: str,
        operator_capability: str,
        step_up_capability: str,
        session_ttl_seconds: int = 28_800,
        step_up_ttl_seconds: int = 300,
        clock: Callable[[], float] = time.time,
        session_store: BrowserSessionStore | None = None,
    ) -> None:
        self.allowed_origin = _validate_origin(allowed_origin)
        if not operator_capability:
            raise ValueError("operator_capability must be non-empty")
        if not step_up_capability:
            raise ValueError("step_up_capability must be non-empty")
        if operator_capability == step_up_capability:
            raise ValueError("operator and step-up capabilities must be distinct")
        if session_ttl_seconds <= 0:
            raise ValueError("session_ttl_seconds must be positive")
        if step_up_ttl_seconds <= 0 or step_up_ttl_seconds > 300:
            raise ValueError("step_up_ttl_seconds must be between 1 and 300")
        self.operator_capability = operator_capability
        self.step_up_capability = step_up_capability
        self.session_ttl_seconds = session_ttl_seconds
        self.step_up_ttl_seconds = step_up_ttl_seconds
        self._clock = clock
        self._session_store = (
            session_store
            if session_store is not None
            else InMemoryBrowserSessionStore()
        )

    def issue(self, headers: Mapping[str, str] | None) -> BrowserPrincipal:
        normalized = _normalized_headers(headers)
        self.require_same_origin(normalized)
        actor = _trusted_actor(
            normalized.get(TAILSCALE_LOGIN_HEADER),
            missing_reason="trusted_identity_missing",
        )
        capabilities = _parse_capabilities(
            normalized.get(TAILSCALE_CAPABILITIES_HEADER)
        )
        now = self._clock()
        token = secrets.token_urlsafe(32)
        principal = BrowserPrincipal(
            actor=actor,
            capabilities=capabilities,
            csrf_token=secrets.token_urlsafe(32),
            session_token=token,
            expires_at=now + self.session_ttl_seconds,
            step_up_expires_at=(
                now + self.step_up_ttl_seconds
                if self.step_up_capability in capabilities
                else None
            ),
        )
        self._session_store.put(principal, now=now)
        return principal

    def authenticate(
        self,
        headers: Mapping[str, str] | None,
    ) -> BrowserPrincipal | None:
        token = _extract_session_token(headers)
        if token is None:
            return None
        now = self._clock()
        principal = self._session_store.get(token, now=now)
        if principal is None:
            return None
        normalized = _normalized_headers(headers)
        actor = _trusted_actor(
            normalized.get(TAILSCALE_LOGIN_HEADER),
            missing_reason="trusted_identity_mismatch",
        )
        if actor != principal.actor:
            raise BrowserAuthRejected("trusted_identity_mismatch")
        capabilities = _parse_capabilities(
            normalized.get(TAILSCALE_CAPABILITIES_HEADER)
        )
        return replace(principal, capabilities=capabilities)

    def revoke(self, principal: BrowserPrincipal) -> None:
        self._session_store.delete(principal.session_token)

    def require_same_origin(self, headers: Mapping[str, str] | None) -> None:
        origin = _normalized_headers(headers).get("origin")
        if origin is None or origin != self.allowed_origin:
            raise BrowserAuthRejected("origin_rejected")

    def require_csrf(
        self,
        principal: BrowserPrincipal,
        headers: Mapping[str, str] | None,
    ) -> None:
        supplied = _normalized_headers(headers).get("x-csrf-token")
        if supplied is None or not compare_digest(supplied, principal.csrf_token):
            raise BrowserAuthRejected("csrf_rejected")

    def has_step_up(self, principal: BrowserPrincipal) -> bool:
        return principal.has_step_up(
            self.step_up_capability,
            now=self._clock(),
        )

    def session_cookie(self, principal: BrowserPrincipal) -> str:
        return (
            f"{SESSION_COOKIE_NAME}={principal.session_token}; "
            f"Max-Age={self.session_ttl_seconds}; Path=/; "
            "Secure; HttpOnly; SameSite=Strict"
        )

    @staticmethod
    def expired_cookie() -> str:
        return (
            f"{SESSION_COOKIE_NAME}=; Max-Age=0; Path=/; "
            "Secure; HttpOnly; SameSite=Strict"
        )


def _validate_origin(value: str) -> str:
    if value != value.rstrip("/"):
        raise ValueError("allowed_origin must not have a trailing slash")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("allowed_origin must be an HTTPS origin")
    return value


def _normalized_headers(
    headers: Mapping[str, str] | None,
) -> dict[str, str]:
    if headers is None:
        return {}
    return {str(key).lower(): str(value) for key, value in headers.items()}


def _decode_header_value(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        decoded_parts = decode_header(value)
        return "".join(
            part.decode(encoding or "ascii") if isinstance(part, bytes) else part
            for part, encoding in decoded_parts
        )
    except (LookupError, UnicodeDecodeError):
        raise BrowserAuthRejected("trusted_header_invalid") from None


def _trusted_actor(value: str | None, *, missing_reason: str) -> str:
    actor = _decode_header_value(value)
    if not actor:
        raise BrowserAuthRejected(missing_reason)
    if (
        actor != actor.strip()
        or len(actor) > MAX_ACTOR_LENGTH
        or _contains_control_character(actor)
    ):
        raise BrowserAuthRejected("trusted_identity_invalid")
    return actor


def _parse_capabilities(value: str | None) -> frozenset[str]:
    if value is None:
        return frozenset()
    decoded = _decode_header_value(value)
    try:
        payload = json.loads(decoded or "")
    except json.JSONDecodeError:
        raise BrowserAuthRejected("capabilities_invalid") from None
    if not isinstance(payload, dict):
        raise BrowserAuthRejected("capabilities_invalid")
    for key, entries in payload.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(entries, list)
            or any(not isinstance(entry, dict) for entry in entries)
        ):
            raise BrowserAuthRejected("capabilities_invalid")
    return frozenset(payload)


def _extract_session_token(headers: Mapping[str, str] | None) -> str | None:
    raw_cookie = _normalized_headers(headers).get("cookie")
    if raw_cookie is None:
        return None
    cookie = SimpleCookie()
    try:
        cookie.load(raw_cookie)
    except Exception:
        return None
    morsel = cookie.get(SESSION_COOKIE_NAME)
    return None if morsel is None else morsel.value


def _contains_control_character(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)
