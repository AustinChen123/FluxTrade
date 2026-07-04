"""Helpers for live order stream keepalive and REST resync orchestration."""

from dataclasses import dataclass
import logging

from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeUserStreamUnsupported,
    IExchangeAdapter,
)


@dataclass(frozen=True)
class UserStreamKeepaliveResult:
    action: str
    attempts: int
    listen_key: str | None = None
    error: str | None = None


class UserStreamKeepalive:
    """Manage one exchange user-data listen key without owning a thread."""

    def __init__(
        self,
        adapter: IExchangeAdapter,
        *,
        max_attempts: int = 2,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.adapter = adapter
        self.max_attempts = max_attempts
        self.listen_key: str | None = None
        self.logger = logger or logging.getLogger("UserStreamKeepalive")

    def keepalive_once(self) -> UserStreamKeepaliveResult:
        """Create or refresh the listen key, retrying once with a new key."""
        attempts = 0
        last_error: str | None = None
        while attempts < self.max_attempts:
            attempts += 1
            try:
                if self.listen_key is None:
                    self.listen_key = self.adapter.create_user_stream_listen_key()
                self.adapter.keepalive_user_stream(self.listen_key)
                return UserStreamKeepaliveResult(
                    action="refreshed",
                    attempts=attempts,
                    listen_key=self.listen_key,
                )
            except ExchangeUserStreamUnsupported as e:
                return UserStreamKeepaliveResult(
                    action="unsupported",
                    attempts=attempts,
                    error=str(e),
                )
            except ExchangeError as e:
                last_error = str(e)
                self.logger.warning(
                    "User stream keepalive attempt %s/%s failed: %s",
                    attempts,
                    self.max_attempts,
                    e,
                )
                self.listen_key = None

        return UserStreamKeepaliveResult(
            action="failed",
            attempts=attempts,
            error=last_error,
        )
