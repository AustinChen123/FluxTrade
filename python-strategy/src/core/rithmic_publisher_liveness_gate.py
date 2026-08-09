import logging
import threading
from enum import Enum
from typing import Protocol


class _RedisLivenessClient(Protocol):
    def get(self, key: str) -> object: ...

    def close(self) -> None: ...


class RithmicPublisherLivenessState(Enum):
    UNARMED = "unarmed"
    UNCONFIRMED = "unconfirmed"
    CONFIRMED = "confirmed"
    LATCHED = "latched"


class RithmicPublisherLivenessGate:
    """Fail-closed entry gate backed by the Rust publisher's Redis lease."""

    def __init__(
        self,
        *,
        redis_client: _RedisLivenessClient,
        key: str,
        logger: logging.Logger,
    ) -> None:
        self._redis_client = redis_client
        self._key = key
        self._logger = logger
        self._state = RithmicPublisherLivenessState.UNARMED
        self._unconfirmed_logged = False
        self._lock = threading.Lock()

    @property
    def state(self) -> RithmicPublisherLivenessState:
        with self._lock:
            return self._state

    def arm(self) -> None:
        with self._lock:
            if self._state is RithmicPublisherLivenessState.UNARMED:
                self._state = RithmicPublisherLivenessState.UNCONFIRMED

    def observe(self) -> bool:
        with self._lock:
            if self._state in (
                RithmicPublisherLivenessState.UNARMED,
                RithmicPublisherLivenessState.LATCHED,
            ):
                return False

            reason_code: str
            try:
                value = self._redis_client.get(self._key)
            except Exception:
                reason_code = "read_error"
            else:
                if value == "alive" or value == b"alive":
                    if self._state is RithmicPublisherLivenessState.UNCONFIRMED:
                        self._state = RithmicPublisherLivenessState.CONFIRMED
                        self._log(
                            logging.INFO,
                            event_code="publisher_liveness_confirmed",
                            reason_code="alive",
                        )
                    return True
                reason_code = "missing" if value is None else "invalid_value"

            if self._state is RithmicPublisherLivenessState.UNCONFIRMED:
                if not self._unconfirmed_logged:
                    self._unconfirmed_logged = True
                    self._log(
                        logging.WARNING,
                        event_code="publisher_liveness_unconfirmed",
                        reason_code=reason_code,
                    )
                return False

            self._state = RithmicPublisherLivenessState.LATCHED
            self._log(
                logging.WARNING,
                event_code="publisher_liveness_latched",
                reason_code=reason_code,
            )
            return False

    def close(self) -> None:
        try:
            self._redis_client.close()
        except Exception:
            self._log(
                logging.WARNING,
                event_code="publisher_liveness_client_close_failed",
                reason_code="close_error",
            )

    def _log(self, level: int, *, event_code: str, reason_code: str) -> None:
        self._logger.log(
            level,
            "Rithmic data publisher liveness transition",
            extra={
                "component": "rithmic_data_publisher",
                "event_code": event_code,
                "liveness_state": self._state.value,
                "reason_code": reason_code,
            },
        )
