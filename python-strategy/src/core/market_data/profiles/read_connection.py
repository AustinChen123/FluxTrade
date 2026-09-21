"""Lazy, explicitly configured connection ownership for profile readers only.

Pool/connect timeouts are not DNS, statement, or end-to-end guarantees. Repositories
retain transaction/read-only/UTC/wait policy. No environment configuration is read.
Own errors are sanitized; SQLAlchemy/DBAPI exceptions are deliberately not redacted.
"""
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session


class ProfileReadConnectionConfigError(ValueError):
    """Invalid explicit connection configuration."""


class ProfileReadConnectionLifecycleError(RuntimeError):
    """The owner is closed or has active session contexts."""


@dataclass(frozen=True, slots=True)
class ProfileReadConnectionConfig:
    url: URL = field(repr=False)

    def __post_init__(self) -> None:
        url = self.url
        if (type(url) is not URL or type(url.drivername) is not str
                or url.drivername not in ("postgresql", "postgresql+psycopg2")
                or any(type(value) is not str or not value or "\x00" in value
                       for value in (url.username, url.password, url.host, url.database))
                or type(url.port) is not int or not 1 <= url.port <= 65535 or url.query
                or any(character.isspace() or character in ",/\\@" for character in url.host or "")):
            raise ProfileReadConnectionConfigError("invalid profile read connection configuration")
        object.__setattr__(self, "url", URL.create(
            "postgresql+psycopg2", username=url.username, password=url.password,
            host=url.host, port=url.port, database=url.database,
        ))


class ProfileReadConnection:
    """Own one lazy engine and independent sessions; admission occurs on CM entry.

    Engine creation failure leaves the owner OPEN: a later explicit sessions() entry
    may try again, but this owner never retries automatically. close() with active
    contexts fails without changing OPEN. Once CLOSED, disposal is never retried.
    Session contexts only close sessions; they do not establish transaction policy.
    """

    def __init__(self, config: ProfileReadConnectionConfig) -> None:
        if type(config) is not ProfileReadConnectionConfig:
            raise ProfileReadConnectionConfigError("invalid profile read connection configuration")
        config.__post_init__()
        self._config = config
        self._lock = Lock()
        self._engine: Engine | None = None
        self._active = 0
        self._closed = False

    @contextmanager
    def sessions(self) -> Iterator[Session]:
        with self._lock:
            if self._closed:
                raise ProfileReadConnectionLifecycleError("profile read connection is closed")
            if self._engine is None:
                self._engine = create_engine(
                    self._config.url, pool_size=2, max_overflow=0, pool_timeout=0.25,
                    connect_args={"connect_timeout": 2}, pool_pre_ping=False, pool_recycle=-1,
                )
            engine = self._engine
            self._active += 1
        try:
            with Session(bind=engine, autoflush=False) as session:
                yield session
        finally:
            with self._lock:
                self._active -= 1

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._active:
                raise ProfileReadConnectionLifecycleError("profile read connection has active sessions")
            self._closed = True
            engine = self._engine
        if engine is not None:
            engine.dispose()
