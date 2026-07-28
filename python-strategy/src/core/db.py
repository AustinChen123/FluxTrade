import os
import threading
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

from src.core.database_url import build_postgres_url


_lock = threading.RLock()
_engine = None
_session_factory = None


def _build_database_url() -> URL:
    """Build the DATABASE_URL from environment variables."""
    load_dotenv(os.path.join(os.path.dirname(__file__), '../../../.env'))
    return build_postgres_url(os.environ)


def get_engine():
    """Return the SQLAlchemy engine, creating it lazily on first call.

    Thread-safe via module-level lock.
    """
    global _engine
    if _engine is not None:
        return _engine
    with _lock:
        if _engine is not None:
            return _engine
        _engine = create_engine(_build_database_url(), echo=False)
        return _engine


def get_session_factory():
    """Return the session factory, creating it lazily on first call."""
    global _session_factory
    if _session_factory is not None:
        return _session_factory
    with _lock:
        if _session_factory is not None:
            return _session_factory
        _session_factory = sessionmaker(
            autocommit=False, autoflush=False, bind=get_engine()
        )
        return _session_factory


def SessionLocal() -> Session:
    """Create a new database session from the lazy session factory.

    Drop-in replacement for the old module-level ``SessionLocal`` variable.
    Callers that did ``SessionLocal()`` still work unchanged.
    """
    factory = get_session_factory()
    return factory()


def get_db():
    """Dependency for getting DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def reset_engine() -> None:
    """Dispose the engine and reset module state.

    Useful for test cleanup or reconfiguration.
    """
    global _engine, _session_factory
    with _lock:
        if _engine is not None:
            _engine.dispose()
            _engine = None
        _session_factory = None
