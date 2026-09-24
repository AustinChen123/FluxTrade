from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError


_REQUIRED_POSTGRES_SETTINGS = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
)


def select_migration_url(
    configured_url: str | None,
    settings: Mapping[str, str | None],
    expected_database: object = None,
) -> URL:
    """Prefer an explicit Alembic URL; placeholder configuration requires full env.

    An optional isolated-database guard is checked before engine construction.
    Parsing/guard errors deliberately omit URLs, credentials and database names.
    """
    if configured_url and configured_url != "driver://user:pass@localhost/dbname":
        try:
            url = make_url(configured_url)
        except (ArgumentError, TypeError, ValueError):
            # SQLAlchemy URL parsing can raise ArgumentError with the raw input.
            raise ValueError("invalid configured migration URL") from None
    else:
        url = build_postgres_url(settings)
    if expected_database is not None and (
        not isinstance(expected_database, str)
        or not expected_database
        or url.database != expected_database
        or bool(url.query)
    ):
        raise ValueError("migration database isolation mismatch")
    return url


def build_postgres_url(settings: Mapping[str, str | None]) -> URL:
    """Build a safely encoded PostgreSQL URL from explicit settings."""
    missing = [
        name
        for name in _REQUIRED_POSTGRES_SETTINGS
        if not str(settings.get(name) or "").strip()
    ]
    if missing:
        raise ValueError(
            "missing PostgreSQL settings: " + ", ".join(sorted(missing))
        )

    raw_port = str(settings["POSTGRES_PORT"])
    try:
        port = int(raw_port)
    except ValueError:
        raise ValueError("POSTGRES_PORT must be an integer") from None
    if not 1 <= port <= 65_535:
        raise ValueError("POSTGRES_PORT must be between 1 and 65535")

    return URL.create(
        "postgresql",
        username=str(settings["POSTGRES_USER"]),
        password=str(settings["POSTGRES_PASSWORD"]),
        host=str(settings["POSTGRES_HOST"]),
        port=port,
        database=str(settings["POSTGRES_DB"]),
    )
