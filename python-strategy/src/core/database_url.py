from __future__ import annotations

from collections.abc import Mapping

from sqlalchemy.engine import URL


_REQUIRED_POSTGRES_SETTINGS = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
)


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
