"""SQL-based pre-validation helpers for Alembic migrations.

Designed to be called from inside a migration's ``upgrade()`` body to fix
malformed data **before** a structural change (e.g. ``ALTER COLUMN ... TYPE
JSONB``) that would otherwise abort the entire transaction.

Why pure SQL?
-------------
Some FluxTrade tables (notably ``signal_audit``) can grow to millions of rows.
Pulling every row into Python with ``cursor.fetchall()`` to inspect each value
would either OOM the migration container or take so long it appears stuck.
The helpers here run a single ``UPDATE`` driven by a session-local
``pg_temp.is_valid_json`` PL/pgSQL function so that validation happens entirely
inside PostgreSQL and only the affected rows are touched.

PostgreSQL 16 ships ``pg_input_is_valid('jsonb', text)`` natively but FluxTrade
targets PostgreSQL 15, so this module deliberately implements a PG15-compatible
path (try-cast inside an ``EXCEPTION`` block).
"""

from __future__ import annotations

import re

from sqlalchemy import text
from sqlalchemy.engine import Connection

# Identifiers we accept must be plain ASCII names. This blocks obvious SQL
# injection vectors (semicolons, comments, quotes, whitespace) without trying
# to fully parse PostgreSQL identifier grammar — migrations are authored by
# trusted code, but defence-in-depth is cheap.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(kind: str, value: str) -> str:
    """Reject any string that does not look like a bare SQL identifier.

    Raises:
        ValueError: if ``value`` contains characters outside ``[A-Za-z0-9_]``
            or does not start with a letter/underscore.
    """
    if not isinstance(value, str) or not _IDENT_RE.match(value):
        raise ValueError(
            f"Invalid {kind} identifier: {value!r}. "
            "Only [A-Za-z_][A-Za-z0-9_]* is permitted."
        )
    return value


def null_invalid_json_in_text_column(
    connection: Connection,
    table_name: str,
    column_name: str,
) -> int:
    """NULL out every row whose TEXT column does not parse as JSONB.

    Used as a one-shot data-cleanup step prior to ``ALTER COLUMN ... TYPE
    JSONB`` migrations. The validation is done with a session-local PL/pgSQL
    helper (``pg_temp.is_valid_json``) so no rows are streamed to Python and
    the cleanup is committed inside the migration's own transaction.

    Args:
        connection: An active SQLAlchemy ``Connection`` bound to PostgreSQL.
            Typically ``op.get_bind()`` from inside an Alembic migration.
        table_name: Bare SQL identifier of the target table (e.g.
            ``"signal_audit"``).
        column_name: Bare SQL identifier of the TEXT column to clean
            (e.g. ``"details_json"``).

    Returns:
        The number of rows whose value was set to ``NULL``.

    Raises:
        ValueError: If ``table_name`` or ``column_name`` is not a plain
            ASCII identifier (defence against SQL injection).
    """
    safe_table = _validate_identifier("table", table_name)
    safe_column = _validate_identifier("column", column_name)

    # 1. Define a session-local validator. ``pg_temp`` is automatically
    #    dropped at session end, so no cleanup is required and the function
    #    cannot leak across migrations.
    create_validator_sql = text(
        """
        CREATE OR REPLACE FUNCTION pg_temp.is_valid_json(value text)
        RETURNS boolean AS $$
        BEGIN
            IF value IS NULL THEN
                RETURN TRUE;
            END IF;
            PERFORM value::jsonb;
            RETURN TRUE;
        EXCEPTION WHEN others THEN
            RETURN FALSE;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    connection.execute(create_validator_sql)

    # 2. Single-statement cleanup. Identifiers are validated above, so
    #    f-string interpolation is safe here.
    update_sql = text(
        f'UPDATE "{safe_table}" '
        f'SET "{safe_column}" = NULL '
        f'WHERE "{safe_column}" IS NOT NULL '
        f'AND NOT pg_temp.is_valid_json("{safe_column}")'
    )
    result = connection.execute(update_sql)
    # rowcount is documented to return -1 when unknown; coerce to 0 so
    # callers can rely on a non-negative integer.
    rowcount = result.rowcount
    return rowcount if rowcount and rowcount > 0 else 0
