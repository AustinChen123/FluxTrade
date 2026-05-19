"""Unit tests for ``src.core.migration_validators``.

These are pure unit tests against a mocked SQLAlchemy ``Connection``. No real
PostgreSQL is required because the helper builds two SQL statements whose
correctness is fully determined by string structure:

1. ``CREATE OR REPLACE FUNCTION pg_temp.is_valid_json ...``
2. ``UPDATE "<table>" SET "<col>" = NULL WHERE ... NOT pg_temp.is_valid_json(...)``

A future end-to-end migration test (see Task 0.7 in
``docs/internal/plans/2026-05-05-architecture-fixes-p0/``) will exercise the
helper against a real PostgreSQL container.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.core.migration_validators import (
    _validate_identifier,
    null_invalid_json_in_text_column,
)


def _make_mock_connection(rowcount: int = 0) -> MagicMock:
    """Return a ``Connection``-shaped mock whose ``execute`` returns a result
    object with the given ``rowcount``."""
    conn = MagicMock()
    result = MagicMock()
    result.rowcount = rowcount
    conn.execute.return_value = result
    return conn


def _executed_sql_strings(conn: MagicMock) -> list[str]:
    """Extract the rendered SQL string from each ``execute()`` call."""
    return [str(call.args[0]) for call in conn.execute.call_args_list]


# ---------------------------------------------------------------------------
# _validate_identifier
# ---------------------------------------------------------------------------


class TestValidateIdentifier:
    def test_accepts_simple_snake_case(self) -> None:
        assert _validate_identifier("table", "signal_audit") == "signal_audit"

    def test_accepts_underscore_prefix(self) -> None:
        assert _validate_identifier("column", "_internal_col") == "_internal_col"

    @pytest.mark.parametrize(
        "bad",
        [
            "signal_audit; DROP TABLE foo",  # statement injection
            "details_json --comment",        # SQL comment
            'col"; --',                      # quote injection
            "1leading_digit",                # digit start
            "with space",                    # whitespace
            "schema.table",                  # qualified name
            "",                              # empty
            "col\nname",                     # newline
        ],
    )
    def test_rejects_injection_attempts(self, bad: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("column", bad)

    def test_rejects_non_string_input(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            _validate_identifier("table", 123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# null_invalid_json_in_text_column
# ---------------------------------------------------------------------------


class TestNullInvalidJsonInTextColumn:
    def test_emits_create_function_then_update(self) -> None:
        conn = _make_mock_connection(rowcount=0)

        null_invalid_json_in_text_column(conn, "signal_audit", "details_json")

        sqls = _executed_sql_strings(conn)
        assert len(sqls) == 2, "expected exactly CREATE FUNCTION + UPDATE"

        create_sql, update_sql = sqls[0], sqls[1]
        assert "CREATE OR REPLACE FUNCTION pg_temp.is_valid_json" in create_sql
        assert "value::jsonb" in create_sql
        assert "EXCEPTION WHEN others" in create_sql

        assert update_sql.startswith('UPDATE "signal_audit"')
        assert 'SET "details_json" = NULL' in update_sql
        assert 'NOT pg_temp.is_valid_json("details_json")' in update_sql

    def test_quotes_identifiers_with_double_quotes(self) -> None:
        """Identifiers must be double-quoted so reserved words still work."""
        conn = _make_mock_connection(rowcount=0)

        null_invalid_json_in_text_column(conn, "order", "intent_payload")

        update_sql = _executed_sql_strings(conn)[1]
        assert '"order"' in update_sql
        assert '"intent_payload"' in update_sql

    def test_returns_rowcount_from_update(self) -> None:
        conn = _make_mock_connection(rowcount=42)

        affected = null_invalid_json_in_text_column(
            conn, "signal_audit", "details_json"
        )

        assert affected == 42

    def test_returns_zero_when_rowcount_is_negative(self) -> None:
        # SQLAlchemy/DB-API return -1 when rowcount is unknown; we coerce to 0.
        conn = _make_mock_connection(rowcount=-1)

        affected = null_invalid_json_in_text_column(
            conn, "signal_audit", "details_json"
        )

        assert affected == 0

    def test_returns_zero_when_rowcount_is_none(self) -> None:
        conn = MagicMock()
        result = MagicMock()
        result.rowcount = None
        conn.execute.return_value = result

        affected = null_invalid_json_in_text_column(
            conn, "signal_audit", "details_json"
        )

        assert affected == 0

    def test_rejects_injection_in_table_name(self) -> None:
        conn = _make_mock_connection()

        with pytest.raises(ValueError, match="Invalid table"):
            null_invalid_json_in_text_column(
                conn, "signal_audit; DROP TABLE strategy", "details_json"
            )
        # Must short-circuit before issuing any SQL.
        conn.execute.assert_not_called()

    def test_rejects_injection_in_column_name(self) -> None:
        conn = _make_mock_connection()

        with pytest.raises(ValueError, match="Invalid column"):
            null_invalid_json_in_text_column(
                conn, "signal_audit", 'details_json"; --'
            )
        conn.execute.assert_not_called()

    def test_rejects_qualified_table_name(self) -> None:
        conn = _make_mock_connection()

        # Schema-qualified names are rejected; callers must use search_path
        # instead. This keeps the identifier whitelist minimal.
        with pytest.raises(ValueError, match="Invalid table"):
            null_invalid_json_in_text_column(
                conn, "public.signal_audit", "details_json"
            )
        conn.execute.assert_not_called()
