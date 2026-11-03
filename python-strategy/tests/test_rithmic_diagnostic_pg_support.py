"""Small disposable PostgreSQL schema contract for F4 JSONB tests."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.skipif(
    os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
    reason="set FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS=1 to use disposable PostgreSQL",
)


class NoopAdapter:
    def close(self) -> None:
        pass


def postgres_url() -> str:
    return (
        "postgresql+psycopg2://"
        f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@"
        f"{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/"
        f"{os.getenv('POSTGRES_DB', 'postgres')}"
    )


def create_diagnostic_schema(engine: object, schema: str) -> None:
    with engine.begin() as connection:  # type: ignore[attr-defined]
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(
            text(
                f'CREATE TABLE "{schema}".system_events ('
                "id BIGSERIAL PRIMARY KEY, event_type VARCHAR(64) NOT NULL, "
                "event_subtype VARCHAR(64), related_strategy_id VARCHAR, "
                "related_order_id VARCHAR, related_gene_id BIGINT, "
                "payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
        )
        connection.execute(
            text(
                f'''CREATE FUNCTION "{schema}".reject_diagnostic() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                      IF NEW.payload #>> '{{safe_diagnostic,raw_bits}}' = '123' THEN
                    RAISE EXCEPTION 'injected diagnostic statement failure';
                  END IF;
                  RETURN NEW;
                END $$'''
            )
        )
        connection.execute(
            text(
                f'CREATE TRIGGER reject_diagnostic BEFORE INSERT ON "{schema}".system_events '
                f'FOR EACH ROW EXECUTE FUNCTION "{schema}".reject_diagnostic()'
            )
        )


def test_disposable_diagnostic_schema_has_jsonb_rejection_gate() -> None:
    pytest.importorskip("psycopg2")
    engine = create_engine(postgres_url(), connect_args={"connect_timeout": 3})
    schema = f"f4diag_support_{uuid4().hex}"
    try:
        create_diagnostic_schema(engine, schema)
        with engine.connect() as connection:
            table = connection.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = 'system_events' "
                    "AND column_name = 'payload'"
                ),
                {"schema": schema},
            ).scalar_one()
        assert table == "jsonb"
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()
