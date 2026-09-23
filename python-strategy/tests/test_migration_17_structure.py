"""Offline invalidation DDL contracts; actual PostgreSQL DML acceptance is separate."""
import importlib.util
import io
import re

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory

from test_migration_16_structure import ROOT

REVISION = "7d3a9c02e5f8"


def migration_sql(direction: str) -> str:
    path = ROOT / "versions" / f"{REVISION}_add_market_data_invalidation.py"
    spec = importlib.util.spec_from_file_location("invalidation_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = io.StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        getattr(module, direction)()
    return " ".join(output.getvalue().split())


def test_invalidation_is_only_new_head() -> None:
    config = Config()
    config.set_main_option("script_location", str(ROOT))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["c84f1a92d607"]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "6c2f8a91d4e7"
    assert re.fullmatch(r"[0-9a-f]{12}", REVISION)


def test_exact_invalidation_schema_and_statement_guard() -> None:
    sql = migration_sql("upgrade")
    assert sql.count("CREATE TABLE") == 1 and "CREATE TABLE market_data_invalidation" in sql
    for fragment in (
        'event_id VARCHAR(128) COLLATE "C" NOT NULL PRIMARY KEY',
        "snapshot_id VARCHAR(64) NOT NULL REFERENCES volume_profile_snapshot(id) ON DELETE RESTRICT",
        "replacement_snapshot_id VARCHAR(64) REFERENCES volume_profile_snapshot(id) ON DELETE RESTRICT",
        "reason_code VARCHAR(64) NOT NULL", "source VARCHAR(64) NOT NULL",
        "recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp())",
        "event_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", "reason_code COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,64}$'",
        "source COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,64}$'", "snapshot_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'",
        "replacement_snapshot_id IS NULL OR (replacement_snapshot_id COLLATE \"C\" ~ '^[0-9a-f]{64}$' AND replacement_snapshot_id <> snapshot_id)",
        "recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00'",
        "recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'",
        "recorded_at = date_trunc('milliseconds', recorded_at)",
        "CREATE INDEX ix_mdi_snapshot_recorded_event ON market_data_invalidation (snapshot_id, recorded_at, event_id)",
        "CREATE INDEX ix_mdi_replacement ON market_data_invalidation (replacement_snapshot_id)",
        "CREATE FUNCTION reject_market_data_invalidation_mutation() RETURNS trigger LANGUAGE plpgsql",
        "RAISE EXCEPTION 'market data invalidation is append-only' USING ERRCODE = '55000'",
        "CREATE TRIGGER market_data_invalidation_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON market_data_invalidation FOR EACH STATEMENT EXECUTE FUNCTION reject_market_data_invalidation_mutation()",
    ):
        assert fragment in sql
    assert sql.count("ON DELETE RESTRICT") == 2 and "UNIQUE" not in sql
    assert "FOR EACH ROW" not in sql and "WHEN (" not in sql


def test_downgrade_only_drops_guard_table_function_in_order() -> None:
    assert migration_sql("downgrade") == (
        "DROP TRIGGER market_data_invalidation_append_only ON market_data_invalidation; "
        "DROP TABLE market_data_invalidation; DROP FUNCTION reject_market_data_invalidation_mutation();"
    )
