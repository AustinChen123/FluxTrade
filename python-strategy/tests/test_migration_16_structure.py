"""Offline PostgreSQL DDL checks; not a substitute for the explicit real-PG lane."""

import importlib.util
import io
import re
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[2] / "database" / "alembic"
REVISION = "6c2f8a91d4e7"
MIGRATION = ROOT / "versions" / f"{REVISION}_add_volume_profile_storage.py"

def migration_sql(direction: str) -> str:
    spec = importlib.util.spec_from_file_location("vp_migration", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        dialect_opts={"paramstyle": "named"},
        opts={"as_sql": True, "output_buffer": output},
    )
    with Operations.context(context):
        getattr(module, direction)()
    return " ".join(output.getvalue().split())

def test_revision_chain_has_one_new_head() -> None:
    config = Config()
    config.set_main_option("script_location", str(ROOT))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [REVISION]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "4e8c1a2b7d90"
    assert re.fullmatch(r"[0-9a-f]{12}", REVISION)

def test_postgresql_ddl_constraints_registry_and_indexes() -> None:
    sql = migration_sql("upgrade")
    for table in ("snapshot", "bin", "ingest_job"):
        assert f"CREATE TABLE volume_profile_{table}" in sql
    for forbidden in ("venue", "market_type", "symbol"):
        assert not re.search(rf"\b{forbidden}\s+(?:VARCHAR|TEXT)", sql)
    assert sql.count("REFERENCES product(id)") == 2
    assert "REFERENCES volume_profile_snapshot(id) ON DELETE CASCADE" in sql
    assert (
        "completed_snapshot_id VARCHAR(64) REFERENCES volume_profile_snapshot(id)"
        in sql
    )
    assert "PRIMARY KEY (snapshot_id, bin_index)" in sql
    for constraint in (
        "window_start_ms >= 0 AND window_end_ms > window_start_ms",
        "window_start_ms % 86400000 = 0 AND window_end_ms % 86400000 = 0",
        "window_end_ms - window_start_ms = 86400000",
        "period = '1d' AND timezone = 'UTC'",
        "bin_step > 0",
        "revision > 0",
        "content_sha256 ~ '^[0-9a-f]{64}$'",
        "config_sha256 ~ '^[0-9a-f]{64}$'",
        "base_volume >= 0 AND quote_volume >= 0 AND aggregate_count >= 0 AND occupied_bins >= 0",
        "base_volume > 0 AND quote_volume > 0 AND aggregate_count > 0",
        "quality IN ('VERIFIED', 'PARTIAL', 'CONFLICT')",
        "(quality = 'VERIFIED') = (published_at IS NOT NULL)",
        "availability_basis IN ('OBSERVED', 'MODELED')",
        "raw_retention_state IN ('PRESENT', 'DELETED', 'NOT_STORED')",
        "id ~ '^[A-Za-z0-9_-]{1,64}$'",
        "status IN ('RUNNING', 'RETRYABLE', 'DONE', 'FAILED')",
        "attempt >= 0",
        "(lease_owner IS NULL) = (lease_expires_at IS NULL)",
    ):
        assert constraint in sql
    for field in (
        "source_manifest",
        "reconciliation",
        "source_cursor",
        "progress_manifest",
    ):
        assert f"{field} JSONB NOT NULL" in sql
        assert f"jsonb_typeof({field}) = 'object'" in sql
    logical = "product_id, window_start_ms, window_end_ms, grid_id, algorithm_version"
    assert f"CONSTRAINT uq_vp_snapshot_revision UNIQUE ( {logical}, revision)" in sql
    assert (
        f"CONSTRAINT uq_vp_snapshot_content UNIQUE ( {logical}, content_sha256)" in sql
    )
    assert (
        "CREATE INDEX ix_vp_snapshot_reader ON volume_profile_snapshot (product_id, grid_id, window_start_ms, window_end_ms, algorithm_version, quality, published_at)"
        in sql
    )
    assert (
        "CREATE INDEX ix_vp_job_retry_lease ON volume_profile_ingest_job (status, retry_after_at, lease_expires_at)"
        in sql
    )
    assert "CHECK (status = 'DONE'" not in sql
    assert sql.count("NOT NULL DEFAULT now()") == 2
    finite = "NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)"
    assert sql.count(finite) == 6
    for table, columns in (
        (
            "volume_profile_snapshot",
            ("bin_origin", "bin_step", "base_volume", "quote_volume"),
        ),
        ("volume_profile_bin", ("base_volume", "quote_volume")),
    ):
        for column in columns:
            assert (
                f"ALTER TABLE {table} ADD CONSTRAINT ck_{table}_{column}_finite "
                f"CHECK ({column} {finite})"
            ) in sql


def test_downgrade_is_fk_safe() -> None:
    assert migration_sql("downgrade") == (
        "DROP TABLE volume_profile_ingest_job; DROP TABLE volume_profile_bin; "
        "DROP TABLE volume_profile_snapshot;"
    )
