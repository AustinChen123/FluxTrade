"""Additive immutable BUILD root and source-market tape DDL contract."""

import importlib.util
import io
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[2] / "database"
REVISION = "d8a61c3f9b42"


def _migration_sql(direction: str) -> str:
    path = ROOT / "alembic/versions" / f"{REVISION}_add_spider_source_market_receipts.py"
    spec = importlib.util.spec_from_file_location("spider_source_market_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
    )
    with Operations.context(context):
        getattr(module, direction)()
    return " ".join(output.getvalue().split())


def test_source_market_schema_is_additive_and_preserves_financial_refs():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [REVISION]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "c3f8d1a904b7"

    upgrade = _migration_sql("upgrade")
    assert "original_build_checkpoint BYTEA" in upgrade
    assert "original_build_checkpoint_sha256 VARCHAR(64) COLLATE \"C\"" in upgrade
    assert "SET original_build_checkpoint = runtime.source_checkpoint" in upgrade
    assert "SET NOT NULL" in upgrade
    assert "kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET')" in upgrade
    assert "local_order_id IS NOT NULL" in upgrade
    assert "action_id IS NOT NULL" in upgrade
    assert "role COLLATE \"C\" = 'GRID'" in upgrade
    assert "local_order_id IS NULL" in upgrade
    assert "action_id IS NULL" in upgrade
    assert "role IS NULL" in upgrade

    downgrade = _migration_sql("downgrade")
    assert "Cannot downgrade while Spider source-market receipts exist" in downgrade
    assert "ALTER COLUMN local_order_id SET NOT NULL" in downgrade
    assert "ALTER COLUMN action_id SET NOT NULL" in downgrade
    assert "ALTER COLUMN role SET NOT NULL" in downgrade
    assert "DROP COLUMN original_build_checkpoint_sha256" in downgrade
    assert "DROP COLUMN original_build_checkpoint" in downgrade
