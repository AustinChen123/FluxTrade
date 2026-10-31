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
NEWEST_REVISION = "6d8a1e2f4c90"


def test_latest_revision_is_the_only_head():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)

    assert script.get_heads() == [NEWEST_REVISION]
    newest = script.get_revision(NEWEST_REVISION)
    assert newest is not None and newest.down_revision == "4c2f8a91d5e7"


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
    heads = script.get_heads()
    assert len(heads) == 1
    reachable_revisions = {revision.revision for revision in script.iterate_revisions(heads[0], "base")}
    assert REVISION in reachable_revisions
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "c3f8d1a904b7"
    callback_revision = script.get_revision("f47c2a91d683")
    assert callback_revision is not None and callback_revision.down_revision == REVISION

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
