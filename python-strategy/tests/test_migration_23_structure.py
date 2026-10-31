"""Shared Spider send/delivery tape DDL and ORM ownership contract."""

import importlib.util
import io
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory

ROOT = Path(__file__).resolve().parents[2] / "database"
REVISION = "c3f8d1a904b7"


def _migration_sql(direction: str) -> str:
    path = ROOT / "alembic/versions" / f"{REVISION}_add_spider_transport_tape.py"
    spec = importlib.util.spec_from_file_location("spider_transport_tape_migration", path)
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


def test_transport_tape_is_one_additive_head_with_deterministic_backfill():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    heads = script.get_heads()
    assert len(heads) == 1
    reachable_revisions = {revision.revision for revision in script.iterate_revisions(heads[0], "base")}
    assert REVISION in reachable_revisions
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "a7d0c5e2f941"

    upgrade = _migration_sql("upgrade")
    assert "ALTER TABLE spider_runtime_scope ADD COLUMN last_tape_sequence BIGINT NOT NULL DEFAULT 0" in upgrade
    assert (
        "ALTER TABLE spider_runtime_scope ADD CONSTRAINT ck_spider_runtime_scope_tape_sequence "
        "CHECK (last_tape_sequence >= 0)"
    ) in upgrade
    assert upgrade.count("CREATE TABLE") == 2
    assert "CREATE TABLE spider_runtime_transmission" in upgrade
    assert "CREATE TABLE spider_runtime_tape_index" in upgrade
    assert (
        "FOREIGN KEY (environment, execution_scope_id, strategy_id, delivery_sequence) "
        "REFERENCES spider_runtime_delivery (environment, execution_scope_id, strategy_id, sequence)"
    ) in upgrade
    assert (
        "SELECT environment, execution_scope_id, strategy_id, sequence, 'DELIVERY', sequence, NULL "
        "FROM spider_runtime_delivery ORDER BY environment, execution_scope_id, strategy_id, sequence"
    ) in upgrade
    assert "UPDATE spider_runtime_scope SET last_tape_sequence = last_received_sequence" in upgrade
    assert "DROP TABLE spider_runtime_delivery" not in upgrade
    assert "DROP TABLE spider_runtime_action" not in upgrade

    downgrade = _migration_sql("downgrade")
    assert downgrade.index("DROP TABLE spider_runtime_tape_index") < downgrade.index(
        "DROP TABLE spider_runtime_transmission"
    ) < downgrade.index("DROP CONSTRAINT ck_spider_runtime_scope_tape_sequence") < downgrade.index(
        "DROP COLUMN last_tape_sequence"
    )
    assert "DROP TABLE spider_runtime_delivery" not in downgrade
    assert "DROP TABLE spider_runtime_action" not in downgrade
