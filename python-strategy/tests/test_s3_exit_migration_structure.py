"""Offline DDL contract checks for the S3 prefix A migration."""

import importlib.util
import io
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory


ROOT = Path(__file__).resolve().parents[2] / "database"
REVISION = "71d3a8c4e205"


def _sql(direction: str) -> str:
    path = ROOT / "alembic/versions" / f"{REVISION}_add_s3_exit_persistence.py"
    spec = importlib.util.spec_from_file_location("s3_exit_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = io.StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        getattr(module, direction)()
    return " ".join(output.getvalue().split())


def test_s3_revision_follows_existing_only_head():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [REVISION]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "6d8a1e2f4c90"


def test_s3_ddl_has_exact_receipt_fk_attempt_fields_and_active_index():
    upgrade = _sql("upgrade")
    assert "CREATE TABLE rithmic_exit_attempt" in upgrade
    assert "attempt_id UUID NOT NULL" in upgrade
    for field in (
        "sender_process_instance_id VARCHAR(128)", "dispatch_epoch BIGINT",
        "quiescence_evidence JSONB", "parent_attempt_id UUID",
        "correlation JSONB DEFAULT", "last_observation JSONB DEFAULT",
    ):
        assert field in upgrade
    assert "CREATE UNIQUE INDEX uq_rea_active_target ON rithmic_exit_attempt (account_id, account_profile, product_id) " in upgrade
    assert "WHERE state NOT IN ('COMPLETED','VERIFIED_NOOP','SUPERSEDED_RESIDUAL')" in upgrade
    assert "CREATE UNIQUE INDEX uq_meh_signal" not in upgrade  # represented as a table constraint
    assert "CONSTRAINT uq_meh_signal UNIQUE (environment, product_id, timeframe, timestamp, signal_key)" in upgrade
    assert "CONSTRAINT fk_meh_receipt FOREIGN KEY(environment, product_id, timeframe, timestamp) REFERENCES market_data_application (environment, product_id, timeframe, timestamp)" in upgrade
    assert "CREATE TRIGGER trg_meh_identity_immutable" in upgrade
    assert "execution_scope_id" not in upgrade.split("CONSTRAINT fk_meh_receipt", 1)[1].split(")", 1)[0]
    assert "system_events" not in upgrade


def test_s3_downgrade_refuses_to_delete_live_persistence():
    assert "Cannot downgrade while S3 exit persistence rows exist" in _sql("downgrade")
