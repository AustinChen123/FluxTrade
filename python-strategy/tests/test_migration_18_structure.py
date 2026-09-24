"""Offline decision-input migration and exact ORM contracts."""

import importlib.util
import io
import re
from typing import cast

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import dialect

from src.core.market_data.profiles.orm import MarketDataDecisionInput
from src.core.orm_models import Base
from test_migration_16_structure import ROOT

REVISION = "8e4b2c91a6d0"


def migration_sql(direction):
    spec = importlib.util.spec_from_file_location(
        "decision_input_migration",
        ROOT / "versions" / f"{REVISION}_add_market_data_decision_input.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
    )
    with Operations.context(context):
        getattr(module, direction)()
    return " ".join(output.getvalue().split())


def test_head_and_guard_exact_scope():
    config = Config()
    config.set_main_option("script_location", str(ROOT))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["d95a2b73e608"]
    revision = script.get_revision(REVISION)
    assert revision and revision.down_revision == "7d3a9c02e5f8"
    assert re.fullmatch("[0-9a-f]{12}", REVISION)
    sql = migration_sql("upgrade")
    assert (
        sql.count("CREATE TABLE")
        == sql.count("CREATE FUNCTION")
        == sql.count("CREATE TRIGGER")
        == 1
    )
    assert "CREATE INDEX" not in sql and "ALTER TABLE" not in sql
    assert (
        "BEFORE UPDATE OR DELETE OR TRUNCATE ON market_data_decision_input FOR EACH STATEMENT EXECUTE FUNCTION reject_market_data_decision_input_mutation()"
        in sql
    )
    assert "USING ERRCODE = '55000'" in sql
    assert (
        migration_sql("downgrade")
        == "DROP TRIGGER market_data_decision_input_append_only ON market_data_decision_input; DROP TABLE market_data_decision_input; DROP FUNCTION reject_market_data_decision_input_mutation();"
    )


def test_exact_orm_migration_parity():
    sql = migration_sql("upgrade")
    table = cast(Table, MarketDataDecisionInput.__table__)
    assert Base.metadata.tables[table.name] is table
    body = sql.split("CREATE TABLE market_data_decision_input (", 1)[1].split(";", 1)[0]
    assert body.count("PRIMARY KEY") == 1
    assert 'input_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY' in body
    assert body.count("REFERENCES") == 1
    assert "product_id VARCHAR(64) NOT NULL REFERENCES product(id)," in body
    assert "volume_profile_snapshot" not in body and "ON DELETE" not in body
    assert body.count(" UNIQUE (") == 1
    assert (
        "CONSTRAINT uq_mdd_input_key UNIQUE (environment, execution_scope_id, "
        "strategy_id, strategy_version, config_hash, product_id, trigger_kind, trigger_id)"
    ) in body
    columns = re.findall(
        r'(\w+) (VARCHAR\(\d+\)(?: COLLATE "C")?|BIGINT|INTEGER|BYTEA|TIMESTAMPTZ) NOT NULL',
        body,
    )
    assert len(columns) == 16 and set(table.c.keys()) == {name for name, _ in columns}
    for name, kind in columns:
        assert str(table.c[name].type.compile(dialect=dialect())) == kind.replace(
            "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"
        )
        assert not table.c[name].nullable
    assert [c.name for c in table.primary_key] == ["input_id"]
    assert [
        (fk.parent.name, fk.target_fullname, fk.ondelete) for fk in table.foreign_keys
    ] == [("product_id", "product.id", None)]
    assert not table.indexes
    checks = [c for c in table.constraints if isinstance(c, CheckConstraint)]
    assert len(checks) == body.count(" CHECK (") == 16
    for check in checks:
        historical = str(check.sqltext).replace(
            "^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$", "^[A-Za-z0-9_-]{1,128}$"
        )
        assert f"CONSTRAINT {check.name} CHECK ({historical})" in body.replace(
            "( ", "("
        ).replace(" )", ")")
    unique = [c for c in table.constraints if isinstance(c, UniqueConstraint)]
    assert len(unique) == 1
    assert (
        f"CONSTRAINT {unique[0].name} UNIQUE ({', '.join(c.name for c in unique[0])})"
        in body
    )
    assert "DEFAULT date_trunc('milliseconds', clock_timestamp())" in body
    assert table.c.recorded_at.server_default is not None
    assert (
        str(getattr(table.c.recorded_at.server_default, "arg"))
        == "date_trunc('milliseconds', clock_timestamp())"
    )
