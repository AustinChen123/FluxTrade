"""Offline bootstrap migration/ORM parity; no database access."""

import importlib.util
import io
from typing import cast

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint, Table, UniqueConstraint
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects.postgresql import dialect

from src.core.market_data.profiles.orm import BootstrapSeed
from test_migration_19_structure import ROOT

REVISION = "b73e9a21c604"


def sql(direction):
    spec = importlib.util.spec_from_file_location(
        "bootstrap_migration",
        ROOT / "alembic/versions/b73e9a21c604_add_market_data_bootstrap_seed.py",
    )
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


def test_head_parity_and_guard():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["c84f1a92d607"]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "2f6c8a1e9b04"
    up = sql("upgrade")
    assert (
        up.count("CREATE TABLE")
        == up.count("CREATE FUNCTION")
        == up.count("CREATE TRIGGER")
        == 1
    )
    table = cast(Table, BootstrapSeed.__table__)
    ddl = " ".join(str(CreateTable(table).compile(dialect=dialect())).split())
    for column in table.columns:
        assert not column.nullable
        sql_type = str(column.type.compile(dialect=dialect())).replace(
            "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"
        )
        assert f"{column.name} {sql_type} NOT NULL" in up
    for constraint in table.constraints:
        if isinstance(constraint, CheckConstraint):
            historical = str(constraint.sqltext).replace(
                "^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$", "^[A-Za-z0-9_-]{1,128}$"
            )
            assert f"CONSTRAINT {constraint.name} CHECK ({historical})" in up
        if isinstance(constraint, UniqueConstraint):
            assert (
                f"CONSTRAINT {constraint.name} UNIQUE ({', '.join(c.name for c in constraint.columns)})"
                in up
            )
    assert 'seed_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY' in up
    assert (
        "REFERENCES product(id)" in up
        and "FOREIGN KEY(product_id) REFERENCES product (id)" in ddl
    )
    assert "snapshot" not in up and "CREATE INDEX" not in up
    assert "BEFORE UPDATE OR DELETE OR TRUNCATE" in up and "ERRCODE = '55000'" in up
    down = sql("downgrade")
    assert (
        down.index("DROP TRIGGER")
        < down.index("DROP TABLE")
        < down.index("DROP FUNCTION")
    )
