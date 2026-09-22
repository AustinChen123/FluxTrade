"""Offline terminal schema parity, not atomic application evidence."""

import importlib.util
import io
from pathlib import Path
from typing import cast

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateTable, AddConstraint

from src.core.market_data.profiles.orm import (
    MarketDataDecisionBatch,
    MarketDataDecisionOutcome,
)
from src.core.orm_models import MarketDataApplication

ROOT = Path(__file__).resolve().parents[2] / "database"
REVISION = "2f6c8a1e9b04"


def sql(direction):
    spec = importlib.util.spec_from_file_location(
        "terminal_migration",
        ROOT / "alembic/versions/2f6c8a1e9b04_add_market_data_terminal.py",
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


def test_head_marker_guards_and_downgrade():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [REVISION]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "8e4b2c91a6d0"
    up = sql("upgrade")
    assert up.count("CREATE TABLE") == 2 and up.count("CREATE FUNCTION") == 1
    assert up.count("CREATE TRIGGER") == 2 and "CREATE INDEX" not in up
    assert "ADD COLUMN decision_contract_version INTEGER;" in up
    assert "UPDATE market_data_application" not in up
    for table in ("market_data_decision_batch", "market_data_decision_outcome"):
        assert (
            f"BEFORE UPDATE OR DELETE OR TRUNCATE ON {table} FOR EACH STATEMENT" in up
        )
    assert "ERRCODE = '55000'" in up
    down = sql("downgrade")
    targets = [
        "DROP TRIGGER market_data_decision_outcome",
        "DROP TABLE market_data_decision_outcome",
        "DROP TRIGGER market_data_decision_batch",
        "DROP TABLE market_data_decision_batch",
        "DROP FUNCTION",
        "DROP CONSTRAINT uq_mda_decision_contract",
        "DROP CONSTRAINT ck_mda_decision_contract",
        "DROP COLUMN decision_contract_version",
    ]
    assert [down.index(target) for target in targets] == sorted(
        down.index(target) for target in targets
    )


def test_exact_columns_constraints_and_fk_parity():
    up = sql("upgrade")
    for model in (MarketDataDecisionBatch, MarketDataDecisionOutcome):
        table = cast(Table, model.__table__)
        ddl = " ".join(str(CreateTable(table).compile(dialect=dialect())).split())
        body = up.split(f"CREATE TABLE {table.name} (", 1)[1].split(";", 1)[0]
        assert not table.indexes
        for column in table.columns:
            kind = str(column.type.compile(dialect=dialect())).replace(
                "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"
            )
            assert f"{column.name} {kind}" in body
            if not column.nullable:
                assert f"{column.name} {kind} NOT NULL" in body
        for constraint in table.constraints:
            compiled = " ".join(
                str(AddConstraint(constraint).compile(dialect=dialect())).split()
            ).split(" ADD ", 1)[1]
            assert compiled.replace(" (", "(") in body.replace(" (", "(")
        assert ddl.count("FOREIGN KEY") == body.count("FOREIGN KEY")
    receipt = cast(Table, MarketDataApplication.__table__)
    assert receipt.c.decision_contract_version.nullable
    assert receipt.c.decision_contract_version.server_default is None
    for constraint in receipt.constraints:
        if constraint.name in ("ck_mda_decision_contract", "uq_mda_decision_contract"):
            assert (
                " ".join(
                    str(AddConstraint(constraint).compile(dialect=dialect())).split()
                )
                in up
            )
