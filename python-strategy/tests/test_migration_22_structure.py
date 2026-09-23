"""Offline activation request DDL/ORM parity; lifecycle execution is a PG gate."""

import importlib.util
import io
from typing import cast

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint, DefaultClause, Table
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.schema import CreateIndex

from src.core.market_data.profiles.orm import ProfileActivationRequest
from test_migration_19_structure import ROOT


def sql(direction):
    spec = importlib.util.spec_from_file_location(
        "activation_migration",
        ROOT / "alembic/versions/d95a2b73e608_add_profile_activation_request.py",
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


def test_single_head_exact_schema_and_orm_parity():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["d95a2b73e608"]
    revision = script.get_revision("d95a2b73e608")
    assert revision is not None and revision.down_revision == "c84f1a92d607"
    up = sql("upgrade")
    table = cast(Table, ProfileActivationRequest.__table__)
    assert set(table.columns.keys()) == set(
        "request_id environment execution_scope_id strategy_id expected_state_version canonical_payload payload_digest contract_version requested_at status terminal_at terminal_reason".split()
    )
    for column in table.columns:
        kind = str(column.type.compile(dialect=dialect())).replace(
            "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"
        )
        assert f"{column.name} {kind}" + ("" if column.nullable else " NOT NULL") in up
        assert column.nullable == (column.name in ("terminal_at", "terminal_reason"))
    for constraint in table.constraints:
        if isinstance(constraint, CheckConstraint):
            assert f"CONSTRAINT {constraint.name} CHECK ({constraint.sqltext})" in up
    assert len([c for c in table.constraints if isinstance(c, CheckConstraint)]) == 13
    assert list(table.primary_key.columns.keys()) == ["request_id"]
    assert 'request_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY' in up
    assert {fk.target_fullname for fk in table.foreign_keys} == {"strategy.id"}
    assert up.count("REFERENCES strategy(id)") == 1 and "ON DELETE" not in up
    default = table.c.requested_at.server_default
    assert isinstance(default, DefaultClause)
    assert str(default.arg) == "date_trunc('milliseconds', clock_timestamp())"
    assert "DEFAULT date_trunc('milliseconds', clock_timestamp())" in up
    assert len(table.indexes) == 1
    assert str(CreateIndex(next(iter(table.indexes))).compile(dialect=dialect())) in up
    assert up.count("CREATE TABLE") == up.count("CREATE FUNCTION") == 1
    assert up.count("CREATE TRIGGER") == 2


def test_one_way_guard_and_downgrade_order():
    up = sql("upgrade")
    assert (
        "OLD.status <> 'PENDING' OR NEW.status NOT IN ('CONSUMED', 'CANCELLED', 'STALE')"
        in up
    )
    immutable = "request_id environment execution_scope_id strategy_id expected_state_version canonical_payload payload_digest contract_version requested_at".split()
    for prefix in ("NEW", "OLD"):
        assert "ROW(" + ", ".join(f"{prefix}.{c}" for c in immutable) + ")" in up
    assert "IS DISTINCT FROM" in up and up.count("ERRCODE = '55000'") == 2
    assert "BEFORE UPDATE ON strategy_profile_activation_request FOR EACH ROW" in up
    assert (
        "BEFORE DELETE OR TRUNCATE ON strategy_profile_activation_request FOR EACH STATEMENT"
        in up
    )
    assert sql("downgrade") == (
        "DROP TRIGGER spar_delete ON strategy_profile_activation_request; "
        "DROP TRIGGER spar_update ON strategy_profile_activation_request; "
        "DROP TABLE strategy_profile_activation_request; "
        "DROP FUNCTION guard_profile_activation_request();"
    )
