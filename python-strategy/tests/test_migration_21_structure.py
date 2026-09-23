"""Version-domain migration changes only the three existing CHECK constraints."""

import importlib.util
import io

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint

from src.core.market_data.profiles.orm import Base
from test_migration_19_structure import ROOT

REVISION = "c84f1a92d607"
TARGETS = (
    ("market_data_decision_input", "ck_mdd_version"),
    ("market_data_decision_outcome", "ck_mdo_version"),
    ("market_data_bootstrap_seed", "ck_mbs_strategy_version"),
)
NEW = "^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$"
OLD = "^[A-Za-z0-9_-]{1,128}$"


def test_exact_migration_scope_and_orm_parity():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [REVISION]
    revision = script.get_revision(REVISION)
    assert revision is not None and revision.down_revision == "b73e9a21c604"
    spec = importlib.util.spec_from_file_location("version_migration", revision.path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for direction, pattern in (("upgrade", NEW), ("downgrade", OLD)):
        output = io.StringIO()
        context = MigrationContext.configure(
            dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
        )
        with Operations.context(context):
            getattr(module, direction)()
        expected = []
        for table, name in TARGETS:
            expected.extend(
                (
                    f"ALTER TABLE {table} DROP CONSTRAINT {name};",
                    f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK (strategy_version COLLATE \"C\" ~ '{pattern}');",
                )
            )
            checks = [
                c
                for c in Base.metadata.tables[table].constraints
                if isinstance(c, CheckConstraint) and c.name == name
            ]
            assert (
                len(checks) == 1
                and str(checks[0].sqltext)
                == f"strategy_version COLLATE \"C\" ~ '{NEW}'"
            )
        assert " ".join(output.getvalue().split()) == " ".join(expected)
