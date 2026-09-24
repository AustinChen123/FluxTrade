"""Isolated PostgreSQL version-domain acceptance; no guard disabling."""

import hashlib

import pytest
from sqlalchemy import insert, select, text, inspect
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles.orm import Base
from test_profile_bootstrap_pg_fixture import build, Consumer
from test_migrations import _upgrade, _downgrade
from test_migration_21_structure import TARGETS, NEW, OLD

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]


def copied_row(engine, table_name, version):
    table = Base.metadata.tables[table_name]
    with engine.connect() as conn:
        values = dict(conn.execute(select(table).limit(1)).mappings().one())
    token = hashlib.sha256((table_name + version).encode()).hexdigest()
    values.update(strategy_id="variant_" + token[:16], strategy_version=version)
    if table_name == "market_data_decision_input":
        values["input_id"] = token
    elif table_name == "market_data_bootstrap_seed":
        values["seed_id"] = token
    return table, values


def fingerprint(engine):
    with engine.connect() as conn:
        revision = conn.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        rows = tuple(
            tuple(
                conn.execute(
                    select(Base.metadata.tables[name]).order_by(
                        *Base.metadata.tables[name].primary_key.columns
                    )
                ).all()
            )
            for name, _ in TARGETS
        )
    checks = tuple(
        next(
            c["sqltext"]
            for c in inspect(engine).get_check_constraints(name)
            if c["name"] == constraint
        )
        for name, constraint in TARGETS
    )
    return revision, checks, rows


@pytest.mark.parametrize("table_name,constraint", TARGETS)
def test_each_table_version_boundaries_independently(
    profile_repository_pg, table_name, constraint
):
    build(profile_repository_pg, True)
    for version in ("v" * 128, "bad:version", "版本", ".v", "_v", "-v", "", "v" * 129):
        table, values = copied_row(profile_repository_pg, table_name, version)
        with profile_repository_pg.begin() as conn:
            if len(version) == 128:
                conn.execute(insert(table).values(**values))
            else:
                with pytest.raises(DBAPIError) as caught:
                    with conn.begin_nested():
                        conn.execute(insert(table).values(**values))
                if len(version) == 129:
                    assert getattr(caught.value.orig, "pgcode", None) == "22001"
                else:
                    assert getattr(caught.value.orig, "pgcode", None) == "23514"
                    assert (
                        getattr(
                            getattr(caught.value.orig, "diag", None),
                            "constraint_name",
                            None,
                        )
                        == constraint
                    )


def test_clean_upgrade_downgrade_upgrade(profile_repository_pg, fresh_pg_db):
    before = fingerprint(profile_repository_pg)
    assert all(NEW in c for c in before[1])
    _downgrade(fresh_pg_db, "b73e9a21c604")
    middle = fingerprint(profile_repository_pg)
    assert middle[0] == "b73e9a21c604" and all(OLD in c for c in middle[1])
    _upgrade(fresh_pg_db)
    assert fingerprint(profile_repository_pg) == before


@pytest.mark.parametrize("table_name,constraint", TARGETS)
@pytest.mark.parametrize(
    "direction,version",
    [("upgrade", "_legacy"), ("upgrade", "-legacy"), ("downgrade", "1.2.0")],
)
def test_incompatible_existing_row_rolls_back_entire_migration(
    profile_repository_pg,
    fresh_pg_db,
    monkeypatch,
    table_name,
    constraint,
    direction,
    version,
):
    # Baseline is valid under both domains; only the selected table is damaged.
    monkeypatch.setattr(Consumer, "__fluxtrade_artifact_version__", "v1")
    build(profile_repository_pg, True)
    if direction == "upgrade":
        _downgrade(fresh_pg_db, "b73e9a21c604")
    table, values = copied_row(
        profile_repository_pg,
        table_name,
        version,
    )
    with profile_repository_pg.begin() as conn:
        conn.execute(insert(table).values(**values))
    before = fingerprint(profile_repository_pg)
    with pytest.raises(DBAPIError) as caught:
        if direction == "upgrade":
            _upgrade(fresh_pg_db)
        else:
            _downgrade(fresh_pg_db, "b73e9a21c604")
    assert getattr(caught.value.orig, "pgcode", None) == "23514"
    assert (
        getattr(getattr(caught.value.orig, "diag", None), "constraint_name", None)
        == constraint
    )
    assert fingerprint(profile_repository_pg) == before
