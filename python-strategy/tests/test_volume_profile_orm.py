"""PostgreSQL metadata contracts only: no SQLite, engine, or schema mutation."""
import importlib
import re
from decimal import Decimal
from typing import cast
from sqlalchemy import CheckConstraint, Numeric, Table, UniqueConstraint, inspect
from sqlalchemy.dialects.postgresql import JSONB, dialect
from sqlalchemy.schema import CreateIndex, DefaultClause

from src.core.market_data.profiles.orm import (
    VolumeProfileBin,
    VolumeProfileIngestJob,
    VolumeProfileSnapshot,
)
from src.core.orm_models import Base
from test_migration_16_structure import migration_sql

def normalized(value: str) -> str:
    return re.sub(r"\s+", " ", value).replace("( ", "(").replace(" )", ")").strip()

def test_profile_metadata_matches_committed_postgresql_migration() -> None:
    sql = normalized(migration_sql("upgrade"))
    models = (VolumeProfileSnapshot, VolumeProfileBin, VolumeProfileIngestJob)
    assert [model.__tablename__ for model in models] == [
        "volume_profile_snapshot",
        "volume_profile_bin",
        "volume_profile_ingest_job",
    ]
    for model in models:
        table = cast(Table, model.__table__)
        assert Base.metadata.tables[table.name] is table
        assert not inspect(model).relationships
        body = sql.split(f"CREATE TABLE {table.name} (", 1)[1].split(";", 1)[0]
        columns = {
            name: (kind, tail)
            for name, kind, tail in re.findall(
                r"\b(\w+) (VARCHAR\(\d+\)|BIGINT|NUMERIC|JSONB|TEXT|TIMESTAMP WITH TIME ZONE)([^,]*)",
                body,
            )
        }
        assert set(table.c.keys()) == set(columns)
        assert not {"venue", "market_type", "symbol"} & set(columns)
        for column in table.c:
            kind, tail = columns[column.name]
            assert str(column.type.compile(dialect=dialect())) == kind
            assert column.nullable == (
                "NOT NULL" not in tail and "PRIMARY KEY" not in tail
            )
            assert column.default is None
            default = (
                str(cast(DefaultClause, column.server_default).arg)
                if column.server_default is not None
                else None
            )
            assert default == ("now()" if "DEFAULT now()" in tail else None)
            if kind == "NUMERIC":
                assert isinstance(column.type, Numeric) and column.type.asdecimal
                assert column.type.python_type is Decimal
            if kind == "JSONB":
                assert isinstance(column.type, JSONB)
        composite = re.findall(r"PRIMARY KEY \(([^)]+)\)", body)
        expected_pk = composite[0].split(", ") if composite else [
            name for name, (_, tail) in columns.items() if "PRIMARY KEY" in tail
        ]
        assert list(table.primary_key.columns.keys()) == expected_pk
        expected_fk = {
            (name, *match) for name, (_, tail) in columns.items()
            for match in re.findall(r"REFERENCES (\w+)\((\w+)\)(?: ON DELETE (CASCADE|RESTRICT|NO ACTION|SET NULL|SET DEFAULT))?", tail)
        }
        actual_fk = {(fk.parent.name, *fk.target_fullname.split("."), fk.ondelete or "")
                     for fk in table.foreign_keys}
        assert actual_fk == expected_fk
        for constraint in table.constraints:
            if isinstance(constraint, CheckConstraint):
                assert (
                    f"CONSTRAINT {constraint.name} CHECK ({normalized(str(constraint.sqltext))})"
                    in sql
                )
            elif isinstance(constraint, UniqueConstraint):
                keys = ", ".join(constraint.columns.keys())
                assert f"CONSTRAINT {constraint.name} UNIQUE ({keys})" in body
        expected_names = set(re.findall(r"CONSTRAINT (\w+)", body))
        expected_names.update(
            re.findall(rf"ALTER TABLE {table.name} ADD CONSTRAINT (\w+)", sql)
        )
        assert {
            c.name for c in table.constraints if c.name is not None
        } == expected_names
        expected_indexes = set(re.findall(rf"CREATE INDEX (\w+) ON {table.name} ", sql))
        assert {index.name for index in table.indexes} == expected_indexes
        for index in table.indexes:
            assert normalized(str(CreateIndex(index).compile(dialect=dialect()))) in sql
    assert (
        importlib.import_module("src.core.market_data.profiles.orm").VolumeProfileBin
        is VolumeProfileBin
    )
