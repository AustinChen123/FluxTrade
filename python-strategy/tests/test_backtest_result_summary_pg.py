"""Isolated PostgreSQL coverage for formal backtest summary columns."""

from __future__ import annotations

from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from test_migrations import (
    _insert_importing_research_dataset,
    _insert_research_prerequisites,
    _target_url,
    _upgrade,
    fresh_pg_db as _fresh_pg_db,
)
from test_backtest_result_summary_migration_pg import (
    _FORMAL_COLUMNS as _FORMAL_FIELDS,
    _NUMERIC_COLUMNS as _METRICS,
)

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_PRECISE = Decimal("12345678901234567890.1234567890123456789012345678")


def _seed_foreign_keys(engine: sa.Engine) -> None:
    _insert_research_prerequisites(engine)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO strategy (id, name) VALUES ('summary-strategy', 'Fixture')"
            )
        )
        _insert_importing_research_dataset(conn, "summary-dataset")


def _formal_row() -> dict[str, object]:
    return {
        "strategy_id": "summary-strategy",
        "start_time": 1_700_000_000_000,
        "end_time": 1_700_000_060_000,
        "total_pnl": _PRECISE,
        "job_id": "summary-job",
        "dataset_id": "summary-dataset",
        "subject_kind": "STRATEGY_ARTIFACT",
        "subject_id": "summary-strategy:v1",
        "input_digest": "a" * 64,
        "result_digest": "b" * 64,
        "product_id": "RITHMIC:MNQ-CONTINUOUS",
        "timeframe": "1m",
        "currency": "USDT",
        "completed_at": 1_700_000_060_000,
        "initial_balance": Decimal("10000.0000000000000000000000001"),
        "net_pnl": Decimal("-123.0000000000000000000000001"),
        "return_pct": Decimal("-0.01230000000000000000000001"),
        "max_drawdown": Decimal("0.05000000000000000000000001"),
        "sharpe": Decimal("1.1234567890123456789012345678"),
        "sortino": Decimal("2.1234567890123456789012345678"),
        "calmar": Decimal("3.1234567890123456789012345678"),
    }


def test_sqlite_metadata_rejects_formal_summary_without_subject_kind() -> None:
    from src.core.orm_models import BacktestResultSummary

    engine = sa.create_engine("sqlite://")
    table = BacktestResultSummary.__table__
    table.create(engine)
    invalid = _formal_row()
    invalid["subject_kind"] = None
    try:
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(sa.insert(table).values(**invalid))
    finally:
        engine.dispose()


def test_formal_summary_constraints_reject_invalid_rows(fresh_pg_db: str) -> None:
    from src.core.orm_models import BacktestResultSummary

    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _seed_foreign_keys(engine)
    table = BacktestResultSummary.__table__
    inspector = sa.inspect(engine)
    columns = {
        column["name"]: column
        for column in inspector.get_columns("backtest_result_summary")
    }
    assert all(columns[name]["nullable"] for name in _FORMAL_FIELDS)
    assert all(isinstance(columns[name]["type"], sa.Numeric) for name in _METRICS)
    assert "uq_backtest_summary_job_id" in {
        item["name"]
        for item in inspector.get_unique_constraints("backtest_result_summary")
    }
    foreign_keys = {
        item["name"]: item["referred_table"]
        for item in inspector.get_foreign_keys("backtest_result_summary")
    }
    assert (
        foreign_keys["fk_backtest_summary_dataset_id_research_dataset"]
        == "research_dataset"
    )
    assert foreign_keys["fk_backtest_summary_product_id_product"] == "product"

    formal = _formal_row()
    with engine.begin() as conn:
        formal_id = conn.execute(
            sa.insert(table).values(**formal).returning(table.c.id)
        ).scalar_one()

    with engine.connect() as conn:
        saved = (
            conn.execute(sa.select(table).where(table.c.id == formal_id))
            .mappings()
            .one()
        )
    for field in ("total_pnl", *_METRICS):
        assert saved[field] == formal[field]
        assert isinstance(saved[field], Decimal)

    for index, field in enumerate(_FORMAL_FIELDS):
        invalid = dict(formal)
        if field != "job_id":
            invalid["job_id"] = f"missing-field-{index}"
        invalid[field] = None
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(sa.insert(table).values(**invalid))

    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(sa.insert(table).values(**formal))

    for assignment in (
        {"dataset_id": "missing-dataset"},
        {"product_id": "missing-product"},
        {"subject_kind": "OTHER"},
        {"initial_balance": Decimal("0")},
        {"max_drawdown": Decimal("-0.01")},
    ):
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    sa.update(table).where(table.c.id == formal_id).values(**assignment)
                )

    for field in ("total_pnl", *_METRICS):
        for special in ("NaN", "Infinity", "-Infinity"):
            with pytest.raises(IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        sa.text(
                            f"UPDATE backtest_result_summary SET {field} = "
                            "CAST(:value AS NUMERIC) WHERE id = :id"
                        ),
                        {"value": special, "id": formal_id},
                    )

    engine.dispose()
