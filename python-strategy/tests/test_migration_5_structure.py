"""Structural tests for Migration 5 ORM additions.

These tests do not require a live PostgreSQL connection — they only
inspect the SQLAlchemy ``Table`` metadata to confirm that the ORM model
mirrors the schema produced by the rev ``7c9e4f2a1b3d`` migration.
End-to-end migration round-trip coverage is owned by Task 0.7.
"""
from __future__ import annotations

from sqlalchemy import CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB

from src.core.orm_models import Order, SignalAudit, SystemEvent


def test_order_has_migration5_columns() -> None:
    cols = Order.__table__.columns
    expected = {
        "client_order_id",
        "intent_payload",
        "submitted_at",
        "acked_at",
        "last_reconciled_at",
    }
    missing = expected - set(cols.keys())
    assert not missing, f"Order missing migration 5 columns: {missing}"

    # All five new columns must be nullable (historical rows have no data).
    for name in expected:
        assert cols[name].nullable, f"Order.{name} should be nullable"

    # intent_payload should map to JSONB.
    assert isinstance(cols["intent_payload"].type, JSONB)


def test_signal_audit_has_migration5_columns_and_jsonb() -> None:
    cols = SignalAudit.__table__.columns
    expected = {
        "client_order_id",
        "intent_payload",
        "outcome_payload",
        "signal_batch_id",
    }
    missing = expected - set(cols.keys())
    assert not missing, f"SignalAudit missing migration 5 columns: {missing}"

    # details_json was upgraded TEXT -> JSONB in this migration.
    assert isinstance(
        cols["details_json"].type, JSONB
    ), "SignalAudit.details_json should be JSONB after migration 5"

    # JSONB payload columns should also be JSONB.
    for name in ("intent_payload", "outcome_payload"):
        assert isinstance(cols[name].type, JSONB), f"{name} should be JSONB"


def test_system_event_class_exists_with_required_columns() -> None:
    cols = SystemEvent.__table__.columns
    expected = {
        "id",
        "event_type",
        "event_subtype",
        "related_strategy_id",
        "related_order_id",
        "related_gene_id",
        "payload",
        "created_at",
    }
    missing = expected - set(cols.keys())
    assert not missing, f"SystemEvent missing columns: {missing}"

    # NOT NULL columns.
    assert not cols["event_type"].nullable
    assert not cols["payload"].nullable
    assert not cols["created_at"].nullable

    # Nullable columns.
    for name in (
        "event_subtype",
        "related_strategy_id",
        "related_order_id",
        "related_gene_id",
    ):
        assert cols[name].nullable, f"SystemEvent.{name} should be nullable"

    # payload must be JSONB.
    assert isinstance(cols["payload"].type, JSONB)


def test_system_event_has_check_constraint_on_event_type() -> None:
    constraints = [
        c
        for c in SystemEvent.__table__.constraints
        if isinstance(c, CheckConstraint)
    ]
    names = {c.name for c in constraints}
    assert "chk_system_events_type" in names, (
        f"Expected chk_system_events_type CHECK constraint, got {names}"
    )


def test_system_event_related_order_id_is_string_fk() -> None:
    """``order.id`` is a string PK in this codebase, so ``related_order_id``
    must also be a string. A BIGINT FK (as a strict reading of the plan
    suggested) would fail to be created at migration time.
    """
    col = SystemEvent.__table__.columns["related_order_id"]
    fks = list(col.foreign_keys)
    assert len(fks) == 1
    assert fks[0].column.table.name == "order"
    # Python type should be ``str``.
    assert col.type.python_type is str
