"""Structural tests for Migration 9 ops event type support."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import CheckConstraint

from src.core.audit_service import SYSTEM_EVENT_TYPES
from src.core.orm_models import SystemEvent

MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "4f7a2c9d1e6b_add_ops_system_event_type.py"
)


def test_system_event_types_include_ops() -> None:
    assert "ops" in SYSTEM_EVENT_TYPES


def test_system_event_check_constraint_allows_ops() -> None:
    constraints = [
        c
        for c in SystemEvent.__table__.constraints
        if isinstance(c, CheckConstraint)
        and c.name == "chk_system_events_type"
    ]
    assert len(constraints) == 1
    assert "ops" in str(constraints[0].sqltext)


def test_downgrade_transforms_ops_rows_before_restoring_constraint() -> None:
    source = MIGRATION_PATH.read_text(encoding="utf-8")
    downgrade = source.split("def downgrade() -> None:", 1)[1]

    assert "UPDATE system_events" in downgrade
    assert "jsonb_build_object" in downgrade
    assert downgrade.index("UPDATE system_events") < downgrade.index(
        "ADD CONSTRAINT chk_system_events_type"
    )


def test_upgrade_restores_wrapped_ops_rows_before_constraint() -> None:
    source = MIGRATION_PATH.read_text(encoding="utf-8")
    upgrade = source.split("def upgrade() -> None:", 1)[1].split(
        "def downgrade() -> None:", 1
    )[0]

    assert "payload ->" in upgrade
    assert upgrade.index("UPDATE system_events") < upgrade.index(
        "ADD CONSTRAINT chk_system_events_type"
    )
