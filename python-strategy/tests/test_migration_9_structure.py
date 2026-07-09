"""Structural tests for Migration 9 ops event type support."""

from __future__ import annotations

from sqlalchemy import CheckConstraint

from src.core.audit_service import SYSTEM_EVENT_TYPES
from src.core.orm_models import SystemEvent


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
