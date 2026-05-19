"""Structural tests for Migration 7 (evolution_epochs + gene_records).

These tests validate the ORM definitions and the GeneRole enum without
requiring a live PostgreSQL instance. End-to-end migration round-trip
testing lives in Task 0.7 (``tests/test_migrations.py``).
"""
from __future__ import annotations

from sqlalchemy import Numeric

from src.core.models import GeneRole
from src.core.orm_models import EvolutionEpoch, GeneRecord


# ---------------------------------------------------------------------------
# EvolutionEpoch
# ---------------------------------------------------------------------------

def test_evolution_epoch_has_all_eval_columns() -> None:
    """Critical fix I4: the four eval_* columns must exist so that
    ``best_score`` carries its evaluation context."""
    cols = EvolutionEpoch.__table__.columns
    for name in ("eval_pair", "eval_start_date", "eval_end_date", "eval_timeframe"):
        assert name in cols, f"EvolutionEpoch missing required column: {name}"
        assert cols[name].nullable is False, (
            f"EvolutionEpoch.{name} must be NOT NULL — best_score is "
            f"meaningless without full evaluation context"
        )


def test_evolution_epoch_best_score_is_numeric_not_float() -> None:
    """``best_score`` must be Numeric (Decimal). float is forbidden for
    monetary / score values per project rules."""
    col = EvolutionEpoch.__table__.columns["best_score"]
    assert isinstance(col.type, Numeric), (
        f"EvolutionEpoch.best_score must be Numeric, got {type(col.type).__name__}"
    )
    # Numeric(18, 8) per spec.
    assert col.type.precision == 18
    assert col.type.scale == 8


def test_evolution_epoch_check_constraint_exists() -> None:
    """``chk_epoch_status`` must restrict status to the canonical 3 values."""
    constraints = {c.name for c in EvolutionEpoch.__table__.constraints}
    assert "chk_epoch_status" in constraints


# ---------------------------------------------------------------------------
# GeneRecord
# ---------------------------------------------------------------------------

def test_gene_record_has_role_column() -> None:
    cols = GeneRecord.__table__.columns
    assert "role" in cols
    assert cols["role"].nullable is False


def test_gene_record_score_total_is_numeric() -> None:
    col = GeneRecord.__table__.columns["score_total"]
    assert isinstance(col.type, Numeric)
    assert col.type.precision == 18
    assert col.type.scale == 8


def test_gene_record_max_drawdown_is_numeric() -> None:
    col = GeneRecord.__table__.columns["max_drawdown"]
    assert isinstance(col.type, Numeric)
    assert col.type.precision == 10
    assert col.type.scale == 8


def test_gene_record_epoch_id_fk_targets_evolution_epochs() -> None:
    """The FK on ``epoch_id`` must point to ``evolution_epochs.id``."""
    col = GeneRecord.__table__.columns["epoch_id"]
    fks = list(col.foreign_keys)
    assert len(fks) == 1, f"epoch_id should have exactly one FK, got {len(fks)}"
    target = fks[0].target_fullname
    assert target == "evolution_epochs.id", (
        f"epoch_id FK target must be evolution_epochs.id, got {target}"
    )


def test_gene_record_check_constraint_exists() -> None:
    constraints = {c.name for c in GeneRecord.__table__.constraints}
    assert "chk_gene_role" in constraints


# ---------------------------------------------------------------------------
# GeneRole enum
# ---------------------------------------------------------------------------

def test_gene_role_enum_values_are_lowercase() -> None:
    """Values must be lower-case to match the DB CHECK constraint
    ``chk_gene_role`` on ``gene_records.role``."""
    assert GeneRole.CHALLENGER.value == "challenger"
    assert GeneRole.CHAMPION.value == "champion"
    assert GeneRole.RETIRED.value == "retired"

    # Sanity: exactly three roles, no extras.
    all_values = {member.value for member in GeneRole}
    assert all_values == {"challenger", "champion", "retired"}
