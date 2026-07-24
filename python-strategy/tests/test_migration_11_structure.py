from pathlib import Path

from sqlalchemy import Numeric

from src.core.orm_models import GeneRecord


MIGRATION = (
    Path(__file__).parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "7a3c9e1b5d2f_add_gene_checkpoint_identity.py"
)


def test_gene_record_exposes_checkpoint_identity() -> None:
    assert {"generation_index", "candidate_id"} <= set(
        GeneRecord.__table__.columns.keys()
    )
    assert "uq_gene_epoch_generation_candidate" in {
        constraint.name for constraint in GeneRecord.__table__.constraints
    }
    drawdown_type = GeneRecord.__table__.columns["max_drawdown"].type
    assert isinstance(drawdown_type, Numeric)
    assert (drawdown_type.precision, drawdown_type.scale) == (18, 8)


def test_checkpoint_identity_migration_is_latest_and_reversible() -> None:
    source = MIGRATION.read_text()

    assert 'revision: str = "7a3c9e1b5d2f"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "c6d4e2f8a1b0"' in source
    assert source.count("op.add_column(") == 2
    assert source.count("op.drop_column(") == 2
    assert source.count('op.alter_column(') == 4
    assert '"uq_gene_epoch_generation_candidate"' in source
