from pathlib import Path

from src.core.orm_models import Order


MIGRATION = (
    Path(__file__).parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "c6d4e2f8a1b0_add_rithmic_order_account_identity.py"
)


def test_order_has_rithmic_account_identity_columns() -> None:
    assert {"account_profile", "account_id"} <= set(Order.__table__.columns.keys())
    assert "chk_order_account_identity_complete" in {
        constraint.name for constraint in Order.__table__.constraints
    }


def test_account_identity_migration_is_latest_and_reversible() -> None:
    source = MIGRATION.read_text()

    assert 'revision: str = "c6d4e2f8a1b0"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "4f7a2c9d1e6b"' in source
    assert source.count("op.add_column(") == 2
    assert source.count("op.drop_column(") == 2
    assert '"chk_order_account_identity_complete"' in source
