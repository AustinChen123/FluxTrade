from pathlib import Path

from src.core.orm_models import Order


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "4e8c1a2b7d90_scope_order_identity_uniqueness.py"
)
MIGRATION_TEST_OWNER = Path(__file__).with_name("test_migrations.py")


def test_order_identity_migration_replaces_global_uniqueness_atomically() -> None:
    source = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "4e8c1a2b7d90"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "9b7e2c4d6f10"' in source
    assert "DROP CONSTRAINT IF EXISTS uq_order_exchange_id" in source
    assert 'op.drop_constraint("uq_order_exchange_id"' not in source
    for name in (
        "uq_order_identified_client_order_id",
        "uq_order_legacy_client_order_id",
        "uq_order_identified_exchange_order_id",
        "uq_order_legacy_exchange_order_id",
    ):
        assert name in source
        assert name in {index.name for index in Order.__table__.indexes}
    assert not any(
        constraint.name == "uq_order_exchange_id"
        for constraint in Order.__table__.constraints
    )


def test_order_identity_downgrade_preflights_before_dropping_indexes() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    downgrade = source.split("def downgrade() -> None:", 1)[1]

    first_drop = downgrade.index("op.drop_index")
    assert downgrade.index("order_identity_downgrade_collision") < first_drop
    assert "GROUP BY client_order_id" in downgrade[:first_drop]
    assert "GROUP BY exchange_id, exchange_order_id" in downgrade[:first_drop]


def test_explicit_postgres_migration_lane_cannot_skip_connectivity_failure() -> None:
    source = MIGRATION_TEST_OWNER.read_text(encoding="utf-8")

    assert source.count("pytest.skip(") == 1
    assert (
        "PostgreSQL migration test service unavailable after explicit enablement"
        in source
    )
