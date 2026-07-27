from pathlib import Path

from src.core.orm_models import MarketDataApplication


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "9b7e2c4d6f10_add_market_data_application.py"
)


def test_market_data_application_migration_is_latest_and_reversible() -> None:
    source = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "9b7e2c4d6f10"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "f1a8c3d6e9b2"' in source
    assert 'op.create_table(\n        "market_data_application"' in source
    assert 'op.drop_table("market_data_application")' in source


def test_market_data_application_identity_and_payload_are_durable() -> None:
    source = MIGRATION.read_text(encoding="utf-8")

    for column in (
        "environment",
        "product_id",
        "timeframe",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "applied_at",
    ):
        assert f'"{column}"' in source
    assert '"pk_market_data_application"' in source
    assert 'sa.Column("timeframe", sa.String(), nullable=False)' in source
    assert 'sa.String(length=32)' not in source
    assert MarketDataApplication.__table__.c.timeframe.type.length is None
