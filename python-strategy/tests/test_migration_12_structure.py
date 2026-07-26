from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "e2f4a6b8c0d1_add_research_datasets.py"
)


def test_research_dataset_migration_follows_current_head():
    source = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "e2f4a6b8c0d1"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "7a3c9e1b5d2f"' in source


def test_research_dataset_migration_separates_versioned_candles():
    source = MIGRATION.read_text(encoding="utf-8")

    assert '"research_dataset"' in source
    assert '"research_candlestick"' in source
    assert '"checksum_sha256"' in source
    assert '"roll_policy"' in source
    assert '"timestamp_format"' in source
    assert '"source_contract"' in source
    assert '"lifecycle_state"' in source
    assert '"sealed_at"' in source
    assert 'sa.PrimaryKeyConstraint("dataset_id", "timestamp")' in source
    assert "trg_research_dataset_immutable" in source
    assert "trg_research_candlestick_immutable" in source
    assert "trg_research_dataset_no_truncate" in source
    assert "trg_research_candlestick_no_truncate" in source
    assert "reject_sealed_research_dataset_mutation" in source
    assert "reject_sealed_research_candlestick_mutation" in source
    assert "reject_research_table_truncate" in source
    assert "BEFORE INSERT OR UPDATE OR DELETE ON research_dataset" in source
    assert "actual_row_count <> NEW.row_count" in source
    assert "ix_research_candlestick_dataset_timestamp" not in source
