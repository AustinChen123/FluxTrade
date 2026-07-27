from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "database"
    / "alembic"
    / "versions"
    / "f1a8c3d6e9b2_add_backtest_fill_sequence.py"
)


def test_backtest_fill_sequence_migration_follows_current_head():
    source = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "f1a8c3d6e9b2"' in source
    assert 'down_revision: Union[str, Sequence[str], None] = "e2f4a6b8c0d1"' in source


def test_backtest_fill_sequence_migration_preserves_legacy_ambiguity():
    source = MIGRATION.read_text(encoding="utf-8")

    assert '"fill_sequence", sa.BigInteger(), nullable=True' in source
    assert '"uq_backtest_trade_log_session_fill_sequence"' in source
    assert '"chk_backtest_trade_log_fill_sequence_nonnegative"' in source
    assert "UPDATE backtest_trade_log" not in source
