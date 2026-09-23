"""Align persisted strategy versions with the existing decision-key domain."""

from alembic import op

revision = "c84f1a92d607"
down_revision = "b73e9a21c604"
branch_labels = None
depends_on = None


def _replace(pattern: str) -> None:
    for table, constraint in (
        ("market_data_decision_input", "ck_mdd_version"),
        ("market_data_decision_outcome", "ck_mdo_version"),
        ("market_data_bootstrap_seed", "ck_mbs_strategy_version"),
    ):
        op.drop_constraint(constraint, table, type_="check")
        op.create_check_constraint(
            constraint, table, f"strategy_version COLLATE \"C\" ~ '{pattern}'"
        )


def upgrade() -> None:
    _replace("^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")


def downgrade() -> None:
    _replace("^[A-Za-z0-9_-]{1,128}$")
