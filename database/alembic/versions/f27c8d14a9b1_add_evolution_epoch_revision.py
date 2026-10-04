"""Add durable revisions to evolution epochs."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "f27c8d14a9b1"
down_revision: Union[str, Sequence[str], None] = "e83d14a79b2c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "evolution_epochs",
        sa.Column(
            "revision",
            sa.BigInteger(),
            server_default=sa.text("1"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        "ck_evolution_epoch_revision_positive",
        "evolution_epochs",
        "revision > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_evolution_epoch_revision_positive",
        "evolution_epochs",
        type_="check",
    )
    op.drop_column("evolution_epochs", "revision")
