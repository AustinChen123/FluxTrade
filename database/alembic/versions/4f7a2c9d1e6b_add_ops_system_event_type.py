"""add_ops_system_event_type

Revision ID: 4f7a2c9d1e6b
Revises: fb8c6e6098e3
Create Date: 2026-07-09 00:00:00.000000

Allow live operations events in the system_events event_type CHECK.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "4f7a2c9d1e6b"
down_revision: Union[str, Sequence[str], None] = "fb8c6e6098e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_WITH_OPS = (
    "event_type IN "
    "('reconcile','gene_promote','gene_retire','ops','system_error')"
)
_WITHOUT_OPS = (
    "event_type IN "
    "('reconcile','gene_promote','gene_retire','system_error')"
)


def upgrade() -> None:
    op.execute(
        "ALTER TABLE system_events "
        "DROP CONSTRAINT IF EXISTS chk_system_events_type"
    )
    op.execute(
        "ALTER TABLE system_events "
        f"ADD CONSTRAINT chk_system_events_type CHECK ({_WITH_OPS})"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE system_events "
        "DROP CONSTRAINT IF EXISTS chk_system_events_type"
    )
    op.execute(
        "ALTER TABLE system_events "
        f"ADD CONSTRAINT chk_system_events_type CHECK ({_WITHOUT_OPS})"
    )
