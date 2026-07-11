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
_WRAPPER_KEY = "__migration_4f7a2c9d1e6b_ops_event"


def upgrade() -> None:
    op.execute(
        "ALTER TABLE system_events "
        "DROP CONSTRAINT IF EXISTS chk_system_events_type"
    )
    op.execute(
        "UPDATE system_events "
        "SET event_type = 'ops', "
        f"payload = payload -> '{_WRAPPER_KEY}' -> 'payload' "
        "WHERE event_type = 'system_error' "
        "AND jsonb_typeof(payload) = 'object' "
        f"AND payload ? '{_WRAPPER_KEY}' "
        f"AND (payload -> '{_WRAPPER_KEY}') ? 'payload' "
        f"AND payload -> '{_WRAPPER_KEY}' ->> 'migration' = '{revision}' "
        f"AND payload -> '{_WRAPPER_KEY}' ->> 'event_type' = 'ops'"
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
        "UPDATE system_events "
        "SET event_type = 'system_error', "
        "payload = jsonb_build_object("
        f"'{_WRAPPER_KEY}', jsonb_build_object("
        f"'migration', '{revision}', "
        "'event_type', 'ops', "
        "'payload', payload)) "
        "WHERE event_type = 'ops'"
    )
    op.execute(
        "ALTER TABLE system_events "
        f"ADD CONSTRAINT chk_system_events_type CHECK ({_WITHOUT_OPS})"
    )
