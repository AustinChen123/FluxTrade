"""Add append-only profile invalidation events (DML guard, not DDL protection).

Revision ID: 7d3a9c02e5f8
Revises: 6c2f8a91d4e7
"""
from alembic import op

revision = "7d3a9c02e5f8"
down_revision = "6c2f8a91d4e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE market_data_invalidation (
            event_id VARCHAR(128) COLLATE "C" NOT NULL PRIMARY KEY,
            snapshot_id VARCHAR(64) NOT NULL REFERENCES volume_profile_snapshot(id) ON DELETE RESTRICT,
            reason_code VARCHAR(64) NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            replacement_snapshot_id VARCHAR(64) REFERENCES volume_profile_snapshot(id) ON DELETE RESTRICT,
            source VARCHAR(64) NOT NULL,
            CONSTRAINT ck_mdi_event CHECK (event_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mdi_snapshot CHECK (snapshot_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mdi_reason CHECK (reason_code COLLATE "C" ~ '^[A-Za-z0-9_-]{1,64}$'),
            CONSTRAINT ck_mdi_source CHECK (source COLLATE "C" ~ '^[A-Za-z0-9_-]{1,64}$'),
            CONSTRAINT ck_mdi_recorded CHECK (
                recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00'
                AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'
                AND recorded_at = date_trunc('milliseconds', recorded_at)
            ),
            CONSTRAINT ck_mdi_replacement CHECK (
                replacement_snapshot_id IS NULL OR
                (replacement_snapshot_id COLLATE "C" ~ '^[0-9a-f]{64}$' AND replacement_snapshot_id <> snapshot_id)
            )
        )
    """)
    op.execute("CREATE INDEX ix_mdi_snapshot_recorded_event ON market_data_invalidation (snapshot_id, recorded_at, event_id)")
    op.execute("CREATE INDEX ix_mdi_replacement ON market_data_invalidation (replacement_snapshot_id)")
    op.execute("""
        CREATE FUNCTION reject_market_data_invalidation_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'market data invalidation is append-only' USING ERRCODE = '55000';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER market_data_invalidation_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON market_data_invalidation
        FOR EACH STATEMENT EXECUTE FUNCTION reject_market_data_invalidation_mutation()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER market_data_invalidation_append_only ON market_data_invalidation")
    op.execute("DROP TABLE market_data_invalidation")
    op.execute("DROP FUNCTION reject_market_data_invalidation_mutation()")
