"""Immutable bootstrap seed; DML guard is not admin/DDL protection."""

from alembic import op

revision = "b73e9a21c604"
down_revision = "2f6c8a1e9b04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE market_data_bootstrap_seed (
            seed_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_version VARCHAR(128) COLLATE "C" NOT NULL,
            config_hash VARCHAR(64) COLLATE "C" NOT NULL,
            requirements_digest VARCHAR(64) COLLATE "C" NOT NULL,
            policy_digest VARCHAR(64) COLLATE "C" NOT NULL,
            dataset_digest VARCHAR(64) COLLATE "C" NOT NULL,
            seed_digest VARCHAR(64) COLLATE "C" NOT NULL,
            product_id VARCHAR(64) NOT NULL REFERENCES product(id),
            timeframe VARCHAR(32) COLLATE "C" NOT NULL,
            contract_version INTEGER NOT NULL,
            cutover_ms BIGINT NOT NULL,
            lookback BIGINT NOT NULL,
            canonical_payload BYTEA NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT uq_mbs_key UNIQUE (environment, execution_scope_id, strategy_id, strategy_version, config_hash, product_id, timeframe, contract_version),
            CONSTRAINT ck_mbs_seed_id CHECK (seed_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mbs_config_hash CHECK (config_hash COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mbs_requirements_digest CHECK (requirements_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mbs_policy_digest CHECK (policy_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mbs_dataset_digest CHECK (dataset_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mbs_seed_digest CHECK (seed_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mbs_environment CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mbs_execution_scope_id CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mbs_strategy_id CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mbs_strategy_version CHECK (strategy_version COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mbs_product CHECK (product_id COLLATE "C" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'),
            CONSTRAINT ck_mbs_timeframe CHECK (timeframe COLLATE "C" ~ '^[A-Za-z0-9_-]{1,32}$'),
            CONSTRAINT ck_mbs_contract CHECK (contract_version = 1),
            CONSTRAINT ck_mbs_cutover CHECK (cutover_ms >= 0),
            CONSTRAINT ck_mbs_lookback CHECK (lookback >= 0),
            CONSTRAINT ck_mbs_payload CHECK (octet_length(canonical_payload) BETWEEN 1 AND 33554432),
            CONSTRAINT ck_mbs_recorded CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND recorded_at = date_trunc('milliseconds', recorded_at))
        )
    """)
    op.execute("""
        CREATE FUNCTION reject_market_data_bootstrap_seed_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'bootstrap seed is append-only' USING ERRCODE = '55000';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER market_data_bootstrap_seed_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON market_data_bootstrap_seed
        FOR EACH STATEMENT EXECUTE FUNCTION reject_market_data_bootstrap_seed_mutation()
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER market_data_bootstrap_seed_append_only ON market_data_bootstrap_seed"
    )
    op.execute("DROP TABLE market_data_bootstrap_seed")
    op.execute("DROP FUNCTION reject_market_data_bootstrap_seed_mutation()")
