"""Immutable decision inputs; statement DML guard, not admin/DDL protection."""

from alembic import op

revision = "8e4b2c91a6d0"
down_revision = "7d3a9c02e5f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE market_data_decision_input (
            input_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_version VARCHAR(128) COLLATE "C" NOT NULL,
            config_hash VARCHAR(64) COLLATE "C" NOT NULL,
            product_id VARCHAR(64) NOT NULL REFERENCES product(id),
            trigger_kind VARCHAR(16) COLLATE "C" NOT NULL,
            trigger_id VARCHAR(52) COLLATE "C" NOT NULL,
            requirements_digest VARCHAR(64) COLLATE "C" NOT NULL,
            policy_digest VARCHAR(64) COLLATE "C" NOT NULL,
            input_digest VARCHAR(64) COLLATE "C" NOT NULL,
            decision_time_ms BIGINT NOT NULL,
            contract_version INTEGER NOT NULL,
            canonical_payload BYTEA NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT uq_mdd_input_key UNIQUE (environment, execution_scope_id, strategy_id, strategy_version, config_hash, product_id, trigger_kind, trigger_id),
            CONSTRAINT ck_mdd_input_id CHECK (input_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mdd_environment CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mdd_scope CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mdd_strategy CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mdd_version CHECK (strategy_version COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_mdd_config CHECK (config_hash COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mdd_product CHECK (product_id COLLATE "C" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'),
            CONSTRAINT ck_mdd_trigger_kind CHECK (trigger_kind = 'CANDLE'),
            CONSTRAINT ck_mdd_trigger CHECK (trigger_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,32}:(0|[1-9][0-9]{0,18})$' AND (length(split_part(trigger_id, ':', 2)) < 19 OR (length(split_part(trigger_id, ':', 2)) = 19 AND split_part(trigger_id, ':', 2) COLLATE "C" <= '9223372036854775807'))),
            CONSTRAINT ck_mdd_requirements CHECK (requirements_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mdd_policy CHECK (policy_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mdd_digest CHECK (input_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_mdd_time CHECK (decision_time_ms >= 0 AND decision_time_ms <= 9223372036854775807),
            CONSTRAINT ck_mdd_contract CHECK (contract_version = 1),
            CONSTRAINT ck_mdd_payload CHECK (octet_length(canonical_payload) BETWEEN 1 AND 8388608),
            CONSTRAINT ck_mdd_recorded CHECK (
                recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00'
                AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'
                AND recorded_at = date_trunc('milliseconds', recorded_at)
            )
        )
    """)
    op.execute("""
        CREATE FUNCTION reject_market_data_decision_input_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'market data decision input is append-only' USING ERRCODE = '55000';
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER market_data_decision_input_append_only
        BEFORE UPDATE OR DELETE OR TRUNCATE ON market_data_decision_input
        FOR EACH STATEMENT EXECUTE FUNCTION reject_market_data_decision_input_mutation()
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER market_data_decision_input_append_only ON market_data_decision_input"
    )
    op.execute("DROP TABLE market_data_decision_input")
    op.execute("DROP FUNCTION reject_market_data_decision_input_mutation()")
