"""Decision terminal schema; DML guards do not protect against admin/DDL changes."""

from alembic import op

revision = "2f6c8a1e9b04"
down_revision = "8e4b2c91a6d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE market_data_application ADD COLUMN decision_contract_version INTEGER"
    )
    op.execute(
        "ALTER TABLE market_data_application ADD CONSTRAINT ck_mda_decision_contract CHECK (decision_contract_version IS NULL OR decision_contract_version = 1)"
    )
    op.execute(
        "ALTER TABLE market_data_application ADD CONSTRAINT uq_mda_decision_contract UNIQUE (environment, product_id, timeframe, timestamp, decision_contract_version)"
    )
    op.execute("""CREATE TABLE market_data_decision_batch (
environment VARCHAR(64) NOT NULL,
product_id VARCHAR(64) NOT NULL,
timeframe VARCHAR(32) NOT NULL,
bar_start_ms BIGINT NOT NULL,
execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
contract_version INTEGER NOT NULL,
participant_count INTEGER NOT NULL,
canonical_payload BYTEA NOT NULL,
batch_digest VARCHAR(64) COLLATE "C" NOT NULL,
recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
CONSTRAINT pk_mdb PRIMARY KEY (environment, product_id, timeframe, bar_start_ms),
CONSTRAINT uq_mdb_scope UNIQUE (environment, product_id, timeframe, bar_start_ms, execution_scope_id, contract_version),
CONSTRAINT fk_mdb_receipt FOREIGN KEY (environment, product_id, timeframe, bar_start_ms, contract_version) REFERENCES market_data_application(environment, product_id, timeframe, timestamp, decision_contract_version) ON DELETE RESTRICT,
CONSTRAINT ck_mdb_environment CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,64}$'),
CONSTRAINT ck_mdb_product CHECK (product_id COLLATE "C" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'),
CONSTRAINT ck_mdb_timeframe CHECK (timeframe COLLATE "C" ~ '^[A-Za-z0-9_-]{1,32}$'),
CONSTRAINT ck_mdb_time CHECK (bar_start_ms >= 0),
CONSTRAINT ck_mdb_scope CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
CONSTRAINT ck_mdb_contract CHECK (contract_version = 1),
CONSTRAINT ck_mdb_count CHECK (participant_count BETWEEN 0 AND 256),
CONSTRAINT ck_mdb_payload CHECK (octet_length(canonical_payload) BETWEEN 1 AND 1048576),
CONSTRAINT ck_mdb_digest CHECK (batch_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
CONSTRAINT ck_mdb_recorded CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND recorded_at = date_trunc('milliseconds', recorded_at))
)""")
    op.execute("""CREATE TABLE market_data_decision_outcome (
environment VARCHAR(64) NOT NULL,
product_id VARCHAR(64) NOT NULL,
timeframe VARCHAR(32) NOT NULL,
bar_start_ms BIGINT NOT NULL,
execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
contract_version INTEGER NOT NULL,
strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
strategy_version VARCHAR(128) COLLATE "C" NOT NULL,
config_hash VARCHAR(64) COLLATE "C" NOT NULL,
trigger_kind VARCHAR(16) COLLATE "C" NOT NULL,
trigger_id VARCHAR(52) COLLATE "C" NOT NULL,
disposition VARCHAR(16) NOT NULL,
input_id VARCHAR(64) COLLATE "C",
input_digest VARCHAR(64) COLLATE "C",
reason VARCHAR(64),
signal_suppressed BOOLEAN NOT NULL,
suppression_reason VARCHAR(64),
CONSTRAINT pk_mdo PRIMARY KEY (environment, execution_scope_id, strategy_id, strategy_version, config_hash, product_id, trigger_kind, trigger_id),
CONSTRAINT uq_mdo_strategy UNIQUE (environment, product_id, timeframe, bar_start_ms, execution_scope_id, strategy_id),
CONSTRAINT fk_mdo_batch FOREIGN KEY (environment, product_id, timeframe, bar_start_ms, execution_scope_id, contract_version) REFERENCES market_data_decision_batch(environment, product_id, timeframe, bar_start_ms, execution_scope_id, contract_version) ON DELETE RESTRICT,
CONSTRAINT fk_mdo_input FOREIGN KEY (input_id) REFERENCES market_data_decision_input(input_id) ON DELETE RESTRICT,
CONSTRAINT ck_mdo_environment CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,64}$'),
CONSTRAINT ck_mdo_product CHECK (product_id COLLATE "C" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'),
CONSTRAINT ck_mdo_timeframe CHECK (timeframe COLLATE "C" ~ '^[A-Za-z0-9_-]{1,32}$'),
CONSTRAINT ck_mdo_time CHECK (bar_start_ms >= 0),
CONSTRAINT ck_mdo_scope CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
CONSTRAINT ck_mdo_contract CHECK (contract_version = 1),
CONSTRAINT ck_mdo_strategy CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
CONSTRAINT ck_mdo_version CHECK (strategy_version COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
CONSTRAINT ck_mdo_config CHECK (config_hash COLLATE "C" ~ '^[0-9a-f]{64}$'),
CONSTRAINT ck_mdo_trigger CHECK (trigger_kind = 'CANDLE' AND trigger_id = timeframe || ':' || bar_start_ms::text),
CONSTRAINT ck_mdo_input CHECK (input_id IS NULL OR input_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
CONSTRAINT ck_mdo_digest CHECK (input_digest IS NULL OR input_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
CONSTRAINT ck_mdo_pair CHECK ((input_id IS NULL) = (input_digest IS NULL)),
CONSTRAINT ck_mdo_disposition CHECK ((disposition = 'APPLIED' AND input_id IS NOT NULL AND reason IS NULL) OR (disposition = 'SKIPPED' AND reason IS NOT NULL AND reason IN ('INPUT_STORE_FAILED', 'INPUT_COMMIT_UNCONFIRMED') AND NOT signal_suppressed)),
CONSTRAINT ck_mdo_suppression CHECK ((NOT signal_suppressed AND suppression_reason IS NULL) OR (signal_suppressed AND disposition = 'APPLIED' AND suppression_reason IS NOT NULL AND suppression_reason = 'SNAPSHOT_REVOKED'))
)""")
    op.execute("""
        CREATE FUNCTION reject_market_data_terminal_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'market data terminal evidence is append-only' USING ERRCODE = '55000';
        END;
        $$
    """)
    for table in ("market_data_decision_batch", "market_data_decision_outcome"):
        op.execute(
            f"CREATE TRIGGER {table}_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION reject_market_data_terminal_mutation()"
        )


def downgrade() -> None:
    for table in ("market_data_decision_outcome", "market_data_decision_batch"):
        op.execute(f"DROP TRIGGER {table}_append_only ON {table}")
        op.execute(f"DROP TABLE {table}")
    op.execute("DROP FUNCTION reject_market_data_terminal_mutation()")
    op.execute(
        "ALTER TABLE market_data_application DROP CONSTRAINT uq_mda_decision_contract"
    )
    op.execute(
        "ALTER TABLE market_data_application DROP CONSTRAINT ck_mda_decision_contract"
    )
    op.execute(
        "ALTER TABLE market_data_application DROP COLUMN decision_contract_version"
    )
