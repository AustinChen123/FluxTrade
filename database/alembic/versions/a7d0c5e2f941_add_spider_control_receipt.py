"""Persist scoped Spider STOP control receipts."""

from alembic import op

revision = "a7d0c5e2f941"
down_revision = "51ac9d7e3b24"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE spider_control_receipt (
            receipt_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            actor VARCHAR(64) COLLATE "C" NOT NULL,
            idempotency_key VARCHAR(128) COLLATE "C" NOT NULL,
            request_payload BYTEA NOT NULL,
            request_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            operation_id VARCHAR(64) COLLATE "C",
            generation BIGINT NOT NULL,
            before_state_version INTEGER NOT NULL,
            after_state_version INTEGER NOT NULL,
            barrier_sequence BIGINT NOT NULL,
            outcome VARCHAR(12) COLLATE "C" NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT uq_spider_control_receipt_idempotency
                UNIQUE (environment, execution_scope_id, strategy_id, actor, idempotency_key),
            CONSTRAINT fk_spider_control_receipt_scope
                FOREIGN KEY (environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_scope (environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_control_receipt_operation
                FOREIGN KEY (operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_operation
                    (operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_control_receipt_id
                CHECK (receipt_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_control_receipt_environment
                CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_control_receipt_scope
                CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_control_receipt_strategy
                CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_control_receipt_actor
                CHECK (actor COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$'),
            CONSTRAINT ck_spider_control_receipt_key
                CHECK (idempotency_key COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'),
            CONSTRAINT ck_spider_control_receipt_payload
                CHECK (octet_length(request_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_control_receipt_hash
                CHECK (request_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_control_receipt_operation_id
                CHECK (operation_id IS NULL OR operation_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_control_receipt_generation CHECK (generation > 0),
            CONSTRAINT ck_spider_control_receipt_before_version
                CHECK (before_state_version BETWEEN 0 AND 2147483647),
            CONSTRAINT ck_spider_control_receipt_after_version
                CHECK (after_state_version BETWEEN 1 AND 2147483647
                       AND after_state_version = before_state_version + 1),
            CONSTRAINT ck_spider_control_receipt_barrier CHECK (barrier_sequence >= 0),
            CONSTRAINT ck_spider_control_receipt_outcome CHECK (outcome = 'COMPLETED'),
            CONSTRAINT ck_spider_control_receipt_recorded_at
                CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00'
                       AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'
                       AND recorded_at = date_trunc('milliseconds', recorded_at))
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE spider_control_receipt")
