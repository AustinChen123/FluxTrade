"""Add immutable Spider configuration request evidence."""

from alembic import op

revision = "e4a1c9d7b203"
down_revision = "f27c8d14a9b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE spider_configuration_request (
            request_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL REFERENCES strategy(id),
            request_kind VARCHAR(16) COLLATE "C" NOT NULL,
            actor VARCHAR(64) COLLATE "C" NOT NULL,
            idempotency_key VARCHAR(128) COLLATE "C" NOT NULL,
            expected_state_version INTEGER NOT NULL,
            configuration_payload BYTEA NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            proposal_request_id VARCHAR(64) COLLATE "C",
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT uq_spider_config_request_scope_ref
                UNIQUE (request_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_config_request_proposal_scope
                FOREIGN KEY (proposal_request_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_configuration_request
                    (request_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_config_request_id
                CHECK (request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_config_environment
                CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_config_execution_scope
                CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_config_strategy_id
                CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_config_request_kind
                CHECK (request_kind IN ('PROPOSAL', 'CONFIRMATION')),
            CONSTRAINT ck_spider_config_actor
                CHECK (actor COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$'),
            CONSTRAINT ck_spider_config_idempotency_key
                CHECK (idempotency_key COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'),
            CONSTRAINT ck_spider_config_state_version
                CHECK (expected_state_version BETWEEN 0 AND 2147483647),
            CONSTRAINT ck_spider_config_payload
                CHECK (octet_length(configuration_payload) BETWEEN 1 AND 65536),
            CONSTRAINT ck_spider_config_digest
                CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_config_proposal_reference
                CHECK ((request_kind = 'PROPOSAL' AND proposal_request_id IS NULL) OR
                       (request_kind = 'CONFIRMATION' AND proposal_request_id IS NOT NULL)),
            CONSTRAINT ck_spider_config_recorded_at
                CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND
                       recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND
                       recorded_at = date_trunc('milliseconds', recorded_at))
        )
        """
    )


def downgrade() -> None:
    op.drop_table("spider_configuration_request")
