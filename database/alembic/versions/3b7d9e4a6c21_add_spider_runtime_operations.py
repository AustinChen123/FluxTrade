"""Persist immutable Spider operation proposal and confirmation evidence."""

from alembic import op

revision = "3b7d9e4a6c21"
down_revision = "e4a1c9d7b203"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE spider_runtime_operation (
            operation_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL REFERENCES strategy(id),
            actor VARCHAR(64) COLLATE "C" NOT NULL,
            operation_key VARCHAR(128) COLLATE "C" NOT NULL,
            operation_core_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            operation_core_payload BYTEA NOT NULL,
            configuration_request_id VARCHAR(64) COLLATE "C" NOT NULL,
            phase VARCHAR(16) NOT NULL DEFAULT 'PREVIEW',
            authorization_revision INTEGER,
            confirmation_request_id VARCHAR(64) COLLATE "C",
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT uq_spider_runtime_operation_scope_ref
                UNIQUE (operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_operation_configuration_scope
                FOREIGN KEY (configuration_request_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_configuration_request
                    (request_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_runtime_operation_id
                CHECK (operation_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_operation_core_hash
                CHECK (operation_core_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_operation_core_payload
                CHECK (octet_length(operation_core_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_runtime_operation_phase
                CHECK (phase IN ('PREVIEW', 'DRAIN', 'POSITION', 'RECONCILE', 'COMMIT',
                                 'BUILD', 'RUN', 'INCOMPLETE', 'STOP')),
            CONSTRAINT ck_spider_runtime_operation_revision
                CHECK (authorization_revision IS NULL OR authorization_revision BETWEEN 1 AND 2147483647),
            CONSTRAINT ck_spider_runtime_operation_confirmation
                CHECK (confirmation_request_id IS NULL OR
                       confirmation_request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_operation_recorded_at
                CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND
                       recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND
                       recorded_at = date_trunc('milliseconds', recorded_at))
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_operation_request (
            request_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            operation_id VARCHAR(64) COLLATE "C" NOT NULL,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            request_kind VARCHAR(16) NOT NULL,
            actor VARCHAR(64) COLLATE "C" NOT NULL,
            idempotency_key VARCHAR(128) COLLATE "C" NOT NULL,
            operation_key VARCHAR(128) COLLATE "C" NOT NULL,
            operation_core_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            authorization_revision INTEGER NOT NULL,
            proposal_request_id VARCHAR(64) COLLATE "C",
            authorization_confirmed BOOLEAN NOT NULL,
            request_payload BYTEA NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT fk_spider_operation_request_operation_scope
                FOREIGN KEY (operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_operation
                    (operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_operation_request_proposal_scope
                FOREIGN KEY (proposal_request_id, operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_operation_request
                    (request_id, operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_operation_request_idempotency
                UNIQUE (operation_id, environment, execution_scope_id, strategy_id, actor, idempotency_key),
            CONSTRAINT uq_spider_operation_request_scope_ref
                UNIQUE (request_id, operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_operation_request_id
                CHECK (request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_operation_request_operation_id
                CHECK (operation_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_operation_request_core_hash
                CHECK (operation_core_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_operation_request_kind
                CHECK (request_kind IN ('PROPOSAL', 'CONFIRMATION')),
            CONSTRAINT ck_spider_operation_request_revision
                CHECK (authorization_revision BETWEEN 1 AND 2147483647),
            CONSTRAINT ck_spider_operation_request_payload
                CHECK (octet_length(request_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_operation_request_confirmed
                CHECK (authorization_confirmed = (request_kind = 'CONFIRMATION')),
            CONSTRAINT ck_spider_operation_request_recorded_at
                CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND
                       recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND
                       recorded_at = date_trunc('milliseconds', recorded_at))
        )
        """
    )


def downgrade() -> None:
    op.drop_table("spider_operation_request")
    op.drop_table("spider_runtime_operation")
