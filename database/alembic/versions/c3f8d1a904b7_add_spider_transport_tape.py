"""Persist Spider transmission envelopes and shared scoped tape ordering."""

from alembic import op


revision = "c3f8d1a904b7"
down_revision = "a7d0c5e2f941"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE spider_runtime_scope "
        "ADD COLUMN last_tape_sequence BIGINT NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE spider_runtime_scope ADD CONSTRAINT "
        "ck_spider_runtime_scope_tape_sequence CHECK (last_tape_sequence >= 0)"
    )
    op.execute(
        """
        CREATE TABLE spider_runtime_transmission (
            transmission_group_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            operation_id VARCHAR(64) COLLATE "C" NOT NULL,
            plan_id VARCHAR(64) COLLATE "C" NOT NULL,
            generation BIGINT NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            operation VARCHAR(8) COLLATE "C" NOT NULL,
            request_payload BYTEA NOT NULL,
            request_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            response_payload BYTEA,
            response_sha256 VARCHAR(64) COLLATE "C",
            response_failure VARCHAR(32) COLLATE "C",
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            response_recorded_at TIMESTAMPTZ,
            CONSTRAINT fk_spider_runtime_transmission_operation
                FOREIGN KEY (operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_operation
                    (operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_transmission_plan
                FOREIGN KEY (plan_id, operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_plan
                    (plan_id, operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_transmission_scope_ref
                UNIQUE (transmission_group_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_runtime_transmission_id
                CHECK (transmission_group_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_transmission_generation_config
                CHECK (generation > 0 AND configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_transmission_operation
                CHECK (operation IN ('PLACE', 'CANCEL')),
            CONSTRAINT ck_spider_runtime_transmission_request
                CHECK (octet_length(request_payload) BETWEEN 1 AND 1048576 AND
                       request_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_transmission_response
                CHECK ((response_payload IS NULL) = (response_sha256 IS NULL) AND
                       (response_payload IS NULL OR
                        octet_length(response_payload) BETWEEN 1 AND 1048576) AND
                       (response_sha256 IS NULL OR
                        response_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$') AND
                       (response_failure IS NULL OR response_failure IN
                        ('TRANSPORT_UNKNOWN', 'RESPONSE_INVALID', 'RESPONSE_TOO_LARGE')) AND
                       NOT (response_payload IS NOT NULL AND response_failure IS NOT NULL) AND
                       ((response_payload IS NULL AND response_failure IS NULL AND
                         response_recorded_at IS NULL) OR
                        (response_recorded_at IS NOT NULL AND
                         (response_payload IS NOT NULL OR response_failure IS NOT NULL)))),
            CONSTRAINT ck_spider_runtime_transmission_timestamps
                CHECK (recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND
                       recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND
                       recorded_at = date_trunc('milliseconds', recorded_at) AND
                       (response_recorded_at IS NULL OR
                        (response_recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND
                         response_recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND
                         response_recorded_at = date_trunc('milliseconds', response_recorded_at))))
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_runtime_tape_index (
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            ordinal BIGINT NOT NULL,
            kind VARCHAR(16) COLLATE "C" NOT NULL,
            delivery_sequence BIGINT,
            transmission_group_id VARCHAR(64) COLLATE "C",
            PRIMARY KEY (environment, execution_scope_id, strategy_id, ordinal),
            CONSTRAINT fk_spider_runtime_tape_scope
                FOREIGN KEY (environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_scope
                    (environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_tape_delivery
                FOREIGN KEY (environment, execution_scope_id, strategy_id, delivery_sequence)
                REFERENCES spider_runtime_delivery
                    (environment, execution_scope_id, strategy_id, sequence),
            CONSTRAINT fk_spider_runtime_tape_transmission
                FOREIGN KEY (transmission_group_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_transmission
                    (transmission_group_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_tape_delivery
                UNIQUE (environment, execution_scope_id, strategy_id, delivery_sequence),
            CONSTRAINT uq_spider_runtime_tape_transmission
                UNIQUE (environment, execution_scope_id, strategy_id, transmission_group_id),
            CONSTRAINT ck_spider_runtime_tape_ordinal CHECK (ordinal > 0),
            CONSTRAINT ck_spider_runtime_tape_reference
                CHECK ((kind = 'DELIVERY' AND delivery_sequence IS NOT NULL AND
                        transmission_group_id IS NULL) OR
                       (kind = 'SEND_MARKER' AND delivery_sequence IS NULL AND
                        transmission_group_id IS NOT NULL))
        )
        """
    )
    op.execute(
        """
        INSERT INTO spider_runtime_tape_index
            (environment, execution_scope_id, strategy_id, ordinal, kind,
             delivery_sequence, transmission_group_id)
        SELECT environment, execution_scope_id, strategy_id, sequence, 'DELIVERY',
               sequence, NULL
        FROM spider_runtime_delivery
        ORDER BY environment, execution_scope_id, strategy_id, sequence
        """
    )
    op.execute(
        "UPDATE spider_runtime_scope SET last_tape_sequence = last_received_sequence"
    )


def downgrade() -> None:
    op.drop_table("spider_runtime_tape_index")
    op.drop_table("spider_runtime_transmission")
    op.drop_constraint(
        "ck_spider_runtime_scope_tape_sequence", "spider_runtime_scope", type_="check"
    )
    op.drop_column("spider_runtime_scope", "last_tape_sequence")
