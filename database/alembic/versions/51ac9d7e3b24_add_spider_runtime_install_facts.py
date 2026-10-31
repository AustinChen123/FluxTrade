"""Persist the first guarded Spider INSTALL plan and financial receipts."""

from alembic import op

revision = "51ac9d7e3b24"
down_revision = "3b7d9e4a6c21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE spider_runtime_plan (
            plan_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            operation_id VARCHAR(64) COLLATE "C" NOT NULL,
            generation BIGINT NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            source_checkpoint_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            specification_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            observed_input_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            tape_cursor VARCHAR(256) COLLATE "C" NOT NULL,
            manifest_payload BYTEA NOT NULL,
            manifest_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            plan_status VARCHAR(12) COLLATE "C" NOT NULL,
            missing_members BYTEA NOT NULL,
            CONSTRAINT fk_spider_runtime_plan_operation
                FOREIGN KEY (operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_operation
                    (operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_plan_scope_ref
                UNIQUE (plan_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_plan_operation_ref
                UNIQUE (plan_id, operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_runtime_plan_id
                CHECK (plan_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_operation_id
                CHECK (operation_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_generation CHECK (generation > 0),
            CONSTRAINT ck_spider_runtime_plan_configuration_hash
                CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_source_hash
                CHECK (source_checkpoint_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_specification_hash
                CHECK (specification_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_observed_hash
                CHECK (observed_input_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_manifest
                CHECK (octet_length(manifest_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_runtime_plan_manifest_hash
                CHECK (manifest_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_plan_status
                CHECK (plan_status IN ('INCOMPLETE', 'COMPLETE')),
            CONSTRAINT ck_spider_runtime_plan_missing_members
                CHECK (octet_length(missing_members) BETWEEN 1 AND 1048576)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_runtime_scope (
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL REFERENCES strategy(id),
            host_contract_id VARCHAR(64) COLLATE "C" NOT NULL,
            host_contract_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            confirmation_request_id VARCHAR(64) COLLATE "C" NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            generation BIGINT NOT NULL,
            state_version BIGINT NOT NULL,
            control_mode VARCHAR(8) COLLATE "C" NOT NULL,
            current_operation_id VARCHAR(64) COLLATE "C" NOT NULL,
            last_received_sequence BIGINT NOT NULL DEFAULT 0,
            last_consumed_sequence BIGINT NOT NULL DEFAULT 0,
            source_checkpoint BYTEA NOT NULL,
            source_checkpoint_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            control_barrier_sequence BIGINT NOT NULL DEFAULT 0,
            initial_plan_id VARCHAR(64) COLLATE "C" NOT NULL,
            PRIMARY KEY (environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_scope_confirmation
                FOREIGN KEY (confirmation_request_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_configuration_request
                    (request_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_scope_operation
                FOREIGN KEY (current_operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_operation
                    (operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_scope_initial_plan
                FOREIGN KEY (initial_plan_id, current_operation_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_plan
                    (plan_id, operation_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_runtime_scope_host
                CHECK (host_contract_id = 'SPIDER_DURABLE_HOST_V1'),
            CONSTRAINT ck_spider_runtime_scope_host_hash
                CHECK (host_contract_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_scope_confirmation_id
                CHECK (confirmation_request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_scope_configuration_hash
                CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_scope_generation
                CHECK (generation BETWEEN 1 AND 9223372036854775807),
            CONSTRAINT ck_spider_runtime_scope_state_version CHECK (state_version BETWEEN 0 AND 2147483647),
            CONSTRAINT ck_spider_runtime_scope_control CHECK (control_mode IN ('RUN', 'STOP', 'DRAIN')),
            CONSTRAINT ck_spider_runtime_scope_operation_id
                CHECK (current_operation_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_scope_sequences
                CHECK (last_received_sequence >= 0 AND last_consumed_sequence BETWEEN 0 AND last_received_sequence),
            CONSTRAINT ck_spider_runtime_scope_checkpoint
                CHECK (octet_length(source_checkpoint) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_runtime_scope_checkpoint_hash
                CHECK (source_checkpoint_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_scope_barrier CHECK (control_barrier_sequence >= 0),
            CONSTRAINT ck_spider_runtime_scope_plan_id
                CHECK (initial_plan_id COLLATE "C" ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_runtime_action (
            action_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            plan_id VARCHAR(64) COLLATE "C" NOT NULL,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            trigger_delivery_id VARCHAR(64) COLLATE "C",
            parent_action_id VARCHAR(64) COLLATE "C",
            generation BIGINT NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            role VARCHAR(32) COLLATE "C" NOT NULL,
            product_id VARCHAR(128) COLLATE "C" NOT NULL,
            local_order_id VARCHAR(36) COLLATE "C" NOT NULL REFERENCES "order"(id),
            original_payload BYTEA NOT NULL,
            payload_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            candidate_ordinal BIGINT NOT NULL,
            transmission_group BIGINT NOT NULL,
            transmission_ordinal BIGINT NOT NULL,
            outcome_state VARCHAR(16) COLLATE "C" NOT NULL,
            outcome_reference VARCHAR(64) COLLATE "C",
            CONSTRAINT fk_spider_runtime_action_plan
                FOREIGN KEY (plan_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_plan
                    (plan_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_action_parent
                FOREIGN KEY (parent_action_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action
                    (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_action_scope_ref
                UNIQUE (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_action_candidate UNIQUE (plan_id, candidate_ordinal),
            CONSTRAINT ck_spider_runtime_action_id CHECK (action_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_action_trigger
                CHECK (trigger_delivery_id IS NULL OR trigger_delivery_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_action_generation CHECK (generation > 0),
            CONSTRAINT ck_spider_runtime_action_configuration_hash
                CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_action_role CHECK (role COLLATE "C" = 'GRID'),
            CONSTRAINT ck_spider_runtime_action_payload
                CHECK (octet_length(original_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_runtime_action_payload_hash
                CHECK (payload_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_action_ordinals
                CHECK (candidate_ordinal >= 0 AND transmission_group >= 0 AND transmission_ordinal >= 0),
            CONSTRAINT ck_spider_runtime_action_outcome
                CHECK (outcome_state IN ('UNSENT', 'SEND_UNKNOWN', 'ACCEPTED', 'REJECTED', 'TERMINAL')),
            CONSTRAINT ck_spider_runtime_action_outcome_ref
                CHECK (outcome_reference IS NULL OR outcome_reference COLLATE "C" ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_runtime_binding (
            action_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            local_order_id VARCHAR(36) COLLATE "C" NOT NULL REFERENCES "order"(id),
            source_product_id VARCHAR(128) COLLATE "C" NOT NULL,
            canonical_product_id VARCHAR(128) COLLATE "C" NOT NULL,
            source_client_order_id VARCHAR(64) COLLATE "C" NOT NULL,
            product_map_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            CONSTRAINT fk_spider_runtime_binding_action
                FOREIGN KEY (action_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action
                    (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_binding_order UNIQUE (local_order_id),
            CONSTRAINT ck_spider_runtime_binding_action_id
                CHECK (action_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_binding_source_client_id
                CHECK (octet_length(source_client_order_id) BETWEEN 1 AND 64),
            CONSTRAINT ck_spider_runtime_binding_map_hash
                CHECK (product_map_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_runtime_delivery (
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            sequence BIGINT NOT NULL,
            delivery_id VARCHAR(64) COLLATE "C" NOT NULL,
            content_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            kind VARCHAR(32) COLLATE "C" NOT NULL,
            local_order_id VARCHAR(36) COLLATE "C" NOT NULL REFERENCES "order"(id),
            trade_id VARCHAR(64) COLLATE "C" REFERENCES trade(id),
            action_id VARCHAR(64) COLLATE "C" NOT NULL,
            generation BIGINT NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            role VARCHAR(32) COLLATE "C" NOT NULL,
            source_payload BYTEA NOT NULL,
            visible_at TIMESTAMPTZ NOT NULL,
            elapsed_ms BIGINT,
            disposition VARCHAR(12) COLLATE "C" NOT NULL DEFAULT 'PENDING',
            suppression_reason VARCHAR(64) COLLATE "C",
            PRIMARY KEY (environment, execution_scope_id, strategy_id, sequence),
            CONSTRAINT fk_spider_runtime_delivery_scope
                FOREIGN KEY (environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_scope (environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_runtime_delivery_action
                FOREIGN KEY (action_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action
                    (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_runtime_delivery_identity
                UNIQUE (delivery_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_runtime_delivery_sequence CHECK (sequence > 0),
            CONSTRAINT ck_spider_runtime_delivery_id CHECK (delivery_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_delivery_hash CHECK (content_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_delivery_generation CHECK (generation > 0),
            CONSTRAINT ck_spider_runtime_delivery_configuration_hash
                CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_runtime_delivery_role CHECK (role COLLATE "C" = 'GRID'),
            CONSTRAINT ck_spider_runtime_delivery_payload
                CHECK (octet_length(source_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_runtime_delivery_visible_at
                CHECK (visible_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND
                       visible_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND
                       visible_at = date_trunc('milliseconds', visible_at)),
            CONSTRAINT ck_spider_runtime_delivery_elapsed CHECK (elapsed_ms IS NULL OR elapsed_ms >= 0),
            CONSTRAINT ck_spider_runtime_delivery_disposition
                CHECK (disposition IN ('PENDING', 'CONSUMED', 'SUPPRESSED')),
            CONSTRAINT ck_spider_runtime_delivery_suppression
                CHECK (suppression_reason IS NULL OR disposition = 'SUPPRESSED')
        )
        """
    )


def downgrade() -> None:
    op.drop_table("spider_runtime_delivery")
    op.drop_table("spider_runtime_binding")
    op.drop_table("spider_runtime_action")
    op.drop_table("spider_runtime_scope")
    op.drop_table("spider_runtime_plan")
