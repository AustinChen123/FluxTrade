"""Persist explicit BUILD member resubmission intent and lineage."""

from alembic import op

revision = "4c2f8a91d5e7"
down_revision = "f47c2a91d683"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE spider_member_resubmission_request (
            request_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            actor VARCHAR(64) COLLATE "C" NOT NULL,
            idempotency_key VARCHAR(128) COLLATE "C" NOT NULL,
            expected_version INTEGER NOT NULL,
            generation BIGINT NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            initial_plan_id VARCHAR(64) COLLATE "C" NOT NULL,
            target_action_id VARCHAR(64) COLLATE "C" NOT NULL,
            request_payload BYTEA NOT NULL,
            request_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            CONSTRAINT uq_spider_member_resubmit_request_key
                UNIQUE (environment, execution_scope_id, strategy_id, actor, idempotency_key),
            CONSTRAINT uq_spider_member_resubmit_request_scope
                UNIQUE (request_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_member_resubmit_request_scope
                FOREIGN KEY (environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_scope (environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_member_resubmit_request_target
                FOREIGN KEY (target_action_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT ck_spider_member_resubmit_request_id
                CHECK (request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmit_environment
                CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_member_resubmit_scope
                CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_member_resubmit_strategy
                CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_member_resubmit_actor
                CHECK (actor COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$'),
            CONSTRAINT ck_spider_member_resubmit_key
                CHECK (idempotency_key COLLATE "C" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'),
            CONSTRAINT ck_spider_member_resubmit_version CHECK (expected_version BETWEEN 0 AND 2147483647),
            CONSTRAINT ck_spider_member_resubmit_generation CHECK (generation > 0),
            CONSTRAINT ck_spider_member_resubmit_config_hash
                CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmit_plan_id
                CHECK (initial_plan_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmit_target_id
                CHECK (target_action_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmit_payload CHECK (octet_length(request_payload) BETWEEN 1 AND 1048576),
            CONSTRAINT ck_spider_member_resubmit_payload_hash
                CHECK (request_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmit_recorded_at CHECK (recorded_at = date_trunc('milliseconds', recorded_at))
        )
        """
    )
    op.execute(
        """
        CREATE TABLE spider_member_resubmission (
            request_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            control_receipt_id VARCHAR(64) COLLATE "C" NOT NULL UNIQUE,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL,
            generation BIGINT NOT NULL,
            configuration_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            initial_plan_id VARCHAR(64) COLLATE "C" NOT NULL,
            initial_member_id VARCHAR(64) COLLATE "C" NOT NULL,
            target_action_id VARCHAR(64) COLLATE "C" NOT NULL,
            resubmission_action_id VARCHAR(64) COLLATE "C" NOT NULL UNIQUE,
            tape_ordinal BIGINT NOT NULL,
            before_checkpoint_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            after_checkpoint_sha256 VARCHAR(64) COLLATE "C" NOT NULL,
            CONSTRAINT fk_spider_member_resubmission_request
                FOREIGN KEY (request_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_member_resubmission_request (request_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_member_resubmission_plan
                FOREIGN KEY (initial_plan_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_plan (plan_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_member_resubmission_initial_member
                FOREIGN KEY (initial_member_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_member_resubmission_target
                FOREIGN KEY (target_action_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT fk_spider_member_resubmission_action
                FOREIGN KEY (resubmission_action_id, environment, execution_scope_id, strategy_id)
                REFERENCES spider_runtime_action (action_id, environment, execution_scope_id, strategy_id),
            CONSTRAINT uq_spider_member_resubmission_tape_ordinal
                UNIQUE (environment, execution_scope_id, strategy_id, tape_ordinal),
            CONSTRAINT ck_spider_member_resubmission_request_id CHECK (request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_receipt_id CHECK (control_receipt_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_environment CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_member_resubmission_scope CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_member_resubmission_strategy CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spider_member_resubmission_generation CHECK (generation > 0),
            CONSTRAINT ck_spider_member_resubmission_config_hash CHECK (configuration_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_plan_id CHECK (initial_plan_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_initial_id CHECK (initial_member_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_target_id CHECK (target_action_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_action_id CHECK (resubmission_action_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_tape_ordinal CHECK (tape_ordinal > 0),
            CONSTRAINT ck_spider_member_resubmission_before_hash CHECK (before_checkpoint_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spider_member_resubmission_after_hash CHECK (after_checkpoint_sha256 COLLATE "C" ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute(
        "ALTER TABLE spider_runtime_tape_index "
        "ADD COLUMN member_resubmission_request_id VARCHAR(64) COLLATE \"C\""
    )
    op.execute(
        "ALTER TABLE spider_runtime_tape_index ADD CONSTRAINT "
        "fk_spider_runtime_tape_member_resubmission FOREIGN KEY "
        "(member_resubmission_request_id, environment, execution_scope_id, strategy_id) "
        "REFERENCES spider_member_resubmission_request "
        "(request_id, environment, execution_scope_id, strategy_id)"
    )
    op.execute(
        "ALTER TABLE spider_runtime_tape_index ADD CONSTRAINT "
        "uq_spider_runtime_tape_member_resubmission UNIQUE "
        "(environment, execution_scope_id, strategy_id, member_resubmission_request_id)"
    )
    op.execute(
        "ALTER TABLE spider_runtime_tape_index DROP CONSTRAINT ck_spider_runtime_tape_reference"
    )
    op.execute(
        "ALTER TABLE spider_runtime_tape_index ADD CONSTRAINT ck_spider_runtime_tape_reference CHECK ("
        "(kind = 'DELIVERY' AND delivery_sequence IS NOT NULL AND transmission_group_id IS NULL AND member_resubmission_request_id IS NULL) OR "
        "(kind = 'SEND_MARKER' AND delivery_sequence IS NULL AND transmission_group_id IS NOT NULL AND member_resubmission_request_id IS NULL) OR "
        "(kind = 'RESUBMIT' AND delivery_sequence IS NULL AND transmission_group_id IS NULL AND member_resubmission_request_id IS NOT NULL))"
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM spider_runtime_tape_index WHERE kind = 'RESUBMIT') "
        "THEN RAISE EXCEPTION 'Cannot downgrade while Spider resubmission tape entries exist'; END IF; END $$"
    )
    op.execute("ALTER TABLE spider_runtime_tape_index DROP CONSTRAINT ck_spider_runtime_tape_reference")
    op.execute(
        "ALTER TABLE spider_runtime_tape_index ADD CONSTRAINT ck_spider_runtime_tape_reference CHECK ("
        "(kind = 'DELIVERY' AND delivery_sequence IS NOT NULL AND transmission_group_id IS NULL) OR "
        "(kind = 'SEND_MARKER' AND delivery_sequence IS NULL AND transmission_group_id IS NOT NULL))"
    )
    op.execute("ALTER TABLE spider_runtime_tape_index DROP CONSTRAINT uq_spider_runtime_tape_member_resubmission")
    op.execute("ALTER TABLE spider_runtime_tape_index DROP CONSTRAINT fk_spider_runtime_tape_member_resubmission")
    op.execute("ALTER TABLE spider_runtime_tape_index DROP COLUMN member_resubmission_request_id")
    op.execute("DROP TABLE spider_member_resubmission")
    op.execute("DROP TABLE spider_member_resubmission_request")
