"""Add durable source-market observations to the Spider runtime tape."""

from alembic import op


revision = "d8a61c3f9b42"
down_revision = "c3f8d1a904b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE spider_runtime_plan "
        "ADD COLUMN original_build_checkpoint BYTEA, "
        "ADD COLUMN original_build_checkpoint_sha256 VARCHAR(64) COLLATE \"C\""
    )
    op.execute(
        """
        UPDATE spider_runtime_plan AS plan
        SET original_build_checkpoint = runtime.source_checkpoint,
            original_build_checkpoint_sha256 = runtime.source_checkpoint_sha256
        FROM spider_runtime_scope AS runtime
        WHERE runtime.initial_plan_id = plan.plan_id
          AND runtime.environment = plan.environment
          AND runtime.execution_scope_id = plan.execution_scope_id
          AND runtime.strategy_id = plan.strategy_id
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM spider_runtime_plan
                WHERE original_build_checkpoint IS NULL
                   OR original_build_checkpoint_sha256 IS NULL
            ) THEN
                RAISE EXCEPTION 'Spider plan is missing its original BUILD checkpoint';
            END IF;
        END $$
        """
    )
    op.execute(
        "ALTER TABLE spider_runtime_plan "
        "ALTER COLUMN original_build_checkpoint SET NOT NULL, "
        "ALTER COLUMN original_build_checkpoint_sha256 SET NOT NULL"
    )
    op.execute(
        "ALTER TABLE spider_runtime_plan ADD CONSTRAINT "
        "ck_spider_runtime_plan_build_checkpoint "
        "CHECK (octet_length(original_build_checkpoint) BETWEEN 1 AND 1048576)"
    )
    op.execute(
        "ALTER TABLE spider_runtime_plan ADD CONSTRAINT "
        "ck_spider_runtime_plan_build_checkpoint_hash "
        "CHECK (original_build_checkpoint_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$')"
    )

    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_role"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ALTER COLUMN local_order_id DROP NOT NULL, "
        "ALTER COLUMN action_id DROP NOT NULL, ALTER COLUMN role DROP NOT NULL"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_kind "
        "CHECK (kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET'))"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_kind_references CHECK ("
        "(kind = 'SOURCE_MARKET' AND local_order_id IS NULL AND trade_id IS NULL "
        "AND action_id IS NULL AND role IS NULL) OR "
        "(kind IN ('FILL', 'ORDER_STATE') AND local_order_id IS NOT NULL "
        "AND action_id IS NOT NULL AND role IS NOT NULL "
        "AND role COLLATE \"C\" = 'GRID'))"
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM spider_runtime_delivery WHERE kind = 'SOURCE_MARKET'
            ) THEN
                RAISE EXCEPTION 'Cannot downgrade while Spider source-market receipts exist';
            END IF;
        END $$
        """
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT "
        "ck_spider_runtime_delivery_kind_references"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ALTER COLUMN local_order_id SET NOT NULL, "
        "ALTER COLUMN action_id SET NOT NULL, ALTER COLUMN role SET NOT NULL"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_role CHECK (role COLLATE \"C\" = 'GRID')"
    )
    op.drop_constraint(
        "ck_spider_runtime_plan_build_checkpoint_hash", "spider_runtime_plan", type_="check"
    )
    op.drop_constraint(
        "ck_spider_runtime_plan_build_checkpoint", "spider_runtime_plan", type_="check"
    )
    op.drop_column("spider_runtime_plan", "original_build_checkpoint_sha256")
    op.drop_column("spider_runtime_plan", "original_build_checkpoint")
