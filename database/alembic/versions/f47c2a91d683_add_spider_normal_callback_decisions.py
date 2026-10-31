"""Persist normal Spider callback decisions and transport-result receipts."""

from alembic import op


revision = "f47c2a91d683"
down_revision = "d8a61c3f9b42"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE spider_runtime_action "
        "ADD COLUMN operation VARCHAR(8) COLLATE \"C\" NOT NULL DEFAULT 'PLACE'"
    )
    op.execute(
        "ALTER TABLE spider_runtime_action ADD CONSTRAINT "
        "ck_spider_runtime_action_operation CHECK (operation IN ('PLACE', 'CANCEL'))"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery "
        "ADD COLUMN decision_payload BYTEA, "
        "ADD COLUMN decision_sha256 VARCHAR(64) COLLATE \"C\", "
        "ADD COLUMN full_callback_key VARCHAR(64) COLLATE \"C\""
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_decision_pair CHECK "
        "((decision_payload IS NULL) = (decision_sha256 IS NULL))"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_decision_size CHECK "
        "(decision_payload IS NULL OR octet_length(decision_payload) BETWEEN 1 AND 1048576)"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_decision_hash CHECK "
        "(decision_sha256 IS NULL OR decision_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$')"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_full_callback_hash CHECK "
        "(full_callback_key IS NULL OR full_callback_key COLLATE \"C\" ~ '^[0-9a-f]{64}$')"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "uq_spider_runtime_delivery_full_callback UNIQUE "
        "(environment, execution_scope_id, strategy_id, full_callback_key)"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_kind CHECK "
        "(kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET', 'TRANSPORT_RESULT'))"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT "
        "ck_spider_runtime_delivery_kind_references"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_kind_references CHECK ("
        "(kind IN ('SOURCE_MARKET', 'TRANSPORT_RESULT') AND local_order_id IS NULL "
        "AND trade_id IS NULL AND action_id IS NULL AND role IS NULL) OR "
        "(kind IN ('FILL', 'ORDER_STATE') AND local_order_id IS NOT NULL "
        "AND action_id IS NOT NULL AND role IS NOT NULL AND role COLLATE \"C\" = 'GRID'))"
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM spider_runtime_delivery "
        "WHERE kind = 'TRANSPORT_RESULT' OR decision_payload IS NOT NULL "
        "OR full_callback_key IS NOT NULL) OR EXISTS (SELECT 1 FROM spider_runtime_action "
        "WHERE operation = 'CANCEL') THEN RAISE EXCEPTION "
        "'Cannot downgrade while Spider callback decisions exist'; END IF; END $$"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT "
        "ck_spider_runtime_delivery_kind_references"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_kind CHECK "
        "(kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET'))"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT "
        "ck_spider_runtime_delivery_kind_references CHECK ("
        "(kind = 'SOURCE_MARKET' AND local_order_id IS NULL AND trade_id IS NULL "
        "AND action_id IS NULL AND role IS NULL) OR "
        "(kind IN ('FILL', 'ORDER_STATE') AND local_order_id IS NOT NULL "
        "AND action_id IS NOT NULL AND role IS NOT NULL AND role COLLATE \"C\" = 'GRID'))"
    )
    op.drop_constraint("uq_spider_runtime_delivery_full_callback", "spider_runtime_delivery", type_="unique")
    op.drop_constraint("ck_spider_runtime_delivery_full_callback_hash", "spider_runtime_delivery", type_="check")
    op.drop_constraint("ck_spider_runtime_delivery_decision_hash", "spider_runtime_delivery", type_="check")
    op.drop_constraint("ck_spider_runtime_delivery_decision_size", "spider_runtime_delivery", type_="check")
    op.drop_constraint("ck_spider_runtime_delivery_decision_pair", "spider_runtime_delivery", type_="check")
    op.drop_column("spider_runtime_delivery", "full_callback_key")
    op.drop_column("spider_runtime_delivery", "decision_sha256")
    op.drop_column("spider_runtime_delivery", "decision_payload")
    op.drop_constraint("ck_spider_runtime_action_operation", "spider_runtime_action", type_="check")
    op.drop_column("spider_runtime_action", "operation")
