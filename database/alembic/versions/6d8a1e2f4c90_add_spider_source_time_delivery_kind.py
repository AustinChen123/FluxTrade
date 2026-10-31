"""Allow storage-only SOURCE_TIME observations in Spider deliveries."""

from alembic import op

revision = "6d8a1e2f4c90"
down_revision = "4c2f8a91d5e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind")
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT ck_spider_runtime_delivery_kind "
        "CHECK (kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET', 'TRANSPORT_RESULT', 'SOURCE_TIME'))"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind_references"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT ck_spider_runtime_delivery_kind_references CHECK ("
        "(kind = 'SOURCE_MARKET' AND local_order_id IS NULL AND trade_id IS NULL AND action_id IS NULL AND role IS NULL) OR "
        "(kind = 'TRANSPORT_RESULT' AND local_order_id IS NULL AND trade_id IS NULL AND action_id IS NULL AND role IS NULL) OR "
        "(kind = 'SOURCE_TIME' AND local_order_id IS NULL AND trade_id IS NULL AND action_id IS NULL AND role IS NULL) OR "
        "(kind IN ('FILL', 'ORDER_STATE') AND local_order_id IS NOT NULL AND action_id IS NOT NULL "
        "AND role IS NOT NULL AND role COLLATE \"C\" = 'GRID')"
        ")"
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM spider_runtime_delivery WHERE kind = 'SOURCE_TIME') "
        "THEN RAISE EXCEPTION 'Cannot downgrade while Spider SOURCE_TIME deliveries exist'; "
        "END IF; END $$"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind_references"
    )
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT ck_spider_runtime_delivery_kind_references CHECK ("
        "(kind = 'SOURCE_MARKET' AND local_order_id IS NULL AND trade_id IS NULL AND action_id IS NULL AND role IS NULL) OR "
        "(kind = 'TRANSPORT_RESULT' AND local_order_id IS NULL AND trade_id IS NULL AND action_id IS NULL AND role IS NULL) OR "
        "(kind IN ('FILL', 'ORDER_STATE') AND local_order_id IS NOT NULL AND action_id IS NOT NULL "
        "AND role IS NOT NULL AND role COLLATE \"C\" = 'GRID')"
        ")"
    )
    op.execute("ALTER TABLE spider_runtime_delivery DROP CONSTRAINT ck_spider_runtime_delivery_kind")
    op.execute(
        "ALTER TABLE spider_runtime_delivery ADD CONSTRAINT ck_spider_runtime_delivery_kind "
        "CHECK (kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET', 'TRANSPORT_RESULT'))"
    )
