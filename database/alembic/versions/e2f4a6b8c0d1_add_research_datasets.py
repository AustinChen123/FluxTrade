"""add_research_datasets

Revision ID: e2f4a6b8c0d1
Revises: 7a3c9e1b5d2f
Create Date: 2026-07-26 00:00:00.000000

Store immutable, versioned research candles separately from the canonical
live candlestick table.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "e2f4a6b8c0d1"
down_revision: Union[str, Sequence[str], None] = "7a3c9e1b5d2f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "research_dataset",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("product_id", sa.String(), nullable=False),
        sa.Column("timeframe", sa.String(length=32), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("revision", sa.String(length=128), nullable=False),
        sa.Column("timestamp_format", sa.String(length=32), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("roll_policy", sa.String(length=128), nullable=True),
        sa.Column("start_time", sa.BigInteger(), nullable=False),
        sa.Column("end_time", sa.BigInteger(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        sa.Column("quality_status", sa.String(length=32), nullable=False),
        sa.Column("lifecycle_state", sa.String(length=32), nullable=False),
        sa.Column("sealed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "quality_status IN ('validated')",
            name="chk_research_dataset_quality_status",
        ),
        sa.CheckConstraint(
            "lifecycle_state IN ('importing', 'sealed')",
            name="chk_research_dataset_lifecycle_state",
        ),
        sa.CheckConstraint(
            "(lifecycle_state = 'importing' AND sealed_at IS NULL) OR "
            "(lifecycle_state = 'sealed' AND sealed_at IS NOT NULL)",
            name="chk_research_dataset_seal_consistency",
        ),
        sa.CheckConstraint(
            "row_count > 0",
            name="chk_research_dataset_nonempty",
        ),
        sa.CheckConstraint(
            "start_time <= end_time",
            name="chk_research_dataset_time_range",
        ),
        sa.ForeignKeyConstraint(["product_id"], ["product.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "research_candlestick",
        sa.Column("dataset_id", sa.String(length=128), nullable=False),
        sa.Column("timestamp", sa.BigInteger(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.Column("volume", sa.Numeric(), nullable=False),
        sa.Column("source_contract", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["research_dataset.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("dataset_id", "timestamp"),
    )
    op.execute(
        """
        CREATE FUNCTION reject_sealed_research_dataset_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            actual_row_count BIGINT;
            actual_start_time BIGINT;
            actual_end_time BIGINT;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.lifecycle_state <> 'importing' OR NEW.sealed_at IS NOT NULL THEN
                    RAISE EXCEPTION
                        'research dataset must be created in importing state: %',
                        NEW.id
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.lifecycle_state = 'sealed' THEN
                RAISE EXCEPTION 'sealed research dataset is immutable: %', OLD.id
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF NEW.lifecycle_state = 'sealed' THEN
                SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
                INTO actual_row_count, actual_start_time, actual_end_time
                FROM research_candlestick
                WHERE dataset_id = NEW.id;

                IF actual_row_count <> NEW.row_count
                   OR actual_start_time IS DISTINCT FROM NEW.start_time
                   OR actual_end_time IS DISTINCT FROM NEW.end_time THEN
                    RAISE EXCEPTION
                        'research dataset seal summary does not match candles: %',
                        NEW.id
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_research_dataset_immutable
        BEFORE INSERT OR UPDATE OR DELETE ON research_dataset
        FOR EACH ROW
        EXECUTE FUNCTION reject_sealed_research_dataset_mutation()
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_sealed_research_candlestick_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                PERFORM 1
                FROM research_dataset
                WHERE id = NEW.dataset_id
                FOR UPDATE;
            ELSIF TG_OP = 'DELETE' THEN
                PERFORM 1
                FROM research_dataset
                WHERE id = OLD.dataset_id
                FOR UPDATE;
            ELSE
                PERFORM 1
                FROM research_dataset
                WHERE id IN (OLD.dataset_id, NEW.dataset_id)
                ORDER BY id
                FOR UPDATE;
            END IF;

            IF TG_OP IN ('UPDATE', 'DELETE') THEN
                IF EXISTS (
                    SELECT 1
                    FROM research_dataset
                    WHERE id = OLD.dataset_id
                      AND lifecycle_state = 'sealed'
                ) THEN
                    RAISE EXCEPTION
                        'sealed research dataset candles are immutable: %',
                        OLD.dataset_id
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            IF TG_OP IN ('INSERT', 'UPDATE') THEN
                IF EXISTS (
                    SELECT 1
                    FROM research_dataset
                    WHERE id = NEW.dataset_id
                      AND lifecycle_state = 'sealed'
                ) THEN
                    RAISE EXCEPTION
                        'sealed research dataset candles are immutable: %',
                        NEW.dataset_id
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_research_candlestick_immutable
        BEFORE INSERT OR UPDATE OR DELETE ON research_candlestick
        FOR EACH ROW
        EXECUTE FUNCTION reject_sealed_research_candlestick_mutation()
        """
    )
    op.execute(
        """
        CREATE FUNCTION reject_research_table_truncate()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'research dataset tables cannot be truncated'
                USING ERRCODE = '55000';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_research_dataset_no_truncate
        BEFORE TRUNCATE ON research_dataset
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_research_table_truncate()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_research_candlestick_no_truncate
        BEFORE TRUNCATE ON research_candlestick
        FOR EACH STATEMENT
        EXECUTE FUNCTION reject_research_table_truncate()
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_research_candlestick_no_truncate "
        "ON research_candlestick"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_research_dataset_no_truncate "
        "ON research_dataset"
    )
    op.execute("DROP FUNCTION IF EXISTS reject_research_table_truncate()")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_research_candlestick_immutable "
        "ON research_candlestick"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS reject_sealed_research_candlestick_mutation()"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_research_dataset_immutable "
        "ON research_dataset"
    )
    op.execute(
        "DROP FUNCTION IF EXISTS reject_sealed_research_dataset_mutation()"
    )
    op.drop_table("research_candlestick")
    op.drop_table("research_dataset")
