"""Add durable S3 handoff, attempt and target-authority foundations."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision: str = "71d3a8c4e205"
down_revision: Union[str, Sequence[str], None] = "6d8a1e2f4c90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "rithmic_exit_attempt",
        sa.Column("attempt_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("account_id", sa.String(128), nullable=False),
        sa.Column("account_profile", sa.String(128), nullable=False),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("intent_key", sa.String(128), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("correlation", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("last_observation", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("owner_id", sa.String(128), nullable=True),
        sa.Column("fence", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("disposition_code", sa.String(64), nullable=True),
        sa.Column("disposition_detail", sa.Text(), nullable=True),
        sa.Column("next_action_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("parent_attempt_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("sender_process_instance_id", sa.String(128), nullable=True),
        sa.Column("dispatch_epoch", sa.BigInteger(), nullable=True),
        sa.Column("quiescence_evidence", postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("attempt_id", name="pk_rea"),
        sa.UniqueConstraint("intent_key", name="uq_rea_intent"),
        sa.UniqueConstraint("parent_attempt_id", name="uq_rea_parent"),
        sa.ForeignKeyConstraint(["parent_attempt_id"], ["rithmic_exit_attempt.attempt_id"], name="fk_rea_parent"),
        sa.CheckConstraint("state IN ('PREPARED','MAY_HAVE_SENT','ACCEPTED','FOLLOWUP_PENDING','VERIFYING','UNRESOLVED','SUPERSEDED_RESIDUAL','COMPLETED','VERIFIED_NOOP')", name="ck_rea_state"),
        sa.CheckConstraint("fence >= 0 AND version >= 0 AND (dispatch_epoch IS NULL OR dispatch_epoch >= 0)", name="ck_rea_versions"),
        sa.CheckConstraint("state NOT IN ('COMPLETED','VERIFIED_NOOP','SUPERSEDED_RESIDUAL') OR next_action_at IS NULL", name="ck_rea_terminal_action"),
    )
    op.create_index(
        "uq_rea_active_target", "rithmic_exit_attempt",
        ["account_id", "account_profile", "product_id"], unique=True,
        postgresql_where=sa.text("state NOT IN ('COMPLETED','VERIFIED_NOOP','SUPERSEDED_RESIDUAL')"),
    )
    op.create_index("ix_rea_due", "rithmic_exit_attempt", ["next_action_at", "updated_at"])
    op.create_table(
        "rithmic_exit_target_authority",
        sa.Column("account_id", sa.String(128), nullable=False),
        sa.Column("account_profile", sa.String(128), nullable=False),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("dispatch_epoch", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("process_instance_id", sa.String(128), nullable=True),
        sa.Column("current_attempt_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("current_fence", sa.BigInteger(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("account_id", "account_profile", "product_id", name="pk_reta"),
        sa.ForeignKeyConstraint(["current_attempt_id"], ["rithmic_exit_attempt.attempt_id"], name="fk_reta_attempt"),
        sa.CheckConstraint("dispatch_epoch >= 0 AND (current_fence IS NULL OR current_fence >= 0)", name="ck_reta_epoch_fence"),
    )
    op.create_table(
        "market_exit_handoff",
        sa.Column("handoff_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("environment", sa.String(64), nullable=False),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("timeframe", sa.String(32), nullable=False),
        sa.Column("timestamp", sa.BigInteger(), nullable=False),
        sa.Column("signal_key", sa.String(128), nullable=False),
        sa.Column("emission_ordinal", sa.Integer(), nullable=False),
        sa.Column("signal_payload", postgresql.JSONB(), nullable=False),
        sa.Column("signal_digest", sa.String(64), nullable=False),
        sa.Column("execution_scope_id", sa.String(128), nullable=True),
        sa.Column("strategy_id", sa.String(128), nullable=True),
        sa.Column("strategy_version", sa.String(128), nullable=True),
        sa.Column("config_hash", sa.String(64), nullable=True),
        sa.Column("account_id", sa.String(128), nullable=True),
        sa.Column("account_profile", sa.String(128), nullable=True),
        sa.Column("cause_generation", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(24), server_default=sa.text("'QUEUED'"), nullable=False),
        sa.Column("failure_detail", sa.Text(), nullable=True),
        sa.Column("attempt_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("handoff_id", name="pk_meh"),
        sa.ForeignKeyConstraint(["environment", "product_id", "timeframe", "timestamp"], ["market_data_application.environment", "market_data_application.product_id", "market_data_application.timeframe", "market_data_application.timestamp"], name="fk_meh_receipt", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["attempt_id"], ["rithmic_exit_attempt.attempt_id"], name="fk_meh_attempt", ondelete="RESTRICT"),
        sa.UniqueConstraint("environment", "product_id", "timeframe", "timestamp", "signal_key", name="uq_meh_signal"),
        sa.CheckConstraint("emission_ordinal >= 0", name="ck_meh_ordinal"),
        sa.CheckConstraint("signal_digest ~ '^[0-9a-f]{64}$'", name="ck_meh_digest"),
        sa.CheckConstraint("status IN ('QUEUED','LEASED','COMPLETED','VERIFIED_NOOP','UNRESOLVED')", name="ck_meh_status"),
        sa.CheckConstraint("(strategy_version IS NULL) = (strategy_id IS NULL)", name="ck_meh_strategy_pair"),
        sa.CheckConstraint("(account_id IS NULL) = (account_profile IS NULL)", name="ck_meh_account_pair"),
    )
    op.create_index("ix_meh_pending", "market_exit_handoff", ["status", "created_at"])
    op.execute("""
        CREATE FUNCTION reject_s3_handoff_identity_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.handoff_id, NEW.environment, NEW.product_id, NEW.timeframe, NEW.timestamp,
                NEW.signal_key, NEW.emission_ordinal, NEW.signal_payload, NEW.signal_digest,
                NEW.execution_scope_id, NEW.strategy_id, NEW.strategy_version, NEW.config_hash,
                NEW.account_id, NEW.account_profile, NEW.cause_generation)
               IS DISTINCT FROM
               (OLD.handoff_id, OLD.environment, OLD.product_id, OLD.timeframe, OLD.timestamp,
                OLD.signal_key, OLD.emission_ordinal, OLD.signal_payload, OLD.signal_digest,
                OLD.execution_scope_id, OLD.strategy_id, OLD.strategy_version, OLD.config_hash,
                OLD.account_id, OLD.account_profile, OLD.cause_generation) THEN
                RAISE EXCEPTION 'market_exit_handoff identity and signal payload are immutable';
            END IF;
            RETURN NEW;
        END $$
    """)
    op.execute("CREATE TRIGGER trg_meh_identity_immutable BEFORE UPDATE ON market_exit_handoff FOR EACH ROW EXECUTE FUNCTION reject_s3_handoff_identity_update()")


def downgrade() -> None:
    op.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM market_exit_handoff) OR EXISTS (SELECT 1 FROM rithmic_exit_attempt) OR EXISTS (SELECT 1 FROM rithmic_exit_target_authority) THEN RAISE EXCEPTION 'Cannot downgrade while S3 exit persistence rows exist'; END IF; END $$")
    op.drop_index("ix_meh_pending", table_name="market_exit_handoff")
    op.drop_table("market_exit_handoff")
    op.execute("DROP FUNCTION reject_s3_handoff_identity_update()")
    op.drop_table("rithmic_exit_target_authority")
    op.drop_index("ix_rea_due", table_name="rithmic_exit_attempt")
    op.drop_index("uq_rea_active_target", table_name="rithmic_exit_attempt")
    op.drop_table("rithmic_exit_attempt")
