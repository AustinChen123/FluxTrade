"""ORM metadata for immutable Spider configuration request evidence."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from src.core.orm_models import Base


class SpiderConfigurationRequest(Base):
    __tablename__ = "spider_configuration_request"

    request_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(
        String(128, collation="C"), nullable=False
    )
    strategy_id: Mapped[str] = mapped_column(
        String(128, collation="C"), ForeignKey("strategy.id"), nullable=False
    )
    request_kind: Mapped[str] = mapped_column(String(16, collation="C"), nullable=False)
    actor: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(
        String(128, collation="C"), nullable=False
    )
    expected_state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    configuration_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(
        String(64, collation="C"), nullable=False
    )
    proposal_request_id: Mapped[str | None] = mapped_column(
        String(64, collation="C"), nullable=True
    )
    recorded_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("date_trunc('milliseconds', clock_timestamp())"),
    )

    __table_args__ = (
        UniqueConstraint(
            "request_id",
            "environment",
            "execution_scope_id",
            "strategy_id",
            name="uq_spider_config_request_scope_ref",
        ),
        ForeignKeyConstraint(
            ["proposal_request_id", "environment", "execution_scope_id", "strategy_id"],
            [
                "spider_configuration_request.request_id",
                "spider_configuration_request.environment",
                "spider_configuration_request.execution_scope_id",
                "spider_configuration_request.strategy_id",
            ],
            name="fk_spider_config_request_proposal_scope",
        ),
        CheckConstraint(
            "request_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'",
            name="ck_spider_config_request_id",
        ),
        CheckConstraint(
            "environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'",
            name="ck_spider_config_environment",
        ),
        CheckConstraint(
            "execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'",
            name="ck_spider_config_execution_scope",
        ),
        CheckConstraint(
            "strategy_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'",
            name="ck_spider_config_strategy_id",
        ),
        CheckConstraint(
            "request_kind IN ('PROPOSAL', 'CONFIRMATION')",
            name="ck_spider_config_request_kind",
        ),
        CheckConstraint(
            "actor COLLATE \"C\" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$'",
            name="ck_spider_config_actor",
        ),
        CheckConstraint(
            "idempotency_key COLLATE \"C\" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'",
            name="ck_spider_config_idempotency_key",
        ),
        CheckConstraint(
            "expected_state_version BETWEEN 0 AND 2147483647",
            name="ck_spider_config_state_version",
        ),
        CheckConstraint(
            "octet_length(configuration_payload) BETWEEN 1 AND 65536",
            name="ck_spider_config_payload",
        ),
        CheckConstraint(
            "configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'",
            name="ck_spider_config_digest",
        ),
        CheckConstraint(
            "(request_kind = 'PROPOSAL' AND proposal_request_id IS NULL) OR "
            "(request_kind = 'CONFIRMATION' AND proposal_request_id IS NOT NULL)",
            name="ck_spider_config_proposal_reference",
        ),
        CheckConstraint(
            "recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND "
            "recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND "
            "recorded_at = date_trunc('milliseconds', recorded_at)",
            name="ck_spider_config_recorded_at",
        ),
    )


class SpiderRuntimeOperation(Base):
    __tablename__ = "spider_runtime_operation"

    operation_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), ForeignKey("strategy.id"), nullable=False)
    actor: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    operation_key: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    operation_core_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    operation_core_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    configuration_request_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    phase: Mapped[str] = mapped_column(String(16), nullable=False, server_default="PREVIEW")
    authorization_revision: Mapped[int | None] = mapped_column(Integer, nullable=True)
    confirmation_request_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    recorded_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=text("date_trunc('milliseconds', clock_timestamp())"),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["configuration_request_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_configuration_request.request_id", "spider_configuration_request.environment",
             "spider_configuration_request.execution_scope_id", "spider_configuration_request.strategy_id"],
            name="fk_spider_runtime_operation_configuration_scope",
        ),
        UniqueConstraint(
            "operation_id", "environment", "execution_scope_id", "strategy_id",
            name="uq_spider_runtime_operation_scope_ref",
        ),
        CheckConstraint("operation_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_operation_id"),
        CheckConstraint("operation_core_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_operation_core_hash"),
        CheckConstraint("octet_length(operation_core_payload) BETWEEN 1 AND 1048576", name="ck_spider_runtime_operation_core_payload"),
        CheckConstraint(
            "phase IN ('PREVIEW', 'DRAIN', 'POSITION', 'RECONCILE', 'COMMIT', "
            "'BUILD', 'RUN', 'INCOMPLETE', 'STOP')",
            name="ck_spider_runtime_operation_phase",
        ),
        CheckConstraint("authorization_revision IS NULL OR authorization_revision BETWEEN 1 AND 2147483647", name="ck_spider_runtime_operation_revision"),
        CheckConstraint("confirmation_request_id IS NULL OR confirmation_request_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_operation_confirmation"),
        CheckConstraint("recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_spider_runtime_operation_recorded_at"),
    )


class SpiderOperationRequestRow(Base):
    __tablename__ = "spider_operation_request"

    request_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    request_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    operation_key: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    operation_core_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    authorization_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    proposal_request_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    authorization_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    request_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=text("date_trunc('milliseconds', clock_timestamp())"),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_operation.operation_id", "spider_runtime_operation.environment",
             "spider_runtime_operation.execution_scope_id", "spider_runtime_operation.strategy_id"],
            name="fk_spider_operation_request_operation_scope",
        ),
        ForeignKeyConstraint(
            ["proposal_request_id", "operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_operation_request.request_id", "spider_operation_request.operation_id",
             "spider_operation_request.environment", "spider_operation_request.execution_scope_id",
             "spider_operation_request.strategy_id"],
            name="fk_spider_operation_request_proposal_scope",
        ),
        UniqueConstraint(
            "operation_id", "environment", "execution_scope_id", "strategy_id", "actor", "idempotency_key",
            name="uq_spider_operation_request_idempotency",
        ),
        UniqueConstraint(
            "request_id", "operation_id", "environment", "execution_scope_id", "strategy_id",
            name="uq_spider_operation_request_scope_ref",
        ),
        CheckConstraint("request_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_operation_request_id"),
        CheckConstraint("operation_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_operation_request_operation_id"),
        CheckConstraint("operation_core_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_operation_request_core_hash"),
        CheckConstraint("request_kind IN ('PROPOSAL', 'CONFIRMATION')", name="ck_spider_operation_request_kind"),
        CheckConstraint("authorization_revision BETWEEN 1 AND 2147483647", name="ck_spider_operation_request_revision"),
        CheckConstraint("octet_length(request_payload) BETWEEN 1 AND 1048576", name="ck_spider_operation_request_payload"),
        CheckConstraint("authorization_confirmed = (request_kind = 'CONFIRMATION')", name="ck_spider_operation_request_confirmed"),
        CheckConstraint("recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_spider_operation_request_recorded_at"),
    )


class SpiderRuntimeScope(Base):
    """Authoritative, exactly scoped installed Spider runtime projection."""

    __tablename__ = "spider_runtime_scope"

    environment: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), ForeignKey("strategy.id"), primary_key=True)
    host_contract_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    host_contract_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    confirmation_request_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    control_mode: Mapped[str] = mapped_column(String(8, collation="C"), nullable=False)
    current_operation_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    last_received_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    last_consumed_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    last_tape_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    source_checkpoint: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    source_checkpoint_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    control_barrier_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    initial_plan_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["confirmation_request_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_configuration_request.request_id", "spider_configuration_request.environment",
             "spider_configuration_request.execution_scope_id", "spider_configuration_request.strategy_id"],
            name="fk_spider_runtime_scope_confirmation",
        ),
        ForeignKeyConstraint(
            ["current_operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_operation.operation_id", "spider_runtime_operation.environment",
             "spider_runtime_operation.execution_scope_id", "spider_runtime_operation.strategy_id"],
            name="fk_spider_runtime_scope_operation",
        ),
        ForeignKeyConstraint(
            ["initial_plan_id", "current_operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_plan.plan_id", "spider_runtime_plan.operation_id",
             "spider_runtime_plan.environment", "spider_runtime_plan.execution_scope_id",
             "spider_runtime_plan.strategy_id"],
            name="fk_spider_runtime_scope_initial_plan",
        ),
        CheckConstraint("host_contract_id = 'SPIDER_DURABLE_HOST_V1'", name="ck_spider_runtime_scope_host"),
        CheckConstraint("host_contract_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_scope_host_hash"),
        CheckConstraint("confirmation_request_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_scope_confirmation_id"),
        CheckConstraint("configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_scope_configuration_hash"),
        CheckConstraint("generation BETWEEN 1 AND 9223372036854775807", name="ck_spider_runtime_scope_generation"),
        CheckConstraint("state_version BETWEEN 0 AND 2147483647", name="ck_spider_runtime_scope_state_version"),
        CheckConstraint("control_mode IN ('RUN', 'STOP', 'DRAIN')", name="ck_spider_runtime_scope_control"),
        CheckConstraint("current_operation_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_scope_operation_id"),
        CheckConstraint("last_received_sequence >= 0 AND last_consumed_sequence BETWEEN 0 AND last_received_sequence", name="ck_spider_runtime_scope_sequences"),
        CheckConstraint("last_tape_sequence >= 0", name="ck_spider_runtime_scope_tape_sequence"),
        CheckConstraint("octet_length(source_checkpoint) BETWEEN 1 AND 1048576", name="ck_spider_runtime_scope_checkpoint"),
        CheckConstraint("source_checkpoint_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_scope_checkpoint_hash"),
        CheckConstraint("control_barrier_sequence >= 0", name="ck_spider_runtime_scope_barrier"),
        CheckConstraint("initial_plan_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_scope_plan_id"),
    )


class SpiderRuntimePlan(Base):
    """Immutable initial candidate/action evidence for one installed generation."""

    __tablename__ = "spider_runtime_plan"

    plan_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    operation_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    source_checkpoint_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    original_build_checkpoint: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    original_build_checkpoint_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    specification_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    observed_input_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    tape_cursor: Mapped[str] = mapped_column(String(256, collation="C"), nullable=False)
    manifest_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    plan_status: Mapped[str] = mapped_column(String(12, collation="C"), nullable=False)
    missing_members: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_operation.operation_id", "spider_runtime_operation.environment",
             "spider_runtime_operation.execution_scope_id", "spider_runtime_operation.strategy_id"],
            name="fk_spider_runtime_plan_operation",
        ),
        UniqueConstraint("plan_id", "environment", "execution_scope_id", "strategy_id", name="uq_spider_runtime_plan_scope_ref"),
        UniqueConstraint("plan_id", "operation_id", "environment", "execution_scope_id", "strategy_id", name="uq_spider_runtime_plan_operation_ref"),
        CheckConstraint("plan_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_id"),
        CheckConstraint("operation_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_operation_id"),
        CheckConstraint("generation > 0", name="ck_spider_runtime_plan_generation"),
        CheckConstraint("configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_configuration_hash"),
        CheckConstraint("source_checkpoint_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_source_hash"),
        CheckConstraint("octet_length(original_build_checkpoint) BETWEEN 1 AND 1048576", name="ck_spider_runtime_plan_build_checkpoint"),
        CheckConstraint("original_build_checkpoint_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_build_checkpoint_hash"),
        CheckConstraint("specification_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_specification_hash"),
        CheckConstraint("observed_input_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_observed_hash"),
        CheckConstraint("octet_length(manifest_payload) BETWEEN 1 AND 1048576", name="ck_spider_runtime_plan_manifest"),
        CheckConstraint("manifest_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_plan_manifest_hash"),
        CheckConstraint("plan_status IN ('INCOMPLETE', 'COMPLETE')", name="ck_spider_runtime_plan_status"),
        CheckConstraint("octet_length(missing_members) BETWEEN 1 AND 1048576", name="ck_spider_runtime_plan_missing_members"),
    )


class SpiderRuntimeAction(Base):
    """One immutable admitted action in a source-ordered Spider plan."""

    __tablename__ = "spider_runtime_action"

    action_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    plan_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    trigger_delivery_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    parent_action_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    operation: Mapped[str] = mapped_column(String(8, collation="C"), nullable=False, server_default="PLACE")
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    role: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    local_order_id: Mapped[str] = mapped_column(String(36, collation="C"), nullable=False)
    original_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    candidate_ordinal: Mapped[int] = mapped_column(BigInteger, nullable=False)
    transmission_group: Mapped[int] = mapped_column(BigInteger, nullable=False)
    transmission_ordinal: Mapped[int] = mapped_column(BigInteger, nullable=False)
    outcome_state: Mapped[str] = mapped_column(String(16, collation="C"), nullable=False)
    outcome_reference: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["plan_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_plan.plan_id", "spider_runtime_plan.environment",
             "spider_runtime_plan.execution_scope_id", "spider_runtime_plan.strategy_id"],
            name="fk_spider_runtime_action_plan",
        ),
        ForeignKeyConstraint(
            ["parent_action_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_action.action_id", "spider_runtime_action.environment",
             "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
            name="fk_spider_runtime_action_parent",
        ),
        ForeignKeyConstraint(["local_order_id"], ["order.id"], name="fk_spider_runtime_action_order"),
        UniqueConstraint("action_id", "environment", "execution_scope_id", "strategy_id", name="uq_spider_runtime_action_scope_ref"),
        UniqueConstraint("plan_id", "candidate_ordinal", name="uq_spider_runtime_action_candidate"),
        CheckConstraint("action_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_action_id"),
        CheckConstraint("trigger_delivery_id IS NULL OR trigger_delivery_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_action_trigger"),
        CheckConstraint("generation > 0", name="ck_spider_runtime_action_generation"),
        CheckConstraint("configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_action_configuration_hash"),
        CheckConstraint("role COLLATE \"C\" = 'GRID'", name="ck_spider_runtime_action_role"),
        CheckConstraint("operation IN ('PLACE', 'CANCEL')", name="ck_spider_runtime_action_operation"),
        CheckConstraint("octet_length(original_payload) BETWEEN 1 AND 1048576", name="ck_spider_runtime_action_payload"),
        CheckConstraint("payload_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_action_payload_hash"),
        CheckConstraint("candidate_ordinal >= 0 AND transmission_group >= 0 AND transmission_ordinal >= 0", name="ck_spider_runtime_action_ordinals"),
        CheckConstraint("outcome_state IN ('UNSENT', 'SEND_UNKNOWN', 'ACCEPTED', 'REJECTED', 'TERMINAL')", name="ck_spider_runtime_action_outcome"),
        CheckConstraint("outcome_reference IS NULL OR outcome_reference COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_action_outcome_ref"),
    )


class SpiderRuntimeBinding(Base):
    """Exact source/canonical dual identity for an original local Order."""

    __tablename__ = "spider_runtime_binding"

    action_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    local_order_id: Mapped[str] = mapped_column(String(36, collation="C"), nullable=False)
    source_product_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    canonical_product_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    source_client_order_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    product_map_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["action_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_action.action_id", "spider_runtime_action.environment",
             "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
            name="fk_spider_runtime_binding_action",
        ),
        ForeignKeyConstraint(["local_order_id"], ["order.id"], name="fk_spider_runtime_binding_order"),
        UniqueConstraint("local_order_id", name="uq_spider_runtime_binding_order"),
        CheckConstraint("action_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_binding_action_id"),
        CheckConstraint("octet_length(source_client_order_id) BETWEEN 1 AND 64", name="ck_spider_runtime_binding_source_client_id"),
        CheckConstraint("product_map_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_binding_map_hash"),
    )


class SpiderRuntimeDelivery(Base):
    """Durable scoped source or financial receipt in the shared runtime tape."""

    __tablename__ = "spider_runtime_delivery"

    environment: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    delivery_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    kind: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    local_order_id: Mapped[str | None] = mapped_column(String(36, collation="C"), nullable=True)
    trade_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    action_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    role: Mapped[str | None] = mapped_column(String(32, collation="C"), nullable=True)
    source_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    visible_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    elapsed_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    disposition: Mapped[str] = mapped_column(String(12, collation="C"), nullable=False, server_default="PENDING")
    suppression_reason: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    decision_payload: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    decision_sha256: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    full_callback_key: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_scope.environment", "spider_runtime_scope.execution_scope_id", "spider_runtime_scope.strategy_id"],
            name="fk_spider_runtime_delivery_scope",
        ),
        ForeignKeyConstraint(
            ["action_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_action.action_id", "spider_runtime_action.environment",
             "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
            name="fk_spider_runtime_delivery_action",
        ),
        ForeignKeyConstraint(["local_order_id"], ["order.id"], name="fk_spider_runtime_delivery_order"),
        ForeignKeyConstraint(["trade_id"], ["trade.id"], name="fk_spider_runtime_delivery_trade"),
        UniqueConstraint("delivery_id", "environment", "execution_scope_id", "strategy_id", name="uq_spider_runtime_delivery_identity"),
        CheckConstraint("sequence > 0", name="ck_spider_runtime_delivery_sequence"),
        CheckConstraint("delivery_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_delivery_id"),
        CheckConstraint("content_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_delivery_hash"),
        CheckConstraint("generation > 0", name="ck_spider_runtime_delivery_generation"),
        CheckConstraint("configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_delivery_configuration_hash"),
        UniqueConstraint("environment", "execution_scope_id", "strategy_id", "full_callback_key", name="uq_spider_runtime_delivery_full_callback"),
        CheckConstraint("kind IN ('FILL', 'ORDER_STATE', 'SOURCE_MARKET', 'TRANSPORT_RESULT', 'SOURCE_TIME')", name="ck_spider_runtime_delivery_kind"),
        CheckConstraint(
            "(kind = 'SOURCE_MARKET' AND local_order_id IS NULL AND trade_id IS NULL "
            "AND action_id IS NULL AND role IS NULL) OR "
            "(kind = 'TRANSPORT_RESULT' AND local_order_id IS NULL AND trade_id IS NULL "
            "AND action_id IS NULL AND role IS NULL) OR "
            "(kind = 'SOURCE_TIME' AND local_order_id IS NULL AND trade_id IS NULL "
            "AND action_id IS NULL AND role IS NULL) OR "
            "(kind IN ('FILL', 'ORDER_STATE') AND local_order_id IS NOT NULL "
            "AND action_id IS NOT NULL AND role IS NOT NULL "
            "AND role COLLATE \"C\" = 'GRID')",
            name="ck_spider_runtime_delivery_kind_references",
        ),
        CheckConstraint("octet_length(source_payload) BETWEEN 1 AND 1048576", name="ck_spider_runtime_delivery_payload"),
        CheckConstraint("visible_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND visible_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND visible_at = date_trunc('milliseconds', visible_at)", name="ck_spider_runtime_delivery_visible_at"),
        CheckConstraint("elapsed_ms IS NULL OR elapsed_ms >= 0", name="ck_spider_runtime_delivery_elapsed"),
        CheckConstraint("disposition IN ('PENDING', 'CONSUMED', 'SUPPRESSED')", name="ck_spider_runtime_delivery_disposition"),
        CheckConstraint("suppression_reason IS NULL OR disposition = 'SUPPRESSED'", name="ck_spider_runtime_delivery_suppression"),
        CheckConstraint("(decision_payload IS NULL) = (decision_sha256 IS NULL)", name="ck_spider_runtime_delivery_decision_pair"),
        CheckConstraint("decision_payload IS NULL OR octet_length(decision_payload) BETWEEN 1 AND 1048576", name="ck_spider_runtime_delivery_decision_size"),
        CheckConstraint("decision_sha256 IS NULL OR decision_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_delivery_decision_hash"),
        CheckConstraint("full_callback_key IS NULL OR full_callback_key COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_runtime_delivery_full_callback_hash"),
    )


class SpiderRuntimeTransmission(Base):
    """Immutable request marker and once-recorded raw response for one group."""

    __tablename__ = "spider_runtime_transmission"

    transmission_group_id: Mapped[str] = mapped_column(
        String(64, collation="C"), primary_key=True
    )
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(
        String(128, collation="C"), nullable=False
    )
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    operation_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    plan_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(
        String(64, collation="C"), nullable=False
    )
    operation: Mapped[str] = mapped_column(String(8, collation="C"), nullable=False)
    request_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    response_payload: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    response_sha256: Mapped[str | None] = mapped_column(
        String(64, collation="C"), nullable=True
    )
    response_failure: Mapped[str | None] = mapped_column(
        String(32, collation="C"), nullable=True
    )
    recorded_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    response_recorded_at: Mapped[DateTime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_operation.operation_id", "spider_runtime_operation.environment",
             "spider_runtime_operation.execution_scope_id", "spider_runtime_operation.strategy_id"],
            name="fk_spider_runtime_transmission_operation",
        ),
        ForeignKeyConstraint(
            ["plan_id", "operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_plan.plan_id", "spider_runtime_plan.operation_id",
             "spider_runtime_plan.environment", "spider_runtime_plan.execution_scope_id",
             "spider_runtime_plan.strategy_id"],
            name="fk_spider_runtime_transmission_plan",
        ),
        UniqueConstraint(
            "transmission_group_id", "environment", "execution_scope_id", "strategy_id",
            name="uq_spider_runtime_transmission_scope_ref",
        ),
        CheckConstraint(
            "transmission_group_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'",
            name="ck_spider_runtime_transmission_id",
        ),
        CheckConstraint(
            "generation > 0 AND configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'",
            name="ck_spider_runtime_transmission_generation_config",
        ),
        CheckConstraint(
            "operation IN ('PLACE', 'CANCEL')",
            name="ck_spider_runtime_transmission_operation",
        ),
        CheckConstraint(
            "octet_length(request_payload) BETWEEN 1 AND 1048576 AND "
            "request_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'",
            name="ck_spider_runtime_transmission_request",
        ),
        CheckConstraint(
            "(response_payload IS NULL) = (response_sha256 IS NULL) AND "
            "(response_payload IS NULL OR octet_length(response_payload) BETWEEN 1 AND 1048576) AND "
            "(response_sha256 IS NULL OR response_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$') AND "
            "(response_failure IS NULL OR response_failure IN "
            "('TRANSPORT_UNKNOWN', 'RESPONSE_INVALID', 'RESPONSE_TOO_LARGE')) AND "
            "NOT (response_payload IS NOT NULL AND response_failure IS NOT NULL) AND "
            "((response_payload IS NULL AND response_failure IS NULL AND response_recorded_at IS NULL) OR "
            "(response_recorded_at IS NOT NULL AND "
            "(response_payload IS NOT NULL OR response_failure IS NOT NULL)))",
            name="ck_spider_runtime_transmission_response",
        ),
        CheckConstraint(
            "recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND "
            "recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND "
            "recorded_at = date_trunc('milliseconds', recorded_at) AND "
            "(response_recorded_at IS NULL OR "
            "(response_recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND "
            "response_recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND "
            "response_recorded_at = date_trunc('milliseconds', response_recorded_at)))",
            name="ck_spider_runtime_transmission_timestamps",
        ),
    )


class SpiderRuntimeTapeIndex(Base):
    """Shared ordering index for financial deliveries and outbound markers."""

    __tablename__ = "spider_runtime_tape_index"

    environment: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    execution_scope_id: Mapped[str] = mapped_column(
        String(128, collation="C"), primary_key=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True)
    ordinal: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    kind: Mapped[str] = mapped_column(String(16, collation="C"), nullable=False)
    delivery_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    transmission_group_id: Mapped[str | None] = mapped_column(
        String(64, collation="C"), nullable=True
    )
    member_resubmission_request_id: Mapped[str | None] = mapped_column(
        String(64, collation="C"), nullable=True
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_scope.environment", "spider_runtime_scope.execution_scope_id",
             "spider_runtime_scope.strategy_id"],
            name="fk_spider_runtime_tape_scope",
        ),
        ForeignKeyConstraint(
            ["environment", "execution_scope_id", "strategy_id", "delivery_sequence"],
            ["spider_runtime_delivery.environment", "spider_runtime_delivery.execution_scope_id",
             "spider_runtime_delivery.strategy_id", "spider_runtime_delivery.sequence"],
            name="fk_spider_runtime_tape_delivery",
        ),
        ForeignKeyConstraint(
            ["transmission_group_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_transmission.transmission_group_id",
             "spider_runtime_transmission.environment",
             "spider_runtime_transmission.execution_scope_id",
             "spider_runtime_transmission.strategy_id"],
            name="fk_spider_runtime_tape_transmission",
        ),
        ForeignKeyConstraint(
            ["member_resubmission_request_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_member_resubmission_request.request_id", "spider_member_resubmission_request.environment",
             "spider_member_resubmission_request.execution_scope_id", "spider_member_resubmission_request.strategy_id"],
            name="fk_spider_runtime_tape_member_resubmission",
        ),
        UniqueConstraint(
            "environment", "execution_scope_id", "strategy_id", "delivery_sequence",
            name="uq_spider_runtime_tape_delivery",
        ),
        UniqueConstraint(
            "environment", "execution_scope_id", "strategy_id", "transmission_group_id",
            name="uq_spider_runtime_tape_transmission",
        ),
        UniqueConstraint(
            "environment", "execution_scope_id", "strategy_id", "member_resubmission_request_id",
            name="uq_spider_runtime_tape_member_resubmission",
        ),
        CheckConstraint("ordinal > 0", name="ck_spider_runtime_tape_ordinal"),
        CheckConstraint(
            "(kind = 'DELIVERY' AND delivery_sequence IS NOT NULL AND "
            "transmission_group_id IS NULL AND member_resubmission_request_id IS NULL) OR "
            "(kind = 'SEND_MARKER' AND delivery_sequence IS NULL AND "
            "transmission_group_id IS NOT NULL AND member_resubmission_request_id IS NULL) OR "
            "(kind = 'RESUBMIT' AND delivery_sequence IS NULL AND "
            "transmission_group_id IS NULL AND member_resubmission_request_id IS NOT NULL)",
            name="ck_spider_runtime_tape_reference",
        ),
    )


class SpiderControlReceipt(Base):
    """Immutable proof of a scoped, nonfinancial Spider STOP command."""

    __tablename__ = "spider_control_receipt"

    receipt_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    actor: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    request_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    operation_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    before_state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    after_state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    barrier_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    outcome: Mapped[str] = mapped_column(String(12, collation="C"), nullable=False)
    recorded_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("date_trunc('milliseconds', clock_timestamp())"),
    )

    __table_args__ = (
        UniqueConstraint(
            "environment", "execution_scope_id", "strategy_id", "actor", "idempotency_key",
            name="uq_spider_control_receipt_idempotency",
        ),
        ForeignKeyConstraint(
            ["environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_scope.environment", "spider_runtime_scope.execution_scope_id", "spider_runtime_scope.strategy_id"],
            name="fk_spider_control_receipt_scope",
        ),
        ForeignKeyConstraint(
            ["operation_id", "environment", "execution_scope_id", "strategy_id"],
            ["spider_runtime_operation.operation_id", "spider_runtime_operation.environment", "spider_runtime_operation.execution_scope_id", "spider_runtime_operation.strategy_id"],
            name="fk_spider_control_receipt_operation",
        ),
        CheckConstraint("receipt_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_control_receipt_id"),
        CheckConstraint("environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_control_receipt_environment"),
        CheckConstraint("execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_control_receipt_scope"),
        CheckConstraint("strategy_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_control_receipt_strategy"),
        CheckConstraint("actor COLLATE \"C\" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$'", name="ck_spider_control_receipt_actor"),
        CheckConstraint("idempotency_key COLLATE \"C\" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'", name="ck_spider_control_receipt_key"),
        CheckConstraint("octet_length(request_payload) BETWEEN 1 AND 1048576", name="ck_spider_control_receipt_payload"),
        CheckConstraint("request_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_control_receipt_hash"),
        CheckConstraint("operation_id IS NULL OR operation_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_control_receipt_operation_id"),
        CheckConstraint("generation > 0", name="ck_spider_control_receipt_generation"),
        CheckConstraint("before_state_version BETWEEN 0 AND 2147483647", name="ck_spider_control_receipt_before_version"),
        CheckConstraint("after_state_version BETWEEN 1 AND 2147483647 AND after_state_version = before_state_version + 1", name="ck_spider_control_receipt_after_version"),
        CheckConstraint("barrier_sequence >= 0", name="ck_spider_control_receipt_barrier"),
        CheckConstraint("outcome = 'COMPLETED'", name="ck_spider_control_receipt_outcome"),
        CheckConstraint("recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_spider_control_receipt_recorded_at"),
    )


class SpiderMemberResubmissionRequest(Base):
    """Saved immutable operator intent; it cannot itself authorize a send."""

    __tablename__ = "spider_member_resubmission_request"

    request_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    actor: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    expected_version: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    initial_plan_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    target_action_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    request_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    recorded_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        server_default=text("date_trunc('milliseconds', clock_timestamp())"),
    )

    __table_args__ = (
        UniqueConstraint("environment", "execution_scope_id", "strategy_id", "actor", "idempotency_key",
                         name="uq_spider_member_resubmit_request_key"),
        UniqueConstraint("request_id", "environment", "execution_scope_id", "strategy_id",
                         name="uq_spider_member_resubmit_request_scope"),
        ForeignKeyConstraint(["environment", "execution_scope_id", "strategy_id"],
                             ["spider_runtime_scope.environment", "spider_runtime_scope.execution_scope_id",
                              "spider_runtime_scope.strategy_id"],
                             name="fk_spider_member_resubmit_request_scope"),
        ForeignKeyConstraint(["target_action_id", "environment", "execution_scope_id", "strategy_id"],
                             ["spider_runtime_action.action_id", "spider_runtime_action.environment",
                              "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
                             name="fk_spider_member_resubmit_request_target"),
        CheckConstraint("request_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmit_request_id"),
        CheckConstraint("environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_member_resubmit_environment"),
        CheckConstraint("execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_member_resubmit_scope"),
        CheckConstraint("strategy_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_member_resubmit_strategy"),
        CheckConstraint("actor COLLATE \"C\" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,63}$'", name="ck_spider_member_resubmit_actor"),
        CheckConstraint("idempotency_key COLLATE \"C\" ~ '^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$'", name="ck_spider_member_resubmit_key"),
        CheckConstraint("expected_version BETWEEN 0 AND 2147483647", name="ck_spider_member_resubmit_version"),
        CheckConstraint("generation > 0", name="ck_spider_member_resubmit_generation"),
        CheckConstraint("configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmit_config_hash"),
        CheckConstraint("initial_plan_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmit_plan_id"),
        CheckConstraint("target_action_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmit_target_id"),
        CheckConstraint("octet_length(request_payload) BETWEEN 1 AND 1048576", name="ck_spider_member_resubmit_payload"),
        CheckConstraint("request_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmit_payload_hash"),
        CheckConstraint("recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_spider_member_resubmit_recorded_at"),
    )


class SpiderMemberResubmission(Base):
    """Immutable relation from an original candidate through one explicit attempt."""

    __tablename__ = "spider_member_resubmission"

    request_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True)
    control_receipt_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False, unique=True)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    initial_plan_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    initial_member_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    target_action_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    resubmission_action_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False, unique=True)
    tape_ordinal: Mapped[int] = mapped_column(BigInteger, nullable=False)
    before_checkpoint_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    after_checkpoint_sha256: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "environment", "execution_scope_id", "strategy_id", "tape_ordinal",
            name="uq_spider_member_resubmission_tape_ordinal",
        ),
        ForeignKeyConstraint(["request_id", "environment", "execution_scope_id", "strategy_id"],
                             ["spider_member_resubmission_request.request_id", "spider_member_resubmission_request.environment",
                              "spider_member_resubmission_request.execution_scope_id", "spider_member_resubmission_request.strategy_id"],
                             name="fk_spider_member_resubmission_request"),
        ForeignKeyConstraint(["initial_plan_id", "environment", "execution_scope_id", "strategy_id"],
                             ["spider_runtime_plan.plan_id", "spider_runtime_plan.environment",
                              "spider_runtime_plan.execution_scope_id", "spider_runtime_plan.strategy_id"],
                             name="fk_spider_member_resubmission_plan"),
        ForeignKeyConstraint(["initial_member_id", "environment", "execution_scope_id", "strategy_id"],
                             ["spider_runtime_action.action_id", "spider_runtime_action.environment",
                              "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
                             name="fk_spider_member_resubmission_initial_member"),
        ForeignKeyConstraint(["target_action_id", "environment", "execution_scope_id", "strategy_id"],
                             ["spider_runtime_action.action_id", "spider_runtime_action.environment",
                              "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
                             name="fk_spider_member_resubmission_target"),
        ForeignKeyConstraint(["resubmission_action_id", "environment", "execution_scope_id", "strategy_id"],
                             ["spider_runtime_action.action_id", "spider_runtime_action.environment",
                              "spider_runtime_action.execution_scope_id", "spider_runtime_action.strategy_id"],
                             name="fk_spider_member_resubmission_action"),
        CheckConstraint("request_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_request_id"),
        CheckConstraint("control_receipt_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_receipt_id"),
        CheckConstraint("environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_member_resubmission_environment"),
        CheckConstraint("execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_member_resubmission_scope"),
        CheckConstraint("strategy_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_spider_member_resubmission_strategy"),
        CheckConstraint("generation > 0", name="ck_spider_member_resubmission_generation"),
        CheckConstraint("configuration_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_config_hash"),
        CheckConstraint("initial_plan_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_plan_id"),
        CheckConstraint("initial_member_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_initial_id"),
        CheckConstraint("target_action_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_target_id"),
        CheckConstraint("resubmission_action_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_action_id"),
        CheckConstraint("tape_ordinal > 0", name="ck_spider_member_resubmission_tape_ordinal"),
        CheckConstraint("before_checkpoint_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_before_hash"),
        CheckConstraint("after_checkpoint_sha256 COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_spider_member_resubmission_after_hash"),
    )
