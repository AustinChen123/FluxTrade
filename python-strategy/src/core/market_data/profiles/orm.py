"""PostgreSQL profile metadata only; migration remains the schema-change owner."""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.core.orm_models import Base


def _finite(table: str, *columns: str) -> tuple[CheckConstraint, ...]:
    return tuple(
        CheckConstraint(
            f"{column} NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
            name=f"ck_{table}_{column}_finite",
        )
        for column in columns
    )


class MarketDataDecisionBatch(Base):
    __tablename__ = "market_data_decision_batch"
    environment: Mapped[str] = mapped_column(String(64), nullable=False)
    product_id: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(32), nullable=False)
    bar_start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    participant_count: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    batch_digest: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("date_trunc('milliseconds', clock_timestamp())"))
    __table_args__ = (
        PrimaryKeyConstraint("environment", "product_id", "timeframe", "bar_start_ms", name="pk_mdb"),
        UniqueConstraint("environment", "product_id", "timeframe", "bar_start_ms", "execution_scope_id", "contract_version", name="uq_mdb_scope"),
        ForeignKeyConstraint(["environment", "product_id", "timeframe", "bar_start_ms", "contract_version"], ["market_data_application.environment", "market_data_application.product_id", "market_data_application.timeframe", "market_data_application.timestamp", "market_data_application.decision_contract_version"], ondelete="RESTRICT", name="fk_mdb_receipt"),
        CheckConstraint("environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,64}$'", name="ck_mdb_environment"),
        CheckConstraint("product_id COLLATE \"C\" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'", name="ck_mdb_product"),
        CheckConstraint("timeframe COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,32}$'", name="ck_mdb_timeframe"),
        CheckConstraint("bar_start_ms >= 0", name="ck_mdb_time"),
        CheckConstraint("execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdb_scope"),
        CheckConstraint("contract_version = 1", name="ck_mdb_contract"),
        CheckConstraint("participant_count BETWEEN 0 AND 256", name="ck_mdb_count"),
        CheckConstraint("octet_length(canonical_payload) BETWEEN 1 AND 1048576", name="ck_mdb_payload"),
        CheckConstraint("batch_digest COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdb_digest"),
        CheckConstraint("recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_mdb_recorded"),
    )


class MarketDataDecisionOutcome(Base):
    __tablename__ = "market_data_decision_outcome"
    environment: Mapped[str] = mapped_column(String(64), nullable=False)
    product_id: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(32), nullable=False)
    bar_start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_version: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    trigger_kind: Mapped[str] = mapped_column(String(16, collation="C"), nullable=False)
    trigger_id: Mapped[str] = mapped_column(String(52, collation="C"), nullable=False)
    disposition: Mapped[str] = mapped_column(String(16), nullable=False)
    input_id: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    input_digest: Mapped[str | None] = mapped_column(String(64, collation="C"), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    signal_suppressed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    suppression_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    __table_args__ = (
        PrimaryKeyConstraint("environment", "execution_scope_id", "strategy_id", "strategy_version", "config_hash", "product_id", "trigger_kind", "trigger_id", name="pk_mdo"),
        UniqueConstraint("environment", "product_id", "timeframe", "bar_start_ms", "execution_scope_id", "strategy_id", name="uq_mdo_strategy"),
        ForeignKeyConstraint(["environment", "product_id", "timeframe", "bar_start_ms", "execution_scope_id", "contract_version"], ["market_data_decision_batch.environment", "market_data_decision_batch.product_id", "market_data_decision_batch.timeframe", "market_data_decision_batch.bar_start_ms", "market_data_decision_batch.execution_scope_id", "market_data_decision_batch.contract_version"], ondelete="RESTRICT", name="fk_mdo_batch"),
        ForeignKeyConstraint(["input_id"], ["market_data_decision_input.input_id"], ondelete="RESTRICT", name="fk_mdo_input"),
        CheckConstraint("environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,64}$'", name="ck_mdo_environment"),
        CheckConstraint("product_id COLLATE \"C\" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'", name="ck_mdo_product"),
        CheckConstraint("timeframe COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,32}$'", name="ck_mdo_timeframe"),
        CheckConstraint("bar_start_ms >= 0", name="ck_mdo_time"),
        CheckConstraint("execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdo_scope"),
        CheckConstraint("contract_version = 1", name="ck_mdo_contract"),
        CheckConstraint("strategy_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdo_strategy"),
        CheckConstraint("strategy_version COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdo_version"),
        CheckConstraint("config_hash COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdo_config"),
        CheckConstraint("trigger_kind = 'CANDLE' AND trigger_id = timeframe || ':' || bar_start_ms::text", name="ck_mdo_trigger"),
        CheckConstraint("input_id IS NULL OR input_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdo_input"),
        CheckConstraint("input_digest IS NULL OR input_digest COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdo_digest"),
        CheckConstraint("(input_id IS NULL) = (input_digest IS NULL)", name="ck_mdo_pair"),
        CheckConstraint("(disposition = 'APPLIED' AND input_id IS NOT NULL AND reason IS NULL) OR (disposition = 'SKIPPED' AND reason IS NOT NULL AND reason IN ('INPUT_STORE_FAILED', 'INPUT_COMMIT_UNCONFIRMED') AND NOT signal_suppressed)", name="ck_mdo_disposition"),
        CheckConstraint("(NOT signal_suppressed AND suppression_reason IS NULL) OR (signal_suppressed AND disposition = 'APPLIED' AND suppression_reason IS NOT NULL AND suppression_reason = 'SNAPSHOT_REVOKED')", name="ck_mdo_suppression"),
    )


class MarketDataDecisionInput(Base):
    __tablename__ = "market_data_decision_input"
    input_id: Mapped[str] = mapped_column(String(64, collation="C"), primary_key=True, nullable=False)
    environment: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    execution_scope_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    strategy_version: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    trigger_kind: Mapped[str] = mapped_column(String(16, collation="C"), nullable=False)
    trigger_id: Mapped[str] = mapped_column(String(52, collation="C"), nullable=False)
    requirements_digest: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    policy_digest: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    input_digest: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    product_id: Mapped[str] = mapped_column(String(64), ForeignKey("product.id"), nullable=False)
    decision_time_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    contract_version: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=text("date_trunc('milliseconds', clock_timestamp())"))
    __table_args__ = (
        UniqueConstraint("environment", "execution_scope_id", "strategy_id", "strategy_version", "config_hash", "product_id", "trigger_kind", "trigger_id", name="uq_mdd_input_key"),
        CheckConstraint("input_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdd_input_id"),
        CheckConstraint("environment COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdd_environment"),
        CheckConstraint("execution_scope_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdd_scope"),
        CheckConstraint("strategy_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdd_strategy"),
        CheckConstraint("strategy_version COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdd_version"),
        CheckConstraint("config_hash COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdd_config"),
        CheckConstraint("product_id COLLATE \"C\" ~ '^[A-Z0-9_]+:[A-Z0-9_-]+$'", name="ck_mdd_product"),
        CheckConstraint("trigger_kind = 'CANDLE'", name="ck_mdd_trigger_kind"),
        CheckConstraint("trigger_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,32}:(0|[1-9][0-9]{0,18})$' AND (length(split_part(trigger_id, ':', 2)) < 19 OR (length(split_part(trigger_id, ':', 2)) = 19 AND split_part(trigger_id, ':', 2) COLLATE \"C\" <= '9223372036854775807'))", name="ck_mdd_trigger"),
        CheckConstraint("requirements_digest COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdd_requirements"),
        CheckConstraint("policy_digest COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdd_policy"),
        CheckConstraint("input_digest COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdd_digest"),
        CheckConstraint("decision_time_ms >= 0 AND decision_time_ms <= 9223372036854775807", name="ck_mdd_time"),
        CheckConstraint("contract_version = 1", name="ck_mdd_contract"),
        CheckConstraint("octet_length(canonical_payload) BETWEEN 1 AND 8388608", name="ck_mdd_payload"),
        CheckConstraint("recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_mdd_recorded"),
    )


class MarketDataInvalidation(Base):
    __tablename__ = "market_data_invalidation"
    event_id: Mapped[str] = mapped_column(String(128, collation="C"), primary_key=True, nullable=False)
    snapshot_id: Mapped[str] = mapped_column(String(64), ForeignKey("volume_profile_snapshot.id", ondelete="RESTRICT"), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False,
        server_default=text("date_trunc('milliseconds', clock_timestamp())"))
    replacement_snapshot_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("volume_profile_snapshot.id", ondelete="RESTRICT"), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    __table_args__ = (
        CheckConstraint("event_id COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,128}$'", name="ck_mdi_event"),
        CheckConstraint("snapshot_id COLLATE \"C\" ~ '^[0-9a-f]{64}$'", name="ck_mdi_snapshot"),
        CheckConstraint("reason_code COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,64}$'", name="ck_mdi_reason"),
        CheckConstraint("source COLLATE \"C\" ~ '^[A-Za-z0-9_-]{1,64}$'", name="ck_mdi_source"),
        CheckConstraint("recorded_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND recorded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND recorded_at = date_trunc('milliseconds', recorded_at)", name="ck_mdi_recorded"),
        CheckConstraint("replacement_snapshot_id IS NULL OR (replacement_snapshot_id COLLATE \"C\" ~ '^[0-9a-f]{64}$' AND replacement_snapshot_id <> snapshot_id)", name="ck_mdi_replacement"),
        Index("ix_mdi_snapshot_recorded_event", "snapshot_id", "recorded_at", "event_id"),
        Index("ix_mdi_replacement", "replacement_snapshot_id"),
    )


class VolumeProfileSnapshot(Base):
    __tablename__ = "volume_profile_snapshot"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(64), ForeignKey("product.id"))
    window_start_ms: Mapped[int] = mapped_column(BigInteger)
    window_end_ms: Mapped[int] = mapped_column(BigInteger)
    period: Mapped[str] = mapped_column(String(8))
    timezone: Mapped[str] = mapped_column(String(8))
    grid_id: Mapped[str] = mapped_column(String(64))
    bin_origin: Mapped[Decimal] = mapped_column(Numeric)
    bin_step: Mapped[Decimal] = mapped_column(Numeric)
    algorithm_version: Mapped[str] = mapped_column(String(32))
    revision: Mapped[int] = mapped_column(BigInteger)
    content_sha256: Mapped[str] = mapped_column(String(64))
    source_manifest: Mapped[dict[str, object]] = mapped_column(JSONB)
    base_volume: Mapped[Decimal] = mapped_column(Numeric)
    quote_volume: Mapped[Decimal] = mapped_column(Numeric)
    aggregate_count: Mapped[int] = mapped_column(BigInteger)
    occupied_bins: Mapped[int] = mapped_column(BigInteger)
    quality: Mapped[str] = mapped_column(String(16))
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_available_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    availability_basis: Mapped[str] = mapped_column(String(16))
    reconciliation: Mapped[dict[str, object]] = mapped_column(JSONB)
    raw_retention_state: Mapped[str] = mapped_column(String(16))
    __table_args__ = (
        CheckConstraint(
            "window_start_ms >= 0 AND window_end_ms > window_start_ms AND window_start_ms % 86400000 = 0 AND window_end_ms % 86400000 = 0 AND window_end_ms - window_start_ms = 86400000",
            name="ck_vp_snapshot_window",
        ),
        CheckConstraint(
            "period = '1d' AND timezone = 'UTC'", name="ck_vp_snapshot_period"
        ),
        CheckConstraint("bin_step > 0", name="ck_vp_snapshot_step"),
        CheckConstraint("revision > 0", name="ck_vp_snapshot_revision"),
        CheckConstraint(
            "content_sha256 ~ '^[0-9a-f]{64}$'", name="ck_vp_snapshot_digest"
        ),
        CheckConstraint(
            "base_volume >= 0 AND quote_volume >= 0 AND aggregate_count >= 0 AND occupied_bins >= 0",
            name="ck_vp_snapshot_totals",
        ),
        CheckConstraint(
            "jsonb_typeof(source_manifest) = 'object' AND jsonb_typeof(reconciliation) = 'object'",
            name="ck_vp_snapshot_json",
        ),
        CheckConstraint(
            "quality IN ('VERIFIED', 'PARTIAL', 'CONFLICT')",
            name="ck_vp_snapshot_quality",
        ),
        CheckConstraint(
            "(quality = 'VERIFIED') = (published_at IS NOT NULL)",
            name="ck_vp_snapshot_published",
        ),
        CheckConstraint(
            "availability_basis IN ('OBSERVED', 'MODELED')",
            name="ck_vp_snapshot_availability",
        ),
        CheckConstraint(
            "raw_retention_state IN ('PRESENT', 'DELETED', 'NOT_STORED')",
            name="ck_vp_snapshot_retention",
        ),
        UniqueConstraint(
            "product_id",
            "window_start_ms",
            "window_end_ms",
            "grid_id",
            "algorithm_version",
            "revision",
            name="uq_vp_snapshot_revision",
        ),
        UniqueConstraint(
            "product_id",
            "window_start_ms",
            "window_end_ms",
            "grid_id",
            "algorithm_version",
            "content_sha256",
            name="uq_vp_snapshot_content",
        ),
        Index(
            "ix_vp_snapshot_reader",
            "product_id",
            "grid_id",
            "window_start_ms",
            "window_end_ms",
            "algorithm_version",
            "quality",
            "published_at",
        ),
        *_finite(
            __tablename__, "bin_origin", "bin_step", "base_volume", "quote_volume"
        ),
    )


class VolumeProfileBin(Base):
    __tablename__ = "volume_profile_bin"
    snapshot_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("volume_profile_snapshot.id", ondelete="CASCADE"),
        primary_key=True,
    )
    bin_index: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    base_volume: Mapped[Decimal] = mapped_column(Numeric)
    quote_volume: Mapped[Decimal] = mapped_column(Numeric)
    aggregate_count: Mapped[int] = mapped_column(BigInteger)
    __table_args__ = (
        CheckConstraint(
            "base_volume > 0 AND quote_volume > 0 AND aggregate_count > 0",
            name="ck_vp_bin_positive",
        ),
        *_finite(__tablename__, "base_volume", "quote_volume"),
    )


class VolumeProfileIngestJob(Base):
    __tablename__ = "volume_profile_ingest_job"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(64), ForeignKey("product.id"))
    window_start_ms: Mapped[int] = mapped_column(BigInteger)
    window_end_ms: Mapped[int] = mapped_column(BigInteger)
    grid_id: Mapped[str] = mapped_column(String(64))
    algorithm_version: Mapped[str] = mapped_column(String(32))
    config_sha256: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16))
    source_cursor: Mapped[dict[str, object]] = mapped_column(JSONB)
    attempt: Mapped[int] = mapped_column(BigInteger)
    retry_after_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    progress_manifest: Mapped[dict[str, object]] = mapped_column(JSONB)
    last_error_code: Mapped[str | None] = mapped_column(String(64))
    last_error_detail: Mapped[str | None] = mapped_column(Text)
    completed_snapshot_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("volume_profile_snapshot.id")
    )
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    __table_args__ = (
        CheckConstraint("id ~ '^[A-Za-z0-9_-]{1,64}$'", name="ck_vp_job_id"),
        CheckConstraint(
            "window_start_ms >= 0 AND window_end_ms > window_start_ms",
            name="ck_vp_job_window",
        ),
        CheckConstraint("config_sha256 ~ '^[0-9a-f]{64}$'", name="ck_vp_job_config"),
        CheckConstraint(
            "status IN ('RUNNING', 'RETRYABLE', 'DONE', 'FAILED')",
            name="ck_vp_job_status",
        ),
        CheckConstraint(
            "jsonb_typeof(source_cursor) = 'object' AND jsonb_typeof(progress_manifest) = 'object'",
            name="ck_vp_job_json",
        ),
        CheckConstraint("attempt >= 0", name="ck_vp_job_attempt"),
        CheckConstraint(
            "(lease_owner IS NULL) = (lease_expires_at IS NULL)", name="ck_vp_job_lease"
        ),
        Index("ix_vp_job_retry_lease", "status", "retry_after_at", "lease_expires_at"),
    )
