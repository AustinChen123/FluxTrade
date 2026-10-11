"""ORM metadata for immutable Spider configuration request evidence."""

from sqlalchemy import (
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
