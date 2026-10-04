from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()


def _autoincrement_bigint():
    """Use PostgreSQL BIGINT, but SQLite INTEGER for rowid autoincrement."""
    return BigInteger().with_variant(Integer, "sqlite")


class Exchange(Base):
    __tablename__ = "exchange"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)


class Product(Base):
    __tablename__ = "product"
    id = Column(String, primary_key=True)
    exchange_id = Column(String, ForeignKey("exchange.id"), nullable=False)
    base_asset = Column(String, nullable=False)
    quote_asset = Column(String, nullable=False)


class Candlestick(Base):
    __tablename__ = "candlestick"
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("product.id"),
        primary_key=True,
    )
    timeframe: Mapped[str] = mapped_column(String, primary_key=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    open: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric, nullable=False)


class MarketDataApplication(Base):
    __tablename__ = "market_data_application"
    decision_contract_version = Column(Integer, nullable=True)
    __table_args__ = (
        CheckConstraint("decision_contract_version IS NULL OR decision_contract_version = 1", name="ck_mda_decision_contract"),
        UniqueConstraint("environment", "product_id", "timeframe", "timestamp", "decision_contract_version", name="uq_mda_decision_contract"),
    )

    environment = Column(String(64), primary_key=True)
    product_id = Column(String, ForeignKey("product.id"), primary_key=True)
    timeframe = Column(String, primary_key=True)
    timestamp = Column(BigInteger, primary_key=True)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)
    applied_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ResearchDataset(Base):
    __tablename__ = "research_dataset"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("product.id"),
        nullable=False,
    )
    timeframe: Mapped[str] = mapped_column(String(32), nullable=False)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    revision: Mapped[str] = mapped_column(String(128), nullable=False)
    timestamp_format: Mapped[str] = mapped_column(String(32), nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    roll_policy: Mapped[str | None] = mapped_column(String(128), nullable=True)
    start_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    quality_status: Mapped[str] = mapped_column(String(32), nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(String(32), nullable=False)
    sealed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint("row_count > 0", name="chk_research_dataset_nonempty"),
        CheckConstraint(
            "start_time <= end_time",
            name="chk_research_dataset_time_range",
        ),
        CheckConstraint(
            "quality_status IN ('validated')",
            name="chk_research_dataset_quality_status",
        ),
        CheckConstraint(
            "lifecycle_state IN ('importing', 'sealed')",
            name="chk_research_dataset_lifecycle_state",
        ),
        CheckConstraint(
            "(lifecycle_state = 'importing' AND sealed_at IS NULL) OR "
            "(lifecycle_state = 'sealed' AND sealed_at IS NOT NULL)",
            name="chk_research_dataset_seal_consistency",
        ),
    )


class ResearchCandlestick(Base):
    __tablename__ = "research_candlestick"

    dataset_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("research_dataset.id", ondelete="CASCADE"),
        primary_key=True,
    )
    timestamp: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    open: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    source_contract: Mapped[str | None] = mapped_column(String(64), nullable=True)


class Strategy(Base):
    __tablename__ = "strategy"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    configuration_json = Column(Text, nullable=True)


class Order(Base):
    __tablename__ = "order"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String, nullable=True)
    strategy_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("strategy.id"),
        nullable=False,
    )
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("product.id"),
        nullable=False,
    )
    exchange_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("exchange.id"),
        nullable=False,
    )
    account_profile: Mapped[str | None] = mapped_column(String(128), nullable=True)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    trigger_price: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    filled_quantity: Mapped[Decimal | None] = mapped_column(
        Numeric,
        nullable=True,
        default=0,
    )
    filled_price: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)

    # Migration 5 — idempotency / lifecycle columns.
    client_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    intent_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    acked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_reconciled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        Index(
            "uq_order_identified_client_order_id",
            "account_profile",
            "account_id",
            "client_order_id",
            unique=True,
            postgresql_where=text(
                "account_profile IS NOT NULL AND account_id IS NOT NULL "
                "AND client_order_id IS NOT NULL"
            ),
            sqlite_where=text(
                "account_profile IS NOT NULL AND account_id IS NOT NULL "
                "AND client_order_id IS NOT NULL"
            ),
        ),
        Index(
            "uq_order_legacy_client_order_id",
            "client_order_id",
            unique=True,
            postgresql_where=text(
                "account_profile IS NULL AND account_id IS NULL "
                "AND client_order_id IS NOT NULL"
            ),
            sqlite_where=text(
                "account_profile IS NULL AND account_id IS NULL "
                "AND client_order_id IS NOT NULL"
            ),
        ),
        Index(
            "uq_order_identified_exchange_order_id",
            "exchange_id",
            "account_profile",
            "account_id",
            "exchange_order_id",
            unique=True,
            postgresql_where=text(
                "account_profile IS NOT NULL AND account_id IS NOT NULL "
                "AND exchange_order_id IS NOT NULL"
            ),
            sqlite_where=text(
                "account_profile IS NOT NULL AND account_id IS NOT NULL "
                "AND exchange_order_id IS NOT NULL"
            ),
        ),
        Index(
            "uq_order_legacy_exchange_order_id",
            "exchange_id",
            "exchange_order_id",
            unique=True,
            postgresql_where=text(
                "account_profile IS NULL AND account_id IS NULL "
                "AND exchange_order_id IS NOT NULL"
            ),
            sqlite_where=text(
                "account_profile IS NULL AND account_id IS NULL "
                "AND exchange_order_id IS NOT NULL"
            ),
        ),
        CheckConstraint(
            "(account_profile IS NULL AND account_id IS NULL) OR "
            "(account_profile IS NOT NULL AND account_id IS NOT NULL AND "
            "TRIM(account_profile) <> '' AND TRIM(account_id) <> '')",
            name="chk_order_account_identity_complete",
        ),
    )


class Trade(Base):
    __tablename__ = "trade"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("order.id"),
        nullable=False,
    )
    exchange_trade_id: Mapped[str | None] = mapped_column(String, nullable=True)
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("product.id"),
        nullable=False,
    )
    side: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    fee: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    fee_asset: Mapped[str | None] = mapped_column(String, nullable=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)


class Position(Base):
    __tablename__ = "position"

    strategy_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("strategy.id"),
        primary_key=True,
    )
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("product.id"),
        primary_key=True,
    )
    side: Mapped[str] = mapped_column(String, primary_key=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    last_update_timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)


class SignalAudit(Base):
    __tablename__ = "signal_audit"

    id: Mapped[int] = mapped_column(
        _autoincrement_bigint(),
        primary_key=True,
        autoincrement=True,
    )

    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)

    strategy_id: Mapped[str] = mapped_column(String, nullable=False)

    product_id: Mapped[str] = mapped_column(String, nullable=False)

    signal_type: Mapped[str] = mapped_column(String, nullable=False)

    risk_status: Mapped[str] = mapped_column(String, nullable=False)  # PASS, REJECT

    risk_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    order_id: Mapped[str | None] = mapped_column(String, nullable=True)

    # Migration 5 — TEXT upgraded to JSONB.
    details_json: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    # Migration 5 — Path B audit linkage + multi-signal batch correlation.
    client_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    intent_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    outcome_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSONB,
        nullable=True,
    )

    signal_batch_id: Mapped[str | None] = mapped_column(String(64), nullable=True)


class SystemEvent(Base):
    """Cross-cutting system events log (Migration 5).

    Captures reconcile / gene_promote / gene_retire / system_error events
    so that operational tooling can audit non-trade activity without
    polluting the trade audit tables. ``related_order_id`` is a string FK
    because ``order.id`` itself is a string PK in this codebase.
    """

    __tablename__ = "system_events"

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    event_type = Column(String(64), nullable=False)
    event_subtype = Column(String(64), nullable=True)
    related_strategy_id = Column(String, ForeignKey("strategy.id"), nullable=True)
    related_order_id = Column(String, ForeignKey("order.id"), nullable=True)
    # No FK — gene_records lands in migration 7.
    related_gene_id = Column(BigInteger, nullable=True)
    payload = Column(JSONB, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('reconcile','gene_promote','gene_retire','ops','system_error')",
            name="chk_system_events_type",
        ),
    )


class BacktestResultSummary(Base):
    __tablename__ = "backtest_result_summary"
    __table_args__ = (
        UniqueConstraint("job_id", name="uq_backtest_summary_job_id"),
        CheckConstraint(
            "((job_id IS NULL) AND dataset_id IS NULL AND subject_kind IS NULL "
            "AND subject_id IS NULL AND input_digest IS NULL AND result_digest IS NULL "
            "AND product_id IS NULL AND timeframe IS NULL AND currency IS NULL "
            "AND completed_at IS NULL AND initial_balance IS NULL AND net_pnl IS NULL "
            "AND return_pct IS NULL AND max_drawdown IS NULL AND sharpe IS NULL "
            "AND sortino IS NULL AND calmar IS NULL) OR "
            "((job_id IS NOT NULL) AND dataset_id IS NOT NULL "
            "AND subject_kind IS NOT NULL AND subject_kind = 'STRATEGY_ARTIFACT' "
            "AND subject_id IS NOT NULL "
            "AND input_digest IS NOT NULL AND result_digest IS NOT NULL "
            "AND product_id IS NOT NULL AND timeframe IS NOT NULL "
            "AND currency IS NOT NULL AND completed_at IS NOT NULL "
            "AND initial_balance IS NOT NULL AND net_pnl IS NOT NULL "
            "AND return_pct IS NOT NULL AND max_drawdown IS NOT NULL "
            "AND sharpe IS NOT NULL AND sortino IS NOT NULL AND calmar IS NOT NULL)",
            name="ck_backtest_summary_formal_fields",
        ),
        CheckConstraint(
            "subject_kind IS NULL OR subject_kind = 'STRATEGY_ARTIFACT'",
            name="ck_backtest_summary_subject_kind",
        ),
        CheckConstraint(
            "initial_balance IS NULL OR initial_balance > 0",
            name="ck_backtest_summary_initial_balance_positive",
        ),
        CheckConstraint(
            "max_drawdown IS NULL OR max_drawdown >= 0",
            name="ck_backtest_summary_max_drawdown_nonnegative",
        ),
        CheckConstraint(
            "job_id IS NULL OR total_pnl NOT IN "
            "('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
            name="ck_backtest_summary_total_pnl_finite_formal",
        ).ddl_if(dialect="postgresql"),
        *(
            CheckConstraint(
                f"{field} IS NULL OR {field} NOT IN "
                "('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
                name=f"ck_backtest_summary_{field}_finite",
            ).ddl_if(dialect="postgresql")
            for field in (
                "initial_balance",
                "net_pnl",
                "return_pct",
                "max_drawdown",
                "sharpe",
                "sortino",
                "calmar",
            )
        ),
    )
    id: Mapped[int] = mapped_column(
        _autoincrement_bigint(),
        primary_key=True,
        autoincrement=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("strategy.id"),
        nullable=False,
    )
    start_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    # Text is used for JSONB compatibility in the generic ORM.
    metrics_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_id: Mapped[str | None] = mapped_column(String, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(
        String(128),
        ForeignKey(
            "research_dataset.id",
            name="fk_backtest_summary_dataset_id_research_dataset",
        ),
        nullable=True,
    )
    subject_kind: Mapped[str | None] = mapped_column(String, nullable=True)
    subject_id: Mapped[str | None] = mapped_column(String, nullable=True)
    input_digest: Mapped[str | None] = mapped_column(String, nullable=True)
    result_digest: Mapped[str | None] = mapped_column(String, nullable=True)
    product_id: Mapped[str | None] = mapped_column(
        String,
        ForeignKey("product.id", name="fk_backtest_summary_product_id_product"),
        nullable=True,
    )
    timeframe: Mapped[str | None] = mapped_column(String(32), nullable=True)
    currency: Mapped[str | None] = mapped_column(String, nullable=True)
    completed_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    initial_balance: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    net_pnl: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    return_pct: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    max_drawdown: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    sharpe: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    sortino: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    calmar: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)


class BacktestEquitySample(Base):
    __tablename__ = "backtest_equity_sample"
    __table_args__ = (
        CheckConstraint("sequence >= 0", name="ck_backtest_equity_sample_sequence"),
        CheckConstraint(
            "drawdown >= 0", name="ck_backtest_equity_sample_drawdown_nonnegative"
        ),
        CheckConstraint(
            "equity NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
            name="ck_backtest_equity_sample_equity_finite",
        ).ddl_if(dialect="postgresql"),
        CheckConstraint(
            "drawdown NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
            name="ck_backtest_equity_sample_drawdown_finite",
        ).ddl_if(dialect="postgresql"),
    )

    summary_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "backtest_result_summary.id",
            name="fk_backtest_equity_sample_summary",
        ),
        primary_key=True,
    )
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    equity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    drawdown: Mapped[Decimal] = mapped_column(Numeric, nullable=False)


class BacktestClosedTrade(Base):
    __tablename__ = "backtest_closed_trade"
    __table_args__ = (
        CheckConstraint("sequence >= 0", name="ck_backtest_closed_trade_sequence"),
        CheckConstraint("side IN ('LONG', 'SHORT')", name="ck_backtest_closed_trade_side"),
        CheckConstraint(
            "quantity > 0", name="ck_backtest_closed_trade_quantity_positive"
        ),
        *(
            CheckConstraint(
                f"{field} NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
                name=f"ck_backtest_closed_trade_{field}_finite",
            ).ddl_if(dialect="postgresql")
            for field in ("quantity", "entry_price", "exit_price", "fee", "pnl")
        ),
    )

    summary_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "backtest_result_summary.id",
            name="fk_backtest_closed_trade_summary",
        ),
        primary_key=True,
    )
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    entry_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    exit_time: Mapped[int] = mapped_column(BigInteger, nullable=False)
    side: Mapped[str] = mapped_column(String(5), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    exit_price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    fee: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False)


class BacktestMonthlyReturn(Base):
    __tablename__ = "backtest_monthly_return"
    __table_args__ = (
        CheckConstraint(
            "return_pct NOT IN ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
            name="ck_backtest_monthly_return_finite",
        ).ddl_if(dialect="postgresql"),
    )

    summary_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "backtest_result_summary.id",
            name="fk_backtest_monthly_return_summary",
        ),
        primary_key=True,
    )
    month: Mapped[str] = mapped_column(String(7), primary_key=True)
    return_pct: Mapped[Decimal] = mapped_column(Numeric, nullable=False)


class BacktestPnlDistribution(Base):
    __tablename__ = "backtest_pnl_distribution"
    __table_args__ = (
        CheckConstraint("sequence >= 0", name="ck_backtest_pnl_distribution_sequence"),
        CheckConstraint("count >= 0", name="ck_backtest_pnl_distribution_count"),
        *(
            CheckConstraint(
                f"{field} IS NULL OR {field} NOT IN "
                "('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)",
                name=f"ck_backtest_pnl_distribution_{field}_finite",
            ).ddl_if(dialect="postgresql")
            for field in ("lower", "upper")
        ),
    )

    summary_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "backtest_result_summary.id",
            name="fk_backtest_pnl_distribution_summary",
        ),
        primary_key=True,
    )
    sequence: Mapped[int] = mapped_column(Integer, primary_key=True)
    lower: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    upper: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    count: Mapped[int] = mapped_column(Integer, nullable=False)


class BacktestTradeLog(Base):
    __tablename__ = "backtest_trade_log"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    session_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("backtest_result_summary.id"),
        nullable=False,
    )
    strategy_id: Mapped[str | None] = mapped_column(String, nullable=True)
    order_id: Mapped[str] = mapped_column(String, nullable=False)
    exchange_trade_id: Mapped[str | None] = mapped_column(String, nullable=True)
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("product.id"),
        nullable=False,
    )
    side: Mapped[str] = mapped_column(String, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    fee: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    fee_asset: Mapped[str | None] = mapped_column(String, nullable=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fill_sequence: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "fill_sequence",
            name="uq_backtest_trade_log_session_fill_sequence",
        ),
        CheckConstraint(
            "fill_sequence IS NULL OR fill_sequence >= 0",
            name="chk_backtest_trade_log_fill_sequence_nonnegative",
        ),
    )


class StrategyState(Base):
    __tablename__ = "strategy_state"
    strategy_id: Mapped[str] = mapped_column(String, primary_key=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    config_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    performance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_heartbeat: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    uptime_start: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Migration 6 — audit / lifecycle metadata + optimistic-lock version.
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    entered_error_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    recovered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    stopped_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    __table_args__ = (
        CheckConstraint(
            "status <> 'ERROR' OR "
            "(entered_error_at IS NOT NULL AND last_error_message IS NOT NULL)",
            name="chk_error_state",
        ),
        CheckConstraint(
            "status <> 'STOPPED' OR stopped_at IS NOT NULL",
            name="chk_stopped_state",
        ),
    )


class StrategyStateTransition(Base):
    """Append-only audit log of strategy status transitions (Migration 6).

    Each row captures one ``from_status -> to_status`` change so operators
    can reconstruct the lifecycle of any strategy without relying on the
    point-in-time ``strategy_state`` row.
    """

    __tablename__ = "strategy_state_transitions"

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    strategy_id = Column(String, ForeignKey("strategy.id"), nullable=False)
    from_status = Column(String(32), nullable=False)
    to_status = Column(String(32), nullable=False)
    transitioned_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    reason = Column(Text, nullable=True)
    actor = Column(String(64), nullable=True)


class DailyNavSnapshot(Base):
    """End-of-day NAV snapshot per strategy (Migration 6).

    Used to compute realised drawdown / period returns without scanning
    the trade log. ``nav`` is ``NUMERIC(28, 8)`` — float is forbidden for
    monetary values per FluxTrade Decimal rules.
    """

    __tablename__ = "daily_nav_snapshots"

    id: Mapped[int] = mapped_column(
        _autoincrement_bigint(),
        primary_key=True,
        autoincrement=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("strategy.id"),
        nullable=False,
    )
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    nav: Mapped[Decimal] = mapped_column(Numeric(28, 8), nullable=False)
    base_currency: Mapped[str] = mapped_column(String(16), nullable=False)
    drawdown: Mapped[Decimal | None] = mapped_column(Numeric(10, 8), nullable=True)
    return_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 8),
        nullable=True,
    )
    source: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default="eod_snapshot",
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "source IN ('eod_snapshot','startup_reconcile','manual')",
            name="chk_nav_source",
        ),
        UniqueConstraint(
            "strategy_id",
            "snapshot_date",
            name="uq_daily_nav_strategy_date",
        ),
    )


class EvolutionEpoch(Base):
    """GA evolution epoch record (Migration 7).

    Append-only ledger of every GA run. The four ``eval_*`` columns are
    mandatory because ``best_score`` is only meaningful when paired with
    its evaluation context (pair / window / timeframe).
    """

    __tablename__ = "evolution_epochs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    revision: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
        server_default="1",
    )
    strategy_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("strategy.id"),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    pop_size: Mapped[int] = mapped_column(Integer, nullable=False)
    max_generations: Mapped[int] = mapped_column(Integer, nullable=False)
    generations_run: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Numeric (Decimal) — float forbidden for monetary / ratio values.
    best_score: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 8),
        nullable=True,
    )
    seed: Mapped[int] = mapped_column(BigInteger, nullable=False)
    config_json: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
        server_default="'{}'::jsonb",
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default="running",
    )
    eval_pair: Mapped[str] = mapped_column(String(32), nullable=False)
    eval_start_date: Mapped[date] = mapped_column(Date, nullable=False)
    eval_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    eval_timeframe: Mapped[str] = mapped_column(String(8), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'completed', 'aborted')",
            name="chk_epoch_status",
        ),
        CheckConstraint("revision > 0", name="ck_evolution_epoch_revision_positive"),
    )


class GeneRecord(Base):
    """GA gene record with role lifecycle (Migration 7).

    Role transitions: ``challenger`` -> ``champion`` -> ``retired``. At
    most one ``champion`` per strategy is enforced by a partial unique
    index (defined in the migration via raw DDL).
    """

    __tablename__ = "gene_records"

    id: Mapped[int] = mapped_column(
        _autoincrement_bigint(),
        primary_key=True,
        autoincrement=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("strategy.id"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    param_pack: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    score_total: Mapped[Decimal] = mapped_column(
        Numeric(18, 8),
        nullable=False,
    )
    score_breakdown: Mapped[dict[str, object]] = mapped_column(
        JSONB,
        nullable=False,
    )
    # Positive loss magnitude normalized at the parameter-search boundary.
    max_drawdown: Mapped[Decimal] = mapped_column(
        Numeric(18, 8),
        nullable=False,
    )
    generation_index: Mapped[int] = mapped_column(Integer, nullable=False)
    candidate_id: Mapped[str] = mapped_column(String(64), nullable=False)
    epoch_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("evolution_epochs.id"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    activated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    retired_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "role IN ('challenger', 'champion', 'retired')",
            name="chk_gene_role",
        ),
        UniqueConstraint(
            "epoch_id",
            "generation_index",
            "candidate_id",
            name="uq_gene_epoch_generation_candidate",
        ),
    )
