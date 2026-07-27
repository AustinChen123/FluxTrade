from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

Base = declarative_base()


def _autoincrement_bigint():
    """Use PostgreSQL BIGINT, but SQLite INTEGER for rowid autoincrement."""
    return BigInteger().with_variant(Integer, "sqlite")


class Exchange(Base):
    __tablename__ = 'exchange'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

class Product(Base):
    __tablename__ = 'product'
    id = Column(String, primary_key=True)
    exchange_id = Column(String, ForeignKey('exchange.id'), nullable=False)
    base_asset = Column(String, nullable=False)
    quote_asset = Column(String, nullable=False)

class Candlestick(Base):
    __tablename__ = 'candlestick'
    product_id = Column(String, ForeignKey('product.id'), primary_key=True)
    timeframe = Column(String, primary_key=True)
    timestamp = Column(BigInteger, primary_key=True)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)


class MarketDataApplication(Base):
    __tablename__ = "market_data_application"

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
    __tablename__ = 'research_dataset'

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    product_id: Mapped[str] = mapped_column(
        String,
        ForeignKey('product.id'),
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
        CheckConstraint('row_count > 0', name='chk_research_dataset_nonempty'),
        CheckConstraint(
            'start_time <= end_time',
            name='chk_research_dataset_time_range',
        ),
        CheckConstraint(
            "quality_status IN ('validated')",
            name='chk_research_dataset_quality_status',
        ),
        CheckConstraint(
            "lifecycle_state IN ('importing', 'sealed')",
            name='chk_research_dataset_lifecycle_state',
        ),
        CheckConstraint(
            "(lifecycle_state = 'importing' AND sealed_at IS NULL) OR "
            "(lifecycle_state = 'sealed' AND sealed_at IS NOT NULL)",
            name='chk_research_dataset_seal_consistency',
        ),
    )


class ResearchCandlestick(Base):
    __tablename__ = 'research_candlestick'

    dataset_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey('research_dataset.id', ondelete='CASCADE'),
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
    __tablename__ = 'strategy'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    configuration_json = Column(Text, nullable=True)

class Order(Base):
    __tablename__ = 'order'
    id = Column(String, primary_key=True)
    exchange_order_id = Column(String, nullable=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
    product_id = Column(String, ForeignKey('product.id'), nullable=False)
    exchange_id = Column(String, ForeignKey('exchange.id'), nullable=False)
    account_profile = Column(String(128), nullable=True)
    account_id = Column(String(128), nullable=True)
    type = Column(String, nullable=False)
    side = Column(String, nullable=False)
    price = Column(Numeric, nullable=True)
    trigger_price = Column(Numeric, nullable=True)
    quantity = Column(Numeric, nullable=False)
    status = Column(String, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    filled_quantity = Column(Numeric, nullable=True, default=0)
    filled_price = Column(Numeric, nullable=True)

    # Migration 5 — idempotency / lifecycle columns.
    client_order_id = Column(String(128), nullable=True)
    intent_payload = Column(JSONB, nullable=True)
    submitted_at = Column(DateTime(timezone=True), nullable=True)
    acked_at = Column(DateTime(timezone=True), nullable=True)
    last_reconciled_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint('exchange_order_id', 'exchange_id', name='uq_order_exchange_id'),
        CheckConstraint(
            "(account_profile IS NULL AND account_id IS NULL) OR "
            "(account_profile IS NOT NULL AND account_id IS NOT NULL AND "
            "TRIM(account_profile) <> '' AND TRIM(account_id) <> '')",
            name='chk_order_account_identity_complete',
        ),
    )

class Trade(Base):
    __tablename__ = 'trade'
    id = Column(String, primary_key=True)
    order_id = Column(String, ForeignKey('order.id'), nullable=False)
    exchange_trade_id = Column(String, nullable=True)
    product_id = Column(String, ForeignKey('product.id'), nullable=False)
    side = Column(String, nullable=False)
    price = Column(Numeric, nullable=False)
    quantity = Column(Numeric, nullable=False)
    fee = Column(Numeric, nullable=True)
    fee_asset = Column(String, nullable=True)
    timestamp = Column(BigInteger, nullable=False)

class Position(Base):

    __tablename__ = 'position'

    strategy_id = Column(String, ForeignKey('strategy.id'), primary_key=True)

    product_id = Column(String, ForeignKey('product.id'), primary_key=True)

    side = Column(String, primary_key=True)

    quantity = Column(Numeric, nullable=False)

    entry_price = Column(Numeric, nullable=False)

    unrealized_pnl = Column(Numeric, nullable=False)

    last_update_timestamp = Column(BigInteger, nullable=False)



class SignalAudit(Base):

    __tablename__ = 'signal_audit'

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)

    timestamp = Column(BigInteger, nullable=False)

    strategy_id = Column(String, nullable=False)

    product_id = Column(String, nullable=False)

    signal_type = Column(String, nullable=False)

    risk_status = Column(String, nullable=False) # PASS, REJECT

    risk_message = Column(Text, nullable=True)

    order_id = Column(String, nullable=True)

    # Migration 5 — TEXT upgraded to JSONB.
    details_json = Column(JSONB, nullable=True)

    # Migration 5 — Path B audit linkage + multi-signal batch correlation.
    client_order_id = Column(String(128), nullable=True)

    intent_payload = Column(JSONB, nullable=True)

    outcome_payload = Column(JSONB, nullable=True)

    signal_batch_id = Column(String(64), nullable=True)


class SystemEvent(Base):
    """Cross-cutting system events log (Migration 5).

    Captures reconcile / gene_promote / gene_retire / system_error events
    so that operational tooling can audit non-trade activity without
    polluting the trade audit tables. ``related_order_id`` is a string FK
    because ``order.id`` itself is a string PK in this codebase.
    """

    __tablename__ = 'system_events'

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    event_type = Column(String(64), nullable=False)
    event_subtype = Column(String(64), nullable=True)
    related_strategy_id = Column(String, ForeignKey('strategy.id'), nullable=True)
    related_order_id = Column(String, ForeignKey('order.id'), nullable=True)
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
            name='chk_system_events_type',
        ),
    )


class BacktestResultSummary(Base):
    __tablename__ = 'backtest_result_summary'
    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
    start_time = Column(BigInteger, nullable=False)
    end_time = Column(BigInteger, nullable=False)
    total_pnl = Column(Numeric, nullable=False)
    metrics_json = Column(Text, nullable=True) # Using Text for JSONB compatibility in generic ORM

class BacktestTradeLog(Base):
    __tablename__ = 'backtest_trade_log'
    id = Column(String, primary_key=True)
    session_id = Column(BigInteger, ForeignKey('backtest_result_summary.id'), nullable=False)
    strategy_id = Column(String, nullable=True)
    order_id = Column(String, nullable=False)
    exchange_trade_id = Column(String, nullable=True)
    product_id = Column(String, ForeignKey('product.id'), nullable=False)
    side = Column(String, nullable=False)
    price = Column(Numeric, nullable=False)
    quantity = Column(Numeric, nullable=False)
    fee = Column(Numeric, nullable=True)
    fee_asset = Column(String, nullable=True)
    timestamp = Column(BigInteger, nullable=False)
    fill_sequence = Column(BigInteger, nullable=True)

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
    __tablename__ = 'strategy_state'
    strategy_id = Column(String, primary_key=True)
    status = Column(String, nullable=False)
    config_json = Column(Text, nullable=True)
    performance_json = Column(Text, nullable=True)
    last_heartbeat = Column(BigInteger, nullable=True)
    uptime_start = Column(BigInteger, nullable=True)

    # Migration 6 — audit / lifecycle metadata + optimistic-lock version.
    last_error_message = Column(Text, nullable=True)
    entered_error_at = Column(DateTime(timezone=True), nullable=True)
    recovered_at = Column(DateTime(timezone=True), nullable=True)
    stopped_at = Column(DateTime(timezone=True), nullable=True)
    version = Column(Integer, nullable=False, server_default='0')

    __table_args__ = (
        CheckConstraint(
            "status <> 'ERROR' OR "
            "(entered_error_at IS NOT NULL AND last_error_message IS NOT NULL)",
            name='chk_error_state',
        ),
        CheckConstraint(
            "status <> 'STOPPED' OR stopped_at IS NOT NULL",
            name='chk_stopped_state',
        ),
    )


class StrategyStateTransition(Base):
    """Append-only audit log of strategy status transitions (Migration 6).

    Each row captures one ``from_status -> to_status`` change so operators
    can reconstruct the lifecycle of any strategy without relying on the
    point-in-time ``strategy_state`` row.
    """

    __tablename__ = 'strategy_state_transitions'

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
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

    __tablename__ = 'daily_nav_snapshots'

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    nav = Column(Numeric(28, 8), nullable=False)
    base_currency = Column(String(16), nullable=False)
    drawdown = Column(Numeric(10, 8), nullable=True)
    return_pct = Column(Numeric(10, 8), nullable=True)
    source = Column(
        String(32),
        nullable=False,
        server_default='eod_snapshot',
    )
    recorded_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    notes = Column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "source IN ('eod_snapshot','startup_reconcile','manual')",
            name='chk_nav_source',
        ),
        UniqueConstraint(
            'strategy_id',
            'snapshot_date',
            name='uq_daily_nav_strategy_date',
        ),
    )


class EvolutionEpoch(Base):
    """GA evolution epoch record (Migration 7).

    Append-only ledger of every GA run. The four ``eval_*`` columns are
    mandatory because ``best_score`` is only meaningful when paired with
    its evaluation context (pair / window / timeframe).
    """

    __tablename__ = 'evolution_epochs'

    id = Column(String(64), primary_key=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    pop_size = Column(Integer, nullable=False)
    max_generations = Column(Integer, nullable=False)
    generations_run = Column(Integer, nullable=True)
    # Numeric (Decimal) — float forbidden for monetary / ratio values.
    best_score = Column(Numeric(18, 8), nullable=True)
    seed = Column(BigInteger, nullable=False)
    config_json = Column(
        JSONB,
        nullable=False,
        server_default="'{}'::jsonb",
    )
    status = Column(
        String(32),
        nullable=False,
        server_default='running',
    )
    eval_pair = Column(String(32), nullable=False)
    eval_start_date = Column(Date, nullable=False)
    eval_end_date = Column(Date, nullable=False)
    eval_timeframe = Column(String(8), nullable=False)
    notes = Column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'completed', 'aborted')",
            name='chk_epoch_status',
        ),
    )


class GeneRecord(Base):
    """GA gene record with role lifecycle (Migration 7).

    Role transitions: ``challenger`` -> ``champion`` -> ``retired``. At
    most one ``champion`` per strategy is enforced by a partial unique
    index (defined in the migration via raw DDL).
    """

    __tablename__ = 'gene_records'

    id = Column(_autoincrement_bigint(), primary_key=True, autoincrement=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
    role = Column(String(16), nullable=False)
    param_pack = Column(JSONB, nullable=False)
    score_total = Column(Numeric(18, 8), nullable=False)
    score_breakdown = Column(JSONB, nullable=False)
    # Positive loss magnitude normalized at the parameter-search boundary.
    max_drawdown = Column(Numeric(18, 8), nullable=False)
    generation_index = Column(Integer, nullable=False)
    candidate_id = Column(String(64), nullable=False)
    epoch_id = Column(
        String(64),
        ForeignKey('evolution_epochs.id'),
        nullable=False,
    )
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    activated_at = Column(DateTime(timezone=True), nullable=True)
    retired_at = Column(DateTime(timezone=True), nullable=True)
    notes = Column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "role IN ('challenger', 'champion', 'retired')",
            name='chk_gene_role',
        ),
        UniqueConstraint(
            'epoch_id',
            'generation_index',
            'candidate_id',
            name='uq_gene_epoch_generation_candidate',
        ),
    )
