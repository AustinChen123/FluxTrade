"""Prospective portfolio paper evidence over the shared consumer and runtime."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import IO, Any, Protocol, cast

from sqlalchemy import select

from src.core.clock import BacktestClock
from src.core.consumer import DataConsumer
from src.core.engine import StrategyEngine
from src.core.mocks.account_service import BacktestAccountService
from src.core.models import Candlestick
from src.core.orm_models import Order, Trade
from src.core.portfolio_runtime import PortfolioDefinition
from src.core.product_registry import to_stream_key
from src.core.repositories import LiveOrderRepository
from src.core.runtime_environment import RuntimeEnvironment
from src.core.session_calendar import CmeEquityIndexCalendar
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from src.validation.paper_lifecycle import (
    PaperFillEvidence,
    PaperOrderEvidence,
    _adapter,
    _order_evidence,
    _session_factory,
    _validate_paper_product,
)
from src.validation.strategy_evidence import (
    StrategyEvidenceIdentity,
    require_verified_portfolio_identity,
)

_SCHEMA_VERSION = 2
_SESSION_CALENDAR = CmeEquityIndexCalendar()
_ONE_MINUTE_MS = 60_000
_FIVE_MINUTES_MS = 300_000
_CONSUMER_STOP_GRACE_SECONDS = 5.0


class _PaperForwardConsumer(Protocol):
    def acquire_service_ownership(self) -> None: ...

    def start(self) -> None: ...

    def request_stop(self) -> None: ...

    def assert_no_unresolved_deliveries(self) -> None: ...

    def cleanup_consumer_group(self) -> None: ...

    def stop(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PortfolioPaperForwardReport:
    environment: str
    run_id: str
    source_stream_key: str
    decision_stream_key: str
    strategy: StrategyEvidenceIdentity
    warmup_candles: int
    warmup_first_timestamp: int
    warmup_last_timestamp: int
    warmup_digest: str
    source_candles: int
    first_source_timestamp: int | None
    last_source_timestamp: int | None
    source_digest: str
    prospective_candles: int
    skipped_decision_prefix_candles: int
    first_prospective_timestamp: int | None
    last_prospective_timestamp: int | None
    candle_digest: str
    order_count: int
    fill_count: int
    reconciliation_unresolved_count: int
    reconciliation_verification_blocked_count: int
    final_position_count: int
    final_working_order_count: int
    orders: tuple[PaperOrderEvidence, ...]
    fills: tuple[PaperFillEvidence, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class _PaperForwardSession:
    def __init__(
        self,
        workspace: Path,
        *,
        run_id: str,
        definition: PortfolioDefinition,
        identity: StrategyEvidenceIdentity,
        warmup_candles: Sequence[Candlestick],
        output: IO[str],
    ) -> None:
        self.identity = identity

        strategy_ids = tuple(
            sleeve.strategy.strategy_id for sleeve in definition.sleeves
        )
        self._session_factory = _session_factory(
            workspace / "paper_forward.db",
            definition.product_id,
            (definition.portfolio_id, *strategy_ids),
        )
        self._adapter = _adapter(definition.product_id)
        self._clock = BacktestClock()
        account_service = BacktestAccountService(adapter=self._adapter)
        self._engine = StrategyEngine(
            None,
            self._clock,
            order_repository=LiveOrderRepository(
                db_session_factory=self._session_factory
            ),
            account_service=account_service,
            adapter=self._adapter,
            db_session_factory=self._session_factory,
            audit_external_orders=True,
            is_backtest=True,
        )
        self._engine.execution_engine.default_quantity = Decimal("1")
        warmup_processor = SignalProcessor(
            StrategyRegistry(),
            self._engine.execution_engine,
        )
        for sleeve in definition.sleeves:
            lookback = max(int(sleeve.strategy.requirements.lookback_window), 0)
            warmup_processor.warm_up(
                sleeve.strategy,
                list(warmup_candles[-lookback:]) if lookback else [],
            )
        self._engine.add_portfolio(definition)

        self.run_id = run_id
        self.environment = self._engine.runtime_environment.identity
        self.product_id = definition.product_id
        self.source_stream_key = to_stream_key(definition.product_id, "1m")
        self.decision_stream_key = to_stream_key(definition.product_id, "5m")
        self.warmup_candles = len(warmup_candles)
        self._warmup_first_timestamp = warmup_candles[0].timestamp
        self._warmup_last_timestamp = warmup_candles[-1].timestamp
        warmup_digest = hashlib.sha256()
        for candle in warmup_candles:
            _update_candle_digest(warmup_digest, candle)
        self._warmup_digest = warmup_digest.hexdigest()
        self._output = output
        self._lock = threading.Lock()
        self._source_digest = hashlib.sha256()
        self._source_candles = 0
        self._first_source_timestamp: int | None = None
        self._last_source_timestamp: int | None = None
        self._first_complete_bucket_timestamp: int | None = None
        self._source_segment_start_timestamp: int | None = None
        self._required_decision_timestamps: list[int] = []
        self._pending_decisions: list[Candlestick] = []
        self._last_observed_decision_timestamp: int | None = None
        self._skipped_decision_prefix_candles = 0
        self._digest = hashlib.sha256()
        self._prospective_candles = 0
        self._first_timestamp: int | None = None
        self._last_timestamp: int | None = None
        self._write(
            {
                "schema_version": _SCHEMA_VERSION,
                "type": "session",
                "run_id": run_id,
                "environment": self.environment,
                "source_stream_key": self.source_stream_key,
                "decision_stream_key": self.decision_stream_key,
                "warmup_candles": self.warmup_candles,
                "warmup_first_timestamp": self._warmup_first_timestamp,
                "warmup_last_timestamp": self._warmup_last_timestamp,
                "warmup_digest": self._warmup_digest,
                "strategy": self.identity.to_dict(),
            }
        )

    @staticmethod
    def _validate_warmup(
        definition: PortfolioDefinition,
        candles: Sequence[Candlestick],
    ) -> None:
        if definition.sleeves[0].strategy.requirements.timeframe != "5m":
            raise ValueError("paper-forward portfolio timeframe must be 5m")
        previous: int | None = None
        for candle in candles:
            if candle.product_id != definition.product_id or candle.timeframe != "5m":
                raise ValueError("paper-forward warmup candle identity mismatch")
            _assert_aligned_timestamp(candle.timestamp, _FIVE_MINUTES_MS, "warmup")
            if previous is not None:
                _assert_session_continuity(
                    previous,
                    candle.timestamp,
                    _FIVE_MINUTES_MS,
                    "paper-forward warmup",
                )
            previous = candle.timestamp
        required = max(
            int(sleeve.strategy.requirements.lookback_window)
            for sleeve in definition.sleeves
        )
        required = max(required, 1)
        if len(candles) < required:
            raise ValueError(
                "paper-forward warmup has insufficient candles: "
                f"available={len(candles)} required={required}"
            )

    def build_stream_channels(self) -> list[str]:
        return sorted((self.source_stream_key, self.decision_stream_key))

    def apply(self, model: object) -> None:
        if not isinstance(model, Candlestick):
            raise TypeError("paper-forward accepts completed candles only")
        candle = model
        with self._lock:
            if candle.product_id != self.product_id:
                raise ValueError("paper-forward prospective candle identity mismatch")
            if candle.timeframe == "1m":
                self._apply_source_candle(candle)
                return
            if candle.timeframe == "5m":
                self._accept_decision_candle(candle)
                return
            raise ValueError("paper-forward prospective candle identity mismatch")

    def _apply_source_candle(self, candle: Candlestick) -> None:
        _assert_aligned_timestamp(candle.timestamp, _ONE_MINUTE_MS, "source")
        if self._required_decision_timestamps:
            raise ValueError(
                "paper-forward source advanced before required decision: "
                f"{self._required_decision_timestamps[0]}"
            )
        previous_source_timestamp = self._last_source_timestamp
        previous_segment_start_timestamp = self._source_segment_start_timestamp
        starts_new_segment = self._last_source_timestamp is None
        if self._last_source_timestamp is not None:
            _assert_session_continuity(
                self._last_source_timestamp,
                candle.timestamp,
                _ONE_MINUTE_MS,
                "paper-forward source",
            )
            starts_new_segment = (
                candle.timestamp != self._last_source_timestamp + _ONE_MINUTE_MS
            )
        if starts_new_segment:
            self._source_segment_start_timestamp = candle.timestamp
        self._clock.set_time(candle.timestamp / 1_000)
        self._engine.on_backtest_market_data(candle)
        payload = candle.model_dump(mode="json")
        _update_candle_digest(self._source_digest, candle)
        self._source_candles += 1
        self._first_source_timestamp = self._first_source_timestamp or candle.timestamp
        self._last_source_timestamp = candle.timestamp
        self._first_complete_bucket_timestamp = (
            self._first_complete_bucket_timestamp
            or ((candle.timestamp + _FIVE_MINUTES_MS - 1) // _FIVE_MINUTES_MS)
            * _FIVE_MINUTES_MS
        )
        self._write(
            {
                "schema_version": _SCHEMA_VERSION,
                "type": "source_candle",
                "candle": payload,
            }
        )
        decision_timestamp: int | None = None
        if (
            starts_new_segment
            and previous_source_timestamp is not None
            and previous_segment_start_timestamp is not None
        ):
            previous_bucket = (
                previous_source_timestamp // _FIVE_MINUTES_MS
            ) * _FIVE_MINUTES_MS
            if (
                previous_source_timestamp
                == previous_bucket + _FIVE_MINUTES_MS - _ONE_MINUTE_MS
                and previous_segment_start_timestamp <= previous_bucket
            ):
                decision_timestamp = previous_bucket
        elif candle.timestamp % _FIVE_MINUTES_MS == 0:
            candidate = candle.timestamp - _FIVE_MINUTES_MS
            if (
                self._source_segment_start_timestamp is not None
                and self._source_segment_start_timestamp <= candidate
            ):
                decision_timestamp = candidate
        if (
            decision_timestamp is not None
            and self._first_complete_bucket_timestamp is not None
            and decision_timestamp >= self._first_complete_bucket_timestamp
        ):
            self._required_decision_timestamps.append(decision_timestamp)
        self._commit_ready_decisions()

    def _accept_decision_candle(self, candle: Candlestick) -> None:
        _assert_aligned_timestamp(candle.timestamp, _FIVE_MINUTES_MS, "decision")
        previous_timestamp = (
            self._last_observed_decision_timestamp
            if self._last_observed_decision_timestamp is not None
            else self._warmup_last_timestamp
        )
        _assert_session_continuity(
            previous_timestamp,
            candle.timestamp,
            _FIVE_MINUTES_MS,
            "paper-forward decision",
        )
        self._write(
            {
                "schema_version": _SCHEMA_VERSION,
                "type": "observed_decision",
                "candle": candle.model_dump(mode="json"),
            }
        )
        self._last_observed_decision_timestamp = candle.timestamp
        self._pending_decisions.append(candle)
        self._commit_ready_decisions()

    def _commit_ready_decisions(self) -> None:
        while self._pending_decisions:
            candle = self._pending_decisions[0]
            if self._first_complete_bucket_timestamp is None:
                return
            if candle.timestamp < self._first_complete_bucket_timestamp:
                self._pending_decisions.pop(0)
                self._skipped_decision_prefix_candles += 1
                self._write(
                    {
                        "schema_version": _SCHEMA_VERSION,
                        "type": "skipped_decision_prefix",
                        "candle": candle.model_dump(mode="json"),
                    }
                )
                continue
            if not self._required_decision_timestamps:
                return
            required_timestamp = self._required_decision_timestamps[0]
            if candle.timestamp != required_timestamp:
                raise ValueError(
                    "paper-forward decision does not match completed source bucket: "
                    f"required={required_timestamp} observed={candle.timestamp}"
                )
            if (
                self._last_source_timestamp is None
                or self._last_source_timestamp < candle.timestamp + _FIVE_MINUTES_MS
            ):
                return
            self._pending_decisions.pop(0)
            self._required_decision_timestamps.pop(0)
            self._engine.on_backtest_decision_candle(candle)
            payload = candle.model_dump(mode="json")
            _update_candle_digest(self._digest, candle)
            self._prospective_candles += 1
            self._first_timestamp = self._first_timestamp or candle.timestamp
            self._last_timestamp = candle.timestamp
            self._write(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "type": "prospective_candle",
                    "candle": payload,
                }
            )

    def prepare_report(self) -> PortfolioPaperForwardReport:
        """Validate the stopped run and build its report without marking success."""
        with self._lock:
            self._commit_ready_decisions()
            if self._required_decision_timestamps:
                raise ValueError(
                    "paper-forward evidence is incomplete: completed source bucket "
                    "has no observed decision"
                )
            if self._pending_decisions:
                raise ValueError(
                    "paper-forward evidence is incomplete: source watermark did "
                    "not cover all observed decisions"
                )
            if self._source_candles == 0:
                raise ValueError(
                    "paper-forward evidence requires at least one source candle"
                )
            if self._prospective_candles == 0:
                raise ValueError(
                    "paper-forward evidence requires at least one prospective candle"
                )
            reconciliation = (
                self._engine.execution_engine.reconcile_recoverable_client_orders()
            )
            unresolved_count = int(reconciliation["unresolved_count"])
            verification_blocked_count = int(
                reconciliation["verification_blocked_count"]
            )
            if unresolved_count or verification_blocked_count:
                raise AssertionError(
                    "paper-forward reconciliation was not resolved and verified: "
                    f"unresolved={unresolved_count} "
                    f"verification_blocked={verification_blocked_count}"
                )
            strategy_ids = tuple(
                sleeve.strategy.strategy_id
                for definition in self._engine.portfolio_instances.values()
                for sleeve in definition.sleeves
            )
            with self._session_factory() as session:
                orders = tuple(
                    session.scalars(select(Order).order_by(Order.timestamp, Order.id))
                )
                trades = tuple(
                    session.scalars(select(Trade).order_by(Trade.timestamp, Trade.id))
                )
            order_types = {str(order.id): str(order.type) for order in orders}
            order_strategy_ids = {
                str(order.id): str(order.strategy_id) for order in orders
            }
            report = PortfolioPaperForwardReport(
                environment=self.environment,
                run_id=self.run_id,
                source_stream_key=self.source_stream_key,
                decision_stream_key=self.decision_stream_key,
                strategy=self.identity,
                warmup_candles=self.warmup_candles,
                warmup_first_timestamp=self._warmup_first_timestamp,
                warmup_last_timestamp=self._warmup_last_timestamp,
                warmup_digest=self._warmup_digest,
                source_candles=self._source_candles,
                first_source_timestamp=self._first_source_timestamp,
                last_source_timestamp=self._last_source_timestamp,
                source_digest=self._source_digest.hexdigest(),
                prospective_candles=self._prospective_candles,
                skipped_decision_prefix_candles=(self._skipped_decision_prefix_candles),
                first_prospective_timestamp=self._first_timestamp,
                last_prospective_timestamp=self._last_timestamp,
                candle_digest=self._digest.hexdigest(),
                order_count=len(orders),
                fill_count=len(trades),
                reconciliation_unresolved_count=unresolved_count,
                reconciliation_verification_blocked_count=(verification_blocked_count),
                final_position_count=sum(
                    self._adapter.get_position(
                        self.product_id,
                        strategy_id=strategy_id,
                    )
                    is not None
                    for strategy_id in strategy_ids
                ),
                final_working_order_count=sum(
                    len(
                        self._adapter.get_open_orders(
                            self.product_id,
                            strategy_id,
                        )
                    )
                    for strategy_id in strategy_ids
                ),
                orders=tuple(_order_evidence(order) for order in orders),
                fills=tuple(
                    PaperFillEvidence(
                        order_id=str(trade.order_id),
                        strategy_id=order_strategy_ids[str(trade.order_id)],
                        order_type=order_types[str(trade.order_id)],
                        side=str(trade.side),
                        price=_decimal_text(cast(Decimal, trade.price)),
                        quantity=_decimal_text(cast(Decimal, trade.quantity)),
                        fee=(
                            None
                            if trade.fee is None
                            else _decimal_text(cast(Decimal, trade.fee))
                        ),
                        timestamp=int(cast(int, trade.timestamp)),
                    )
                    for trade in trades
                ),
            )
            return report

    def write_summary(self, report: PortfolioPaperForwardReport) -> None:
        """Persist the success boundary after ephemeral Redis cleanup."""
        with self._lock:
            self._write(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "type": "summary",
                    "report": report.to_dict(),
                }
            )

    def close(self) -> None:
        self._engine.shutdown()

    def _write(self, payload: dict[str, Any]) -> None:
        self._output.write(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        )
        self._output.flush()


def validate_portfolio_paper_forward_inputs(
    definition: PortfolioDefinition,
    warmup_candles: Sequence[Candlestick],
) -> None:
    """Validate paper-forward strategy and warm-up before creating artifacts."""
    _PaperForwardSession._validate_warmup(definition, warmup_candles)


def validate_portfolio_paper_forward_run(
    workspace: Path,
    *,
    run_id: str,
    definition: PortfolioDefinition,
    warmup_candles: Sequence[Candlestick],
    duration_seconds: float,
) -> tuple[RuntimeEnvironment, StrategyEvidenceIdentity]:
    """Validate one run before creating its workspace or evidence output."""
    if duration_seconds <= 0:
        raise ValueError("paper-forward duration_seconds must be positive")
    environment = RuntimeEnvironment.from_env()
    if not environment.identity.startswith("paper-forward-"):
        raise ValueError(
            "portfolio paper-forward requires FLUXTRADE_ENVIRONMENT=paper-forward-*"
        )
    if not run_id or any(char.isspace() for char in run_id):
        raise ValueError("paper-forward run_id must be non-empty without whitespace")
    if workspace.exists():
        raise FileExistsError(f"paper-forward workspace already exists: {workspace}")
    _validate_paper_product(definition.product_id)
    identity = require_verified_portfolio_identity(definition)
    validate_portfolio_paper_forward_inputs(definition, warmup_candles)
    return environment, identity


def run_portfolio_paper_forward(
    workspace: Path,
    *,
    run_id: str,
    portfolio_factory: Callable[[], PortfolioDefinition],
    warmup_candles: Sequence[Candlestick],
    output: IO[str],
    duration_seconds: float,
    consumer_factory: Callable[..., _PaperForwardConsumer] = DataConsumer,
    monotonic: Callable[[], float] = time.monotonic,
) -> PortfolioPaperForwardReport:
    """Run prospective 1m execution and completed 5m decision candles."""
    definition = portfolio_factory()
    environment, identity = validate_portfolio_paper_forward_run(
        workspace,
        run_id=run_id,
        definition=definition,
        warmup_candles=warmup_candles,
        duration_seconds=duration_seconds,
    )
    workspace.mkdir(parents=True, exist_ok=False)
    session = _PaperForwardSession(
        workspace,
        run_id=run_id,
        definition=definition,
        identity=identity,
        warmup_candles=warmup_candles,
        output=output,
    )
    try:
        consumer = consumer_factory(
            channels=[],
            on_message_callback=session.apply,
            channel_provider=session.build_stream_channels,
            pending_replay_callback=None,
            runtime_environment=environment,
            group_name=f"paper_forward:{environment.identity}:{run_id}",
            ephemeral_group=True,
        )
    except Exception:
        session.close()
        raise
    failure: list[BaseException] = []
    consumer_finished = threading.Event()
    stop_requested = threading.Event()

    def consume() -> None:
        try:
            consumer.start()
            if not stop_requested.is_set():
                failure.append(
                    RuntimeError("paper-forward consumer stopped unexpectedly")
                )
        except BaseException as exc:
            failure.append(exc)
        finally:
            consumer_finished.set()

    thread = threading.Thread(
        target=consume,
        name=f"paper-forward-{run_id}",
        daemon=False,
    )
    try:
        consumer.acquire_service_ownership()
        thread.start()
        deadline = monotonic() + duration_seconds
        while monotonic() < deadline and not failure:
            if consumer_finished.is_set():
                break
            time.sleep(min(0.1, max(0.0, deadline - monotonic())))
        if failure:
            raise RuntimeError("paper-forward consumer failed") from failure[0]
        stop_requested.set()
        consumer.request_stop()
        thread.join(timeout=_CONSUMER_STOP_GRACE_SECONDS)
        if thread.is_alive():
            thread.join()
            raise RuntimeError(
                "paper-forward consumer did not stop within shutdown grace period"
            )
        if failure:
            raise RuntimeError("paper-forward consumer failed") from failure[0]
        consumer.assert_no_unresolved_deliveries()
        report = session.prepare_report()
        consumer.cleanup_consumer_group()
        session.write_summary(report)
        return report
    finally:
        consumer.request_stop()
        if thread.is_alive():
            thread.join()
        try:
            consumer.stop()
        finally:
            session.close()


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _assert_aligned_timestamp(
    timestamp: int,
    interval_ms: int,
    label: str,
) -> None:
    if timestamp % interval_ms:
        raise ValueError(f"paper-forward {label} timestamp is not interval-aligned")


def _assert_session_continuity(
    previous_timestamp: int,
    timestamp: int,
    interval_ms: int,
    label: str,
) -> None:
    expected = previous_timestamp + interval_ms
    if timestamp == expected:
        return
    if timestamp <= previous_timestamp:
        raise ValueError(f"{label} timestamps must be strictly increasing")
    if timestamp < expected or _SESSION_CALENDAR.has_open_time(
        expected,
        timestamp,
    ):
        raise ValueError(f"{label} is not contiguous with scheduled prior history")


def _update_candle_digest(digest: Any, candle: Candlestick) -> None:
    digest.update(
        json.dumps(
            candle.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
