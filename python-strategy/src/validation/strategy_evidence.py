from __future__ import annotations

import csv
import hashlib
import io
import json
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import IO, Any, Callable, Iterable, Iterator, Protocol

from fluxtrade_core import (
    CandleAggregator,  # pyright: ignore[reportAttributeAccessIssue]
    Candlestick as RustCandlestick,  # pyright: ignore[reportAttributeAccessIssue]
)

from src.core.models import Candlestick, Signal, SignalType
from src.core.portfolio_runtime import (
    PortfolioCoordinator,
    PortfolioDefinition,
    PortfolioExposureSnapshot,
    PortfolioFactory,
    build_portfolio_artifact,
    portfolio_replay_configuration,
)
from src.core.strategy_loader import StrategyLoader
from src.strategies.base import BaseStrategy

_CANDLE_FIELDS = ("open", "high", "low", "close", "volume")
_FIVE_MINUTES_MS = 5 * 60_000
_SHADOW_SCHEMA_VERSION = 2


@dataclass(frozen=True, slots=True)
class StrategyEvidenceIdentity:
    strategy_id: str
    class_name: str
    display_name: str | None
    artifact_version: str | None
    readiness: str | None
    catalog_sha256: str | None
    replay_configuration_sha256: str

    def to_dict(self) -> dict[str, str | None]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class HistoricalParityReport:
    product_id: str
    source_timeframe: str
    target_timeframe: str
    matched_candles: int
    skipped_reference_prefix_candles: int
    extra_source_candles: int
    source_decision_count: int
    reference_decision_count: int
    source_actionable_signal_count: int
    reference_actionable_signal_count: int
    source_signal_digest: str
    reference_signal_digest: str
    source_sha256: str
    reference_sha256: str
    strategy: StrategyEvidenceIdentity

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ShadowRunReport:
    source_stream_key: str
    decision_stream_key: str
    product_id: str
    target_timeframe: str
    source_candles: int
    completed_candles: int
    skipped_decision_prefix_candles: int
    actionable_signal_count: int
    first_source_timestamp: int | None
    last_source_timestamp: int | None
    source_digest: str
    completed_digest: str
    decision_digest: str
    strategy: StrategyEvidenceIdentity

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class _DecisionEvidenceRuntime(Protocol):
    identity: StrategyEvidenceIdentity

    @property
    def product_id(self) -> str: ...

    @property
    def timeframe(self) -> str: ...

    def decide(self, candle: Candlestick) -> list[Signal]: ...


@dataclass(slots=True)
class _StrategyDecisionEvidenceRuntime:
    strategy: BaseStrategy
    identity: StrategyEvidenceIdentity

    @property
    def product_id(self) -> str:
        return self.strategy.product_id

    @property
    def timeframe(self) -> str:
        return self.strategy.requirements.timeframe

    def decide(self, candle: Candlestick) -> list[Signal]:
        return _signals(self.strategy, candle)


@dataclass(slots=True)
class _PortfolioDecisionEvidenceRuntime:
    definition: PortfolioDefinition
    identity: StrategyEvidenceIdentity
    coordinator: PortfolioCoordinator

    @property
    def product_id(self) -> str:
        return self.definition.product_id

    @property
    def timeframe(self) -> str:
        return self.definition.sleeves[0].strategy.requirements.timeframe

    def decide(self, candle: Candlestick) -> list[Signal]:
        decisions = [
            (sleeve.strategy.strategy_id, _signals(sleeve.strategy, candle))
            for sleeve in self.definition.sleeves
        ]
        coordinated = self.coordinator.coordinate_candle_decisions(
            candle,
            decisions,
            exposure_loader=lambda strategy_ids, _product_id, _intents: (
                PortfolioExposureSnapshot(
                    {strategy_id: Decimal("0") for strategy_id in strategy_ids}
                )
            ),
            default_quantity=Decimal("1"),
        )
        return [
            signal
            for _strategy_id, signals in coordinated
            for signal in signals
        ]


def load_strategy(
    strategy_directory: Path,
    strategy_id: str,
    product_id: str,
) -> BaseStrategy:
    loaded = StrategyLoader.scan_directory(str(strategy_directory))
    strategy_class = loaded.get(strategy_id)
    if strategy_class is None:
        raise ValueError(f"strategy not found: {strategy_id}")
    if isinstance(strategy_class, str):
        raise RuntimeError(strategy_class)
    if not issubclass(strategy_class, BaseStrategy):
        raise TypeError(f"artifact is not a strategy: {strategy_id}")
    strategy = strategy_class(strategy_id, product_id)
    require_verified_strategy_identity(strategy)
    return strategy


def load_portfolio(
    strategy_directory: Path,
    portfolio_id: str,
    product_id: str,
    *,
    config: dict[str, object],
) -> PortfolioDefinition:
    loaded = StrategyLoader.scan_directory(str(strategy_directory))
    factory_class = loaded.get(portfolio_id)
    if factory_class is None:
        raise ValueError(f"portfolio not found: {portfolio_id}")
    if isinstance(factory_class, str):
        raise RuntimeError(factory_class)
    if not issubclass(factory_class, PortfolioFactory):
        raise TypeError(f"artifact is not a portfolio: {portfolio_id}")
    definition = build_portfolio_artifact(
        factory_class,
        portfolio_id=portfolio_id,
        product_id=product_id,
        config=config,
    )
    require_verified_portfolio_identity(definition)
    return definition


def verify_historical_stream_parity(
    source_1m_path: Path,
    reference_5m_path: Path,
    *,
    product_id: str,
    strategy_factory: Callable[[], BaseStrategy],
) -> HistoricalParityReport:
    """Compare closed Rust aggregates and signal decisions against a 5m baseline.

    The reference may end before the source. Every reference row must match;
    source candles after its terminal timestamp are reported as extra coverage.
    """
    return _verify_historical_stream_parity(
        source_1m_path,
        reference_5m_path,
        product_id=product_id,
        runtime_factory=lambda: _strategy_evidence_runtime(
            strategy_factory()
        ),
    )


def verify_portfolio_historical_stream_parity(
    source_1m_path: Path,
    reference_5m_path: Path,
    *,
    product_id: str,
    portfolio_factory: Callable[[], PortfolioDefinition],
) -> HistoricalParityReport:
    """Verify aggregate and decision parity for one catalog portfolio."""
    return _verify_historical_stream_parity(
        source_1m_path,
        reference_5m_path,
        product_id=product_id,
        runtime_factory=lambda: _portfolio_evidence_runtime(
            portfolio_factory()
        ),
    )


def _verify_historical_stream_parity(
    source_1m_path: Path,
    reference_5m_path: Path,
    *,
    product_id: str,
    runtime_factory: Callable[[], _DecisionEvidenceRuntime],
) -> HistoricalParityReport:
    source_runtime = runtime_factory()
    reference_runtime = runtime_factory()
    strategy_identity = source_runtime.identity
    if reference_runtime.identity != strategy_identity:
        raise ValueError("historical parity runtime factory is not deterministic")
    if (
        source_runtime.product_id != product_id
        or reference_runtime.product_id != product_id
    ):
        raise ValueError("historical parity runtime product mismatch")
    if source_runtime.timeframe != "5m" or reference_runtime.timeframe != "5m":
        raise ValueError("historical parity runtime timeframe must be 5m")
    source_digest = hashlib.sha256()
    reference_digest = hashlib.sha256()
    source_decision_count = 0
    reference_decision_count = 0
    source_actionable_signal_count = 0
    reference_actionable_signal_count = 0
    matched = 0
    skipped_reference_prefix = 0
    extra = 0
    aggregator = CandleAggregator()
    first_source_timestamp: int | None = None

    with (
        _verified_csv_snapshot(source_1m_path) as (
            source_handle,
            source_file_sha256,
        ),
        _verified_csv_snapshot(reference_5m_path) as (
            reference_handle,
            reference_file_sha256,
        ),
    ):
        source_rows = csv.DictReader(source_handle)
        reference_rows = iter(csv.DictReader(reference_handle))
        reference_row = next(reference_rows, None)

        for source_row in source_rows:
            source_timestamp = _timestamp_ms(source_row["timestamp"])
            first_source_timestamp = first_source_timestamp or source_timestamp
            completed = aggregator.add_candle(
                _rust_candle(source_row, product_id, source_timestamp),
                "5m",
            )
            if completed is None:
                continue
            if reference_row is None:
                extra += 1
                continue

            reference_timestamp = _timestamp_ms(reference_row["timestamp"])
            if matched == 0 and reference_timestamp < completed.timestamp:
                partial_bucket_start = (
                    first_source_timestamp // _FIVE_MINUTES_MS
                ) * _FIVE_MINUTES_MS
                if (
                    reference_timestamp == partial_bucket_start
                    and first_source_timestamp > partial_bucket_start
                ):
                    skipped_reference_prefix += 1
                    reference_row = next(reference_rows, None)
                    if reference_row is None:
                        extra += 1
                        continue
                    reference_timestamp = _timestamp_ms(reference_row["timestamp"])
            if completed.timestamp != reference_timestamp:
                raise AssertionError(
                    "historical candle timestamp mismatch: "
                    f"aggregate={completed.timestamp} reference={reference_timestamp}"
                )

            source_candle = _python_candle(completed)
            reference_candle = _csv_candle(
                reference_row,
                product_id=product_id,
                timeframe="5m",
            )
            _assert_candle_equal(source_candle, reference_candle)

            source_signals = source_runtime.decide(source_candle)
            reference_signals = reference_runtime.decide(reference_candle)
            _update_digest(source_digest, source_signals)
            _update_digest(reference_digest, reference_signals)
            source_decision_count += len(source_signals)
            reference_decision_count += len(reference_signals)
            source_actionable_signal_count += sum(
                signal.type != SignalType.NO_SIGNAL for signal in source_signals
            )
            reference_actionable_signal_count += sum(
                signal.type != SignalType.NO_SIGNAL for signal in reference_signals
            )
            if _canonical_signals(source_signals) != _canonical_signals(
                reference_signals
            ):
                raise AssertionError(
                    "historical signal mismatch at "
                    f"{reference_timestamp}: "
                    f"aggregate={_canonical_signals(source_signals)} "
                    f"reference={_canonical_signals(reference_signals)}"
                )
            matched += 1
            reference_row = next(reference_rows, None)

        if reference_row is not None:
            remaining_timestamp = _timestamp_ms(reference_row["timestamp"])
            raise AssertionError(
                "source ended before reference was fully verified: "
                f"first_unverified_reference={remaining_timestamp}"
            )

    source_hexdigest = source_digest.hexdigest()
    reference_hexdigest = reference_digest.hexdigest()
    if source_hexdigest != reference_hexdigest:
        raise AssertionError(
            "historical signal digest mismatch: "
            f"aggregate={source_hexdigest} reference={reference_hexdigest}"
        )
    if matched == 0:
        raise ValueError("historical parity range contains no matching candles")
    return HistoricalParityReport(
        product_id=product_id,
        source_timeframe="1m",
        target_timeframe="5m",
        matched_candles=matched,
        skipped_reference_prefix_candles=skipped_reference_prefix,
        extra_source_candles=extra,
        source_decision_count=source_decision_count,
        reference_decision_count=reference_decision_count,
        source_actionable_signal_count=source_actionable_signal_count,
        reference_actionable_signal_count=reference_actionable_signal_count,
        source_signal_digest=source_hexdigest,
        reference_signal_digest=reference_hexdigest,
        source_sha256=source_file_sha256,
        reference_sha256=reference_file_sha256,
        strategy=strategy_identity,
    )


def run_shadow_evidence(
    redis_client: Any,
    *,
    source_stream_key: str,
    decision_stream_key: str,
    strategy: BaseStrategy,
    output: IO[str],
    duration_seconds: float,
    target_timeframe: str = "5m",
    monotonic: Callable[[], float] = time.monotonic,
) -> ShadowRunReport:
    """Record source candles and decisions from the official derived stream."""
    return _run_shadow_evidence(
        redis_client,
        source_stream_key=source_stream_key,
        decision_stream_key=decision_stream_key,
        runtime=_strategy_evidence_runtime(strategy),
        output=output,
        duration_seconds=duration_seconds,
        target_timeframe=target_timeframe,
        monotonic=monotonic,
    )


def run_portfolio_shadow_evidence(
    redis_client: Any,
    *,
    source_stream_key: str,
    decision_stream_key: str,
    portfolio: PortfolioDefinition,
    output: IO[str],
    duration_seconds: float,
    target_timeframe: str = "5m",
    monotonic: Callable[[], float] = time.monotonic,
) -> ShadowRunReport:
    """Record decision-only shadow evidence for one catalog portfolio."""
    return _run_shadow_evidence(
        redis_client,
        source_stream_key=source_stream_key,
        decision_stream_key=decision_stream_key,
        runtime=_portfolio_evidence_runtime(portfolio),
        output=output,
        duration_seconds=duration_seconds,
        target_timeframe=target_timeframe,
        monotonic=monotonic,
    )


def _run_shadow_evidence(
    redis_client: Any,
    *,
    source_stream_key: str,
    decision_stream_key: str,
    runtime: _DecisionEvidenceRuntime,
    output: IO[str],
    duration_seconds: float,
    target_timeframe: str,
    monotonic: Callable[[], float],
) -> ShadowRunReport:
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    if not source_stream_key or not decision_stream_key:
        raise ValueError("shadow stream keys must be non-empty")
    if source_stream_key == decision_stream_key:
        raise ValueError("shadow source and decision streams must be different")
    if target_timeframe != "5m":
        raise ValueError("shadow evidence currently requires a 5m target")
    if runtime.timeframe != target_timeframe:
        raise ValueError(
            "runtime timeframe does not match shadow target: "
            f"runtime={runtime.timeframe} target={target_timeframe}"
        )

    identity = runtime.identity
    source_digest = hashlib.sha256()
    completed_digest = hashlib.sha256()
    decision_digest = hashlib.sha256()
    deadline = monotonic() + duration_seconds
    last_ids = {
        source_stream_key: "$",
        decision_stream_key: "$",
    }
    source_count = 0
    completed_count = 0
    skipped_decision_prefix_count = 0
    signal_count = 0
    first_timestamp: int | None = None
    last_timestamp: int | None = None
    first_complete_bucket_timestamp: int | None = None
    last_completed_timestamp: int | None = None
    pending_decisions: list[Candlestick] = []

    def commit_ready_decisions() -> None:
        nonlocal completed_count
        nonlocal skipped_decision_prefix_count
        nonlocal signal_count
        while pending_decisions and first_complete_bucket_timestamp is not None:
            candle = pending_decisions[0]
            candle_payload = candle.model_dump(mode="json")
            if candle.timestamp < first_complete_bucket_timestamp:
                pending_decisions.pop(0)
                skipped_decision_prefix_count += 1
                _write_bundle_record(
                    output,
                    {
                        "schema_version": _SHADOW_SCHEMA_VERSION,
                        "type": "skipped_decision_prefix",
                        "candle": candle_payload,
                    },
                )
                continue
            if (
                last_timestamp is None
                or last_timestamp < candle.timestamp + _FIVE_MINUTES_MS
            ):
                return
            pending_decisions.pop(0)
            completed_count += 1
            signals = runtime.decide(candle)
            signal_payloads = [signal.model_dump(mode="json") for signal in signals]
            _update_payload_digest(completed_digest, candle_payload)
            _update_payload_digest(decision_digest, signal_payloads)
            _write_bundle_record(
                output,
                {
                    "schema_version": _SHADOW_SCHEMA_VERSION,
                    "type": "decision",
                    "candle": candle_payload,
                    "signals": signal_payloads,
                },
            )
            signal_count += sum(
                signal.type != SignalType.NO_SIGNAL for signal in signals
            )

    _write_bundle_record(
        output,
        {
            "schema_version": _SHADOW_SCHEMA_VERSION,
            "type": "session",
            "source_stream_key": source_stream_key,
            "decision_stream_key": decision_stream_key,
            "target_timeframe": target_timeframe,
            "strategy": identity.to_dict(),
        },
    )

    while monotonic() < deadline:
        remaining_ms = max(1, int((deadline - monotonic()) * 1000))
        response = redis_client.xread(
            last_ids,
            count=1,
            block=min(remaining_ms, 1_000),
        )
        if not response:
            continue
        observed_batches: list[tuple[str, Any]] = []
        for raw_stream, messages in response:
            observed_stream = _text(raw_stream)
            if observed_stream not in last_ids:
                raise RuntimeError(
                    f"shadow received unexpected stream: {observed_stream}"
                )
            observed_batches.append((observed_stream, messages))
        observed_batches.sort(key=lambda item: item[0] != source_stream_key)
        for observed_stream, messages in observed_batches:
            for raw_id, raw_fields in messages:
                last_ids[observed_stream] = _text(raw_id)
                candle = _stream_candle(raw_fields)
                if candle.product_id != runtime.product_id:
                    raise ValueError(
                        "shadow product mismatch: "
                        f"runtime={runtime.product_id} candle={candle.product_id}"
                    )
                candle_payload = candle.model_dump(mode="json")
                if observed_stream == source_stream_key:
                    if candle.timeframe != "1m":
                        raise ValueError(
                            "shadow source timeframe must be 1m: "
                            f"{candle.timeframe}"
                        )
                    if (
                        last_timestamp is not None
                        and candle.timestamp <= last_timestamp
                    ):
                        raise ValueError(
                            "shadow source timestamps must be strictly increasing"
                        )
                    source_count += 1
                    first_timestamp = first_timestamp or candle.timestamp
                    last_timestamp = candle.timestamp
                    first_complete_bucket_timestamp = (
                        first_complete_bucket_timestamp
                        or (
                            (candle.timestamp + _FIVE_MINUTES_MS - 1)
                            // _FIVE_MINUTES_MS
                        )
                        * _FIVE_MINUTES_MS
                    )
                    _update_payload_digest(source_digest, candle_payload)
                    _write_bundle_record(
                        output,
                        {
                            "schema_version": _SHADOW_SCHEMA_VERSION,
                            "type": "source_candle",
                            "candle": candle_payload,
                        },
                    )
                    commit_ready_decisions()
                    continue

                if candle.timeframe != target_timeframe:
                    raise ValueError(
                        "shadow decision timeframe mismatch: "
                        f"{candle.timeframe}"
                    )
                if (
                    last_completed_timestamp is not None
                    and candle.timestamp <= last_completed_timestamp
                ):
                    raise ValueError(
                        "shadow decision timestamps must be strictly increasing"
                    )
                last_completed_timestamp = candle.timestamp
                pending_decisions.append(candle)
                commit_ready_decisions()

    commit_ready_decisions()
    if pending_decisions:
        raise ValueError(
            "shadow evidence is incomplete: source watermark did not cover "
            "all observed decision candles"
        )
    if source_count == 0 or completed_count == 0:
        raise ValueError(
            "shadow evidence is insufficient: at least one source candle and "
            "one completed candle are required"
        )

    report = ShadowRunReport(
        source_stream_key=source_stream_key,
        decision_stream_key=decision_stream_key,
        product_id=runtime.product_id,
        target_timeframe=target_timeframe,
        source_candles=source_count,
        completed_candles=completed_count,
        skipped_decision_prefix_candles=skipped_decision_prefix_count,
        actionable_signal_count=signal_count,
        first_source_timestamp=first_timestamp,
        last_source_timestamp=last_timestamp,
        source_digest=source_digest.hexdigest(),
        completed_digest=completed_digest.hexdigest(),
        decision_digest=decision_digest.hexdigest(),
        strategy=identity,
    )
    _write_bundle_record(
        output,
        {
            "schema_version": _SHADOW_SCHEMA_VERSION,
            "type": "summary",
            "report": report.to_dict(),
        },
    )
    return report


def verify_shadow_evidence_bundle(
    bundle: IO[str],
    *,
    strategy_factory: Callable[[], BaseStrategy],
) -> ShadowRunReport:
    """Replay one completed shadow bundle and verify every recorded decision."""
    return _verify_shadow_evidence_bundle(
        bundle,
        runtime=_strategy_evidence_runtime(strategy_factory()),
    )


def verify_portfolio_shadow_evidence_bundle(
    bundle: IO[str],
    *,
    portfolio_factory: Callable[[], PortfolioDefinition],
) -> ShadowRunReport:
    """Replay one portfolio shadow bundle and verify every recorded decision."""
    return _verify_shadow_evidence_bundle(
        bundle,
        runtime=_portfolio_evidence_runtime(portfolio_factory()),
    )


def _verify_shadow_evidence_bundle(
    bundle: IO[str],
    *,
    runtime: _DecisionEvidenceRuntime,
) -> ShadowRunReport:
    records = [json.loads(line) for line in bundle if line.strip()]
    if len(records) < 2:
        raise ValueError("shadow evidence bundle is incomplete")
    session = records[0]
    summary = records[-1]
    if session.get("type") != "session" or summary.get("type") != "summary":
        raise ValueError("shadow evidence bundle boundaries are invalid")
    if any(
        record.get("schema_version") != _SHADOW_SCHEMA_VERSION for record in records
    ):
        raise ValueError("shadow evidence bundle schema is unsupported")

    identity = runtime.identity
    if session.get("strategy") != identity.to_dict():
        raise AssertionError("shadow evidence runtime identity mismatch")
    target_timeframe = session.get("target_timeframe")
    if not isinstance(target_timeframe, str):
        raise ValueError("shadow evidence target timeframe is missing")
    if runtime.timeframe != target_timeframe:
        raise ValueError("runtime timeframe does not match shadow evidence target")
    source_stream_key = session.get("source_stream_key")
    decision_stream_key = session.get("decision_stream_key")
    if not isinstance(source_stream_key, str) or not source_stream_key:
        raise ValueError("shadow evidence source stream key is missing")
    if not isinstance(decision_stream_key, str) or not decision_stream_key:
        raise ValueError("shadow evidence decision stream key is missing")
    if source_stream_key == decision_stream_key:
        raise ValueError("shadow evidence stream keys must be different")

    source_digest = hashlib.sha256()
    completed_digest = hashlib.sha256()
    decision_digest = hashlib.sha256()
    source_count = 0
    completed_count = 0
    skipped_decision_prefix_count = 0
    signal_count = 0
    first_timestamp: int | None = None
    last_timestamp: int | None = None
    first_complete_bucket_timestamp: int | None = None
    last_completed_timestamp: int | None = None

    for record in records[1:-1]:
        record_type = record.get("type")
        if record_type == "source_candle":
            candle_payload = record.get("candle")
            if not isinstance(candle_payload, dict):
                raise ValueError("shadow source candle payload is invalid")
            candle = Candlestick.model_validate(candle_payload)
            if candle.product_id != runtime.product_id:
                raise ValueError("shadow evidence product identity mismatch")
            if candle.timeframe != "1m":
                raise ValueError("shadow evidence source timeframe must be 1m")
            if last_timestamp is not None and candle.timestamp <= last_timestamp:
                raise ValueError(
                    "shadow evidence source timestamps must be strictly increasing"
                )
            source_count += 1
            first_timestamp = first_timestamp or candle.timestamp
            last_timestamp = candle.timestamp
            first_complete_bucket_timestamp = (
                first_complete_bucket_timestamp
                or (
                    (candle.timestamp + _FIVE_MINUTES_MS - 1)
                    // _FIVE_MINUTES_MS
                )
                * _FIVE_MINUTES_MS
            )
            _update_payload_digest(source_digest, candle_payload)
            continue
        if record_type not in {"decision", "skipped_decision_prefix"}:
            raise ValueError(f"unexpected shadow evidence record: {record_type}")
        candle_payload = record.get("candle")
        if not isinstance(candle_payload, dict):
            raise ValueError("shadow decision candle payload is invalid")
        candle = Candlestick.model_validate(candle_payload)
        if candle.product_id != runtime.product_id:
            raise ValueError("shadow evidence product identity mismatch")
        if candle.timeframe != target_timeframe:
            raise ValueError("shadow evidence decision timeframe mismatch")
        if (
            last_completed_timestamp is not None
            and candle.timestamp <= last_completed_timestamp
        ):
            raise ValueError(
                "shadow evidence decision timestamps must be strictly increasing"
            )
        last_completed_timestamp = candle.timestamp
        is_prefix = (
            first_complete_bucket_timestamp is None
            or candle.timestamp < first_complete_bucket_timestamp
        )
        if record_type == "skipped_decision_prefix":
            if not is_prefix:
                raise AssertionError("shadow skipped a fully captured decision")
            skipped_decision_prefix_count += 1
            continue
        if is_prefix:
            raise AssertionError("shadow decision lacks complete source coverage")
        if (
            last_timestamp is None
            or last_timestamp < candle.timestamp + _FIVE_MINUTES_MS
        ):
            raise AssertionError(
                "shadow decision precedes its source watermark"
            )
        expected_signals = [
            signal.model_dump(mode="json") for signal in runtime.decide(candle)
        ]
        signal_payloads = record.get("signals")
        if signal_payloads != expected_signals:
            raise AssertionError("shadow strategy decision mismatch")
        completed_count += 1
        signal_count += sum(
            signal.get("type") != SignalType.NO_SIGNAL.value
            for signal in expected_signals
        )
        _update_payload_digest(completed_digest, candle_payload)
        _update_payload_digest(decision_digest, expected_signals)

    if source_count == 0 or completed_count == 0:
        raise ValueError(
            "shadow evidence is insufficient: at least one source candle and "
            "one completed candle are required"
        )
    observed = ShadowRunReport(
        source_stream_key=source_stream_key,
        decision_stream_key=decision_stream_key,
        product_id=runtime.product_id,
        target_timeframe=target_timeframe,
        source_candles=source_count,
        completed_candles=completed_count,
        skipped_decision_prefix_candles=skipped_decision_prefix_count,
        actionable_signal_count=signal_count,
        first_source_timestamp=first_timestamp,
        last_source_timestamp=last_timestamp,
        source_digest=source_digest.hexdigest(),
        completed_digest=completed_digest.hexdigest(),
        decision_digest=decision_digest.hexdigest(),
        strategy=identity,
    )
    if summary.get("report") != observed.to_dict():
        raise AssertionError("shadow evidence summary mismatch")
    return observed


def _timestamp_ms(value: str) -> int:
    if value.isdigit():
        numeric = int(value)
        return numeric * 1_000 if numeric < 1_000_000_000_000 else numeric
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("candle timestamp must include a timezone")
    return int(parsed.astimezone(UTC).timestamp() * 1_000)


def _rust_candle(
    row: dict[str, str],
    product_id: str,
    timestamp: int,
) -> RustCandlestick:
    return RustCandlestick(
        product_id=product_id,
        timeframe="1m",
        timestamp=timestamp,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
    )


def _rust_candle_from_python(candle: Candlestick) -> RustCandlestick:
    return RustCandlestick(
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        timestamp=candle.timestamp,
        open=str(candle.open),
        high=str(candle.high),
        low=str(candle.low),
        close=str(candle.close),
        volume=str(candle.volume),
    )


def _python_candle(candle: Any) -> Candlestick:
    return Candlestick(
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        timestamp=candle.timestamp,
        open=Decimal(str(candle.open)),
        high=Decimal(str(candle.high)),
        low=Decimal(str(candle.low)),
        close=Decimal(str(candle.close)),
        volume=Decimal(str(candle.volume)),
    )


def _csv_candle(
    row: dict[str, str],
    *,
    product_id: str,
    timeframe: str,
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=_timestamp_ms(row["timestamp"]),
        open=Decimal(row["open"]),
        high=Decimal(row["high"]),
        low=Decimal(row["low"]),
        close=Decimal(row["close"]),
        volume=Decimal(row["volume"]),
    )


def _assert_candle_equal(actual: Candlestick, expected: Candlestick) -> None:
    actual_values = (actual.timestamp,) + tuple(
        getattr(actual, field) for field in _CANDLE_FIELDS
    )
    expected_values = (expected.timestamp,) + tuple(
        getattr(expected, field) for field in _CANDLE_FIELDS
    )
    if actual_values != expected_values:
        raise AssertionError(
            "historical candle value mismatch at "
            f"{expected.timestamp}: aggregate={actual_values} "
            f"reference={expected_values}"
        )


def _signals(strategy: BaseStrategy, candle: Candlestick) -> list[Signal]:
    result: object = strategy.on_candle(candle)
    if result is None:
        return []
    if isinstance(result, Signal):
        return [result]
    if isinstance(result, list) and all(
        isinstance(signal, Signal) for signal in result
    ):
        return result
    raise TypeError("strategy.on_candle() must return None, Signal, or list[Signal]")


def _canonical_signals(signals: Iterable[Signal]) -> tuple[str, ...]:
    return tuple(
        json.dumps(
            signal.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        for signal in signals
    )


def _update_digest(digest: Any, signals: Iterable[Signal]) -> None:
    for payload in _canonical_signals(signals):
        digest.update(payload.encode())
        digest.update(b"\n")


def strategy_evidence_identity(
    strategy: BaseStrategy,
) -> StrategyEvidenceIdentity:
    strategy_class = type(strategy)
    replay_configuration = json.dumps(
        strategy.replay_configuration(),
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return StrategyEvidenceIdentity(
        strategy_id=strategy.strategy_id,
        class_name=strategy_class.__name__,
        display_name=getattr(
            strategy_class,
            "__fluxtrade_display_name__",
            None,
        ),
        artifact_version=getattr(
            strategy_class,
            "__fluxtrade_artifact_version__",
            None,
        ),
        readiness=getattr(
            strategy_class,
            "__fluxtrade_readiness__",
            None,
        ),
        catalog_sha256=getattr(
            strategy_class,
            "__fluxtrade_catalog_sha256__",
            None,
        ),
        replay_configuration_sha256=hashlib.sha256(replay_configuration).hexdigest(),
    )


def require_verified_strategy_identity(
    strategy: BaseStrategy,
) -> StrategyEvidenceIdentity:
    identity = strategy_evidence_identity(strategy)
    _require_complete_evidence_identity(identity)
    return identity


def portfolio_evidence_identity(
    definition: PortfolioDefinition,
) -> StrategyEvidenceIdentity:
    replay_configuration = json.dumps(
        portfolio_replay_configuration(definition),
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return StrategyEvidenceIdentity(
        strategy_id=definition.portfolio_id,
        class_name=type(definition).__name__,
        display_name=definition.display_name,
        artifact_version=definition.artifact_version,
        readiness=definition.readiness,
        catalog_sha256=definition.catalog_sha256,
        replay_configuration_sha256=hashlib.sha256(
            replay_configuration
        ).hexdigest(),
    )


def require_verified_portfolio_identity(
    definition: PortfolioDefinition,
) -> StrategyEvidenceIdentity:
    identity = portfolio_evidence_identity(definition)
    _require_complete_evidence_identity(identity)
    return identity


def _strategy_evidence_runtime(
    strategy: BaseStrategy,
) -> _StrategyDecisionEvidenceRuntime:
    return _StrategyDecisionEvidenceRuntime(
        strategy=strategy,
        identity=require_verified_strategy_identity(strategy),
    )


def _portfolio_evidence_runtime(
    definition: PortfolioDefinition,
) -> _PortfolioDecisionEvidenceRuntime:
    coordinator = PortfolioCoordinator()
    coordinator.register(definition)
    return _PortfolioDecisionEvidenceRuntime(
        definition=definition,
        identity=require_verified_portfolio_identity(definition),
        coordinator=coordinator,
    )


def _require_complete_evidence_identity(
    identity: StrategyEvidenceIdentity,
) -> None:
    required = {
        "display_name": identity.display_name,
        "artifact_version": identity.artifact_version,
        "readiness": identity.readiness,
        "catalog_sha256": identity.catalog_sha256,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(
            "pre-live evidence requires a catalog-loaded strategy with "
            f"complete provenance; missing={','.join(missing)}"
        )
    if (
        identity.catalog_sha256 is None
        or len(identity.catalog_sha256) != 64
        or any(
            character not in "0123456789abcdef" for character in identity.catalog_sha256
        )
    ):
        raise ValueError("strategy catalog SHA-256 provenance is invalid")
    if identity.readiness not in StrategyLoader.READINESS_VALUES:
        raise ValueError("runtime readiness provenance is invalid")


@contextmanager
def _verified_csv_snapshot(path: Path) -> Iterator[tuple[IO[str], str]]:
    digest = hashlib.sha256()
    with (
        path.open("rb") as source,
        tempfile.SpooledTemporaryFile(
            max_size=64 * 1024 * 1024, mode="w+b"
        ) as snapshot,
    ):
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            snapshot.write(chunk)
        snapshot.seek(0)
        text = io.TextIOWrapper(snapshot, encoding="utf-8", newline="")
        try:
            yield text, digest.hexdigest()
        finally:
            text.detach()


def _write_bundle_record(output: IO[str], payload: dict[str, object]) -> None:
    output.write(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    output.flush()


def _update_payload_digest(digest: Any, payload: object) -> None:
    digest.update(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    digest.update(b"\n")


def _stream_candle(raw_fields: Any) -> Candlestick:
    fields = {_text(key): _text(value) for key, value in dict(raw_fields).items()}
    payload = fields.get("json")
    if payload is None:
        raise ValueError("shadow stream entry is missing json payload")
    return Candlestick.model_validate_json(payload)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
