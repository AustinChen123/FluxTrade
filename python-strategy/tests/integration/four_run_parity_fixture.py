"""Deterministic real-pipeline fixture for the four-run parity gate."""

from __future__ import annotations

import hashlib
import importlib
import importlib.machinery
import json
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Literal
from unittest.mock import MagicMock
from uuid import UUID

from pytest import MonkeyPatch
from sqlalchemy import create_engine, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session, sessionmaker

import src.core.adapters.simulated as simulated_module
import src.core.order_manager as order_manager_module
from src.core.adapters.simulated import SimulatedAdapter
from src.core.backtest_runner import BacktestRunner
from src.core.clock import BacktestClock
from src.core.consumer import (
    DataConsumer,
    FENCED_EPHEMERAL_GROUP_CLEANUP,
    FENCED_XACK,
    FENCED_XREADGROUP,
    RELEASE_OWNERSHIP_LEASE,
    RENEW_OWNERSHIP_LEASE,
)
from src.core.data_sources.memory import MemoryDataSource
from src.core.decimal_math import canonical_decimal_text
from src.core.engine import StrategyEngine
from src.core.mocks.account_service import BacktestAccountService
from src.core.models import Candlestick, Signal, SignalType, Trade as MarketTrade
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Order,
    Position,
    Product,
    SignalAudit,
    Strategy,
    SystemEvent,
    Trade,
)
from src.core.repositories import LiveOrderRepository
from src.core.runtime_environment import RuntimeEnvironment
from src.strategies.callable_strategy import CallableStrategy
from src.validation.backtest_capture import (
    build_normal_backtest_trading_outcome,
    capture_signal_batch,
)
from src.validation.live_like_capture import LiveLikeOutcomeCapture
from src.validation.trading_outcome import SignalObservation, TradingOutcome
from src.validation.trading_parity import TradingParityRun
from src.validation.trading_parity_matrix import (
    FourRunParityReport,
    compare_four_run_parity,
)

PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "d0b4_four_run"
TIMEFRAME = "1m"
INITIAL_BALANCE = Decimal("10000")
MAKER_FEE = Decimal("0")
TAKER_FEE = Decimal("0.001")
TIMESTAMPS = (
    1_800_000_000_000,
    1_800_000_060_000,
    1_800_000_120_000,
    1_800_000_180_000,
)
PARENT_SHA = "6494c2aa3d436f57c4c5466320d5e7a25c4b8a0a"
PARENT_TREE = "96e1c296dd0c506d12a3ae2edf59c538f3e89b3e"
FEATURE_COMMIT_SHA = "98b453ee5ae21e08bd46fcbef9b6984370cdf8ef"
PARENT_MANIFEST_ENTRIES = 171
PARENT_MANIFEST_BYTES = 17_008
PARENT_MANIFEST_SHA256 = (
    "707dedc8764a7bd964a3869049b4644b6f9b1f8e86951fd4f6760dad6b6ae081"
)
PRODUCT_ROOTS = (
    "python-strategy/src",
    "python-strategy/pyproject.toml",
    "python-strategy/uv.lock",
    "rust-data-service/src",
    "rust-data-service/Cargo.toml",
    "rust-data-service/Cargo.lock",
)
BACKTEST_RUNTIME_PATHS = (
    "python-strategy/src/core/backtest_runner.py",
    "python-strategy/src/core/engine.py",
    "python-strategy/src/core/signal_processor.py",
    "python-strategy/src/core/adapters/simulated.py",
    "python-strategy/src/core/repositories.py",
    "python-strategy/src/validation/backtest_capture.py",
)
LIVE_LIKE_RUNTIME_PATHS = (
    "python-strategy/src/core/consumer.py",
    "python-strategy/src/core/engine.py",
    "python-strategy/src/core/signal_processor.py",
    "python-strategy/src/core/adapters/simulated.py",
    "python-strategy/src/core/repositories.py",
    "python-strategy/src/validation/live_like_capture.py",
)
CALLABLE_STRATEGY_PATH = "python-strategy/src/strategies/callable_strategy.py"
_ROOT = Path(__file__).resolve().parents[3]


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(
    type_: object,
    compiler: object,
    **kw: object,
) -> str:
    return "JSON"


@dataclass(frozen=True, slots=True)
class RunEvidence:
    label: str
    mode: Literal["live_like", "backtest"]
    owner: str
    observed_batches: int
    persisted_orders: int
    persisted_fills: int
    raw_ids: tuple[str, ...]
    processed_deliveries: int
    acked: tuple[str, ...]
    pending: int
    cleanup_calls: int


@dataclass(frozen=True, slots=True)
class CollectedOutcome:
    outcome: TradingOutcome
    evidence: RunEvidence


@dataclass(frozen=True, slots=True)
class CollectedFourRuns:
    cells: tuple[
        CollectedOutcome,
        CollectedOutcome,
        CollectedOutcome,
        CollectedOutcome,
    ]


@dataclass(frozen=True, slots=True)
class FourRunMatrix:
    runs: tuple[
        TradingParityRun,
        TradingParityRun,
        TradingParityRun,
        TradingParityRun,
    ]
    evidence: tuple[RunEvidence, RunEvidence, RunEvidence, RunEvidence]
    report: FourRunParityReport
    product_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class _RawIds:
    order_and_trade: tuple[str, str, str, str]
    exchange: tuple[str, str]


_RAW_IDS = (
    _RawIds(
        (
            "ffffffff-ffff-ffff-ffff-fffffffffff1",
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeee1",
            "00000000-0000-0000-0000-000000000001",
            "11111111-1111-1111-1111-111111111111",
        ),
        (
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1",
        ),
    ),
    _RawIds(
        (
            "dddddddd-dddd-dddd-dddd-ddddddddddd1",
            "cccccccc-cccc-cccc-cccc-ccccccccccc1",
            "00000000-0000-0000-0000-000000000002",
            "22222222-2222-2222-2222-222222222222",
        ),
        (
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2",
        ),
    ),
    _RawIds(
        (
            "99999999-9999-9999-9999-999999999991",
            "88888888-8888-8888-8888-888888888881",
            "00000000-0000-0000-0000-000000000003",
            "33333333-3333-3333-3333-333333333333",
        ),
        (
            "77777777-7777-7777-7777-777777777771",
            "66666666-6666-6666-6666-666666666661",
        ),
    ),
    _RawIds(
        (
            "55555555-5555-5555-5555-555555555551",
            "44444444-4444-4444-4444-444444444441",
            "00000000-0000-0000-0000-000000000004",
            "33333333-3333-3333-3333-333333333334",
        ),
        (
            "22222222-2222-2222-2222-222222222221",
            "11111111-1111-1111-1111-111111111112",
        ),
    ),
)


class _UuidSequence:
    def __init__(self, values: tuple[str, ...]) -> None:
        self._values = iter(UUID(value) for value in values)

    def __call__(self) -> UUID:
        return next(self._values)


class _DeterministicRedis:
    def __init__(
        self,
        messages: tuple[tuple[str, dict[str, str]], ...],
        trace: list[tuple[str, object]],
    ) -> None:
        self.messages = list(messages)
        self.owner: str | None = None
        self.pending = 0
        self.acked: list[str] = []
        self.trace = trace
        self.registered_streams: set[str] = set()
        self.quarantined: set[str] = set()
        self.cleanup_calls = 0

    def set(self, _key: str, value: str, *, nx: bool, px: int) -> bool:
        assert nx is True and px > 0
        if self.owner is not None:
            return False
        self.owner = value
        return True

    def get(self, _key: str) -> str | None:
        return self.owner

    def eval(self, script: str, *args: object) -> object:
        if script == RENEW_OWNERSHIP_LEASE:
            return 1 if self.owner == args[2] else 0
        if script == RELEASE_OWNERSHIP_LEASE:
            if self.owner == args[2]:
                self.owner = None
                return 1
            return 0
        if script == FENCED_XREADGROUP:
            if self.owner != args[3]:
                return [0]
            if not self.messages:
                return [1]
            message_id, fields = self.messages.pop(0)
            self.pending += 1
            self.trace.append(("claim", message_id))
            flattened = [item for key, value in fields.items() for item in (key, value)]
            return [1, [[args[2], [[message_id, flattened]]]]]
        if script == FENCED_XACK:
            if self.owner != args[2]:
                return [0]
            message_id = str(args[5])
            self.pending -= 1
            self.acked.append(message_id)
            self.trace.append(("ack", message_id))
            return [1, 1]
        if script == FENCED_EPHEMERAL_GROUP_CLEANUP:
            if self.owner != args[-2]:
                return [0]
            self.cleanup_calls += 1
            return [1, self.pending, 4 if self.pending else 0]
        raise AssertionError("unexpected Redis script")

    def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str,
        mkstream: bool,
    ) -> bool:
        assert stream and group and id == "$" and mkstream is True
        return True

    def xpending(self, _stream: str, _group: str) -> dict[str, int]:
        return {"pending": self.pending}

    def scan_iter(self, *, match: str, _type: str):
        assert match == "stream:market:*" and _type == "stream"
        return iter(())

    def xinfo_groups(self, _stream: str) -> list[object]:
        return []

    def smembers(self, key: str) -> set[str]:
        return (
            self.quarantined if key.endswith(":quarantine") else self.registered_streams
        )

    def sadd(self, key: str, *values: str) -> int:
        target = (
            self.quarantined if key.endswith(":quarantine") else self.registered_streams
        )
        before = len(target)
        target.update(values)
        return len(target) - before

    def time(self) -> tuple[int, int]:
        return 1_800_000_200, 0

    def close(self) -> None:
        return None


def _git(*args: str) -> bytes:
    completed = subprocess.run(
        ("git", *args),
        cwd=_ROOT,
        check=True,
        capture_output=True,
    )
    return completed.stdout


def _manifest_bytes(revision: str) -> bytes:
    return _git("ls-tree", "-r", revision, "--", *PRODUCT_ROOTS)


def _commit_tree_and_parents(revision: str) -> tuple[str, list[str]]:
    tree: str | None = None
    parents: list[str] = []
    for line in _git("cat-file", "commit", revision).splitlines():
        if not line:
            break
        key, separator, value = line.partition(b" ")
        if not separator:
            continue
        if key == b"tree":
            tree = value.decode("ascii")
        elif key == b"parent":
            parents.append(value.decode("ascii"))
    if tree is None:
        raise ValueError("Git commit object is missing its tree")
    return tree, parents


def _reviewed_candidate_parent(candidate_sha: str) -> str:
    return PARENT_SHA if candidate_sha == FEATURE_COMMIT_SHA else FEATURE_COMMIT_SHA


def validate_frozen_product_manifest(manifest: bytes) -> str:
    if type(manifest) is not bytes:
        raise ValueError("product manifest must be exact bytes")
    digest = hashlib.sha256(manifest).hexdigest()
    if (
        manifest.count(b"\n") != PARENT_MANIFEST_ENTRIES
        or len(manifest) != PARENT_MANIFEST_BYTES
        or digest != PARENT_MANIFEST_SHA256
    ):
        raise ValueError("product runtime manifest differs from reviewed parent")
    return digest


def _manifest_records(manifest: bytes) -> dict[str, str]:
    records: dict[str, str] = {}
    for raw_line in manifest.splitlines():
        header, raw_path = raw_line.split(b"\t", 1)
        mode, kind, raw_oid = header.split(b" ", 2)
        if kind != b"blob" or mode not in {b"100644", b"100755", b"120000"}:
            raise ValueError("product manifest contains unsupported entry")
        path = raw_path.decode("utf-8")
        oid = raw_oid.decode("ascii")
        if path in records:
            raise ValueError("product manifest contains duplicate path")
        records[path] = oid
    return records


def _git_blob_oid(path: Path, mode: str) -> str:
    if mode == "120000":
        content = os.readlink(path).encode()
    else:
        content = path.read_bytes()
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def verify_reviewed_product_runtime() -> str:
    if _git("rev-parse", "--show-object-format").strip() != b"sha1":
        raise ValueError("D0B4B requires the reviewed SHA-1 Git object format")
    head = _git("rev-parse", "HEAD").strip().decode()
    tree, parents = _commit_tree_and_parents("HEAD")
    if head == PARENT_SHA:
        if tree != PARENT_TREE:
            raise ValueError("reviewed parent tree differs")
    else:
        if parents != [_reviewed_candidate_parent(head)] or tree == PARENT_TREE:
            raise ValueError("D0B4B candidate identity differs from reviewed scope")
    return _verify_current_product_runtime(_manifest_bytes("HEAD"))


def _verify_current_product_runtime(manifest: bytes) -> str:
    digest = validate_frozen_product_manifest(manifest)
    if subprocess.run(
        ("git", "diff", "--quiet", "HEAD", "--", *PRODUCT_ROOTS),
        cwd=_ROOT,
        check=False,
    ).returncode:
        raise ValueError("tracked product worktree differs from candidate HEAD")
    if subprocess.run(
        ("git", "diff", "--cached", "--quiet", "HEAD", "--", *PRODUCT_ROOTS),
        cwd=_ROOT,
        check=False,
    ).returncode:
        raise ValueError("product index differs from candidate HEAD")
    if _git("ls-files", "--others", "--exclude-standard", "--", *PRODUCT_ROOTS):
        raise ValueError("untracked product runtime path is present")
    for raw_line in manifest.splitlines():
        header, raw_path = raw_line.split(b"\t", 1)
        mode, _kind, raw_oid = header.split(b" ", 2)
        path = raw_path.decode("utf-8")
        if _git_blob_oid(_ROOT / path, mode.decode("ascii")) != raw_oid.decode("ascii"):
            raise ValueError("loaded product filesystem differs from candidate blob")
    return digest


def committed_candidate_available() -> bool:
    return _git("rev-parse", "HEAD").strip().decode() != PARENT_SHA


def _candidate_identity() -> tuple[str, str, dict[str, str], str]:
    candidate_sha = _git("rev-parse", "HEAD").strip().decode()
    if candidate_sha == PARENT_SHA:
        raise ValueError("D0B4B candidate commit does not exist yet")
    candidate_tree, parents = _commit_tree_and_parents("HEAD")
    if parents != [_reviewed_candidate_parent(candidate_sha)]:
        raise ValueError("D0B4B candidate must have the reviewed exact parent")
    if candidate_tree == PARENT_TREE:
        raise ValueError("D0B4B candidate tree must be distinct")
    manifest = _manifest_bytes("HEAD")
    digest = _verify_current_product_runtime(manifest)
    return candidate_sha, candidate_tree, _manifest_records(manifest), digest


def _canonical_sha(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def semantic_identity_sha256() -> tuple[str, str]:
    candles = _candles()
    decisions = tuple(_predict(candle) for candle in candles)

    def decision_payload(decision: Signal | None) -> dict[str, object] | None:
        if decision is None:
            return None
        if type(decision.quantity) is not Decimal:
            raise ValueError("D0B4B entry decisions require exact Decimal quantity")
        return {
            "strategy_id": decision.strategy_id,
            "product_id": decision.product_id,
            "timeframe": decision.timeframe,
            "timestamp": decision.timestamp,
            "type": decision.type.value,
            "quantity": canonical_decimal_text(decision.quantity),
        }

    input_sha = _canonical_sha(
        {
            "candles": [
                {
                    "product_id": candle.product_id,
                    "timeframe": candle.timeframe,
                    "timestamp": candle.timestamp,
                    "open": canonical_decimal_text(candle.open),
                    "high": canonical_decimal_text(candle.high),
                    "low": canonical_decimal_text(candle.low),
                    "close": canonical_decimal_text(candle.close),
                    "volume": canonical_decimal_text(candle.volume),
                }
                for candle in candles
            ],
            "decisions": [decision_payload(decision) for decision in decisions],
        }
    )
    configuration_sha = _canonical_sha(
        {
            "initial_balance": canonical_decimal_text(INITIAL_BALANCE),
            "maker_fee": canonical_decimal_text(MAKER_FEE),
            "product_id": PRODUCT_ID,
            "taker_fee": canonical_decimal_text(TAKER_FEE),
            "timeframe": TIMEFRAME,
        }
    )
    return input_sha, configuration_sha


def _identity_values(records: dict[str, str]) -> dict[str, str]:
    input_sha, configuration_sha = semantic_identity_sha256()

    def owner_manifest(paths: Sequence[str], owner: str) -> str:
        return _canonical_sha(
            {
                "owner": owner,
                "paths": [[path, records[path]] for path in paths],
            }
        )

    callable_path = Path(
        importlib.import_module(CallableStrategy.__module__).__file__ or ""
    )
    expected_callable = _ROOT / CALLABLE_STRATEGY_PATH
    if callable_path.resolve() != expected_callable.resolve():
        raise ValueError("loaded CallableStrategy owner path differs")
    loaded_artifact = hashlib.sha256(callable_path.read_bytes()).hexdigest()
    native_matcher = native_matcher_sha256()
    return {
        "input": input_sha,
        "configuration": configuration_sha,
        "loaded_artifact": loaded_artifact,
        "native_matcher": native_matcher,
        "backtest_runtime": owner_manifest(
            BACKTEST_RUNTIME_PATHS,
            "BacktestRunner",
        ),
        "backtest_runner": owner_manifest(
            ("python-strategy/src/core/backtest_runner.py",),
            "BacktestRunner.run",
        ),
        "live_like_runtime": owner_manifest(
            LIVE_LIKE_RUNTIME_PATHS,
            "DataConsumer+LiveLikeOutcomeCapture",
        ),
        "live_like_runner": owner_manifest(
            (
                "python-strategy/src/core/consumer.py",
                "python-strategy/src/validation/live_like_capture.py",
            ),
            "LiveLikeOutcomeCapture.run",
        ),
    }


def selected_native_matcher_module() -> ModuleType:
    top_level = importlib.import_module("fluxtrade_core")
    if getattr(top_level, "__path__", None) is not None:
        selected = importlib.import_module("fluxtrade_core.fluxtrade_core")
    else:
        selected = top_level
    path_value = selected.__file__
    if type(path_value) is not str or not any(
        path_value.endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES
    ):
        raise ValueError("selected matcher module is not a native extension")
    if getattr(top_level, "PyMatchingEngine", None) is not getattr(
        selected,
        "PyMatchingEngine",
        None,
    ):
        raise ValueError("top-level matcher does not export the selected native class")
    return selected


def native_matcher_sha256() -> str:
    module = selected_native_matcher_module()
    assert type(module.__file__) is str
    return hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()


def _candles(final_price: str = "103") -> tuple[Candlestick, ...]:
    prices = ("100", "101", "102", final_price)
    return tuple(
        Candlestick(
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=timestamp,
            open=Decimal(price),
            high=Decimal(price),
            low=Decimal(price),
            close=Decimal(price),
            volume=Decimal("1"),
        )
        for timestamp, price in zip(TIMESTAMPS, prices, strict=True)
    )


def _predict(candle: Candlestick) -> Signal | None:
    if candle.timestamp == TIMESTAMPS[0]:
        signal_type = SignalType.LONG
    elif candle.timestamp == TIMESTAMPS[2]:
        signal_type = SignalType.EXIT_LONG
    else:
        return None
    return Signal(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        timestamp=candle.timestamp,
        type=signal_type,
        quantity=Decimal("1"),
    )


def _install_uuid_sequences(monkeypatch: MonkeyPatch, raw_ids: _RawIds) -> None:
    monkeypatch.setattr(
        order_manager_module,
        "uuid",
        SimpleNamespace(uuid4=_UuidSequence(raw_ids.order_and_trade)),
    )
    monkeypatch.setattr(
        simulated_module,
        "uuid",
        SimpleNamespace(uuid4=_UuidSequence(raw_ids.exchange)),
    )


def _shutdown_engine(engine: StrategyEngine | None) -> None:
    if engine is not None:
        engine.shutdown(timeout=1)


def _backtest_database(path: Path) -> tuple[Engine, sessionmaker[Session]]:
    database_engine = create_engine(
        f"sqlite:///{path}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in (
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ):
        table.create(database_engine, checkfirst=True)
    factory = sessionmaker(bind=database_engine, expire_on_commit=False)
    with factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.commit()
    return database_engine, factory


def _collect_backtest(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    *,
    label: str,
    raw_ids: _RawIds,
    final_price: str = "103",
) -> CollectedOutcome:
    _install_uuid_sequences(monkeypatch, raw_ids)
    engine_redis = MagicMock()
    engine_redis.close.return_value = None
    monkeypatch.setattr("src.core.engine.create_redis_client", lambda: engine_redis)
    database_engine, factory = _backtest_database(tmp_path / f"{label}.db")
    observed_batches: list[tuple[SignalObservation, ...]] = []

    def observe(batch: tuple[Signal, ...]) -> None:
        observed_batches.append(capture_signal_batch(batch))

    runner = BacktestRunner(
        start_time=TIMESTAMPS[0],
        end_time=TIMESTAMPS[-1],
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=INITIAL_BALANCE,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(list(_candles(final_price))),
        fee_config={"maker": MAKER_FEE, "taker": TAKER_FEE},
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=factory,
        signal_batch_observer=observe,
    )
    runner.add_strategy(CallableStrategy(STRATEGY_ID, _predict, PRODUCT_ID, TIMEFRAME))
    try:
        result = runner.run()
        if type(result) is not dict:
            raise ValueError("BacktestRunner did not return its result payload")
        with factory() as session:
            rows = tuple(
                session.scalars(
                    select(BacktestTradeLog).order_by(
                        BacktestTradeLog.timestamp,
                        BacktestTradeLog.fill_sequence,
                        BacktestTradeLog.id,
                    )
                ).all()
            )
            persisted_fills = tuple(
                {
                    "id": row.id,
                    "strategy_id": row.strategy_id,
                    "order_id": row.order_id,
                    "exchange_trade_id": row.exchange_trade_id,
                    "product_id": row.product_id,
                    "side": row.side,
                    "price": row.price,
                    "quantity": row.quantity,
                    "fee": row.fee,
                    "fee_asset": row.fee_asset,
                    "timestamp": row.timestamp,
                    "fill_sequence": row.fill_sequence,
                }
                for row in rows
            )
            audit_count = session.scalar(select(func.count()).select_from(SignalAudit))
        signals = tuple(signal for batch in observed_batches for signal in batch)
        outcome = build_normal_backtest_trading_outcome(
            signals=signals,
            fills=persisted_fills,
            journal=tuple(result["journal"]),
            endpoint_state=result["endpoint_state"],
            initial_balance=runner.initial_balance,
            total_pnl=result["total_pnl"],
        )
        raw_values = tuple(
            str(value)
            for row in rows
            for value in (row.id, row.order_id, row.exchange_trade_id)
        )
        evidence = RunEvidence(
            label=label,
            mode="backtest",
            owner="BacktestRunner",
            observed_batches=len(observed_batches),
            persisted_orders=int(audit_count or 0),
            persisted_fills=len(rows),
            raw_ids=raw_values,
            processed_deliveries=len(observed_batches),
            acked=(),
            pending=0,
            cleanup_calls=0,
        )
        return CollectedOutcome(outcome=outcome, evidence=evidence)
    finally:
        _shutdown_engine(runner.engine)
        database_engine.dispose()


def _live_database(path: Path) -> tuple[Engine, sessionmaker[Session]]:
    database_engine = create_engine(
        f"sqlite:///{path}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in (
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        Order.__table__,
        Trade.__table__,
        Position.__table__,
        SignalAudit.__table__,
        SystemEvent.__table__,
    ):
        table.create(database_engine, checkfirst=True)
    factory = sessionmaker(bind=database_engine, expire_on_commit=False)
    with factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.add(Strategy(id=STRATEGY_ID, name=STRATEGY_ID))
        session.commit()
    return database_engine, factory


def _payload(candle: Candlestick) -> dict[str, str]:
    return {"json": candle.model_dump_json()}


def _collect_live_like(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    *,
    label: str,
    raw_ids: _RawIds,
    final_price: str = "103",
) -> CollectedOutcome:
    _install_uuid_sequences(monkeypatch, raw_ids)
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-d0b4")
    database_engine, factory = _live_database(tmp_path / f"{label}.db")
    capture = LiveLikeOutcomeCapture(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT_ID,
        initial_balance=INITIAL_BALANCE,
        expected_deliveries=len(TIMESTAMPS),
        completion_timeout_seconds=2,
        shutdown_timeout_seconds=2,
    )
    adapter = SimulatedAdapter(
        initial_balance=INITIAL_BALANCE,
        maker_fee=MAKER_FEE,
        taker_fee=TAKER_FEE,
    )
    engine_redis = MagicMock()
    engine_redis.close.return_value = None
    monkeypatch.setattr("src.core.engine.create_redis_client", lambda: engine_redis)
    clock = BacktestClock()
    engine = StrategyEngine(
        None,
        clock,
        order_repository=LiveOrderRepository(db_session_factory=factory),
        account_service=BacktestAccountService(adapter=adapter),
        adapter=adapter,
        journal=capture.journal,
        db_session_factory=factory,
        is_backtest=True,
        signal_batch_observer=capture.observe_signal_batch,
    )
    engine.add_strategy(CallableStrategy(STRATEGY_ID, _predict, PRODUCT_ID, TIMEFRAME))
    trace: list[tuple[str, object]] = []
    candles = _candles(final_price)
    transport = _DeterministicRedis(
        tuple((f"{candle.timestamp}-0", _payload(candle)) for candle in candles),
        trace,
    )
    monkeypatch.setattr("src.core.consumer.create_redis_client", lambda: transport)

    def consumer_factory(
        *,
        channels: list[str],
        on_message_callback: Callable[[Candlestick | MarketTrade], None],
        pending_replay_callback: Callable[[Candlestick | MarketTrade], None] | None,
        runtime_environment: RuntimeEnvironment,
        group_name: str,
        ephemeral_group: bool,
    ) -> DataConsumer:
        return DataConsumer(
            channels=channels,
            on_message_callback=on_message_callback,
            pending_replay_callback=pending_replay_callback,
            runtime_environment=runtime_environment,
            group_name=group_name,
            ephemeral_group=ephemeral_group,
        )

    try:
        outcome = capture.run(
            engine=engine,
            adapter=adapter,
            clock=clock,
            session_factory=factory,
            channels=[f"stream:market:binance:btcusdt-perp:{TIMEFRAME}"],
            group_name=f"d0b4_{label}",
            consumer_factory=consumer_factory,
        )
        with factory() as session:
            orders = tuple(
                session.scalars(select(Order).order_by(Order.timestamp, Order.id)).all()
            )
            trades = tuple(
                session.scalars(select(Trade).order_by(Trade.timestamp, Trade.id)).all()
            )
        raw_values = tuple(
            [str(order.id) for order in orders]
            + [str(trade.id) for trade in trades]
            + [str(trade.exchange_trade_id) for trade in trades]
        )
        evidence = RunEvidence(
            label=label,
            mode="live_like",
            owner="DataConsumer+LiveLikeOutcomeCapture",
            observed_batches=len(capture.observed_signals),
            persisted_orders=len(orders),
            persisted_fills=len(trades),
            raw_ids=raw_values,
            processed_deliveries=capture.processed_deliveries,
            acked=tuple(transport.acked),
            pending=transport.pending,
            cleanup_calls=transport.cleanup_calls,
        )
        return CollectedOutcome(outcome=outcome, evidence=evidence)
    finally:
        _shutdown_engine(engine)
        database_engine.dispose()


def collect_real_mode_outcomes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> CollectedFourRuns:
    cells: list[CollectedOutcome] = []
    requests = (
        ("BL", "live_like", 6, _RAW_IDS[0]),
        ("BB", "backtest", 6, _RAW_IDS[1]),
        ("CL", "live_like", 60, _RAW_IDS[2]),
        ("CB", "backtest", 60, _RAW_IDS[3]),
    )
    for label, mode, precision, raw_ids in requests:
        with localcontext() as context:
            context.prec = precision
            if mode == "live_like":
                cells.append(
                    _collect_live_like(
                        tmp_path,
                        monkeypatch,
                        label=label,
                        raw_ids=raw_ids,
                    )
                )
            else:
                cells.append(
                    _collect_backtest(
                        tmp_path,
                        monkeypatch,
                        label=label,
                        raw_ids=raw_ids,
                    )
                )
    return CollectedFourRuns(cells=(cells[0], cells[1], cells[2], cells[3]))


def collect_backtest_with_final_price(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    *,
    label: str,
    final_price: str,
    raw_ids_index: int,
) -> CollectedOutcome:
    return _collect_backtest(
        tmp_path,
        monkeypatch,
        label=label,
        raw_ids=_RAW_IDS[raw_ids_index],
        final_price=final_price,
    )


def build_four_run_matrix(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> FourRunMatrix:
    candidate_sha, candidate_tree, records, manifest_sha = _candidate_identity()
    identity = _identity_values(records)
    collected = collect_real_mode_outcomes(tmp_path, monkeypatch)
    roles = ("BL", "BB", "CL", "CB")
    runs: list[TradingParityRun] = []
    for role, cell in zip(roles, collected.cells, strict=True):
        baseline = role in {"BL", "BB"}
        live_like = role in {"BL", "CL"}
        runs.append(
            TradingParityRun(
                role=role,
                source_version="baseline" if baseline else "candidate",
                mode="live_like" if live_like else "backtest",
                revision_sha=PARENT_SHA if baseline else candidate_sha,
                tree_oid=PARENT_TREE if baseline else candidate_tree,
                runtime_source_sha256=identity[
                    "live_like_runtime" if live_like else "backtest_runtime"
                ],
                input_sha256=identity["input"],
                configuration_sha256=identity["configuration"],
                runner_sha256=identity[
                    "live_like_runner" if live_like else "backtest_runner"
                ],
                loaded_artifact_sha256=identity["loaded_artifact"],
                native_matcher_sha256=identity["native_matcher"],
                outcome=cell.outcome,
            )
        )
    run_tuple = (runs[0], runs[1], runs[2], runs[3])
    return FourRunMatrix(
        runs=run_tuple,
        evidence=(
            collected.cells[0].evidence,
            collected.cells[1].evidence,
            collected.cells[2].evidence,
            collected.cells[3].evidence,
        ),
        report=compare_four_run_parity(run_tuple),
        product_manifest_sha256=manifest_sha,
    )
