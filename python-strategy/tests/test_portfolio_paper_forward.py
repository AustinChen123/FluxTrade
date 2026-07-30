from __future__ import annotations

import io
import threading
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

import src.validation.portfolio_paper_forward as paper_forward_module
from src.core.models import Candlestick, Signal, SignalType
from src.core.portfolio_runtime import (
    PortfolioDecisionRejected,
    PortfolioDefinition,
    PortfolioSleeve,
)
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.validation.portfolio_paper_forward import (
    run_portfolio_paper_forward,
    validate_portfolio_paper_forward_run,
)

PRODUCT_ID = "RITHMIC:MNQ-202609"
ET = ZoneInfo("America/New_York")


class _PaperForwardStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str) -> None:
        super().__init__(strategy_id, product_id)
        self.seen = 0
        self._in_position = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 2)

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(self.strategy_id, self.product_id)

    def replay_configuration(self) -> object:
        return ()

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        self.seen += 1
        should_enter = self.seen == 3 and not self._in_position
        if should_enter:
            self._in_position = True
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=SignalType.LONG if should_enter else SignalType.NO_SIGNAL,
            quantity=Decimal("1"),
            stop_loss=Decimal("95") if should_enter else None,
            take_profit=Decimal("105") if should_enter else None,
        )


setattr(_PaperForwardStrategy, "__fluxtrade_display_name__", "Paper Forward Fixture")
setattr(_PaperForwardStrategy, "__fluxtrade_artifact_version__", "1.0.0")
setattr(_PaperForwardStrategy, "__fluxtrade_readiness__", "RESEARCH_FROZEN")
setattr(_PaperForwardStrategy, "__fluxtrade_catalog_sha256__", "1" * 64)


class _ZeroLookbackStrategy(_PaperForwardStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 0)


class _OneMinuteStrategy(_PaperForwardStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 2)


def _portfolio() -> PortfolioDefinition:
    return PortfolioDefinition(
        portfolio_id="paper_forward_portfolio",
        product_id=PRODUCT_ID,
        sleeves=(
            PortfolioSleeve(
                _PaperForwardStrategy("paper_forward_portfolio.sleeve", PRODUCT_ID)
            ),
        ),
        max_gross_quantity=Decimal("1"),
        artifact_version="1.0.0",
        display_name="Paper Forward Fixture",
        readiness="RESEARCH_FROZEN",
        catalog_sha256="2" * 64,
    )


def _zero_lookback_portfolio() -> PortfolioDefinition:
    definition = _portfolio()
    return PortfolioDefinition(
        portfolio_id=definition.portfolio_id,
        product_id=definition.product_id,
        sleeves=(
            PortfolioSleeve(
                _ZeroLookbackStrategy(
                    "paper_forward_portfolio.sleeve",
                    PRODUCT_ID,
                )
            ),
        ),
        max_gross_quantity=definition.max_gross_quantity,
        artifact_version=definition.artifact_version,
        display_name=definition.display_name,
        readiness=definition.readiness,
        catalog_sha256=definition.catalog_sha256,
    )


def _one_minute_portfolio() -> PortfolioDefinition:
    definition = _portfolio()
    return PortfolioDefinition(
        portfolio_id=definition.portfolio_id,
        product_id=definition.product_id,
        sleeves=(
            PortfolioSleeve(
                _OneMinuteStrategy(
                    "paper_forward_portfolio.sleeve",
                    PRODUCT_ID,
                )
            ),
        ),
        max_gross_quantity=definition.max_gross_quantity,
        artifact_version=definition.artifact_version,
        display_name=definition.display_name,
        readiness=definition.readiness,
        catalog_sha256=definition.catalog_sha256,
    )


def _candle(timestamp: int, timeframe: str = "5m") -> Candlestick:
    return Candlestick(
        product_id=PRODUCT_ID,
        timeframe=timeframe,
        timestamp=timestamp,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("10"),
    )


def _et_ms(hour: int, minute: int) -> int:
    return int(datetime(2026, 7, 29, hour, minute, tzinfo=ET).timestamp() * 1_000)


def _prospective_sequence(*decision_timestamps: int) -> tuple[Candlestick, ...]:
    events: list[Candlestick] = []
    next_source = decision_timestamps[0]
    for decision_timestamp in decision_timestamps:
        while next_source <= decision_timestamp + 300_000:
            events.append(_candle(next_source, "1m"))
            next_source += 60_000
        events.append(_candle(decision_timestamp))
    return tuple(events)


class _CandleConsumer:
    instances: list["_CandleConsumer"] = []
    candles: tuple[Candlestick, ...] = ()

    def __init__(self, **kwargs) -> None:
        self.callback = kwargs["on_message_callback"]
        self.group_name = kwargs["group_name"]
        self.channel_provider = kwargs["channel_provider"]
        self.stopped = False
        self.cleaned = False
        self._stop = threading.Event()
        type(self).instances.append(self)

    def acquire_service_ownership(self) -> None:
        return None

    def start(self) -> None:
        assert self.channel_provider() == [
            "stream:market:rithmic:mnq-202609:1m",
            "stream:market:rithmic:mnq-202609:5m",
        ]
        for candle in self.candles:
            self.callback(candle)
        self._stop.wait(timeout=1)

    def request_stop(self) -> None:
        self.stopped = True
        self._stop.set()

    def stop(self) -> None:
        self.stopped = True
        self._stop.set()

    def assert_no_unresolved_deliveries(self) -> None:
        return None

    def cleanup_consumer_group(self) -> None:
        self.cleaned = True


def test_portfolio_paper_forward_warms_then_runs_shared_engine(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _CandleConsumer.candles = _prospective_sequence(
        _et_ms(18, 0),
        _et_ms(18, 5),
    )
    _CandleConsumer.instances.clear()
    output = io.StringIO()

    report = run_portfolio_paper_forward(
        tmp_path / "paper",
        run_id="session-1",
        portfolio_factory=_portfolio,
        warmup_candles=warmup,
        output=output,
        duration_seconds=0.01,
        consumer_factory=_CandleConsumer,
    )

    assert report.warmup_candles == 2
    assert report.warmup_first_timestamp == warmup[0].timestamp
    assert report.warmup_last_timestamp == warmup[-1].timestamp
    assert len(report.warmup_digest) == 64
    assert report.source_candles == 11
    assert report.prospective_candles == 2
    assert report.order_count == 3
    assert report.fill_count == 1
    assert report.fills[0].timestamp == _et_ms(18, 6)
    assert report.reconciliation_unresolved_count == 0
    assert report.reconciliation_verification_blocked_count == 0
    assert report.final_position_count == 1
    assert report.final_working_order_count == 2
    assert _CandleConsumer.instances[0].group_name == (
        "paper_forward:paper-forward-test:session-1"
    )
    assert _CandleConsumer.instances[0].stopped
    assert _CandleConsumer.instances[0].cleaned
    records = [line for line in output.getvalue().splitlines() if line]
    assert len(records) == 17
    assert '"type":"summary"' in records[-1]


def test_portfolio_paper_forward_digest_anchors_warmup_ohlcv(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    changed = [
        warmup[0].model_copy(update={"close": Decimal("100.25")}),
        warmup[1],
    ]
    _CandleConsumer.candles = _prospective_sequence(_et_ms(18, 0))

    first = run_portfolio_paper_forward(
        tmp_path / "first",
        run_id="digest-1",
        portfolio_factory=_portfolio,
        warmup_candles=warmup,
        output=io.StringIO(),
        duration_seconds=0.01,
        consumer_factory=_CandleConsumer,
    )
    second = run_portfolio_paper_forward(
        tmp_path / "second",
        run_id="digest-2",
        portfolio_factory=_portfolio,
        warmup_candles=changed,
        output=io.StringIO(),
        duration_seconds=0.01,
        consumer_factory=_CandleConsumer,
    )

    assert first.warmup_digest != second.warmup_digest


class _StopFailureConsumer(_CandleConsumer):
    def start(self) -> None:
        for candle in self.candles:
            self.callback(candle)
        self._stop.wait(timeout=1)
        raise RuntimeError("ownership lost during stop")


class _BlockingConsumer(_CandleConsumer):
    started = threading.Event()
    stop_requested = threading.Event()
    release = threading.Event()

    def start(self) -> None:
        type(self).started.set()
        type(self).release.wait()

    def request_stop(self) -> None:
        super().request_stop()
        type(self).stop_requested.set()


class _StopCleanupFailureConsumer(_CandleConsumer):
    def stop(self) -> None:
        super().stop()
        raise RuntimeError("consumer cleanup failed")


def test_portfolio_paper_forward_keeps_session_open_until_consumer_stops(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    monkeypatch.setattr(
        paper_forward_module,
        "_CONSUMER_STOP_GRACE_SECONDS",
        0,
    )
    _BlockingConsumer.started = threading.Event()
    _BlockingConsumer.stop_requested = threading.Event()
    _BlockingConsumer.release = threading.Event()
    session_closed = threading.Event()
    original_close = paper_forward_module._PaperForwardSession.close

    def record_close(session) -> None:
        original_close(session)
        session_closed.set()

    monkeypatch.setattr(
        paper_forward_module._PaperForwardSession,
        "close",
        record_close,
    )
    failure: list[BaseException] = []

    def run() -> None:
        try:
            run_portfolio_paper_forward(
                tmp_path / "paper",
                run_id="blocked-stop",
                portfolio_factory=_portfolio,
                warmup_candles=[
                    _candle(_et_ms(16, 50)),
                    _candle(_et_ms(16, 55)),
                ],
                output=io.StringIO(),
                duration_seconds=0.01,
                consumer_factory=_BlockingConsumer,
            )
        except BaseException as exc:
            failure.append(exc)

    runner = threading.Thread(target=run)
    runner.start()
    try:
        assert _BlockingConsumer.started.wait(timeout=1)
        assert _BlockingConsumer.stop_requested.wait(timeout=1)
        assert runner.is_alive()
        assert not session_closed.is_set()
    finally:
        _BlockingConsumer.release.set()
        runner.join(timeout=1)

    assert not runner.is_alive()
    assert session_closed.is_set()
    assert len(failure) == 1
    assert isinstance(failure[0], RuntimeError)
    assert "shutdown grace period" in str(failure[0])


def test_portfolio_paper_forward_closes_session_when_consumer_cleanup_fails(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    _StopCleanupFailureConsumer.candles = _prospective_sequence(_et_ms(18, 0))
    session_closed = threading.Event()
    original_close = paper_forward_module._PaperForwardSession.close

    def record_close(session) -> None:
        original_close(session)
        session_closed.set()

    monkeypatch.setattr(
        paper_forward_module._PaperForwardSession,
        "close",
        record_close,
    )

    with pytest.raises(RuntimeError, match="consumer cleanup failed"):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="cleanup-failure",
            portfolio_factory=_portfolio,
            warmup_candles=[
                _candle(_et_ms(16, 50)),
                _candle(_et_ms(16, 55)),
            ],
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_StopCleanupFailureConsumer,
        )

    assert session_closed.is_set()


def test_portfolio_paper_forward_rejects_failure_reported_during_stop(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _StopFailureConsumer.candles = _prospective_sequence(_et_ms(18, 0))

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="stop-failure",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_StopFailureConsumer,
        )

    assert isinstance(error.value.__cause__, RuntimeError)
    assert "ownership lost during stop" in str(error.value.__cause__)


def test_portfolio_paper_forward_rejects_insufficient_warmup_before_consumer(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    _CandleConsumer.instances.clear()

    with pytest.raises(ValueError, match="insufficient candles"):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="session-2",
            portfolio_factory=_portfolio,
            warmup_candles=[_candle(1_800_000_000_000)],
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert _CandleConsumer.instances == []
    assert not (tmp_path / "paper").exists()


def test_zero_lookback_still_requires_one_continuity_anchor(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")

    with pytest.raises(ValueError, match="insufficient candles"):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="zero-lookback",
            portfolio_factory=_zero_lookback_portfolio,
            warmup_candles=[],
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert not (tmp_path / "paper").exists()


def test_portfolio_paper_forward_fails_on_overlapping_prospective_candle(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _CandleConsumer.candles = (_candle(_et_ms(16, 55)),)

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="session-3",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert isinstance(error.value.__cause__, ValueError)
    assert "strictly increasing" in str(error.value.__cause__)


def test_portfolio_paper_forward_rejects_missing_open_session_warmup(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 45)), _candle(_et_ms(16, 50))]
    _CandleConsumer.candles = (_candle(_et_ms(18, 0)),)

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="session-gap",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert isinstance(error.value.__cause__, ValueError)
    assert "not contiguous" in str(error.value.__cause__)


def test_portfolio_paper_forward_rejects_midstream_session_gap(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _CandleConsumer.candles = (
        _candle(_et_ms(18, 0)),
        _candle(_et_ms(18, 10)),
    )

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="session-midstream-gap",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert isinstance(error.value.__cause__, ValueError)
    assert "not contiguous" in str(error.value.__cause__)


def test_portfolio_paper_forward_rejects_live_environment(tmp_path) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
        with pytest.raises(
            ValueError,
            match="FLUXTRADE_ENVIRONMENT=paper-forward",
        ):
            run_portfolio_paper_forward(
                tmp_path / "paper",
                run_id="session-4",
                portfolio_factory=_portfolio,
                warmup_candles=[
                    _candle(1_800_000_000_000),
                    _candle(1_800_000_300_000),
                ],
                output=io.StringIO(),
                duration_seconds=0.01,
                consumer_factory=_CandleConsumer,
            )

    assert not (tmp_path / "paper").exists()


def test_portfolio_paper_forward_preflight_rejects_existing_workspace(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    workspace = tmp_path / "paper"
    workspace.mkdir()

    with pytest.raises(FileExistsError, match="workspace already exists"):
        validate_portfolio_paper_forward_run(
            workspace,
            run_id="existing-workspace",
            definition=_portfolio(),
            warmup_candles=[
                _candle(_et_ms(16, 50)),
                _candle(_et_ms(16, 55)),
            ],
            duration_seconds=0.01,
        )


@pytest.mark.parametrize("interval_minutes", [4, 6])
def test_portfolio_paper_forward_rejects_irregular_warmup_cadence(
    monkeypatch,
    tmp_path,
    interval_minutes,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    first = _et_ms(16, 50)

    with pytest.raises(
        ValueError,
        match="interval-aligned|not contiguous",
    ):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id=f"warmup-{interval_minutes}m",
            portfolio_factory=_portfolio,
            warmup_candles=[
                _candle(first),
                _candle(first + interval_minutes * 60_000),
            ],
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert not (tmp_path / "paper").exists()


def test_portfolio_paper_forward_rejects_source_gap(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _CandleConsumer.candles = (
        _candle(_et_ms(18, 0), "1m"),
        _candle(_et_ms(18, 2), "1m"),
    )

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="source-gap",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert isinstance(error.value.__cause__, ValueError)
    assert "not contiguous" in str(error.value.__cause__)


def test_portfolio_paper_forward_requires_source_watermark(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _CandleConsumer.candles = (_candle(_et_ms(18, 0)),)
    _CandleConsumer.instances.clear()
    output = io.StringIO()

    with pytest.raises(ValueError, match="source watermark"):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="missing-source",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=output,
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert not _CandleConsumer.instances[0].cleaned
    assert '"type":"observed_decision"' in output.getvalue()
    assert '"type":"summary"' not in output.getvalue()


def test_portfolio_paper_forward_holds_decision_until_source_watermark(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    decision_timestamp = _et_ms(18, 0)
    _CandleConsumer.candles = (
        _candle(decision_timestamp),
        *(
            _candle(timestamp, "1m")
            for timestamp in range(
                decision_timestamp,
                decision_timestamp + 300_001,
                60_000,
            )
        ),
    )

    report = run_portfolio_paper_forward(
        tmp_path / "paper",
        run_id="decision-first",
        portfolio_factory=_portfolio,
        warmup_candles=warmup,
        output=io.StringIO(),
        duration_seconds=0.01,
        consumer_factory=_CandleConsumer,
    )

    assert report.source_candles == 6
    assert report.prospective_candles == 1
    assert report.first_prospective_timestamp == decision_timestamp


def test_portfolio_paper_forward_rejects_source_advancing_before_decision(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    decision_timestamp = _et_ms(18, 0)
    _CandleConsumer.candles = tuple(
        _candle(timestamp, "1m")
        for timestamp in range(
            decision_timestamp,
            decision_timestamp + 360_001,
            60_000,
        )
    ) + (_candle(decision_timestamp),)
    _CandleConsumer.instances.clear()
    output = io.StringIO()

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="late-decision",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=output,
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert isinstance(error.value.__cause__, ValueError)
    assert "source advanced before required decision" in str(error.value.__cause__)
    assert not _CandleConsumer.instances[0].cleaned
    assert '"type":"summary"' not in output.getvalue()


def test_portfolio_paper_forward_rejects_missing_completed_bucket_decision(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    decision_timestamp = _et_ms(18, 0)
    _CandleConsumer.candles = _prospective_sequence(decision_timestamp) + tuple(
        _candle(timestamp, "1m")
        for timestamp in range(
            decision_timestamp + 360_000,
            decision_timestamp + 600_001,
            60_000,
        )
    )
    _CandleConsumer.instances.clear()
    output = io.StringIO()

    with pytest.raises(ValueError, match="completed source bucket"):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="missing-decision",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=output,
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert not _CandleConsumer.instances[0].cleaned
    assert '"type":"summary"' not in output.getvalue()


def test_portfolio_paper_forward_resets_source_bucket_after_session_close(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 45)), _candle(_et_ms(16, 50))]
    _CandleConsumer.candles = (
        *(
            _candle(timestamp, "1m")
            for timestamp in range(
                _et_ms(16, 55),
                _et_ms(16, 59) + 1,
                60_000,
            )
        ),
        _candle(_et_ms(18, 0), "1m"),
        _candle(_et_ms(16, 55)),
        *(
            _candle(timestamp, "1m")
            for timestamp in range(
                _et_ms(18, 1),
                _et_ms(18, 5) + 1,
                60_000,
            )
        ),
        _candle(_et_ms(18, 0)),
    )

    report = run_portfolio_paper_forward(
        tmp_path / "paper",
        run_id="session-reset",
        portfolio_factory=_portfolio,
        warmup_candles=warmup,
        output=io.StringIO(),
        duration_seconds=0.01,
        consumer_factory=_CandleConsumer,
    )

    assert report.source_candles == 11
    assert report.prospective_candles == 2
    assert report.last_prospective_timestamp == _et_ms(18, 0)


def test_portfolio_paper_forward_skips_incomplete_startup_decision(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    first_decision = _et_ms(18, 0)
    next_decision = _et_ms(18, 5)
    _CandleConsumer.candles = (
        _candle(first_decision),
        *(
            _candle(timestamp, "1m")
            for timestamp in range(
                _et_ms(18, 1),
                _et_ms(18, 6),
                60_000,
            )
        ),
        _candle(next_decision),
        *(
            _candle(timestamp, "1m")
            for timestamp in range(
                _et_ms(18, 6),
                _et_ms(18, 11),
                60_000,
            )
        ),
    )

    report = run_portfolio_paper_forward(
        tmp_path / "paper",
        run_id="prefix",
        portfolio_factory=_portfolio,
        warmup_candles=warmup,
        output=io.StringIO(),
        duration_seconds=0.01,
        consumer_factory=_CandleConsumer,
    )

    assert report.skipped_decision_prefix_candles == 1
    assert report.prospective_candles == 1
    assert report.first_prospective_timestamp == next_decision


def test_portfolio_paper_forward_rejects_non_5m_portfolio_before_artifacts(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")

    with pytest.raises(ValueError, match="portfolio timeframe must be 5m"):
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="wrong-timeframe",
            portfolio_factory=_one_minute_portfolio,
            warmup_candles=[
                _candle(_et_ms(16, 50)),
                _candle(_et_ms(16, 55)),
            ],
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert not (tmp_path / "paper").exists()


def test_portfolio_paper_forward_fails_on_risk_rejected_signal(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "paper-forward-test")
    monkeypatch.setattr(
        "src.core.risk_manager.RiskManager.check_risk",
        lambda self, signal, current_price=None: (False, "fixture rejection"),
    )
    warmup = [_candle(_et_ms(16, 50)), _candle(_et_ms(16, 55))]
    _CandleConsumer.candles = _prospective_sequence(_et_ms(18, 0))

    with pytest.raises(RuntimeError, match="consumer failed") as error:
        run_portfolio_paper_forward(
            tmp_path / "paper",
            run_id="risk-rejection",
            portfolio_factory=_portfolio,
            warmup_candles=warmup,
            output=io.StringIO(),
            duration_seconds=0.01,
            consumer_factory=_CandleConsumer,
        )

    assert isinstance(error.value.__cause__, PortfolioDecisionRejected)
    assert "portfolio_submission_rejected" in str(error.value.__cause__)
