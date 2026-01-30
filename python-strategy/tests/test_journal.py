"""Tests for StrategyJournal — Phase 4.6."""

import json
import pytest
from decimal import Decimal
from src.core.journal import StrategyJournal, JournalEntry
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Signal, SignalType, Candlestick
from src.core.execution import ExecutionEngine


# =============================================================================
# StrategyJournal Unit Tests
# =============================================================================


class TestJournalBasicLogging:
    def test_log_single_event(self):
        j = StrategyJournal("strat_1")
        j.log("entry", {"side": "LONG", "reason": "bos_confirm"}, timestamp=1000)
        assert len(j) == 1
        assert j._entries[0].tag == "entry"
        assert j._entries[0].data["side"] == "LONG"

    def test_log_multiple_events(self):
        j = StrategyJournal("strat_1")
        j.log("entry", {"side": "LONG"}, timestamp=1000)
        j.log("skip", {"reason": "rr_low"}, timestamp=2000)
        j.log("structure", {"trend": "UP"}, timestamp=3000)
        assert len(j) == 3

    def test_log_with_trade_id(self):
        j = StrategyJournal("strat_1")
        j.log("entry", {"side": "LONG"}, timestamp=1000, trade_id="order-123")
        assert j._entries[0].trade_id == "order-123"

    def test_log_without_trade_id(self):
        j = StrategyJournal("strat_1")
        j.log("skip", {"reason": "no_setup"}, timestamp=1000)
        assert j._entries[0].trade_id is None

    def test_strategy_id_stored(self):
        j = StrategyJournal("my_strategy")
        assert j.strategy_id == "my_strategy"

    def test_default_strategy_id(self):
        j = StrategyJournal()
        assert j.strategy_id == ""


class TestJournalFiltering:
    @pytest.fixture
    def populated_journal(self):
        j = StrategyJournal("strat_1")
        j.log("entry", {"side": "LONG"}, timestamp=1000, trade_id="t1")
        j.log("structure", {"trend": "UP"}, timestamp=1500)
        j.log("sl_hit", {"price": 41000}, timestamp=2000, trade_id="t1")
        j.log("entry", {"side": "SHORT"}, timestamp=3000, trade_id="t2")
        j.log("tp_hit", {"price": 40000}, timestamp=4000, trade_id="t2")
        j.log("skip", {"reason": "no_volume"}, timestamp=5000)
        return j

    def test_filter_by_tag(self, populated_journal):
        entries = populated_journal.entries(tag="entry")
        assert len(entries) == 2
        assert all(e.tag == "entry" for e in entries)

    def test_filter_by_trade_id(self, populated_journal):
        entries = populated_journal.entries(trade_id="t1")
        assert len(entries) == 2
        assert entries[0].tag == "entry"
        assert entries[1].tag == "sl_hit"

    def test_filter_by_time_range(self, populated_journal):
        entries = populated_journal.entries(start=2000, end=4000)
        assert len(entries) == 3
        assert entries[0].tag == "sl_hit"
        assert entries[-1].tag == "tp_hit"

    def test_filter_combined(self, populated_journal):
        entries = populated_journal.entries(tag="entry", trade_id="t2")
        assert len(entries) == 1
        assert entries[0].data["side"] == "SHORT"

    def test_filter_no_match(self, populated_journal):
        entries = populated_journal.entries(tag="nonexistent")
        assert len(entries) == 0

    def test_filter_start_only(self, populated_journal):
        entries = populated_journal.entries(start=4000)
        assert len(entries) == 2

    def test_filter_end_only(self, populated_journal):
        entries = populated_journal.entries(end=1500)
        assert len(entries) == 2

    def test_no_filter_returns_all(self, populated_journal):
        entries = populated_journal.entries()
        assert len(entries) == 6


class TestJournalTags:
    def test_tags_returns_unique_sorted(self):
        j = StrategyJournal("s")
        j.log("entry", {}, timestamp=0)
        j.log("skip", {}, timestamp=0)
        j.log("entry", {}, timestamp=0)
        j.log("sl_hit", {}, timestamp=0)
        assert j.tags == ["entry", "skip", "sl_hit"]

    def test_tags_empty_journal(self):
        j = StrategyJournal("s")
        assert j.tags == []


class TestJournalExport:
    def test_to_jsonl_format(self):
        j = StrategyJournal("strat_1")
        j.log("entry", {"side": "LONG"}, timestamp=1000, trade_id="t1")
        j.log("sl_hit", {"price": 41000}, timestamp=2000, trade_id="t1")

        jsonl = j.to_jsonl()
        lines = jsonl.strip().split("\n")
        assert len(lines) == 2

        line1 = json.loads(lines[0])
        assert line1["strategy_id"] == "strat_1"
        assert line1["tag"] == "entry"
        assert line1["trade_id"] == "t1"
        assert line1["timestamp"] == 1000

        line2 = json.loads(lines[1])
        assert line2["tag"] == "sl_hit"

    def test_to_jsonl_no_trade_id(self):
        j = StrategyJournal("s")
        j.log("skip", {"reason": "no_setup"}, timestamp=500)
        jsonl = j.to_jsonl()
        obj = json.loads(jsonl)
        assert "trade_id" not in obj

    def test_to_dicts(self):
        j = StrategyJournal("strat_1")
        j.log("entry", {"side": "LONG"}, timestamp=1000)
        j.log("skip", {"reason": "rr"}, timestamp=2000)

        dicts = j.to_dicts()
        assert len(dicts) == 2
        assert dicts[0]["tag"] == "entry"
        assert dicts[1]["data"]["reason"] == "rr"

    def test_to_jsonl_empty(self):
        j = StrategyJournal("s")
        assert j.to_jsonl() == ""

    def test_to_dicts_empty(self):
        j = StrategyJournal("s")
        assert j.to_dicts() == []

    def test_to_jsonl_decimal_values(self):
        j = StrategyJournal("s")
        j.log("fill", {"price": Decimal("42000.50")}, timestamp=0)
        jsonl = j.to_jsonl()
        obj = json.loads(jsonl)
        assert obj["data"]["price"] == "42000.50"


class TestJournalClear:
    def test_clear(self):
        j = StrategyJournal("s")
        j.log("a", {}, timestamp=0)
        j.log("b", {}, timestamp=0)
        assert len(j) == 2
        j.clear()
        assert len(j) == 0
        assert j.entries() == []


# =============================================================================
# BaseStrategy Journal Integration
# =============================================================================


class DummyStrategy(BaseStrategy):
    """Minimal strategy for testing journal integration."""

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe="1m",
            lookback_window=10,
        )

    def on_candle(self, candle):
        self.journal.log("candle_seen", {"close": str(candle.close)}, timestamp=candle.timestamp)
        return None


class TestBaseStrategyJournal:
    def test_strategy_has_journal(self):
        s = DummyStrategy("test_strat", "BINANCE:BTCUSDT-PERP")
        assert isinstance(s.journal, StrategyJournal)
        assert s.journal.strategy_id == "test_strat"

    def test_strategy_can_log(self):
        s = DummyStrategy("test_strat", "BINANCE:BTCUSDT-PERP")
        s.journal.log("entry", {"side": "LONG"}, timestamp=1000)
        assert len(s.journal) == 1

    def test_strategy_on_candle_logs(self):
        s = DummyStrategy("test_strat", "BINANCE:BTCUSDT-PERP")
        candle = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1000,
            open=Decimal("42000"),
            high=Decimal("42500"),
            low=Decimal("41500"),
            close=Decimal("42200"),
            volume=Decimal("100"),
        )
        s.on_candle(candle)
        assert len(s.journal) == 1
        assert s.journal._entries[0].tag == "candle_seen"

    def test_journal_replacement(self):
        s = DummyStrategy("test_strat", "BINANCE:BTCUSDT-PERP")
        new_journal = StrategyJournal("injected")
        s.journal = new_journal
        s.journal.log("test", {}, timestamp=0)
        assert s.journal.strategy_id == "injected"
        assert len(s.journal) == 1


# =============================================================================
# ExecutionEngine Journal Integration
# =============================================================================


class TestExecutionEngineJournal:
    @pytest.fixture
    def engine_with_journal(self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
        journal = StrategyJournal("test_strat")
        engine = ExecutionEngine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
            journal=journal,
        )
        return engine, journal

    def test_engine_stores_journal(self, engine_with_journal):
        engine, journal = engine_with_journal
        assert engine.journal is journal

    def test_engine_no_journal(self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
        engine = ExecutionEngine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        assert engine.journal is None

    def test_execute_signal_logs_entry(self, engine_with_journal):
        engine, journal = engine_with_journal
        signal = Signal(
            strategy_id="test_strat",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1000,
            type=SignalType.LONG,
            quantity=Decimal("0.1"),
        )
        order_id = engine.execute_signal(signal)
        assert order_id is not None

        entries = journal.entries(tag="entry")
        assert len(entries) == 1
        assert entries[0].data["side"] == "buy"
        assert entries[0].timestamp == 1000

    def test_execute_signal_with_sl_tp_logs(self, engine_with_journal):
        engine, journal = engine_with_journal
        signal = Signal(
            strategy_id="test_strat",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=2000,
            type=SignalType.LONG,
            quantity=Decimal("0.1"),
            stop_loss=Decimal("41000"),
            take_profit=Decimal("44000"),
        )
        engine.execute_signal(signal)

        entries = journal.entries(tag="entry")
        assert len(entries) == 1
        assert entries[0].data["stop_loss"] == "41000"
        assert entries[0].data["take_profit"] == "44000"

    def test_fill_logs_to_journal(self, engine_with_journal):
        engine, journal = engine_with_journal

        # Simulate a fill via on_market_data
        candle = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=5000,
            open=Decimal("42000"),
            high=Decimal("42500"),
            low=Decimal("41500"),
            close=Decimal("42200"),
            volume=Decimal("100"),
        )

        # Place a signal first to get an order in the adapter
        signal = Signal(
            strategy_id="test_strat",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=4000,
            type=SignalType.LONG,
            quantity=Decimal("0.1"),
        )
        engine.execute_signal(signal)
        journal.clear()  # clear entry log to isolate fill test

        engine.process_market_data(candle)

        fill_entries = journal.entries(tag="fill")
        assert len(fill_entries) >= 1
        assert fill_entries[0].data["fill_type"] == "MARKET"
        assert fill_entries[0].timestamp == 5000

    def test_no_journal_no_error(self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
        """ExecutionEngine without journal should not error on fills."""
        engine = ExecutionEngine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        signal = Signal(
            strategy_id="test_strat",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1000,
            type=SignalType.LONG,
            quantity=Decimal("0.1"),
        )
        order_id = engine.execute_signal(signal)
        assert order_id is not None


# =============================================================================
# JournalEntry dataclass
# =============================================================================


class TestJournalEntry:
    def test_entry_creation(self):
        e = JournalEntry(timestamp=1000, tag="test", data={"key": "val"})
        assert e.timestamp == 1000
        assert e.tag == "test"
        assert e.data == {"key": "val"}
        assert e.trade_id is None

    def test_entry_with_trade_id(self):
        e = JournalEntry(timestamp=1000, tag="test", data={}, trade_id="t1")
        assert e.trade_id == "t1"
