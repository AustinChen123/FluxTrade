"""
Smoke tests for FluxTrade core trading pipeline.

Run with: uv run pytest -m smoke
Exercises the full signal-to-position pipeline in-memory (no Docker/Redis/Postgres).
"""

import pytest
from decimal import Decimal

from src.core.models import SignalType
from src.core.execution import ExecutionEngine
from src.core.risk_manager import RiskManager
from src.core.adapters.simulated import SimulatedAdapter
from src.core.repositories import BacktestOrderRepository


@pytest.mark.smoke
class TestTradingPipeline:
    """End-to-end pipeline: Signal -> Risk -> Order -> Execution -> Fill -> Position."""

    def _build_engine(self, mock_db_session, mock_clock):
        """Build a complete execution engine with simulated adapter."""
        adapter = SimulatedAdapter(initial_balance=Decimal("100000"))
        repo = BacktestOrderRepository(mock_db_session, session_id=1, initial_balance=Decimal("100000"))
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=repo,
        )
        return engine, adapter, repo

    def test_long_entry_and_exit_with_profit(
        self, mock_db_session, mock_clock, signal_factory, candlestick_factory
    ):
        """Full round-trip: open LONG -> fill -> close with profit -> PnL realized."""
        engine, adapter, repo = self._build_engine(mock_db_session, mock_clock)
        initial_balance = adapter.get_balance()

        # 1. Execute LONG signal
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("40000"),
            quantity=Decimal("1.0"),
        )
        order_id = engine.execute_signal(signal)
        assert order_id is not None
        assert len(adapter._engine.open_orders) == 1

        # 2. Market data triggers fill (low touches limit price)
        candle = candlestick_factory(
            low=Decimal("39500"), high=Decimal("41000"), close=Decimal("40500")
        )
        engine.process_market_data(candle)
        assert len(adapter._engine.open_orders) == 0

        # 3. Position should exist
        pos = adapter.get_position(signal.product_id)
        assert pos is not None
        assert pos.side == "LONG"
        assert pos.quantity == Decimal("1.0")

        # 4. Exit with profit
        exit_signal = signal_factory(
            signal_type=SignalType.EXIT_LONG,
            price=Decimal("42000"),
            quantity=Decimal("1.0"),
        )
        exit_order_id = engine.execute_signal(exit_signal)
        assert exit_order_id is not None

        exit_candle = candlestick_factory(
            low=Decimal("41500"), high=Decimal("43000"), close=Decimal("42500")
        )
        engine.process_market_data(exit_candle)

        # 5. Verify PnL realized via adapter (Rust engine tracks balance)
        expected_pnl = (Decimal("42000") - Decimal("40000")) * Decimal("1.0")
        assert adapter.get_balance() == initial_balance + expected_pnl

    def test_short_entry_and_exit_with_profit(
        self, mock_db_session, mock_clock, signal_factory, candlestick_factory
    ):
        """Full round-trip: open SHORT -> fill -> close with profit."""
        engine, adapter, repo = self._build_engine(mock_db_session, mock_clock)
        initial_balance = adapter.get_balance()

        # 1. SHORT entry
        signal = signal_factory(
            signal_type=SignalType.SHORT,
            price=Decimal("44000"),
            quantity=Decimal("0.5"),
        )
        engine.execute_signal(signal)

        candle = candlestick_factory(
            low=Decimal("43000"), high=Decimal("45000"), close=Decimal("44500")
        )
        engine.process_market_data(candle)

        pos = adapter.get_position(signal.product_id)
        assert pos is not None
        assert pos.side == "SHORT"

        # 2. EXIT_SHORT at lower price (profit)
        exit_signal = signal_factory(
            signal_type=SignalType.EXIT_SHORT,
            price=Decimal("42000"),
            quantity=Decimal("0.5"),
        )
        engine.execute_signal(exit_signal)

        exit_candle = candlestick_factory(
            low=Decimal("41500"), high=Decimal("43000"), close=Decimal("42500")
        )
        engine.process_market_data(exit_candle)

        expected_pnl = (Decimal("44000") - Decimal("42000")) * Decimal("0.5")
        assert adapter.get_balance() == initial_balance + expected_pnl

    def test_market_order_fills_immediately_on_next_candle(
        self, mock_db_session, mock_clock, signal_factory, candlestick_factory
    ):
        """Market order (no price) fills at open on next candle."""
        engine, adapter, repo = self._build_engine(mock_db_session, mock_clock)

        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=None,
            value=None,
            quantity=Decimal("0.1"),
        )
        order_id = engine.execute_signal(signal)
        assert order_id is not None
        assert len(adapter._engine.open_orders) == 1
        assert adapter._engine.open_orders[0].order_type == "MARKET"

        candle = candlestick_factory(close=Decimal("41000"))
        engine.process_market_data(candle)

        assert len(adapter._engine.open_orders) == 0
        pos = adapter.get_position(signal.product_id)
        assert pos is not None

    def test_no_signal_produces_no_order(
        self, mock_db_session, mock_clock, signal_factory
    ):
        """NO_SIGNAL should not create any order."""
        engine, adapter, _ = self._build_engine(mock_db_session, mock_clock)

        signal = signal_factory(signal_type=SignalType.NO_SIGNAL)
        result = engine.execute_signal(signal)

        assert result is None
        assert len(adapter._engine.open_orders) == 0

    def test_adapter_failure_does_not_crash_pipeline(
        self, mock_db_session, mock_clock, signal_factory
    ):
        """Adapter failure should be handled gracefully."""
        engine, adapter, _ = self._build_engine(mock_db_session, mock_clock)

        # Force adapter to fail
        adapter._fail = True
        original_place = adapter.place_order

        def failing_place(order):
            from src.core.interfaces.exchange import ExchangeError
            raise ExchangeError("Connection timeout")

        adapter.place_order = failing_place

        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("42000"),
            quantity=Decimal("0.1"),
        )
        result = engine.execute_signal(signal)
        assert result is None


@pytest.mark.smoke
class TestRiskGatePipeline:
    """Verify risk checks block/allow signals correctly in context."""

    def test_risk_blocks_entry_on_zero_balance(
        self, mock_account_service, signal_factory
    ):
        """Risk manager should reject entry when balance is zero."""
        mock_account_service.set_balance(Decimal("0"))
        rm = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
        allowed, reason = rm.check_risk(signal)
        assert allowed is False

    def test_risk_allows_exit_on_zero_balance(
        self, mock_account_service, signal_factory
    ):
        """Exit signals should pass risk even with zero balance."""
        mock_account_service.set_balance(Decimal("0"))
        rm = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.EXIT_LONG)
        allowed, reason = rm.check_risk(signal)
        assert allowed is True


@pytest.mark.smoke
class TestSimulatedAdapterPipeline:
    """Verify SimulatedAdapter fills and position netting."""

    def test_position_netting_across_multiple_fills(
        self, order_factory, candlestick_factory
    ):
        """Multiple buys should accumulate, sell should reduce position."""
        adapter = SimulatedAdapter()

        # Two buy fills
        for _ in range(2):
            order = order_factory(order_type="market", side="buy", quantity=Decimal("0.5"))
            adapter.place_order(order)
            adapter.on_market_data(candlestick_factory(close=Decimal("42000")))

        pos = adapter.get_position("BINANCE:BTCUSDT-PERP")
        assert pos.quantity == Decimal("1.0")
        assert pos.side == "LONG"

        # Partial sell
        sell = order_factory(order_type="market", side="sell", quantity=Decimal("0.3"))
        adapter.place_order(sell)
        adapter.on_market_data(candlestick_factory(close=Decimal("43000")))

        pos = adapter.get_position("BINANCE:BTCUSDT-PERP")
        assert pos.quantity == Decimal("0.7")
        assert pos.side == "LONG"


@pytest.mark.smoke
class TestBacktestRepositoryPipeline:
    """Verify balance/PnL via Rust-backed SimulatedAdapter."""

    def test_round_trip_pnl(self, order_factory, candlestick_factory):
        """Open -> close should calculate correct PnL via adapter."""
        adapter = SimulatedAdapter(initial_balance=Decimal("10000"))

        # Buy 1.0 BTC at 40000 (limit)
        buy = order_factory(order_type="limit", side="buy",
                            product_id="BINANCE:BTCUSDT-PERP",
                            price=Decimal("40000"), quantity=Decimal("1.0"))
        adapter.place_order(buy)
        adapter.on_market_data(candlestick_factory(
            low=Decimal("39500"), high=Decimal("41000"), close=Decimal("40500")))

        # Sell 1.0 BTC at 42000 (limit)
        sell = order_factory(order_type="limit", side="sell",
                             product_id="BINANCE:BTCUSDT-PERP",
                             price=Decimal("42000"), quantity=Decimal("1.0"))
        adapter.place_order(sell)
        adapter.on_market_data(candlestick_factory(
            low=Decimal("41500"), high=Decimal("43000"), close=Decimal("42500")))

        assert adapter.get_balance() == Decimal("12000")
