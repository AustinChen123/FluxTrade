"""
Tests for src/core/execution.py

Covers:
- Signal to order conversion
- Side determination (LONG/SHORT/EXIT)
- Order type detection (market/limit)
- Adapter delegation
- Error handling on execution failure
- Market data processing for simulated fills
"""

from contextlib import nullcontext

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderSnapshot
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.interfaces.exchange import ExchangeError
from src.core.interfaces.exchange import ExchangeOrderLookupUnsupported
from src.core.models import OrderStatus, Position, PositionSide, SignalType
from src.core.orm_models import SystemEvent
from src.core.client_order_id import generate_client_order_id, parse_client_order_id


@pytest.fixture
def execution_engine(mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo):
    """Provides an ExecutionEngine with mock dependencies."""
    return ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo
    )


def _binance_btcusdt_market_rules(min_notional: str = "10") -> dict:
    return {
        "BTC/USDT:USDT": {
            "info": {
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "stepSize": "0.001",
                    },
                    {
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": min_notional,
                    },
                ],
            },
        },
    }


def _ccxt_adapter_with_market_rules(markets: dict) -> tuple[CcxtExchangeAdapter, MagicMock]:
    client = MagicMock()
    client.load_markets.return_value = markets
    client.create_order.return_value = {"id": "EX-quantized"}
    with patch("src.core.adapters.ccxt_adapter.ccxt") as mock_ccxt:
        exchange_cls = MagicMock(return_value=client)
        mock_ccxt.binance = exchange_cls
        setattr(mock_ccxt, "binance", exchange_cls)
        adapter = CcxtExchangeAdapter(
            exchange_id="binance",
            api_key="test-key",
            secret="test-secret",
            testnet=False,
        )
    adapter.client = client
    return adapter, client


class TestSideDetermination:
    """Tests for signal type to order side mapping."""

    def test_long_signal_becomes_buy(self, execution_engine, signal_factory):
        """LONG signal should produce a buy order."""
        signal = signal_factory(signal_type=SignalType.LONG, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_short_signal_becomes_sell(self, execution_engine, signal_factory):
        """SHORT signal should produce a sell order."""
        signal = signal_factory(signal_type=SignalType.SHORT, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_exit_long_becomes_sell(self, execution_engine, signal_factory):
        """EXIT_LONG signal should produce a sell order."""
        signal = signal_factory(signal_type=SignalType.EXIT_LONG, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_exit_short_becomes_buy(self, execution_engine, signal_factory):
        """EXIT_SHORT signal should produce a buy order."""
        signal = signal_factory(signal_type=SignalType.EXIT_SHORT, price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is not None

    def test_no_signal_returns_none(self, execution_engine, signal_factory):
        """NO_SIGNAL should return None (no order created)."""
        signal = signal_factory(signal_type=SignalType.NO_SIGNAL)
        order_id = execution_engine.execute_signal(signal)

        assert order_id is None


class TestOrderTypeDetection:
    """Tests for order type (market/limit) detection."""

    def test_signal_with_price_creates_limit(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal with price should create limit order."""
        signal = signal_factory(price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1
        assert mock_exchange_adapter.open_orders[0].type == "limit"

    def test_signal_with_value_creates_limit(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal with value (legacy) should create limit order."""
        signal = signal_factory(price=None, value=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1
        assert mock_exchange_adapter.open_orders[0].type == "limit"

    def test_signal_without_price_creates_market(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal without price should create market order."""
        signal = signal_factory(price=None, value=None)
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1
        assert mock_exchange_adapter.open_orders[0].type == "market"


class TestQuantityHandling:
    """Tests for quantity determination."""

    def test_signal_quantity_used(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Signal's quantity should be used when provided."""
        signal = signal_factory(quantity=Decimal("0.5"), price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.5")

    def test_default_quantity_when_none(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Default quantity should be used when signal quantity is None."""
        signal = signal_factory(quantity=None, price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.01")

    def test_default_quantity_when_zero(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Default quantity should be used when signal quantity is zero."""
        signal = signal_factory(quantity=Decimal("0"), price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.01")

    def test_exit_without_quantity_closes_current_position(
        self,
        execution_engine,
        signal_factory,
        mock_exchange_adapter,
    ):
        """EXIT signals without quantity should use the current position size."""
        product_id = "BINANCE:BTCUSDT-PERP"
        mock_exchange_adapter.positions[product_id] = Position(
            strategy_id="test-strategy",
            product_id=product_id,
            side=PositionSide.LONG,
            quantity=Decimal("0.25"),
            entry_price=Decimal("42000"),
            unrealized_pnl=Decimal("0"),
        )
        signal = signal_factory(
            signal_type=SignalType.EXIT_LONG,
            product_id=product_id,
            quantity=None,
            price=Decimal("42000"),
        )

        execution_engine.execute_signal(signal)

        assert mock_exchange_adapter.open_orders[0].quantity == Decimal("0.25")


class TestAdapterDelegation:
    """Tests for adapter order placement."""

    def test_order_sent_to_adapter(self, execution_engine, signal_factory, mock_exchange_adapter):
        """Order should be sent to adapter for execution."""
        signal = signal_factory(price=Decimal("42000"))
        execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 1

    def test_exchange_id_recorded(self, execution_engine, signal_factory, mock_order_repo):
        """Exchange order ID should be recorded after placement."""
        signal = signal_factory(price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        order = mock_order_repo.orders[order_id]
        assert order.exchange_order_id.startswith("MOCK-")

    def test_multiple_signals_create_multiple_orders(
        self, execution_engine, signal_factory, mock_exchange_adapter
    ):
        """Multiple signals should create independent orders."""
        for _ in range(3):
            signal = signal_factory(price=Decimal("42000"))
            execution_engine.execute_signal(signal)

        assert len(mock_exchange_adapter.open_orders) == 3


class TestExecutionTradingRules:
    """Regression coverage for exchange trading rules at execution entrypoints."""

    def test_quantizes_order_before_external_placement(
        self, mock_db_session, mock_clock, mock_order_repo, signal_factory
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("50123.456"),
            quantity=Decimal("0.0109"),
        )

        order_id = engine.execute_signal(signal)

        assert order_id is not None
        order = mock_order_repo.orders[order_id]
        assert order.exchange_order_id == "EX-quantized"
        assert order.quantity == Decimal("0.010")
        assert order.price == Decimal("50123.40")
        client.create_order.assert_called_once()
        call = client.create_order.call_args
        assert call.kwargs["amount"] == "0.010"
        assert call.kwargs["price"] == "50123.40"

    def test_quantizes_protective_trigger_and_persists_prevalidated_values(
        self, mock_db_session, mock_clock, mock_order_repo, signal_factory
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        updated_order_ids = []
        original_update_order = mock_order_repo.update_order

        def track_update_order(order):
            updated_order_ids.append(order.id)
            original_update_order(order)

        mock_order_repo.update_order = track_update_order
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("50123.456"),
            quantity=Decimal("0.0109"),
            stop_loss=Decimal("49999.987"),
        )

        order_id = engine.execute_signal(signal)

        assert order_id is not None
        assert client.create_order.call_count == 2
        entry_order = mock_order_repo.orders[order_id]
        stop_order = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        assert entry_order.quantity == Decimal("0.010")
        assert entry_order.price == Decimal("50123.40")
        assert stop_order.quantity == Decimal("0.010")
        assert stop_order.trigger_price == Decimal("50000.00")
        assert entry_order.id in updated_order_ids
        assert stop_order.id in updated_order_ids

    def test_entry_journal_records_quantized_values(
        self, mock_db_session, mock_clock, mock_order_repo, signal_factory
    ):
        """Journal must log what was actually submitted, not pre-quantization locals."""
        adapter, _client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        journal = MagicMock()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            journal=journal,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("50123.456"),
            quantity=Decimal("0.0109"),
        )

        order_id = engine.execute_signal(signal)

        assert order_id is not None
        entry_calls = [c for c in journal.log.call_args_list if c.args[0] == "entry"]
        assert len(entry_calls) == 1
        payload = entry_calls[0].args[1]
        assert payload["quantity"] == "0.010"
        assert payload["price"] == "50123.40"

    def test_min_notional_rejection_fails_local_order_and_audit(
        self, mock_db_session, mock_clock, mock_order_repo, signal_factory
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("5000.00"),
            quantity=Decimal("0.001"),
        )

        with pytest.raises(ExchangeError, match="min_notional_not_met"):
            engine.execute_signal(signal)

        client.create_order.assert_not_called()
        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = mock_db_session.add.call_args_list[0].args[0]
        assert audit.order_id == failed_orders[0].id
        assert audit.risk_message is not None
        assert "min_notional_not_met" in audit.risk_message
        assert audit.outcome_payload["status"] == "failed"
        assert "min_notional_not_met" in audit.outcome_payload["error"]
        events = [
            call.args[0]
            for call in mock_db_session.add.call_args_list
            if isinstance(call.args[0], SystemEvent)
        ]
        assert len(events) == 1
        assert events[0].event_type == "system_error"
        assert events[0].event_subtype == "order_rejected"
        assert events[0].related_order_id == failed_orders[0].id
        assert events[0].payload["reason"] == "min_notional_not_met"
        assert events[0].payload["phase"] == "audited_execution"

    def test_market_min_notional_without_reference_price_fails_local_order_and_audit(
        self, mock_db_session, mock_clock, mock_order_repo, signal_factory
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=None,
            value=None,
            quantity=Decimal("0.001"),
        )

        with pytest.raises(ExchangeError, match="min_notional_unverifiable"):
            engine.execute_signal(signal)

        client.create_order.assert_not_called()
        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = mock_db_session.add.call_args_list[0].args[0]
        assert audit.order_id == failed_orders[0].id
        assert audit.risk_message is not None
        assert "min_notional_unverifiable" in audit.risk_message
        assert audit.outcome_payload["status"] == "failed"
        assert "min_notional_unverifiable" in audit.outcome_payload["error"]

    def test_market_min_notional_uses_candle_close_reference_price(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
        candlestick_factory,
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=None,
            value=None,
            quantity=Decimal("0.001"),
        )
        candle = candlestick_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            close=Decimal("12000"),
        )

        order_id = engine.execute_signal(signal, candle=candle)

        assert order_id is not None
        order = mock_order_repo.orders[order_id]
        assert getattr(order, "min_notional_reference_price") == Decimal("12000")
        client.create_order.assert_called_once()

    def test_market_min_notional_rejects_low_candle_close_reference_price(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
        candlestick_factory,
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=None,
            value=None,
            quantity=Decimal("0.001"),
        )
        candle = candlestick_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            close=Decimal("5000"),
        )

        with pytest.raises(ExchangeError, match="min_notional_not_met"):
            engine.execute_signal(signal, candle=candle)

        client.create_order.assert_not_called()

    def test_trailing_only_signal_prevalidates_and_places_protective_order(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
        candlestick_factory,
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=None,
            value=None,
            quantity=Decimal("0.001"),
            trailing_distance=Decimal("100"),
        )
        candle = candlestick_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            close=Decimal("12000"),
        )

        order_id = engine.execute_signal(signal, candle=candle)

        assert order_id is not None
        assert client.create_order.call_count == 2
        orders = list(mock_order_repo.orders.values())
        assert {order.type for order in orders} == {"market", "trailing_stop"}
        trailing_order = next(order for order in orders if order.type == "trailing_stop")
        assert getattr(trailing_order, "min_notional_reference_price") == Decimal("12000")

    def test_trailing_validation_failure_blocks_entry_order(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
        candlestick_factory,
    ):
        adapter, client = _ccxt_adapter_with_market_rules(_binance_btcusdt_market_rules())
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )
        signal = signal_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("12000"),
            quantity=Decimal("0.001"),
            trailing_distance=Decimal("100"),
        )
        candle = candlestick_factory(
            product_id="BINANCE:BTCUSDT-PERP",
            close=Decimal("5000"),
        )

        with pytest.raises(ExchangeError, match="min_notional_not_met"):
            engine.execute_signal(signal, candle=candle)

        client.create_order.assert_not_called()
        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 2


class TestLiveOrderEventSync:
    """Regression coverage for live exchange order event state application."""

    def _engine(self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, journal=None):
        return ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            journal=journal,
            is_backtest=True,
        )

    def test_live_order_event_records_partial_then_final_fill_delta(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
    ):
        journal = MagicMock()
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
            journal=journal,
        )
        order = order_factory(
            client_order_id="strategy-worker-entry-1704067200000000000",
            exchange_order_id=None,
            status=OrderStatus.SUBMITTED.value,
            price=Decimal("100"),
            quantity=Decimal("0.10"),
        )
        order.intent_payload = {"order": {"price": "99"}}
        mock_order_repo.add_order(order)

        partial_result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="open",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                exchange_order_id="EX-1",
                cumulative_filled_quantity=Decimal("0.04"),
                cumulative_average_price=Decimal("101"),
                last_fill_quantity=Decimal("0.04"),
                last_fill_price=Decimal("101"),
                fee=Decimal("0.001"),
                fee_asset="BNB",
                event_timestamp=1704067200001,
            )
        )

        assert partial_result["action"] == "applied"
        assert partial_result["fill_quantity"] == Decimal("0.04")
        assert order.exchange_order_id == "EX-1"
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert order.filled_price == Decimal("101")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.04")
        assert mock_order_repo.trades[0].fee == Decimal("0.001")
        assert mock_order_repo.trades[0].fee_asset == "BNB"

        final_result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                exchange_order_id="EX-1",
                cumulative_filled_quantity=Decimal("0.10"),
                cumulative_average_price=Decimal("102.2"),
                last_fill_quantity=Decimal("0.06"),
                last_fill_price=Decimal("103"),
                fee=Decimal("0.002"),
                fee_asset="USDT",
                event_timestamp=1704067200002,
            )
        )

        assert final_result["action"] == "applied"
        assert final_result["fill_quantity"] == Decimal("0.06")
        assert order.status == OrderStatus.FILLED.value
        assert order.filled_quantity == Decimal("0.10")
        assert order.filled_price == Decimal("102.2")
        assert len(mock_order_repo.trades) == 2
        assert mock_order_repo.trades[1].quantity == Decimal("0.06")
        assert mock_order_repo.trades[1].price == Decimal("103")
        assert mock_order_repo.trades[1].fee_asset == "USDT"

        fill_calls = [call for call in journal.log.call_args_list if call.args[0] == "fill"]
        assert len(fill_calls) == 2
        payload = fill_calls[-1].args[1]
        assert payload["signal_price"] == "99"
        assert payload["submitted_price"] == "100"
        assert payload["fill_price"] == "103"
        assert payload["quantity"] == "0.06"
        assert payload["fee_asset"] == "USDT"

    def test_live_order_event_recomputes_delta_price_from_cumulative_average(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id="EX-cumulative-average",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                exchange_order_id="EX-cumulative-average",
                cumulative_filled_quantity=Decimal("0.10"),
                cumulative_average_price=Decimal("102.4"),
            )
        )

        assert result["action"] == "applied"
        assert result["fill_quantity"] == Decimal("0.06")
        assert order.status == OrderStatus.FILLED.value
        assert order.filled_quantity == Decimal("0.10")
        assert order.filled_price == Decimal("102.4")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.06")
        assert mock_order_repo.trades[0].price == Decimal("104")

    def test_live_order_event_does_not_price_catch_up_delta_with_last_fill_price(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id="EX-catch-up-delta",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                exchange_order_id="EX-catch-up-delta",
                cumulative_filled_quantity=Decimal("0.10"),
                cumulative_average_price=Decimal("102.4"),
                last_fill_quantity=Decimal("0.02"),
                last_fill_price=Decimal("110"),
            )
        )

        assert result["action"] == "applied"
        assert order.status == OrderStatus.FILLED.value
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.06")
        assert mock_order_repo.trades[0].price == Decimal("104")

    @pytest.mark.parametrize("exchange_status", ["partial", "filled", "liquidated"])
    def test_live_order_event_catch_up_delta_without_cumulative_average_is_unresolved(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id=f"EX-catch-up-unpriced-{exchange_status}",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-catch-up-unpriced-{exchange_status}",
                cumulative_filled_quantity=Decimal("0.10"),
                last_fill_quantity=Decimal("0.02"),
                last_fill_price=Decimal("110"),
            )
        )

        assert result["action"] == "unresolved_missing_fill_price"
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize("exchange_status", ["partial", "filled", "liquidated"])
    def test_live_order_event_cumulative_fill_above_order_quantity_is_unresolved(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id=f"EX-overfilled-{exchange_status}",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-overfilled-{exchange_status}",
                cumulative_filled_quantity=Decimal("0.12"),
                cumulative_average_price=Decimal("101"),
            )
        )

        assert result["action"] == "unresolved_exchange_fill_exceeds_order_quantity"
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize("exchange_status", ["open", "submitted", "accepted"])
    @pytest.mark.parametrize(
        ("local_filled", "cumulative_filled", "initial_status", "expected_status"),
        [
            (
                Decimal("0"),
                Decimal("0"),
                OrderStatus.SUBMITTED_UNCONFIRMED.value,
                OrderStatus.SUBMITTED.value,
            ),
            (
                Decimal("0.04"),
                Decimal("0.04"),
                OrderStatus.PARTIALLY_FILLED.value,
                OrderStatus.PARTIALLY_FILLED.value,
            ),
        ],
    )
    def test_open_live_order_event_status_preserves_partial_fill_progress(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
        local_filled,
        cumulative_filled,
        initial_status,
        expected_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id=f"EX-open-{exchange_status}-{local_filled}",
            status=initial_status,
            quantity=Decimal("0.10"),
            filled_quantity=local_filled,
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-open-{exchange_status}-{local_filled}",
                cumulative_filled_quantity=cumulative_filled,
            )
        )
        second_result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-open-{exchange_status}-{local_filled}",
                cumulative_filled_quantity=cumulative_filled,
            )
        )

        assert result["action"] == "applied"
        assert second_result["action"] == "applied"
        assert order.status == expected_status
        assert order.filled_quantity == local_filled
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize(
        "exchange_status",
        ["open", "partially_filled", "filled", "canceled", "liquidated"],
    )
    def test_last_fill_only_live_order_events_are_unresolved_and_idempotent(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id=f"EX-last-only-{exchange_status}",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)
        event = ExchangeOrderEvent(
            status=exchange_status,
            product_id=order.product_id,
            exchange_order_id=f"EX-last-only-{exchange_status}",
            last_fill_quantity=Decimal("0.02"),
            last_fill_price=Decimal("101"),
        )

        first_result = engine.process_exchange_order_event(event)
        second_result = engine.process_exchange_order_event(event)

        assert first_result["action"] == "unresolved_last_fill_without_cumulative_quantity"
        assert second_result["action"] == "unresolved_last_fill_without_cumulative_quantity"
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert order.filled_price == Decimal("100")
        assert mock_order_repo.trades == []

    def test_live_order_event_exchange_lookup_is_scoped_to_event_product(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        binance_order = order_factory(
            order_id="binance-order",
            exchange_order_id="reused-exchange-id",
            exchange_id="BINANCE",
            product_id="BINANCE:BTCUSDT-PERP",
            status=OrderStatus.SUBMITTED.value,
            quantity=Decimal("0.10"),
        )
        bybit_order = order_factory(
            order_id="bybit-order",
            exchange_order_id="reused-exchange-id",
            exchange_id="BYBIT",
            product_id="BYBIT:BTCUSDT-PERP",
            status=OrderStatus.SUBMITTED.value,
            quantity=Decimal("0.10"),
        )
        mock_order_repo.add_order(binance_order)
        mock_order_repo.add_order(bybit_order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id="BYBIT:BTCUSDT-PERP",
                exchange_order_id="reused-exchange-id",
                cumulative_filled_quantity=Decimal("0.10"),
                cumulative_average_price=Decimal("101"),
            )
        )

        assert result["action"] == "applied"
        assert result["order_id"] == "bybit-order"
        assert bybit_order.status == OrderStatus.FILLED.value
        assert bybit_order.filled_quantity == Decimal("0.10")
        assert binance_order.status == OrderStatus.SUBMITTED.value
        assert binance_order.filled_quantity == Decimal("0")

    @pytest.mark.parametrize(
        ("exchange_status", "expected_status"),
        [
            ("canceled", OrderStatus.CANCELLED.value),
            ("expired", OrderStatus.FAILED.value),
        ],
    )
    def test_terminal_live_order_event_records_fill_before_terminal_status(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
        expected_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id="EX-terminal",
            status=OrderStatus.SUBMITTED.value,
            price=Decimal("100"),
            quantity=Decimal("0.10"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id="EX-terminal",
                cumulative_filled_quantity=Decimal("0.03"),
                cumulative_average_price=Decimal("101"),
                fee=Decimal("0.01"),
                fee_asset="BTC",
            )
        )

        assert result["action"] == "applied"
        assert result["fill_quantity"] == Decimal("0.03")
        assert order.status == expected_status
        assert order.filled_quantity == Decimal("0.03")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].fee_asset == "BTC"

    @pytest.mark.parametrize("exchange_status", ["filled", "liquidated"])
    def test_terminal_filled_live_order_event_with_partial_cumulative_is_unresolved(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id=f"EX-underfilled-terminal-{exchange_status}",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-underfilled-terminal-{exchange_status}",
                cumulative_filled_quantity=Decimal("0.04"),
                cumulative_average_price=Decimal("100"),
            )
        )

        assert result["action"] == "unresolved_terminal_fill_quantity_below_order_quantity"
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize("exchange_status", ["filled", "liquidated"])
    @pytest.mark.parametrize(
        "event_kwargs",
        [
            {},
            {"last_fill_quantity": Decimal("0")},
        ],
    )
    def test_terminal_filled_live_order_event_without_fill_quantity_is_unresolved_until_full(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
        event_kwargs,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id=f"EX-{exchange_status}",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-{exchange_status}",
                **event_kwargs,
            )
        )

        assert result["action"] == "unresolved_missing_terminal_fill_quantity"
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert mock_order_repo.trades == []

        order.filled_quantity = Decimal("0.10")
        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id=f"EX-{exchange_status}",
                **event_kwargs,
            )
        )

        assert result["action"] == "applied"
        expected_status = (
            OrderStatus.FILLED.value
            if exchange_status == "filled"
            else OrderStatus.LIQUIDATED.value
        )
        assert order.status == expected_status
        assert order.filled_quantity == Decimal("0.10")
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize(
        ("exchange_status", "expected_status"),
        [
            ("rejected", "failed"),
            ("failed", "failed"),
            ("cancelled", OrderStatus.CANCELLED.value),
        ],
    )
    def test_terminal_live_order_event_without_fill_updates_status_only(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
        expected_status,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id="EX-status",
            status=OrderStatus.SUBMITTED.value,
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status=exchange_status,
                product_id=order.product_id,
                exchange_order_id="EX-status",
            )
        )

        assert result["action"] == "applied"
        assert order.status == expected_status
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize(
        ("local_filled", "event_kwargs", "expected_action"),
        [
            (
                Decimal("0"),
                {
                    "status": "filled",
                    "cumulative_filled_quantity": Decimal("0.01"),
                },
                "unresolved_missing_fill_price",
            ),
            (
                Decimal("0.02"),
                {
                    "status": "filled",
                    "cumulative_filled_quantity": Decimal("0.01"),
                    "cumulative_average_price": Decimal("100"),
                },
                "unresolved_local_fill_exceeds_exchange",
            ),
            (
                Decimal("0.02"),
                {
                    "status": "mystery",
                    "cumulative_filled_quantity": Decimal("0.03"),
                    "cumulative_average_price": Decimal("100"),
                },
                "unknown_status",
            ),
        ],
    )
    def test_live_order_event_unresolved_or_unknown_does_not_mutate_state(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        local_filled,
        event_kwargs,
        expected_action,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            exchange_order_id="EX-unresolved",
            status=OrderStatus.PARTIALLY_FILLED.value,
            filled_quantity=local_filled,
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                product_id=order.product_id,
                exchange_order_id="EX-unresolved",
                **event_kwargs,
            )
        )

        assert result["action"] == expected_action
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == local_filled
        assert order.filled_price == Decimal("100")
        assert mock_order_repo.trades == []

    def test_live_order_event_unknown_order_returns_without_creating_local_state(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id="BINANCE:BTCUSDT-PERP",
                client_order_id="missing-client",
                exchange_order_id="missing-exchange",
                cumulative_filled_quantity=Decimal("0.01"),
                cumulative_average_price=Decimal("100"),
            )
        )

        assert result["action"] == "unknown_order"
        assert mock_order_repo.orders == {}
        assert mock_order_repo.trades == []


class TestExecutionErrorHandling:
    """Tests for error handling during execution."""

    def test_adapter_failure_marks_order_failed(
        self, execution_engine, signal_factory, mock_exchange_adapter, mock_order_repo
    ):
        """Adapter failure should mark order as failed."""
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")

        signal = signal_factory(price=Decimal("42000"))
        order_id = execution_engine.execute_signal(signal)

        assert order_id is None

        # Order should be in repo with failed status
        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1

    def test_adapter_failure_fails_precreated_conditional_orders(
        self, execution_engine, signal_factory, mock_exchange_adapter, mock_order_repo
    ):
        """Entry placement failure must not leave pre-created SL/TP/trailing orders non-terminal."""
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
            take_profit=Decimal("43000"),
            trailing_distance=Decimal("100"),
        )
        result = execution_engine.execute_signal(signal)

        assert result is None
        orders = list(mock_order_repo.orders.values())
        assert {order.type for order in orders} == {
            "limit",
            "stop_loss",
            "take_profit",
            "trailing_stop",
        }
        assert all(order.status == "failed" for order in orders)
        conditional_orders = [o for o in orders if o.type != "limit"]
        assert len(conditional_orders) == 3

    def test_adapter_failure_returns_none(
        self, execution_engine, signal_factory, mock_exchange_adapter
    ):
        """Adapter failure should return None."""
        mock_exchange_adapter.set_should_fail(True, "Insufficient funds")

        signal = signal_factory(price=Decimal("42000"))
        result = execution_engine.execute_signal(signal)

        assert result is None


class TestAuditedExecution:
    """Tests for opt-in fail-stop audit execution path."""

    def test_requires_session_factory(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="requires db_session_factory"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

    def test_success_writes_intent_and_outcome(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )
        signal = signal_factory(price=Decimal("42000"), quantity=Decimal("0.25"))

        order_id = engine.execute_signal(signal)

        assert order_id is not None
        order = mock_order_repo.orders[order_id]
        coid = parse_client_order_id(order.client_order_id)
        assert coid.strategy_id == signal.strategy_id
        assert coid.instance_id == "execution"
        assert coid.action == "long"
        assert order.intent_payload["order"]["quantity"] == "0.25"
        assert order.intent_payload["order"]["price"] == "42000"
        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id.startswith("MOCK-")
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.client_order_id == order.client_order_id
        assert audit.intent_payload["order"]["client_order_id"] == order.client_order_id
        assert audit.outcome_payload["status"] == "placed"
        assert audit.outcome_payload["exchange_order_id"].startswith("MOCK-")
        assert audit.order_id == order.id
        assert audit_session.flush.call_count == 1
        assert audit_session.commit.call_count == 2

    def test_exchange_failure_writes_outcome_then_raises(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        with pytest.raises(ExchangeError, match="Connection timeout"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.order_id == failed_orders[0].id
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Connection timeout",
        }
        assert audit_session.flush.call_count == 1
        assert audit_session.commit.call_count == 2

    def test_intent_audit_failure_stops_before_external_order(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_db_session.flush.side_effect = RuntimeError("intent audit failed")
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="intent audit failed"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        assert mock_exchange_adapter.open_orders == []
        mock_db_session.rollback.assert_called_once()

    def test_success_outcome_audit_failure_raises_after_external_order(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_db_session.commit.side_effect = [None, RuntimeError("outcome audit failed")]
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="outcome audit failed"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        assert len(mock_exchange_adapter.open_orders) == 1
        audit = mock_db_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload["status"] == "placed"
        mock_db_session.rollback.assert_called_once()

    def test_exchange_failure_outcome_audit_failure_raises_audit_error(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.set_should_fail(True, "Connection timeout")
        mock_db_session.commit.side_effect = [None, RuntimeError("outcome audit failed")]
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(RuntimeError, match="outcome audit failed"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = mock_db_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Connection timeout",
        }
        mock_db_session.rollback.assert_called_once()

    def test_existing_client_order_id_returns_existing_order_without_resubmit(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory, order_factory
    ):
        client_order_id = generate_client_order_id(
            "test_strategy",
            "execution",
            "long",
            clock_ns=lambda: 1704067200000000000,
        )
        existing_order = order_factory(
            order_id="existing-order",
            client_order_id=client_order_id,
            exchange_order_id="EX-EXISTING",
        )
        mock_order_repo.add_order(existing_order)
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        result = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                metadata={"client_order_id": client_order_id},
            )
        )

        assert result == "existing-order"
        assert mock_exchange_adapter.open_orders == []
        mock_db_session.add.assert_not_called()

    def test_invalid_metadata_client_order_id_raises(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
        )

        with pytest.raises(ValueError, match="client_order_id"):
            engine.execute_signal(
                signal_factory(
                    price=Decimal("42000"),
                    metadata={"client_order_id": "not-valid"},
                )
            )

        assert mock_exchange_adapter.open_orders == []

    def test_list_recoverable_client_orders_returns_inflight_client_orders(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
        )
        recoverable = order_factory(
            order_id="recoverable",
            client_order_id="client-1",
            status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
        )
        partially_filled = order_factory(
            order_id="partially-filled",
            client_order_id="client-partial",
            status=OrderStatus.PARTIALLY_FILLED.value,
        )
        closed = order_factory(
            order_id="closed",
            client_order_id="client-2",
            status="closed",
        )
        no_client_id = order_factory(
            order_id="missing-client-id",
            client_order_id=None,
            status=OrderStatus.SUBMITTED.value,
        )
        mock_order_repo.add_order(recoverable)
        mock_order_repo.add_order(partially_filled)
        mock_order_repo.add_order(closed)
        mock_order_repo.add_order(no_client_id)

        assert engine.list_recoverable_client_orders() == [recoverable, partially_filled]

    def test_record_recoverable_order_scan_writes_reconcile_event(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
        )
        new_order = order_factory(
            order_id="new-order",
            client_order_id="client-new",
            status=OrderStatus.NEW.value,
            exchange_order_id=None,
        )
        submitted_order = order_factory(
            order_id="submitted-order",
            client_order_id="client-submitted",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-1",
        )
        closed_order = order_factory(
            order_id="closed-order",
            client_order_id="client-closed",
            status=OrderStatus.CANCELLED.value,
        )
        mock_order_repo.add_order(new_order)
        mock_order_repo.add_order(submitted_order)
        mock_order_repo.add_order(closed_order)

        payload = engine.record_recoverable_order_scan()

        assert payload["recoverable_count"] == 2
        assert payload["status_counts"] == {
            OrderStatus.NEW.value: 1,
            OrderStatus.SUBMITTED.value: 1,
        }
        assert [order["order_id"] for order in payload["orders"]] == [
            "new-order",
            "submitted-order",
        ]
        event = mock_db_session.add.call_args.args[0]
        assert event.event_type == "reconcile"
        assert event.event_subtype == "startup_recovery_scan"
        assert event.payload == payload
        mock_db_session.commit.assert_called_once()
        mock_db_session.rollback.assert_not_called()

    def test_record_recoverable_order_scan_requires_session_factory(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
        )

        with pytest.raises(RuntimeError, match="requires db_session_factory"):
            engine.record_recoverable_order_scan()

    def test_record_recoverable_order_scan_rolls_back_on_event_failure(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        mock_db_session.add.side_effect = RuntimeError("event write failed")
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
        )
        mock_order_repo.add_order(
            order_factory(
                order_id="recoverable",
                client_order_id="client-1",
                status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
            )
        )

        with pytest.raises(RuntimeError, match="event write failed"):
            engine.record_recoverable_order_scan()

        mock_db_session.rollback.assert_called_once()

    def test_reconcile_recoverable_client_orders_records_exchange_snapshots(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
        )
        found_order = order_factory(
            order_id="found-local",
            client_order_id="client-found",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
        )
        missing_order = order_factory(
            order_id="missing-local",
            client_order_id="client-missing",
            status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
            exchange_order_id=None,
        )
        ignored_order = order_factory(
            order_id="closed-local",
            client_order_id="client-closed",
            status=OrderStatus.CANCELLED.value,
        )
        mock_order_repo.add_order(found_order)
        mock_order_repo.add_order(missing_order)
        mock_order_repo.add_order(ignored_order)
        mock_exchange_adapter.open_orders.append(found_order)

        payload = engine.reconcile_recoverable_client_orders()

        assert payload["recoverable_count"] == 2
        assert payload["result_counts"] == {
            "exchange_found": 1,
            "exchange_not_found": 1,
        }
        assert payload["decision_counts"] == {
            "exchange_open": 1,
            "exchange_unknown": 1,
        }
        assert payload["unresolved_count"] == 0
        assert payload["verification_blocked_count"] == 1
        assert payload["results"][0] == {
            "order_id": "found-local",
            "client_order_id": "client-found",
            "local_status": OrderStatus.SUBMITTED.value,
            "product_id": found_order.product_id,
            "strategy_id": found_order.strategy_id,
            "local_exchange_order_id": "EX-LOCAL",
            "result": "exchange_found",
            "decision": "exchange_open",
            "exchange_order_id": "EX-LOCAL",
            "exchange_status": OrderStatus.SUBMITTED.value,
            "repair_action": "restored_tracking",
            "repair_reason": None,
            "unresolved": False,
            "verification_blocked": False,
        }
        assert payload["results"][1] == {
            "order_id": "missing-local",
            "client_order_id": "client-missing",
            "local_status": OrderStatus.SUBMITTED_UNCONFIRMED.value,
            "product_id": missing_order.product_id,
            "strategy_id": missing_order.strategy_id,
            "local_exchange_order_id": None,
            "result": "exchange_not_found",
            "decision": "exchange_unknown",
            "exchange_order_id": None,
            "exchange_status": None,
            "repair_action": "none",
            "repair_reason": "exchange_snapshot_unavailable",
            "unresolved": False,
            "verification_blocked": True,
        }
        event = mock_db_session.add.call_args.args[0]
        assert event.event_type == "reconcile"
        assert event.event_subtype == "startup_exchange_reconcile"
        assert event.payload == payload
        mock_db_session.commit.assert_called_once()
        mock_db_session.rollback.assert_not_called()

    def test_reconcile_recoverable_client_orders_restores_exchange_open_order(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-open",
            client_order_id="client-open",
            status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
            exchange_order_id=None,
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-OPEN",
                status="open",
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-OPEN"
        assert order.last_reconciled_at is not None
        assert payload["decision_counts"] == {"exchange_open": 1}
        assert payload["results"][0]["local_status"] == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert payload["results"][0]["local_exchange_order_id"] is None
        assert payload["results"][0]["repair_action"] == "restored_tracking"

    def test_reconcile_recoverable_client_orders_records_open_partial_fill_delta(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-partial",
            client_order_id="client-partial",
            status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
            exchange_order_id=None,
            quantity=Decimal("0.50"),
            filled_quantity=Decimal("0.10"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-PARTIAL",
                status="open",
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("160"),
                fee=Decimal("0.08"),
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-PARTIAL"
        assert order.filled_quantity == Decimal("0.25")
        assert order.filled_price == Decimal("160")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.15")
        assert mock_order_repo.trades[0].price == Decimal("200")
        assert mock_order_repo.trades[0].fee == Decimal("0")
        assert payload["decision_counts"] == {"exchange_open": 1}
        assert payload["results"][0]["local_status"] == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert payload["results"][0]["local_exchange_order_id"] is None
        assert (
            payload["results"][0]["repair_action"]
            == "recorded_partial_fill_and_restored_tracking"
        )

    def test_reconcile_recoverable_client_orders_flags_open_missing_price_partial_unresolved(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-open-missing-price",
            client_order_id="client-open-missing-price",
            status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
            exchange_order_id=None,
            quantity=Decimal("0.50"),
            filled_quantity=Decimal("0.10"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-OPEN-MISSING-PRICE",
                status="open",
                filled_quantity=Decimal("0.25"),
                average_price=None,
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-OPEN-MISSING-PRICE"
        assert order.last_reconciled_at is None
        assert mock_order_repo.trades == []
        assert payload["unresolved_count"] == 1
        assert payload["results"][0]["unresolved"] is True
        assert (
            payload["results"][0]["repair_action"]
            == "unresolved_open_missing_fill_price"
        )
        assert (
            payload["results"][0]["repair_reason"]
            == "exchange_snapshot_missing_fill_price"
        )

    def test_reconcile_recoverable_client_orders_flags_open_local_overfill_unresolved(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-open-local-overfill",
            client_order_id="client-open-local-overfill",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.50"),
            filled_quantity=Decimal("0.25"),
            filled_price=Decimal("160"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-OPEN-LESS-FILLED",
                status="open",
                filled_quantity=Decimal("0.10"),
                average_price=Decimal("100"),
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-OPEN-LESS-FILLED"
        assert order.last_reconciled_at is None
        assert mock_order_repo.trades == []
        assert payload["unresolved_count"] == 1
        assert payload["results"][0]["unresolved"] is True
        assert (
            payload["results"][0]["repair_action"]
            == "unresolved_open_local_fill_exceeds_exchange"
        )
        assert (
            payload["results"][0]["repair_reason"]
            == "exchange_filled_quantity_less_than_local"
        )

    @pytest.mark.parametrize("exchange_status", ["open", "closed", "canceled", "expired"])
    @pytest.mark.parametrize(
        ("fill_state", "snapshot_filled", "snapshot_average", "expected_unresolved"),
        [
            ("delta_priced", Decimal("0.25"), Decimal("160"), False),
            ("delta_unpriced", Decimal("0.25"), None, True),
            ("delta_zero", Decimal("0.10"), Decimal("100"), False),
            ("delta_negative", Decimal("0.05"), Decimal("100"), True),
        ],
    )
    def test_reconcile_recoverable_client_orders_fill_delta_invariants(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
        fill_state,
        snapshot_filled,
        snapshot_average,
        expected_unresolved,
    ):
        local_filled = Decimal("0.10")
        local_average = Decimal("100")
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id=f"recoverable-{exchange_status}-{fill_state}",
            client_order_id=f"client-{exchange_status}-{fill_state}",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.50"),
            filled_quantity=local_filled,
            filled_price=local_average,
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=f"EX-{exchange_status.upper()}-{fill_state.upper()}",
                status=exchange_status,
                filled_quantity=snapshot_filled,
                average_price=snapshot_average,
            )
        )

        first_payload = engine.reconcile_recoverable_client_orders()
        first_result = first_payload["results"][0]
        trade_count_after_first = len(mock_order_repo.trades)

        assert first_result["unresolved"] is expected_unresolved
        assert (order.last_reconciled_at is None) is expected_unresolved
        assert (first_payload["unresolved_count"] == 1) is expected_unresolved
        assert first_payload["unresolved_count"] in {0, 1}

        unrecorded_exchange_fill = snapshot_filled > local_filled and trade_count_after_first == 0
        if unrecorded_exchange_fill:
            assert first_result["unresolved"] is True

        if fill_state == "delta_negative":
            assert first_result["repair_reason"] == "exchange_filled_quantity_less_than_local"
            assert first_result["unresolved"] is True

        if exchange_status == "closed" and fill_state == "delta_zero":
            assert first_result["repair_reason"] == "exchange_fill_already_recorded"

        second_payload = engine.reconcile_recoverable_client_orders()

        assert len(mock_order_repo.trades) == trade_count_after_first
        if expected_unresolved:
            assert second_payload["unresolved_count"] == first_payload["unresolved_count"]
            assert second_payload["results"][0]["repair_action"] == first_result["repair_action"]

    def test_startup_reconcile_after_restart_restores_tracking_without_resubmit(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        client_order_id = generate_client_order_id(
            "test_strategy",
            "execution",
            "long",
            clock_ns=lambda: 1704067200000000000,
        )
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("42000"),
            metadata={"client_order_id": client_order_id},
        )
        first_engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order_id = first_engine.execute_signal(signal)
        persisted_order = mock_order_repo.get_order(order_id)
        assert persisted_order is not None
        assert len(mock_exchange_adapter.open_orders) == 1

        restarted_adapter = type(mock_exchange_adapter)()
        restarted_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=persisted_order.exchange_order_id,
                status="open",
            )
        )
        restarted_engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=restarted_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )

        payload = restarted_engine.reconcile_recoverable_client_orders()
        repeated_result = restarted_engine.execute_signal(signal)

        assert payload["decision_counts"] == {"exchange_open": 1}
        assert payload["results"][0]["repair_action"] == "restored_tracking"
        assert persisted_order.status == OrderStatus.SUBMITTED.value
        assert repeated_result == order_id
        assert restarted_adapter.open_orders == []

    def test_reconcile_recoverable_client_orders_fills_from_closed_exchange_snapshot(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-filled",
            client_order_id="client-filled",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.25"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-FILLED",
                status="closed",
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("42010.5"),
                fee=Decimal("0.11"),
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.FILLED.value
        assert order.filled_quantity == Decimal("0.25")
        assert order.filled_price == Decimal("42010.5")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].fee == Decimal("0.11")
        assert payload["decision_counts"] == {"exchange_closed": 1}
        assert payload["results"][0]["repair_action"] == "filled_from_exchange_snapshot"

    def test_reconcile_recoverable_client_orders_fills_only_terminal_delta(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-terminal-delta",
            client_order_id="client-terminal-delta",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.25"),
            filled_quantity=Decimal("0.10"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-FILLED",
                status="closed",
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("160"),
                fee=Decimal("0.05"),
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.FILLED.value
        assert order.filled_quantity == Decimal("0.25")
        assert order.filled_price == Decimal("160")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.15")
        assert mock_order_repo.trades[0].price == Decimal("200")
        assert mock_order_repo.trades[0].fee == Decimal("0")
        assert payload["decision_counts"] == {"exchange_closed": 1}
        assert payload["results"][0]["repair_action"] == "filled_from_exchange_snapshot"

    def test_reconcile_recoverable_client_orders_marks_closed_without_fake_fill(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
        )
        order = order_factory(
            order_id="recoverable-no-fill",
            client_order_id="client-no-fill",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-CLOSED",
                status="closed",
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.FILLED.value
        assert order.filled_quantity in (None, Decimal("0"))
        assert mock_order_repo.trades == []
        assert payload["results"][0]["repair_action"] == "marked_filled_without_fill"
        assert (
            payload["results"][0]["repair_reason"]
            == "exchange_snapshot_missing_fill_details"
        )

    @pytest.mark.parametrize(
        ("exchange_status", "expected_status", "expected_action"),
        [
            ("filled", OrderStatus.FILLED.value, "filled_from_exchange_snapshot"),
            ("closed", OrderStatus.FILLED.value, "filled_from_exchange_snapshot"),
            ("canceled", OrderStatus.CANCELLED.value, "filled_delta_and_marked_cancelled"),
            ("cancelled", OrderStatus.CANCELLED.value, "filled_delta_and_marked_cancelled"),
            ("rejected", OrderStatus.FAILED.value, "filled_delta_and_marked_failed"),
            ("expired", OrderStatus.FAILED.value, "filled_delta_and_marked_failed"),
            ("failed", OrderStatus.FAILED.value, "filled_delta_and_marked_failed"),
        ],
    )
    def test_reconcile_recoverable_client_orders_records_terminal_partial_fill(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
        expected_status,
        expected_action,
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id=f"recoverable-{exchange_status}",
            client_order_id=f"client-{exchange_status}",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.50"),
            filled_quantity=Decimal("0.10"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=f"EX-{exchange_status.upper()}",
                status=exchange_status,
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("160"),
                fee=Decimal("0.08"),
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == expected_status
        assert order.filled_quantity == Decimal("0.25")
        assert order.filled_price == Decimal("160")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.15")
        assert mock_order_repo.trades[0].price == Decimal("200")
        assert mock_order_repo.trades[0].fee == Decimal("0")
        assert payload["decision_counts"] == {"exchange_closed": 1}
        assert payload["unresolved_count"] == 0
        assert payload["results"][0]["repair_action"] == expected_action

    @pytest.mark.parametrize("exchange_status", ["closed", "cancelled", "expired"])
    def test_reconcile_recoverable_client_orders_leaves_missing_price_partial_unresolved(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        exchange_status,
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-cancelled-missing-price",
            client_order_id="client-cancelled-missing-price",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.50"),
            filled_quantity=Decimal("0.10"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=f"EX-{exchange_status.upper()}",
                status=exchange_status,
                filled_quantity=Decimal("0.25"),
                average_price=None,
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.last_reconciled_at is None
        assert mock_order_repo.trades == []
        assert payload["unresolved_count"] == 1
        assert payload["results"][0]["unresolved"] is True
        assert payload["results"][0]["repair_action"] == "unresolved_missing_fill_price"
        assert (
            payload["results"][0]["repair_reason"]
            == "exchange_snapshot_missing_fill_price"
        )

    def test_reconcile_recoverable_client_orders_is_idempotent_for_terminal_partial_fill(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )
        order = order_factory(
            order_id="recoverable-cancelled-idempotent",
            client_order_id="client-cancelled-idempotent",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
            quantity=Decimal("0.50"),
            filled_quantity=Decimal("0.10"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-CANCELLED",
                status="cancelled",
                filled_quantity=Decimal("0.25"),
                average_price=Decimal("160"),
            )
        )

        first_payload = engine.reconcile_recoverable_client_orders()
        second_payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.CANCELLED.value
        assert len(mock_order_repo.trades) == 1
        assert first_payload["results"][0]["repair_action"] == "filled_delta_and_marked_cancelled"
        assert second_payload["recoverable_count"] == 0

    def test_reconcile_recoverable_client_orders_fails_local_only_order_and_prevents_resubmit(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        signal_factory,
    ):
        client_order_id = generate_client_order_id(
            "test_strategy",
            "execution",
            "long",
            clock_ns=lambda: 1704067200000000000,
        )
        order = order_factory(
            order_id="recoverable-local-only",
            client_order_id=client_order_id,
            status=OrderStatus.NEW.value,
            exchange_order_id=None,
        )
        mock_order_repo.add_order(order)
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            is_backtest=True,
            audit_external_orders=True,
        )

        payload = engine.reconcile_recoverable_client_orders()
        assert mock_order_repo.get_order_by_client_order_id(client_order_id) is order
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("42000"),
            metadata={"client_order_id": client_order_id},
        )
        result = engine.execute_signal(signal)

        assert order.status == "failed"
        assert order.last_reconciled_at is not None
        assert payload["decision_counts"] == {"local_only": 1}
        assert payload["results"][0]["repair_action"] == "marked_failed"
        assert result == "recoverable-local-only"
        assert mock_exchange_adapter.open_orders == []

    def test_reconcile_recoverable_client_orders_requires_session_factory(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
        )

        with pytest.raises(RuntimeError, match="requires db_session_factory"):
            engine.reconcile_recoverable_client_orders()

    def test_reconcile_recoverable_client_orders_distinguishes_unsupported_lookup(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        def unsupported_lookup(client_order_id, product_id):
            raise ExchangeOrderLookupUnsupported("unsupported")

        mock_exchange_adapter.get_order_by_client_id = unsupported_lookup
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
        )
        order = order_factory(
            order_id="recoverable",
            client_order_id="client-1",
            status=OrderStatus.NEW.value,
        )
        mock_order_repo.add_order(order)

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.NEW.value
        assert order.last_reconciled_at is None
        assert payload["result_counts"] == {"exchange_lookup_unsupported": 1}
        assert payload["decision_counts"] == {"exchange_unknown": 1}
        assert payload["verification_blocked_count"] == 1
        assert payload["results"][0]["result"] == "exchange_lookup_unsupported"
        assert payload["results"][0]["repair_action"] == "none"
        assert payload["results"][0]["repair_reason"] == "exchange_lookup_unsupported"
        assert payload["results"][0]["verification_blocked"] is True

    def test_reconcile_recoverable_client_orders_flags_unknown_exchange_status_blocked(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, order_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
        )
        order = order_factory(
            order_id="recoverable-weird-status",
            client_order_id="client-weird-status",
            status=OrderStatus.SUBMITTED.value,
            exchange_order_id="EX-LOCAL",
        )
        mock_order_repo.add_order(order)

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id: (
            ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-WEIRD",
                status="weird_status",
            )
        )

        payload = engine.reconcile_recoverable_client_orders()

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.last_reconciled_at is None
        assert payload["unresolved_count"] == 0
        assert payload["verification_blocked_count"] == 1
        assert payload["results"][0]["decision"] == "exchange_unknown"
        assert payload["results"][0]["repair_action"] == "none"
        assert payload["results"][0]["repair_reason"] == "exchange_status_unrecognized"
        assert payload["results"][0]["verification_blocked"] is True

    @pytest.mark.parametrize(
        ("local_status", "exchange_status", "expected"),
        [
            (OrderStatus.NEW.value, None, "local_only"),
            (OrderStatus.SUBMITTED.value, None, "exchange_unknown"),
            (OrderStatus.SUBMITTED.value, "open", "exchange_open"),
            (OrderStatus.SUBMITTED.value, "PARTIALLY_FILLED", "exchange_open"),
            (OrderStatus.SUBMITTED.value, "closed", "exchange_closed"),
            (OrderStatus.SUBMITTED.value, "CANCELLED", "exchange_closed"),
            (OrderStatus.SUBMITTED.value, "mystery", "exchange_unknown"),
        ],
    )
    def test_reconcile_decision_categories(self, local_status, exchange_status, expected):
        assert ExecutionEngine._reconcile_decision(local_status, exchange_status) == expected


class TestCancelOrder:
    """Tests for execution-level cancellation."""

    def test_cancel_order_returns_false_when_order_missing(
        self, execution_engine, mock_exchange_adapter
    ):
        assert execution_engine.cancel_order("missing") is False
        assert mock_exchange_adapter.open_orders == []

    def test_cancel_order_calls_adapter_and_marks_cancelled(
        self, execution_engine, signal_factory, mock_order_repo, mock_exchange_adapter
    ):
        order_id = execution_engine.execute_signal(signal_factory(price=None, value=None))
        order = mock_order_repo.orders[order_id]

        result = execution_engine.cancel_order(order_id)

        assert result is True
        assert order.status == OrderStatus.CANCELLED.value
        assert mock_exchange_adapter.open_orders == []

    def test_cancel_order_prefers_client_order_id(
        self, execution_engine, signal_factory, mock_order_repo, mock_exchange_adapter
    ):
        order_id = execution_engine.execute_signal(signal_factory(price=None, value=None))
        order = mock_order_repo.orders[order_id]
        order.client_order_id = "client-123"
        order.exchange_order_id = "stale-exchange-id"

        result = execution_engine.cancel_order(order_id)

        assert result is True
        assert order.status == OrderStatus.CANCELLED.value
        assert mock_exchange_adapter.open_orders == []

    def test_cancel_order_is_idempotent_for_cancelled_order(
        self, execution_engine, signal_factory, mock_order_repo, mock_exchange_adapter
    ):
        order_id = execution_engine.execute_signal(signal_factory(price=None, value=None))
        order = mock_order_repo.orders[order_id]
        order.status = OrderStatus.CANCELLED.value

        result = execution_engine.cancel_order(order_id)

        assert result is True
        assert len(mock_exchange_adapter.open_orders) == 1


class TestMarketDataProcessing:
    """Tests for process_market_data (simulated fills)."""

    def test_market_data_triggers_fills(
        self, mock_db_session, mock_clock, mock_exchange_adapter, signal_factory, candlestick_factory
    ):
        """Market data should trigger fills for pending orders."""
        from src.core.repositories import BacktestOrderRepository
        backtest_repo = BacktestOrderRepository(mock_db_session, session_id=1)
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=backtest_repo
        )

        signal = signal_factory(price=Decimal("42000"))
        engine.execute_signal(signal)
        assert len(mock_exchange_adapter.open_orders) == 1

        candle = candlestick_factory(close=Decimal("42100"))
        engine.process_market_data(candle)

        # Order should be filled
        assert len(mock_exchange_adapter.open_orders) == 0

    def test_no_orders_no_fills(self, execution_engine, candlestick_factory):
        """No fills when no pending orders."""
        candle = candlestick_factory()
        # Should not raise
        execution_engine.process_market_data(candle)


class TestConditionalOrderErrorHandling:
    """Tests for SL/TP/Trailing Stop order placement error paths."""

    def test_sl_order_failure_logs_error(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """SL order placement failure should log error but not fail main order."""
        mock_exchange_adapter.set_fail_on_order_types({"stop_loss"})

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should succeed
        assert order_id is not None
        # Entry order placed, SL order failed
        assert len(mock_exchange_adapter.open_orders) == 1
        assert "Failed to place SL order" in caplog.text

    def test_tp_order_failure_logs_error(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """TP order placement failure should log error but not fail main order."""
        mock_exchange_adapter.set_fail_on_order_types({"take_profit"})

        signal = signal_factory(
            price=Decimal("42000"),
            take_profit=Decimal("45000"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should succeed
        assert order_id is not None
        # Entry order placed, TP order failed
        assert len(mock_exchange_adapter.open_orders) == 1
        assert "Failed to place TP order" in caplog.text

    def test_trailing_stop_failure_logs_error(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """Trailing stop order placement failure should log error."""
        mock_exchange_adapter.set_fail_on_order_types({"trailing_stop"})

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
            trailing_distance=Decimal("500"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should succeed
        assert order_id is not None
        assert "Failed to place trailing stop order" in caplog.text

    def test_all_conditional_orders_fail_main_succeeds(
        self, execution_engine, signal_factory, mock_exchange_adapter, caplog
    ):
        """All conditional orders can fail while main order succeeds."""
        mock_exchange_adapter.set_fail_on_order_types(
            {"stop_loss", "take_profit", "trailing_stop"}
        )

        signal = signal_factory(
            price=Decimal("42000"),
            stop_loss=Decimal("41000"),
            take_profit=Decimal("45000"),
            trailing_distance=Decimal("500"),
        )
        order_id = execution_engine.execute_signal(signal)

        # Main order should still succeed
        assert order_id is not None
        assert len(mock_exchange_adapter.open_orders) == 1
        # All conditional order errors logged
        assert "Failed to place SL order" in caplog.text
        assert "Failed to place TP order" in caplog.text
        assert "Failed to place trailing stop order" in caplog.text
