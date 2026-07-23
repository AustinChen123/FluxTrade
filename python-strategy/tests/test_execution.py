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
from copy import copy
import threading
from types import SimpleNamespace

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from prometheus_client import REGISTRY

from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderSnapshot
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.interfaces.exchange import ExchangeError
from src.core.interfaces.exchange import NetworkError
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
            "contract": True,
            "linear": True,
            "inverse": False,
            "contractSize": "1",
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


def _make_order_repo_return_detached_instances(mock_order_repo):
    """Make the mock repo behave like a session-factory repository."""

    def clone_order(order):
        return copy(order) if order is not None else None

    def update_order(order):
        mock_order_repo.orders[order.id] = clone_order(order)

    def update_order_exchange_id(order, exchange_order_id):
        order.exchange_order_id = exchange_order_id
        update_order(order)

    def get_order_by_client_order_id(client_order_id):
        return clone_order(
            next(
                (
                    order
                    for order in mock_order_repo.orders.values()
                    if order.client_order_id == client_order_id
                ),
                None,
            )
        )

    mock_order_repo.update_order = update_order
    mock_order_repo.update_order_exchange_id = update_order_exchange_id
    mock_order_repo.get_order = lambda order_id: clone_order(
        mock_order_repo.orders.get(order_id)
    )
    mock_order_repo.get_order_by_client_order_id = get_order_by_client_order_id


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

    def test_trailing_stop_validation_rejects_group_before_entry_submit(
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
            price=Decimal("50123.40"),
            quantity=Decimal("0.010"),
            trailing_distance=Decimal("100"),
        )

        order_id = engine.execute_signal(signal)

        assert order_id is None
        client.create_order.assert_not_called()
        orders = list(mock_order_repo.orders.values())
        assert {order.type for order in orders} == {"limit", "trailing_stop"}
        assert all(order.status == "failed" for order in orders)

    def test_rithmic_rejects_trailing_protection_before_entry_submit(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        signal = signal_factory(
            product_id="RITHMIC:NQ-202609",
            price=Decimal("20000.25"),
            quantity=Decimal("1"),
            trailing_distance=Decimal("25.00"),
        )

        with pytest.raises(ExchangeError, match="native_bracket_leg_unsupported"):
            engine.execute_signal(signal)

        client.submit.assert_not_called()
        client.submit_bracket.assert_not_called()
        assert all(
            order.status == "failed"
            for order in mock_order_repo.orders.values()
        )

    def test_rithmic_native_bracket_is_atomic_and_not_resubmitted_after_entry_fill(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )

        order_id = engine.execute_signal(
            signal_factory(
                product_id="RITHMIC:NQ-202609",
                price=Decimal("20000.25"),
                quantity=Decimal("1"),
                stop_loss=Decimal("19998.25"),
                take_profit=Decimal("20003.25"),
            )
        )

        entry = mock_order_repo.orders[order_id]
        protections = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        client.submit.assert_not_called()
        client.submit_bracket.assert_called_once()
        assert entry.exchange_order_id == "parent-1"
        assert all(order.status == OrderStatus.SUBMITTED.value for order in protections)
        assert all(
            order.intent_payload["placement_mode"] == "attach-at-entry"
            for order in protections
        )

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                client_order_id=entry.client_order_id,
                exchange_order_id="parent-1",
                cumulative_filled_quantity=Decimal("1"),
                cumulative_average_price=Decimal("20000.25"),
            )
        )

        assert result["action"] == "applied"
        client.submit_bracket.assert_called_once()
        client.submit.assert_not_called()
        assert {
            order.type: order.intent_payload["expected_effective_price"]
            for order in protections
        } == {"stop_loss": "19998.25", "take_profit": "20003.25"}
        assert all(
            order.intent_payload["protection_confirmation"] == "pending_remote_event"
            for order in protections
        )
        for order in protections:
            is_stop = order.type == "stop_loss"
            result = engine.process_exchange_order_event(
                ExchangeOrderEvent(
                    status="open",
                    product_id=entry.product_id,
                    client_order_id=order.client_order_id,
                    exchange_order_id=f"child-{order.type}",
                    raw={
                        "original_basket_id": "parent-1",
                        "price_type": "stop_market" if is_stop else "limit",
                        "trigger_price": "19998.25" if is_stop else None,
                        "price": None if is_stop else "20003.25",
                        "bracket_type": "target_and_stop_static",
                    },
                )
            )
            assert result["action"] == "applied"
        assert {
            order.type: order.intent_payload["effective_price"]
            for order in protections
        } == {"stop_loss": "19998.25", "take_profit": "20003.25"}
        stop = next(order for order in protections if order.type == "stop_loss")
        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="open",
                product_id=entry.product_id,
                client_order_id=stop.client_order_id,
                exchange_order_id="unexpected-child",
                raw={"original_basket_id": "parent-1"},
            )
        )
        assert result["action"] == "unresolved_native_protection_basket_mismatch"
        assert stop.exchange_order_id == "child-stop_loss"
        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="open",
                product_id=entry.product_id,
                client_order_id=stop.client_order_id,
                exchange_order_id="child-stop_loss",
                raw={
                    "original_basket_id": "parent-1",
                    "price_type": "stop_market",
                    "trigger_price": "19998.00",
                    "bracket_type": "target_and_stop_static",
                },
            )
        )
        assert result["action"] == "unresolved_native_protection_price_mismatch"
        assert stop.trigger_price == Decimal("19998.25")
        assert stop.intent_payload["protection_confirmation"] == "conflict"

    def test_rithmic_native_bracket_fill_drift_is_audited_without_lockdown(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )
        order_id = engine.execute_signal(
            signal_factory(
                product_id="RITHMIC:NQ-202609",
                price=Decimal("20000.25"),
                quantity=Decimal("1"),
                stop_loss=Decimal("19998.25"),
            )
        )
        entry = mock_order_repo.orders[order_id]

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                client_order_id=entry.client_order_id,
                exchange_order_id="parent-1",
                cumulative_filled_quantity=Decimal("1"),
                cumulative_average_price=Decimal("20000.75"),
            )
        )

        protection = next(
            order
            for order in mock_order_repo.orders.values()
            if order.type == "stop_loss"
        )
        assert result["action"] == "applied"
        assert protection.status == OrderStatus.SUBMITTED.value
        assert protection.trigger_price == Decimal("19998.25")
        assert protection.intent_payload["requested_price"] == "19998.25"
        assert protection.intent_payload["expected_effective_price"] == "19998.75"
        assert protection.intent_payload["protection_confirmation"] == "pending_remote_event"
        assert protection.intent_payload["price_drift"] == "0.50"
        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="open",
                product_id=entry.product_id,
                client_order_id=protection.client_order_id,
                exchange_order_id="child-stop-1",
                raw={
                    "original_basket_id": "parent-1",
                    "price_type": "stop_market",
                    "trigger_price": "19998.75",
                    "bracket_type": "stop_only_static",
                },
            )
        )
        assert result["action"] == "applied"
        assert protection.trigger_price == Decimal("19998.75")
        assert protection.intent_payload["effective_price"] == "19998.75"
        assert protection.intent_payload["protection_confirmation"] == "confirmed"
        client.cancel.assert_not_called()

    def test_rithmic_native_protection_modify_commits_only_after_confirmation(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
        client.modify_protection.return_value = True
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )
        entry_id = engine.execute_signal(
            signal_factory(
                product_id="RITHMIC:NQ-202609",
                price=Decimal("20000.25"),
                quantity=Decimal("1"),
                stop_loss=Decimal("19998.25"),
            )
        )
        stop = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        stop.exchange_order_id = "child-stop-1"
        mock_order_repo.update_order(stop)

        result = engine.modify_protection(
            entry_id,
            stop_loss=Decimal("19999.00"),
        )

        stored = mock_order_repo.orders[stop.id]
        assert result["effective_price"] == "19999.00"
        assert stored.trigger_price == Decimal("19999.00")
        assert stored.intent_payload["modifications"][-1]["requested_price"] == "19999.00"
        assert stored.intent_payload["modifications"][-1]["status"] == "confirmed"
        event_result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="modify_rejected",
                product_id=stored.product_id,
                client_order_id=stored.client_order_id,
                exchange_order_id="child-stop-1",
            )
        )
        assert event_result["action"] == "applied"
        assert stored.trigger_price == Decimal("19999.00")

    @pytest.mark.parametrize(
        ("result", "error", "reconcile_halted", "fail_ambiguous_persistence"),
        [
            (False, ExchangeError, False, False),
            (
                RuntimeError("Rithmic modify-order result is ambiguous: disconnected"),
                NetworkError,
                True,
                False,
            ),
            (
                RuntimeError("Rithmic modify-order result is ambiguous: disconnected"),
                RuntimeError,
                True,
                True,
            ),
        ],
    )
    def test_rithmic_native_protection_modify_failure_keeps_previous_price(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
        result,
        error,
        reconcile_halted,
        fail_ambiguous_persistence,
    ):
        client = MagicMock()
        client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
        if isinstance(result, Exception):
            client.modify_protection.side_effect = result
        else:
            client.modify_protection.return_value = result
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )
        entry_id = engine.execute_signal(
            signal_factory(
                product_id="RITHMIC:NQ-202609",
                price=Decimal("20000.25"),
                quantity=Decimal("1"),
                stop_loss=Decimal("19998.25"),
            )
        )
        stop = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        stop.exchange_order_id = "child-stop-1"
        mock_order_repo.update_order(stop)
        if fail_ambiguous_persistence:
            update_order = mock_order_repo.update_order

            def fail_ambiguous_attempt(order):
                attempts = (order.intent_payload or {}).get("modifications") or []
                if attempts and attempts[-1]["status"] == "ambiguous":
                    raise RuntimeError("database unavailable")
                update_order(order)

            mock_order_repo.update_order = fail_ambiguous_attempt

        with pytest.raises(error):
            engine.modify_protection(entry_id, stop_loss=Decimal("19999.00"))

        assert mock_order_repo.orders[stop.id].trigger_price == Decimal("19998.25")
        expected_status = "ambiguous" if reconcile_halted else "rejected"
        assert (
            mock_order_repo.orders[stop.id].intent_payload["modifications"][-1]["status"]
            == expected_status
        )
        assert engine._reconcile_halt is reconcile_halted

    def test_ambiguous_native_bracket_submit_requires_authoritative_reconciliation(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        client.submit_bracket.side_effect = RuntimeError(
            "Rithmic bracket-order result is ambiguous: disconnected"
        )
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )

        with pytest.raises(NetworkError, match="ambiguous"):
            engine.execute_signal(
                signal_factory(
                    product_id="RITHMIC:NQ-202609",
                    price=Decimal("20000.25"),
                    quantity=Decimal("1"),
                    stop_loss=Decimal("19998.25"),
                )
            )

        assert engine._reconcile_halt is True
        assert {
            order.status for order in mock_order_repo.orders.values()
        } == {OrderStatus.SUBMITTED_UNCONFIRMED.value}

    def test_native_bracket_post_ack_persistence_failure_halts_for_reconcile(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )
        update_order = mock_order_repo.update_order

        def fail_after_remote_ack(order):
            if client.submit_bracket.called and order.type == "stop_loss":
                raise RuntimeError("database unavailable")
            update_order(order)

        mock_order_repo.update_order = fail_after_remote_ack

        with pytest.raises(RuntimeError, match="database unavailable"):
            engine.execute_signal(
                signal_factory(
                    product_id="RITHMIC:NQ-202609",
                    price=Decimal("20000.25"),
                    quantity=Decimal("1"),
                    stop_loss=Decimal("19998.25"),
                )
            )

        client.submit_bracket.assert_called_once()
        assert engine._reconcile_halt is True

    def test_native_modify_post_confirmation_persistence_failure_stays_pending(
        self,
        mock_db_session,
        mock_clock,
        mock_order_repo,
        signal_factory,
    ):
        client = MagicMock()
        client.submit_bracket.return_value = SimpleNamespace(basket_id="parent-1")
        client.modify_protection.return_value = True
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=client),
        )
        adapter.start_order_event_stream()
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
            rithmic_account_profile="test",
            rithmic_account_id="ACCOUNT",
        )
        entry_id = engine.execute_signal(
            signal_factory(
                product_id="RITHMIC:NQ-202609",
                price=Decimal("20000.25"),
                quantity=Decimal("1"),
                stop_loss=Decimal("19998.25"),
            )
        )
        stop = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        stop.exchange_order_id = "child-stop-1"
        mock_order_repo.update_order(stop)
        update_order = mock_order_repo.update_order

        def fail_confirmed_modify(order):
            attempts = (order.intent_payload or {}).get("modifications") or []
            if attempts and attempts[-1]["status"] == "confirmed":
                raise RuntimeError("database unavailable")
            update_order(order)

        mock_order_repo.update_order = fail_confirmed_modify

        with pytest.raises(RuntimeError, match="database unavailable"):
            engine.modify_protection(entry_id, stop_loss=Decimal("19999.00"))

        assert stop.trigger_price == Decimal("19998.25")
        assert stop.intent_payload["modifications"][-1]["status"] == "pending"
        assert engine._reconcile_halt is True

    def test_fast_fill_event_cannot_be_regressed_by_late_submit_ack(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        signal_factory,
    ):
        _make_order_repo_return_detached_instances(mock_order_repo)
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        event_applied = threading.Event()
        event_threads = []

        def place_order(order):
            def apply_fill():
                engine.process_exchange_order_event(
                    ExchangeOrderEvent(
                        status="filled",
                        product_id=order.product_id,
                        client_order_id=order.client_order_id,
                        exchange_order_id="EX-FAST-FILL",
                        cumulative_filled_quantity=order.quantity,
                        cumulative_average_price=order.price,
                    )
                )
                event_applied.set()

            thread = threading.Thread(target=apply_fill)
            event_threads.append(thread)
            thread.start()
            assert event_applied.wait(1.0)
            return "EX-FAST-FILL"

        mock_exchange_adapter.place_order = MagicMock(side_effect=place_order)

        order_id = engine.execute_signal(
            signal_factory(price=Decimal("42000"), quantity=Decimal("0.01"))
        )
        for thread in event_threads:
            thread.join(timeout=1.0)

        order = mock_order_repo.orders[order_id]
        assert order.status == OrderStatus.FILLED.value
        assert order.exchange_order_id == "EX-FAST-FILL"

    def test_ack_without_client_order_id_preserves_legacy_submitted_transition(
        self,
        execution_engine,
        signal_factory,
    ):
        order = execution_engine.order_manager.create_order(
            signal=signal_factory(),
            side="buy",
            order_type="market",
            quantity=Decimal("0.01"),
        )

        execution_engine._record_order_ack(order, "EX-LEGACY")

        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-LEGACY"

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

    def test_trailing_only_signal_fails_closed_before_audited_entry_submit(
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

        with pytest.raises(ExchangeError, match="trailing_stop_mapping_unsupported"):
            engine.execute_signal(signal, candle=candle)

        client.create_order.assert_not_called()
        orders = list(mock_order_repo.orders.values())
        assert {order.type for order in orders} == {"market", "trailing_stop"}
        assert all(order.status == "failed" for order in orders)

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

    def test_resync_recoverable_order_events_applies_snapshot_through_event_sync_idempotently(
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
            client_order_id="strategy-worker-entry-1704067200000000000",
            exchange_order_id="EX-resync",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)
        snapshot = ExchangeOrderSnapshot(
            client_order_id=order.client_order_id,
            exchange_order_id="EX-resync",
            status="open",
            filled_quantity=Decimal("0.06"),
            average_price=Decimal("102"),
            fee=Decimal("0.01"),
        )
        mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=snapshot)

        first_payload = engine.resync_recoverable_order_events()
        second_payload = engine.resync_recoverable_order_events()

        assert first_payload["recoverable_count"] == 1
        assert first_payload["applied_count"] == 1
        assert first_payload["unresolved_count"] == 0
        assert first_payload["verification_blocked_count"] == 0
        assert second_payload["recoverable_count"] == 1
        assert second_payload["applied_count"] == 1
        assert second_payload["unresolved_count"] == 0
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.06")
        assert order.filled_price == Decimal("102")
        assert len(mock_order_repo.trades) == 1
        assert mock_order_repo.trades[0].quantity == Decimal("0.02")
        assert mock_order_repo.trades[0].price == Decimal("106")
        assert mock_order_repo.trades[0].fee == Decimal("0.01")

    @pytest.mark.parametrize(
        ("snapshot", "expected_action"),
        [
            (
                ExchangeOrderSnapshot(
                    client_order_id="client-resync",
                    exchange_order_id="EX-resync-unresolved",
                    status="filled",
                    filled_quantity=Decimal("0.10"),
                    average_price=None,
                ),
                "unresolved_missing_fill_price",
            ),
            (
                ExchangeOrderSnapshot(
                    client_order_id="client-resync",
                    exchange_order_id="EX-resync-unresolved",
                    status="filled",
                    filled_quantity=Decimal("0.04"),
                    average_price=Decimal("100"),
                ),
                "unresolved_terminal_fill_quantity_below_order_quantity",
            ),
        ],
    )
    def test_resync_recoverable_order_events_surfaces_unresolved_snapshots(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        snapshot,
        expected_action,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            client_order_id="client-resync",
            exchange_order_id="EX-resync-unresolved",
            status=OrderStatus.PARTIALLY_FILLED.value,
            quantity=Decimal("0.10"),
            filled_quantity=Decimal("0.04"),
            filled_price=Decimal("100"),
        )
        mock_order_repo.add_order(order)
        mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=snapshot)

        payload = engine.resync_recoverable_order_events()

        assert payload["recoverable_count"] == 1
        assert payload["applied_count"] == 0
        assert payload["unresolved_count"] == 1
        assert payload["verification_blocked_count"] == 0
        assert payload["results"][0]["action"] == expected_action
        assert order.status == OrderStatus.PARTIALLY_FILLED.value
        assert order.filled_quantity == Decimal("0.04")
        assert mock_order_repo.trades == []

    def test_resync_recoverable_order_events_counts_unknown_status_as_verification_blocked(
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
            client_order_id="client-resync-unknown-status",
            exchange_order_id="EX-resync-unknown-status",
            status=OrderStatus.SUBMITTED.value,
        )
        mock_order_repo.add_order(order)
        mock_exchange_adapter.get_order_by_client_id = MagicMock(
            return_value=ExchangeOrderSnapshot(
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                status="weird_status",
            )
        )

        payload = engine.resync_recoverable_order_events()

        assert payload["recoverable_count"] == 1
        assert payload["applied_count"] == 0
        assert payload["unresolved_count"] == 0
        assert payload["verification_blocked_count"] == 1
        assert payload["results"][0]["action"] == "unknown_status"
        assert payload["results"][0]["verification_blocked"] is True
        assert order.status == OrderStatus.SUBMITTED.value
        assert mock_order_repo.trades == []

    @pytest.mark.parametrize(
        ("lookup_result", "expected_action"),
        [
            (None, "verification_blocked_order_snapshot_missing"),
            (ExchangeOrderLookupUnsupported("unsupported"), "verification_blocked_order_lookup_unsupported"),
            (ExchangeError("network down"), "verification_blocked_order_lookup_failed"),
        ],
    )
    def test_resync_recoverable_order_events_surfaces_verification_blocked_lookup(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        order_factory,
        lookup_result,
        expected_action,
    ):
        engine = self._engine(
            mock_db_session,
            mock_clock,
            mock_exchange_adapter,
            mock_order_repo,
        )
        order = order_factory(
            client_order_id="client-resync-blocked",
            exchange_order_id="EX-resync-blocked",
            status=OrderStatus.SUBMITTED.value,
        )
        mock_order_repo.add_order(order)
        if isinstance(lookup_result, Exception):
            mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup_result)
        else:
            mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=lookup_result)

        payload = engine.resync_recoverable_order_events()

        assert payload["recoverable_count"] == 1
        assert payload["applied_count"] == 0
        assert payload["unresolved_count"] == 0
        assert payload["verification_blocked_count"] == 1
        assert payload["results"][0]["action"] == expected_action
        assert order.status == OrderStatus.SUBMITTED.value
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
        mock_exchange_adapter.set_should_fail(True, "Order rejected")
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        with pytest.raises(ExchangeError, match="Order rejected"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        failed_orders = [o for o in mock_order_repo.orders.values() if o.status == "failed"]
        assert len(failed_orders) == 1
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.order_id == failed_orders[0].id
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Order rejected",
        }
        assert audit_session.flush.call_count == 1
        assert audit_session.commit.call_count == 2

    def test_ambiguous_submit_error_adopts_exchange_order_by_client_id(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-ADOPTED",
                status="open",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        order_id = engine.execute_signal(signal_factory(price=Decimal("42000")))

        order = mock_order_repo.orders[order_id]
        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id == "EX-ADOPTED"
        assert len(mock_exchange_adapter.open_orders) == 0
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload == {
            "status": "placed",
            "exchange_order_id": "EX-ADOPTED",
        }
        mock_exchange_adapter.get_order_by_client_id.assert_called_once_with(
            order.client_order_id,
            order.product_id,
            order_type=order.type,
        )

    def test_ambiguous_submit_error_adoption_audits_detached_repo_exchange_id(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        _make_order_repo_return_detached_instances(mock_order_repo)
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-DETACHED",
                status="open",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        order_id = engine.execute_signal(signal_factory(price=Decimal("42000")))

        stored_order = mock_order_repo.orders[order_id]
        assert stored_order.exchange_order_id == "EX-DETACHED"
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload == {
            "status": "placed",
            "exchange_order_id": "EX-DETACHED",
        }

    def test_ambiguous_validation_error_does_not_attempt_submit_adoption(
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
        engine._validate_order_group = MagicMock(
            side_effect=ExchangeError("Connection timeout before submit")
        )
        mock_exchange_adapter.get_order_by_client_id = MagicMock()

        with pytest.raises(ExchangeError, match="Connection timeout before submit"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        order = next(iter(mock_order_repo.orders.values()))
        assert order.status == "failed"
        assert order.exchange_order_id is None
        assert mock_exchange_adapter.open_orders == []
        mock_exchange_adapter.get_order_by_client_id.assert_not_called()
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Connection timeout before submit",
        }

    def test_deterministic_submit_error_does_not_attempt_adoption(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=ExchangeError("Unknown symbol")
        )
        mock_exchange_adapter.get_order_by_client_id = MagicMock()
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        with pytest.raises(ExchangeError, match="Unknown symbol"):
            engine.execute_signal(signal_factory(price=Decimal("42000")))

        order = next(iter(mock_order_repo.orders.values()))
        assert order.status == "failed"
        assert order.exchange_order_id is None
        mock_exchange_adapter.get_order_by_client_id.assert_not_called()
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload == {
            "status": "failed",
            "error": "Unknown symbol",
        }

    @pytest.mark.parametrize(
        ("lookup_result", "expected_action"),
        [
            (None, "verification_blocked_order_snapshot_missing"),
            (
                ExchangeOrderLookupUnsupported("unsupported"),
                "verification_blocked_order_lookup_unsupported",
            ),
            (ExchangeError("lookup failed"), "verification_blocked_order_lookup_failed"),
        ],
    )
    def test_ambiguous_submit_error_without_adoption_keeps_order_recoverable(
        self,
        mock_db_session,
        mock_clock,
        mock_exchange_adapter,
        mock_order_repo,
        signal_factory,
        lookup_result,
        expected_action,
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )
        if isinstance(lookup_result, Exception):
            mock_exchange_adapter.get_order_by_client_id = MagicMock(
                side_effect=lookup_result
            )
        else:
            mock_exchange_adapter.get_order_by_client_id = MagicMock(
                return_value=lookup_result
            )
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

        orders = list(mock_order_repo.orders.values())
        assert len(orders) == 1
        order = orders[0]
        assert order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert order.exchange_order_id is None
        assert engine.list_recoverable_client_orders() == [order]
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload["status"] == "verification_blocked"
        assert audit.outcome_payload["adoption"]["action"] == expected_action

    def test_ambiguous_submit_error_with_unresolved_snapshot_keeps_order_recoverable(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-UNRESOLVED",
                status="filled",
                filled_quantity=Decimal("0.10"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
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

        order = next(iter(mock_order_repo.orders.values()))
        assert order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert order.exchange_order_id == "EX-UNRESOLVED"
        assert engine.list_recoverable_client_orders() == [order]
        assert mock_order_repo.trades == []
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload["status"] == "unresolved"
        assert audit.outcome_payload["adoption"]["action"] == "unresolved_missing_fill_price"

    def test_ambiguous_submit_snapshot_without_exchange_id_is_verification_blocked(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=None,
                status="open",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
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

        order = next(iter(mock_order_repo.orders.values()))
        assert order.status == OrderStatus.SUBMITTED.value
        assert order.exchange_order_id is None
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload["status"] == "verification_blocked"
        assert (
            audit.outcome_payload["adoption"]["action"]
            == "verification_blocked_order_snapshot_missing_exchange_order_id"
        )

    def test_ambiguous_submit_verification_blocked_keeps_protection_pending_with_warning(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )
        mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=None)
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
            engine.execute_signal(
                signal_factory(
                    price=Decimal("42000"),
                    stop_loss=Decimal("41000"),
                    take_profit=Decimal("43000"),
                    trailing_distance=Decimal("100"),
                )
            )

        orders = list(mock_order_repo.orders.values())
        entry = next(order for order in orders if order.type == "limit")
        conditional_orders = [order for order in orders if order.type != "limit"]
        assert entry.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert {order.status for order in conditional_orders} == {OrderStatus.NEW.value}
        assert {order.exchange_order_id for order in conditional_orders} == {None}
        assert {
            order.intent_payload["pending_entry_order_id"]
            for order in conditional_orders
        } == {entry.id}
        event = next(
            call.args[0]
            for call in audit_session.add.call_args_list
            if isinstance(call.args[0], SystemEvent)
            and call.args[0].event_subtype
            == "protective_orders_pending_after_submit_uncertainty"
        )
        assert event.event_subtype == "protective_orders_pending_after_submit_uncertainty"
        assert event.related_order_id == entry.id
        assert set(event.payload["conditional_order_ids"]) == {
            order.id for order in conditional_orders
        }

    def test_ambiguous_submit_error_with_terminal_snapshot_does_not_place_protection(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="EX-CANCELED",
                status="canceled",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
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
            engine.execute_signal(
                signal_factory(
                    price=Decimal("42000"),
                    stop_loss=Decimal("41000"),
                )
            )

        orders = list(mock_order_repo.orders.values())
        entry = next(order for order in orders if order.type == "limit")
        conditional = next(order for order in orders if order.type == "stop_loss")
        assert entry.status == OrderStatus.CANCELLED.value
        assert entry.exchange_order_id == "EX-CANCELED"
        assert conditional.status == "failed"
        audit = audit_session.add.call_args_list[0].args[0]
        assert audit.outcome_payload["status"] == "terminal_after_submit_error"
        assert audit.outcome_payload["adoption"]["action"] == "terminal_after_submit_error"

    def test_audited_conditional_orders_place_after_entry_fill(
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
            is_backtest=True,
        )

        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )

        entry = mock_order_repo.orders[order_id]
        conditional_orders = [
            order for order in mock_order_repo.orders.values() if order.id != order_id
        ]
        assert len(mock_exchange_adapter.open_orders) == 1
        assert {order.status for order in conditional_orders} == {OrderStatus.NEW.value}
        assert {order.exchange_order_id for order in conditional_orders} == {None}

        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )

        assert len(mock_exchange_adapter.open_orders) == 3
        assert {order.status for order in conditional_orders} == {
            OrderStatus.SUBMITTED.value
        }
        assert all(order.exchange_order_id for order in conditional_orders)

    def test_partial_entry_fill_places_protection_for_filled_quantity(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                quantity=Decimal("0.10"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partially_filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=Decimal("0.04"),
                cumulative_average_price=entry.price,
            )
        )

        conditional_orders = [
            order for order in mock_order_repo.orders.values() if order.id != order_id
        ]
        assert result["action"] == "applied"
        assert entry.status == OrderStatus.PARTIALLY_FILLED.value
        assert {order.status for order in conditional_orders} == {
            OrderStatus.SUBMITTED.value
        }
        assert {order.quantity for order in conditional_orders} == {Decimal("0.04")}

    def test_entry_fill_increment_after_protection_reports_unresolved_resize(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                quantity=Decimal("0.10"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partially_filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=Decimal("0.04"),
                cumulative_average_price=entry.price,
            )
        )

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=Decimal("0.10"),
                cumulative_average_price=entry.price,
            )
        )

        assert result["action"] == "unresolved_conditional_order_placement_failed"
        assert {
            failure["reason"] for failure in result["failures"]
        } == {"conditional_order_resize_required_after_entry_fill"}
        assert {
            failure["required_quantity"] for failure in result["failures"]
        } == {"0.10"}

    def test_entry_fill_reports_unresolved_when_protective_order_placement_fails(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        mock_exchange_adapter.set_fail_on_order_types({"stop_loss"})

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )

        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )
        assert result["action"] == "unresolved_conditional_order_placement_failed"
        assert result["failures"][0]["order_id"] == stop_loss.id
        assert stop_loss.status == "failed"
        assert take_profit.status == OrderStatus.SUBMITTED.value
        event = next(
            call.args[0]
            for call in audit_session.add.call_args_list
            if isinstance(call.args[0], SystemEvent)
            and call.args[0].event_subtype
            == "conditional_order_placement_failed_after_entry_fill"
        )
        assert event.related_order_id == entry.id

    def test_non_idempotent_ambiguous_conditional_submit_is_not_retried(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            is_backtest=True,
        )
        original_place_order = mock_exchange_adapter.place_order

        def place_order(order):
            if order.type == "stop_loss":
                raise NetworkError("request timed out")
            return original_place_order(order)

        mock_exchange_adapter.place_order = MagicMock(side_effect=place_order)
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )

        assert stop_loss.client_order_id is None
        assert stop_loss.status == OrderStatus.SUBMITTED_UNCONFIRMED.value

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )

        assert result["action"] == "applied"
        stop_loss_place_calls = [
            call for call in mock_exchange_adapter.place_order.call_args_list
            if call.args[0].type == "stop_loss"
        ]
        assert len(stop_loss_place_calls) == 1

    def test_filled_protective_order_cancels_linked_sibling(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )

        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=stop_loss.product_id,
                exchange_order_id=stop_loss.exchange_order_id,
                cumulative_filled_quantity=stop_loss.quantity,
                cumulative_average_price=stop_loss.trigger_price,
            )
        )

        assert stop_loss.status == OrderStatus.FILLED.value
        assert take_profit.status == OrderStatus.CANCELLED.value
        assert take_profit not in mock_exchange_adapter.open_orders

    def test_sibling_cancel_passes_conditional_order_type_to_adapter(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )
        mock_exchange_adapter.cancel_order_by_client_id = MagicMock(
            wraps=mock_exchange_adapter.cancel_order_by_client_id
        )

        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=stop_loss.product_id,
                exchange_order_id=stop_loss.exchange_order_id,
                cumulative_filled_quantity=stop_loss.quantity,
                cumulative_average_price=stop_loss.trigger_price,
            )
        )

        assert take_profit.status == OrderStatus.CANCELLED.value
        mock_exchange_adapter.cancel_order_by_client_id.assert_called_once_with(
            take_profit.client_order_id,
            take_profit.product_id,
            order_type="take_profit",
        )

    def test_partial_protective_fill_keeps_linked_sibling_and_reports_unresolved(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partially_filled",
                product_id=stop_loss.product_id,
                exchange_order_id=stop_loss.exchange_order_id,
                cumulative_filled_quantity=stop_loss.quantity / Decimal("2"),
                cumulative_average_price=stop_loss.trigger_price,
            )
        )

        assert result["action"] == "unresolved_protective_partial_fill_requires_resize"
        assert stop_loss.status == OrderStatus.PARTIALLY_FILLED.value
        assert take_profit.status == OrderStatus.SUBMITTED.value
        assert take_profit in mock_exchange_adapter.open_orders
        event = next(
            call.args[0]
            for call in audit_session.add.call_args_list
            if isinstance(call.args[0], SystemEvent)
            and call.args[0].event_subtype
            == "protective_partial_fill_requires_resize"
        )
        assert event.related_order_id == stop_loss.id

    def test_filled_protective_order_reports_unresolved_when_sibling_cancel_fails(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )
        mock_exchange_adapter.cancel_order_by_client_id = MagicMock(return_value=False)
        mock_exchange_adapter.cancel_order = MagicMock(return_value=False)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=stop_loss.product_id,
                exchange_order_id=stop_loss.exchange_order_id,
                cumulative_filled_quantity=stop_loss.quantity,
                cumulative_average_price=stop_loss.trigger_price,
            )
        )

        assert result["action"] == "unresolved_linked_conditional_cancel_failed"
        assert result["failure"]["order_id"] == take_profit.id
        assert take_profit.status == OrderStatus.SUBMITTED.value
        event = next(
            call.args[0]
            for call in audit_session.add.call_args_list
            if isinstance(call.args[0], SystemEvent)
            and call.args[0].event_subtype == "linked_conditional_order_cancel_failed"
        )
        assert event.related_order_id == stop_loss.id

    def test_replayed_entry_fill_event_does_not_resubmit_unconfirmed_protection(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        fill_event = ExchangeOrderEvent(
            status="filled",
            product_id=entry.product_id,
            exchange_order_id=entry.exchange_order_id,
            cumulative_filled_quantity=entry.quantity,
            cumulative_average_price=entry.price,
        )
        def place_order(order):
            if order.type in {"stop_loss", "take_profit"}:
                raise NetworkError("request timed out")
            return f"EX-{order.type}"

        mock_exchange_adapter.place_order = MagicMock(side_effect=place_order)
        mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=None)
        first = engine.process_exchange_order_event(fill_event)
        assert first["action"] == "unresolved_conditional_order_placement_failed"
        conditionals = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        assert all(
            order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
            for order in conditionals
        )

        replay = engine.process_exchange_order_event(fill_event)

        assert replay["action"] == "applied"
        assert replay["fill_quantity"] == Decimal("0")
        assert all(
            order.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
            for order in conditionals
        )
        conditional_place_calls = [
            call for call in mock_exchange_adapter.place_order.call_args_list
            if call.args[0].type in {"stop_loss", "take_profit"}
        ]
        assert len(conditional_place_calls) == 2

    def test_conditional_orders_derive_stable_client_order_ids(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        entry_client_id = "strategy-worker-entry-1704067200000000000"

        engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
                trailing_distance=Decimal("100"),
                metadata={"client_order_id": entry_client_id},
            )
        )

        conditionals = {
            order.type: order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit", "trailing_stop"}
        }
        assert conditionals["stop_loss"].client_order_id == (
            "strategy-worker-sl-1704067200000000000"
        )
        assert conditionals["take_profit"].client_order_id == (
            "strategy-worker-tp-1704067200000000000"
        )
        assert conditionals["trailing_stop"].client_order_id == (
            "strategy-worker-tr-1704067200000000000"
        )
        for order in conditionals.values():
            parse_client_order_id(order.client_order_id)

    def test_ambiguous_conditional_submit_adopts_exchange_order_without_replay_duplicate(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        accepted: dict[str, str] = {}
        original_place_order = mock_exchange_adapter.place_order

        def place_order(order):
            if order.type == "stop_loss":
                exchange_order_id = "EX-stop-loss"
                order.exchange_order_id = exchange_order_id
                accepted[order.client_order_id] = exchange_order_id
                mock_exchange_adapter.open_orders.append(order)
                raise NetworkError("request timed out")
            return original_place_order(order)

        def lookup(client_order_id, product_id, *, order_type=None):
            exchange_order_id = accepted.get(client_order_id)
            if exchange_order_id is None:
                return None
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                status="open",
            )

        mock_exchange_adapter.place_order = MagicMock(side_effect=place_order)
        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
        fill_event = ExchangeOrderEvent(
            status="filled",
            product_id=entry.product_id,
            exchange_order_id=entry.exchange_order_id,
            cumulative_filled_quantity=entry.quantity,
            cumulative_average_price=entry.price,
        )

        first = engine.process_exchange_order_event(fill_event)
        replay = engine.process_exchange_order_event(fill_event)

        assert first["action"] == "applied"
        assert replay["action"] == "applied"
        assert stop_loss.status == OrderStatus.SUBMITTED.value
        assert stop_loss.exchange_order_id == "EX-stop-loss"
        stop_loss_place_calls = [
            call for call in mock_exchange_adapter.place_order.call_args_list
            if call.args[0].type == "stop_loss"
        ]
        assert len(stop_loss_place_calls) == 1
        assert [
            order for order in mock_exchange_adapter.open_orders
            if order.client_order_id == stop_loss.client_order_id
        ] == [stop_loss]

    def test_ambiguous_conditional_submit_missing_snapshot_stays_unconfirmed(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )

        def place_order(order):
            if order.type == "stop_loss":
                raise NetworkError("request timed out")
            return f"EX-{order.type}"

        mock_exchange_adapter.place_order = MagicMock(side_effect=place_order)
        mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=None)
        fill_event = ExchangeOrderEvent(
            status="filled",
            product_id=entry.product_id,
            exchange_order_id=entry.exchange_order_id,
            cumulative_filled_quantity=entry.quantity,
            cumulative_average_price=entry.price,
        )

        first = engine.process_exchange_order_event(fill_event)
        replay = engine.process_exchange_order_event(fill_event)

        assert first["action"] == "unresolved_conditional_order_placement_failed"
        assert first["failures"][0]["reason"] == (
            "verification_blocked_order_snapshot_missing"
        )
        assert replay["action"] == "applied"
        assert stop_loss.status == OrderStatus.SUBMITTED_UNCONFIRMED.value
        assert stop_loss.exchange_order_id is None
        stop_loss_place_calls = [
            call for call in mock_exchange_adapter.place_order.call_args_list
            if call.args[0].type == "stop_loss"
        ]
        assert len(stop_loss_place_calls) == 1
        assert stop_loss in engine.list_recoverable_client_orders()

    def test_mixed_pending_and_submitted_protection_reports_underprotected_order(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(mock_db_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                quantity=Decimal("0.02"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        entry.filled_quantity = Decimal("0.02")
        entry.filled_price = entry.price
        mock_order_repo.update_order(entry)
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )
        stop_loss.quantity = Decimal("0.01")
        stop_loss.status = OrderStatus.SUBMITTED.value
        stop_loss.exchange_order_id = "EX-stop-loss"
        mock_order_repo.update_order(stop_loss)
        take_profit.quantity = Decimal("0.01")
        take_profit.status = OrderStatus.NEW.value
        mock_order_repo.update_order(take_profit)

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="open",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=Decimal("0.02"),
                cumulative_average_price=entry.price,
            )
        )

        assert result["action"] == "unresolved_conditional_order_placement_failed"
        assert {
            failure["reason"] for failure in result["failures"]
        } == {"conditional_order_resize_required_after_entry_fill"}
        assert result["failures"][0]["order_id"] == stop_loss.id
        assert result["failures"][0]["current_quantity"] == "0.01"
        assert result["failures"][0]["required_quantity"] == "0.02"
        assert stop_loss.status == OrderStatus.SUBMITTED.value
        assert take_profit.status == OrderStatus.SUBMITTED.value
        assert take_profit.quantity == Decimal("0.02")

    def test_startup_reconcile_places_pending_protection_for_filled_entry(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        entry.status = OrderStatus.FILLED.value
        entry.filled_quantity = entry.quantity
        entry.filled_price = entry.price
        mock_order_repo.update_order(entry)
        conditionals = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        assert all(order.status == OrderStatus.NEW.value for order in conditionals)
        mock_exchange_adapter.set_fail_on_order_types(set())

        payload = engine.reconcile_recoverable_client_orders()

        assert payload["protection_recovery"]["entries_attempted"] == 1
        assert payload["protection_recovery"]["failures"] == []
        assert all(
            order.status == OrderStatus.SUBMITTED.value for order in conditionals
        )

    def test_startup_reconcile_places_pending_protection_after_entry_fill_repair(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        conditionals = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        assert entry.filled_quantity == Decimal("0")
        assert all(order.status == OrderStatus.NEW.value for order in conditionals)

        def lookup(client_order_id, product_id, *, order_type=None):
            if client_order_id == entry.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id="EX-entry",
                    status="closed",
                    filled_quantity=entry.quantity,
                    average_price=entry.price,
                )
            return None

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        payload = engine.reconcile_recoverable_client_orders()

        assert payload["protection_recovery"]["entries_attempted"] == 1
        assert payload["protection_recovery"]["failures"] == []
        assert payload["decision_counts"] == {"exchange_closed": 1}
        assert payload["protection_unresolved_count"] == 0
        assert entry.filled_quantity == entry.quantity
        assert all(
            order.status == OrderStatus.SUBMITTED.value for order in conditionals
        )
        assert all(order.exchange_order_id is not None for order in conditionals)

    def test_startup_reconcile_counts_pending_protection_failures_as_unresolved(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        entry.status = OrderStatus.FILLED.value
        entry.filled_quantity = entry.quantity
        entry.filled_price = entry.price
        mock_order_repo.update_order(entry)
        mock_exchange_adapter.set_fail_on_order_types({"stop_loss", "take_profit"})

        payload = engine.reconcile_recoverable_client_orders()

        assert payload["protection_recovery"]["entries_attempted"] == 1
        assert len(payload["protection_recovery"]["failures"]) == 2
        assert payload["reconciliation_unresolved_count"] == 0
        assert payload["protection_unresolved_count"] == 2
        assert payload["unresolved_count"] == 2

    def test_replayed_protective_fill_event_retries_sibling_cancel(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            order for order in mock_order_repo.orders.values() if order.type == "stop_loss"
        )
        take_profit = next(
            order for order in mock_order_repo.orders.values() if order.type == "take_profit"
        )
        original_cancel_by_client_id = mock_exchange_adapter.cancel_order_by_client_id
        mock_exchange_adapter.cancel_order_by_client_id = MagicMock(return_value=False)
        mock_exchange_adapter.cancel_order = MagicMock(return_value=False)
        protective_fill_event = ExchangeOrderEvent(
            status="filled",
            product_id=stop_loss.product_id,
            exchange_order_id=stop_loss.exchange_order_id,
            cumulative_filled_quantity=stop_loss.quantity,
            cumulative_average_price=stop_loss.trigger_price,
        )
        first = engine.process_exchange_order_event(protective_fill_event)
        assert first["action"] == "unresolved_linked_conditional_cancel_failed"
        assert take_profit.status == OrderStatus.SUBMITTED.value

        mock_exchange_adapter.cancel_order_by_client_id = original_cancel_by_client_id
        mock_exchange_adapter.cancel_order = MagicMock(
            wraps=mock_exchange_adapter.cancel_order
        )
        replay = engine.process_exchange_order_event(protective_fill_event)

        assert replay["action"] == "applied"
        assert take_profit.status == OrderStatus.CANCELLED.value

    def test_resync_skips_pending_protective_orders(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]

        def lookup(client_order_id, product_id, *, order_type=None):
            assert client_order_id == entry.client_order_id, (
                "resync must not look up pending protective orders"
            )
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=entry.exchange_order_id,
                status="open",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        summary = engine.resync_recoverable_order_events()

        assert summary["verification_blocked_count"] == 0
        assert summary["unresolved_count"] == 0

    def test_entry_cancelled_without_fill_fails_pending_protection(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="canceled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
            )
        )

        assert result["action"] == "applied"
        assert entry.status == OrderStatus.CANCELLED.value
        conditionals = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        assert len(conditionals) == 2
        assert all(order.status == "failed" for order in conditionals)

    def test_entry_cancelled_with_partial_fill_keeps_pending_protection(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partially_filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity / Decimal("2"),
                cumulative_average_price=entry.price,
            )
        )

        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="canceled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity / Decimal("2"),
                cumulative_average_price=entry.price,
            )
        )

        conditionals = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        assert len(conditionals) == 2
        assert all(
            order.status == OrderStatus.SUBMITTED.value for order in conditionals
        )

    def test_reconcile_fails_pending_protection_for_entry_cancelled_on_exchange(
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
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]

        def lookup(client_order_id, product_id, *, order_type=None):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=entry.exchange_order_id,
                status="canceled",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        payload = engine.reconcile_recoverable_client_orders()

        assert entry.status == OrderStatus.CANCELLED.value
        conditionals = [
            order
            for order in mock_order_repo.orders.values()
            if order.type in {"stop_loss", "take_profit"}
        ]
        assert all(order.status == "failed" for order in conditionals)
        assert payload["protection_recovery"]["entries_attempted"] == 0

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
        mock_exchange_adapter.set_should_fail(True, "Order rejected")
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
            "error": "Order rejected",
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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
        restarted_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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
        def unsupported_lookup(client_order_id, product_id, *, order_type=None):
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

        mock_exchange_adapter.get_order_by_client_id = lambda client_order_id, product_id, *, order_type=None: (
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

    def test_skipped_pending_protection_count_reported_in_resync_and_reconcile(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Fix 1: resync and reconcile payloads expose skipped_pending_protection_count."""
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]

        def lookup(client_order_id, product_id, *, order_type=None):
            assert client_order_id == entry.client_order_id, (
                "resync/reconcile must not look up pending protective orders"
            )
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=entry.exchange_order_id,
                status="open",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        resync_summary = engine.resync_recoverable_order_events()
        assert resync_summary["skipped_pending_protection_count"] == 2

        reconcile_summary = engine.reconcile_recoverable_client_orders()
        assert reconcile_summary["skipped_pending_protection_count"] == 2

    def test_conditional_order_placement_increments_orders_total(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Fix 2: successful SL/TP placement increments ORDERS_TOTAL with status=placed reason=none."""
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )

        labels_sl = {"order_type": "stop_loss", "status": "placed", "reason": "none"}
        labels_tp = {"order_type": "take_profit", "status": "placed", "reason": "none"}
        before_sl = REGISTRY.get_sample_value("fluxtrade_orders_total", labels_sl) or 0.0
        before_tp = REGISTRY.get_sample_value("fluxtrade_orders_total", labels_tp) or 0.0

        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )

        after_sl = REGISTRY.get_sample_value("fluxtrade_orders_total", labels_sl) or 0.0
        after_tp = REGISTRY.get_sample_value("fluxtrade_orders_total", labels_tp) or 0.0

        assert after_sl - before_sl == 1.0
        assert after_tp - before_tp == 1.0

    # ---------------------------------------------------------------------------
    # Protective terminal without fill — protection gap detection
    # ---------------------------------------------------------------------------

    def _setup_engine_with_filled_entry(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Return (engine, entry, stop_loss, take_profit) after entry fill event."""
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            o for o in mock_order_repo.orders.values() if o.type == "stop_loss"
        )
        take_profit = next(
            o for o in mock_order_repo.orders.values() if o.type == "take_profit"
        )
        return engine, entry, stop_loss, take_profit

    def test_protective_cancel_without_fill_reports_gap(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """A stop_loss cancelled with zero fill while entry is filled → unresolved gap."""
        engine, entry, stop_loss, take_profit = self._setup_engine_with_filled_entry(
            mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
        )

        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="canceled",
                product_id=stop_loss.product_id,
                exchange_order_id=stop_loss.exchange_order_id,
            )
        )

        assert result["action"] == "unresolved_protective_terminal_without_fill"
        assert stop_loss.status == OrderStatus.CANCELLED.value
        assert take_profit.status == OrderStatus.SUBMITTED.value
        system_event = next(
            call.args[0]
            for call in mock_db_session.add.call_args_list
            if isinstance(call.args[0], SystemEvent)
            and call.args[0].event_subtype == "protective_order_terminal_without_fill"
        )
        assert system_event.related_order_id == stop_loss.id

    def test_oco_sibling_cancel_after_fill_not_reported_as_gap(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Exchange cancel for OCO sibling after the other leg filled → action 'applied', no gap."""
        engine, entry, stop_loss, take_profit = self._setup_engine_with_filled_entry(
            mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
        )
        # SL fills — this cancels TP locally
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=stop_loss.product_id,
                exchange_order_id=stop_loss.exchange_order_id,
                cumulative_filled_quantity=stop_loss.quantity,
                cumulative_average_price=stop_loss.trigger_price,
            )
        )
        assert stop_loss.status == OrderStatus.FILLED.value
        assert take_profit.status == OrderStatus.CANCELLED.value

        # Exchange confirms the TP cancel we requested
        result = engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="canceled",
                product_id=take_profit.product_id,
                exchange_order_id=take_profit.exchange_order_id,
            )
        )

        assert result["action"] == "applied"

    def test_protective_terminal_without_fill_helper_returns_none_when_entry_unfilled(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Helper returns None when the entry has no fill (no position to protect)."""
        engine, _entry, stop_loss, _take_profit = self._setup_engine_with_filled_entry(
            mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
        )
        # Manually clear entry fill to simulate unfilled entry
        entry = next(
            o for o in mock_order_repo.orders.values() if o.type == "limit"
        )
        entry.filled_quantity = Decimal("0")
        mock_order_repo.update_order(entry)

        assert engine._protective_terminal_without_fill_failure(stop_loss) is None

    def test_reconcile_reports_gap_for_protective_cancelled_without_fill(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Reconcile path: SUBMITTED protective cancelled by exchange with no fill → unresolved."""
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        # Fill the entry via event so SL/TP are placed (SUBMITTED)
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            order = next(
                (o for o in mock_order_repo.orders.values()
                 if o.client_order_id == client_order_id),
                None,
            )
            if order is None:
                return None
            if order.type in ("limit", "market"):
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    status="closed",
                    filled_quantity=order.quantity,
                    average_price=order.price,
                )
            # Protective orders come back as canceled with zero fill
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=order.exchange_order_id,
                status="canceled",
                filled_quantity=Decimal("0"),
                average_price=None,
            )

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        payload = engine.reconcile_recoverable_client_orders()

        assert payload["unresolved_count"] >= 1
        assert any(
            r["repair_action"] == "unresolved_protective_terminal_without_fill"
            for r in payload["results"]
        )

    def test_uncertain_submit_preserves_submitted_leg_and_marks_pending_new_leg(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Partial success: SL placed (SUBMITTED), TP lookup failed (stays NEW).

        The helper must skip the SUBMITTED SL and only merge payload into the
        still-NEW TP.  With the old code the SL would be destructively reset to
        NEW and its exchange_order_id would be nulled — an orphan on the exchange.
        """
        def place_order_side_effect(order):
            if order.type == "limit":
                raise NetworkError("Connection timeout")
            if order.type == "stop_loss":
                # mark_submitted(order, ex_id) called by engine after this return
                return "MOCK-SL-1"
            # take_profit branch: never reached in this scenario (TP excluded
            # from placement_candidates because lookup raises below)
            raise ExchangeError("tp placement failed")

        mock_exchange_adapter.place_order = MagicMock(side_effect=place_order_side_effect)

        def lookup(client_order_id, product_id, *, order_type=None):
            if order_type == "limit":
                # Entry was filled on the exchange
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id="EX-ENTRY-FILLED",
                    status="filled",
                    filled_quantity=Decimal("0.01"),
                    average_price=Decimal("42000"),
                )
            if order_type == "take_profit":
                # Pre-submit adoption lookup for TP fails → TP stays NEW,
                # excluded from placement_candidates; failures list non-empty
                # → event returns unresolved → helper is called
                raise ExchangeError("boom")
            # stop_loss pre-submit lookup: no existing exchange order
            return None

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,  # fill recording uses repo.update_position, not Redis
        )

        with pytest.raises(NetworkError, match="Connection timeout"):
            engine.execute_signal(
                signal_factory(
                    price=Decimal("42000"),
                    stop_loss=Decimal("41000"),
                    take_profit=Decimal("43000"),
                )
            )

        orders = list(mock_order_repo.orders.values())
        sl_order = next(o for o in orders if o.type == "stop_loss")
        tp_order = next(o for o in orders if o.type == "take_profit")

        # SL was placed on the exchange; the helper must leave it untouched
        assert sl_order.status == OrderStatus.SUBMITTED.value
        assert sl_order.exchange_order_id == "MOCK-SL-1"
        assert sl_order.intent_payload.get("linked_order_id") == str(tp_order.id)

        # TP was never submitted (pre-submit lookup failed); the helper merges
        # pending_reason into the existing payload without destroying linked_order_id
        assert tp_order.status == OrderStatus.NEW.value
        assert tp_order.exchange_order_id is None
        assert tp_order.intent_payload.get("linked_order_id") == str(sl_order.id)
        assert tp_order.intent_payload.get("pending_reason") == "entry_submit_outcome_uncertain"

    def test_uncertain_submit_merges_pending_payload_without_replacing_it(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """No-placement case: lookup returns None → verification_blocked.

        Both conditionals remain NEW.  The helper must MERGE the pending keys
        into the existing payload so that linked_order_id and placement_mode
        survive alongside the new pending_reason.
        """
        mock_exchange_adapter.place_order = MagicMock(
            side_effect=NetworkError("Connection timeout")
        )
        mock_exchange_adapter.get_order_by_client_id = MagicMock(return_value=None)
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
        )

        with pytest.raises(NetworkError, match="Connection timeout"):
            engine.execute_signal(
                signal_factory(
                    price=Decimal("42000"),
                    stop_loss=Decimal("41000"),
                    take_profit=Decimal("43000"),
                )
            )

        orders = list(mock_order_repo.orders.values())
        conditional_orders = [o for o in orders if o.type != "limit"]
        assert len(conditional_orders) == 2

        for order in conditional_orders:
            assert order.status == OrderStatus.NEW.value
            assert order.exchange_order_id is None
            # linked_order_id must survive (merge, not replace)
            assert order.intent_payload.get("linked_order_id") is not None
            # pending_reason must be added alongside the original keys
            assert order.intent_payload.get("pending_reason") == "entry_submit_outcome_uncertain"

    # ------------------------------------------------------------------
    # OCO sibling cancellation during startup reconciliation (P1 fix)
    # ------------------------------------------------------------------

    def test_reconcile_offline_protective_fill_cancels_sibling(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Gap 1: SL fills while service is offline.

        After restart, reconcile sees SL closed (filled) and TP still open.
        The fix must cancel TP and NOT leave the stale leg live.
        """
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        # Bring entry to FILLED via live event (SL/TP become SUBMITTED)
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            o for o in mock_order_repo.orders.values() if o.type == "stop_loss"
        )
        take_profit = next(
            o for o in mock_order_repo.orders.values() if o.type == "take_profit"
        )

        # Simulate offline fill: exchange reports SL filled, TP still open
        def lookup(client_order_id, product_id, *, order_type=None):
            if client_order_id == entry.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=entry.exchange_order_id,
                    status="filled",
                    filled_quantity=entry.quantity,
                    average_price=entry.price,
                )
            if client_order_id == stop_loss.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=stop_loss.exchange_order_id,
                    status="filled",
                    filled_quantity=stop_loss.quantity,
                    average_price=stop_loss.trigger_price,
                )
            if client_order_id == take_profit.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=take_profit.exchange_order_id,
                    status="open",
                    filled_quantity=Decimal("0"),
                    average_price=None,
                )
            return None

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        payload = engine.reconcile_recoverable_client_orders()

        # SL must be recorded as filled; TP must be cancelled (stale OCO leg)
        assert stop_loss.status == OrderStatus.FILLED.value
        assert take_profit.status == OrderStatus.CANCELLED.value
        # At least one result should reflect the SL fill reconciliation
        repair_actions = [r["repair_action"] for r in payload["results"]]
        assert "filled_from_exchange_snapshot" in repair_actions

    def test_reconcile_crash_window_stale_protective_leg_cancelled(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Gap 2: crash after recording SL FILLED but before cancelling TP.

        Simulate by directly setting SL status to FILLED in the repo.
        Reconcile sees TP as 'open' on exchange — it must cancel, not restore.
        """
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        # Bring entry to FILLED via live event (SL/TP become SUBMITTED)
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            o for o in mock_order_repo.orders.values() if o.type == "stop_loss"
        )
        take_profit = next(
            o for o in mock_order_repo.orders.values() if o.type == "take_profit"
        )

        # Simulate crash window: SL was recorded as FILLED but sibling cancel
        # never happened.  Manually set SL to FILLED in the repo.
        stop_loss.status = OrderStatus.FILLED.value
        stop_loss.filled_quantity = stop_loss.quantity
        mock_order_repo.update_order(stop_loss)
        # Remove SL from open_orders (exchange already executed it)
        mock_exchange_adapter.open_orders = [
            o for o in mock_exchange_adapter.open_orders if o.id != stop_loss.id
        ]

        # Exchange still shows TP open; entry is terminal (not in scan)
        def lookup(client_order_id, product_id, *, order_type=None):
            if client_order_id == take_profit.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=take_profit.exchange_order_id,
                    status="open",
                    filled_quantity=Decimal("0"),
                    average_price=None,
                )
            return None

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)

        payload = engine.reconcile_recoverable_client_orders()

        # TP must be cancelled — NOT restored as an active tracking order
        assert take_profit.status == OrderStatus.CANCELLED.value
        repair_actions = [r["repair_action"] for r in payload["results"]]
        assert "cancelled_stale_protective_leg" in repair_actions
        assert "restored_tracking" not in repair_actions

    def test_reconcile_offline_protective_fill_sibling_cancel_failure_unresolved(
        self, mock_db_session, mock_clock, mock_exchange_adapter, mock_order_repo, signal_factory
    ):
        """Cancel failure during sibling cancellation after offline protective fill
        must yield an unresolved result, not silently drop the failure.
        """
        audit_session = mock_db_session
        engine = ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=mock_order_repo,
            db_session_factory=lambda: nullcontext(audit_session),
            audit_external_orders=True,
            is_backtest=True,
        )
        order_id = engine.execute_signal(
            signal_factory(
                price=Decimal("42000"),
                stop_loss=Decimal("41000"),
                take_profit=Decimal("43000"),
            )
        )
        entry = mock_order_repo.orders[order_id]
        engine.process_exchange_order_event(
            ExchangeOrderEvent(
                status="filled",
                product_id=entry.product_id,
                exchange_order_id=entry.exchange_order_id,
                cumulative_filled_quantity=entry.quantity,
                cumulative_average_price=entry.price,
            )
        )
        stop_loss = next(
            o for o in mock_order_repo.orders.values() if o.type == "stop_loss"
        )
        take_profit = next(
            o for o in mock_order_repo.orders.values() if o.type == "take_profit"
        )

        def lookup(client_order_id, product_id, *, order_type=None):
            if client_order_id == entry.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=entry.exchange_order_id,
                    status="filled",
                    filled_quantity=entry.quantity,
                    average_price=entry.price,
                )
            if client_order_id == stop_loss.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=stop_loss.exchange_order_id,
                    status="filled",
                    filled_quantity=stop_loss.quantity,
                    average_price=stop_loss.trigger_price,
                )
            if client_order_id == take_profit.client_order_id:
                return ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=take_profit.exchange_order_id,
                    status="open",
                    filled_quantity=Decimal("0"),
                    average_price=None,
                )
            return None

        mock_exchange_adapter.get_order_by_client_id = MagicMock(side_effect=lookup)
        # Force cancel to always fail so the sibling cancel returns failure
        mock_exchange_adapter.cancel_order = MagicMock(return_value=False)
        mock_exchange_adapter.cancel_order_by_client_id = MagicMock(return_value=False)

        payload = engine.reconcile_recoverable_client_orders()

        assert payload["unresolved_count"] >= 1
        repair_actions = [r["repair_action"] for r in payload["results"]]
        assert "unresolved_linked_conditional_cancel_failed" in repair_actions


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


def test_reconcile_gate_is_independent_of_kill_switch(execution_engine):
    """Reconnect reconcile gate and kill-switch halt must never clear each other.

    This is the core safety property: resuming after a reconnect reconcile must
    not lift an active kill-switch halt, and clearing the kill switch must not
    lift an in-progress reconcile gate. Submissions are blocked while either is
    raised.
    """
    # Both gates raised.
    execution_engine.halt_and_drain(timeout=0)
    execution_engine.halt_for_reconcile(timeout=0)
    assert execution_engine._submissions_halted is True
    assert execution_engine._reconcile_halt is True

    # Resuming after reconcile clears ONLY the reconcile gate; kill switch stays.
    execution_engine.resume_after_reconcile()
    assert execution_engine._reconcile_halt is False
    assert execution_engine._submissions_halted is True

    # And vice versa: clearing the kill switch leaves an active reconcile gate.
    execution_engine.halt_for_reconcile(timeout=0)
    execution_engine.resume_submissions()
    assert execution_engine._submissions_halted is False
    assert execution_engine._reconcile_halt is True
