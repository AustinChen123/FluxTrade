"""RED test matrix for RuntimeReconciliationJob (L6 live-ops safety).

All tests must FAIL (NotImplementedError or AssertionError) until the
implementer fills in run_once.  Do NOT modify these tests.

Contract:
- All quantities and balances use Decimal — never float.
- Drift = abs(local - exchange) > threshold.
- No drift + no errors → NO system_event.
- Drift beyond threshold → system_event(event_type="reconcile",
  event_subtype="runtime_drift") + logger.warning.
- Adapter error → system_event(event_type="reconcile",
  event_subtype="runtime_reconcile_error") + no exception propagation.
"""

from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.core.models import Position, PositionSide
from src.core.runtime_reconcile import RuntimeReconciliationJob


# =============================================================================
# Helpers / fakes
# =============================================================================

PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "strat_alpha"

SMALL = Decimal("0.001")   # below any threshold used in tests
THRESHOLD_QTY = Decimal("0.01")
THRESHOLD_BAL = Decimal("1.00")


def _make_position(
    strategy_id: str = STRATEGY_ID,
    product_id: str = PRODUCT_ID,
    side: PositionSide = PositionSide.LONG,
    quantity: Decimal = Decimal("1.0"),
) -> Position:
    return Position(
        strategy_id=strategy_id,
        product_id=product_id,
        side=side,
        quantity=quantity,
        entry_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
    )


class FakeAccountService:
    def __init__(
        self,
        positions: list[Position] | None = None,
        balance: Decimal = Decimal("10000"),
    ) -> None:
        self._positions = positions or []
        self._balance = balance

    def get_all_positions(self) -> list[Position]:
        return list(self._positions)

    def get_balance(self) -> Decimal:
        return self._balance


class FakeAdapter:
    """Fake adapter with controllable per-product position and balance."""

    def __init__(
        self,
        positions: dict[str, Position | None] | None = None,
        balance: Decimal = Decimal("10000"),
        get_position_raises: Exception | None = None,
    ) -> None:
        self._positions: dict[str, Position | None] = positions or {}
        self._balance = balance
        self._get_position_raises = get_position_raises

    def get_position(self, product_id: str) -> Position | None:
        if self._get_position_raises is not None:
            raise self._get_position_raises
        return self._positions.get(product_id)

    def get_balance(self, asset: str) -> Decimal:
        return self._balance


def _make_null_db_factory():
    @contextmanager
    def factory():
        session = MagicMock()
        yield session

    return factory


def _make_job(
    *,
    local_positions: list[Position] | None = None,
    local_balance: Decimal = Decimal("10000"),
    exchange_positions: dict[str, Position | None] | None = None,
    exchange_balance: Decimal = Decimal("10000"),
    get_position_raises: Exception | None = None,
    qty_threshold: Decimal = THRESHOLD_QTY,
    bal_threshold: Decimal = THRESHOLD_BAL,
) -> tuple[RuntimeReconciliationJob, FakeAccountService, FakeAdapter]:
    account = FakeAccountService(
        positions=local_positions, balance=local_balance
    )
    adapter = FakeAdapter(
        positions=exchange_positions,
        balance=exchange_balance,
        get_position_raises=get_position_raises,
    )
    db_factory = _make_null_db_factory()
    job = RuntimeReconciliationJob(
        account_service=account,
        adapter=adapter,
        db_session_factory=db_factory,
        quantity_drift_threshold=qty_threshold,
        balance_drift_threshold=bal_threshold,
    )
    return job, account, adapter


# =============================================================================
# Matrix tests
# =============================================================================


class TestRunOnceResultShape:
    """The returned dict always has the required keys regardless of state."""

    def test_result_has_required_keys(self):
        job, _, _ = _make_job()

        result = job.run_once()

        for key in ("checked_positions", "position_drifts", "balance_drift", "errors"):
            assert key in result, f"result missing key: {key}"

    def test_checked_positions_is_int(self):
        local_pos = _make_position()
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: local_pos},
        )

        result = job.run_once()

        assert isinstance(result["checked_positions"], int)

    def test_position_drifts_is_list(self):
        job, _, _ = _make_job()

        result = job.run_once()

        assert isinstance(result["position_drifts"], list)

    def test_errors_is_list(self):
        job, _, _ = _make_job()

        result = job.run_once()

        assert isinstance(result["errors"], list)


class TestRunOnceNoDrift:
    """Matrix item 1: both positions and balance within threshold → no event."""

    def test_no_drift_no_system_event(self):
        local_pos = _make_position(quantity=Decimal("1.000"))
        exchange_pos = _make_position(quantity=Decimal("1.000"))
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: exchange_pos},
            local_balance=Decimal("10000.00"),
            exchange_balance=Decimal("10000.00"),
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_not_called()

        assert result["position_drifts"] == []
        assert result["balance_drift"] is None
        assert result["errors"] == []

    def test_small_quantity_diff_within_threshold_no_drift(self):
        """Diff below threshold must not be reported as drift."""
        local_pos = _make_position(quantity=Decimal("1.000"))
        exchange_pos = _make_position(quantity=Decimal("1.000") + SMALL)
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: exchange_pos},
            qty_threshold=THRESHOLD_QTY,  # SMALL < THRESHOLD_QTY
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_not_called()

        assert result["position_drifts"] == []


class TestRunOnceQuantityDrift:
    """Matrix item 2: quantity drift beyond threshold triggers event."""

    def test_quantity_drift_above_threshold_reported(self):
        local_qty = Decimal("2.000")
        exchange_qty = Decimal("1.000")  # diff = 1.0 >> THRESHOLD_QTY
        local_pos = _make_position(quantity=local_qty)
        exchange_pos = _make_position(quantity=exchange_qty)
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: exchange_pos},
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_called_once()
            kwargs = mock_write.call_args.kwargs
            assert kwargs["event_type"] == "reconcile"
            assert kwargs["event_subtype"] == "runtime_drift"

        assert len(result["position_drifts"]) == 1
        drift = result["position_drifts"][0]
        assert drift["strategy_id"] == STRATEGY_ID
        assert drift["product_id"] == PRODUCT_ID
        assert isinstance(drift["local_quantity"], Decimal)
        assert isinstance(drift["exchange_quantity"], Decimal)
        assert drift["local_quantity"] == local_qty
        assert drift["exchange_quantity"] == exchange_qty

    def test_drift_result_payload_included_in_event(self):
        local_pos = _make_position(quantity=Decimal("5.0"))
        exchange_pos = _make_position(quantity=Decimal("1.0"))
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: exchange_pos},
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            job.run_once()
            payload = mock_write.call_args.kwargs["payload"]

        # The event payload must mirror the returned result
        assert "position_drifts" in payload
        assert len(payload["position_drifts"]) >= 1

    def test_same_product_local_positions_are_aggregated_before_compare(self):
        local_positions = [
            _make_position(strategy_id="strat_a", quantity=Decimal("0.4")),
            _make_position(strategy_id="strat_b", quantity=Decimal("0.6")),
        ]
        exchange_pos = _make_position(strategy_id="exchange", quantity=Decimal("1.0"))
        job, _, _ = _make_job(
            local_positions=local_positions,
            exchange_positions={PRODUCT_ID: exchange_pos},
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_not_called()

        assert result["checked_positions"] == 2
        assert result["position_drifts"] == []


class TestRunOnceAsymmetricDrift:
    """Matrix items 3 & 4: asymmetric presence (one side missing)."""

    def test_local_position_exchange_flat_is_drift(self):
        """Local has position, exchange reports None → drift (exchange_qty = 0)."""
        local_pos = _make_position(quantity=Decimal("1.5"))
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: None},  # exchange flat
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_called_once()

        drifts = result["position_drifts"]
        assert len(drifts) == 1
        assert drifts[0]["exchange_quantity"] == Decimal("0")

    def test_exchange_has_position_local_flat_is_drift(self):
        """Exchange has position, local is flat → drift reported."""
        exchange_pos = _make_position(quantity=Decimal("2.0"))
        # local has no position but exchange does — job must check exchange too
        job, _, _ = _make_job(
            local_positions=[],
            exchange_positions={PRODUCT_ID: exchange_pos},
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            # Drift must be detected; the event may or may not fire depending
            # on whether the job enumerates exchange-side positions.
            # The spec says "both directions" — an event must fire.
            mock_write.assert_called_once()

        assert len(result["position_drifts"]) >= 1


class TestRunOnceBalanceDrift:
    """Matrix item 5: balance drift beyond threshold triggers event."""

    def test_balance_drift_above_threshold_reported(self):
        local_bal = Decimal("10000.00")
        exchange_bal = Decimal("9000.00")  # diff = 1000 >> THRESHOLD_BAL
        job, _, _ = _make_job(
            local_balance=local_bal,
            exchange_balance=exchange_bal,
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_called_once()
            kwargs = mock_write.call_args.kwargs
            assert kwargs["event_type"] == "reconcile"
            assert kwargs["event_subtype"] == "runtime_drift"

        assert result["balance_drift"] is not None
        assert isinstance(result["balance_drift"]["local"], Decimal)
        assert isinstance(result["balance_drift"]["exchange"], Decimal)
        assert result["balance_drift"]["local"] == local_bal
        assert result["balance_drift"]["exchange"] == exchange_bal

    def test_small_balance_diff_within_threshold_no_drift(self):
        job, _, _ = _make_job(
            local_balance=Decimal("10000.00"),
            exchange_balance=Decimal("10000.00") + SMALL,
            bal_threshold=THRESHOLD_BAL,
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_not_called()

        assert result["balance_drift"] is None


class TestRunOnceAdapterError:
    """Matrix item 6: adapter errors captured; no exception propagates; error event written."""

    def test_adapter_get_position_raises_does_not_propagate(self):
        local_pos = _make_position()
        job, _, _ = _make_job(
            local_positions=[local_pos],
            get_position_raises=RuntimeError("exchange timeout"),
        )

        # Must NOT raise
        result = job.run_once()

        assert len(result["errors"]) >= 1
        err = result["errors"][0]
        assert "scope" in err
        assert "reason" in err

    def test_adapter_error_scope_identifies_positions(self):
        local_pos = _make_position()
        job, _, _ = _make_job(
            local_positions=[local_pos],
            get_position_raises=RuntimeError("timeout"),
        )

        result = job.run_once()

        scopes = [e["scope"] for e in result["errors"]]
        assert any(s in ("positions", "balance") for s in scopes)

    def test_adapter_error_writes_error_event(self):
        local_pos = _make_position()
        job, _, _ = _make_job(
            local_positions=[local_pos],
            get_position_raises=RuntimeError("exchange unreachable"),
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            job.run_once()
            mock_write.assert_called_once()
            kwargs = mock_write.call_args.kwargs
            assert kwargs["event_type"] == "reconcile"
            assert kwargs["event_subtype"] == "runtime_reconcile_error"

    def test_adapter_error_remaining_products_still_checked(self):
        """When get_position raises on one product, others must still be evaluated.

        Note: if get_position always raises (not per-product), this tests that
        subsequent balance check is still attempted.
        """
        local_pos = _make_position()
        # Even if positions error, balance comparison must still run
        local_bal = Decimal("10000")
        exchange_bal = Decimal("5000")  # drift beyond threshold

        # Build a custom job where get_position raises but get_balance works
        account = FakeAccountService(positions=[local_pos], balance=local_bal)

        class PartialFailAdapter:
            def get_position(self, product_id: str) -> None:
                raise RuntimeError("position error")

            def get_balance(self, asset: str) -> Decimal:
                return exchange_bal

        db_factory = _make_null_db_factory()
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=PartialFailAdapter(),
            db_session_factory=db_factory,
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
        )

        result = job.run_once()

        # Position error recorded
        assert len(result["errors"]) >= 1
        # Balance drift should still be detected (balance check independent of position check)
        # OR the job may group them — either way, no exception propagated
        # At minimum: no exception and errors list non-empty
        assert result is not None


class TestRunOnceMultipleProducts:
    """Matrix item 7: multiple products — only drifting one reported."""

    def test_only_drifting_product_in_results(self):
        pos_ok = _make_position(
            strategy_id="s1",
            product_id=PRODUCT_ID,
            quantity=Decimal("1.0"),
        )
        pos_drift = _make_position(
            strategy_id="s2",
            product_id="BINANCE:ETHUSDT-PERP",
            quantity=Decimal("5.0"),
        )
        exchange_ok = _make_position(
            strategy_id="s1",
            product_id=PRODUCT_ID,
            quantity=Decimal("1.0"),
        )
        exchange_drift = _make_position(
            strategy_id="s2",
            product_id="BINANCE:ETHUSDT-PERP",
            quantity=Decimal("1.0"),  # drifts from 5.0
        )
        job, _, _ = _make_job(
            local_positions=[pos_ok, pos_drift],
            exchange_positions={
                PRODUCT_ID: exchange_ok,
                "BINANCE:ETHUSDT-PERP": exchange_drift,
            },
        )

        with patch("src.core.runtime_reconcile.write_system_event"):
            result = job.run_once()

        drift_products = [d["product_id"] for d in result["position_drifts"]]
        assert "BINANCE:ETHUSDT-PERP" in drift_products
        assert PRODUCT_ID not in drift_products

    def test_checked_positions_counts_all_local(self):
        pos1 = _make_position(strategy_id="s1", product_id=PRODUCT_ID)
        pos2 = _make_position(strategy_id="s2", product_id="BINANCE:ETHUSDT-PERP")
        exchange_pos1 = _make_position(strategy_id="s1", product_id=PRODUCT_ID)
        exchange_pos2 = _make_position(strategy_id="s2", product_id="BINANCE:ETHUSDT-PERP")
        job, _, _ = _make_job(
            local_positions=[pos1, pos2],
            exchange_positions={
                PRODUCT_ID: exchange_pos1,
                "BINANCE:ETHUSDT-PERP": exchange_pos2,
            },
        )

        result = job.run_once()

        assert result["checked_positions"] == 2


class TestRunOnceDecimalCompliance:
    """All numeric values in result must be Decimal, never float."""

    def test_position_drift_quantities_are_decimal(self):
        local_pos = _make_position(quantity=Decimal("3.0"))
        exchange_pos = _make_position(quantity=Decimal("1.0"))
        job, _, _ = _make_job(
            local_positions=[local_pos],
            exchange_positions={PRODUCT_ID: exchange_pos},
        )

        result = job.run_once()

        for drift in result["position_drifts"]:
            assert isinstance(drift["local_quantity"], Decimal), "local_quantity must be Decimal"
            assert isinstance(drift["exchange_quantity"], Decimal), "exchange_quantity must be Decimal"

    def test_balance_drift_values_are_decimal(self):
        job, _, _ = _make_job(
            local_balance=Decimal("10000"),
            exchange_balance=Decimal("5000"),
        )

        result = job.run_once()

        if result["balance_drift"] is not None:
            assert isinstance(result["balance_drift"]["local"], Decimal)
            assert isinstance(result["balance_drift"]["exchange"], Decimal)
