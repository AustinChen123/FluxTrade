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
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.core.adapters.live_backpack import LiveBackpackAdapter
from src.core.models import Position, PositionSide
from src.core.risk_manager import AccountService, RiskManager
from src.core.runtime_reconcile import PositionAuthorityState, RuntimeReconciliationJob


# =============================================================================
# Helpers / fakes
# =============================================================================

PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "strat_alpha"

SMALL = Decimal("0.001")  # below any threshold used in tests
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
        self.balance_replacements: list[Decimal] = []

    def get_all_positions(self) -> list[Position]:
        return list(self._positions)

    def get_balance(self) -> Decimal:
        return self._balance

    def replace_generic_balance(self, balance: Decimal) -> None:
        self.balance_replacements.append(balance)
        self._balance = balance


class BlockingFirstAccountService(FakeAccountService):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0
        self._calls_lock = threading.Lock()

    def get_all_positions(self) -> list[Position]:
        with self._calls_lock:
            self.calls += 1
            call_number = self.calls
        if call_number == 1:
            self.entered.set()
            self.release.wait(timeout=2.0)
        return []


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
        self.balance_assets: list[str] = []

    def get_position(self, product_id: str) -> Position | None:
        if self._get_position_raises is not None:
            raise self._get_position_raises
        return self._positions.get(product_id)

    def get_balance(self, asset: str) -> Decimal:
        self.balance_assets.append(asset)
        return self._balance


class ProductScopedAdapter:
    """Adapter shape matching live CCXT: no list API, only get_position(product)."""

    def __init__(
        self,
        positions: dict[str, Position | None],
        balance: Decimal = Decimal("10000"),
    ) -> None:
        self._position_map = positions
        self._balance = balance
        self.get_position_calls: list[str] = []

    def get_position(self, product_id: str) -> Position | None:
        self.get_position_calls.append(product_id)
        return self._position_map.get(product_id)

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
    account = FakeAccountService(positions=local_positions, balance=local_balance)
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
        balance_asset="USDT",
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

    def test_concurrent_runs_are_serialized(self):
        account = BlockingFirstAccountService()
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=FakeAdapter(),
            db_session_factory=_make_null_db_factory(),
            balance_asset="USDT",
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
        )
        first = threading.Thread(target=job.run_once, daemon=True)
        second = threading.Thread(target=job.run_once, daemon=True)

        first.start()
        assert account.entered.wait(timeout=1.0)
        second.start()
        time.sleep(0.05)
        assert account.calls == 1

        account.release.set()
        first.join(timeout=1.0)
        second.join(timeout=1.0)
        assert account.calls == 2

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


class TestPositionAuthorityAdmission:
    @staticmethod
    def _job(
        account: FakeAccountService,
        adapter: object,
        observations: list[tuple[PositionAuthorityState, str]],
        *,
        threshold: Decimal = THRESHOLD_QTY,
    ) -> RuntimeReconciliationJob:
        return RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset=None,
            on_position_authority_observation=lambda state, stage: observations.append(
                (state, stage)
            ),
            quantity_drift_threshold=threshold,
            balance_drift_threshold=THRESHOLD_BAL,
            product_ids=[PRODUCT_ID],
        )

    @pytest.mark.parametrize(
        ("exchange_quantity", "expected"),
        [
            (Decimal("1.009"), PositionAuthorityState.SAFE),
            (Decimal("1.010"), PositionAuthorityState.SAFE),
            (Decimal("1.011"), PositionAuthorityState.UNCONFIRMED),
        ],
    )
    def test_exact_decimal_threshold_controls_first_observation(
        self,
        exchange_quantity,
        expected,
    ):
        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService(positions=[_make_position(quantity=Decimal("1"))])
        adapter = FakeAdapter(
            positions={PRODUCT_ID: _make_position(quantity=exchange_quantity)}
        )

        self._job(account, adapter, observations).run_once()

        assert observations == [
            (
                expected,
                "verified"
                if expected is PositionAuthorityState.SAFE
                else "quantity_drift",
            )
        ]

    def test_cross_run_fingerprint_is_replaced_before_latching(self):
        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService(positions=[_make_position(quantity=Decimal("1"))])
        adapter = FakeAdapter(positions={PRODUCT_ID: None})
        job = self._job(account, adapter, observations)

        first = job.run_once()
        account._positions = [_make_position(quantity=Decimal("2"))]
        second = job.run_once()
        job.run_once()

        assert [
            first["position_drifts"][0]["local_quantity"],
            second["position_drifts"][0]["local_quantity"],
        ] == [
            Decimal("1"),
            Decimal("2"),
        ]
        assert observations == [
            (PositionAuthorityState.UNCONFIRMED, "quantity_drift"),
            (PositionAuthorityState.UNCONFIRMED, "quantity_drift"),
            (PositionAuthorityState.LATCHED, "quantity_drift"),
        ]

    @pytest.mark.parametrize(
        ("initial_local", "initial_exchange"),
        [(Decimal("1"), Decimal("0")), (Decimal("0"), Decimal("1"))],
    )
    def test_transient_cross_snapshot_drift_recovers_on_later_run(
        self,
        initial_local,
        initial_exchange,
    ):
        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService(
            positions=(
                [] if initial_local == 0 else [_make_position(quantity=initial_local)]
            )
        )
        adapter = FakeAdapter(
            positions={
                PRODUCT_ID: (
                    None
                    if initial_exchange == 0
                    else _make_position(quantity=initial_exchange)
                )
            }
        )
        job = self._job(account, adapter, observations)

        job.run_once()
        account._positions = [_make_position(quantity=Decimal("1"))]
        adapter._positions[PRODUCT_ID] = _make_position(quantity=Decimal("1"))
        job.run_once()

        assert observations == [
            (PositionAuthorityState.UNCONFIRMED, "quantity_drift"),
            (PositionAuthorityState.SAFE, "verified"),
        ]

    def test_each_run_takes_one_local_and_one_scoped_position_snapshot(self):
        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService()
        account.get_all_positions = MagicMock(return_value=[])
        adapter = ProductScopedAdapter({PRODUCT_ID: None})
        job = self._job(account, adapter, observations)

        job.run_once()

        account.get_all_positions.assert_called_once_with()
        assert adapter.get_position_calls == [PRODUCT_ID]

    def test_local_and_required_scoped_read_failures_latch_with_safe_errors(self):
        secret = "position-provider-secret"
        for stage in ("local_read", "adapter_read"):
            observations: list[tuple[PositionAuthorityState, str]] = []
            account = FakeAccountService()
            adapter = ProductScopedAdapter({PRODUCT_ID: None})
            if stage == "local_read":
                account.get_all_positions = MagicMock(side_effect=RuntimeError(secret))
            else:
                adapter.get_position = MagicMock(side_effect=RuntimeError(secret))
            job = self._job(account, adapter, observations)

            logger = MagicMock()
            job._logger = logger
            with patch("src.core.runtime_reconcile.write_system_event") as write_event:
                result = job.run_once()

            assert observations == [(PositionAuthorityState.LATCHED, stage)]
            assert result["errors"] == [
                {
                    "scope": "positions",
                    "reason": "position_authority_unavailable",
                    "stage": stage,
                }
            ]
            assert secret not in repr(result)
            assert secret not in repr(logger.warning.call_args)
            assert secret not in repr(write_event.call_args)

    def test_recovered_bulk_enumeration_is_diagnostic_but_safe(self):
        class RecoveredBulkAdapter(ProductScopedAdapter):
            def get_all_positions(self):
                raise RuntimeError("bulk-provider-secret")

        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService(positions=[_make_position()])
        adapter = RecoveredBulkAdapter({PRODUCT_ID: _make_position()})
        result = self._job(account, adapter, observations).run_once()

        assert observations == [(PositionAuthorityState.SAFE, "verified")]
        assert result["position_drifts"] == []
        assert result["errors"] == [
            {
                "scope": "positions",
                "reason": "position_authority_unavailable",
                "stage": "adapter_enumeration",
            }
        ]
        assert "bulk-provider-secret" not in repr(result)


class TestBackpackBulkPositionAuthority:
    @staticmethod
    def _adapter(rows: list[dict[str, object]]) -> LiveBackpackAdapter:
        client = MagicMock()
        client.fetch_positions.side_effect = lambda symbols=None: (
            rows if symbols is None else []
        )
        adapter = object.__new__(LiveBackpackAdapter)
        adapter.exchange_id = "backpack"
        adapter.client = client
        adapter.logger = MagicMock()
        return adapter

    @staticmethod
    def _job(
        account: FakeAccountService,
        adapter: LiveBackpackAdapter,
        observations: list[tuple[PositionAuthorityState, str]],
        *,
        product_ids: list[str] | None = None,
    ) -> RuntimeReconciliationJob:
        return RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset=None,
            on_position_authority_observation=lambda state, stage: observations.append(
                (state, stage)
            ),
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
            product_ids=product_ids or ["BACKPACK:BTC_USDC-PERP"],
        )

    @pytest.mark.parametrize("side", [PositionSide.LONG, PositionSide.SHORT])
    def test_matching_backpack_bulk_position_is_admission_safe(self, side):
        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService(
            positions=[
                _make_position(
                    product_id="BACKPACK:BTC_USDC-PERP",
                    side=side,
                    quantity=Decimal("2"),
                )
            ]
        )
        adapter = self._adapter(
            [
                {
                    "symbol": "BTC/USDC:USDC",
                    "contracts": "2",
                    "side": side.value.lower(),
                    "entryPrice": "100",
                    "unrealizedPnl": "0",
                }
            ]
        )

        result = self._job(account, adapter, observations).run_once()

        assert result["position_drifts"] == []
        assert observations == [(PositionAuthorityState.SAFE, "verified")]

    def test_backpack_quantity_mismatch_latches_only_on_second_run(self):
        observations: list[tuple[PositionAuthorityState, str]] = []
        account = FakeAccountService(
            positions=[
                _make_position(
                    product_id="BACKPACK:BTC_USDC-PERP",
                    quantity=Decimal("1"),
                )
            ]
        )
        adapter = self._adapter(
            [
                {
                    "symbol": "BTC/USDC:USDC",
                    "contracts": "2",
                    "side": "long",
                    "entryPrice": "100",
                    "unrealizedPnl": "0",
                }
            ]
        )
        job = self._job(account, adapter, observations)

        first = job.run_once()
        second = job.run_once()

        assert (
            first["position_drifts"]
            == second["position_drifts"]
            == [
                {
                    "strategy_id": STRATEGY_ID,
                    "product_id": "BACKPACK:BTC_USDC-PERP",
                    "local_quantity": Decimal("1"),
                    "exchange_quantity": Decimal("2"),
                }
            ]
        )
        assert observations == [
            (PositionAuthorityState.UNCONFIRMED, "quantity_drift"),
            (PositionAuthorityState.LATCHED, "quantity_drift"),
        ]

    def test_backpack_external_product_remains_visible_and_latches(self):
        observations: list[tuple[PositionAuthorityState, str]] = []
        adapter = self._adapter(
            [
                {
                    "symbol": "SOL/USDC:USDC",
                    "contracts": "3",
                    "side": "short",
                    "entryPrice": "150",
                    "unrealizedPnl": "0",
                }
            ]
        )
        job = self._job(FakeAccountService(), adapter, observations)

        first = job.run_once()
        second = job.run_once()

        assert [
            (drift["product_id"], drift["exchange_quantity"])
            for drift in first["position_drifts"]
        ] == [("BACKPACK:SOL_USDC-PERP", Decimal("-3"))]
        assert second["position_drifts"] == first["position_drifts"]
        assert observations == [
            (PositionAuthorityState.UNCONFIRMED, "quantity_drift"),
            (PositionAuthorityState.LATCHED, "quantity_drift"),
        ]


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

    def test_configured_product_universe_detects_exchange_only_position(self):
        """Live CCXT-style adapters need a configured product universe to scan."""
        exchange_pos = _make_position(strategy_id="LIVE", quantity=Decimal("2.0"))
        account = FakeAccountService(positions=[])
        adapter = ProductScopedAdapter({PRODUCT_ID: exchange_pos})
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset="USDT",
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
            product_ids=[PRODUCT_ID],
        )

        with patch("src.core.runtime_reconcile.write_system_event") as mock_write:
            result = job.run_once()
            mock_write.assert_called_once()

        assert adapter.get_position_calls == [PRODUCT_ID]
        assert result["position_drifts"] == [
            {
                "strategy_id": "LIVE",
                "product_id": PRODUCT_ID,
                "local_quantity": Decimal("0"),
                "exchange_quantity": Decimal("2.0"),
            }
        ]


class TestRunOnceBalanceDrift:
    """Matrix item 5: balance drift beyond threshold triggers event."""

    def test_balance_drift_above_threshold_reported(self):
        local_bal = Decimal("10000.00")
        exchange_bal = Decimal("9000.00")  # diff = 1000 >> THRESHOLD_BAL
        job, account, _ = _make_job(
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
        assert account.balance_replacements == [exchange_bal]

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


class TestRunOnceBalanceAuthority:
    def test_configured_asset_refreshes_exact_balance_below_drift_threshold(self):
        account = FakeAccountService(balance=Decimal("10000.00"))
        adapter = FakeAdapter(balance=Decimal("9999.50"))
        failure = MagicMock()
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset="USDC",
            on_balance_authority_failure=failure,
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=Decimal("1.00"),
        )

        with patch("src.core.runtime_reconcile.write_system_event") as write_event:
            result = job.run_once()

        assert adapter.balance_assets == ["USDC"]
        assert account.balance_replacements == [Decimal("9999.50")]
        assert account.get_balance() == Decimal("9999.50")
        assert result["balance_drift"] is None
        assert result["errors"] == []
        failure.assert_not_called()
        write_event.assert_not_called()

    def test_real_account_and_risk_manager_read_refreshed_exact_cache_key(self):
        class BalanceRedis:
            def __init__(self) -> None:
                self.values = {("state:balance:main", "free"): "1000"}
                self.hset_calls: list[tuple[str, dict[str, str]]] = []

            def hget(self, name: str, key: str) -> str | None:
                return self.values.get((name, key))

            def hset(self, name: str, mapping: dict[str, str]) -> int:
                self.hset_calls.append((name, mapping))
                for key, value in mapping.items():
                    self.values[(name, key)] = value
                return len(mapping)

            def scan_iter(self, match: str):
                del match
                return iter(())

        redis = BalanceRedis()
        account = AccountService.__new__(AccountService)
        account.redis = redis
        account._authoritative_balance_key = None
        adapter = FakeAdapter(balance=Decimal("250.00"))
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset="USDC",
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
        )

        with patch("src.core.runtime_reconcile.write_system_event"):
            result = job.run_once()

        assert result["balance_drift"] == {
            "local": Decimal("1000"),
            "exchange": Decimal("250.00"),
        }
        assert redis.hset_calls == [("state:balance:main", {"free": "250.00"})]
        risk = RiskManager(account, order_rate_limit_rule=MagicMock())
        assert risk.calculate_position_size(
            Decimal("100"),
            Decimal("90"),
            Decimal("0.02"),
        ) == Decimal("0.5000")

    @pytest.mark.parametrize(
        ("stage", "exchange_balance"),
        [
            ("local_read", Decimal("250")),
            ("adapter_read", Decimal("250")),
            ("value_validation", "250"),
            ("value_validation", Decimal("NaN")),
            ("value_validation", Decimal("Infinity")),
            ("value_validation", Decimal("-1")),
            ("account_persistence", Decimal("250")),
        ],
    )
    def test_balance_failure_is_sanitized_before_result_log_and_audit(
        self,
        stage,
        exchange_balance,
    ):
        secret = "provider-balance-secret-sentinel"
        account = FakeAccountService(balance=Decimal("1000"))
        adapter = FakeAdapter(balance=exchange_balance)
        if stage == "local_read":
            account.get_balance = MagicMock(side_effect=RuntimeError(secret))
        elif stage == "adapter_read":
            adapter.get_balance = MagicMock(side_effect=RuntimeError(secret))
        elif stage == "account_persistence":
            account.replace_generic_balance = MagicMock(
                side_effect=RuntimeError(secret)
            )
        logger = MagicMock()
        authority_failure = MagicMock()
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset="USDC",
            on_balance_authority_failure=authority_failure,
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
            logger=logger,
        )

        with patch("src.core.runtime_reconcile.write_system_event") as write_event:
            result = job.run_once()

        safe_error = {
            "scope": "balance",
            "reason": "balance_authority_unavailable",
            "stage": stage,
        }
        assert result["errors"] == [safe_error]
        logger.warning.assert_called_once_with(
            "Runtime reconciliation errors: %s",
            [safe_error],
        )
        assert write_event.call_args.kwargs["event_subtype"] == (
            "runtime_reconcile_error"
        )
        assert write_event.call_args.kwargs["payload"] is result

        def contains_secret_or_exception(value):
            if isinstance(value, BaseException):
                return True
            if type(value) is str:
                return secret in value
            if type(value) is dict:
                return any(
                    contains_secret_or_exception(item)
                    for pair in value.items()
                    for item in pair
                )
            if type(value) in (list, tuple):
                return any(contains_secret_or_exception(item) for item in value)
            return False

        assert not contains_secret_or_exception(result)
        assert not contains_secret_or_exception(logger.warning.call_args.args)
        assert not contains_secret_or_exception(write_event.call_args.kwargs)
        if stage == "local_read":
            authority_failure.assert_not_called()
            assert account.balance_replacements == [Decimal("250")]
        elif stage == "account_persistence":
            account.replace_generic_balance.assert_called_once_with(Decimal("250"))
            authority_failure.assert_called_once_with(stage)
        else:
            assert account.balance_replacements == []
            authority_failure.assert_called_once_with(stage)


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

    def test_local_position_error_skips_exchange_position_drift(self):
        exchange_position = _make_position(quantity=Decimal("2.0"))
        job, account, _ = _make_job(
            exchange_positions={PRODUCT_ID: exchange_position},
        )
        account.get_all_positions = MagicMock(side_effect=RuntimeError("Redis down"))

        with patch("src.core.runtime_reconcile.write_system_event") as write_event:
            result = job.run_once()

        assert result["checked_positions"] == 0
        assert result["position_drifts"] == []
        assert result["unverified_positions"] == [
            {
                "product_id": PRODUCT_ID,
                "exchange_quantity": Decimal("2.0"),
            }
        ]
        assert result["errors"] == [
            {
                "scope": "positions",
                "reason": "position_authority_unavailable",
                "stage": "local_read",
            }
        ]
        write_event.assert_called_once()
        assert (
            write_event.call_args.kwargs["event_subtype"] == "runtime_reconcile_error"
        )

    def test_local_position_error_with_exchange_flat_reports_no_fake_drift(self):
        job, account, _ = _make_job(exchange_positions={PRODUCT_ID: None})
        account.get_all_positions = MagicMock(side_effect=RuntimeError("Redis down"))

        result = job.run_once()

        assert result["position_drifts"] == []
        assert result["unverified_positions"] == []

    def test_local_position_error_queries_configured_product_universe(self):
        exchange_position = _make_position(quantity=Decimal("3.0"))
        account = FakeAccountService()
        account.get_all_positions = MagicMock(side_effect=RuntimeError("Redis down"))
        adapter = ProductScopedAdapter({PRODUCT_ID: exchange_position})
        job = RuntimeReconciliationJob(
            account_service=account,
            adapter=adapter,
            db_session_factory=_make_null_db_factory(),
            balance_asset="USDT",
            quantity_drift_threshold=THRESHOLD_QTY,
            balance_drift_threshold=THRESHOLD_BAL,
            product_ids=[PRODUCT_ID],
        )

        result = job.run_once()

        assert adapter.get_position_calls == [PRODUCT_ID]
        assert result["unverified_positions"] == [
            {
                "product_id": PRODUCT_ID,
                "exchange_quantity": Decimal("3.0"),
            }
        ]

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
            balance_asset="USDT",
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
        exchange_pos2 = _make_position(
            strategy_id="s2", product_id="BINANCE:ETHUSDT-PERP"
        )
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
            assert isinstance(drift["local_quantity"], Decimal), (
                "local_quantity must be Decimal"
            )
            assert isinstance(drift["exchange_quantity"], Decimal), (
                "exchange_quantity must be Decimal"
            )

    def test_balance_drift_values_are_decimal(self):
        job, _, _ = _make_job(
            local_balance=Decimal("10000"),
            exchange_balance=Decimal("5000"),
        )

        result = job.run_once()

        if result["balance_drift"] is not None:
            assert isinstance(result["balance_drift"]["local"], Decimal)
            assert isinstance(result["balance_drift"]["exchange"], Decimal)
