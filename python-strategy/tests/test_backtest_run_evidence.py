from dataclasses import replace
from decimal import Decimal, getcontext, localcontext
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.core.backtest.run_evidence import (
    BacktestDatasetEvidence,
    build_backtest_run_provenance,
    canonical_decision_snapshot,
    canonical_fill_records,
    configuration_sha256,
    _native_extension_path,
)
from src.core.models import Candlestick, OrderSide
from src.core.strategy_context import FillSnapshot, StrategyContext


def _candle(timestamp: int, *, close: str = "100") -> Candlestick:
    value = Decimal(close)
    return Candlestick(
        product_id="BINANCE:BTCUSDT-SPOT",
        timeframe="1h",
        timestamp=timestamp,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=Decimal("1"),
    )


def test_dataset_identity_is_ordered_exact_and_decimal_scale_neutral():
    canonical = BacktestDatasetEvidence()
    canonical.observe(_candle(0, close="100.0"))
    canonical.observe(_candle(1, close="101"))

    same_values = BacktestDatasetEvidence()
    same_values.observe(_candle(0, close="100.000"))
    same_values.observe(_candle(1, close="101.0"))

    changed = BacktestDatasetEvidence()
    changed.observe(_candle(0, close="100"))
    changed.observe(_candle(1, close="101.0001"))

    assert canonical.hexdigest() == same_values.hexdigest()
    assert canonical.hexdigest() != changed.hexdigest()
    assert canonical.count == 2
    assert canonical.first_timestamp == 0
    assert canonical.last_timestamp == 1


def test_dataset_identity_rejects_timestamp_regression_and_model_subclasses():
    evidence = BacktestDatasetEvidence()
    evidence.observe(_candle(2))
    with pytest.raises(ValueError, match="non-decreasing"):
        evidence.observe(_candle(1))

    class DerivedCandle(Candlestick):
        pass

    with pytest.raises(TypeError, match="exact Candlestick"):
        BacktestDatasetEvidence().observe(DerivedCandle(**_candle(0).model_dump()))


def test_configuration_identity_is_mapping_order_and_context_independent():
    original_precision = getcontext().prec
    left = {
        "fees": {"taker": Decimal("0.001"), "maker": Decimal("0")},
        "quantity": Decimal("0.1000"),
    }
    right = {
        "quantity": Decimal("0.1"),
        "fees": {"maker": Decimal("0.0"), "taker": Decimal("0.0010")},
    }
    with localcontext() as context:
        context.prec = 7
        left_hash = configuration_sha256(left)
    with localcontext() as context:
        context.prec = 50
        right_hash = configuration_sha256(right)

    assert left_hash == right_hash
    assert getcontext().prec == original_precision
    assert left_hash != configuration_sha256({**right, "quantity": Decimal("0.1001")})


class _Trade:
    def __init__(
        self,
        *,
        fill_sequence: int | None,
        price: Decimal = Decimal("100"),
    ) -> None:
        self.fill_sequence = fill_sequence
        self.strategy_id = "strategy"
        self.product_id = "BINANCE:BTCUSDT-SPOT"
        self.side = OrderSide.BUY
        self.price = price
        self.quantity = Decimal("0.1")
        self.fee = Decimal("0.01")
        self.fee_asset = "USDT"
        self.timestamp = 1
        self.order_id = "opaque-provider-id"


def test_fill_projection_preserves_economics_not_opaque_order_ids():
    record = canonical_fill_records([_Trade(fill_sequence=0)])[0]

    assert record.fill_sequence == 0
    assert record.side == "buy"
    assert record.price == Decimal("100")
    assert record.quantity == Decimal("0.1")
    assert record.fee == Decimal("0.01")
    assert record.fee_asset == "USDT"
    assert not hasattr(record, "order_id")

    with pytest.raises(ValueError, match="contiguous"):
        canonical_fill_records([_Trade(fill_sequence=1)])
    with pytest.raises(ValueError, match="finite exact Decimal"):
        canonical_fill_records([_Trade(fill_sequence=0, price=Decimal("NaN"))])


def test_decision_projection_ignores_only_opaque_fill_identity():
    def context(order_id: str) -> StrategyContext:
        return StrategyContext(
            strategy_id="strategy",
            product_id="BINANCE:BTCUSDT-SPOT",
            timestamp=1,
            available_cash=Decimal("90"),
            total_equity=Decimal("100"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            current_drawdown=Decimal("0"),
            max_drawdown=Decimal("0"),
            latest_fills=(
                FillSnapshot(
                    order_id=order_id,
                    product_id="BINANCE:BTCUSDT-SPOT",
                    side=OrderSide.BUY,
                    price=Decimal("100"),
                    quantity=Decimal("0.1"),
                    fee=Decimal("0"),
                    timestamp=1,
                ),
            ),
        )

    assert canonical_decision_snapshot(context("full-local-id")) == (
        canonical_decision_snapshot(context("research-local-id"))
    )
    changed = replace(context("id"), available_cash=Decimal("89"))
    assert canonical_decision_snapshot(context("id")) != (
        canonical_decision_snapshot(changed)
    )


def test_run_provenance_reports_all_required_hashes_and_matching_contract():
    dataset = BacktestDatasetEvidence()
    dataset.observe(_candle(0))
    provenance = build_backtest_run_provenance(
        runner_kind="research",
        dataset=dataset,
        configuration={"shared": Decimal("1")},
        runner_configuration={"runner": "research"},
        program_components=(_Trade,),
    )

    assert provenance.schema_version == "fluxtrade.backtest_run_provenance.v1"
    assert provenance.dataset_identity_version == "fluxtrade.backtest_dataset.v1"
    assert provenance.configuration_identity_version == (
        "fluxtrade.backtest_configuration.v1"
    )
    assert provenance.program_version == "0.1.0"
    assert provenance.extension_version == "0.1.0"
    assert provenance.matching_model == "atomic_whole_order_v1"
    for digest in (
        provenance.dataset_sha256,
        provenance.program_sha256,
        provenance.extension_sha256,
        provenance.configuration_sha256,
        provenance.runner_configuration_sha256,
    ):
        assert len(digest) == 64
        int(digest, 16)


def test_native_extension_path_accepts_package_and_top_level_layouts(tmp_path):
    native = SimpleNamespace(
        __file__=str(tmp_path / f"fluxtrade_core{EXTENSION_SUFFIXES[0]}")
    )
    package = SimpleNamespace(
        __file__=str(tmp_path / "fluxtrade_core/__init__.py"),
        fluxtrade_core=native,
    )

    assert _native_extension_path(native) == Path(native.__file__)
    assert _native_extension_path(package) == Path(native.__file__)

    with pytest.raises(RuntimeError, match="native extension path is unavailable"):
        _native_extension_path(SimpleNamespace(__file__="fluxtrade_core.py"))
