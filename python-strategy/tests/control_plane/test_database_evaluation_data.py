from __future__ import annotations

from decimal import Decimal, InvalidOperation

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.evaluation_data import (
    CsvEvaluationDataSourceProvider,
    DatabaseEvaluationDataSourceProvider,
)
from src.control_plane.models import ParameterCandidate, ParameterSearchJobRequest
from src.control_plane.parameter_evaluation import ResearchBacktestParameterEvaluator
from src.core.orm_models import (
    Base,
    Exchange,
    Product,
    ResearchCandlestick,
    ResearchDataset,
)
from src.core.research_datasets import (
    ResearchDatasetImporter,
    ResearchDatasetIntegrityError,
    ResearchDatasetSpec,
)
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Signal, SignalType

PRODUCT = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "1m"
START = 1_704_067_200_000
END = START + 420_000
DATASET = "sealed-btc-1m-v1"


@pytest.fixture
def stored_dataset(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'datasets.db'}")
    Base.metadata.create_all(
        engine,
        tables=[
            Exchange.__table__,
            Product.__table__,
            ResearchDataset.__table__,
            ResearchCandlestick.__table__,
        ],
    )
    factory = sessionmaker(bind=engine)
    csv_path = tmp_path / "candles.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        f"{START},100,101,99,100,1\n"
        f"{START + 60_000},100,102,99,101,2\n"
        f"{START + 120_000},101,103,100,102,3\n"
        f"{START + 180_000},102,103,99,100,4\n"
        f"{START + 240_000},100,102,98,99,5\n"
        f"{START + 300_000},99,101,98,101,6\n"
        f"{START + 360_000},101,104,100,103,7\n"
        f"{START + 420_000},103,104,100,101,8\n",
        encoding="utf-8",
    )
    ResearchDatasetImporter(session_factory=factory).import_csv(
        csv_path,
        ResearchDatasetSpec(
            dataset_id=DATASET,
            product_id=PRODUCT,
            timeframe=TIMEFRAME,
            source="synthetic-test",
            revision="v1",
        ),
    )
    try:
        yield factory, csv_path
    finally:
        engine.dispose()


def _request(
    start: int = START,
    end: int = START + 420_000,
    csv_path=None,
) -> ParameterSearchJobRequest:
    backtest = {"initial_balance": "1000", "maker_fee": "0", "taker_fee": "0"}
    if csv_path is not None:
        backtest["candles_csv_path"] = str(csv_path)
    return ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "noop",
            "product_id": PRODUCT,
            "timeframe": TIMEFRAME,
            "start_time": start,
            "end_time": end,
            "backtest": backtest,
            "candidates": [{"candidate_id": "a", "param_pack": {}}],
        }
    )


class _AlternatingStrategy(BaseStrategy):
    def __init__(self, strategy_id, product_id):
        super().__init__(strategy_id, product_id)
        self._index = 0

    @property
    def requirements(self):
        return StrategyRequirements(self.product_id, TIMEFRAME, 1)

    def on_candle(self, candle, context=None):
        self._index += 1
        signal_type = (
            SignalType.LONG
            if self._index in {1, 5}
            else SignalType.SHORT
            if self._index in {3, 7}
            else SignalType.NO_SIGNAL
        )
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=signal_type,
            value=candle.close,
            quantity=Decimal("1") if signal_type != SignalType.NO_SIGNAL else None,
        )


def _factory(strategy_id, product_id, timeframe, param_pack):
    return _AlternatingStrategy(strategy_id, product_id)


def test_bound_provider_reads_sealed_decimal_rows_and_inclusive_single_point(
    stored_dataset,
):
    factory, _ = stored_dataset
    provider = DatabaseEvaluationDataSourceProvider(DATASET, session_factory=factory)
    request = _request(START + 60_000, START + 60_000)

    source = provider.create(request)
    candles = list(
        source.get_candles(PRODUCT, TIMEFRAME, request.start_time, request.end_time)
    )

    assert isinstance(source, ResearchDatabaseDataSource)
    assert len(candles) == 1
    assert candles[0].timestamp == START + 60_000
    assert candles[0].close == Decimal("101")


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"product_id": "BINANCE:ETHUSDT-PERP"}, "product/timeframe"),
        ({"timeframe": "5m"}, "product/timeframe"),
        ({"start_time": START - 1}, "coverage"),
        ({"end_time": END + 1}, "coverage"),
        ({"start_time": END, "end_time": START}, "range"),
    ],
)
def test_provider_rejects_identity_and_range_before_load(
    stored_dataset,
    changes,
    message,
    monkeypatch,
):
    factory, _ = stored_dataset
    provider = DatabaseEvaluationDataSourceProvider(DATASET, session_factory=factory)
    request = _request().model_copy(update=changes)

    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_candles",
        lambda *args, **kwargs: pytest.fail("candles loaded before validation"),
    )
    for method in (provider.create, provider.cache_key):
        with pytest.raises(ValueError, match=message):
            method(request)


@pytest.mark.parametrize("dataset_state", ["absent", "unsealed", "unvalidated"])
def test_provider_preserves_seal_integrity_failure(
    stored_dataset,
    dataset_state,
    monkeypatch,
):
    factory, _ = stored_dataset
    dataset_id = "missing" if dataset_state == "absent" else DATASET
    if dataset_state != "absent":
        with factory() as session:
            row = session.get(ResearchDataset, DATASET)
            assert row is not None
            if dataset_state == "unsealed":
                row.lifecycle_state = "importing"
                row.sealed_at = None
            else:
                session.connection().exec_driver_sql(
                    "PRAGMA ignore_check_constraints=ON"
                )
                row.quality_status = "pending"
            session.commit()
    provider = DatabaseEvaluationDataSourceProvider(dataset_id, session_factory=factory)
    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_candles",
        lambda *args, **kwargs: pytest.fail("candles loaded before validation"),
    )

    for method in (provider.create, provider.cache_key):
        with pytest.raises(ResearchDatasetIntegrityError, match="integrity validation"):
            method(_request())


def test_provider_cache_key_is_bound_to_dataset_and_inclusive_range(stored_dataset):
    factory, _ = stored_dataset
    provider = DatabaseEvaluationDataSourceProvider(DATASET, session_factory=factory)
    request = _request()
    other_dataset_id = "sealed-btc-1m-v2"
    ResearchDatasetImporter(session_factory=factory).import_csv(
        stored_dataset[1],
        ResearchDatasetSpec(
            dataset_id=other_dataset_id,
            product_id=PRODUCT,
            timeframe=TIMEFRAME,
            source="synthetic-test",
            revision="v2",
        ),
    )

    assert provider.cache_key(request) == (DATASET, PRODUCT, TIMEFRAME, START, END)
    assert provider.cache_key(_request(START + 60_000, END)) != provider.cache_key(
        request
    )
    assert DatabaseEvaluationDataSourceProvider(
        other_dataset_id, session_factory=factory
    ).cache_key(request) != provider.cache_key(request)
    with pytest.raises(ValueError, match="dataset_id"):
        DatabaseEvaluationDataSourceProvider(" ", session_factory=factory)


def test_research_evaluator_preloads_once_and_matches_csv_evaluation(
    stored_dataset, monkeypatch
):
    factory, csv_path = stored_dataset
    db_provider = DatabaseEvaluationDataSourceProvider(DATASET, session_factory=factory)
    csv_provider = CsvEvaluationDataSourceProvider()
    calls = 0
    original = ResearchDatabaseDataSource.get_candles

    def counted(source, *args, **kwargs):
        nonlocal calls
        calls += 1
        yield from original(source, *args, **kwargs)

    monkeypatch.setattr(ResearchDatabaseDataSource, "get_candles", counted)
    db_evaluator = ResearchBacktestParameterEvaluator(
        _factory, data_source_provider=db_provider
    )
    csv_evaluator = ResearchBacktestParameterEvaluator(
        _factory, data_source_provider=csv_provider
    )
    request = _request(csv_path=csv_path)
    first = ParameterCandidate(candidate_id="a", param_pack={})
    second = ParameterCandidate(candidate_id="b", param_pack={})

    db_result = db_evaluator.evaluate(request, first)
    db_evaluator.evaluate(request, second)
    csv_result = csv_evaluator.evaluate(request, first)

    assert calls == 1
    assert db_result.score_total != 0
    assert db_result.metrics["closed_trade_count"] > 0
    assert db_result.score_total == csv_result.score_total
    assert db_result.max_drawdown == csv_result.max_drawdown
    _assert_decimal_tree_equal(db_result.metrics, csv_result.metrics)


def _assert_decimal_tree_equal(left, right):
    if isinstance(left, dict):
        assert left.keys() == right.keys()
        for key in left:
            _assert_decimal_tree_equal(left[key], right[key])
    elif isinstance(left, list):
        assert len(left) == len(right)
        for left_item, right_item in zip(left, right):
            _assert_decimal_tree_equal(left_item, right_item)
    elif isinstance(left, str) and isinstance(right, str):
        try:
            assert Decimal(left) == Decimal(right)
        except InvalidOperation:
            assert left == right
    else:
        assert left == right
