from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.evaluation_data import (
    CsvEvaluationDataSourceProvider,
    RequestEvaluationDataSourceProvider,
)
from src.control_plane.models import ParameterSearchJobRequest
from src.core.data_sources.csv_source import CsvDataSource
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import (
    Base,
    Exchange,
    Product,
    ResearchCandlestick,
    ResearchDataset,
)
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec

PRODUCT = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "1m"
START = 1_704_067_200_000
END = START + 60_000
DATASET_ID = "sealed-search-test-v1"


@pytest.fixture
def sealed_store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'sealed-search.db'}")
    Base.metadata.create_all(
        engine,
        tables=[
            Exchange.__table__,
            Product.__table__,
            ResearchDataset.__table__,
            ResearchCandlestick.__table__,
        ],
    )
    sessions = sessionmaker(bind=engine)
    csv_path = tmp_path / "candles.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        f"{START},100,101,99,100,1\n{END},100,102,99,101,2\n",
        encoding="utf-8",
    )
    ResearchDatasetImporter(session_factory=sessions).import_csv(
        csv_path,
        ResearchDatasetSpec(
            dataset_id=DATASET_ID,
            product_id=PRODUCT,
            timeframe=TIMEFRAME,
            source="synthetic-test",
            revision="v1",
        ),
    )
    try:
        yield sessions, csv_path
    finally:
        engine.dispose()


def _request(**updates) -> ParameterSearchJobRequest:
    payload = {
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross_search",
        "product_id": PRODUCT,
        "timeframe": TIMEFRAME,
        "start_time": START,
        "end_time": END,
        "backtest": {"initial_balance": "1000", "maker_fee": "0", "taker_fee": "0"},
        "candidates": [{"candidate_id": "one", "param_pack": {}}],
    }
    payload.update(updates)
    return ParameterSearchJobRequest.model_validate(payload)


def test_sealed_market_data_reference_is_strict_and_round_trips():
    request = _request(
        market_data={"kind": "sealed_dataset", "dataset_id": "  exact-id  "}
    )

    assert request.market_data is not None
    assert request.market_data.dataset_id == "  exact-id  "
    assert (
        ParameterSearchJobRequest.model_validate(request.model_dump(mode="json"))
        == request
    )
    boundary_request = _request(
        market_data={"kind": "sealed_dataset", "dataset_id": "x" * 128}
    )
    assert boundary_request.market_data is not None
    assert boundary_request.market_data.dataset_id == "x" * 128


@pytest.mark.parametrize(
    "market_data",
    [
        {"dataset_id": DATASET_ID},
        {"kind": "csv", "dataset_id": DATASET_ID},
        {"kind": "sealed_dataset", "dataset_id": ""},
        {"kind": "sealed_dataset", "dataset_id": "   "},
        {"kind": "sealed_dataset", "dataset_id": "x" * 129},
        {"kind": "sealed_dataset", "dataset_id": DATASET_ID, "path": "candles.csv"},
        {"kind": "sealed_dataset", "datasetId": DATASET_ID},
    ],
)
def test_sealed_market_data_reference_rejects_invalid_shape(market_data):
    with pytest.raises(ValidationError):
        _request(market_data=market_data)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"strategy_type": "csv_signal"}, "golden_cross"),
        ({"backtest": None}, "backtest"),
        ({"backtest": {"candles_csv_path": "candles.csv"}}, "candles_csv_path"),
        ({"backtest": {"write_reports": True}}, "write_reports"),
        ({"fitness": {}}, "fitness"),
        (
            {
                "evaluation_set": {
                    "datasets": [
                        {
                            "dataset_id": "fold",
                            "product_id": PRODUCT,
                            "timeframe": TIMEFRAME,
                            "start_time": START,
                            "end_time": END,
                        }
                    ]
                }
            },
            "evaluation_set",
        ),
        (
            {
                "evaluation_set": {
                    "datasets": [
                        {
                            "dataset_id": "fold",
                            "product_id": PRODUCT,
                            "timeframe": TIMEFRAME,
                            "start_time": START,
                            "end_time": END,
                        }
                    ]
                },
                "fitness": {},
            },
            "evaluation_set",
        ),
    ],
)
def test_sealed_request_rejects_conflicting_modes(updates, message):
    with pytest.raises(ValidationError) as exc_info:
        _request(
            market_data={"kind": "sealed_dataset", "dataset_id": DATASET_ID}, **updates
        )
    assert message in exc_info.value.errors()[0]["msg"]


@pytest.mark.parametrize("value", [None, "absent"])
def test_legacy_market_data_is_omitted_from_serialization(value):
    updates = {} if value == "absent" else {"market_data": value}
    request = _request(
        **updates,
        backtest={"candles_csv_path": "local.csv"},
    )

    assert request.model_dump(mode="json").get("market_data", "absent") == "absent"
    assert (
        ParameterSearchJobRequest.model_validate(request.model_dump(mode="json"))
        == request
    )


def test_request_provider_selects_sealed_dataset_and_preserves_legacy_csv(sealed_store):
    sessions, csv_path = sealed_store
    provider = RequestEvaluationDataSourceProvider(session_factory=sessions)
    sealed_request = _request(
        market_data={"kind": "sealed_dataset", "dataset_id": DATASET_ID}
    )
    legacy_request = _request(backtest={"candles_csv_path": str(csv_path)})

    sealed_source = provider.create(sealed_request)
    legacy_source = provider.create(legacy_request)
    assert isinstance(sealed_source, ResearchDatabaseDataSource)
    assert provider.cache_key(sealed_request) == (
        DATASET_ID,
        PRODUCT,
        TIMEFRAME,
        START,
        END,
    )
    assert isinstance(legacy_source, CsvDataSource)
    assert provider.cache_key(
        legacy_request
    ) == CsvEvaluationDataSourceProvider().cache_key(legacy_request)
    candles = list(sealed_source.get_candles(PRODUCT, TIMEFRAME, START, END))
    assert [candle.close for candle in candles] == [Decimal("100"), Decimal("101")]
