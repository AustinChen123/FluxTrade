import pytest

from src.core.evaluation_set import EvaluationDataset, EvaluationSet


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"


def test_evaluation_set_accepts_single_dataset():
    dataset = EvaluationDataset(
        dataset_id="full_history",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=1_700_000_000_000,
        end_time=1_700_086_400_000,
    )

    evaluation_set = EvaluationSet.single(dataset)

    assert len(evaluation_set) == 1
    assert list(evaluation_set) == [dataset]
    assert evaluation_set["full_history"] is dataset
    assert dataset.replay_start_time == dataset.start_time


def test_evaluation_set_accepts_multiple_datasets():
    trend = EvaluationDataset(
        dataset_id="trend_regime",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=1_700_000_000_000,
        end_time=1_700_086_400_000,
        metadata={"regime": "trend"},
    )
    chop = EvaluationDataset(
        dataset_id="chop_regime",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=1_700_086_400_000,
        end_time=1_700_172_800_000,
        metadata={"regime": "chop"},
    )

    evaluation_set = EvaluationSet((trend, chop))

    assert [dataset.dataset_id for dataset in evaluation_set] == [
        "trend_regime",
        "chop_regime",
    ]
    assert evaluation_set["chop_regime"].metadata["regime"] == "chop"


def test_evaluation_set_rejects_duplicate_dataset_ids():
    first = EvaluationDataset(
        dataset_id="duplicate",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=1,
        end_time=2,
    )
    second = EvaluationDataset(
        dataset_id="duplicate",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=3,
        end_time=4,
    )

    with pytest.raises(ValueError, match="duplicate dataset_id values: duplicate"):
        EvaluationSet((first, second))


def test_evaluation_set_rejects_empty_collections():
    with pytest.raises(ValueError, match="at least one dataset"):
        EvaluationSet(())


def test_evaluation_dataset_validates_time_range():
    with pytest.raises(ValueError, match="end_time must be greater than start_time"):
        EvaluationDataset(
            dataset_id="bad_range",
            product_id=PRODUCT_ID,
            timeframe="5m",
            start_time=2,
            end_time=2,
        )


def test_evaluation_dataset_validates_warmup_range():
    with pytest.raises(ValueError, match="warmup_start_time must be <= start_time"):
        EvaluationDataset(
            dataset_id="bad_warmup",
            product_id=PRODUCT_ID,
            timeframe="5m",
            start_time=10,
            end_time=20,
            warmup_start_time=11,
        )


def test_evaluation_dataset_uses_warmup_as_replay_start():
    dataset = EvaluationDataset(
        dataset_id="with_warmup",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=10,
        end_time=20,
        warmup_start_time=5,
    )

    assert dataset.replay_start_time == 5


def test_evaluation_dataset_trims_required_string_fields():
    dataset = EvaluationDataset(
        dataset_id="  trimmed  ",
        product_id=f"  {PRODUCT_ID}  ",
        timeframe="  5m  ",
        start_time=1,
        end_time=2,
    )

    assert dataset.dataset_id == "trimmed"
    assert dataset.product_id == PRODUCT_ID
    assert dataset.timeframe == "5m"


def test_evaluation_dataset_rejects_blank_required_string_fields():
    with pytest.raises(ValueError, match="dataset_id must be non-empty"):
        EvaluationDataset(
            dataset_id=" ",
            product_id=PRODUCT_ID,
            timeframe="5m",
            start_time=1,
            end_time=2,
        )


def test_evaluation_dataset_metadata_is_read_only_snapshot():
    raw_metadata = {"regime": "trend"}
    dataset = EvaluationDataset(
        dataset_id="immutable_metadata",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=1,
        end_time=2,
        metadata=raw_metadata,
    )

    raw_metadata["regime"] = "mutated"

    assert dataset.metadata["regime"] == "trend"
    with pytest.raises(TypeError):
        dataset.metadata["regime"] = "sideways"


def test_evaluation_set_raises_key_error_for_unknown_dataset():
    dataset = EvaluationDataset(
        dataset_id="known",
        product_id=PRODUCT_ID,
        timeframe="5m",
        start_time=1,
        end_time=2,
    )
    evaluation_set = EvaluationSet.single(dataset)

    with pytest.raises(KeyError):
        evaluation_set["missing"]
