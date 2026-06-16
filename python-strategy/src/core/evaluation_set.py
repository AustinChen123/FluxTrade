"""Evaluation dataset boundaries for research and GA scoring.

This module describes *what* data ranges should be evaluated. It does not load
candles, run strategies, or decide whether ranges are sliding windows,
walk-forward splits, hand-picked regimes, or a single full-history dataset.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping


@dataclass(frozen=True, slots=True)
class EvaluationDataset:
    """One replay dataset used by research or optimizer evaluation."""

    dataset_id: str
    product_id: str
    timeframe: str
    start_time: int
    end_time: int
    warmup_start_time: int | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        dataset_id = self.dataset_id.strip()
        product_id = self.product_id.strip()
        timeframe = self.timeframe.strip()
        if not dataset_id:
            raise ValueError("dataset_id must be non-empty")
        if not product_id:
            raise ValueError("product_id must be non-empty")
        if not timeframe:
            raise ValueError("timeframe must be non-empty")
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be greater than start_time")
        if self.warmup_start_time is not None and self.warmup_start_time > self.start_time:
            raise ValueError("warmup_start_time must be <= start_time")

        object.__setattr__(self, "dataset_id", dataset_id)
        object.__setattr__(self, "product_id", product_id)
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @property
    def replay_start_time(self) -> int:
        """Start timestamp including warmup, if a warmup range is configured."""
        return self.warmup_start_time if self.warmup_start_time is not None else self.start_time


@dataclass(frozen=True, slots=True)
class EvaluationSet:
    """Immutable collection of evaluation datasets with unique ids."""

    datasets: tuple[EvaluationDataset, ...]

    def __init__(self, datasets: Iterable[EvaluationDataset]):
        dataset_tuple = tuple(datasets)
        if not dataset_tuple:
            raise ValueError("EvaluationSet requires at least one dataset")

        seen_ids: set[str] = set()
        duplicate_ids: set[str] = set()
        for dataset in dataset_tuple:
            if dataset.dataset_id in seen_ids:
                duplicate_ids.add(dataset.dataset_id)
            seen_ids.add(dataset.dataset_id)
        if duplicate_ids:
            duplicates = ", ".join(sorted(duplicate_ids))
            raise ValueError(f"duplicate dataset_id values: {duplicates}")

        object.__setattr__(self, "datasets", dataset_tuple)

    @classmethod
    def single(cls, dataset: EvaluationDataset) -> "EvaluationSet":
        """Create an evaluation set from one dataset."""
        return cls((dataset,))

    def __iter__(self) -> Iterator[EvaluationDataset]:
        return iter(self.datasets)

    def __len__(self) -> int:
        return len(self.datasets)

    def __getitem__(self, dataset_id: str) -> EvaluationDataset:
        for dataset in self.datasets:
            if dataset.dataset_id == dataset_id:
                return dataset
        raise KeyError(dataset_id)
