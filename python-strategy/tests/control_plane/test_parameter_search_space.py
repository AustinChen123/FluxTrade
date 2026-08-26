from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from src.control_plane import (
    InMemoryJobStore,
    ParameterEvaluationResult,
    ParameterSearchJobExecutor,
)
from src.control_plane.models import ParameterCandidate, ParameterSearchJobRequest
from src.control_plane.search_space import generate_parameter_candidates
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy, SystemEvent


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


class _ScoreFromWindowEvaluator:
    def __init__(self) -> None:
        self.param_packs: list[dict] = []

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        del request
        self.param_packs.append(candidate.param_pack)
        score = Decimal(str(candidate.param_pack["short_window"]))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            metrics={"score": str(score)},
        )


def _sqlite_gene_registry_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'parameter_search_space_gene_registry.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
        poolclass=NullPool,
    )
    for table in [
        Strategy.__table__,
        SystemEvent.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Strategy(id="golden_cross", name="Golden Cross"))
        session.commit()
    return session_factory


def test_parameter_search_generates_full_grid_candidates_in_order():
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1,
            "end_time": 2,
            "search_space": {
                "parameters": {
                    "short_window": {
                        "type": "integer",
                        "min": 5,
                        "max": 10,
                        "step": 5,
                    },
                    "long_window": {
                        "type": "integer",
                        "min": 15,
                        "max": 20,
                        "step": 5,
                    },
                    "quantity": {
                        "type": "decimal",
                        "min": "0.01",
                        "max": "0.01",
                        "step": "0.01",
                    },
                }
            },
            "candidate_sample_count": 4,
            "seed": 7,
        }
    )
    assert request.search_space is not None

    candidates = generate_parameter_candidates(
        request.search_space,
        sample_count=request.candidate_sample_count or 0,
        seed=request.seed,
    )

    assert [candidate.candidate_id for candidate in candidates] == [
        "generated_000001",
        "generated_000002",
        "generated_000003",
        "generated_000004",
    ]
    assert [candidate.param_pack for candidate in candidates] == [
        {"short_window": 5, "long_window": 15, "quantity": Decimal("0.01")},
        {"short_window": 5, "long_window": 20, "quantity": Decimal("0.01")},
        {"short_window": 10, "long_window": 15, "quantity": Decimal("0.01")},
        {"short_window": 10, "long_window": 20, "quantity": Decimal("0.01")},
    ]


def test_parameter_search_space_sampling_is_seeded_and_unique():
    payload = {
        "parameters": {
            "short_window": {"type": "integer", "min": 5, "max": 50, "step": 5},
            "mode": {"type": "categorical", "choices": ["fast", "safe"]},
        }
    }
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1,
            "end_time": 2,
            "search_space": payload,
            "candidate_sample_count": 5,
            "seed": 17,
        }
    )
    assert request.search_space is not None

    first = generate_parameter_candidates(
        request.search_space,
        sample_count=5,
        seed=request.seed,
    )
    second = generate_parameter_candidates(
        request.search_space,
        sample_count=5,
        seed=request.seed,
    )

    assert [candidate.param_pack for candidate in first] == [
        candidate.param_pack for candidate in second
    ]
    assert len({tuple(candidate.param_pack.items()) for candidate in first}) == 5


def test_parameter_search_request_requires_one_candidate_source():
    base_payload = {
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "15m",
        "start_time": 1,
        "end_time": 2,
    }

    with pytest.raises(ValidationError, match="provide exactly one"):
        ParameterSearchJobRequest.model_validate(base_payload)

    with pytest.raises(ValidationError, match="provide exactly one"):
        ParameterSearchJobRequest.model_validate(
            {
                **base_payload,
                "candidates": [
                    {"candidate_id": "manual", "param_pack": {"score": "1"}}
                ],
                "search_space": {
                    "parameters": {
                        "score": {"type": "integer", "min": 1, "max": 2, "step": 1}
                    }
                },
                "candidate_sample_count": 1,
            }
        )

    with pytest.raises(ValidationError, match="candidate_sample_count is required"):
        ParameterSearchJobRequest.model_validate(
            {
                **base_payload,
                "search_space": {
                    "parameters": {
                        "score": {"type": "integer", "min": 1, "max": 2, "step": 1}
                    }
                },
            }
        )


def test_parameter_search_executor_evaluates_generated_candidates():
    store = InMemoryJobStore()
    evaluator = _ScoreFromWindowEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator,
        store=store,
        run_inline=True,
    )
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1,
            "end_time": 2,
            "search_space": {
                "parameters": {
                    "short_window": {
                        "type": "integer",
                        "min": 5,
                        "max": 10,
                        "step": 5,
                    },
                    "long_window": {
                        "type": "integer",
                        "min": 20,
                        "max": 20,
                        "step": 5,
                    },
                }
            },
            "candidate_sample_count": 2,
        }
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["best_candidate"]["candidate_id"] == "generated_000002"
    assert job.result["best_candidate_param_pack"] == {
        "short_window": 10,
        "long_window": 20,
    }
    assert job.result["resolved_candidates"] == [
        {
            "candidate_id": "generated_000001",
            "param_pack": {"short_window": 5, "long_window": 20},
        },
        {
            "candidate_id": "generated_000002",
            "param_pack": {"short_window": 10, "long_window": 20},
        },
    ]
    assert evaluator.param_packs == [
        {"short_window": 5, "long_window": 20},
        {"short_window": 10, "long_window": 20},
    ]


def test_parameter_search_integer_dimension_rejects_boolean_bounds():
    payload = {
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "15m",
        "start_time": 1,
        "end_time": 2,
        "search_space": {
            "parameters": {
                "short_window": {
                    "type": "integer",
                    "min": True,
                    "max": 10,
                    "step": 1,
                }
            }
        },
        "candidate_sample_count": 1,
    }

    with pytest.raises(ValidationError, match="cannot be boolean"):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_records_generated_decimal_param_packs(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    executor = ParameterSearchJobExecutor(
        _ScoreFromWindowEvaluator(),
        store=InMemoryJobStore(),
        run_inline=True,
        db_session_factory=session_factory,
    )
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1_700_000_000_000,
            "end_time": 1_700_001_800_000,
            "search_space": {
                "parameters": {
                    "short_window": {
                        "type": "integer",
                        "min": 5,
                        "max": 5,
                        "step": 1,
                    },
                    "quantity": {
                        "type": "decimal",
                        "min": "0.01",
                        "max": "0.02",
                        "step": "0.01",
                    },
                }
            },
            "candidate_sample_count": 2,
        }
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["epoch_id"].startswith("epoch_")
    assert job.result["resolved_candidates"] == [
        {
            "candidate_id": "generated_000001",
            "param_pack": {"short_window": 5, "quantity": "0.01"},
        },
        {
            "candidate_id": "generated_000002",
            "param_pack": {"short_window": 5, "quantity": "0.02"},
        },
    ]

    with session_factory() as session:
        genes = (
            session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == job.result["epoch_id"])
            .order_by(GeneRecord.id)
            .all()
        )

    assert [gene.param_pack for gene in genes] == [
        {"short_window": 5, "quantity": "0.01"},
        {"short_window": 5, "quantity": "0.02"},
    ]
