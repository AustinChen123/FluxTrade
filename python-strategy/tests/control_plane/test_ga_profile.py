from __future__ import annotations

import ast
import copy
import json
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.control_plane.evaluation_data import DatabaseEvaluationDataSourceProvider
from src.control_plane.evolution import initial_population
from src.control_plane.ga_profile import (
    CompiledGaProfileRequest,
    compile_golden_cross_request,
    get_golden_cross_profile,
)
from src.control_plane.models import ParameterSearchJobRequest
from src.control_plane.parameter_evaluation import GoldenCrossResearchParameterEvaluator
from src.core.data_sources.research_database import (
    ResearchDatabaseDataSource,
    ResearchDatasetMetadata,
)
from src.core.orm_models import (
    Base,
    Exchange,
    Product,
    ResearchCandlestick,
    ResearchDataset,
)
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from test_control_plane import PRODUCT_ID, TIMEFRAME, _write_research_candles
import src.strategies.golden_cross as golden_cross_module


def _metadata(
    *,
    dataset_id: str = "sealed-profile-fixture",
    product_id: str = PRODUCT_ID,
    timeframe: str = TIMEFRAME,
    start_time: int = 1_704_067_200_000,
    end_time: int = 1_704_067_800_000,
    checksum: str = "a" * 64,
) -> ResearchDatasetMetadata:
    return ResearchDatasetMetadata(
        id=dataset_id,
        product_id=product_id,
        timeframe=timeframe,
        checksum_sha256=checksum,
        start_time=start_time,
        end_time=end_time,
    )


def _payload(metadata: ResearchDatasetMetadata) -> dict[str, object]:
    profile = get_golden_cross_profile()
    return {
        "parameter_search_profile_id": profile["parameter_search_profile_id"],
        "strategy_subject": profile["strategy_subject"],
        "fitness_profile_id": profile["fitness_profile_id"],
        "cost_profile_id": profile["cost_profile_id"],
        "profile_revision": profile["profile_revision"],
        "strategy_version": profile["strategy_version"],
        "dataset_id": metadata.id,
        "start_time": metadata.start_time,
        "end_time": metadata.end_time,
        "initial_balance": "10000.123456789012345678901234",
        "fees": {"maker": "0.000001", "taker": "0.000123"},
        "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        "parameters": {
            "short_window": {"min": 1, "max": 2, "step": 1},
            "long_window": {"min": 3, "max": 4, "step": 1},
            "quantity": "0.01",
        },
        "population_size": 2,
        "max_generations": 3,
        "seed": 17,
    }


def test_profile_is_detached_and_pins_imported_builtin_source():
    profile = get_golden_cross_profile()
    source_path = Path(golden_cross_module.__file__)
    assert profile["strategy_version"] == sha256(source_path.read_bytes()).hexdigest()
    assert len(profile["profile_revision"]) == 64
    assert set(profile["accepted_fields"]) == {
        "parameter_search_profile_id",
        "strategy_subject",
        "fitness_profile_id",
        "cost_profile_id",
        "profile_revision",
        "strategy_version",
        "dataset_id",
        "start_time",
        "end_time",
        "initial_balance",
        "fees",
        "instrument",
        "parameters",
        "population_size",
        "max_generations",
        "seed",
    }
    assert set(profile["compiled_fields"]) == {
        "kind",
        "strategy_type",
        "strategy_id",
        "objective",
        "market_data",
        "write_reports",
        "capital_allocation",
        "evaluation_set",
        "fitness",
    }
    fields = profile["accepted_fields"]
    assert fields["parameters"]["fields"]["short_window"]["constraint"] == (
        "min <= max; step > 0"
    )
    assert fields["parameters"]["quantity_constraint"] == (
        "must align to instrument.quantity_step when configured"
    )
    assert fields["population_size"]["constraint"] == (
        "must not exceed parameter Cartesian cardinality"
    )
    instrument_fields = fields["instrument"]["fields"]
    assert instrument_fields["quantity_step"]["constraint"] == (
        "required for dated_future products"
    )
    assert instrument_fields["price_tick"]["constraint"] == (
        "required for dated_future products"
    )
    assert instrument_fields["capital_per_contract"]["constraint"] == (
        "positive and required only when capital_model is per_contract"
    )
    assert "evolution_defaults" in profile
    profile["accepted_fields"]["parameters"]["fields"]["quantity"]["default"] = "9"
    again = get_golden_cross_profile()
    assert (
        again["accepted_fields"]["parameters"]["fields"]["quantity"]["default"]
        == "0.01"
    )


def test_compiler_returns_bound_existing_request_and_stable_digest():
    metadata = _metadata()
    payload = _payload(metadata)
    compiled = compile_golden_cross_request(payload, metadata)
    repeated = compile_golden_cross_request(copy.deepcopy(payload), metadata)

    assert isinstance(compiled, ParameterSearchJobRequest)
    assert isinstance(compiled, CompiledGaProfileRequest)
    assert compiled.model_dump()["ga_binding"] == repeated.model_dump()["ga_binding"]
    assert compiled.ga_binding.dataset_checksum == metadata.checksum_sha256
    assert compiled.product_id == metadata.product_id
    assert compiled.timeframe == metadata.timeframe
    assert compiled.start_time == metadata.start_time
    assert compiled.end_time == metadata.end_time
    assert compiled.strategy_id == compiled.strategy_type == "golden_cross"
    assert compiled.objective == "maximize_score"
    assert compiled.market_data is not None
    assert compiled.market_data.dataset_id == metadata.id
    assert compiled.backtest is not None
    assert compiled.backtest.candles_csv_path is None
    assert compiled.backtest.write_reports is False
    initial_balance = payload["initial_balance"]
    assert isinstance(initial_balance, str)
    assert compiled.backtest.initial_balance == Decimal(initial_balance)
    assert compiled.backtest.maker_fee == Decimal("0.000001")
    assert compiled.backtest.taker_fee == Decimal("0.000123")
    assert compiled.evolution is not None and compiled.evolution.epoch_id is None
    assert compiled.evolution.tournament_size == 2
    assert compiled.evolution.elite_count == 1
    assert compiled.evolution.crossover_probability == Decimal("0.9")
    assert compiled.evolution.mutation_probability == Decimal("0.1")
    assert compiled.evolution.mutation_sigma_steps == Decimal("1")
    assert compiled.search_space is not None
    assert compiled.evolution is not None
    assert compiled.seed is not None
    candidates = initial_population(
        compiled.search_space, compiled.evolution, seed=compiled.seed
    )
    assert len(candidates) == compiled.evolution.population_size == 2
    assert all(
        isinstance(candidate.param_pack["quantity"], Decimal)
        for candidate in candidates
    )
    with pytest.raises(ValueError):
        compiled.ga_binding.input_digest = "f" * 64

    legacy_data = compiled.model_dump()
    legacy_data.pop("ga_binding")
    legacy = ParameterSearchJobRequest.model_validate(legacy_data)
    assert "ga_binding" not in legacy.model_dump()
    assert legacy.model_dump() == legacy_data


@pytest.mark.parametrize(
    "change",
    [
        lambda payload: payload.update(
            initial_balance="10000.123456789012345678901235"
        ),
        lambda payload: payload["fees"].update(taker="0.000124"),
        lambda payload: payload["parameters"]["short_window"].update(max=1),
        lambda payload: payload.update(seed=18),
    ],
    ids=["balance", "fee", "range", "seed"],
)
def test_every_changed_compiled_input_changes_digest(change):
    metadata = _metadata()
    original = compile_golden_cross_request(_payload(metadata), metadata)
    changed_payload = _payload(metadata)
    change(changed_payload)
    changed = compile_golden_cross_request(changed_payload, metadata)
    assert changed.ga_binding.input_digest != original.ga_binding.input_digest


def test_dataset_checksum_changes_digest_and_compiler_derives_identity():
    metadata = _metadata()
    first = compile_golden_cross_request(_payload(metadata), metadata)
    changed_metadata = replace(metadata, checksum_sha256="b" * 64)
    changed = compile_golden_cross_request(_payload(changed_metadata), changed_metadata)
    assert changed.ga_binding.input_digest != first.ga_binding.input_digest
    assert changed.ga_binding.dataset_checksum == "b" * 64


@pytest.mark.parametrize(
    "field",
    [
        "parameter_search_profile_id",
        "strategy_subject",
        "fitness_profile_id",
        "cost_profile_id",
        "profile_revision",
        "strategy_version",
        "dataset_id",
        "start_time",
        "end_time",
        "initial_balance",
        "fees",
        "instrument",
    ],
)
def test_every_declared_required_input_is_required(field):
    metadata = _metadata()
    payload = _payload(metadata)
    del payload[field]
    with pytest.raises(ValueError, match="missing required fields"):
        compile_golden_cross_request(payload, metadata)


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ("no-optional-fields", (32, 10, 0, (5, 50, 5), (60, 200, 10), "0.01")),
        ("short-default", (2, 3, 17, (5, 50, 5), (60, 70, 10), "0.01")),
        ("long-default", (2, 3, 17, (1, 2, 1), (60, 200, 10), "0.01")),
        ("quantity-default", (2, 3, 17, (1, 2, 1), (3, 4, 1), "0.01")),
        ("population-default", (32, 3, 17, (5, 50, 5), (60, 200, 10), "0.01")),
        ("generations-default", (2, 10, 17, (1, 2, 1), (3, 4, 1), "0.01")),
        ("seed-default", (2, 3, 0, (1, 2, 1), (3, 4, 1), "0.01")),
    ],
    ids=[
        "all-defaults",
        "short-range-omitted",
        "long-range-omitted",
        "quantity-omitted",
        "population-omitted",
        "generations-omitted",
        "seed-omitted",
    ],
)
def test_omitted_optional_values_use_independently_declared_defaults(change, expected):
    metadata = _metadata()
    payload = _payload(metadata)
    if change == "no-optional-fields":
        for key in ("parameters", "population_size", "max_generations", "seed"):
            payload.pop(key)
    else:
        if change == "short-default":
            payload["parameters"] = {"long_window": {"min": 60, "max": 70, "step": 10}}
        elif change == "long-default":
            payload["parameters"] = {"short_window": {"min": 1, "max": 2, "step": 1}}
        elif change == "quantity-default":
            payload["parameters"] = {
                "short_window": {"min": 1, "max": 2, "step": 1},
                "long_window": {"min": 3, "max": 4, "step": 1},
            }
        elif change == "population-default":
            payload.pop("population_size")
            payload.pop("parameters")
        elif change == "generations-default":
            payload.pop("max_generations")
        elif change == "seed-default":
            payload.pop("seed")
    compiled = compile_golden_cross_request(payload, metadata)
    assert compiled.evolution is not None and compiled.search_space is not None
    assert (
        compiled.evolution.population_size,
        compiled.evolution.max_generations,
        compiled.seed,
        (
            compiled.search_space.parameters["short_window"].min,
            compiled.search_space.parameters["short_window"].max,
            compiled.search_space.parameters["short_window"].step,
        ),
        (
            compiled.search_space.parameters["long_window"].min,
            compiled.search_space.parameters["long_window"].max,
            compiled.search_space.parameters["long_window"].step,
        ),
        str(compiled.search_space.parameters["quantity"].min),
    ) == expected


@pytest.mark.parametrize(
    ("name", "mutation"),
    [
        ("profile-id", lambda p: p.update(parameter_search_profile_id="other")),
        ("strategy-subject", lambda p: p.update(strategy_subject="other")),
        ("fitness-id", lambda p: p.update(fitness_profile_id="other")),
        ("cost-id", lambda p: p.update(cost_profile_id="other")),
    ],
)
def test_compiler_rejects_each_fixed_identity_variant(name, mutation):
    metadata = _metadata()
    payload = _payload(metadata)
    mutation(payload)
    with pytest.raises(ValueError, match="identity or revision"):
        compile_golden_cross_request(payload, metadata)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("min", True),
        ("min", 1.0),
        ("min", "1"),
        ("max", True),
        ("max", 2.0),
        ("max", "2"),
        ("step", False),
        ("step", 1.0),
        ("step", "1"),
        ("min", 0),
        ("max", 10_001),
        ("step", 0),
    ],
)
def test_ranges_reject_undeclared_types_and_domain_values(field, value):
    metadata = _metadata()
    payload = _payload(metadata)
    short_range = cast(dict[str, object], payload["parameters"])["short_window"]
    cast(dict[str, object], short_range)[field] = value
    with pytest.raises((ValueError, TypeError)):
        compile_golden_cross_request(payload, metadata)


@pytest.mark.parametrize("field", ["min", "max", "step"])
def test_integer_range_requires_every_declared_member(field):
    metadata = _metadata()
    payload = _payload(metadata)
    short_range = cast(dict[str, object], payload["parameters"])["short_window"]
    del cast(dict[str, object], short_range)[field]
    with pytest.raises(ValueError, match="requires min, max and step"):
        compile_golden_cross_request(payload, metadata)


def test_integer_range_declared_endpoints_are_accepted():
    metadata = _metadata()
    payload = _payload(metadata)
    payload["parameters"] = {
        "short_window": {"min": 1, "max": 1, "step": 1},
        "long_window": {"min": 2, "max": 10_000, "step": 9_998},
    }
    compiled = compile_golden_cross_request(payload, metadata)
    assert compiled.search_space is not None
    assert compiled.search_space.parameters["short_window"].min == 1
    assert compiled.search_space.parameters["long_window"].max == 10_000


@pytest.mark.parametrize(
    ("start_time", "end_time", "valid"),
    [
        (0, 1, False),
        (1_704_067_199_999, 1_704_067_200_000, False),
        (1_704_067_800_001, 1_704_067_800_001, False),
        (1_704_067_800_001, 1_704_067_800_000, False),
        (1_704_067_200_000, 1_704_067_800_000, True),
    ],
)
def test_window_coverage_endpoints_and_order(start_time, end_time, valid):
    metadata = _metadata()
    payload = _payload(metadata)
    payload.update(start_time=start_time, end_time=end_time)
    if valid:
        assert compile_golden_cross_request(payload, metadata).end_time == end_time
    else:
        with pytest.raises(ValueError):
            compile_golden_cross_request(payload, metadata)


def test_window_accepts_utc_domain_extremes_from_authoritative_metadata():
    maximum = 253_402_300_799_999
    metadata = _metadata(start_time=0, end_time=maximum)
    payload = _payload(metadata)
    compiled = compile_golden_cross_request(payload, metadata)
    assert (compiled.start_time, compiled.end_time) == (0, maximum)


def test_declared_integer_and_money_endpoints_compile_without_float_coercion():
    metadata = _metadata()
    payload = _payload(metadata)
    payload["initial_balance"] = Decimal("10000.123456789012345678901234")
    cast(dict[str, object], payload["fees"]).update(
        maker=Decimal("0.000001"), taker=Decimal("0")
    )
    payload.update(population_size=256, max_generations=100, seed=2_147_483_647)
    payload["parameters"] = {
        "short_window": {"min": 1, "max": 100, "step": 1},
        "long_window": {"min": 101, "max": 103, "step": 1},
    }
    compiled = compile_golden_cross_request(payload, metadata)
    assert compiled.backtest is not None
    assert compiled.backtest.initial_balance == Decimal(
        "10000.123456789012345678901234"
    )
    assert compiled.backtest.maker_fee == Decimal("0.000001")
    assert compiled.backtest.taker_fee == Decimal("0")
    assert compiled.evolution is not None
    assert compiled.evolution.population_size == 256
    assert compiled.evolution.max_generations == 100
    assert compiled.seed == 2_147_483_647


def test_dated_future_and_capital_instrument_rules_are_declared_and_enforced():
    future = _metadata(product_id="CME:MNQ-202612")
    future_payload = _payload(future)
    future_payload["instrument"] = {
        "quantity_step": "0.01",
        "price_tick": "0.25",
    }
    assert (
        compile_golden_cross_request(future_payload, future).product_id
        == future.product_id
    )

    for field in ("quantity_step", "price_tick"):
        invalid = copy.deepcopy(future_payload)
        cast(dict[str, object], invalid["instrument"])[field] = None
        with pytest.raises(ValueError):
            compile_golden_cross_request(invalid, future)

    metadata = _metadata()
    valid_capital = _payload(metadata)
    valid_capital["instrument"] = {
        "capital_model": "per_contract",
        "capital_per_contract": "2500",
    }
    compile_golden_cross_request(valid_capital, metadata)
    for instrument in (
        {"capital_model": "per_contract"},
        {"capital_model": "per_contract", "capital_per_contract": "0"},
        {"capital_model": "notional", "capital_per_contract": "2500"},
        {"fee_model": "unknown"},
        {"capital_model": "unknown"},
    ):
        invalid = _payload(metadata)
        invalid["instrument"] = instrument
        with pytest.raises((ValueError, TypeError)):
            compile_golden_cross_request(invalid, metadata)


def test_input_digest_binds_metadata_window_and_instrument_constraints():
    metadata = _metadata()
    baseline = compile_golden_cross_request(_payload(metadata), metadata)
    changed_inputs = []

    for changed_metadata in (
        replace(metadata, id="another-sealed-dataset"),
        replace(metadata, product_id="BYBIT:ETHUSDT-PERP"),
        replace(metadata, timeframe="5m"),
    ):
        changed_inputs.append(
            compile_golden_cross_request(_payload(changed_metadata), changed_metadata)
        )

    changed_window = _payload(metadata)
    changed_window["start_time"] = metadata.start_time + 1
    changed_inputs.append(compile_golden_cross_request(changed_window, metadata))

    changed_instrument = _payload(metadata)
    cast(dict[str, object], changed_instrument["instrument"])["quantity_step"] = (
        "0.0001"
    )
    changed_inputs.append(compile_golden_cross_request(changed_instrument, metadata))
    assert all(
        item.ga_binding.input_digest != baseline.ga_binding.input_digest
        for item in changed_inputs
    )


def test_profile_revision_is_independently_canonicalized_and_epoch_is_excluded():
    profile = get_golden_cross_profile()
    revision_document = copy.deepcopy(profile)
    revision = revision_document.pop("profile_revision")
    canonical = json.dumps(
        revision_document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    assert sha256(canonical.encode("utf-8")).hexdigest() == revision

    metadata = _metadata()
    compiled = compile_golden_cross_request(_payload(metadata), metadata)
    assert compiled.evolution is not None
    with_epoch = compiled.model_copy(
        update={
            "evolution": compiled.evolution.model_copy(
                update={"epoch_id": "generated-1"}
            )
        }
    )
    from src.control_plane.ga_profile import _input_digest

    assert _input_digest(with_epoch) == compiled.ga_binding.input_digest


def test_builtin_strategy_source_read_failure_is_fixed_and_cache_is_restored(
    monkeypatch,
):
    import src.control_plane.ga_profile as ga_profile_module

    strategy_path = Path(golden_cross_module.__file__)
    original_read_bytes = Path.read_bytes
    ga_profile_module._profile_json.cache_clear()

    def fail_strategy_source(path: Path) -> bytes:
        if path == strategy_path:
            raise OSError("synthetic read failure")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", fail_strategy_source)
    try:
        with pytest.raises(
            RuntimeError, match="builtin GoldenCross source cannot be read"
        ):
            get_golden_cross_profile()
    finally:
        monkeypatch.undo()
        ga_profile_module._profile_json.cache_clear()
    assert len(get_golden_cross_profile()["strategy_version"]) == 64


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(extra="ignored-never"),
        lambda payload: payload.update(profile_revision="0" * 64),
        lambda payload: payload.update(strategy_version="0" * 64),
        lambda payload: payload.update(start_time=True),
        lambda payload: payload.update(start_time=float(1_704_067_200_000)),
        lambda payload: payload.update(end_time=1_704_067_900_000),
        lambda payload: payload.update(initial_balance="0"),
        lambda payload: payload.update(initial_balance="-1"),
        lambda payload: payload.update(initial_balance="Infinity"),
        lambda payload: payload.update(initial_balance=True),
        lambda payload: payload.update(initial_balance="NaN"),
        lambda payload: payload["fees"].update(maker=0.001),
        lambda payload: payload["fees"].update(taker="-0.1"),
        lambda payload: payload["fees"].update(taker="Infinity"),
        lambda payload: payload["instrument"].update(quantity_step=True),
        lambda payload: payload["instrument"].update(unknown="1"),
        lambda payload: payload["parameters"].update(extra=1),
        lambda payload: payload["parameters"]["short_window"].update(step=True),
        lambda payload: payload["parameters"]["long_window"].update(min=2),
        lambda payload: payload["parameters"].update(quantity=0.01),
        lambda payload: payload.update(parameters=None),
        lambda payload: payload.update(population_size=5),
        lambda payload: payload.update(max_generations=0),
        lambda payload: payload.update(seed=True),
    ],
    ids=[
        "unknown-field",
        "revision",
        "strategy-version",
        "bool-time",
        "float-time",
        "coverage",
        "zero-balance",
        "negative-balance",
        "infinite-balance",
        "bool-balance",
        "nonfinite-balance",
        "float-fee",
        "negative-fee",
        "infinite-fee",
        "bool-instrument-decimal",
        "unknown-instrument-field",
        "unknown-parameter",
        "bool-range-step",
        "short-long-dependency",
        "float-quantity",
        "explicit-null-parameters",
        "population-cardinality",
        "generation-bound",
        "bool-seed",
    ],
)
def test_compiler_rejects_unsupported_or_invalid_input(mutate):
    metadata = _metadata()
    payload = _payload(metadata)
    mutate(payload)
    with pytest.raises((ValueError, TypeError)):
        compile_golden_cross_request(payload, metadata)


def test_compiler_rejects_dataset_id_mismatch_and_off_step_quantity():
    metadata = _metadata()
    payload = _payload(metadata)
    payload["dataset_id"] = "another-dataset"
    with pytest.raises(ValueError, match="dataset_id"):
        compile_golden_cross_request(payload, metadata)

    payload = _payload(metadata)
    parameters = cast(dict[str, object], payload["parameters"])
    parameters["quantity"] = "0.0101"
    with pytest.raises(ValueError, match="quantity"):
        compile_golden_cross_request(payload, metadata)

    payload = _payload(metadata)
    instrument = cast(dict[str, object], payload["instrument"])
    instrument.update(quantity_step=None, price_tick=None)
    compiled = compile_golden_cross_request(payload, metadata)
    assert compiled.backtest is not None
    assert compiled.backtest.instrument is not None
    assert compiled.backtest.instrument.quantity_step is None
    assert compiled.backtest.instrument.price_tick is None


def test_profile_compiler_has_no_job_worker_database_or_runner_imports():
    import src.control_plane.ga_profile as ga_profile_module

    source = Path(ga_profile_module.__file__)
    tree = ast.parse(source.read_text())
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    forbidden = (
        "src.control_plane.app",
        "src.control_plane.jobs",
        "src.control_plane.main",
        "src.control_plane.parameter_search",
        "src.control_plane.evolution_persistence",
        "src.core.orm_models",
        "src.core.research_backtest_runner",
    )
    assert not any(
        imported == prefix or imported.startswith(prefix + ".")
        for imported in imports
        for prefix in forbidden
    )


def test_compiled_candidate_runs_native_sealed_evaluator_and_matches_canonical(
    tmp_path,
):
    candle_path = tmp_path / "sealed-candles.csv"
    _write_research_candles(candle_path)
    dataset_id = "ga-profile-native-v1"
    engine = create_engine(f"sqlite:///{tmp_path / 'ga-profile.db'}")
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
    ResearchDatasetImporter(session_factory=sessions).import_csv(
        candle_path,
        ResearchDatasetSpec(
            dataset_id=dataset_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            source="synthetic-test",
            revision="v1",
        ),
    )
    try:
        metadata = ResearchDatabaseDataSource(
            dataset_id, session_factory=sessions
        ).get_dataset_metadata()
        assert metadata is not None
        payload = _payload(metadata)
        compiled = compile_golden_cross_request(payload, metadata)
        assert compiled.search_space is not None and compiled.evolution is not None
        assert compiled.seed is not None
        candidate = initial_population(
            compiled.search_space, compiled.evolution, seed=compiled.seed
        )[0]
        canonical_request = ParameterSearchJobRequest.model_validate(
            {
                key: value
                for key, value in compiled.model_dump().items()
                if key != "ga_binding"
            }
        )
        evaluator = GoldenCrossResearchParameterEvaluator(
            data_source_provider=DatabaseEvaluationDataSourceProvider(
                dataset_id, session_factory=sessions
            )
        )
        compiled_result = evaluator.evaluate(compiled, candidate)
        canonical_result = evaluator.evaluate(canonical_request, candidate)
        assert compiled_result.score_total == canonical_result.score_total
        assert compiled_result.max_drawdown == canonical_result.max_drawdown
        assert compiled_result.metrics == canonical_result.metrics
    finally:
        engine.dispose()
