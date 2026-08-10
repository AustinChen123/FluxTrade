"""Four-run parity over the real backtest and live-like pipelines."""

from __future__ import annotations

import hashlib
import subprocess
from decimal import Decimal
from pathlib import Path

import pytest

import integration.four_run_parity_fixture as fixture_module
from integration.four_run_parity_fixture import (
    INITIAL_BALANCE,
    PARENT_MANIFEST_SHA256,
    TIMESTAMPS,
    build_four_run_matrix,
    collect_backtest_with_final_price,
    collect_real_mode_outcomes,
    committed_candidate_available,
    native_matcher_sha256,
    selected_native_matcher_module,
    semantic_identity_sha256,
    validate_frozen_product_manifest,
    verify_reviewed_product_runtime,
)
from src.core.models import Candlestick
from src.validation.trading_outcome import TradingOutcome
from src.validation.trading_parity_matrix import compare_four_run_parity

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]


def _assert_exact_outcome(outcome: TradingOutcome) -> None:
    assert type(outcome) is TradingOutcome
    assert tuple(signal.signal_type for signal in outcome.signals) == (
        "LONG",
        "NO_SIGNAL",
        "EXIT_LONG",
        "NO_SIGNAL",
    )
    assert tuple(
        (fill.side, fill.price, fill.quantity, fill.fee) for fill in outcome.fills
    ) == (
        ("buy", Decimal("101"), Decimal("1"), Decimal("0.101")),
        ("sell", Decimal("103"), Decimal("1"), Decimal("0.103")),
    )
    assert len(outcome.order_observations) == 4
    assert len(outcome.journal) == 4
    assert outcome.financial.fees == Decimal("0.204")
    assert outcome.financial.realized_pnl == Decimal("1.796")
    assert outcome.financial.unrealized_pnl == Decimal("0")
    assert outcome.financial.equity == Decimal("10001.796")
    assert not outcome.endpoint_state.positions
    assert not outcome.endpoint_state.working_orders
    assert outcome.endpoint_state.final_mark == Decimal("103")
    assert outcome.endpoint_state.end_timestamp == TIMESTAMPS[-1]
    assert outcome.endpoint_state.halted_early is False


def test_parent_manifest_and_loaded_native_binary_are_exact() -> None:
    assert verify_reviewed_product_runtime() == PARENT_MANIFEST_SHA256
    selected = selected_native_matcher_module()
    assert selected.__file__ is not None
    expected = hashlib.sha256(Path(selected.__file__).read_bytes()).hexdigest()
    assert native_matcher_sha256() == expected


def test_real_collectors_produce_the_same_exact_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collected = collect_real_mode_outcomes(tmp_path, monkeypatch)
    outcomes = tuple(cell.outcome for cell in collected.cells)
    for cell, outcome in zip(collected.cells, outcomes, strict=True):
        _assert_exact_outcome(outcome)
        canonical = outcome.canonical_bytes()
        for raw_id in cell.evidence.raw_ids:
            assert raw_id.encode() not in canonical
    assert all(
        outcomes[0].first_difference(outcome) is None for outcome in outcomes[1:]
    )
    assert len({outcome.sha256() for outcome in outcomes}) == 1

    for cell in collected.cells:
        evidence = cell.evidence
        assert evidence.observed_batches == 4
        assert evidence.persisted_fills == 2
        assert len(evidence.raw_ids) >= 4
        if evidence.mode == "live_like":
            assert evidence.owner == "DataConsumer+LiveLikeOutcomeCapture"
            assert evidence.persisted_orders == 2
            assert evidence.processed_deliveries == 4
            assert evidence.acked == tuple(f"{timestamp}-0" for timestamp in TIMESTAMPS)
            assert evidence.pending == 0
            assert evidence.cleanup_calls == 1
        else:
            assert evidence.owner == "BacktestRunner"
            assert evidence.persisted_orders == 2
            assert evidence.processed_deliveries == 4
            assert evidence.acked == ()


def test_product_manifest_rejects_path_or_blob_drift() -> None:
    manifest = subprocess.run(
        (
            "git",
            "ls-tree",
            "-r",
            "HEAD",
            "--",
            "python-strategy/src",
            "python-strategy/pyproject.toml",
            "python-strategy/uv.lock",
            "rust-data-service/src",
            "rust-data-service/Cargo.toml",
            "rust-data-service/Cargo.lock",
        ),
        cwd=Path(__file__).resolve().parents[3],
        check=True,
        capture_output=True,
    ).stdout
    assert validate_frozen_product_manifest(manifest) == PARENT_MANIFEST_SHA256
    lines = manifest.splitlines(keepends=True)
    with pytest.raises(ValueError, match="differs from reviewed parent"):
        validate_frozen_product_manifest(b"".join(lines[1:]))
    mutated = bytearray(manifest)
    mutated[0] = ord("9") if mutated[0] != ord("9") else ord("8")
    with pytest.raises(ValueError, match="differs from reviewed parent"):
        validate_frozen_product_manifest(bytes(mutated))


def test_semantic_identity_tracks_actual_fee_and_candle_volume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline_input, baseline_configuration = semantic_identity_sha256()

    monkeypatch.setattr(fixture_module, "MAKER_FEE", Decimal("0.5"))
    fee_input, fee_configuration = semantic_identity_sha256()
    assert fee_input == baseline_input
    assert fee_configuration != baseline_configuration

    monkeypatch.setattr(fixture_module, "MAKER_FEE", Decimal("0"))
    original_candles = fixture_module._candles

    def volume_changed(final_price: str = "103") -> tuple[Candlestick, ...]:
        candles = original_candles(final_price)
        return (
            candles[0].model_copy(update={"volume": Decimal("2")}),
            candles[1],
            candles[2],
            candles[3],
        )

    monkeypatch.setattr(fixture_module, "_candles", volume_changed)
    volume_input, volume_configuration = semantic_identity_sha256()
    assert volume_input != baseline_input
    assert volume_configuration == baseline_configuration


def test_real_money_path_change_is_detected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = collect_backtest_with_final_price(
        tmp_path,
        monkeypatch,
        label="semantic_baseline",
        final_price="103",
        raw_ids_index=0,
    )
    changed = collect_backtest_with_final_price(
        tmp_path,
        monkeypatch,
        label="semantic_changed",
        final_price="104",
        raw_ids_index=1,
    )
    difference = baseline.outcome.first_difference(changed.outcome)
    assert difference is not None
    assert difference.path in {
        "$.signals[3].value",
        "$.fills[1].price",
        "$.endpoint_state.final_mark",
        "$.financial.realized_pnl",
        "$.financial.equity",
        "$.journal[3].data_json.price",
    }


@pytest.mark.skipif(
    not committed_candidate_available(),
    reason="exact candidate SHA/tree exist only after the reviewed commit",
)
def test_committed_real_four_run_matrix_is_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matrix = build_four_run_matrix(tmp_path, monkeypatch)

    assert tuple(run.role for run in matrix.runs) == ("BL", "BB", "CL", "CB")
    assert tuple(run.source_version for run in matrix.runs) == (
        "baseline",
        "baseline",
        "candidate",
        "candidate",
    )
    assert tuple(run.mode for run in matrix.runs) == (
        "live_like",
        "backtest",
        "live_like",
        "backtest",
    )
    assert matrix.product_manifest_sha256 == PARENT_MANIFEST_SHA256
    assert all(run.input_sha256 == matrix.runs[0].input_sha256 for run in matrix.runs)
    assert all(
        run.configuration_sha256 == matrix.runs[0].configuration_sha256
        for run in matrix.runs
    )
    assert all(
        run.loaded_artifact_sha256 == matrix.runs[0].loaded_artifact_sha256
        for run in matrix.runs
    )
    assert all(
        run.native_matcher_sha256 == matrix.runs[0].native_matcher_sha256
        for run in matrix.runs
    )
    assert INITIAL_BALANCE == Decimal("10000")

    assert matrix.report.comparisons == (
        ("BL_BB", "exact_match"),
        ("CL_CB", "exact_match"),
        ("BL_CL", "exact_match"),
        ("BB_CB", "exact_match"),
    )
    assert tuple(role for role, _digest in matrix.report.run_digests) == (
        "BL",
        "BB",
        "CL",
        "CB",
    )
    repeated = compare_four_run_parity(matrix.runs)
    assert matrix.report.canonical_bytes() == repeated.canonical_bytes()
    assert matrix.report.sha256() == repeated.sha256()
