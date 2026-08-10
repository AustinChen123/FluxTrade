"""Four-run parity over the real backtest and live-like pipelines."""

from __future__ import annotations

import hashlib
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
    native_matcher_sha256,
    reviewed_delivery_checkout_available,
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


_SIGNAL_FIELDS = (
    "strategy_id",
    "product_id",
    "timeframe",
    "timestamp_ms",
    "signal_type",
    "value",
    "quantity",
    "price",
    "stop_loss",
    "take_profit",
    "trailing_distance",
    "metadata_json",
)
_EXPECTED_SIGNALS = (
    (
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        "1m",
        1_800_000_000_000,
        "LONG",
        None,
        Decimal("1"),
        None,
        None,
        None,
        None,
        '["map",[["client_order_id",["string","d0b4_four_run-market-long_0-6359818500763019921"]]]]',
    ),
    (
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        "1m",
        1_800_000_060_000,
        "NO_SIGNAL",
        Decimal("101"),
        None,
        None,
        None,
        None,
        None,
        '["map",[["client_order_id",["string","d0b4_four_run-market-no_signal_0-14594312868061664539"]]]]',
    ),
    (
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        "1m",
        1_800_000_120_000,
        "EXIT_LONG",
        None,
        Decimal("1"),
        None,
        None,
        None,
        None,
        '["map",[["client_order_id",["string","d0b4_four_run-market-exit_long_0-4588675349013221256"]]]]',
    ),
    (
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        "1m",
        1_800_000_180_000,
        "NO_SIGNAL",
        Decimal("103"),
        None,
        None,
        None,
        None,
        None,
        '["map",[["client_order_id",["string","d0b4_four_run-market-no_signal_0-9612643641772838732"]]]]',
    ),
)

_ORDER_FIELDS = (
    "logical_order_id",
    "parent_logical_order_id",
    "linked_logical_order_id",
    "strategy_id",
    "product_id",
    "timestamp_ms",
    "phase",
    "status",
    "order_type",
    "side",
    "quantity",
    "filled_quantity",
    "price",
    "trigger_price",
    "trailing_distance",
)
_EXPECTED_ORDERS = (
    (
        "order-000000",
        None,
        None,
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        1_800_000_000_000,
        "submitted",
        "PLACED",
        "market",
        "buy",
        Decimal("1"),
        Decimal("0"),
        None,
        None,
        None,
    ),
    (
        "order-000000",
        None,
        None,
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        1_800_000_060_000,
        "filled",
        "FILLED",
        "market",
        "buy",
        Decimal("1"),
        Decimal("1"),
        Decimal("101"),
        None,
        None,
    ),
    (
        "order-000001",
        None,
        None,
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        1_800_000_120_000,
        "submitted",
        "PLACED",
        "market",
        "sell",
        Decimal("1"),
        Decimal("0"),
        None,
        None,
        None,
    ),
    (
        "order-000001",
        None,
        None,
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        1_800_000_180_000,
        "filled",
        "FILLED",
        "market",
        "sell",
        Decimal("1"),
        Decimal("1"),
        Decimal("103"),
        None,
        None,
    ),
)

_FILL_FIELDS = (
    "logical_order_id",
    "strategy_id",
    "product_id",
    "timestamp_ms",
    "fill_type",
    "side",
    "price",
    "quantity",
    "fee",
)
_EXPECTED_FILLS = (
    (
        "order-000000",
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        1_800_000_060_000,
        "MARKET",
        "buy",
        Decimal("101"),
        Decimal("1"),
        Decimal("0.101"),
    ),
    (
        "order-000001",
        "d0b4_four_run",
        "BINANCE:BTCUSDT-PERP",
        1_800_000_180_000,
        "MARKET",
        "sell",
        Decimal("103"),
        Decimal("1"),
        Decimal("0.103"),
    ),
)

_JOURNAL_FIELDS = (
    "strategy_id",
    "timestamp_ms",
    "tag",
    "logical_trade_id",
    "data_json",
)
_EXPECTED_JOURNAL = (
    (
        "d0b4_four_run",
        1_800_000_000_000,
        "entry",
        "order-000000",
        '["map",[["order_id",["string","order-000000"]],["order_type",["string","market"]],["price",["string","market"]],["quantity",["string","1"]],["side",["string","buy"]],["stop_loss",["null"]],["take_profit",["null"]],["trailing_distance",["null"]]]]',
    ),
    (
        "d0b4_four_run",
        1_800_000_060_000,
        "fill",
        "order-000000",
        '["map",[["fee",["string","0.101"]],["fill_type",["string","MARKET"]],["order_id",["string","order-000000"]],["price",["string","101"]],["quantity",["string","1"]],["side",["string","buy"]]]]',
    ),
    (
        "d0b4_four_run",
        1_800_000_120_000,
        "entry",
        "order-000001",
        '["map",[["order_id",["string","order-000001"]],["order_type",["string","market"]],["price",["string","market"]],["quantity",["string","1"]],["side",["string","sell"]],["stop_loss",["null"]],["take_profit",["null"]],["trailing_distance",["null"]]]]',
    ),
    (
        "d0b4_four_run",
        1_800_000_180_000,
        "fill",
        "order-000001",
        '["map",[["fee",["string","0.103"]],["fill_type",["string","MARKET"]],["order_id",["string","order-000001"]],["price",["string","103"]],["quantity",["string","1"]],["side",["string","sell"]]]]',
    ),
)


def _assert_exact_outcome(outcome: TradingOutcome) -> None:
    assert type(outcome) is TradingOutcome
    assert (
        tuple(
            tuple(getattr(signal, field) for field in _SIGNAL_FIELDS)
            for signal in outcome.signals
        )
        == _EXPECTED_SIGNALS
    )
    assert (
        tuple(
            tuple(getattr(order, field) for field in _ORDER_FIELDS)
            for order in outcome.order_observations
        )
        == _EXPECTED_ORDERS
    )
    assert (
        tuple(
            tuple(getattr(fill, field) for field in _FILL_FIELDS)
            for fill in outcome.fills
        )
        == _EXPECTED_FILLS
    )
    assert (
        tuple(
            tuple(getattr(row, field) for field in _JOURNAL_FIELDS)
            for row in outcome.journal
        )
        == _EXPECTED_JOURNAL
    )
    assert (
        outcome.financial.fees,
        outcome.financial.realized_pnl,
        outcome.financial.unrealized_pnl,
        outcome.financial.equity,
    ) == (
        Decimal("0.204"),
        Decimal("1.796"),
        Decimal("0"),
        Decimal("10001.796"),
    )
    assert not outcome.endpoint_state.positions
    assert not outcome.endpoint_state.working_orders
    assert outcome.endpoint_state.final_mark == Decimal("103")
    assert outcome.endpoint_state.end_timestamp == TIMESTAMPS[-1]
    assert outcome.endpoint_state.halted_early is False
    assert (
        outcome.sha256()
        == "43ea059c075a8986bada3ac884a470ab26b90ee23c90634a8f4cd1419ebbca6a"
    )


@pytest.mark.skipif(
    not reviewed_delivery_checkout_available(),
    reason="runtime attestation belongs to the exact reviewed D0B4B delivery",
)
def test_parent_manifest_and_loaded_native_binary_are_exact() -> None:
    assert verify_reviewed_product_runtime() == PARENT_MANIFEST_SHA256
    selected = selected_native_matcher_module()
    assert selected.__file__ is not None
    expected = hashlib.sha256(Path(selected.__file__).read_bytes()).hexdigest()
    assert native_matcher_sha256() == expected


def test_candidate_parent_is_read_from_the_raw_commit_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_commit = (
        b"tree cfae260b0a67de1ef7c0f11cb98c664cab9bf3e4\n"
        b"parent 6494c2aa3d436f57c4c5466320d5e7a25c4b8a0a\n"
        b"author Test <test@example.com> 0 +0000\n\nsubject\n"
    )
    monkeypatch.setattr(fixture_module, "_git", lambda *_args: raw_commit)
    assert fixture_module._commit_tree_and_parents("HEAD") == (
        "cfae260b0a67de1ef7c0f11cb98c664cab9bf3e4",
        ["6494c2aa3d436f57c4c5466320d5e7a25c4b8a0a"],
    )


def test_reviewed_candidate_chain_allows_only_one_native_artifact_fix() -> None:
    assert (
        fixture_module._reviewed_candidate_parent(
            "98b453ee5ae21e08bd46fcbef9b6984370cdf8ef"
        )
        == "6494c2aa3d436f57c4c5466320d5e7a25c4b8a0a"
    )
    assert (
        fixture_module._reviewed_candidate_parent(
            "d5eb57d2b12a2bd7a0928f7650e8ea0c121c57f4"
        )
        == "98b453ee5ae21e08bd46fcbef9b6984370cdf8ef"
    )
    assert (
        fixture_module._reviewed_candidate_parent(
            "f5bb2dd628a2501cd25d435a2caaedda85d42feb"
        )
        == "d5eb57d2b12a2bd7a0928f7650e8ea0c121c57f4"
    )
    assert (
        fixture_module._reviewed_candidate_parent(
            "5056c8890e9ca818e0a192e5117b195e78bfd380"
        )
        == "f5bb2dd628a2501cd25d435a2caaedda85d42feb"
    )
    assert (
        fixture_module._reviewed_candidate_parent(
            "1111111111111111111111111111111111111111"
        )
        == "5056c8890e9ca818e0a192e5117b195e78bfd380"
    )


@pytest.mark.parametrize(
    ("head_sha", "expected"),
    (
        ("02d67503b38ae035d11e0d82afb5e8c73ac80cf2", True),
        ("76984432bd9fac138c6c50e307a445c1dfa75423", False),
        ("5056c8890e9ca818e0a192e5117b195e78bfd380", False),
        ("02d67503b38ae035d11e0d82afb5e8c73ac80cf3", False),
    ),
)
def test_reviewed_delivery_runtime_gate_is_exact(
    head_sha: str,
    expected: bool,
) -> None:
    assert reviewed_delivery_checkout_available(head_sha) is expected


def test_untracked_product_paths_allow_only_the_selected_ci_native_binary() -> None:
    selected = "python-strategy/src/fluxtrade_core.so"

    fixture_module._validate_untracked_product_paths((selected,), selected)

    for untracked, allowed in (
        (("python-strategy/src/unrelated.py",), selected),
        ((selected, "python-strategy/src/unrelated.py"), selected),
        ((selected,), None),
    ):
        with pytest.raises(ValueError, match="untracked product runtime path"):
            fixture_module._validate_untracked_product_paths(untracked, allowed)


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
    manifest = fixture_module._manifest_bytes(fixture_module.NATIVE_ARTIFACT_FIX_SHA)
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
    not reviewed_delivery_checkout_available(),
    reason="committed matrix belongs to the exact reviewed D0B4B delivery",
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
