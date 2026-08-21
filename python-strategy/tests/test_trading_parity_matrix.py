from dataclasses import replace
from decimal import Decimal
from collections.abc import Callable

import pytest

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
)
from src.core.models import OrderSide, PositionSide
from src.validation.trading_outcome import (
    FillObservation,
    FinancialOutcome,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)
from src.validation.trading_parity import TradingParityRun
from src.validation.trading_parity_matrix import (
    FourRunParityReport,
    InvalidParityInputIdentity,
    TradingParityMismatch,
    compare_four_run_parity,
)


ROLE_IDENTITY = {
    "BL": ("baseline", "live_like", "a" * 40, "b" * 40, "1" * 64, "2" * 64),
    "BB": ("baseline", "backtest", "a" * 40, "b" * 40, "3" * 64, "4" * 64),
    "CL": ("candidate", "live_like", "c" * 40, "d" * 40, "1" * 64, "2" * 64),
    "CB": ("candidate", "backtest", "c" * 40, "d" * 40, "3" * 64, "4" * 64),
}
COMPARISONS = (
    ("BL_BB", "exact_match"),
    ("CL_CB", "exact_match"),
    ("BL_CL", "exact_match"),
    ("BB_CB", "exact_match"),
)


def _outcome(
    *,
    signals: tuple[SignalObservation, ...] | None = None,
    orders: tuple[OrderObservation, ...] | None = None,
    fills: tuple[FillObservation, ...] | None = None,
    journal: tuple[JournalObservation, ...] | None = None,
    endpoint: ReplayEndpointState | None = None,
    financial: FinancialOutcome | None = None,
) -> TradingOutcome:
    return TradingOutcome.model_validate(
        {
            "signals": signals
            or (
                SignalObservation.model_validate(
                    {
                        "strategy_id": "s",
                        "product_id": "MNQ",
                        "timeframe": "5m",
                        "timestamp_ms": 1,
                        "signal_type": "LONG",
                        "value": Decimal("1"),
                        "quantity": None,
                        "price": None,
                        "stop_loss": None,
                        "take_profit": None,
                        "trailing_distance": None,
                        "metadata_json": {},
                    }
                ),
            ),
            "order_observations": orders
            or (
                OrderObservation(
                    logical_order_id="order-1",
                    parent_logical_order_id=None,
                    linked_logical_order_id=None,
                    strategy_id="s",
                    product_id="MNQ",
                    timestamp_ms=2,
                    phase="submitted",
                    status="NEW",
                    order_type="MARKET",
                    side="buy",
                    quantity=Decimal("1"),
                    filled_quantity=Decimal("0"),
                    price=None,
                    trigger_price=None,
                    trailing_distance=None,
                ),
            ),
            "fills": fills
            or (
                FillObservation(
                    logical_order_id="order-1",
                    strategy_id="s",
                    product_id="MNQ",
                    timestamp_ms=3,
                    fill_type="entry",
                    side="buy",
                    price=Decimal("100"),
                    quantity=Decimal("1"),
                    fee=Decimal("1"),
                ),
            ),
            "endpoint_state": endpoint
            or ReplayEndpointState(
                positions=(),
                working_orders=(),
                final_mark=Decimal("100"),
                end_timestamp=3,
                halted_early=False,
            ),
            "financial": financial
            or FinancialOutcome(
                fees=Decimal("1"),
                realized_pnl=Decimal("0"),
                unrealized_pnl=Decimal("2"),
                equity=Decimal("1002"),
            ),
            "journal": journal
            or (
                JournalObservation.model_validate(
                    {
                        "strategy_id": "s",
                        "timestamp_ms": 3,
                        "tag": "fill",
                        "logical_trade_id": "trade-1",
                        "data_json": {},
                    }
                ),
            ),
        }
    )


def _run(role: str, outcome: TradingOutcome | None = None) -> TradingParityRun:
    source, mode, revision, tree, runtime, runner = ROLE_IDENTITY[role]
    return TradingParityRun.model_validate(
        {
            "role": role,
            "source_version": source,
            "mode": mode,
            "revision_sha": revision,
            "tree_oid": tree,
            "runtime_source_sha256": runtime,
            "input_sha256": "5" * 64,
            "configuration_sha256": "6" * 64,
            "runner_sha256": runner,
            "loaded_artifact_sha256": "7" * 64,
            "native_matcher_sha256": "8" * 64,
            "outcome": outcome or _outcome(),
        }
    )


def _matrix(
    *, changed_role: str | None = None, outcome: TradingOutcome | None = None
) -> tuple[TradingParityRun, ...]:
    return tuple(
        _run(role, outcome if role == changed_role else None)
        for role in ("BL", "BB", "CL", "CB")
    )


def _replace_run(
    matrix: tuple[TradingParityRun, ...], role: str, **updates: object
) -> tuple[TradingParityRun, ...]:
    return tuple(
        run.model_copy(update=updates) if run.role == role else run for run in matrix
    )


def _assert_invalid(matrix: object, field: str) -> None:
    with pytest.raises(InvalidParityInputIdentity) as captured:
        compare_four_run_parity(matrix)
    assert captured.value.args == ("parity input identity is invalid",)
    assert captured.value.classification == "INVALID_INPUT_IDENTITY"
    assert captured.value.canonical_stop_action == "REPLAN"
    assert captured.value.field == field


def test_exact_matrix_is_role_ordered_and_canonical() -> None:
    matrix = _matrix()
    report = compare_four_run_parity((matrix[2], matrix[0], matrix[3], matrix[1]))

    assert type(report) is FourRunParityReport
    assert report.run_digests == tuple((run.role, run.sha256()) for run in matrix)
    assert report.comparisons == COMPARISONS
    assert report.canonical_bytes() == (
        b'["fluxtrade.four_run_parity_report.v1",['
        + b",".join(f'["{run.role}","{run.sha256()}"]'.encode() for run in matrix)
        + b'],[["BL_BB","exact_match"],["CL_CB","exact_match"],'
        b'["BL_CL","exact_match"],["BB_CB","exact_match"]]]'
    )
    assert report.sha256() == report.sha256()
    assert ROLE_IDENTITY["BL"][4:] != ROLE_IDENTITY["BB"][4:]


def test_decimal_scale_equivalent_outcome_remains_exact() -> None:
    scaled = _outcome(
        financial=FinancialOutcome(
            fees=Decimal("1.00"),
            realized_pnl=Decimal("0.0"),
            unrealized_pnl=Decimal("2.000"),
            equity=Decimal("1002.00"),
        )
    )
    report = compare_four_run_parity(_matrix(changed_role="BB", outcome=scaled))
    assert report.comparisons == COMPARISONS


@pytest.mark.parametrize(
    "matrix",
    (
        pytest.param([], id="list"),
        pytest.param(_matrix()[:3], id="short"),
        pytest.param((*_matrix()[:3], object()), id="wrong-item"),
        pytest.param((_run("BL"), _run("BL"), _run("CL"), _run("CB")), id="duplicate"),
    ),
)
def test_matrix_shape_is_invalid_input_identity(matrix: object) -> None:
    _assert_invalid(matrix, "matrix")


@pytest.mark.parametrize(
    ("role", "field", "value"),
    (
        ("BB", "revision_sha", "9" * 40),
        ("BB", "tree_oid", "9" * 40),
        ("CB", "revision_sha", "9" * 40),
        ("CB", "tree_oid", "9" * 40),
        ("CL", "revision_sha", "a" * 40),
        ("CL", "tree_oid", "b" * 40),
        ("CL", "input_sha256", "9" * 64),
        ("CL", "configuration_sha256", "9" * 64),
        ("CL", "loaded_artifact_sha256", "9" * 64),
        ("CL", "native_matcher_sha256", "9" * 64),
        ("CL", "runtime_source_sha256", "9" * 64),
        ("CB", "runner_sha256", "9" * 64),
    ),
)
def test_each_identity_family_fails_before_semantic_comparison(
    role: str, field: str, value: str
) -> None:
    changed = _outcome(
        financial=FinancialOutcome(
            fees=Decimal("9"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("2"),
            equity=Decimal("1002"),
        )
    )
    matrix = _replace_run(
        _matrix(changed_role=role, outcome=changed), role, **{field: value}
    )
    _assert_invalid(matrix, field)


@pytest.mark.parametrize("role", ("BB", "CL", "CB"))
@pytest.mark.parametrize(
    "field",
    (
        "input_sha256",
        "configuration_sha256",
        "loaded_artifact_sha256",
        "native_matcher_sha256",
    ),
)
def test_shared_identity_fields_are_checked_in_every_nonanchor_role(
    role: str, field: str
) -> None:
    _assert_invalid(
        _replace_run(_matrix(), role, **{field: "9" * 64}),
        field,
    )


@pytest.mark.parametrize("method", ("copy", "construct"))
def test_corrupt_run_is_revalidated_as_invalid_identity(method: str) -> None:
    valid = _run("BL")
    if method == "copy":
        corrupt = valid.model_copy(update={"revision_sha": "bad"})
    else:
        corrupt = TradingParityRun.model_construct(
            **{**valid.__dict__, "revision_sha": "bad"}
        )
    _assert_invalid((corrupt, _run("BB"), _run("CL"), _run("CB")), "matrix")


def _changed_financial(field: str) -> TradingOutcome:
    values = _outcome().financial.model_dump()
    values[field] = Decimal("9")
    return _outcome(financial=FinancialOutcome.model_validate(values))


@pytest.mark.parametrize("field", ("fees", "realized_pnl", "unrealized_pnl", "equity"))
def test_every_financial_field_reports_first_semantic_difference(field: str) -> None:
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(
            _matrix(changed_role="BB", outcome=_changed_financial(field))
        )
    assert captured.value.comparison == "BL_BB"
    assert captured.value.difference.path == f"$.financial.{field}"
    assert (
        captured.value.difference.expected_json != captured.value.difference.actual_json
    )


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("final_mark", Decimal("101")),
        ("end_timestamp", 4),
        ("halted_early", True),
    ),
)
def test_every_endpoint_scalar_reports_first_semantic_difference(
    field: str, value: object
) -> None:
    endpoint = _outcome().endpoint_state.model_copy(update={field: value})
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(
            _matrix(changed_role="BB", outcome=_outcome(endpoint=endpoint))
        )
    assert captured.value.comparison == "BL_BB"
    assert captured.value.difference.path == f"$.endpoint_state.{field}"


@pytest.mark.parametrize(
    ("field", "endpoint"),
    (
        (
            "positions",
            ReplayEndpointState(
                positions=(
                    EndpointPosition(
                        strategy_id="s",
                        product_id="MNQ",
                        side=PositionSide.LONG,
                        quantity=Decimal("1"),
                        average_entry_price=Decimal("100"),
                    ),
                ),
                working_orders=(),
                final_mark=Decimal("100"),
                end_timestamp=3,
                halted_early=False,
            ),
        ),
        (
            "working_orders",
            ReplayEndpointState(
                positions=(),
                working_orders=(
                    EndpointOrder(
                        strategy_id="s",
                        product_id="MNQ",
                        side=OrderSide.BUY,
                        order_type="LIMIT",
                        quantity=Decimal("1"),
                        timestamp=3,
                        price=Decimal("99"),
                    ),
                ),
                final_mark=Decimal("100"),
                end_timestamp=3,
                halted_early=False,
            ),
        ),
    ),
)
def test_every_endpoint_collection_reports_first_semantic_difference(
    field: str, endpoint: ReplayEndpointState
) -> None:
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(
            _matrix(changed_role="BB", outcome=_outcome(endpoint=endpoint))
        )
    assert captured.value.comparison == "BL_BB"
    assert captured.value.difference.path.startswith(f"$.endpoint_state.{field}")


@pytest.mark.parametrize(
    ("section", "changed"),
    (
        (
            "signals",
            lambda outcome: _outcome(
                signals=(outcome.signals[0].model_copy(update={"value": Decimal("2")}),)
            ),
        ),
        (
            "order_observations",
            lambda outcome: _outcome(
                orders=(
                    outcome.order_observations[0].model_copy(
                        update={"status": "FILLED"}
                    ),
                )
            ),
        ),
        (
            "fills",
            lambda outcome: _outcome(
                fills=(outcome.fills[0].model_copy(update={"price": Decimal("101")}),)
            ),
        ),
        (
            "journal",
            lambda outcome: _outcome(
                journal=(outcome.journal[0].model_copy(update={"tag": "entry"}),)
            ),
        ),
    ),
)
def test_each_observation_section_is_compared(
    section: str, changed: Callable[[TradingOutcome], TradingOutcome]
) -> None:
    outcome = _outcome()
    actual = changed(outcome)
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(_matrix(changed_role="BB", outcome=actual))
    assert captured.value.difference.path.startswith(f"$.{section}")


@pytest.mark.parametrize(
    "section", ("signals", "order_observations", "fills", "journal")
)
def test_each_observation_sequence_preserves_causal_order(section: str) -> None:
    base = _outcome()
    first = getattr(base, section)[0]
    if section == "signals":
        second = first.model_copy(update={"timestamp_ms": 2, "value": Decimal("2")})
        expected = _outcome(signals=(first, second))
        actual = _outcome(signals=(second, first))
    elif section == "order_observations":
        second = first.model_copy(
            update={"logical_order_id": "order-2", "timestamp_ms": 3}
        )
        expected = _outcome(orders=(first, second))
        actual = _outcome(orders=(second, first))
    elif section == "fills":
        second = first.model_copy(
            update={"logical_order_id": "order-2", "timestamp_ms": 4}
        )
        expected = _outcome(fills=(first, second))
        actual = _outcome(fills=(second, first))
    else:
        second = first.model_copy(
            update={"logical_trade_id": "trade-2", "timestamp_ms": 4}
        )
        expected = _outcome(journal=(first, second))
        actual = _outcome(journal=(second, first))

    matrix = _matrix()
    matrix = tuple(
        _run("BL", expected)
        if run.role == "BL"
        else _run("BB", actual)
        if run.role == "BB"
        else run
        for run in matrix
    )
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(matrix)
    assert captured.value.comparison == "BL_BB"
    assert captured.value.difference.path.startswith(f"$.{section}[0]")


def test_comparison_order_is_fixed_for_multiple_mismatches() -> None:
    matrix = _matrix(changed_role="BB", outcome=_changed_financial("realized_pnl"))
    matrix = tuple(
        _run("CB", _changed_financial("fees")) if run.role == "CB" else run
        for run in matrix
    )
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(matrix)
    assert captured.value.comparison == "BL_BB"
    assert captured.value.difference.path == "$.financial.realized_pnl"


@pytest.mark.parametrize(
    ("changed_roles", "comparison"),
    (
        (("CB",), "CL_CB"),
        (("CL", "CB"), "BL_CL"),
    ),
)
def test_later_comparison_directions_are_reachable(
    changed_roles: tuple[str, ...], comparison: str
) -> None:
    changed = _changed_financial("fees")
    matrix = tuple(
        _run(run.role, changed) if run.role in changed_roles else run
        for run in _matrix()
    )
    with pytest.raises(TradingParityMismatch) as captured:
        compare_four_run_parity(matrix)
    assert captured.value.comparison == comparison
    assert captured.value.difference.path == "$.financial.fees"
    assert captured.value.difference.expected_json == '["decimal",0,"1",0]'
    assert captured.value.difference.actual_json == '["decimal",0,"9",0]'


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("run_digests", (("BL", "bad"),) * 4),
        ("run_digests", (("BL", "1" * 64),) * 3),
        (
            "run_digests",
            list(zip(("BL", "BB", "CL", "CB"), ("1" * 64,) * 4)),
        ),
        ("comparisons", tuple(reversed(COMPARISONS))),
        ("comparisons", list(COMPARISONS)),
        ("comparisons", (("BL_BB", "mismatch"), *COMPARISONS[1:])),
    ),
)
def test_report_replace_revalidates_exact_shape(field: str, value: object) -> None:
    report = compare_four_run_parity(_matrix())
    with pytest.raises(ValueError):
        replace(report, **{field: value})


def test_report_rejects_reversed_valid_role_digest_records() -> None:
    report = compare_four_run_parity(_matrix())
    with pytest.raises(ValueError):
        replace(report, run_digests=tuple(reversed(report.run_digests)))
