from copy import deepcopy
from decimal import Decimal, getcontext, localcontext
from types import SimpleNamespace
from typing import Literal

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.validation.trading_outcome import (
    FillObservation,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)
from src.validation.trading_parity import TradingParityRun


class _FalseyModelExtra(dict[str, object]):
    def __bool__(self) -> bool:
        return False


def _outcome(equity: Decimal = Decimal("1002")) -> TradingOutcome:
    return TradingOutcome.model_validate(
        {
            "signals": (
                SignalObservation.model_validate(
                    dict(
                        strategy_id="s",
                        product_id="MNQ",
                        timeframe="5m",
                        timestamp_ms=1,
                        signal_type="LONG",
                        value=Decimal("1"),
                        quantity=None,
                        price=None,
                        stop_loss=None,
                        take_profit=None,
                        trailing_distance=None,
                        metadata_json={},
                    )
                ),
            ),
            "order_observations": (
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
            "fills": (
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
            "endpoint_state": ReplayEndpointState(
                positions=(),
                working_orders=(),
                final_mark=Decimal("100"),
                end_timestamp=3,
                halted_early=False,
            ),
            "financial": {
                "fees": Decimal("1"),
                "realized_pnl": Decimal("0"),
                "unrealized_pnl": Decimal("2"),
                "equity": equity,
            },
            "journal": (
                JournalObservation.model_validate(
                    dict(
                        strategy_id="s",
                        timestamp_ms=3,
                        tag="fill",
                        logical_trade_id="trade-1",
                        data_json={},
                    )
                ),
            ),
        }
    )


def _payload() -> dict[str, object]:
    return {
        "role": "BL",
        "source_version": "baseline",
        "mode": "live_like",
        "revision_sha": "1" * 40,
        "tree_oid": "2" * 64,
        "runtime_source_sha256": "3" * 64,
        "input_sha256": "4" * 64,
        "configuration_sha256": "5" * 64,
        "runner_sha256": "6" * 64,
        "loaded_artifact_sha256": "7" * 64,
        "native_matcher_sha256": "8" * 64,
        "outcome": _outcome(),
    }


def _run(payload: dict[str, object] | None = None) -> TradingParityRun:
    return TradingParityRun.model_validate(_payload() if payload is None else payload)


def _outcome_construct(
    outcome: TradingOutcome,
    *,
    omit: str | None = None,
    changes: dict[str, object] | None = None,
) -> TradingOutcome:
    values = {field: getattr(outcome, field) for field in TradingOutcome.model_fields}
    if omit is not None:
        del values[omit]
    values.update({} if changes is None else changes)
    return TradingOutcome.model_construct(**values)


def test_all_legal_parity_cells_have_unique_canonical_identity() -> None:
    shared = _payload()
    runs = []
    for role, source_version, mode in (
        ("BL", "baseline", "live_like"),
        ("BB", "baseline", "backtest"),
        ("CL", "candidate", "live_like"),
        ("CB", "candidate", "backtest"),
    ):
        payload = dict(shared)
        payload.update(
            role=role,
            source_version=source_version,
            mode=mode,
        )
        runs.append(_run(payload))

    assert len({run.canonical_bytes() for run in runs}) == 4
    assert len({run.sha256() for run in runs}) == 4


def test_run_preserves_explicit_empty_mapping() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _run({})

    errors = exc_info.value.errors()
    assert len(errors) == 12
    assert {error["loc"] for error in errors} == {
        ("role",),
        ("source_version",),
        ("mode",),
        ("revision_sha",),
        ("tree_oid",),
        ("runtime_source_sha256",),
        ("input_sha256",),
        ("configuration_sha256",),
        ("runner_sha256",),
        ("loaded_artifact_sha256",),
        ("native_matcher_sha256",),
        ("outcome",),
    }
    assert all(error["type"] == "missing" for error in errors)


@pytest.mark.parametrize(
    "role,source_version,mode",
    [
        ("BL", "baseline", "live_like"),
        ("BB", "baseline", "backtest"),
        ("CL", "candidate", "live_like"),
        ("CB", "candidate", "backtest"),
    ],
)
def test_exact_parity_matrix_cells_validate(
    role: str, source_version: str, mode: str
) -> None:
    payload = _payload()
    payload.update(role=role, source_version=source_version, mode=mode)
    run = _run(payload)
    assert (run.role, run.source_version, run.mode) == (role, source_version, mode)


@pytest.mark.parametrize(
    "role,source_version,mode",
    [
        (role, source_version, mode)
        for role in ("BL", "BB", "CL", "CB")
        for source_version in ("baseline", "candidate")
        for mode in ("live_like", "backtest")
        if (role, source_version, mode)
        not in {
            ("BL", "baseline", "live_like"),
            ("BB", "baseline", "backtest"),
            ("CL", "candidate", "live_like"),
            ("CB", "candidate", "backtest"),
        }
    ],
)
def test_every_mismatched_parity_matrix_cell_fails(
    role: str, source_version: str, mode: str
) -> None:
    payload = _payload()
    payload.update(role=role, source_version=source_version, mode=mode)
    with pytest.raises(ValidationError):
        _run(payload)


@pytest.mark.parametrize("field", ["revision_sha", "tree_oid"])
@pytest.mark.parametrize(
    "invalid",
    ["", " " * 40, "g" * 40, "A" * 40, "a" * 39, "a" * 41, "a" * 63, "a" * 65],
)
def test_git_identities_fail_closed(field: str, invalid: str) -> None:
    payload = _payload()
    payload[field] = invalid
    with pytest.raises(ValidationError):
        _run(payload)


@pytest.mark.parametrize(
    "field",
    [
        "runtime_source_sha256",
        "input_sha256",
        "configuration_sha256",
        "runner_sha256",
        "loaded_artifact_sha256",
        "native_matcher_sha256",
    ],
)
@pytest.mark.parametrize(
    "invalid", ["", " " * 64, "g" * 64, "A" * 64, "a" * 63, "a" * 65]
)
def test_sha256_identities_fail_closed(field: str, invalid: str) -> None:
    payload = _payload()
    payload[field] = invalid
    with pytest.raises(ValidationError):
        _run(payload)


def test_decimal_equivalent_outcomes_keep_envelope_identity() -> None:
    left = _run()
    payload = _payload()
    payload["outcome"] = _outcome(Decimal("1002.000"))
    right = _run(payload)
    assert left.canonical_bytes() == right.canonical_bytes()
    assert left.sha256() == right.sha256()


def test_extreme_decimal_parity_envelope_is_compact_and_digest_sensitive() -> None:
    runs = []
    for value in (Decimal("1E+1000000"), Decimal("1E-1000000")):
        payload = _payload()
        payload["outcome"] = _outcome(value)
        runs.append(_run(payload))

    assert tuple(run.outcome.financial.equity.as_tuple() for run in runs) == (
        Decimal("1E+1000000").as_tuple(),
        Decimal("1E-1000000").as_tuple(),
    )
    assert all(len(run.canonical_bytes()) < 2048 for run in runs)
    assert runs[0].sha256() != runs[1].sha256()
    assert TradingParityRun.schema_version == "fluxtrade.trading_parity_run.v2"


def test_long_decimal_parity_identity_is_context_independent() -> None:
    exact = Decimal(
        "123456789012345678901234567890123456789012345678901234567890.123456789"
    )
    adjacent = Decimal(
        "123456789012345678901234567890123456789012345678901234567890.123456788"
    )
    global_context = str(getcontext())
    identities: list[tuple[bytes, str]] = []

    for precision in (6, 28, 50):
        with localcontext() as context:
            context.prec = precision
            exact_payload = _payload()
            exact_payload["outcome"] = _outcome(exact)
            adjacent_payload = _payload()
            adjacent_payload["outcome"] = _outcome(adjacent)
            run = _run(exact_payload)
            changed = _run(adjacent_payload)

            assert run.sha256() != changed.sha256()
            identities.append((run.canonical_bytes(), run.sha256()))

    assert len(set(identities)) == 1
    assert str(getcontext()) == global_context


@pytest.mark.parametrize(
    "field",
    [
        "revision_sha",
        "tree_oid",
        "runtime_source_sha256",
        "input_sha256",
        "configuration_sha256",
        "runner_sha256",
        "loaded_artifact_sha256",
        "native_matcher_sha256",
    ],
)
def test_each_provenance_identity_changes_digest(field: str) -> None:
    baseline = _run()
    changed = _payload()
    changed[field] = "9" * len(str(changed[field]))
    assert baseline.sha256() != _run(changed).sha256()


def test_exact_outcome_financial_change_changes_digest() -> None:
    baseline = _run()
    changed = _payload()
    changed["outcome"] = _outcome(Decimal("1003"))
    assert baseline.sha256() != _run(changed).sha256()


@pytest.mark.parametrize("field", list(_payload()))
def test_every_field_is_required(field: str) -> None:
    payload = _payload()
    del payload[field]
    with pytest.raises(ValidationError):
        _run(payload)


def test_extra_field_fails_closed() -> None:
    payload = deepcopy(_payload())
    payload["provider_run_id"] = "untrusted"
    with pytest.raises(ValidationError):
        _run(payload)


def test_outcome_mapping_fails_closed() -> None:
    payload = _payload()
    payload["outcome"] = _outcome().model_dump()
    with pytest.raises(ValidationError):
        _run(payload)


def test_model_is_frozen_and_schema_is_not_input() -> None:
    run = _run()
    with pytest.raises(ValidationError):
        run.mode = "backtest"
    payload = _payload()
    payload["schema_version"] = TradingParityRun.schema_version
    with pytest.raises(ValidationError):
        _run(payload)


@pytest.mark.parametrize("field", list(TradingOutcome.model_fields))
def test_incomplete_exact_outcome_fails_at_parity_construction(field: str) -> None:
    payload = _payload()
    payload["outcome"] = _outcome_construct(_outcome(), omit=field)
    with pytest.raises(ValidationError):
        _run(payload)


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize(
    "field",
    [
        "signals",
        "order_observations",
        "fills",
        "endpoint_state",
        "financial",
        "journal",
    ],
)
def test_corrupted_exact_outcome_fails_at_parity_construction(
    field: str, method: str
) -> None:
    valid = _outcome()
    if field == "endpoint_state":
        corrupted: object = valid.endpoint_state.model_copy(
            update={"final_mark": 100.0}
        )
    elif field == "financial":
        corrupted = valid.financial.model_copy(update={"equity": 1002.0})
    else:
        observation_class = {
            "signals": SignalObservation,
            "order_observations": OrderObservation,
            "fills": FillObservation,
            "journal": JournalObservation,
        }[field]
        corrupted = (observation_class.model_construct(),)
    outcome = (
        valid.model_copy(update={field: corrupted})
        if method == "model_copy"
        else _outcome_construct(valid, changes={field: corrupted})
    )
    payload = _payload()
    payload["outcome"] = outcome
    with pytest.raises(ValidationError):
        _run(payload)


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize(
    "field",
    [
        "signals",
        "order_observations",
        "fills",
        "endpoint_state",
        "financial",
        "journal",
    ],
)
def test_raw_nested_mappings_fail_at_parity_construction(
    field: str, method: str
) -> None:
    valid = _outcome()
    exact = getattr(valid, field)
    corrupted: object = (
        dict(exact.__dict__)
        if field in {"endpoint_state", "financial"}
        else (dict(exact[0].__dict__),)
    )
    outcome = (
        valid.model_copy(update={field: corrupted})
        if method == "model_copy"
        else _outcome_construct(valid, changes={field: corrupted})
    )
    payload = _payload()
    payload["outcome"] = outcome
    with pytest.raises(ValidationError):
        _run(payload)


def test_valid_exact_outcome_preserves_parity_v2_bytes_and_digest() -> None:
    expected = _run()
    payload = _payload()
    payload["outcome"] = _outcome_construct(expected.outcome)
    actual = _run(payload)
    outcome_sha256 = "f83da50e27173381f574908321be590c123ea6c84be8cc6685e4dc3e5f9c493d"
    parity_sha256 = "173b4491bab684564a4c857d2d73c621b6d7f0eb3bb753dddb972df18b320b0b"
    assert actual.outcome.canonical_bytes() == expected.outcome.canonical_bytes()
    assert actual.outcome.sha256() == outcome_sha256
    assert actual.canonical_bytes() == expected.canonical_bytes()
    assert actual.sha256() == parity_sha256
    assert actual.sha256() == expected.sha256()


def test_trading_outcome_subclass_fails_at_parity_construction() -> None:
    valid = _outcome()
    subclass = type("TradingOutcomeSubclass", (TradingOutcome,), {})
    outcome = subclass.model_construct(
        **{field: getattr(valid, field) for field in TradingOutcome.model_fields}
    )
    payload = _payload()
    payload["outcome"] = outcome
    with pytest.raises(ValidationError):
        _run(payload)


@pytest.mark.parametrize(
    "field",
    [
        "revision_sha",
        "tree_oid",
        "runtime_source_sha256",
        "input_sha256",
        "configuration_sha256",
        "runner_sha256",
        "loaded_artifact_sha256",
        "native_matcher_sha256",
    ],
)
def test_strict_false_rejects_parity_string_coercion(field: str) -> None:
    payload = _payload()
    payload[field] = str(payload[field]).encode()
    with pytest.raises(ValidationError):
        TradingParityRun.model_validate(payload, strict=False)


@pytest.mark.parametrize("extra", ["allow", "ignore"])
def test_parity_call_time_extra_fails_closed(extra: Literal["allow", "ignore"]) -> None:
    payload = _payload()
    payload["unexpected"] = "accepted"
    with pytest.raises(ValidationError):
        TradingParityRun.model_validate(payload, extra=extra)


def test_parity_from_attributes_fails_closed() -> None:
    with pytest.raises(ValidationError):
        TradingParityRun.model_validate(
            SimpleNamespace(**_payload()), from_attributes=True
        )


def test_exact_outcome_model_extra_fails_at_parity_construction() -> None:
    valid = _outcome()
    object.__setattr__(valid, "__pydantic_extra__", {"unexpected": "accepted"})
    assert valid.model_extra == {"unexpected": "accepted"}
    assert "unexpected" not in valid.__dict__
    payload = _payload()
    payload["outcome"] = valid
    with pytest.raises(ValidationError):
        _run(payload)


def test_parity_rejects_falsey_model_extra_on_exact_outcome() -> None:
    outcome = _outcome()
    hidden = _FalseyModelExtra(unexpected="accepted")
    object.__setattr__(outcome, "__pydantic_extra__", hidden)
    assert outcome.model_extra == {"unexpected": "accepted"}
    assert not outcome.model_extra
    payload = _payload()
    payload["outcome"] = outcome
    with pytest.raises(ValidationError):
        _run(payload)


def test_direct_exact_parity_run_without_model_extra_preserves_identity() -> None:
    expected = _run()
    assert expected.model_extra is None
    actual = TradingParityRun.model_validate(expected)
    assert actual.canonical_bytes() == expected.canonical_bytes()
    assert actual.sha256() == expected.sha256()


@pytest.mark.parametrize(
    "hidden",
    [
        pytest.param({}, id="empty_dict"),
        pytest.param(_FalseyModelExtra(unexpected="accepted"), id="falsey_nonempty"),
        pytest.param({"unexpected": "accepted"}, id="truthy_nonempty"),
    ],
)
def test_direct_parity_run_rejects_model_extra(hidden: dict[str, object]) -> None:
    exact = _run()
    object.__setattr__(exact, "__pydantic_extra__", hidden)
    assert exact.model_extra is hidden
    with pytest.raises(ValidationError):
        TradingParityRun.model_validate(exact)


@pytest.mark.parametrize("hidden", [None, {}], ids=["none", "empty_dict"])
def test_parity_subclass_self_validation_preserves_clean_identity(
    hidden: dict[str, object] | None,
) -> None:
    expected = _run()
    subclass = type("FreshTradingParityRun", (TradingParityRun,), {})
    exact = subclass.model_construct(**expected.__dict__)
    object.__setattr__(exact, "__pydantic_extra__", hidden)
    actual = subclass.model_validate(exact)
    assert type(actual) is subclass
    assert actual.model_extra is None
    assert actual.canonical_bytes() == expected.canonical_bytes()
    assert actual.sha256() == expected.sha256()


@pytest.mark.parametrize(
    "hidden",
    [
        pytest.param(_FalseyModelExtra(unexpected="accepted"), id="falsey_nonempty"),
        pytest.param({"unexpected": "accepted"}, id="truthy_nonempty"),
    ],
)
def test_parity_subclass_self_validation_rejects_nonempty_extra(
    hidden: dict[str, object],
) -> None:
    expected = _run()
    subclass = type("FreshTradingParityRun", (TradingParityRun,), {})
    exact = subclass.model_construct(**expected.__dict__)
    object.__setattr__(exact, "__pydantic_extra__", hidden)
    with pytest.raises(ValidationError):
        subclass.model_validate(exact)


def test_fixed_multibyte_v2_outcome_and_parity_identity() -> None:
    tagged, valid = "strategy-策略-交易-ß-🙂", _outcome()
    signal = valid.signals[0].model_copy(update={"strategy_id": tagged})
    outcome = TradingOutcome.model_validate(
        _outcome_construct(valid, changes={"signals": (signal,)})
    )
    parity_payload = _payload()
    parity_payload["outcome"] = outcome
    run = _run(parity_payload)
    old = b'["strategy_id",["string","s"]]'
    head, separator, tail = valid.canonical_bytes().rpartition(old)
    expected_outcome_bytes = (
        head + separator.replace(b'"s"', b'"' + tagged.encode() + b'"') + tail
    )
    assert outcome.canonical_bytes() == expected_outcome_bytes
    assert (
        outcome.sha256()
        == "8775298fa919b23d77247a0592436fa00a515adf816b6dc8dc3ed09797f72cdf"
    )
    expected_run_bytes = (
        _run()
        .canonical_bytes()
        .replace(
            b"f83da50e27173381f574908321be590c123ea6c84be8cc6685e4dc3e5f9c493d",
            outcome.sha256().encode(),
        )
    )
    assert run.canonical_bytes() == expected_run_bytes
    assert (
        run.sha256()
        == "ab8a5dfc518a6372edb4a14dbe73d18fe70b03281d2ab26530110bb0f6f29be7"
    )
