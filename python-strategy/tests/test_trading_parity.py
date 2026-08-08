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


class _CleanTradingParityRun(TradingParityRun):
    pass


class _SemanticTradingParityRun(TradingParityRun):
    semantic_field: str


class _CleanTradingOutcome(TradingOutcome):
    pass


class _HostileIdentity(str):
    calls: list[str]

    def __new__(cls, value: str) -> "_HostileIdentity":
        instance = super().__new__(cls, value)
        instance.calls = []
        return instance

    def strip(self, chars: str | None = None) -> str:
        self.calls.append("strip")
        return str.strip(self, chars)

    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        self.calls.append("encode")
        return str.encode(self, encoding, errors)

    def __hash__(self) -> int:
        self.calls.append("hash")
        return str.__hash__(self)

    def __eq__(self, other: object) -> bool:
        self.calls.append("eq")
        return str.__eq__(self, other)

    def __repr__(self) -> str:
        self.calls.append("repr")
        return str.__repr__(self)

    def __format__(self, format_spec: str) -> str:
        self.calls.append("format")
        return str.__format__(self, format_spec)


PARITY_STRING_FIELDS = (
    "role",
    "source_version",
    "mode",
    "revision_sha",
    "tree_oid",
    "runtime_source_sha256",
    "input_sha256",
    "configuration_sha256",
    "runner_sha256",
    "loaded_artifact_sha256",
    "native_matcher_sha256",
)


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


def _direct_run(
    method: Literal["model_copy", "model_construct"],
    *,
    changes: dict[str, object] | None = None,
) -> TradingParityRun:
    valid = _run()
    if method == "model_copy":
        return valid.model_copy(update={} if changes is None else changes)
    values = dict(valid.__dict__)
    values.update({} if changes is None else changes)
    return TradingParityRun.model_construct(**values)


def _assert_root_cause(
    exc_info: pytest.ExceptionInfo[ValueError | ValidationError],
    message: str,
    loc: tuple[str, ...] = (),
) -> None:
    assert isinstance(exc_info.value, ValidationError)
    assert exc_info.value.title == "TradingParityRun"
    errors = exc_info.value.errors(include_input=False, include_url=False)
    assert len(errors) == 1
    error = errors[0]
    context = error.get("ctx")
    assert error["loc"] == loc
    assert error["type"] == "value_error"
    assert error["msg"] == f"Value error, {message}"
    assert context is not None
    cause = context["error"]
    assert (type(cause), str(cause)) == (ValueError, message)


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


@pytest.mark.parametrize("field", list(TradingParityRun.model_fields))
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
def test_direct_missing_parity_field_has_structured_validation_error(
    field: str, identity_method: Literal["canonical_bytes", "sha256"]
) -> None:
    values = dict(_run().__dict__)
    del values[field]
    corrupt = TradingParityRun.model_construct(**values)

    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()

    assert exc_info.value.errors(include_input=False, include_url=False) == [
        {"type": "missing", "loc": (field,), "msg": "Field required"}
    ]


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
@pytest.mark.parametrize(
    "field,value,error_type,message,context",
    [
        pytest.param(
            "role",
            "XX",
            "literal_error",
            "Input should be 'BL', 'BB', 'CL' or 'CB'",
            {"expected": "'BL', 'BB', 'CL' or 'CB'"},
            id="role_literal",
        ),
        pytest.param(
            "source_version",
            "other",
            "literal_error",
            "Input should be 'baseline' or 'candidate'",
            {"expected": "'baseline' or 'candidate'"},
            id="source_literal",
        ),
        pytest.param(
            "mode",
            "other",
            "literal_error",
            "Input should be 'live_like' or 'backtest'",
            {"expected": "'live_like' or 'backtest'"},
            id="mode_literal",
        ),
        pytest.param(
            "revision_sha",
            "NOT-A-SHA",
            "string_pattern_mismatch",
            "String should match pattern '^(?:[0-9a-f]{40}|[0-9a-f]{64})$'",
            {"pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$"},
            id="git_sha_pattern",
        ),
        pytest.param(
            "input_sha256",
            "NOT-A-SHA",
            "string_pattern_mismatch",
            "String should match pattern '^[0-9a-f]{64}$'",
            {"pattern": "^[0-9a-f]{64}$"},
            id="sha256_pattern",
        ),
    ],
)
def test_direct_invalid_parity_field_has_structured_validation_error(
    method: Literal["model_copy", "model_construct"],
    identity_method: Literal["canonical_bytes", "sha256"],
    field: str,
    value: str,
    error_type: str,
    message: str,
    context: dict[str, str],
) -> None:
    corrupt = _direct_run(method, changes={field: value})

    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()

    assert exc_info.value.errors(include_input=False, include_url=False) == [
        {"type": error_type, "loc": (field,), "msg": message, "ctx": context}
    ]


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
def test_direct_parity_matrix_mismatch_has_structured_validation_error(
    method: Literal["model_copy", "model_construct"],
    identity_method: Literal["canonical_bytes", "sha256"],
) -> None:
    corrupt = _direct_run(method, changes={"mode": "backtest"})

    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()

    _assert_root_cause(
        exc_info,
        "role, source_version, and mode must identify one exact parity cell",
    )


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
@pytest.mark.parametrize("field", PARITY_STRING_FIELDS)
def test_direct_parity_rejects_hostile_string_subclass_before_operations(
    method: Literal["model_copy", "model_construct"],
    identity_method: Literal["canonical_bytes", "sha256"],
    field: str,
) -> None:
    hostile = _HostileIdentity(str(getattr(_run(), field)))
    corrupt = _direct_run(method, changes={field: hostile})
    assert hostile.calls == []

    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()

    _assert_root_cause(exc_info, f"{field} must be a string")
    assert hostile.calls == []


def test_parity_string_field_ledger_matches_owner() -> None:
    assert set(PARITY_STRING_FIELDS) == TradingParityRun._string_fields


@pytest.mark.parametrize(
    "hidden",
    [
        pytest.param(_FalseyModelExtra(unexpected="accepted"), id="falsey"),
        pytest.param({"unexpected": "accepted"}, id="truthy"),
    ],
)
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
def test_direct_parity_identity_rejects_model_extra(
    hidden: dict[str, object],
    identity_method: Literal["canonical_bytes", "sha256"],
) -> None:
    corrupt = _run().model_copy()
    object.__setattr__(corrupt, "__pydantic_extra__", hidden)
    assert corrupt.model_extra is hidden

    with pytest.raises(ValueError) as exc_info:
        getattr(corrupt, identity_method)()

    assert (type(exc_info.value), str(exc_info.value)) == (
        ValueError,
        "parity run contains unexpected fields",
    )


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
@pytest.mark.parametrize("case", ["raw_mapping", "subclass"])
def test_direct_parity_rejects_nonexact_outcome_with_structured_error(
    method: Literal["model_copy", "model_construct"],
    identity_method: Literal["canonical_bytes", "sha256"],
    case: Literal["raw_mapping", "subclass"],
) -> None:
    valid = _outcome()
    outcome: object = (
        valid.model_dump()
        if case == "raw_mapping"
        else _CleanTradingOutcome.model_construct(**valid.__dict__)
    )
    corrupt = _direct_run(method, changes={"outcome": outcome})

    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()

    _assert_root_cause(exc_info, "outcome must be an exact TradingOutcome")


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
@pytest.mark.parametrize(
    "case,expected_loc,expected_message",
    [
        pytest.param(
            "raw_financial",
            (),
            "outcome summaries must have exact canonical types",
            id="raw_financial",
        ),
        pytest.param(
            "missing_financial",
            (),
            "outcome summaries must have exact canonical types",
            id="missing_financial",
        ),
        pytest.param(
            "float_equity",
            ("financial", "equity"),
            "financial values must be finite Decimal instances",
            id="float_equity",
        ),
        pytest.param(
            "falsey_extra",
            (),
            "outcome contains unexpected fields",
            id="falsey_extra",
        ),
        pytest.param(
            "truthy_extra",
            (),
            "outcome contains unexpected fields",
            id="truthy_extra",
        ),
    ],
)
def test_direct_parity_keeps_corrupt_exact_outcome_fail_closed(
    method: Literal["model_copy", "model_construct"],
    identity_method: Literal["canonical_bytes", "sha256"],
    case: str,
    expected_loc: tuple[str, ...],
    expected_message: str,
) -> None:
    valid = _outcome()
    if case == "raw_financial":
        outcome = _outcome_construct(
            valid, changes={"financial": dict(valid.financial.__dict__)}
        )
    elif case == "missing_financial":
        outcome = _outcome_construct(valid, omit="financial")
    elif case == "float_equity":
        financial = valid.financial.model_copy(update={"equity": 1002.0})
        outcome = _outcome_construct(valid, changes={"financial": financial})
    else:
        outcome = _outcome_construct(valid)
        hidden: dict[str, object] = (
            _FalseyModelExtra(unexpected="accepted")
            if case == "falsey_extra"
            else {"unexpected": "accepted"}
        )
        object.__setattr__(outcome, "__pydantic_extra__", hidden)
    corrupt = _direct_run(method, changes={"outcome": outcome})

    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()

    _assert_root_cause(exc_info, expected_message, expected_loc)


SUBCLASS_ERROR = "TradingParityRun subclasses are unsupported"
SUBCLASS_CASES = [
    pytest.param(_CleanTradingParityRun, {}, id="clean"),
    pytest.param(
        _SemanticTradingParityRun,
        {"semantic_field": "semantic-one"},
        id="semantic",
    ),
]


def _assert_subclass_validation_error(
    exc_info: pytest.ExceptionInfo[ValidationError],
) -> None:
    errors = exc_info.value.errors(include_url=False)
    assert len(errors) == 1
    error = errors[0]
    context = error.get("ctx")
    assert error["loc"] == () and context is not None
    cause = context["error"]
    assert (type(cause), str(cause)) == (ValueError, SUBCLASS_ERROR)


def _construct_subclass(
    subclass: type[TradingParityRun], extra: dict[str, object]
) -> TradingParityRun:
    if subclass is _SemanticTradingParityRun:
        semantic = extra["semantic_field"]
        assert type(semantic) is str
        return subclass.model_construct(**_run().__dict__, semantic_field=semantic)
    return subclass.model_construct(**_run().__dict__)


@pytest.mark.parametrize("subclass,extra", SUBCLASS_CASES)
@pytest.mark.parametrize("entrypoint", ["constructor", "model_validate"])
def test_parity_subclasses_reject_normal_validation(
    subclass: type[TradingParityRun],
    extra: dict[str, object],
    entrypoint: Literal["constructor", "model_validate"],
) -> None:
    payload = {**_payload(), **extra}
    with pytest.raises(ValidationError) as exc_info:
        if entrypoint == "constructor":
            if subclass is _SemanticTradingParityRun:
                semantic = extra["semantic_field"]
                assert type(semantic) is str
                subclass(**_run().__dict__, semantic_field=semantic)
            else:
                subclass(**_run().__dict__)
        else:
            subclass.model_validate(payload)
    _assert_subclass_validation_error(exc_info)


@pytest.mark.parametrize(
    "subclass", [_CleanTradingParityRun, _SemanticTradingParityRun]
)
@pytest.mark.parametrize("entrypoint", ["constructor", "model_validate"])
def test_subclass_gate_precedes_empty_payload_validation(
    subclass: type[TradingParityRun],
    entrypoint: Literal["constructor", "model_validate"],
) -> None:
    empty_payload = _run().__dict__.copy()
    empty_payload.clear()
    assert empty_payload == {}
    with pytest.raises(ValidationError) as exc_info:
        if entrypoint == "constructor":
            subclass(**empty_payload)
        else:
            subclass.model_validate(empty_payload)
    _assert_subclass_validation_error(exc_info)


@pytest.mark.parametrize("subclass,extra", SUBCLASS_CASES)
def test_base_owner_rejects_parity_subclass_instance(
    subclass: type[TradingParityRun], extra: dict[str, object]
) -> None:
    instance = _construct_subclass(subclass, extra)
    with pytest.raises(ValidationError) as exc_info:
        TradingParityRun.model_validate(instance)
    _assert_subclass_validation_error(exc_info)


@pytest.mark.parametrize(
    "subclass,extra",
    [
        pytest.param(_CleanTradingParityRun, {}, id="clean"),
        pytest.param(
            _SemanticTradingParityRun,
            {"semantic_field": "semantic-one"},
            id="semantic_one",
        ),
        pytest.param(
            _SemanticTradingParityRun,
            {"semantic_field": "semantic-two"},
            id="semantic_two",
        ),
    ],
)
@pytest.mark.parametrize("copied", [False, True], ids=["construct", "copy"])
@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_direct_parity_subclasses_cannot_emit_identity(
    subclass: type[TradingParityRun],
    extra: dict[str, object],
    copied: bool,
    method: Literal["canonical_bytes", "sha256"],
) -> None:
    instance = _construct_subclass(subclass, extra)
    if copied:
        instance = instance.model_copy()
    with pytest.raises(ValueError) as exc_info:
        getattr(instance, method)()
    assert (type(exc_info.value), str(exc_info.value)) == (ValueError, SUBCLASS_ERROR)


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
