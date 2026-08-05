from copy import deepcopy
from decimal import Decimal, getcontext, localcontext

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.validation.trading_outcome import TradingOutcome
from src.validation.trading_parity import TradingParityRun


def _outcome(equity: Decimal = Decimal("1002")) -> TradingOutcome:
    return TradingOutcome.model_validate(
        {
            "signals": (),
            "order_observations": (),
            "fills": (),
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
            "journal": (),
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
