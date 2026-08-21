"""Adversarial contract tests for canonical Mapping snapshots."""

from collections import UserDict
from collections.abc import Iterator, Mapping
from decimal import Decimal
from types import MappingProxyType
from typing import Literal

import pytest
from pydantic import BaseModel, ValidationError

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.core.canonical_mapping import snapshot_mapping
from src.validation.trading_outcome import (
    FillObservation,
    FinancialOutcome,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)
from src.validation.trading_parity import TradingParityRun


_INVALID = "canonical model field names must be exact strings"


class _HostileDict(dict[str, object]):
    def __init__(self) -> None:
        dict.__init__(self, safe=Decimal("1"))
        self.calls: list[str] = []

    def __iter__(self) -> Iterator[str]:
        self.calls.append("iter")
        return iter(("spoofed",))

    def keys(self):
        self.calls.append("keys")
        return {"spoofed": Decimal("2")}.keys()

    def items(self):
        self.calls.append("items")
        return {"spoofed": Decimal("2")}.items()

    def __getitem__(self, key: str) -> object:
        self.calls.append(f"getitem:{key}")
        return Decimal("2")


class _ArmedString(str):
    armed = False

    def _trap(self) -> None:
        if self.armed:
            raise AssertionError("non-exact key was observed before rejection")

    def __hash__(self) -> int:
        self._trap()
        return str.__hash__(self)

    def __eq__(self, other: object) -> bool:
        self._trap()
        return str.__eq__(self, other)

    def __repr__(self) -> str:
        self._trap()
        return str.__repr__(self)

    def __format__(self, format_spec: str) -> str:
        self._trap()
        return str.__format__(self, format_spec)


class _TrapKey:
    def _trap(self) -> None:
        raise AssertionError("invalid generic key was observed")

    def __hash__(self) -> int:
        self._trap()
        return 0

    def __eq__(self, other: object) -> bool:
        self._trap()
        return False

    def __repr__(self) -> str:
        self._trap()
        return "unreachable"

    def __format__(self, format_spec: str) -> str:
        self._trap()
        return "unreachable"


class _StreamMapping(Mapping[object, object]):
    def __init__(
        self,
        streams: tuple[tuple[object, ...], ...],
        values: tuple[object, ...],
    ) -> None:
        self.streams = streams
        self.lookup_values = values
        self.iterations = 0
        self.lookups: list[object] = []

    def __iter__(self) -> Iterator[object]:
        index = min(self.iterations, len(self.streams) - 1)
        self.iterations += 1
        return iter(self.streams[index])

    def __len__(self) -> int:
        return len(self.streams[0])

    def __getitem__(self, key: object) -> object:
        self.lookups.append(key)
        return self.lookup_values[len(self.lookups) - 1]


_RaisedRuntime = type("_RaisedRuntime", (RuntimeError,), {})
_RaisedKey = type("_RaisedKey", (KeyError,), {})
_RaisedValue = type("_RaisedValue", (ValueError,), {})


class _FailingMapping(Mapping[str, object]):
    def __init__(self, error: Exception, operation: str) -> None:
        self.error = error
        self.operation = operation

    def __iter__(self) -> Iterator[str]:
        if self.operation == "iterator":
            raise self.error
        return iter(("fees",))

    def __len__(self) -> int:
        return 1

    def __getitem__(self, key: str) -> object:
        raise self.error


def _signal_payload() -> dict[str, object]:
    return dict(
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


def _order_payload() -> dict[str, object]:
    return dict(
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
    )


def _fill_payload() -> dict[str, object]:
    return dict(
        logical_order_id="order-1",
        strategy_id="s",
        product_id="MNQ",
        timestamp_ms=3,
        fill_type="entry",
        side="buy",
        price=Decimal("100"),
        quantity=Decimal("1"),
        fee=Decimal("1"),
    )


def _journal_payload() -> dict[str, object]:
    return dict(
        strategy_id="s",
        timestamp_ms=3,
        tag="fill",
        logical_trade_id="trade-1",
        data_json={},
    )


def _outcome_payload() -> dict[str, object]:
    return {
        "signals": (SignalObservation.model_validate(_signal_payload()),),
        "order_observations": (OrderObservation.model_validate(_order_payload()),),
        "fills": (FillObservation.model_validate(_fill_payload()),),
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
            "equity": Decimal("1002"),
        },
        "journal": (JournalObservation.model_validate(_journal_payload()),),
    }


def _outcome() -> TradingOutcome:
    return TradingOutcome.model_validate(_outcome_payload())


def _parity_payload(outcome: TradingOutcome | None = None) -> dict[str, object]:
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
        "outcome": _outcome() if outcome is None else outcome,
    }


def test_mut_dict_value_and_overridable_dict_access() -> None:
    value = _HostileDict()
    actual = snapshot_mapping(value, invalid_key_error=_INVALID)
    assert type(actual) is dict
    assert actual == {"safe": Decimal("1")}
    assert value.calls == []


def test_mut_pre_gate_hash_rejects_armed_stored_str_subclass() -> None:
    key = _ArmedString("safe")
    value = {key: Decimal("1")}
    key.armed = True
    with pytest.raises(ValueError, match=f"^{_INVALID}$"):
        snapshot_mapping(value, invalid_key_error=_INVALID)


def test_primitive_sources_preserve_non_empty_values() -> None:
    expected = {"safe": Decimal("1")}
    sources: tuple[Mapping[str, Decimal], ...] = (
        expected,
        UserDict(expected),
        MappingProxyType(expected),
    )
    for source in sources:
        actual = snapshot_mapping(source, invalid_key_error=_INVALID)
        assert type(actual) is dict
        assert actual == expected


def test_mut_pre_gate_getitem_and_membership_reject_generic_invalid_key() -> None:
    value = _StreamMapping(((_TrapKey(),),), (Decimal("1"),))
    with pytest.raises(ValueError, match=f"^{_INVALID}$"):
        snapshot_mapping(value, invalid_key_error=_INVALID)
    assert value.lookups == []


def test_mut_second_materialization_observes_only_first_key_stream() -> None:
    value = _StreamMapping(
        (("first",), ("second",)),
        (Decimal("1"), Decimal("999")),
    )
    actual = snapshot_mapping(value, invalid_key_error=_INVALID)
    assert type(actual) is dict
    assert actual == {"first": Decimal("1")}
    assert value.iterations == 1
    assert "second" not in actual
    assert Decimal("999") not in actual.values()
    assert value.lookups == ["first"]


def test_mut_late_dedup_uses_one_first_observed_lookup() -> None:
    value = _StreamMapping(
        (("duplicate", "duplicate"),),
        (Decimal("1"), Decimal("999")),
    )
    actual = snapshot_mapping(value, invalid_key_error=_INVALID)
    assert actual == {"duplicate": Decimal("1")}
    assert value.lookups == ["duplicate"]


def test_mut_return_original_is_detached_plain_dict() -> None:
    sources = ({"safe": Decimal("1")}, UserDict({"safe": Decimal("1")}))
    for source in sources:
        actual = snapshot_mapping(source, invalid_key_error=_INVALID)
        source["safe"] = Decimal("999")
        assert type(actual) is dict
        assert actual == {"safe": Decimal("1")}


_EXCEPTIONS = (_RaisedRuntime("runtime"), _RaisedKey("key"), _RaisedValue("value"))


@pytest.mark.parametrize("operation", ["iterator", "getitem"])
@pytest.mark.parametrize("error", _EXCEPTIONS, ids=lambda error: type(error).__name__)
def test_mut_translate_and_suppress_preserve_direct_exception_identity(
    error: Exception, operation: str
) -> None:
    with pytest.raises(type(error)) as exc_info:
        snapshot_mapping(
            _FailingMapping(error, operation),
            invalid_key_error=_INVALID,
        )
    assert exc_info.value is error


@pytest.mark.parametrize("operation", ["iterator", "getitem"])
@pytest.mark.parametrize("error", _EXCEPTIONS, ids=lambda error: type(error).__name__)
def test_mut_translate_and_suppress_preserve_real_pydantic_cause(
    error: Exception, operation: str
) -> None:
    failing = _FailingMapping(error, operation)
    if isinstance(error, ValueError):
        with pytest.raises(ValidationError) as exc_info:
            FinancialOutcome.model_validate(failing)
        context = exc_info.value.errors(include_url=False)[0].get("ctx")
        assert context is not None
        assert context["error"] is error
    else:
        with pytest.raises(type(error)) as exc_info:
            FinancialOutcome.model_validate(failing)
        assert exc_info.value is error


_OWNER_CASES: tuple[tuple[type[BaseModel], dict[str, object]], ...] = (
    (SignalObservation, _signal_payload()),
    (OrderObservation, _order_payload()),
    (FillObservation, _fill_payload()),
    (
        FinancialOutcome,
        dict(
            fees=Decimal("1"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("2"),
            equity=Decimal("1002"),
        ),
    ),
    (JournalObservation, _journal_payload()),
    (TradingOutcome, _outcome_payload()),
    (TradingParityRun, _parity_payload()),
)


def _validate_owner(
    owner: type[BaseModel],
    value: Mapping[object, object] | dict[str, object],
    strict: bool | None,
    extra: Literal["forbid", "allow", "ignore"] | None,
) -> BaseModel:
    if strict is None and extra is None:
        return owner.model_validate(value)
    if strict is None:
        return owner.model_validate(value, extra=extra)
    if extra is None:
        return owner.model_validate(value, strict=strict)
    return owner.model_validate(value, strict=strict, extra=extra)


@pytest.mark.parametrize("extra", [None, "forbid", "allow", "ignore"])
@pytest.mark.parametrize("strict", [None, True, False])
@pytest.mark.parametrize(
    "owner,payload", _OWNER_CASES, ids=[case[0].__name__ for case in _OWNER_CASES]
)
def test_mut_lose_normalized_cause_owner_matrix_rejects_spoofed_key_before_lookup(
    owner: type[BaseModel],
    payload: dict[str, object],
    strict: bool | None,
    extra: Literal["forbid", "allow", "ignore"] | None,
) -> None:
    key = _TrapKey()
    mapping = _StreamMapping(((*payload, key),), (*payload.values(), "hidden"))
    with pytest.raises(ValidationError) as exc_info:
        _validate_owner(owner, mapping, strict, extra)
    error = exc_info.value.errors(include_url=False)[0]
    expected_error = (
        "parity run field names must be exact strings"
        if owner is TradingParityRun
        else _INVALID
    )
    context = error.get("ctx")
    assert context is not None
    assert error["loc"] == ()
    assert context["error"].args == (expected_error,)
    assert mapping.lookups == []


@pytest.mark.parametrize("extra", [None, "forbid", "allow", "ignore"])
@pytest.mark.parametrize("strict", [None, True, False])
@pytest.mark.parametrize(
    "owner,payload", _OWNER_CASES, ids=[case[0].__name__ for case in _OWNER_CASES]
)
def test_legal_stateful_mapping_owner_matrix_observes_first_stream_once(
    owner: type[BaseModel],
    payload: dict[str, object],
    strict: bool | None,
    extra: Literal["forbid", "allow", "ignore"] | None,
) -> None:
    second_key, second_value = "__second_stream_key__", "__second_stream_value__"
    mapping = _StreamMapping(
        (tuple(payload), (second_key,)),
        (*payload.values(), second_value),
    )
    actual = _validate_owner(owner, mapping, strict, extra)
    expected = _validate_owner(owner, payload, strict, extra)
    semantic = actual.model_dump(exclude_computed_fields=True)
    assert semantic == expected.model_dump(exclude_computed_fields=True)
    assert actual.model_extra == ({} if extra == "allow" else None)
    assert mapping.iterations == 1
    assert mapping.lookups == list(payload)
    assert second_key not in repr(semantic)
    assert second_value not in repr(semantic)


def test_representative_outcome_and_parity_identity_controls() -> None:
    outcome = _outcome()
    run = TradingParityRun.model_validate(_parity_payload(outcome))
    assert (
        outcome.sha256()
        == "f83da50e27173381f574908321be590c123ea6c84be8cc6685e4dc3e5f9c493d"
    )
    assert (
        run.sha256()
        == "173b4491bab684564a4c857d2d73c621b6d7f0eb3bb753dddb972df18b320b0b"
    )
    mutated = _outcome().model_copy(
        update={
            "endpoint_state": ReplayEndpointState(
                positions=(),
                working_orders=(),
                final_mark=Decimal("101"),
                end_timestamp=3,
                halted_early=False,
            )
        }
    )
    mutated_run = TradingParityRun.model_validate(_parity_payload(mutated))
    assert mutated.canonical_bytes() != outcome.canonical_bytes()
    assert mutated.sha256() != outcome.sha256()
    assert mutated_run.canonical_bytes() != run.canonical_bytes()
    assert mutated_run.sha256() != run.sha256()
