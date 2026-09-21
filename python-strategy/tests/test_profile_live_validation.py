from dataclasses import FrozenInstanceError, replace
from typing import Callable, cast
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import live_validation as owner
from test_profile_live_query import setup


def fixture(monkeypatch: pytest.MonkeyPatch):
    request, provider, _, context = setup(monkeypatch)
    result = owner.query_live_profile(provider, request, context)
    assert isinstance(result, owner.LiveProfileQueryResult)
    query = Mock(return_value=result)
    monkeypatch.setattr(owner, "query_live_profile", query)
    return request, provider, result, query, context.decision_time_ms


@pytest.mark.parametrize(
    "utc,mono",
    [
        (0, 0),
        (1, 2),
        (2, 1),
        (123, 123),
        (299999, 1),
        (1, 299999),
        (300000, 0),
        (0, 300000),
        (300001, 0),
    ],
)
def test_elapsed_boundary_and_order(
    monkeypatch: pytest.MonkeyPatch, utc: int, mono: int
) -> None:
    request, provider, original, query, start = fixture(monkeypatch)
    order = []
    utc_values, mono_values = iter((start, start + utc)), iter((10, 10 + mono))

    def clock(label: str, values):
        order.append(label)
        return next(values)

    def invoke(*args):
        order.append("query")
        return original

    query.side_effect = invoke
    result = owner.validate_live_profile(
        provider,
        request,
        utc_ms=lambda: clock("utc", utc_values),
        monotonic_ms=lambda: clock("mono", mono_values),
    )
    assert order == ["utc", "mono", "query", "mono", "utc"]
    query.assert_called_once_with(provider, request, owner.LiveSelectionContext(start))
    if max(utc, mono) >= 300000:
        assert result == owner.LiveProfileValidationUnavailable("VALIDATION_EXPIRED")
    else:
        assert isinstance(result, owner.ValidatedLiveProfileQuery)
        assert result.query is original
        assert result.validation_elapsed_ms == max(utc, mono)
        assert (
            result.validation_started_at_ms == start
            and result.validation_completed_at_ms == start + utc
        )
        assert (
            result.validation_max_age_ms == 300000
            and result.validation_expires_at_ms == start + utc + 300000 - max(utc, mono)
            and result.validation_expires_at_ms - result.validation_completed_at_ms
            == 300000 - max(utc, mono)
            and result.validation_expires_at_ms <= start + 300000
        )
        assert not hasattr(result, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(result, "validation_started_at_ms", 0)


@pytest.mark.parametrize(
    "value", [True, -1, 1 << 63, type("Integer", (int,), {})(1), RuntimeError("SECRET")]
)
@pytest.mark.parametrize("position", range(4))
def test_clock_failures(
    monkeypatch: pytest.MonkeyPatch, value: object, position: int
) -> None:
    request, provider, _, query, start = fixture(monkeypatch)
    values: list[object] = [start, 10, 11, start + 1]
    values[position] = value
    iterator = iter(values)

    def clock():
        item = next(iterator)
        if isinstance(item, Exception):
            raise item
        return item

    result = owner.validate_live_profile(
        provider,
        request,
        utc_ms=cast(Callable[[], int], clock),
        monotonic_ms=cast(Callable[[], int], clock),
    )
    assert result == owner.LiveProfileValidationUnavailable("CLOCK_UNCERTAIN")
    assert query.call_count == (0 if position < 2 else 1)


@pytest.mark.parametrize("utc_end,mono_end", [(0, 11), (172800001, 9)])
def test_backward_clocks(
    monkeypatch: pytest.MonkeyPatch, utc_end: int, mono_end: int
) -> None:
    request, provider, _, _, start = fixture(monkeypatch)
    result = owner.validate_live_profile(
        provider,
        request,
        utc_ms=Mock(side_effect=[start, utc_end]),
        monotonic_ms=Mock(side_effect=[10, mono_end]),
    )
    assert result == owner.LiveProfileValidationUnavailable("CLOCK_UNCERTAIN")


def test_overflow_and_base_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, _, query, _ = fixture(monkeypatch)
    assert owner.validate_live_profile(
        provider, request, utc_ms=lambda: (1 << 63) - 1, monotonic_ms=lambda: 0
    ) == owner.LiveProfileValidationUnavailable("CLOCK_UNCERTAIN")
    query.assert_not_called()
    error = KeyboardInterrupt("SECRET")
    with pytest.raises(KeyboardInterrupt) as caught:
        owner.validate_live_profile(
            provider, request, utc_ms=Mock(side_effect=error), monotonic_ms=lambda: 0
        )
    assert caught.value is error


def test_unavailable_and_provider_exception_no_end_clocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request, provider, _, query, start = fixture(monkeypatch)
    unavailable = owner.LiveProfileQueryUnavailable("NOT_READY")
    query.return_value = unavailable
    utc, mono = Mock(side_effect=[start]), Mock(side_effect=[0])
    assert (
        owner.validate_live_profile(provider, request, utc_ms=utc, monotonic_ms=mono)
        is unavailable
    )
    assert utc.call_count == mono.call_count == 1
    error = RuntimeError("SECRET")
    query.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        owner.validate_live_profile(
            provider, request, utc_ms=lambda: start, monotonic_ms=lambda: 0
        )
    assert caught.value is error


def test_wrong_decision_is_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, original, query, start = fixture(monkeypatch)
    query.return_value = replace(
        original, selection=replace(original.selection, decision_time_ms=start + 1)
    )
    with pytest.raises(owner.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        owner.validate_live_profile(
            provider, request, utc_ms=lambda: start, monotonic_ms=lambda: 0
        )


def test_success_evidence_exact_constructor(monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, original, _, start = fixture(monkeypatch)
    constructor = cast(Callable[..., object], owner.ValidatedLiveProfileQuery)
    exact = owner.ValidatedLiveProfileQuery(original, start, start, 0)
    assert exact.validation_expires_at_ms == start + 300000
    conservative = owner.ValidatedLiveProfileQuery(original, start, start + 1, 2)
    assert conservative.validation_elapsed_ms == 2
    derived = type("QuerySubclass", (owner.LiveProfileQueryResult,), {})(
        original.selection, original.profile
    )
    wrong_decision = replace(
        original, selection=replace(original.selection, decision_time_ms=start + 1)
    )
    invalid = (
        (True, start, start, 0),
        (derived, start, start, 0),
        (wrong_decision, start, start, 0),
        (original, True, start, 0),
        (original, -1, start, 0),
        (original, start, start - 1, 0),
        (original, start, start + 2, 1),
        (original, start, start, 300000),
        (original, start, start, type("Integer", (int,), {})(0)),
    )
    for values in invalid:
        with pytest.raises(owner.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
            constructor(*values)
    with pytest.raises(owner.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        replace(exact, validation_elapsed_ms=300000)
    monkeypatch.setattr(owner, "_MAX", start + 299999)
    with pytest.raises(owner.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        constructor(original, start, start, 0)


def test_unavailable_exact_constructor() -> None:
    constructor = cast(Callable[..., object], owner.LiveProfileValidationUnavailable)
    for value in (True, "SECRET", type("Text", (str,), {})("CLOCK_UNCERTAIN")):
        with pytest.raises(ValueError, match="^invalid validation reason$"):
            constructor(value)
    for reason in ("CLOCK_UNCERTAIN", "VALIDATION_EXPIRED"):
        assert owner.LiveProfileValidationUnavailable(reason).reason == reason
