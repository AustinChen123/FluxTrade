from dataclasses import replace
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.core.models import Candlestick
from src.strategies.base import StrategyRequirements
from src.core.market_data.profiles.modeled_input import (
    ModeledProfileInput,
    ModeledProfileInputError,
    build_modeled_bootstrap_seed,
)
from src.core.market_data.profiles.bootstrap_seed import BootstrapSeed
from test_profile_bootstrap_seed import seed
from test_profile_modeled_input import modeled
from test_profile_context_enrichment import S, Collection
from test_profile_context_enrichment import fixture
from test_bootstrap_initial_seed import admission_setup


def setup(status=S.MISSING):
    value = seed()
    contexts = [c.context for c in value.candles]
    item, _ = modeled(status)
    decision = value.cutover_ms
    item = replace(
        item,
        decision_time_ms=decision,
        request=replace(item.request, as_of_ms=decision),
    )
    contexts[1] = Collection(decision, (item,))
    provider = MagicMock(
        availability_policy_id=value.availability_policy_id,
        availability_policy_digest=value.availability_policy_digest,
        dataset_digest=value.dataset_digest,
    )
    provider.context_for.side_effect = contexts
    source = ModeledProfileInput(provider, value.availability_policy_id)
    requirements = StrategyRequirements(
        value.key.product_id, "1m", 2, profile_requirements=value.requirements
    )
    candles = tuple(
        Candlestick(
            product_id=value.key.product_id,
            timeframe="1m",
            timestamp=c.bar_start_ms,
            open=Decimal(10 + i),
            high=Decimal(11 + i),
            low=Decimal(9 + i),
            close=Decimal(10 + i),
            volume=Decimal("0.1") * (i + 1),
        )
        for i, c in enumerate(value.candles)
    )
    return (
        source,
        value.key,
        requirements,
        candles,
        value.cutover_ms,
        provider,
        contexts,
    )


def build(data, **changes):
    source, key, requirements, candles, cutover, *_ = data
    args: dict[str, Any] = (
        dict(
            modeled_input=source,
            key=key,
            requirements=requirements,
            candles=candles,
            cutover_ms=cutover,
            max_seed_candles=2,
        )
        | changes
    )
    return build_modeled_bootstrap_seed(**args)


@pytest.mark.parametrize("status", list(S))
def test_distinct_candles_resolve_in_order_canonical_roundtrip(status):
    data = setup(status)
    result = build(data)
    source, key, reqs, candles, cutover, provider, contexts = data
    assert result.key is key and result.cutover_ms == cutover
    assert [c.context for c in result.candles] == contexts
    assert [c.close for c in result.candles] == [Decimal(10), Decimal(11)]
    assert [c.volume for c in result.candles] == [Decimal("0.1"), Decimal("0.2")]
    assert [
        call.kwargs["decision_time_ms"] for call in provider.context_for.call_args_list
    ] == [c.timestamp + 60000 for c in candles]
    assert all(
        call.args == (reqs.profile_requirements,)
        for call in provider.context_for.call_args_list
    )
    assert (
        BootstrapSeed.from_canonical_bytes(result.canonical_bytes, max_seed_candles=2)
        == result
    )
    provider.context_for.side_effect = contexts
    assert build(data).canonical_bytes == result.canonical_bytes


@pytest.mark.parametrize(
    "damage",
    [
        "source",
        "key",
        "requirements",
        "list",
        "item",
        "product",
        "timeframe",
        "gap",
        "reverse",
        "cutover",
        "lookback",
        "max",
        "empty",
        "nan",
        "ohlc",
        "negative_volume",
        "overflow",
    ],
)
def test_full_preflight_including_late_invalid_has_zero_provider_calls(damage):
    data = setup()
    source, key, requirements, candles, cutover, provider, _ = data
    changes = {}
    if damage in ("source", "key", "requirements"):
        changes[{"source": "modeled_input"}.get(damage, damage)] = None
    elif damage == "list":
        changes["candles"] = list(candles)
    elif damage == "item":
        changes["candles"] = (candles[0], None)
    elif damage == "reverse":
        changes["candles"] = candles[::-1]
    elif damage == "cutover":
        changes["cutover_ms"] = cutover + 60000
    elif damage == "lookback":
        changes["requirements"] = replace(requirements, lookback_window=1)
    elif damage == "empty":
        changes["requirements"] = replace(requirements, profile_requirements=())
    elif damage == "max":
        changes["max_seed_candles"] = 1
    else:
        field, value = {
            "product": ("product_id", "BINANCE:ETHUSDT-SPOT"),
            "timeframe": ("timeframe", "5m"),
            "gap": ("timestamp", cutover),
            "nan": ("close", Decimal("NaN")),
            "ohlc": ("high", Decimal(1)),
            "negative_volume": ("volume", Decimal(-1)),
            "overflow": ("close", Decimal("79228162514264337593543950336")),
        }[damage]
        changes["candles"] = (candles[0], candles[1].model_copy(update={field: value}))
    with pytest.raises(ModeledProfileInputError):
        build(data, **changes)
    provider.context_for.assert_not_called()


@pytest.mark.parametrize("lookback", [True, -1])
def test_exact_lookback_domain_before_resolve(lookback):
    data = setup()
    requirements = replace(data[2], lookback_window=lookback)
    # True equals len=1: only exact-integer validation may reject this valid window.
    with pytest.raises(ModeledProfileInputError):
        build(data, requirements=requirements, candles=data[3][-1:])
    data[5].context_for.assert_not_called()


class IntSubclass(int):
    pass


@pytest.mark.parametrize("field", ["cutover_ms", "max_seed_candles"])
@pytest.mark.parametrize("value", [0, -1, True])
def test_cutover_and_limit_domain_before_resolve(field, value):
    data = setup()
    with pytest.raises(ModeledProfileInputError):
        build(data, **{field: value})
    data[5].context_for.assert_not_called()


def test_cutover_exact_type_and_zero_lookback_control():
    data = setup()
    with pytest.raises(ModeledProfileInputError):
        build(data, cutover_ms=IntSubclass(data[4]))
    result = build(data, requirements=replace(data[2], lookback_window=0), candles=())
    assert result.candles == () and result.lookback == 0
    assert (
        BootstrapSeed.from_canonical_bytes(result.canonical_bytes, max_seed_candles=2)
        == result
    )
    data[5].context_for.assert_not_called()


class Halt(BaseException):
    pass


@pytest.mark.parametrize("error", [RuntimeError("SECRET"), Halt("SECRET")])
def test_second_resolve_exception_identity_no_partial(error):
    data = setup()
    data[5].context_for.side_effect = [data[6][0], error]
    with pytest.raises(type(error)) as caught:
        build(data)
    assert caught.value is error and data[5].context_for.call_count == 2


def test_dataset_and_context_change_content_not_durable_key_or_window():
    original = build(setup(S.MISSING))
    changed = setup(S.MISSING)
    changed[5].dataset_digest = "f" * 64
    changed = (
        ModeledProfileInput(changed[5], changed[0].availability_policy_id),
        *changed[1:],
    )
    provenance = build(changed)
    content = build(setup(S.INVALID))
    assert provenance.candles == original.candles
    assert content.dataset_digest == original.dataset_digest
    assert content.candles[0] == original.candles[0]
    assert content.candles[1].context != original.candles[1].context
    for candidate in (provenance, content):
        assert candidate.key == original.key
        assert candidate.cutover_ms == original.cutover_ms
        assert candidate.canonical_bytes != original.canonical_bytes
        assert candidate.digest != original.digest


def test_live_evidence_rejected_by_builder_without_seed_return():
    data = setup()
    live, _ = fixture(status=S.MISSING)
    end = data[3][0].timestamp + 60000
    live = replace(live, decision_time_ms=end)
    data[5].context_for.side_effect = [Collection(end, (live,))]
    with pytest.raises(ModeledProfileInputError):
        build(data)
    data[5].context_for.assert_called_once()


def test_existing_admission_record_never_executes_real_builder_closure(monkeypatch):
    reader, plan, strategy, store, handle, _, classifier, _, events, _, record = (
        admission_setup(monkeypatch, "existing")
    )
    data = setup()
    factory = MagicMock(
        side_effect=lambda key: build_modeled_bootstrap_seed(
            data[0],
            key,
            strategy.requirements,
            data[3],
            cutover_ms=plan.seed.cutover_ms,
            max_seed_candles=10,
        )
    )
    assert (
        reader.prepare_initial_seed_under_admission(
            strategy, plan.seed.cutover_ms + 60000, factory=factory
        )
        is record
    )
    factory.assert_not_called()
    data[5].context_for.assert_not_called()
    handle.read_history.assert_not_called()
    classifier.assert_not_called()
    store.pin_confirmed.assert_not_called()
    assert events == ["acquire", "lookup", "release"]
