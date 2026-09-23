from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, Inexact, Rounded, localcontext
import hashlib
import json
from typing import Any, cast

import pytest

from src.core.market_data.profiles import bootstrap_seed as owner
from src.core.market_data.profiles.decision_application import MarketDataDecisionOutcome
from src.core.market_data.profiles.decision_input import MarketDataDecisionInput
from src.core.market_data.profiles.decision_context import (
    StrategyMarketDataContext,
    ProfileDecisionStatus,
)
from test_profile_modeled_input import modeled, POLICY, DAY

MINUTE = 60000

KEY_GOLDEN = (
    b'{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    b'"contract_version":1,"environment":"live","execution_scope_id":"deployment",'
    b'"product_id":"BINANCE:BTCUSDT-PERP","schema_version":1,"strategy_id":"s",'
    b'"strategy_version":"v1","timeframe":"1m"}'
)
SEED_GOLDEN = (
    b'{"availability_policy_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
    b'"availability_policy_id":"utc_daily_delay_20m_v1","candles":[{"candle":'
    b'{"bar_start_ms":86400000,"close":"10","high":"11","low":"9","open":"10","volume":"0.1"},'
    b'"modeled_evidence":{"context":{"decision_time_ms":86460000,"profiles":'
    b'[{"available_at_ms":null,"basis":"MODELED","decision_time_ms":86460000,"observed_at_ms":null,'
    b'"profile":null,"reason":"PROFILE_NOT_READY","request":{"algorithm_version":"vp-v1",'
    b'"as_of_ms":86460000,"availability_policy_id":"utc_daily_delay_20m_v1",'
    b'"base_grid_id":"btc_spot_usdt_10_v1","end_ms":86400000,"freshness_policy_id":null,'
    b'"output_grid_id":"btc_spot_usdt_10_v1","pinned_manifest":null,"product_id":"BINANCE:BTCUSDT-SPOT",'
    b'"purpose":"MODELED_RESEARCH","revision":null,"start_ms":0},"schema_version":2,'
    b'"status":"MISSING","validation_checked_at_ms":null}],"schema_version":1},'
    b'"evidence_kind":"BOOTSTRAP_MODELED","schema_version":1}}],"contract_version":1,'
    b'"cutover_ms":86460000,"dataset_digest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
    b'"key":'
    + KEY_GOLDEN
    + b',"lookback":1,"requirements":[{"algorithm_version":"vp-v1",'
    b'"base_grid_id":"btc_spot_usdt_10_v1","freshness_policy_id":"utc_complete_strict_v1",'
    b'"output_grid_id":"btc_spot_usdt_10_v1","product_id":"BINANCE:BTCUSDT-SPOT",'
    b'"schema_version":1,"window_days":1}],"schema_version":1}'
)


def test_bootstrap_literal_identity_and_distinct_modeled_envelope():
    value = seed(ProfileDecisionStatus.MISSING, count=1)
    assert value.key.canonical_bytes == KEY_GOLDEN
    assert (
        value.key.seed_id
        == "98c6993e17700b8c74371ab2320a3de0e40c2a391a8685511d54bc5f3b4df8f1"
    )
    assert value.canonical_bytes == SEED_GOLDEN
    assert (
        value.digest
        == "1da9ccb2be436b6a818b6d04031978fbf22b453f38629b9cc77bb847d2d4c55c"
    )
    evidence = json.loads(value.canonical_bytes)["candles"][0]["modeled_evidence"]
    assert "key" not in evidence and "input_id" not in evidence
    with pytest.raises(ValueError, match="MARKET_DATA_INPUT_INVALID"):
        MarketDataDecisionInput.from_canonical_bytes(owner._bytes(evidence))
    for version in (True, 0, 2, "1"):
        with pytest.raises(owner.BootstrapSeedError):
            replace(value.key, contract_version=cast(Any, version))


@pytest.mark.parametrize("orphan_present", [False, True])
def test_terminal_skipped_null_reference_ignores_orphan_pin(orphan_present):
    value = seed()
    skipped, _ = suffix(value, skipped=True)
    applied, pinned = suffix(value)
    assert (
        classify(
            value,
            completed_recorded_through_ms=value.cutover_ms,
            recorded=((skipped, pinned if orphan_present else None),),
        )
        is owner.BootstrapDisposition.REPLAY
    )
    if not orphan_present:
        with pytest.raises(owner.BootstrapSeedError):
            classify(
                value,
                completed_recorded_through_ms=value.cutover_ms,
                recorded=((applied, None),),
            )


def test_modeled_evidence_resource_bounds_and_no_live_key(monkeypatch):
    value = seed(count=1)
    evidence = json.loads(value.canonical_bytes)["candles"][0]["modeled_evidence"]
    size = len(owner._bytes(evidence))
    assert owner.MAX_INPUT_BYTES == 8 * 1024 * 1024
    assert owner.MAX_INPUT_PROFILES == 32
    assert owner.MAX_INPUT_BINS_PER_PROFILE == 100_000

    def forbidden(*args, **kwargs):
        raise AssertionError("seed must not fabricate a live decision key")

    monkeypatch.setattr(owner.BootstrapKey, "decision_key", forbidden)
    monkeypatch.setattr(owner, "MAX_INPUT_BYTES", size)
    assert value.canonical_bytes
    monkeypatch.setattr(owner, "MAX_INPUT_BYTES", size - 1)
    with pytest.raises(ValueError):
        value.canonical_bytes
    monkeypatch.setattr(owner, "MAX_INPUT_BYTES", 8 * 1024 * 1024)
    monkeypatch.setattr(owner, "MAX_INPUT_BINS_PER_PROFILE", 0)
    with pytest.raises(owner.BootstrapSeedError):
        value.canonical_bytes
    monkeypatch.setattr(owner, "MAX_INPUT_BINS_PER_PROFILE", 100_000)
    monkeypatch.setattr(owner, "MAX_INPUT_PROFILES", 0)
    with pytest.raises(owner.BootstrapSeedError):
        value.canonical_bytes


def seed(status=ProfileDecisionStatus.FRESH, count=2, **changes):
    item, requirement = modeled(status)
    key = owner.BootstrapKey(
        "live", "deployment", "s", "v1", "a" * 64, "BINANCE:BTCUSDT-PERP", "1m"
    )
    candles = []
    for index in range(count):
        start = DAY + index * MINUTE
        decision = start + MINUTE
        entry = replace(
            item,
            decision_time_ms=decision,
            request=replace(item.request, as_of_ms=decision),
        )
        candles.append(
            owner.BootstrapCandle(
                start,
                Decimal("10.00"),
                Decimal(11),
                Decimal(9),
                Decimal(10),
                Decimal("0.10"),
                StrategyMarketDataContext(decision, (entry,)),
            )
        )
    return owner.BootstrapSeed(
        **cast(
            Any,
            (
                dict(
                    key=key,
                    requirements=(requirement,),
                    cutover_ms=DAY + count * MINUTE,
                    lookback=count,
                    availability_policy_id=POLICY,
                    availability_policy_digest="b" * 64,
                    dataset_digest="c" * 64,
                    candles=tuple(candles),
                    max_seed_candles=10,
                )
                | changes
            ),
        )
    )


def classify(value, **changes):
    return owner.classify_bootstrap(
        value.requirements,
        **cast(
            Any,
            (
                dict(
                    proposed=value,
                    stored=value,
                    history_known_absent=False,
                    completed_recorded_through_ms=None,
                    recorded=(),
                    max_seed_candles=10,
                    max_recorded_candles=10,
                )
                | changes
            ),
        ),
    )


def suffix(value, index=0, skipped=False):
    start = value.cutover_ms + index * MINUTE
    key = value.key.decision_key(start)
    if skipped:
        return MarketDataDecisionOutcome(
            key, "SKIPPED", None, None, "INPUT_STORE_FAILED"
        ), None
    item = value.candles[-1].context.profiles[0]
    decision = start + MINUTE
    item = replace(
        item,
        decision_time_ms=decision,
        request=replace(item.request, as_of_ms=decision),
    )
    pinned = MarketDataDecisionInput(
        key, value.requirements, decision, StrategyMarketDataContext(decision, (item,))
    )
    return MarketDataDecisionOutcome(
        key, "APPLIED", pinned.input_id, pinned.input_digest
    ), pinned


@pytest.mark.parametrize("status", list(ProfileDecisionStatus))
def test_seed_canonical_stateful_replay_and_decimal_stability(status):
    value = seed(status)
    raw = value.canonical_bytes
    assert (
        raw
        == json.dumps(
            json.loads(raw), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    )
    assert value.digest == hashlib.sha256(raw).hexdigest()
    assert value.key.seed_id == hashlib.sha256(value.key.canonical_bytes).hexdigest()
    assert json.loads(raw)["candles"][0]["candle"]["open"] == "10"
    with localcontext() as ctx:
        ctx.prec = 1
        ctx.traps[Inexact] = ctx.traps[Rounded] = True
        assert seed(status).canonical_bytes == raw
    assert replace(value, max_seed_candles=99).digest == value.digest
    assert (
        classify(value, stored=None, history_known_absent=True)
        is owner.BootstrapDisposition.BOOTSTRAP
    )
    assert classify(value) is owner.BootstrapDisposition.REPLAY
    rows = (suffix(value), suffix(value, 1, True))
    assert (
        classify(
            value,
            recorded=rows,
            completed_recorded_through_ms=value.cutover_ms + MINUTE,
        )
        is owner.BootstrapDisposition.REPLAY
    )

    # A deterministic stateful consumer gets the same seed inputs, regardless of external latest/clock.
    def state(rows):
        return tuple((row.close, row.context.digest) for row in rows)

    before = state(value.candles)
    latest, clock, invalidated = seed(ProfileDecisionStatus.INVALID), 0, True
    assert latest and clock == 0 and invalidated
    assert state(value.candles) == before and value.canonical_bytes == raw
    with pytest.raises(FrozenInstanceError):
        setattr(value, "cutover_ms", 0)
    assert not hasattr(value, "__dict__")


def test_empty_lookback_legacy_and_unknown_history():
    value = seed(count=0)
    assert value.candles == () and classify(value) is owner.BootstrapDisposition.REPLAY
    assert (
        owner.classify_bootstrap(
            (),
            proposed=None,
            stored=None,
            history_known_absent=False,
            completed_recorded_through_ms=None,
            recorded=(),
            max_seed_candles=1,
            max_recorded_candles=1,
        )
        is owner.BootstrapDisposition.LEGACY
    )
    with pytest.raises(owner.BootstrapSeedError):
        classify(value, stored=None)
    with pytest.raises(owner.BootstrapSeedError):
        classify(
            value,
            stored=None,
            history_known_absent=True,
            completed_recorded_through_ms=value.cutover_ms,
        )


@pytest.mark.parametrize(
    "change",
    ["cutover", "strategy", "config", "requirements", "candle", "policy", "dataset"],
)
def test_same_key_content_drift_conflicts(change):
    value = seed()
    if change == "cutover":
        other = seed(count=1)
        assert other.key.seed_id == value.key.seed_id
    elif change in ("strategy", "config"):
        key = replace(
            value.key,
            **(
                {"strategy_version": "v2"}
                if change == "strategy"
                else {"config_hash": "d" * 64}
            ),
        )
        other = replace(value, key=key, max_seed_candles=10)
    elif change == "requirements":
        # Empty-lookback remains a valid admission fixture for another grid intent.
        value = seed(count=0)
        other = replace(
            value,
            requirements=(replace(value.requirements[0], output_grid_id="other"),),
            max_seed_candles=10,
        )
    elif change == "candle":
        other = replace(
            value,
            candles=(replace(value.candles[0], volume=Decimal(2)), value.candles[1]),
            max_seed_candles=10,
        )
    else:
        other = replace(
            value,
            **{
                (
                    "dataset_digest"
                    if change == "dataset"
                    else "availability_policy_digest"
                ): "d" * 64
            },
            max_seed_candles=10,
        )
    assert other.digest != value.digest
    with pytest.raises(owner.BootstrapSeedError, match="PROFILE_BOOTSTRAP_CONFLICT"):
        classify(other, stored=value)


@pytest.mark.parametrize(
    "damage",
    ["missing", "duplicate", "reverse", "earlier", "later", "input", "requirements"],
)
def test_suffix_exact_coverage_no_modeled_fallback(damage):
    value = seed()
    first, second = suffix(value), suffix(value, 1)
    rows = (first, second)
    if damage == "missing":
        rows = (first,)
    elif damage == "duplicate":
        rows = (first, first)
    elif damage == "reverse":
        rows = (second, first)
    elif damage == "earlier":
        rows = (suffix(value, -1), first)
    elif damage == "later":
        rows = (second, suffix(value, 2))
    elif damage == "input":
        rows = ((first[0], None), second)
    else:
        pinned = first[1]
        assert pinned is not None
        other = replace(
            pinned,
            requirements=(),
            context=StrategyMarketDataContext(pinned.decision_time_ms, ()),
        )
        rows = ((replace(first[0], input_digest=other.input_digest), other), second)
    with pytest.raises(owner.BootstrapSeedError):
        classify(
            value,
            recorded=rows,
            completed_recorded_through_ms=value.cutover_ms + MINUTE,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("cutover_ms", True),
        ("lookback", -1),
        ("max_seed_candles", 0),
        ("max_seed_candles", True),
        ("candles", []),
        ("requirements", []),
        ("dataset_digest", "SECRET"),
        ("cutover_ms", DAY + 1),
        ("cutover_ms", 2**63),
    ],
)
def test_constructor_domains(field, value):
    with pytest.raises(owner.BootstrapSeedError) as caught:
        seed(**{field: value})
    assert (
        str(caught.value) == "PROFILE_BOOTSTRAP_INVALID"
        and caught.value.__cause__ is None
    )


def test_contiguous_seed_and_resource_admission(monkeypatch):
    value = seed()
    for candles in (
        value.candles[::-1],
        (value.candles[0], value.candles[0]),
        value.candles[:1],
    ):
        with pytest.raises(owner.BootstrapSeedError):
            replace(value, candles=candles, max_seed_candles=10)
    assert replace(value, max_seed_candles=2)
    with pytest.raises(owner.BootstrapSeedError):
        replace(value, max_seed_candles=1)
    assert owner.MAX_BOOTSTRAP_BYTES == 32 * 1024 * 1024
    length = len(value.canonical_bytes)
    monkeypatch.setattr(owner, "MAX_BOOTSTRAP_BYTES", length)
    assert seed().canonical_bytes == value.canonical_bytes
    monkeypatch.setattr(owner, "MAX_BOOTSTRAP_BYTES", length - 1)
    with pytest.raises(owner.BootstrapSeedError):
        seed()


@pytest.mark.parametrize(
    "changes",
    [
        {"max_recorded_candles": 0},
        {"max_seed_candles": True},
        {"recorded": []},
        {"history_known_absent": 1},
        {"completed_recorded_through_ms": True},
        {"completed_recorded_through_ms": DAY},
        {"completed_recorded_through_ms": DAY + 1},
    ],
)
def test_classifier_exact_domains(changes):
    with pytest.raises(owner.BootstrapSeedError):
        classify(seed(), **cast(Any, changes))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("open", True),
        ("volume", Decimal(-1)),
        ("high", Decimal(8)),
        ("low", Decimal(12)),
        ("close", Decimal("NaN")),
        ("volume", Decimal("1e-29")),
        ("bar_start_ms", True),
        ("context", None),
    ],
)
def test_candle_domain_and_exact_nested_types(field, bad):
    with pytest.raises(owner.BootstrapSeedError):
        replace(seed().candles[0], **{field: bad})


def test_subclasses_context_time_policy_and_recorded_limit():
    class KeySubclass(owner.BootstrapKey):
        pass

    class IntSubclass(int):
        pass

    value = seed()
    key = KeySubclass(
        **{name: getattr(value.key, name) for name in value.key.__dataclass_fields__}
    )
    with pytest.raises(owner.BootstrapSeedError):
        replace(value, key=key, max_seed_candles=10)
    with pytest.raises(owner.BootstrapSeedError):
        replace(value, max_seed_candles=IntSubclass(10))
    candle = value.candles[0]
    entry = candle.context.profiles[0]
    changed = replace(
        entry, request=replace(entry.request, availability_policy_id="other")
    )
    wrong = replace(
        candle,
        context=StrategyMarketDataContext(candle.context.decision_time_ms, (changed,)),
    )
    with pytest.raises(owner.BootstrapSeedError):
        replace(value, candles=(wrong, value.candles[1]), max_seed_candles=10)
    next_candle = replace(candle, context=value.candles[1].context)
    with pytest.raises(owner.BootstrapSeedError):
        replace(value, candles=(next_candle, value.candles[1]), max_seed_candles=10)
    rows = (suffix(value), suffix(value, 1))
    assert (
        classify(
            value,
            recorded=rows,
            completed_recorded_through_ms=value.cutover_ms + MINUTE,
            max_recorded_candles=2,
        )
        is owner.BootstrapDisposition.REPLAY
    )
    with pytest.raises(owner.BootstrapSeedError):
        classify(
            value,
            recorded=rows,
            completed_recorded_through_ms=value.cutover_ms + MINUTE,
            max_recorded_candles=1,
        )


def test_profile_manifest_and_unavailable_evidence_are_bound():
    value = seed()
    unavailable = seed(ProfileDecisionStatus.MISSING)
    assert unavailable.digest != value.digest
    candle = value.candles[0]
    item = candle.context.profiles[0]
    assert item.profile is not None
    profile = item.profile
    manifest = replace(
        profile.manifest, days=(replace(profile.manifest.days[0], revision=2),)
    )
    changed = replace(item, profile=replace(profile, manifest=manifest))
    revised = replace(
        candle,
        context=StrategyMarketDataContext(candle.context.decision_time_ms, (changed,)),
    )
    other = replace(value, candles=(revised, value.candles[1]), max_seed_candles=10)
    assert other.digest != value.digest
    with pytest.raises(owner.BootstrapSeedError, match="CONFLICT"):
        classify(other, stored=value)
