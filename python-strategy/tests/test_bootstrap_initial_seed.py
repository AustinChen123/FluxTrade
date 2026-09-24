from dataclasses import replace
from contextlib import contextmanager
from unittest.mock import MagicMock
from typing import Any, cast

import pytest

from src.core import bootstrap_hydration_reader as owner
from src.core.bootstrap_hydration_reader import (
    BootstrapHistoryEvidence as Proof,
    BootstrapHydrationReaderError,
)
from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedRecord,
    BootstrapSeedPinResult,
    BootstrapSeedPinStatus as Status,
)
from test_bootstrap_hydration_reader import harness
from test_profile_recorded_decision_owner import NOW
from test_profile_context_enrichment import B


def setup():
    reader, plan, strategy, store, application, inputs, events = harness(count=0)
    store.get.return_value = None
    history = MagicMock(
        return_value=Proof(plan.seed.key, plan.seed.cutover_ms, "ABSENT")
    )
    factory = MagicMock(return_value=plan.seed)
    store.pin_confirmed.return_value = BootstrapSeedPinResult(
        Status.CONFIRMED, BootstrapSeedRecord(plan.seed, NOW, False)
    )

    def call(boundary=None):
        return reader.prepare_initial_seed(
            strategy,
            plan.seed.cutover_ms if boundary is None else boundary,
            history_reader=history,
            factory=factory,
        )

    return call, plan.seed, store, history, factory, application, inputs, events


def test_existing_seed_is_original_record_without_history_factory_or_pin():
    call, seed, store, history, factory, application, inputs, events = setup()
    record = BootstrapSeedRecord(seed, NOW, True)
    store.get.return_value = record
    history.side_effect = factory.side_effect = AssertionError("must remain lazy")
    assert call(seed.cutover_ms + 60000) is record
    history.assert_not_called()
    factory.assert_not_called()
    store.pin_confirmed.assert_not_called()
    assert not application.mock_calls and not inputs.mock_calls and events == []


@pytest.mark.parametrize("already_present", [False, True])
def test_absent_order_classifier_and_exact_confirmed_identity(
    already_present, monkeypatch
):
    call, seed, store, history, factory, application, inputs, events = setup()
    classifier = MagicMock(wraps=owner.classify_bootstrap)
    monkeypatch.setattr(owner, "classify_bootstrap", classifier)
    sequence = MagicMock()
    for name, mock in (
        ("lookup", store.get),
        ("history", history),
        ("factory", factory),
        ("classifier", classifier),
        ("pin", store.pin_confirmed),
    ):
        sequence.attach_mock(mock, name)
    record = BootstrapSeedRecord(seed, NOW, already_present)
    store.pin_confirmed.return_value = BootstrapSeedPinResult(Status.CONFIRMED, record)
    assert call() is record
    assert [entry[0] for entry in sequence.mock_calls] == [
        "lookup",
        "history",
        "factory",
        "classifier",
        "pin",
    ]
    history.assert_called_once_with(seed.key, seed.cutover_ms)
    factory.assert_called_once_with(seed.key)
    classifier.assert_called_once_with(
        seed.requirements,
        proposed=seed,
        stored=None,
        history_known_absent=True,
        completed_recorded_through_ms=None,
        recorded=(),
        max_seed_candles=10,
        max_recorded_candles=3,
    )
    store.pin_confirmed.assert_called_once_with(seed)
    assert all(c.context.profiles[0].basis is B.MODELED for c in record.value.candles)
    assert not application.mock_calls and not inputs.mock_calls and events == []


def test_nonbootstrap_classifier_result_never_pins(monkeypatch):
    call, seed, store, history, factory, *_ = setup()
    classifier = MagicMock(return_value=owner.BootstrapDisposition.REPLAY)
    monkeypatch.setattr(owner, "classify_bootstrap", classifier)
    with pytest.raises(BootstrapHydrationReaderError):
        call()
    history.assert_called_once()
    factory.assert_called_once_with(seed.key)
    classifier.assert_called_once()
    store.pin_confirmed.assert_not_called()


@pytest.mark.parametrize("kind", ["PRESENT", "UNKNOWN", "key", "boundary", "type"])
def test_untrusted_or_nonabsent_proof_never_calls_factory(kind):
    call, seed, store, history, factory, *_ = setup()
    proof = Proof(seed.key, seed.cutover_ms, "ABSENT")
    history.return_value = (
        {
            "PRESENT": replace(proof, state="PRESENT"),
            "UNKNOWN": replace(proof, state="UNKNOWN"),
            "key": replace(proof, key=replace(seed.key, strategy_id="other")),
            "boundary": replace(proof, boundary_bar_start_ms=seed.cutover_ms + 60000),
            "type": None,
        }
    )[kind]
    with pytest.raises(BootstrapHydrationReaderError):
        call()
    factory.assert_not_called()
    store.pin_confirmed.assert_not_called()


@pytest.mark.parametrize("kind", ["FAILED", "UNCONFIRMED", "type", "content"])
def test_pin_not_exactly_confirmed_rejects(kind):
    call, seed, store, *_ = setup()
    other = replace(seed, dataset_digest="f" * 64, max_seed_candles=10)
    store.pin_confirmed.return_value = (
        {
            "FAILED": BootstrapSeedPinResult(Status.FAILED),
            "UNCONFIRMED": BootstrapSeedPinResult(Status.UNCONFIRMED),
            "type": None,
            "content": BootstrapSeedPinResult(
                Status.CONFIRMED, BootstrapSeedRecord(other, NOW, True)
            ),
        }
    )[kind]
    with pytest.raises(BootstrapHydrationReaderError):
        call()
    store.pin_confirmed.assert_called_once()


def admission_setup(monkeypatch, mode="ABSENT"):
    reader, plan, strategy, store, application, inputs, sessions = harness(count=0)
    resolver = MagicMock(wraps=reader._identity)
    reader._identity = resolver
    events, active = [], []
    handle, factory = MagicMock(), MagicMock()
    record = BootstrapSeedRecord(plan.seed, NOW, mode == "existing")

    @contextmanager
    def admission(key):
        assert key == plan.seed.key
        events.append("acquire")
        if mode == "busy":
            raise RuntimeError("busy")
        active.append(True)
        try:
            yield handle
        except BaseException:
            events.append("body_error")
            raise
        finally:
            active.clear()
            events.append("release")

    def phase(name, value):
        def call(*args, **kwargs):
            assert active == [True]
            events.append(name)
            return value

        return call

    store.initial_admission.side_effect = admission
    store.get.side_effect = phase("lookup", record if mode == "existing" else None)
    handle.read_history.side_effect = phase(
        "history",
        Proof(
            plan.seed.key,
            plan.seed.cutover_ms,
            mode if mode in ("PRESENT", "UNKNOWN") else "ABSENT",
        ),
    )
    factory.side_effect = phase("factory", plan.seed)
    classifier = MagicMock(
        side_effect=phase("classifier", owner.BootstrapDisposition.BOOTSTRAP)
    )
    monkeypatch.setattr(owner, "classify_bootstrap", classifier)
    status = Status[mode] if mode in ("FAILED", "UNCONFIRMED") else Status.CONFIRMED
    winner = (
        replace(plan.seed, dataset_digest="f" * 64, max_seed_candles=10)
        if mode == "wrong"
        else plan.seed
    )
    store.pin_confirmed.side_effect = phase(
        "pin",
        BootstrapSeedPinResult(
            status,
            BootstrapSeedRecord(winner, NOW, False)
            if status is Status.CONFIRMED
            else None,
        ),
    )
    return (
        reader,
        plan,
        strategy,
        store,
        handle,
        factory,
        classifier,
        resolver,
        events,
        active,
        record,
    )


@pytest.mark.parametrize(
    "mode",
    [
        "existing",
        "ABSENT",
        "PRESENT",
        "UNKNOWN",
        "busy",
        "FAILED",
        "UNCONFIRMED",
        "wrong",
    ],
)
def test_authority_composition_order_and_terminal_matrix(monkeypatch, mode):
    (
        reader,
        plan,
        strategy,
        store,
        handle,
        factory,
        classifier,
        resolver,
        events,
        active,
        record,
    ) = admission_setup(monkeypatch, mode)

    def call():
        return reader.prepare_initial_seed_under_admission(
            strategy,
            plan.seed.cutover_ms + (60000 if mode == "existing" else 0),
            factory=factory,
        )

    if mode in ("existing", "ABSENT"):
        result = call()
        assert result.value is plan.seed
        if mode == "existing":
            assert result is record
    else:
        with pytest.raises(
            RuntimeError if mode == "busy" else BootstrapHydrationReaderError
        ):
            call()
    resolver.assert_called_once_with(strategy)
    assert active == []
    expected = ["acquire"]
    if mode != "busy":
        expected += ["lookup"]
        if mode != "existing":
            expected += ["history"]
            if mode not in ("PRESENT", "UNKNOWN"):
                expected += ["factory", "classifier", "pin"]
        if mode not in ("existing", "ABSENT"):
            expected += ["body_error"]
        expected += ["release"]
    assert events == expected
    if "history" not in events:
        handle.read_history.assert_not_called()
    if "factory" not in events:
        factory.assert_not_called()
        classifier.assert_not_called()
        store.pin_confirmed.assert_not_called()
    if mode == "busy":
        store.get.assert_not_called()


class AdmissionHalt(BaseException):
    pass


@pytest.mark.parametrize(
    "phase", ["acquire", "lookup", "history", "factory", "classifier", "pin"]
)
@pytest.mark.parametrize("error_type", [RuntimeError, AdmissionHalt])
def test_authority_composition_unwinds_original_exception(
    monkeypatch, phase, error_type
):
    (
        reader,
        plan,
        strategy,
        store,
        handle,
        factory,
        classifier,
        resolver,
        events,
        active,
        _,
    ) = admission_setup(monkeypatch)
    error = error_type("SECRET")
    target = {
        "acquire": store.initial_admission,
        "lookup": store.get,
        "history": handle.read_history,
        "factory": factory,
        "classifier": classifier,
        "pin": store.pin_confirmed,
    }[phase]
    target.side_effect = error
    with pytest.raises(error_type) as caught:
        reader.prepare_initial_seed_under_admission(
            strategy, plan.seed.cutover_ms, factory=factory
        )
    assert caught.value is error and active == []
    if phase == "acquire":
        assert events == []
        store.get.assert_not_called()
        factory.assert_not_called()
        store.pin_confirmed.assert_not_called()
    else:
        assert events[-1] == "release"
    resolver.assert_called_once()


@pytest.mark.parametrize("bad", ["boundary", "strategy", "factory", "no_profile"])
def test_authority_invalid_input_before_admission(monkeypatch, bad):
    reader, plan, strategy, store, *_ = harness(count=0)
    if bad == "no_profile":
        requirements = replace(strategy.requirements, profile_requirements=())
        monkeypatch.setattr(
            type(strategy), "requirements", property(lambda _: requirements)
        )
    with pytest.raises(BootstrapHydrationReaderError):
        reader.prepare_initial_seed_under_admission(
            cast(Any, None) if bad == "strategy" else strategy,
            True if bad == "boundary" else plan.seed.cutover_ms,
            factory=None if bad == "factory" else MagicMock(),
        )
    assert not store.mock_calls
