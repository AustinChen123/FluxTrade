from dataclasses import replace
from unittest.mock import MagicMock

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
