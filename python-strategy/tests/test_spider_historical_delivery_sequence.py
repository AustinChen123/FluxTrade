from copy import deepcopy
from dataclasses import replace
from typing import cast

import pytest

from src.core.backtest.spider_policy_protocol import (
    HistoricalDeliveryKind,
    HistoricalSequenceMode,
    historical_delivery_sequence,
)
from src.core.backtest import spider_run_evidence
from src.core.backtest.spider_historical_input import canonical_bytes, decode_canonical, validate_historical_input
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_bayes_replay import _valid_source14_run


@pytest.mark.parametrize("counter", [0, 1, (1 << 62) - 1])
def test_historical_delivery_sequence_modes_are_disjoint_only_for_new_mode(counter):
    assert historical_delivery_sequence(
        HistoricalSequenceMode.LEGACY_SHARED_SEQUENCE_V1,
        HistoricalDeliveryKind.POLL,
        counter,
    ) == counter
    assert historical_delivery_sequence(
        HistoricalSequenceMode.LEGACY_SHARED_SEQUENCE_V1,
        HistoricalDeliveryKind.NATIVE,
        counter,
    ) == counter
    assert historical_delivery_sequence(
        HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
        HistoricalDeliveryKind.POLL,
        counter,
    ) == 2 * counter
    assert historical_delivery_sequence(
        HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
        HistoricalDeliveryKind.NATIVE,
        counter,
    ) == 2 * counter + 1


@pytest.mark.parametrize("mode,kind,counter", [
    ("HISTORICAL_DISJOINT_SEQUENCE_V1", HistoricalDeliveryKind.POLL, 0),
    (HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1, "POLL", 0),
    (HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1, HistoricalDeliveryKind.POLL, True),
    (HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1, HistoricalDeliveryKind.POLL, -1),
    (HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1, HistoricalDeliveryKind.POLL, 1 << 62),
])
def test_historical_delivery_sequence_rejects_unknown_or_out_of_range_values(mode, kind, counter):
    with pytest.raises(ValueError, match="POLICY_EMISSION_MISMATCH"):
        historical_delivery_sequence(mode, kind, counter)


def test_historical_delivery_sequence_does_not_mutate_inputs():
    values = [HistoricalSequenceMode.HISTORICAL_DISJOINT_SEQUENCE_V1,
              HistoricalDeliveryKind.NATIVE, 17]
    before = deepcopy(values)
    assert historical_delivery_sequence(*values) == 35
    assert values == before


def test_source14_native_ack_and_poll_deliveries_use_disjoint_sequence_namespace():
    run = _valid_source14_run(minutes=16)
    cache = cast(dict[str, object], decode_canonical(run.initial_policy_cache))
    for row in cast(list[dict[str, object]], cache["rows"]):
        row["active"] = "true"
    cache_bytes = canonical_bytes(cache)
    run = replace(run, initial_policy_cache=cache_bytes)
    validate_historical_input(run)

    composition = _ReplayComposition._from_historical_run(run)
    journal = []

    def collect_evidence(kind, scheduler_key, payload):
        journal.append(dict(record_kind=kind, scheduler_key=scheduler_key, payload=payload))

    composition._evidence_callback = collect_evidence
    cutoff = (run.range_start_ms + 60_000) * 16 + 6
    terminal = composition._dispatch_due(cutoff)
    assert terminal["classification"] == "SUCCESS"
    assert composition._terminal is None

    observation = composition._public_scheduler_observation()
    overlap_at = 1_383_120_086
    records = [record for record in composition._records.values()
               if record["key"][0] == overlap_at and record["key"][1] == 2]
    poll_sequences = {
        record["key"][2] for record in records
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "TRADING_SNAPSHOT"
    }
    native_sequences = {
        record["key"][2] for record in records
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"
    }
    assert poll_sequences == {64}
    assert native_sequences and all(sequence % 2 == 1 for sequence in native_sequences)
    assert len({record["key"][:3] for record in composition._records.values()}) == len(composition._records)
    assert spider_run_evidence._historical_polls_endpoint_compatible(observation, cutoff, journal)
    persisted_p3_journal = [row for row in journal if row["record_kind"] != "DELIVERY_ATTEMPT"]
    assert spider_run_evidence._historical_polls_endpoint_compatible(
        observation, cutoff, persisted_p3_journal,
    )
    committed_effects = [row for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"
                         and row["payload"]["result"]["classification"] == "COMMITTED"
                         and len(row["payload"]["request"]["members"]) == 1
                         and row["payload"]["request"]["members"][0]["kind"] == "CANCEL_EFFECT"]
    assert committed_effects
    ack_attempts = [row["payload"]["delivery"] for row in journal
                    if row["record_kind"] == "DELIVERY_ATTEMPT"
                    and row["payload"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"]
    effect_event_ids = {
        row["payload"]["request"]["members"][0]["stamp"]["event_id"]
        for row in committed_effects
    }
    assert effect_event_ids <= {delivery["source_fact_id"] for delivery in ack_attempts}
