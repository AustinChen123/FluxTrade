"""S1 proofs for owner-only historical evidence capture and composer compatibility."""

from copy import deepcopy
from dataclasses import replace
from typing import Any, cast

import pytest

from src.core.backtest import spider_policy_protocol as policy_protocol
from src.core.backtest import spider_scenario_run as run
from src.core.backtest.spider_historical_input import encode_historical_run_input
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_historical_input import _valid_run


def _composition(run_id: str = "s1-capture") -> _ReplayComposition:
    value = replace(
        _valid_run(),
        run_id=run_id,
        policy_source_sha256=run._P3_POLICY_SOURCE,
    )
    return _ReplayComposition._from_historical_run(value)


def _requests(owner: _ReplayComposition, prefix: str, cutoff: int = 1_382_400_000) -> list[dict[str, Any]]:
    return run._owner_requests(owner._account, cutoff, prefix)


def _capture(owner: _ReplayComposition, method_name: str, prefix: str) -> dict[str, Any]:
    method = cast(Any, owner).__getattribute__(method_name)
    return method(1_382_400_000, *_requests(owner, prefix))


class _RecordingCodec:
    def __init__(self, delegate: Any, fail_at: str | None = None) -> None:
        self.delegate = delegate
        self.fail_at = fail_at
        self.calls: list[str] = []
        self.successful_snapshots: list[tuple[str, dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)

    def inspect_state(self) -> dict[str, Any]:
        self.calls.append("inspect_state")
        if self.fail_at == "inspect_state":
            raise RuntimeError("injected inspect_state failure")
        return self.delegate.inspect_state()

    def capture_snapshot(self, request: dict[str, Any]) -> dict[str, Any]:
        kind = cast(str, request["snapshot_kind"])
        self.calls.append(kind)
        if self.fail_at == kind:
            raise RuntimeError(f"injected {kind} failure")
        fact = self.delegate.capture_snapshot(request)
        self.successful_snapshots.append((kind, deepcopy(fact)))
        return fact


def _install_codec(owner: _ReplayComposition, codec: _RecordingCodec) -> None:
    cast(Any, owner)._codec = codec


_NATIVE_CAPTURE_ORDER = ["inspect_state", "TRADING", "POSITIONS", "OPEN_ORDERS"]


def _assert_failure_prefix(
    failure: str,
    calls: list[str],
    successful_snapshots: list[tuple[str, dict[str, Any]]],
) -> None:
    failure_index = _NATIVE_CAPTURE_ORDER.index(failure)
    assert calls == _NATIVE_CAPTURE_ORDER[: failure_index + 1]
    expected_snapshots = _NATIVE_CAPTURE_ORDER[1:failure_index]
    assert [kind for kind, _fact in successful_snapshots] == expected_snapshots


def _assert_intermediate_capture_categories(prefixes: list[str]) -> None:
    assert any(item.startswith("GROUP-") for item in prefixes)
    assert any(item.startswith("MARKET-") for item in prefixes)


@pytest.mark.parametrize("method_name", ["_capture_owner_evidence_core", "capture_owner_evidence"])
def test_all_three_requests_validate_before_any_native_call(monkeypatch, method_name):
    owner = _composition()
    codec = _RecordingCodec(owner._codec)
    _install_codec(owner, codec)
    requests = _requests(owner, "VALIDATION")
    requests[2]["snapshot_kind"] = "TRADING"
    validated: list[str] = []
    original_plan_snapshot = policy_protocol._plan_snapshot

    def record_validation(request: object) -> bytes:
        validated.append(cast(str, cast(dict[str, object], request)["snapshot_kind"]))
        return original_plan_snapshot(request)

    monkeypatch.setattr(policy_protocol, "_plan_snapshot", record_validation)
    method = cast(Any, owner).__getattribute__(method_name)
    with pytest.raises(ValueError):
        method(1_382_400_000, *requests)

    assert validated == ["TRADING", "POSITIONS", "TRADING"]
    assert codec.calls == []


@pytest.mark.parametrize(
    ("method_name", "expected_scheduler_calls"),
    [("_capture_owner_evidence_core", 0), ("capture_owner_evidence", 1)],
)
def test_native_call_order_and_composer_scheduler_call(
    monkeypatch, method_name: str, expected_scheduler_calls: int,
):
    owner = _composition()
    codec = _RecordingCodec(owner._codec)
    _install_codec(owner, codec)
    scheduler_calls: list[bool] = []

    def observe_scheduler() -> dict[str, Any]:
        scheduler_calls.append(True)
        return {"records": [], "polls": [], "callback_actions": []}

    monkeypatch.setattr(owner, "_public_scheduler_observation", observe_scheduler)
    result = _capture(owner, method_name, "ORDER")

    assert codec.calls == _NATIVE_CAPTURE_ORDER
    assert len(scheduler_calls) == expected_scheduler_calls
    if method_name == "capture_owner_evidence":
        assert result["scheduler_observation"] == {"records": [], "polls": [], "callback_actions": []}
    else:
        assert set(result) == {"cutoff", "inspection", "trading_request", "trading_fact",
                               "positions_request", "positions_fact", "open_orders_request", "open_orders_fact"}


@pytest.mark.parametrize("failure", ["inspect_state", "TRADING", "POSITIONS", "OPEN_ORDERS"])
def test_native_failure_stops_at_same_call_with_same_prior_side_effects(failure: str):
    outcomes = []
    for method_name in ("_capture_owner_evidence_core", "capture_owner_evidence"):
        owner = _composition()
        codec = _RecordingCodec(owner._codec, fail_at=failure)
        _install_codec(owner, codec)
        scheduler_calls: list[bool] = []
        cast(Any, owner)._public_scheduler_observation = lambda: scheduler_calls.append(True) or {}
        method = cast(Any, owner).__getattribute__(method_name)
        with pytest.raises(RuntimeError, match=f"injected {failure} failure"):
            method(1_382_400_000, *_requests(owner, "FAILURE"))
        outcomes.append((codec.calls, codec.successful_snapshots, scheduler_calls,
                         codec.delegate.inspect_state()))

    assert outcomes[0] == outcomes[1]
    _assert_failure_prefix(failure, outcomes[0][0], outcomes[0][1])
    assert outcomes[0][2] == []


def test_composer_output_is_core_owner_evidence_plus_public_scheduler_observation():
    expected_owner = _composition()
    actual_owner = _composition()
    expected = expected_owner._capture_owner_evidence_core(
        1_382_400_000, *_requests(expected_owner, "COMPOSER"),
    )
    expected_observation = expected_owner._public_scheduler_observation()

    actual = actual_owner.capture_owner_evidence(
        1_382_400_000, *_requests(actual_owner, "COMPOSER"),
    )

    assert actual == deepcopy(dict(
        owner_evidence=expected,
        scheduler_observation=expected_observation,
    ))


@pytest.mark.parametrize("method_name", ["_capture_owner_evidence_core", "capture_owner_evidence"])
def test_returned_capture_data_is_isolated_from_inputs_and_internal_owner(method_name: str):
    owner = _composition()
    account_before = deepcopy(owner._account)
    method = cast(Any, owner).__getattribute__(method_name)
    result = method(1_382_400_000, *_requests(owner, "ISOLATION"))
    evidence = result["owner_evidence"] if method_name == "capture_owner_evidence" else result

    evidence["inspection"]["isolation_probe"] = "mutated"
    evidence["trading_request"]["account_key"]["account"] = "mutated"
    evidence["trading_fact"]["immutable_payload"]["isolation_probe"] = "mutated"
    if method_name == "capture_owner_evidence":
        result["scheduler_observation"]["callback_actions"].append({"isolation_probe": True})

    assert owner._account == account_before
    assert "isolation_probe" not in owner._codec.inspect_state()
    next_result = _capture(owner, method_name, "ISOLATION-NEXT")
    next_evidence = next_result["owner_evidence"] if method_name == "capture_owner_evidence" else next_result
    assert "isolation_probe" not in next_evidence["inspection"]
    assert "isolation_probe" not in next_evidence["trading_fact"]["immutable_payload"]
    assert "mutated" not in next_evidence["trading_request"]["account_key"].values()


def test_historical_path_uses_composer_only_for_final_capture(tmp_path, monkeypatch):
    core_prefixes: list[str] = []
    composer_prefixes: list[str] = []
    scheduler_call_contexts: list[str] = []
    original_core = _ReplayComposition._capture_owner_evidence_core
    original_composer = _ReplayComposition.capture_owner_evidence
    original_scheduler = _ReplayComposition._public_scheduler_observation

    def prefix(requests: tuple[dict[str, Any], ...]) -> str:
        return cast(str, requests[0]["snapshot_id"]).removesuffix("-TRADING")

    def record_core(owner, cutoff, *requests):
        core_prefixes.append(prefix(requests))
        return original_core(owner, cutoff, *requests)

    def record_composer(owner, cutoff, *requests):
        composer_prefixes.append(prefix(requests))
        return original_composer(owner, cutoff, *requests)

    def record_scheduler(owner):
        scheduler_call_contexts.append(composer_prefixes[-1] if composer_prefixes else "NO_COMPOSER")
        return original_scheduler(owner)

    monkeypatch.setattr(_ReplayComposition, "_capture_owner_evidence_core", record_core)
    monkeypatch.setattr(_ReplayComposition, "capture_owner_evidence", record_composer)
    monkeypatch.setattr(_ReplayComposition, "_public_scheduler_observation", record_scheduler)
    value = replace(
        _valid_run(),
        run_id="s1-capture-class",
        policy_source_sha256=run._P3_POLICY_SOURCE,
    )
    raw = encode_historical_run_input(value)

    result = run.run_spider_scenario(str(tmp_path), value.run_id, run._P3_SELECTOR, raw)

    assert result == dict(run_id=value.run_id, outcome="ADMITTED", reason=None)
    assert core_prefixes[0] == "INITIAL"
    assert core_prefixes[-1] == "FINAL"
    _assert_intermediate_capture_categories(core_prefixes)
    assert composer_prefixes == ["FINAL"]
    assert scheduler_call_contexts == ["FINAL"]
    assert core_prefixes.count("FINAL") == 1
    assert all(item == "FINAL" for item in composer_prefixes)
