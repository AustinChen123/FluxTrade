"""Pure consistency checks for the closed gross-response report projection."""

from __future__ import annotations

from typing import Any

from src.core.backtest.spider_gross_artifact_schema import gross_observation
from src.core.backtest.spider_gross_execution_input import SpiderGrossExecutionInput
from src.core.backtest.spider_gross_observation import _obligation_facts, _overlimit, _peak
from src.core.backtest.spider_gross_response import _observation_state, propose_reductions
from src.core.backtest.spider_run_artifacts import canonical_bytes


def _same(left: object, right: object) -> bool:
    try:
        return canonical_bytes(left) == canonical_bytes(right)
    except (TypeError, ValueError, UnicodeError):
        return False


def _key_order(value: dict[str, Any]) -> tuple[int, int, int, str]:
    classes = {
        "SOURCE_GROUP": 0, "SNAPSHOT_CAPTURE": 1, "DELIVERY": 2,
        "HISTORICAL_MARKET_STEP": 3, "HISTORICAL_MARKET_CACHE": 4,
        "HISTORICAL_TIMER": 5, "PAID_ENDPOINT_DRAIN": 6,
    }
    return (value["visible_at"], classes[value["queue_class"]],
            value["schedule_sequence"], value["stable_id"])


def gross_report_projection(
    report: dict[str, Any], endpoint: dict[str, Any],
    request: SpiderGrossExecutionInput, contract_hash: str,
    journal: list[dict[str, Any]],
) -> bool:
    """Link gross observations/proposals to current scheduler and paid facts."""
    observation = report.get("gross_observation")
    if observation is None:
        return (
            endpoint.get("status") == "FAILED"
            and endpoint.get("owner_evidence") is None
            and report.get("initial_owner_evidence") is None
            and not journal
        )
    if type(observation) is not dict:
        return False
    try:
        gross_observation(observation, expected_hash=contract_hash)
        if observation["schema_version"] != "spider_gross_response_observation_v2":
            return False
        expected_scope = {
            "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
            "strategy_id": request.authorization_scope.strategy_id,
            "ordered_products": list(request.authorization_scope.ordered_products),
        }
        expected_parameters = {
            "trigger_cap": "2", "target_cap": "1.5",
            "allocation": "GROSS_PROPORTIONAL_V1",
            "quantity_rounding": "CEIL_STEP_NO_MINIMUM_UPLIFT_V1",
        }
        if (observation["authorization_scope"] != expected_scope
                or observation["response_parameters"] != expected_parameters):
            return False
        native_observations = observation["observations"]
        scheduler = report["scheduler_observation"]
        scheduler_keys = [record["key"] for record in scheduler["records"]]
        owner_cuts: list[tuple[dict[str, Any] | None, dict[str, Any]]] = []
        initial_owner = report["initial_owner_evidence"]
        if isinstance(initial_owner, dict):
            owner_cuts.append((None, initial_owner["inspection"]))
        for journal_row in journal:
            payload = journal_row["payload"]
            inspection = None
            if journal_row["record_kind"] == "SOURCE_GROUP_RESULT":
                captured = payload.get("owner_evidence_after")
                if isinstance(captured, dict):
                    inspection = captured.get("inspection")
            elif journal_row["record_kind"] == "HISTORICAL_MARKET_RESULT":
                inspection = payload.get("owner_inspection_after")
            if isinstance(inspection, dict):
                owner_cuts.append((journal_row["scheduler_key"], inspection))
        owner_cuts.sort(key=lambda item: (-1, -1, -1, "") if item[0] is None
                        else _key_order(item[0]))
        final_facts = observation["final_obligation_state"]
        if final_facts is not None:
            if (final_facts["observed_at"] != scheduler["current_time"]
                    or not _same(final_facts["key"], scheduler["last_popped"])):
                return False
            final_owner = endpoint["owner_evidence"]
            if final_owner is None:
                return False
            inspection = final_owner["inspection"]
            final_native = final_facts["native_state"]
            admission = final_native["gross_admission"]
            if any(final_native[field] != inspection[field] for field in (
                    "account_key", "owner_state_digest", "account_version",
                    "valuation_context_id",
            )) or any(admission[field] != inspection[field] for field in (
                    "account_key", "account_version", "valuation_context_id",
            )):
                return False
        previous_observed_at = None
        for row in native_observations:
            key = row["key"]
            if key is not None and not any(_same(key, candidate) for candidate in scheduler_keys):
                return False
            if (previous_observed_at is not None
                    and row["observed_at"] < previous_observed_at):
                return False
            previous_observed_at = row["observed_at"]
            state = row["native_state"]
            admission = state["gross_admission"]
            authoritative = [
                inspection for owner_key, inspection in owner_cuts
                if (owner_key is None if key is None
                    else owner_key is None or _key_order(owner_key) <= _key_order(key))
            ]
            if authoritative:
                inspection = authoritative[-1]
                if any(state[field] != inspection[field] for field in (
                    "account_key", "account_version", "valuation_context_id", "owner_state_digest",
                )):
                    return False
            if (state["account_key"] != endpoint["owner_evidence"]["inspection"]["account_key"]
                    if endpoint["owner_evidence"] is not None else False):
                return False
            if (state["account_version"] != admission["account_version"]
                    or state["valuation_context_id"] != admission["valuation_context_id"]):
                return False

        settlement_key = {"visible_at": scheduler["current_time"]}
        # The persisted observation has a closed public ``key`` field; the
        # pure metric helpers consume the original native key spelling. Adapt
        # that same fact without adding or inferring any scheduler identity.
        metric_rows = []
        for row in native_observations:
            metric_row = dict(row)
            if row["key"] is None:
                metric_row["cause"] = "START_READINESS"
                metric_row["proposal_context"] = {"scheduler_key": None}
            else:
                metric_row["scheduler_key"] = row["key"]
            metric_rows.append(metric_row)
        peak = _peak(metric_rows)
        intervals, duration = _overlimit(metric_rows, settlement_key)
        if (not _same(peak, observation["peak_ratio"])
                or not _same(intervals, observation["overlimit_intervals"])
                or duration != observation["overlimit_duration_ms"]):
            return False

        observations_by_key: dict[bytes, list[dict[str, Any]]] = {}
        scheduler_deliveries = [
            record for record in scheduler["records"] if record["kind"] == "DELIVERY"
        ]
        from src.core.backtest.spider_policy_protocol import HistoricalSequenceMode
        from src.core.backtest.spider_run_execution_evidence import _paid_delivery_origins

        role_rows = {
            item["order_id"]: item["role"]
            for item in report["paid_observation"]["order_roles"]
        }
        actions_by_group = {
            action["group_id"]: action for action in endpoint["actions"]
            if action["group_id"] is not None
        }
        source_keys: dict[str, dict[str, Any]] = {}
        for journal_row in journal:
            payload = journal_row["payload"]
            if journal_row["record_kind"] == "HISTORICAL_MARKET_RESULT":
                for product in payload["result"]["products"]:
                    for fill in product["fills"]:
                        source_keys[fill["source_event_id"]] = journal_row["scheduler_key"]
            elif journal_row["record_kind"] == "SOURCE_GROUP_RESULT":
                for member in payload["request"]["members"]:
                    source_keys[member["stamp"]["event_id"]] = journal_row["scheduler_key"]
        delivery_origins = None
        for mode in HistoricalSequenceMode:
            linked_origins = []
            for origin in _paid_delivery_origins(
                    journal, request.execution_input, role_rows, actions_by_group, mode):
                records = [record for record in scheduler_deliveries
                           if record["key"]["visible_at"] == origin["visible_at"]
                           and record["key"]["schedule_sequence"] == origin["schedule_sequence"]]
                if len(records) != 1:
                    break
                linked_origins.append((origin, records[0]["key"]))
            else:
                delivery_origins = linked_origins
                break
        if delivery_origins is None:
            return False
        callbacks_by_key: dict[bytes, list[dict[str, Any]]] = {}
        cancel_receipts_by_event: dict[str, dict[str, Any]] = {}
        for journal_row in journal:
            if journal_row["record_kind"] == "CALLBACK_RESULT":
                callbacks_by_key.setdefault(
                    canonical_bytes(journal_row["scheduler_key"]), [],
                ).append(journal_row)
            elif journal_row["record_kind"] == "SOURCE_GROUP_RESULT":
                for member in journal_row["payload"]["request"]["members"]:
                    if member["kind"] == "CANCEL_EFFECT":
                        cancel_receipts_by_event[member["stamp"]["event_id"]] = journal_row

        def pending_delivery_roles_match(
            operation_state: dict[str, Any], key: dict[str, Any] | None,
            expected_origins: list[tuple[dict[str, Any], dict[str, Any]]],
        ) -> bool:
            pending_ids = operation_state["pending_delivery_ids"]
            item_ids = sorted(
                item["delivery"]["delivery_id"]
                for item in operation_state["pending_deliveries"]
            )
            expected_ids = sorted(delivery_key["stable_id"]
                                  for _, delivery_key in expected_origins)
            if pending_ids != item_ids or item_ids != expected_ids:
                return False
            pending = {
                item["delivery"]["delivery_id"]: item
                for item in operation_state["pending_deliveries"]
            }
            if set(pending) != {delivery_key["stable_id"] for _, delivery_key in expected_origins}:
                return False
            action_rows = operation_state["actions"]
            action_rows_by_id = {item["action_id"]: item for item in action_rows}
            roles = operation_state["delivery_roles"]
            ack_action_ids = operation_state["cancel_action_ids"]["acks"]
            for origin, delivery_key in expected_origins:
                delivery_id = delivery_key["stable_id"]
                item = pending[delivery_id]
                if (item["role"] != origin["role"]
                        or item["cancel_action_id"] != origin["cancel_action_id"]
                        or roles.get(delivery_id) != origin["role"]):
                    return False
                if origin["payload_kind"] != "TRANSPORT_ACK":
                    continue
                cancel_action_id = origin["cancel_action_id"]
                receipt = cancel_receipts_by_event.get(origin["source_fact_id"])
                source_key = source_keys.get(origin["source_fact_id"])
                if (cancel_action_id is None or receipt is None or source_key is None
                        or key is None or _key_order(source_key) > _key_order(key)):
                    return False
                group = receipt["payload"]["request"]
                action = actions_by_group.get(group["group_id"])
                cut_action = action_rows_by_id.get(cancel_action_id)
                if (receipt["payload"]["result"]["classification"] != "COMMITTED"
                        or group["members"] != [
                            member for member in group["members"]
                            if member["kind"] == "CANCEL_EFFECT"
                            and member["stamp"]["event_id"] == origin["source_fact_id"]
                        ]
                        or action is None or action["action_id"] != cancel_action_id
                        or action["role"] not in ("DRAIN_CANCEL", "GROSS_CANCEL")
                        or action["classification"] != "COMMITTED"
                        or cut_action is None or not _same(cut_action, action)
                        or ack_action_ids.get(delivery_id) != cancel_action_id):
                    return False
            return True

        for row in native_observations:
            operation_state = row["operation_state"]
            if operation_state is not None:
                key = row["key"]
                if key is None:
                    expected_pending_origins: list[tuple[dict[str, Any], dict[str, Any]]] = []
                else:
                    expected_pending_origins = []
                    for origin, delivery_key in delivery_origins:
                        source_key = source_keys.get(origin["source_fact_id"])
                        if (not origin["paid_obligation"] or source_key is None
                                or _key_order(source_key) > _key_order(key)):
                            continue
                        callbacks = callbacks_by_key.get(canonical_bytes(delivery_key), [])
                        delivered_successfully = any(
                            callback["payload"]["outcome"] in (
                                "SUCCESS", "CONSUMER_DEDUPLICATED",
                            ) and _key_order(callback["scheduler_key"]) <= _key_order(key)
                            for callback in callbacks
                        )
                        if not delivered_successfully:
                            expected_pending_origins.append((origin, delivery_key))
                if not pending_delivery_roles_match(
                    operation_state, key, expected_pending_origins,
                ):
                    return False
            observations_by_key.setdefault(canonical_bytes(row["key"]), []).append(row)
        actions = endpoint["actions"]
        action_by_id = {action["action_id"]: action for action in actions}
        linked_reductions: set[str] = set()
        sequences: list[int] = []

        def clear_at_observation_cut(
            key: dict[str, Any], episode_sequence: int,
            *, require_recovery: bool = False,
            expected_native: dict[str, Any] | None = None,
            required_action_ids: list[str] | None = None,
        ) -> bool:
            exact_observations = observations_by_key.get(canonical_bytes(key), [])
            for row in exact_observations:
                if row["episode_sequence"] != episode_sequence:
                    continue
                if require_recovery and row["after_state"] != "RECOVERED":
                    continue
                if expected_native is not None and not _same(row["native_state"], expected_native):
                    continue
                operations = row["operation_state"]
                if not isinstance(operations, dict):
                    continue
                if required_action_ids is not None:
                    actions_at_cut = operations.get("actions")
                    if not isinstance(actions_at_cut, list):
                        continue
                    selected_at_cut = [action for action in actions_at_cut
                                       if isinstance(action, dict)
                                       and action.get("action_id") in required_action_ids]
                    if (len(selected_at_cut) != len(required_action_ids)
                            or any(action.get("role") != "GROSS_REDUCTION"
                                   or action.get("classification") not in ("COMMITTED", "REJECTED")
                                   or not _same(action, action_by_id.get(action.get("action_id")))
                                   for action in selected_at_cut)):
                        continue
                if _obligation_facts({
                    "proposal_context": {"operations": operations},
                    "native_state": row["native_state"],
                })[0] is True:
                    return True
            return False

        for episode in observation["episodes"]:
            if episode["recovered_key"] is not None:
                recovered = observations_by_key.get(canonical_bytes(episode["recovered_key"]), [])
                if not any(row["after_state"] == "RECOVERED"
                           and row["native_state"] == episode["recovery_state"]
                           for row in recovered):
                    return False
                if not clear_at_observation_cut(
                    episode["recovered_key"], episode["sequence"],
                    require_recovery=True, expected_native=episode["recovery_state"],
                ):
                    return False
            for round_row in episode["rounds"]:
                sequence = round_row["sequence"]
                action_ids = round_row["action_ids"]
                if sequence is None:
                    if action_ids:
                        return False
                else:
                    sequences.append(sequence)
                proposal = round_row["proposal"]
                key_bytes = canonical_bytes(round_row["proposal_key"])
                candidates = observations_by_key.get(key_bytes, [])
                selected = next((row for row in candidates
                                 if row["native_state"]["owner_state_digest"]
                                 == proposal["native_state_digest"]
                                 and row["native_state"]["account_version"]
                                 == proposal["account_version"]
                                 and row["native_state"]["valuation_context_id"]
                                 == proposal["valuation_context_id"]), None)
                if selected is None:
                    return False
                source_state = proposal["proposal_context"]["source_state"]
                operation_state = proposal["proposal_context"]["operation_state"]
                context = {
                    "scheduler_key": round_row["proposal_key"],
                    "source_cache": source_state,
                    "operations": operation_state,
                }
                expected = propose_reductions(
                    selected["native_state"],
                    strategy_id=request.authorization_scope.strategy_id,
                    ordered_products=request.authorization_scope.ordered_products,
                    proposal_context=context,
                ).detached()
                expected_context = expected["proposal_context"]
                expected["proposal_context"] = {
                    "source_state": expected_context["source_cache"],
                    "operation_state": expected_context["operations"],
                }
                expected["scope"] = expected_scope
                if not _same(expected, proposal):
                    return False
                clear, _, _ = _obligation_facts({
                    "proposal_context": {"operations": operation_state},
                    "native_state": selected["native_state"],
                })
                if action_ids:
                    selected_actions = [action_by_id.get(action_id) for action_id in action_ids]
                    if (any(action is None or action["role"] != "GROSS_REDUCTION"
                            for action in selected_actions)
                            or len({action["product_id"] for action in selected_actions if action is not None})
                            != len(selected_actions)):
                        return False
                    if clear is not True:
                        return False
                    linked_reductions.update(action_ids)
                if round_row["completion_key"] is not None:
                    if not any(_same(round_row["completion_key"], candidate)
                               for candidate in scheduler_keys):
                        return False
                    completion_actions = [action_by_id.get(action_id) for action_id in action_ids]
                    if any(action is None or action["classification"] not in ("COMMITTED", "REJECTED")
                           for action in completion_actions):
                        return False
                    completion_rows = [row for row in journal
                                       if _same(row["scheduler_key"], round_row["completion_key"])]
                    if not completion_rows:
                        return False
                    if not clear_at_observation_cut(
                        round_row["completion_key"], episode["sequence"],
                        required_action_ids=action_ids,
                    ):
                        return False
        if sequences != sorted(set(sequences)):
            return False
        actual_reductions = {action["action_id"] for action in actions
                             if action["role"] == "GROSS_REDUCTION"}
        if linked_reductions != actual_reductions:
            return False
        final_owner = endpoint["owner_evidence"]
        if final_owner is not None:
            terminal_state = final_owner["inspection"]["lifecycle"]
            if terminal_state in ("LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"):
                if observation["current_state"] != terminal_state:
                    return False
        latest = observation["episodes"][-1] if observation["episodes"] else None
        progress = None
        if final_facts is not None:
            progress = final_facts["operation_state"].get("gross_response")
        if not isinstance(progress, dict):
            progress = next((row["operation_state"].get("gross_response")
                             for row in reversed(native_observations)
                             if isinstance(row["operation_state"], dict)
                             and isinstance(row["operation_state"].get("gross_response"), dict)), None)
        if progress is None:
            if latest is not None:
                return False
            progress_state, progress_active = "IDLE", False
        else:
            progress_state = progress["state"]
            progress_active = progress["active"]
        terminal_failure = scheduler["gate"] == "FAILED"
        paid_observation = report["paid_observation"]
        final_cutoff = (
            final_owner["cutoff"] if final_owner is not None
            else paid_observation["observed_at"]
        )
        settlement_complete = (
            not terminal_failure
            and paid_observation["current_evidence_complete"]
            and final_cutoff >= endpoint["settlement_end_ms"] * 16 + 6
        )
        expected_current_state = _observation_state(
            progress_state, progress_active,
            settlement_complete=settlement_complete,
            terminal_failure=terminal_failure,
        )
        if (final_owner is not None and final_owner["inspection"]["lifecycle"] in (
                "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT",
        ) and expected_current_state != final_owner["inspection"]["lifecycle"]):
            return False
        if observation["current_state"] != expected_current_state:
            return False
        expected_reason = native_observations[-1]["reason"] if native_observations else None
        if observation["reason"] != expected_reason:
            return False
        final_facts = observation["final_obligation_state"]
        if final_facts is not None:
            final_operations = final_facts["operation_state"]
            final_key = final_facts["key"]
            final_expected_origins = []
            if final_key is not None:
                for origin, delivery_key in delivery_origins:
                    source_key = source_keys.get(origin["source_fact_id"])
                    if (not origin["paid_obligation"] or source_key is None
                            or _key_order(source_key) > _key_order(final_key)):
                        continue
                    callbacks = callbacks_by_key.get(
                        canonical_bytes(next(
                            record["key"] for record in scheduler_deliveries
                            if record["key"]["visible_at"] == origin["visible_at"]
                            and record["key"]["schedule_sequence"] == origin["schedule_sequence"]
                        )), [],
                    )
                    delivered_successfully = any(
                        callback["payload"]["outcome"] in (
                            "SUCCESS", "CONSUMER_DEDUPLICATED",
                        ) and _key_order(callback["scheduler_key"]) <= _key_order(final_key)
                        for callback in callbacks
                    )
                    if not delivered_successfully:
                        final_expected_origins.append((origin, delivery_key))
            if not pending_delivery_roles_match(
                final_operations, final_key, final_expected_origins,
            ):
                return False
        if latest is not None and latest["state"] in ("RESTRICTED", "INCOMPLETE", "FAILED"):
            if final_facts is None:
                if latest["remaining_obligations"] is not None:
                    return False
            else:
                clear, operations, native = _obligation_facts({
                    "proposal_context": {"operations": final_facts["operation_state"]},
                    "native_state": final_facts["native_state"],
                })
                expected_remaining = (
                    [] if clear is True else None if clear is None else [{
                        "operation_state": operations, "native_state": native,
                    }]
                )
                if not _same(latest["remaining_obligations"], expected_remaining):
                    return False
        return True
    except (ArithmeticError, IndexError, KeyError, TypeError, ValueError):
        return False
