"""Pure detached report projection over existing gross-response owner facts."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, localcontext
from types import SimpleNamespace
from typing import Any

from src.core.backtest.spider_gross_execution_input import SpiderGrossExecutionInput
from src.core.backtest.spider_gross_response import (
    GrossResponseEpisode,
    POLICY_ID,
    gross_obligations_clear,
)

_ROOT_SCHEMA = "spider_gross_response_observation_v1"
_QUEUE_LABELS = (
    "SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP",
    "HISTORICAL_MARKET_CACHE", "HISTORICAL_TIMER", "PAID_ENDPOINT_DRAIN",
)


def _key(observation: dict[str, Any]) -> dict[str, Any] | None:
    context = observation.get("proposal_context")
    if isinstance(context, dict):
        operations_key = context.get("scheduler_key")
        if isinstance(operations_key, dict):
            if (set(operations_key) != {
                    "visible_at", "queue_class", "schedule_sequence", "stable_id",
            } or type(operations_key.get("visible_at")) is not int
                    or type(operations_key.get("schedule_sequence")) is not int
                    or type(operations_key.get("stable_id")) is not str):
                raise ValueError(f"INVALID_SCHEMA scheduler key: {operations_key!r}")
            queue_class = operations_key.get("queue_class")
            if type(queue_class) is int and 0 <= queue_class < len(_QUEUE_LABELS):
                operations_key = dict(operations_key, queue_class=_QUEUE_LABELS[queue_class])
            elif type(queue_class) is not str or queue_class not in _QUEUE_LABELS:
                raise ValueError("INVALID_SCHEMA")
            return deepcopy(operations_key)
        if operations_key is None and observation.get("cause") == "START_READINESS":
            return None
    native_key = observation.get("scheduler_key")
    if isinstance(native_key, dict):
        # Older native observation facts omit queue_class. Do not invent it.
        if set(native_key) == {"visible_at", "queue_class", "schedule_sequence", "stable_id"}:
            return deepcopy(native_key)
    if observation.get("cause") == "START_READINESS":
        return None
    raise ValueError("INVALID_SCHEMA")


def _decimal(value: object) -> Decimal | None:
    if type(value) is Decimal:
        return value if value.is_finite() else None
    if type(value) is str:
        parsed = Decimal(value)
        return parsed if parsed.is_finite() else None
    return None


def _gross_equity(row: dict[str, Any]) -> tuple[Decimal, Decimal]:
    native = row.get("native_state")
    admission = native.get("gross_admission") if isinstance(native, dict) else None
    if not isinstance(admission, dict):
        raise ValueError("INVALID_SCHEMA")
    gross = _decimal(admission.get("gross"))
    equity = _decimal(admission.get("effective_equity"))
    if gross is None or equity is None:
        raise ValueError("INVALID_SCHEMA")
    return gross, equity


def _ratio(row: dict[str, Any]) -> tuple[Decimal, Decimal] | None:
    gross, equity = _gross_equity(row)
    return (gross, equity) if equity > 0 else None


def _with_episode_sequences(
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Project episode membership from actual state-transition checks."""
    result: list[dict[str, Any]] = []
    sequence = 0
    active_sequence: int | None = None
    last_observed_at: int | None = None
    for source in observations:
        row = deepcopy(source)
        observed_at = row.get("observed_at")
        if (type(observed_at) is not int
                or not isinstance(row.get("native_state"), dict)):
            raise ValueError("INVALID_SCHEMA")
        if last_observed_at is not None and observed_at < last_observed_at:
            raise ValueError("INVALID_SCHEDULER_ORDER")
        last_observed_at = observed_at
        before, after = row.get("before_state"), row.get("after_state")
        if (before in ("IDLE", "RECOVERED")
                and after in ("RESTRICTED", "INCOMPLETE")):
            sequence += 1
            active_sequence = sequence
        row["episode_sequence"] = active_sequence
        result.append(row)
        if after in ("RECOVERED", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED"):
            active_sequence = None
    return result


def _peak(observations: list[dict[str, Any]]) -> dict[str, Any] | None:
    best: tuple[Decimal, Decimal] | None = None
    best_key: dict[str, Any] | None = None
    for row in observations:
        ratio = _ratio(row)
        key = _key(row)
        if ratio is None:
            continue
        if best is None:
            best, best_key = ratio, key
        else:
            with localcontext() as context:
                context.prec = max(60, sum(len(value.as_tuple().digits) for value in (*ratio, *best)) + 2)
                is_higher = ratio[0] * best[1] > best[0] * ratio[1]
            if is_higher:
                best, best_key = ratio, key
    if best is None:
        return None
    return {"numerator": best[0], "denominator": best[1], "key": best_key}


def _overlimit(
    observations: list[dict[str, Any]], settlement_key: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], int]:
    intervals: list[dict[str, Any]] = []
    start: tuple[dict[str, Any] | None, int] | None = None
    duration = 0
    for row in observations:
        gross, equity = _gross_equity(row)
        key = _key(row)
        observed_at = row.get("observed_at")
        over = equity > 0 and gross > Decimal(2) * equity
        if over and start is None and type(observed_at) is int:
            start = (key, observed_at)
        elif not over and start is not None and type(observed_at) is int:
            raw_ms = observed_at // 16 - start[1] // 16
            if raw_ms < 0:
                raise ValueError("INVALID_SCHEDULER_ORDER")
            duration += raw_ms
            intervals.append({
                "start_key": start[0], "end_key": key,
                "end_cut": key.get("visible_at") if isinstance(key, dict) else None,
                "open": False,
            })
            start = None
    if start is not None:
        end = settlement_key
        end_visible = end.get("visible_at") if isinstance(end, dict) else None
        raw_ms = 0
        if type(end_visible) is int:
            raw_ms = end_visible // 16 - start[1] // 16
            if raw_ms < 0:
                raise ValueError("INVALID_SCHEDULER_ORDER")
            duration += raw_ms
        intervals.append({
            "start_key": start[0], "end_key": None,
            "end_cut": end_visible if type(end_visible) is int else None,
            "open": True,
        })
    return intervals, duration


def _obligation_facts(
    observation: dict[str, Any],
) -> tuple[bool | None, dict[str, Any] | None, dict[str, Any] | None]:
    """Apply the runtime's one obligation predicate to a detached observation."""
    context = observation.get("proposal_context")
    operations = context.get("operations") if isinstance(context, dict) else None
    native = observation.get("native_state")
    if not isinstance(operations, dict) or not isinstance(native, dict):
        return None, None, None
    actions = operations.get("actions")
    delivery_roles = operations.get("delivery_roles")
    cancel_ids = operations.get("cancel_action_ids")
    dependencies = operations.get("send_dependencies")
    if (not isinstance(actions, list) or not isinstance(delivery_roles, dict)
            or not isinstance(cancel_ids, dict) or not isinstance(dependencies, dict)):
        return None, None, None
    roles_by_order = {
        action.get("order_id"): action.get("role")
        for action in actions
        if isinstance(action, dict) and action.get("order_id") is not None
    }
    action_by_id = {
        action.get("action_id"): action
        for action in actions if isinstance(action, dict)
    }
    ack_actions = cancel_ids.get("acks")
    cancel_ack_action_by_delivery = {}
    if isinstance(ack_actions, dict):
        cancel_ack_action_by_delivery = {
            delivery_id: action_by_id[action_id]
            for delivery_id, action_id in ack_actions.items()
            if action_id in action_by_id
        }
    endpoint = SimpleNamespace(
        actions=actions,
        delivery_roles=delivery_roles,
        pending_delivery_ids=operations.get("pending_delivery_ids", []),
        cancel_ack_action_by_delivery=cancel_ack_action_by_delivery,
        roles_by_order=roles_by_order,
    )
    composition = SimpleNamespace(
        _paid_cancel_pending_orders=operations.get("cancel_pending_order_ids", []),
        _historical_send_dependencies=dependencies,
    )
    clear = gross_obligations_clear(endpoint, composition, native)
    return clear, operations, deepcopy(native)


def _rounds_for(
    episode_sequence: int,
    observations: list[dict[str, Any]],
    actual_rounds: list[dict[str, Any]],
    request: SpiderGrossExecutionInput,
) -> list[dict[str, Any]]:
    positioned: list[tuple[int, dict[str, Any]]] = []
    matched: set[int] = set()
    for actual_round in actual_rounds:
        action_ids = actual_round.get("action_ids")
        if not isinstance(action_ids, list):
            raise ValueError("INVALID_SCHEMA")
        row_index = next((index for index, candidate in enumerate(observations)
                          if candidate.get("observed_at") == actual_round.get("observed_at")
                          and candidate.get("action_ids") == action_ids
                          and isinstance(candidate.get("proposal"), dict)), None)
        if row_index is None:
            raise ValueError("ROUND_OBSERVATION_NOT_FOUND")
        if observations[row_index].get("episode_sequence") != episode_sequence:
            continue
        matched.add(row_index)
        row = observations[row_index]
        proposal = actual_round.get("proposal")
        if not isinstance(proposal, dict):
            raise ValueError("INVALID_SCHEMA")
        proposal_copy = deepcopy(proposal)
        proposal_copy["scope"] = {
            "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
            "strategy_id": request.authorization_scope.strategy_id,
            "ordered_products": list(request.authorization_scope.ordered_products),
        }
        context = proposal_copy.get("proposal_context")
        if isinstance(context, dict):
            proposal_copy["proposal_context"] = {
                "source_state": deepcopy(context.get("source_cache")),
                "operation_state": deepcopy(context.get("operations")),
            }
        later = observations[row_index + 1:]
        completion_key, completion_reason = _confirmed_completion_key(action_ids, later)
        positioned.append((row_index, {
            "sequence": actual_round.get("sequence"),
            "proposal_key": _key(row),
            "proposal": proposal_copy,
            "action_ids": deepcopy(action_ids),
            "completion_key": completion_key,
            "reason": completion_reason or row.get("reason"),
        }))
    for index, row in enumerate(observations):
        if (row.get("episode_sequence") != episode_sequence
                or index in matched or not isinstance(row.get("proposal"), dict)):
            continue
        proposal_copy = deepcopy(row["proposal"])
        proposal_copy["scope"] = {
            "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
            "strategy_id": request.authorization_scope.strategy_id,
            "ordered_products": list(request.authorization_scope.ordered_products),
        }
        context = proposal_copy.get("proposal_context")
        if isinstance(context, dict):
            proposal_copy["proposal_context"] = {
                "source_state": deepcopy(context.get("source_cache")),
                "operation_state": deepcopy(context.get("operations")),
            }
        positioned.append((index, {
            "sequence": None,
            "proposal_key": _key(row),
            "proposal": proposal_copy,
            "action_ids": [],
            "completion_key": None,
            "reason": row.get("reason"),
        }))
    positioned.sort(key=lambda item: item[0])
    return [item for _, item in positioned]


def _confirmed_completion_key(
    action_ids: list[Any], observations: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None]:
    """Require terminal action facts and the shared runtime obligation barrier."""
    if not action_ids:
        return None, None
    for row in observations:
        context = row.get("proposal_context")
        operations = context.get("operations") if isinstance(context, dict) else None
        actions = operations.get("actions") if isinstance(operations, dict) else None
        if not isinstance(actions, list):
            continue
        selected = [action for action in actions if isinstance(action, dict)
                    and action.get("action_id") in action_ids]
        if len(selected) != len(action_ids) or any(
            action.get("role") != "GROSS_REDUCTION"
            or action.get("classification") not in ("COMMITTED", "REJECTED")
            for action in selected
        ):
            continue
        clear, _, _ = _obligation_facts(row)
        if clear is not True:
            continue
        key = _key(row)
        if key is not None:
            completion_reason = (
                "ACTION_REJECTED"
                if any(action.get("classification") == "REJECTED" for action in selected)
                else row.get("reason")
            )
            return key, completion_reason if isinstance(completion_reason, str) else None
    return None, None


def project_gross_observation(
    request: SpiderGrossExecutionInput,
    episode: GrossResponseEpisode | None,
    *,
    endpoint_projection: dict[str, Any] | None = None,
    settlement_key: dict[str, Any] | None = None,
    final_facts: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Build the formal twelve-field gross projection without mutating owner facts."""
    if episode is None:
        return None
    if endpoint_projection is None:
        raise ValueError("ENDPOINT_PROJECTION_REQUIRED")
    rows = _with_episode_sequences(episode.observations)
    episodes_by_sequence: dict[int, dict[str, Any]] = {}
    for row in rows:
        sequence = row.get("episode_sequence")
        if (row.get("before_state") in ("IDLE", "RECOVERED")
                and row.get("after_state") in ("RESTRICTED", "INCOMPLETE")
                and type(sequence) is int):
            episodes_by_sequence.setdefault(sequence, {
                "sequence": sequence,
                "trigger_key": _key(row),
                "trigger_state": deepcopy(row.get("native_state")),
                "rounds": [], "state": row.get("after_state"),
                "reason": row.get("reason"), "recovered_key": None,
                "recovery_state": None, "remaining_obligations": [],
            })
        if type(sequence) is int:
            episodes_by_sequence.setdefault(sequence, {
                "sequence": sequence, "trigger_key": None,
                "trigger_state": None, "rounds": [],
                "state": row.get("after_state"), "reason": row.get("reason"),
                "recovered_key": None, "recovery_state": None,
                "remaining_obligations": [],
            })
            item = episodes_by_sequence[sequence]
            item["state"] = row.get("after_state")
            item["reason"] = row.get("reason")
            if (row.get("after_state") == "RECOVERED"
                    and row.get("before_state") != "RECOVERED"
                    and item["recovered_key"] is None):
                item["recovered_key"] = _key(row)
                item["recovery_state"] = deepcopy(row.get("native_state"))

    for sequence, item in episodes_by_sequence.items():
        item["rounds"] = _rounds_for(sequence, rows, episode.rounds, request)
        episode_rows = [row for row in rows if row.get("episode_sequence") == sequence]
        if episode_rows:
            clear, operations, native = _obligation_facts(episode_rows[-1])
            if clear is True:
                item["remaining_obligations"] = []
            elif clear is False:
                item["remaining_obligations"] = [{
                    "operation_state": deepcopy(operations),
                    "native_state": deepcopy(native),
                }]
            else:
                item["remaining_obligations"] = None

    accepted_endpoint = endpoint_projection
    if isinstance(accepted_endpoint, dict) and isinstance(
        accepted_endpoint.get("gross_response"), dict,
    ):
        accepted_endpoint = accepted_endpoint["gross_response"]
    state = (
        accepted_endpoint.get("current_state")
        if isinstance(accepted_endpoint, dict) else episode.state
    )
    reason = (
        accepted_endpoint.get("reason")
        if isinstance(accepted_endpoint, dict) else episode.reason
    )
    if (type(state) is not str or (reason is not None and type(reason) is not str)):
        raise ValueError("INVALID_ENDPOINT_PROJECTION")
    eligible_latest = False
    if episodes_by_sequence:
        latest = episodes_by_sequence[max(episodes_by_sequence)]
        eligible_latest = latest.get("state") in (
            "RESTRICTED", "INCOMPLETE", "FAILED",
        )
        if (eligible_latest and state in (
            "RESTRICTED", "INCOMPLETE", "FAILED",
        ) and (episode.active or state == "FAILED")):
            latest["state"] = state
            latest["reason"] = reason
    if (episodes_by_sequence and final_facts is not None
            and eligible_latest and (episode.active or state == "FAILED")):
        latest_sequence = max(episodes_by_sequence)
        latest = episodes_by_sequence[latest_sequence]
        facts_observation = {
            "proposal_context": {"operations": final_facts.get("operations")},
            "native_state": final_facts.get("native_state"),
        }
        clear, operations, native = _obligation_facts(facts_observation)
        if clear is True:
            latest["remaining_obligations"] = []
        elif clear is False:
            latest["remaining_obligations"] = [{
                "operation_state": deepcopy(operations),
                "native_state": deepcopy(native),
            }]
        else:
            latest["remaining_obligations"] = None
    intervals, duration = _overlimit(rows, settlement_key)
    return {
        "schema_version": _ROOT_SCHEMA,
        "run_contract_sha256": episode.contract_hash,
        "policy_id": POLICY_ID,
        "response_parameters": {
            "trigger_cap": str(request.response_parameters.trigger_cap),
            "target_cap": str(request.response_parameters.target_cap),
            "allocation": request.response_parameters.allocation,
            "quantity_rounding": request.response_parameters.quantity_rounding,
        },
        "authorization_scope": {
            "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
            "strategy_id": request.authorization_scope.strategy_id,
            "ordered_products": list(request.authorization_scope.ordered_products),
        },
        "observations": [
            {
                "key": _key(row),
                "observed_at": row["observed_at"],
                "native_state": deepcopy(row["native_state"]),
                "before_state": row["before_state"],
                "after_state": row["after_state"],
                "reason": row["reason"],
                "episode_sequence": row.get("episode_sequence"),
            }
            for row in rows
        ],
        "episodes": [episodes_by_sequence[key] for key in sorted(episodes_by_sequence)],
        "peak_ratio": _peak(rows),
        "overlimit_intervals": intervals,
        "overlimit_duration_ms": duration,
        "current_state": state,
        "reason": reason,
    }
