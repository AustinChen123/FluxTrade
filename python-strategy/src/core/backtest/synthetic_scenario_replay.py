"""Internal closed-policy scheduler composition; no financial submission."""

from copy import deepcopy
from collections.abc import Mapping
import heapq
import json
from dataclasses import dataclass
from decimal import Decimal as D, localcontext
from typing import Any, Callable, cast

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest import spider_policy_protocol as policy_protocol
from src.core.backtest.spider_policy import Policy, fmt


def _pairing_check(condition: bool) -> None:
    if not condition:
        raise ValueError("POLICY_EMISSION_MISMATCH")


def _pair_financial(item, digest, operation, order, account, previous, configured_product_ids=None):
    row = policy_protocol._event_object(item, "event_digest action_kind schedule_sequence expected_group")
    policy_protocol._plan_hash(row["event_digest"])
    policy_protocol._plan_sequence(row["schedule_sequence"])
    policy_protocol._plan_group(row["expected_group"], configured_product_ids)
    group = cast(wire.Group, row["expected_group"])
    _pairing_check(row["event_digest"] == digest)
    _pairing_check(row["action_kind"] == ("ORDER_INTENT" if operation == "send" else "CANCEL_REQUEST"))
    _pairing_check(policy_protocol._plan_account(group["account_key"]) == policy_protocol._plan_account(account))
    _pairing_check(group["declared_member_count"] == 1 and len(group["members"]) == 1)
    member = group["members"][0]
    stamp = member["stamp"]
    _pairing_check(group["ordering_contract_id"] == stamp["ordering_contract_id"] == "S_order_v1")
    _pairing_check(group["group_effective_at"] == stamp["effective_at"] > previous)
    _pairing_check(stamp["scenario_ordinal"] == (60 if operation == "send" else 40))
    if operation == "send":
        _pairing_check(member["kind"] == "INTENT")
        payload = cast(wire.Intent, member["payload"])
        _pairing_check(payload["client_order_id"] == order["clOrdId"] and payload["product_id"] == order["instId"])
        _pairing_check(payload["side"] == ("LONG" if order["side"] == "buy" else "SHORT"))
        _pairing_check(payload["quantity_contracts"] == D(order["sz"]))
        _pairing_check(payload["order_type"] == ("LIMIT" if order["ordType"] == "limit" else "MARKET"))
        _pairing_check(payload.get("limit_price") == (D(order["px"]) if order["ordType"] == "limit" else None))
    else:
        _pairing_check(member["kind"] == "CANCEL_REQUEST")
        payload = cast(wire.CancelRequest, member["payload"])
        _pairing_check(payload["targets"] == [dict(target_order_id=order["ordId"], reason="EXPLICIT_SCENARIO")])
    return stamp["effective_at"]


def _pair_market(item, digest, account, visible_at):
    row = policy_protocol._event_object(item, "event_digest capture_sequence snapshot_request delivery_projection")
    policy_protocol._plan_hash(row["event_digest"])
    policy_protocol._plan_sequence(row["capture_sequence"])
    policy_protocol._plan_snapshot(row["snapshot_request"])
    policy_protocol._plan_projection(row["delivery_projection"])
    request = cast(wire.SnapshotRequest, row["snapshot_request"])
    projection = cast(wire.Projection, row["delivery_projection"])
    _pairing_check(row["event_digest"] == digest)
    _pairing_check(policy_protocol._plan_account(request["account_key"]) == policy_protocol._plan_account(account))
    _pairing_check(request["snapshot_kind"] == "MARKET" and request["capture_mode"] == "FROZEN_POLL_FIXTURE")
    _pairing_check(request.get("fixture_key") == "MARKET_GOLDEN_V1")
    _pairing_check(projection["reference"] == dict(namespace="SNAPSHOT", fact_id=request["snapshot_id"]))
    _pairing_check(projection["payload_kind"] == "MARKET_SNAPSHOT" and projection.get("transport") is None)
    _pairing_check(request.get("continuation_id") is None and projection.get("continuation_id") is None)
    _pairing_check(request["captured_at"] > visible_at and projection["visible_at"] >= request["captured_at"])


def _validate_emission_plan(account, delivery, events, plan, configured_product_ids=None):
    """Validate atomically as evidence; return detached items, never dispatch."""
    try:
        row = policy_protocol._event_object(plan, "delivery_id expected_policy_events financial_items market_requests")
        _pairing_check(row["delivery_id"] == delivery["delivery_id"])
        expected, financial, markets = (
            row[k] for k in ("expected_policy_events", "financial_items", "market_requests")
        )
        _pairing_check(isinstance(expected, list) and isinstance(financial, list) and isinstance(markets, list))
        expected, financial, markets = cast(list, expected), cast(list, financial), cast(list, markets)
        _pairing_check(len(events) == len(expected))
        digests = []
        for event, expectation in zip(events, expected, strict=True):
            entry = policy_protocol._event_object(expectation, "event_digest kind")
            digest = policy_protocol._policy_event_digest(event)
            _pairing_check(entry["kind"] == event["kind"] and entry["event_digest"] == digest)
            digests.append(digest)
        fi, mi, previous = 0, 0, delivery["visible_at"]
        validated = []
        for event, digest in zip(events, digests, strict=True):
            if event["kind"] in ("send", "cancel"):
                for order in event["orders"]:
                    if fi == len(financial):
                        raise ValueError("MISSING_NEXT_EVENT_STAMP")
                    previous = _pair_financial(financial[fi], digest, event["kind"], order, account, previous,
                                               configured_product_ids)
                    validated.append(financial[fi])
                    fi += 1
            elif event["kind"] == "request_market":
                if mi == len(markets):
                    raise ValueError("MISSING_NEXT_EVENT_STAMP")
                _pair_market(markets[mi], digest, account, delivery["visible_at"])
                validated.append(markets[mi])
                mi += 1
        _pairing_check(fi == len(financial))
        _pairing_check(mi == len(markets))
        policy_protocol._emission_plan_bytes(plan, configured_product_ids)
        return tuple(deepcopy(validated))
    except (UnicodeError, OverflowError) as exc:
        raise ValueError("POLICY_EMISSION_MISMATCH") from exc


def _closed_policy(profile: wire.Profile) -> Policy:
    golden = profile == "SYNTHETIC_GOLDEN_CANCEL_V1"
    rows = (
        [
            dict(
                name="P_A",
                active="true",
                leverage="4",
                歩差="0.02",
                單數="2",
                hold上限="0.8",
                hold下限="-0.8",
                hold="0",
            )
        ]
        if golden
        else []
    )
    if golden:
        products = [("P_A", "10", "1", "1", "1", 1)]
    else:
        minimum = profile == "SYNTHETIC_MIN_CASH_V1"
        products = [
            ("BTC-USDT-SWAP", "50000" if minimum else "50001", "0.01", "0.01", "0.1", 1),
            ("ETH-USDT-SWAP", "100" if minimum else "1900", "0.1", "0.01", "0.01", 2),
        ]
    markets = {
        name: dict(
            price=price, ctVal=ct, lotSz=lot, minSz=lot, increment=tick, ratioHL="0.1", state="live", instIdCode=code
        )
        for name, price, ct, lot, tick, code in products
    }
    policy = Policy(rows, markets, now_ms=100000)
    cash = D("1000" if golden else "100")
    policy.capital = dict(earn=D(0), usdt=cash, avail=cash, total=cash, position=D(0))
    policy.last_earn_ms = 100000
    policy.running = golden
    return policy


@dataclass(frozen=True)
class _CallbackPrefix:
    events: tuple[dict[str, object], ...]
    exception: Exception | None


@dataclass(frozen=True)
class _PollObservation:
    status: str
    awaiting: dict[str, Any] | None
    prefix: _CallbackPrefix


@dataclass(frozen=True)
class _PollResult:
    observation: _PollObservation
    clock_advance: int | None = None
    next_stage: dict[str, Any] | None = None


@dataclass
class _PollRecord:
    digest: str
    stages: list[dict[str, Any]]
    index: int
    observation: _PollObservation


def _poll_check(condition: bool) -> None:
    if not condition:
        raise ValueError("INVALID_SCHEMA")


def _historical_node_clock(value: object, configured_product_ids: tuple[str, ...] | None) -> tuple[int, int, bytes]:
    """Validate the scheduler-owned envelope and derive the frozen T clock."""
    row = policy_protocol._event_object(
        value,
        "schema_version model_id model_version run_contract_hash bar_open_ms bar_duration_ms "
        "step_index market_slippage_bps bars working_orders",
    )
    if configured_product_ids is None or row["schema_version"] != "historical_node_v1":
        raise ValueError("INVALID_SCHEMA")
    if row["model_id"] not in ("OHLC4_OPEN_HIGH_LOW_CLOSE_V1", "OHLC4_OPEN_LOW_HIGH_CLOSE_V1"):
        raise ValueError("INVALID_SCHEMA")
    if row["model_version"] != "1":
        raise ValueError("INVALID_SCHEMA")
    policy_protocol._plan_hash(row["run_contract_hash"])
    raw_open = row["bar_open_ms"]
    duration = row["bar_duration_ms"]
    step = row["step_index"]
    if type(raw_open) is not int or not 0 <= raw_open < 2**59 or type(duration) is not int or duration != 60_000:
        raise ValueError("INVALID_SCHEMA")
    if type(step) is not int or step not in (0, 1, 2, 3):
        raise ValueError("INVALID_SCHEMA")
    raw = raw_open + (0, 20_000, 40_000, 60_000)[step]
    if raw >= 2**59:
        raise ValueError("INVALID_SCHEMA")
    effective = raw * 16 + (7, 8, 8, 4)[step]
    bars = row["bars"]
    if not isinstance(bars, list) or len(bars) != len(configured_product_ids):
        raise ValueError("INVALID_SCHEMA")
    for product_id, bar_value in zip(configured_product_ids, bars, strict=True):
        pair = policy_protocol._event_object(bar_value, "product_id trade mark")
        if pair["product_id"] != product_id:
            raise ValueError("INVALID_SCHEMA")
        trade = policy_protocol._event_object(
            pair["trade"], "open high low close volume_contracts confirmed source_row_hash"
        )
        mark = policy_protocol._event_object(
            pair["mark"], "open high low close confirmed source_row_hash"
        )
        if trade["confirmed"] is not True or mark["confirmed"] is not True:
            raise ValueError("INVALID_SCHEMA")
        policy_protocol._plan_hash(trade["source_row_hash"])
        policy_protocol._plan_hash(mark["source_row_hash"])
        for values in (trade, mark):
            for field in ("open", "high", "low", "close"):
                price = values[field]
                if type(price) is not D or not price.is_finite() or price <= 0:
                    raise ValueError("INVALID_SCHEMA")
        volume = trade["volume_contracts"]
        if type(volume) is not D or not volume.is_finite() or volume < 0:
            raise ValueError("INVALID_SCHEMA")
    if not isinstance(row["working_orders"], list):
        raise ValueError("INVALID_SCHEMA")
    for order in row["working_orders"]:
        order_row = policy_protocol._event_object(
            order,
            "order_id product_id order_version status remaining_quantity_contracts accepted_at "
            "accepted_source_sequence order_kind side limit_price risk_cancel_pending",
        )
        if (not isinstance(order_row["order_id"], str) or not order_row["order_id"]
                or order_row["product_id"] not in configured_product_ids
                or type(order_row["order_version"]) is not int or order_row["order_version"] < 0
                or order_row["status"] not in ("OPEN", "PARTIALLY_FILLED")
                or order_row["order_kind"] not in ("LIMIT", "MARKET")
                or order_row["side"] not in ("LONG", "SHORT")
                or type(order_row["risk_cancel_pending"]) is not bool):
            raise ValueError("INVALID_SCHEMA")
    node_bytes = wire._encode(value, preserve_null=True).encode("utf-8")
    return raw, effective, node_bytes


class _ScheduleError(Exception):
    pass


class ReplayPersistenceError(Exception):
    """A synchronous evidence barrier failed after an observed result."""


class _EvidenceCallbackError(Exception):
    def __init__(self, original: Exception):
        self.original = original


class _ReplayComposition:
    """One native owner and one source cache, deliberately without a run API."""

    def __init__(self, profile: wire.Profile, account: wire.Account, callback_plans=None, evidence_callback=None, *, configuration: Mapping[str, object] | None = None) -> None:
        configuration_snapshot = (
            json.loads(wire._encode_configuration(configuration))
            if configuration is not None
            else None
        )
        if configuration_snapshot is None:
            self._codec = wire.ScenarioCodec(profile, account)
        else:
            self._codec = wire.ScenarioCodec(profile, account, configuration_snapshot)
        self._configured_product_ids = (tuple(product["product_id"] for product in configuration_snapshot["products"])
                                        if configuration_snapshot is not None else None)
        if profile == "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1":
            configured = cast(dict[str, Any], configuration_snapshot)
            seed_at = configured["seed_effective_at"]
            markets = {}
            for product in configured["products"]:
                active_specs = [
                    row for row in product["specs"]
                    if row["valid_from"] <= seed_at
                    and (row["valid_to"] is None or seed_at < row["valid_to"])
                ]
                active_marks = [
                    row for row in product["marks"]
                    if row["valid_from"] <= seed_at
                    and (row["valid_to"] is None or seed_at < row["valid_to"])
                ]
                spec, mark = active_specs[0], active_marks[0]
                if D(str(spec["multiplier"])) != D(1):
                    raise ValueError("UNSUPPORTED_CONFIGURATION")
                product_id = product["product_id"]
                markets[product_id] = dict(
                    price=D(str(mark["mark"])),
                    ctVal=D(str(spec["contract_value"])),
                    lotSz=D(str(spec["quantity_step"])),
                    minSz=D(str(spec["minimum_quantity"])),
                    increment=D(str(spec["price_tick"])),
                    ratioHL="0.1",
                    state="live",
                    instIdCode=product["instrument_code"],
                )
            self._policy = _closed_policy(profile)
            self._policy.markets = deepcopy(markets)
        else:
            self._policy = _closed_policy(profile)
        self._account = deepcopy(account)
        self._polls: dict[str, _PollRecord] = {}
        self._continuations: dict[str, str] = {}
        self._last_poll_at: int | None = None
        self._callback_plans = deepcopy(callback_plans if callback_plans is not None else {})
        self._queue: list[tuple] = []
        self._records: dict[tuple, dict[str, Any]] = {}
        self._current_time, self._last_popped = 500, None
        self._terminal: dict[str, Any] | None = None
        self._audit: dict[str, list] = {}
        self._evidence_callback = evidence_callback

    def capture_owner_evidence(self, cutoff, trading_request, positions_request, open_orders_request):
        """Observe the owner and scheduler without dispatching or emitting a barrier."""
        _poll_check(type(cutoff) is int and 0 <= cutoff < 2**63)
        requests = deepcopy((trading_request, positions_request, open_orders_request))
        for kind, request in zip(("TRADING", "POSITIONS", "OPEN_ORDERS"), requests, strict=True):
            policy_protocol._plan_snapshot(request)
            _poll_check(request["capture_mode"] == "OWNER_CURRENT" and request["snapshot_kind"] == kind
                        and request["captured_at"] == cutoff and request["account_key"] == self._account)
        owner: dict[str, Any] = dict(cutoff=cutoff, inspection=self._codec.inspect_state())
        for kind, request in zip(("trading", "positions", "open_orders"), requests, strict=True):
            owner[kind + "_request"] = request
            owner[kind + "_fact"] = self._codec.capture_snapshot(deepcopy(request))
        def key(value):
            return dict(visible_at=value[0], queue_class=self._queue_label(value[1]),
                        schedule_sequence=value[2], stable_id=value[3])
        polls = []
        for continuation, poll_id in self._continuations.items():
            observation = self._polls[poll_id].observation
            awaiting = observation.awaiting
            stage = awaiting["snapshot_request"]["snapshot_kind"] if awaiting is not None else None
            polls.append(dict(poll_id=poll_id, continuation_id=continuation, status=observation.status,
                              awaiting="EARN_IF_DUE" if stage == "EARN" else stage))
        actions = [dict(delivery_id=delivery_id, event_index=i, action_index=j, group_id=action["group_id"],
                        status=action["status"], group_result=action.get("group_result"), native_failure=action.get("native_failure"))
                   for delivery_id, events in sorted(self._audit.items()) for i, event in enumerate(events)
                   for j, action in enumerate(event.get("actions", []))]
        observation = dict(current_time=self._current_time, last_popped=key(self._last_popped) if self._last_popped is not None else None,
                           gate="FAILED" if self._terminal is not None else "RUNNING",
                           terminal={name: self._terminal[name] for name in ("kind", "stable_id", "classification", "reason")} if self._terminal is not None else None,
                           pending_keys=[key(value) for value in sorted(self._queue)],
                           records=[dict(key=key(record["key"]), **{name: record["result"][name] for name in ("kind", "stable_id", "classification")})
                                    for _, record in sorted(self._records.items())],
                           polls=sorted(polls, key=lambda row: (row["poll_id"], row["continuation_id"])), callback_actions=actions)
        return deepcopy(dict(owner_evidence=owner, scheduler_observation=observation))

    @staticmethod
    def _queue_label(queue_class: int) -> str:
        return ("SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP")[queue_class]

    def _evidence(self, kind, key, evidence):
        if self._evidence_callback is None:
            return
        scheduler_key = dict(visible_at=key[0], queue_class=self._queue_label(key[1]),
                             schedule_sequence=key[2], stable_id=key[3])
        try:
            self._evidence_callback(kind, deepcopy(scheduler_key), deepcopy(evidence))
        except Exception as exc:
            if type(exc) is ReplayPersistenceError:
                raise _ScheduleError("PERSISTENCE_FAILED") from exc
            raise _EvidenceCallbackError(exc) from exc

    def _callback_evidence(self, record, prefix, events, outcome="SUCCESS", failure=None):
        actions = [dict(event_index=i, action_index=j, group_id=action["group_id"], status=action["status"],
                        group_result=action.get("group_result"), native_failure=action.get("native_failure"))
                   for i, event in enumerate(events) for j, action in enumerate(event.get("actions", []))]
        self._evidence("CALLBACK_RESULT", record["key"], dict(delivery_id=record["item"]["delivery"]["delivery_id"],
                       outcome=outcome, policy_events=list(prefix.events), actions=actions, failure=failure))

    def _scheduler_observation(self):
        result: dict[str, Any] = deepcopy(dict(current_time=self._current_time, last_popped=self._last_popped,
            gate="FAILED" if self._terminal else "RUNNING", terminal=self._terminal,
            pending=sorted(self._queue), records=self._records, callbacks=self._audit))
        for events in result["callbacks"].values():
            for event in events:
                if "actions" in event:
                    count = sum(a["status"] == "SUBMITTED" for a in event["actions"])
                    event["disposition"] = "NONE" if not count else "ALL" if count == len(event["actions"]) else "PARTIAL"
        return result

    def _stop_scheduler(self, exc, kind, stable_id=None, prefix=None, group=None):
        native = wire._native_failure(exc)
        if native is None and type(exc) is not _ScheduleError:
            raise exc
        result = dict(kind=kind, stable_id=stable_id, classification="TERMINAL",
                      reason=native["reason"] if native else exc.args[0], events=deepcopy(prefix if prefix is not None else self._audit.get(stable_id or "", [])))
        if native is not None:
            result["native_failure"] = native
        if group is not None:
            result["group_result"] = deepcopy(group)
        result["callbacks"] = self._scheduler_observation()["callbacks"]
        self._terminal = deepcopy(result)
        return deepcopy(result)

    def _queue_record(self, item, plan=None):
        try:
            kind = item.get("kind") if isinstance(item, dict) else None
            raw = 0
            if kind == "SOURCE_GROUP":
                row = cast(dict[str, Any], policy_protocol._event_object(item, "kind schedule_sequence group"))
                group = row["group"]
                content = policy_protocol._plan_sequence(row["schedule_sequence"]) + policy_protocol._plan_group(
                    group, self._configured_product_ids)
                account, at, sequence, stable, cls = group["account_key"], group["group_effective_at"], row["schedule_sequence"], group["group_id"], 0
            elif kind == "SNAPSHOT_CAPTURE":
                row = cast(dict[str, Any], policy_protocol._event_object(item, "kind capture_sequence request delivery_projection"))
                request, projection = row["request"], row["delivery_projection"]
                content = policy_protocol._plan_sequence(row["capture_sequence"]) + policy_protocol._plan_snapshot(request) + policy_protocol._plan_projection(projection)
                account, at, sequence, stable, cls = request["account_key"], request["captured_at"], row["capture_sequence"], request["snapshot_id"], 1
            elif kind == "DELIVERY":
                row = cast(dict[str, Any], policy_protocol._event_object(item, "kind delivery"))
                d = policy_protocol._event_object(row["delivery"], "account_key delivery_id source_fact_id source_namespace payload_kind occurrence_index schedule_sequence immutable_payload payload_digest visible_at", "snapshot_version snapshot_as_of continuation_id")
                projection = dict(schema_version="delivery_projection_v1", reference=dict(namespace=d["source_namespace"], fact_id=d["source_fact_id"]),
                    **{k: d[k] for k in ("payload_kind", "occurrence_index", "schedule_sequence", "visible_at")}, continuation_id=d.get("continuation_id"))
                policy_protocol._plan_projection(projection)
                policy_protocol._event_id(d["delivery_id"])
                content = (policy_protocol._plan_hash(d["payload_digest"]),
                           policy_protocol._emission_plan_digest(plan, self._configured_product_ids))
                account, at, sequence, stable, cls = d["account_key"], d["visible_at"], d["schedule_sequence"], d["delivery_id"], 2
            elif kind == "HISTORICAL_MARKET_STEP":
                row = cast(dict[str, Any], policy_protocol._event_object(
                    item, "kind schedule_sequence stable_id node"
                ))
                sequence = row["schedule_sequence"]
                stable = row["stable_id"]
                sequence_bytes = policy_protocol._plan_sequence(sequence)
                stable_bytes = policy_protocol._event_id(stable)
                try:
                    raw, at, node_bytes = _historical_node_clock(row["node"], self._configured_product_ids)
                except (ValueError, UnicodeError, OverflowError) as exc:
                    raise _ScheduleError("INVALID_SCHEMA") from exc
                content = sequence_bytes + stable_bytes + node_bytes
                account, cls = self._account, 3
            else:
                raise _ScheduleError("INVALID_SCHEMA")
            if policy_protocol._plan_account(account) != policy_protocol._plan_account(self._account):
                raise _ScheduleError("INVALID_SCHEMA")
            if kind == "HISTORICAL_MARKET_STEP":
                return dict(key=(at, cls, sequence, stable), content=content, item=deepcopy(item), plan=None,
                            raw_time_ms=raw,
                            result=dict(kind=kind, stable_id=stable, classification="PENDING"))
            return dict(key=(at, cls, sequence, stable), content=content, item=deepcopy(item), plan=deepcopy(plan),
                        result=dict(kind=kind, stable_id=stable, classification="PENDING"))
        except (ValueError, UnicodeError, OverflowError) as exc:
            if isinstance(exc, (UnicodeError, OverflowError)) or exc.args == ("POLICY_EMISSION_MISMATCH",):
                raise _ScheduleError("INVALID_SCHEMA") from exc
            raise

    def _admit_set(self, items, commit=True):
        prospective, added, results = dict(self._records), [], []
        for item, plan in items:
            record = self._queue_record(item, plan)
            at, cls, sequence, stable = key = record["key"]
            identity = (cls, stable)
            old = prospective.get(identity)
            if old is not None:
                if old["content"] != record["content"]:
                    raise _ScheduleError("DELIVERY_ID_CONFLICT" if cls == 2 else "INVALID_SCHEMA")
                results.append(old["result"])
                continue
            if at < self._current_time or (self._last_popped is not None and key <= self._last_popped):
                raise _ScheduleError("INVALID_SCHEMA")
            for other in prospective.values():
                other_key = other["key"]
                if key[:3] == other_key[:3] or (cls == other_key[1] == 0 and at == other_key[0]):
                    raise _ScheduleError("INVALID_SCHEMA")
            if cls == 1:
                request, projection = item["request"], item["delivery_projection"]
                if projection["visible_at"] < at or projection["reference"] != dict(namespace="SNAPSHOT", fact_id=request["snapshot_id"]):
                    raise _ScheduleError("INVALID_SCHEMA")
            if cls == 2 and plan["delivery_id"] != stable:
                raise _ScheduleError("INVALID_SCHEMA")
            prospective[identity] = record
            added.append(record)
            results.append(record["result"])
        if commit:
            for record in added:
                key = record["key"]
                self._records[(key[1], key[3])] = record
                heapq.heappush(self._queue, key)
        return deepcopy(results)

    def _enqueue(self, item, plan=None):
        if self._terminal is not None:
            return deepcopy(self._terminal)
        try:
            return self._admit_set([(item, plan)])[0]
        except Exception as exc:
            kind = item.get("kind") if isinstance(item, dict) else "QUEUE"
            kind = kind if isinstance(kind, str) and kind in ("SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY", "HISTORICAL_MARKET_STEP") else "QUEUE"
            field, identity = {"SOURCE_GROUP": ("group", "group_id"), "SNAPSHOT_CAPTURE": ("request", "snapshot_id"), "DELIVERY": ("delivery", "delivery_id"), "HISTORICAL_MARKET_STEP": ("", "stable_id")}.get(kind or "", ("", ""))
            value = item.get(field) if isinstance(item, dict) else None
            if kind == "HISTORICAL_MARKET_STEP":
                value = item
            return self._stop_scheduler(exc, kind, value.get(identity) if isinstance(value, dict) else None)

    @staticmethod
    def _capture_item(stage):
        return dict(kind="SNAPSHOT_CAPTURE", capture_sequence=stage["capture_sequence"],
                    request=stage["snapshot_request"], delivery_projection=stage["delivery_projection"])

    def _begin_poll(self, plan):
        if self._terminal is not None:
            return deepcopy(self._terminal)
        def preflight(issued, stage):
            if issued < self._current_time or any(key[0] < issued for key in self._queue):
                raise _ScheduleError("INVALID_SCHEMA")
            self._admit_set([(self._capture_item(stage), None)], commit=False)
        prefix = []
        try:
            try:
                result = self._start_poll(plan, preflight)
            except ValueError as exc:
                if exc.args != ("INVALID_SCHEMA",):
                    raise
                raise _ScheduleError("INVALID_SCHEMA") from exc
            if result.observation.status == "CALLBACK_FAILED":
                prefix = list(result.observation.prefix.events)
                raise _ScheduleError("CALLBACK_FAILED")
            if result.clock_advance is not None:
                self._current_time = result.clock_advance
                self._admit_set([(self._capture_item(result.next_stage), None)])
            return dict(kind="POLL", stable_id=plan["poll_id"], classification="SUCCESS", status=result.observation.status,
                        events=deepcopy(result.observation.prefix.events))
        except Exception as exc:
            return self._stop_scheduler(exc, "POLL", plan.get("poll_id") if isinstance(plan, dict) else None, prefix=prefix)

    def _deliver_queued(self, record):
        delivery, plan = record["item"]["delivery"], record["plan"]
        self._evidence("DELIVERY_ATTEMPT", record["key"], dict(delivery=delivery,
                       emission_plan_digest=policy_protocol._emission_plan_digest(
                           plan, self._configured_product_ids) if plan is not None else None))
        next_stage = None
        if delivery.get("continuation_id") is not None:
            try:
                result = self._resume_poll(delivery)
            except ValueError as exc:
                if exc.args != ("INVALID_SCHEMA",):
                    raise
                raise _ScheduleError("INVALID_SCHEMA") from exc
            prefix, next_stage = result.observation.prefix, result.next_stage
        else:
            prefix = self._capture_callback(delivery)
        events: list[dict[str, Any]] = [dict(event=deepcopy(e), **({"actions": [dict(group_id=None, status="UNSUBMITTED") for _ in cast(list, e["orders"])]}
                  if e["kind"] in ("send", "cancel") else {})) for e in prefix.events]
        self._audit[delivery["delivery_id"]] = events
        if prefix.exception is not None:
            self._callback_evidence(record, prefix, events, "CALLBACK_FAILED", dict(kind="CALLBACK", reason="CALLBACK_FAILED"))
            raise _ScheduleError("CALLBACK_FAILED")
        try:
            validated = _validate_emission_plan(self._account, delivery, prefix.events, plan,
                                                self._configured_product_ids)
        except ValueError as exc:
            if exc.args not in (("POLICY_EMISSION_MISMATCH",), ("MISSING_NEXT_EVENT_STAMP",)):
                raise
            self._callback_evidence(record, prefix, events, "PLAN_FAILED", dict(kind="PLAN", reason=exc.args[0]))
            raise _ScheduleError(exc.args[0]) from exc
        financial = iter(plan["financial_items"])
        for event in events:
            for action in event.get("actions", []):
                action["group_id"] = next(financial)["expected_group"]["group_id"]
        self._callback_evidence(record, prefix, events)
        derived = [(dict(kind="SOURCE_GROUP", schedule_sequence=v["schedule_sequence"], group=v["expected_group"]), None)
                   if "expected_group" in v else (self._capture_item(dict(capture_sequence=v["capture_sequence"],
                       snapshot_request=v["snapshot_request"], delivery_projection=v["delivery_projection"])), None) for v in validated]
        if next_stage is not None:
            derived.append((self._capture_item(next_stage), None))
        self._admit_set(derived)
        return deepcopy(events)

    def _dispatch_due(self, until):
        if self._terminal is not None:
            return deepcopy(self._terminal)
        record = None
        try:
            if type(until) is not int or not 0 <= until < 2**63 or until < self._current_time:
                raise _ScheduleError("INVALID_SCHEMA")
            while self._queue and self._queue[0][0] <= until:
                key = heapq.heappop(self._queue)
                self._current_time, self._last_popped = key[0], key
                record = self._records[(key[1], key[3])]
                item, kind = record["item"], record["item"]["kind"]
                result = dict(kind=kind, stable_id=key[3], classification="SUCCESS")
                if kind == "SOURCE_GROUP":
                    actions = [a for events in self._audit.values() for e in events for a in e.get("actions", []) if a["group_id"] == key[3]]
                    for action in actions:
                        action["status"] = "SUBMITTED"
                    try:
                        group = self._codec.apply_group(item["group"])
                    except Exception as exc:
                        native = wire._native_failure(exc)
                        if native is not None:
                            for action in actions:
                                action["native_failure"] = native
                        raise
                    result["group_result"] = group
                    for action in actions:
                        action["group_result"] = deepcopy(group)
                    record["result"] = deepcopy(result)
                    self._evidence("SOURCE_GROUP_RESULT", key, dict(request=item["group"], result=group))
                    if group["classification"] == "FAULT":
                        record["result"] = self._stop_scheduler(_ScheduleError(cast(str, group.get("failure"))), kind, key[3], group=group)
                        return deepcopy(record["result"])
                elif kind == "SNAPSHOT_CAPTURE":
                    fact = self._codec.capture_snapshot(item["request"])
                    self._evidence("SNAPSHOT_FACT", key, dict(purpose="POLL", request=item["request"], fact=fact))
                    projection = deepcopy(item["delivery_projection"])
                    projection["reference"] = fact["reference"]
                    delivery = self._codec.build_delivery(projection)
                    plan = self._callback_plans.get(delivery["delivery_id"])
                    if plan is None:
                        raise _ScheduleError("INVALID_SCHEMA")
                    self._admit_set([(dict(kind="DELIVERY", delivery=delivery), plan)])
                elif kind == "DELIVERY":
                    result["events"] = self._deliver_queued(record)
                else:
                    historical_result = self._codec.historical_market_step(item["node"])
                    if (type(historical_result.get("raw_time_ms")) is not int
                            or historical_result["raw_time_ms"] != record["raw_time_ms"]
                            or type(historical_result.get("effective_at")) is not int
                            or historical_result["effective_at"] != key[0]):
                        raise _ScheduleError("INVALID_SCHEMA")
                    result["historical_result"] = deepcopy(historical_result)
                record["result"] = deepcopy(result)
            self._current_time = until
            return dict(kind="DISPATCH", stable_id=None, classification="SUCCESS")
        except _EvidenceCallbackError as exc:
            raise exc.original
        except Exception as exc:
            result = self._stop_scheduler(exc, record["item"]["kind"] if record else "DISPATCH", record["key"][3] if record else None)
            if record is not None:
                record["result"] = deepcopy(result)
            return result

    def _start_poll(self, plan, preflight: Callable[[int, dict[str, Any]], None]) -> _PollResult:
        try:
            digest = policy_protocol._poll_occurrence_plan_digest(plan)
            account = policy_protocol._plan_account(plan["account_key"])
        except ValueError as exc:
            if exc.args != ("POLICY_EMISSION_MISMATCH",):
                raise
            raise ValueError("INVALID_SCHEMA") from exc
        _poll_check(account == policy_protocol._plan_account(self._account))
        poll_id, continuation = plan["poll_id"], plan["continuation_id"]
        if poll_id in self._polls:
            record = self._polls[poll_id]
            _poll_check(record.digest == digest)
            return _PollResult(deepcopy(record.observation))
        _poll_check(continuation not in self._continuations)
        policy, issued = self._policy, plan["issued_at"]
        if policy.paused:
            observation = _PollObservation("PAUSED", None, _CallbackPrefix((), None))
            self._polls[poll_id] = _PollRecord(digest, [], 0, observation)
            self._continuations[continuation] = poll_id
            return _PollResult(deepcopy(observation))
        _poll_check(self._last_poll_at is None or issued >= self._last_poll_at)
        due = issued - policy.last_earn_ms > 60000
        _poll_check((plan.get("earn") is not None) == due)
        kinds = [("earn", "EARN", "EARN_SNAPSHOT")] if due else []
        kinds += [("trading", "TRADING", "TRADING_SNAPSHOT"), ("positions", "POSITIONS", "POSITION_SNAPSHOT"),
                  ("open_orders", "OPEN_ORDERS", "OPEN_ORDER_SNAPSHOT")]
        stages = []
        previous = None
        for key, kind, payload_kind in kinds:
            stage = plan[key]
            request, projection = stage["snapshot_request"], stage["delivery_projection"]
            _poll_check(policy_protocol._plan_account(request["account_key"]) == account)
            _poll_check(request.get("continuation_id") == projection.get("continuation_id") == continuation)
            _poll_check(request["snapshot_kind"] == kind and projection["payload_kind"] == payload_kind)
            _poll_check(projection["reference"] == dict(namespace="SNAPSHOT", fact_id=request["snapshot_id"]))
            _poll_check(projection.get("transport") is None)
            captured, visible = request["captured_at"], projection["visible_at"]
            _poll_check(captured >= issued if previous is None else captured > previous)
            _poll_check(visible >= captured)
            stages.append(deepcopy(stage))
            previous = visible
        preflight(issued, deepcopy(stages[0]))
        self._last_poll_at = issued
        self._continuations[continuation] = poll_id
        start = len(policy.events)
        policy.now_ms = issued
        error = None
        try:
            if due:
                policy.last_earn_ms = issued
                policy.emit("request_earn")
        except Exception as exc:
            error = exc
        prefix = _CallbackPrefix(tuple(deepcopy(policy.events[start:])), error)
        observation = _PollObservation("CALLBACK_FAILED" if error else "IN_PROGRESS", None if error else stages[0], prefix)
        self._polls[poll_id] = _PollRecord(digest, stages, 0, observation)
        return _PollResult(deepcopy(observation), None if error else issued, None if error else deepcopy(stages[0]))

    def _resume_poll(self, delivery: wire.Delivery) -> _PollResult:
        continuation = delivery.get("continuation_id")
        _poll_check(continuation in self._continuations)
        record = self._polls[self._continuations[cast(str, continuation)]]
        _poll_check(record.observation.status == "IN_PROGRESS")
        stage = record.stages[record.index]
        request, projection = stage["snapshot_request"], stage["delivery_projection"]
        _poll_check(policy_protocol._plan_account(delivery["account_key"]) == policy_protocol._plan_account(self._account))
        _poll_check(delivery["source_namespace"] == projection["reference"]["namespace"] == "SNAPSHOT")
        _poll_check(delivery["source_fact_id"] == projection["reference"]["fact_id"] == request["snapshot_id"])
        for key in ("payload_kind", "occurrence_index", "schedule_sequence", "visible_at", "continuation_id"):
            _poll_check(delivery.get(key) == projection.get(key))
        policy, start = self._policy, len(self._policy.events)
        error = None
        try:
            self._apply_payload(delivery)
            if record.index == len(record.stages) - 1:
                policy.raise_leverage()
                if policy.running and not policy.ws_open:
                    policy.emit("check_websocket")
                policy.check_risk()
        except Exception as exc:
            error = exc
        record.index += 1
        next_stage = None if error or record.index == len(record.stages) else record.stages[record.index]
        prefix = _CallbackPrefix(tuple(deepcopy(policy.events[start:])), error)
        status = "CALLBACK_FAILED" if error else "COMPLETED" if next_stage is None else "IN_PROGRESS"
        record.observation = _PollObservation(status, next_stage, prefix)
        return _PollResult(deepcopy(record.observation), next_stage=deepcopy(next_stage))

    def _capture_callback(self, delivery: wire.Delivery) -> _CallbackPrefix:
        start = len(self._policy.events)
        error = None
        try:
            self._apply_payload(delivery)
        except Exception as exc:
            error = exc
        return _CallbackPrefix(tuple(deepcopy(self._policy.events[start:])), error)

    def _apply_payload(self, delivery: wire.Delivery) -> None:
        policy, kind = self._policy, delivery["payload_kind"]
        payload = delivery["immutable_payload"]
        if kind == "EXECUTION_FACT":
            fact = cast(wire.ExecutionFact, payload)
            policy.orders(
                dict(
                    ordId=fact["order_id"],
                    clOrdId=fact["policy_client_order_id"],
                    instId=fact["product_id"],
                    px=fmt(fact["limit_price"]),
                    fillPx=fmt(fact["fill_price"]),
                    sz=fmt(fact["original_size_contracts"]),
                    accFillSz=fmt(fact["cumulative_filled_size_contracts"]),
                    cTime=str(fact["execution_effective_at"]),
                    state=fact["state"],
                    side=fact["side"],
                )
            )
        elif kind == "TRANSPORT_ACK":
            ack = cast(wire.Transport, payload)
            if ack["route"] == "WS":
                return
            entry = {"clOrdId": ack["client_order_id"], "sCode": ack["code"]}
            for source, target in [
                ("order_id", "ordId"),
                ("message", "sMsg"),
                ("product_id", "instId"),
                ("side", "side"),
                ("limit_price", "px"),
                ("size_contracts", "sz"),
            ]:
                value = ack.get(source)
                if value is not None:
                    entry[target] = fmt(value) if isinstance(value, D) else str(value)
            policy.rest_ack("send" if ack["operation"] == "ORDER" else "cancel", [entry])
        elif kind == "MARKET_SNAPSHOT":
            market = cast(wire.MarketSnapshot, payload)
            policy.apply_market(
                {
                    row["product_id"]: dict(
                        price=row["price"],
                        ctVal=row["contract_value"],
                        lotSz=row["lot_size"],
                        minSz=row["minimum_size"],
                        increment=row["price_increment"],
                        ratioHL=row["high_low_ratio"],
                        state=row["state"],
                        instIdCode=row["instrument_code"],
                    )
                    for row in market["markets"]
                }
            )
        elif cast(wire.Failure, payload).get("outcome") == "FAILURE":
            return
        elif kind == "EARN_SNAPSHOT":
            policy.capital["earn"] = cast(wire.Earn, payload)["earn"]
        elif kind == "TRADING_SNAPSHOT":
            trading = cast(wire.Trading, payload)
            policy.capital.update(usdt=trading["equity"], avail=trading["available_equity"])
            if "earn" not in policy.capital:
                raise ValueError("Source total is NaN before an earn snapshot exists")
            with localcontext() as context:
                context.prec = 50
                policy.capital["total"] = policy.a("usdt") + policy.a("earn") - policy.p("fixed")
        elif kind == "POSITION_SNAPSHOT":
            positions = cast(wire.Positions, payload)
            policy.positions_response(
                [
                    dict(
                        instId=row["product_id"],
                        mgnMode=row["margin_mode"],
                        pos=row["position_contracts"],
                        **({"last": row["last_price"]} if "last_price" in row else {}),
                        **({"notionalUsd": row["notional_usd"]} if "notional_usd" in row else {}),
                    )
                    for row in positions["rows"]
                ]
            )
        elif kind == "OPEN_ORDER_SNAPSHOT":
            orders = cast(wire.OpenOrders, payload)
            policy.open_orders(
                [
                    dict(
                        ordId=row["order_id"],
                        clOrdId=row["client_order_id"],
                        instId=row["product_id"],
                        state=row["state"],
                        side=row["side"],
                        px=fmt(row["limit_price"]),
                        sz=fmt(row["original_size_contracts"]),
                        accFillSz=fmt(row["cumulative_filled_size_contracts"]),
                        cTime=str(row["created_at"]),
                    )
                    for row in orders["rows"]
                ]
            )
