"""Pure journal/endpoint structures, never completion or runtime authority."""

from typing import cast

from src.core.backtest import spider_run_native_schema as native
from src.core.backtest.spider_run_artifacts import (
    ConfigurationContext, _boolean, _boundary, _decimal_text, _enum, _integer,
    _list, _object, _require, _text, configuration_context,
)

_CLASSES = ["SOURCE_GROUP", "SNAPSHOT_CAPTURE", "DELIVERY"]
_KINDS = ["SOURCE_GROUP_RESULT", "SNAPSHOT_FACT", "DELIVERY_ATTEMPT", "CALLBACK_RESULT"]


def _string(value: object) -> None:
    _require(type(value) is str)
    try:
        cast(str, value).encode("utf-8")
    except UnicodeEncodeError:
        _require(False)


def _policy_id(value: object) -> None:
    _text(value)
    _string(value)


def _code(value: object) -> None:
    _text(value, "[A-Z][A-Z0-9_]{0,127}")


def _ordered(keys: list[tuple]) -> None:
    _require(keys == sorted(set(keys)))


def _context(row: dict[str, object]) -> ConfigurationContext | None:
    if "configuration_context" not in row:
        return None
    return configuration_context(row["configuration_context"])


def _policy_order(value: object, operation: str) -> None:
    if operation == "cancel":
        row = _object(value, "ordId instId instIdCode")
        _policy_id(row["ordId"])
    else:
        row = _object(value, "instId instIdCode tdMode clOrdId tag side ordType px sz")
        _policy_id(row["clOrdId"])
        _string(row["tdMode"])
        _string(row["tag"])
        _enum(row["side"], ["buy", "sell"])
        kind = _enum(row["ordType"], ["limit", "market"])
        if kind == "market":
            _require(type(row["px"]) is str and row["px"] == "")
        else:
            _decimal_text(row["px"])
        _decimal_text(row["sz"])
    _policy_id(row["instId"])
    _require(type(row["instIdCode"]) is int and -(1 << 63) <= cast(int, row["instIdCode"]) < (1 << 63))


def _policy_event(value: object) -> None:
    _require(type(value) is dict)
    event = cast(dict[str, object], value)
    _require(_integer(event.get("at_ms")) < 1 << 63)
    kind = _enum(event.get("kind"), ["request_market", "request_earn", "check_websocket", "alert", "send", "cancel", "console_only", "unsupported_source_path"])
    if kind in ("request_market", "request_earn", "check_websocket"):
        _object(event, "at_ms kind")
    elif kind == "alert":
        reason = _enum(event.get("reason"), ["offline", "reset_step", "high_low_limit", "individual_limit", "total_limit", "daily_stop"])
        extra = " operation" if reason == "offline" else " name" if reason in ("reset_step", "high_low_limit", "individual_limit") else ""
        _object(event, "at_ms kind reason" + extra)
        if reason == "offline":
            _enum(event["operation"], ["send", "cancel"])
        elif extra:
            _string(event["name"])
    elif kind in ("send", "cancel", "console_only"):
        _object(event, "at_ms kind operation route orders")
        operation = _enum(event["operation"], ["send", "cancel"])
        _require(kind == "console_only" or kind == operation)
        _enum(event["route"], ["REST", "WS"])
        for order in _list(event["orders"]):
            _policy_order(order, operation)
    else:
        _object(event, "at_ms kind reason order")
        _enum(event["reason"], ["non_grid_fill_enters_orderFilled"])
        row = _object(event["order"], "instId side px clOrdId state sz accFillSz", "cTime sMsg")
        _policy_id(row["instId"])
        _policy_id(row["clOrdId"])
        _enum(row["side"], ["buy", "sell"])
        _string(row["state"])
        _string(row["px"])
        if row["px"] != "":
            _decimal_text(row["px"])
        _decimal_text(row["sz"])
        _decimal_text(row["accFillSz"])
        for key in ("cTime", "sMsg"):
            if key in row and row[key] is not None:
                _string(row[key])


def owner_evidence(value: object, *, context: ConfigurationContext | None = None) -> None:
    row = _object(value, "cutoff inspection trading_request trading_fact positions_request positions_fact open_orders_request open_orders_fact")
    _integer(row["cutoff"])
    native.inspection(row["inspection"], context=context)
    for name in ("trading", "positions", "open_orders"):
        native.snapshot_request(row[name + "_request"])
        native.snapshot_fact(row[name + "_fact"], context=context)
        request = cast(dict[str, object], row[name + "_request"])
        fact = cast(dict[str, object], row[name + "_fact"])
        _require(request["capture_mode"] == "OWNER_CURRENT")
        _require(request["snapshot_kind"] == fact["snapshot_kind"] == name.upper())


def scheduler_key(value: object) -> tuple[int, int, int, str]:
    row = _object(value, "visible_at queue_class schedule_sequence stable_id")
    return (_integer(row["visible_at"]), _CLASSES.index(_enum(row["queue_class"], _CLASSES)),
            _integer(row["schedule_sequence"]), _text(row["stable_id"]))


def callback_action(value: object, *, observed: bool = False) -> tuple:
    row = _object(value, "event_index action_index group_id status group_result native_failure" + (" delivery_id" if observed else ""))
    pair = (_integer(row["event_index"]), _integer(row["action_index"]))
    _enum(row["status"], ["UNSUBMITTED", "SUBMITTED"])
    if row["group_id"] is not None:
        _text(row["group_id"])
    if row["group_result"] is not None:
        native.group_result(row["group_result"])
    if row["native_failure"] is not None:
        failure = _object(row["native_failure"], "reason poisoned")
        _code(failure["reason"])
        _boolean(failure["poisoned"])
    return (_text(row["delivery_id"]), *pair) if observed else pair


def callback_result(value: object) -> None:
    row = _object(value, "delivery_id outcome policy_events actions failure")
    _text(row["delivery_id"])
    _enum(row["outcome"], ["SUCCESS", "CALLBACK_FAILED", "PLAN_FAILED"])
    pairs: list[tuple] = []
    for index, item in enumerate(_list(row["policy_events"])):
        _policy_event(item)
        event = cast(dict[str, object], item)
        if event["kind"] in ("send", "cancel"):
            pairs.extend((index, order) for order in range(len(_list(event["orders"]))))
    actual = [callback_action(item) for item in _list(row["actions"])]
    _ordered(actual)
    _require(actual == pairs)
    if row["failure"] is not None:
        failure = _object(row["failure"], "kind reason")
        _enum(failure["kind"], ["CALLBACK", "PLAN", "NATIVE"])
        _code(failure["reason"])


def journal_record(value: object) -> None:
    row = _object(value, "schema_version run_id journal_seq barrier_id record_kind scheduler_key causal_parent_ids effective_at visible_at account_version_before account_version_after payload", "configuration_context")
    context = _context(row)
    _enum(row["schema_version"], ["spider_journal_record_v1"])
    _text(row["run_id"], "[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    _require(_integer(row["journal_seq"]) > 0)
    _text(row["barrier_id"])
    kind = _enum(row["record_kind"], _KINDS)
    key = scheduler_key(row["scheduler_key"])
    _require(key[1] == (0 if kind == "SOURCE_GROUP_RESULT" else 1 if kind == "SNAPSHOT_FACT" else 2))
    parents = [_text(item) for item in _list(row["causal_parent_ids"])]
    _require(len(parents) == len(set(parents)))
    _integer(row["effective_at"])
    _integer(row["visible_at"])
    for key in ("account_version_before", "account_version_after"):
        if kind == "SOURCE_GROUP_RESULT":
            _integer(row[key])
        else:
            _require(row[key] is None)
    if kind == "SOURCE_GROUP_RESULT":
        payload = _object(row["payload"], "request result owner_evidence_before owner_evidence_after")
        native.group(payload["request"], context=context)
        native.group_result(payload["result"])
        owner_evidence(payload["owner_evidence_before"], context=context)
        owner_evidence(payload["owner_evidence_after"], context=context)
    elif kind == "SNAPSHOT_FACT":
        payload = _object(row["payload"], "purpose request fact")
        _enum(payload["purpose"], ["INITIAL", "POST_GROUP", "POLL", "FINAL"])
        native.snapshot_request(payload["request"])
        native.snapshot_fact(payload["fact"], context=context)
    elif kind == "DELIVERY_ATTEMPT":
        payload = _object(row["payload"], "delivery emission_plan_digest")
        native.delivery(payload["delivery"], context=context)
        if payload["emission_plan_digest"] is not None:
            _text(payload["emission_plan_digest"], "[0-9a-f]{64}")
    else:
        callback_result(row["payload"])


def journal(value: object) -> None:
    barriers: list[str] = []
    for sequence, item in enumerate(_list(value), 1):
        journal_record(item)
        row = cast(dict[str, object], item)
        _require(row["journal_seq"] == sequence)
        barriers.append(_text(row["barrier_id"]))
    _require(len(barriers) == len(set(barriers)))


def scheduler_terminal(value: object) -> None:
    if value is None:
        return
    row = _object(value, "kind stable_id classification reason")
    kind = _enum(row["kind"], [*_CLASSES, "POLL", "DISPATCH", "QUEUE"])
    if row["stable_id"] is not None or kind not in ("DISPATCH", "QUEUE"):
        _text(row["stable_id"])
    _enum(row["classification"], ["TERMINAL"])
    _code(row["reason"])


def poll(value: object) -> tuple[str, str]:
    row = _object(value, "poll_id continuation_id status awaiting")
    _enum(row["status"], ["PAUSED", "IN_PROGRESS", "COMPLETED", "CALLBACK_FAILED"])
    if row["awaiting"] is not None:
        _enum(row["awaiting"], ["EARN_IF_DUE", "TRADING", "POSITIONS", "OPEN_ORDERS", "LOCAL_ACTIONS"])
    return _text(row["poll_id"]), _text(row["continuation_id"])


def scheduler_observation(value: object) -> None:
    row = _object(value, "current_time last_popped gate terminal pending_keys records polls callback_actions")
    _integer(row["current_time"])
    if row["last_popped"] is not None:
        scheduler_key(row["last_popped"])
    _enum(row["gate"], ["RUNNING", "FAILED"])
    scheduler_terminal(row["terminal"])
    _ordered([scheduler_key(item) for item in _list(row["pending_keys"])])
    records: list[tuple] = []
    for item in _list(row["records"]):
        record = _object(item, "key kind stable_id classification")
        key = scheduler_key(record["key"])
        identity = (_CLASSES.index(_enum(record["kind"], _CLASSES)), _text(record["stable_id"]))
        _require(identity == (key[1], key[3]))
        _enum(record["classification"], ["PENDING", "SUCCESS", "TERMINAL"])
        records.append(identity)
    _ordered(records)
    polls = [poll(item) for item in _list(row["polls"])]
    _ordered(polls)
    _require(len({item[0] for item in polls}) == len(polls) == len({item[1] for item in polls}))
    _ordered([callback_action(item, observed=True) for item in _list(row["callback_actions"])])


def endpoint(value: object) -> None:
    row = _object(value, "schema_version run_id terminal_reason cutoff initial_owner_evidence final_owner_evidence scheduler_observation remaining_planned_barriers", "configuration_context")
    context = _context(row)
    _enum(row["schema_version"], ["spider_endpoint_v1"])
    _text(row["run_id"], "[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    _enum(row["terminal_reason"], ["SCHEDULED_MTM", "LEGAL_NATIVE_LIQUIDATION_FINAL_EVENT", "O03_NON_ATOMIC_COMPLETE"])
    cutoff = _object(row["cutoff"], "scheduler_time persisted_boundary")
    _integer(cutoff["scheduler_time"])
    _boundary(cutoff["persisted_boundary"])
    owner_evidence(row["initial_owner_evidence"], context=context)
    owner_evidence(row["final_owner_evidence"], context=context)
    scheduler_observation(row["scheduler_observation"])
    barriers: list[tuple] = []
    for item in _list(row["remaining_planned_barriers"]):
        barrier = _object(item, "ordinal barrier_id record_kind")
        _require(_integer(barrier["ordinal"]) > 0)
        _enum(barrier["record_kind"], _KINDS)
        barriers.append((barrier["ordinal"], _text(barrier["barrier_id"])))
    _ordered(barriers)
    _require(len({item[0] for item in barriers}) == len(barriers) == len({item[1] for item in barriers}))
