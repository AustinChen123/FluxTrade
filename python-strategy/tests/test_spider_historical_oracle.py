"""Independent causal closure for the frozen H02-H04 oracle cases."""

from dataclasses import replace
from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256
import json

from src.core.backtest.spider_historical_input import validate_historical_input
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_historical_input import (
    ANSWERS,
    ANSWERS_SHA256,
    INPUTS,
    INPUTS_SHA256,
    _initial_account_state,
    _manifest,
    _p2_configuration,
    _policy_cache,
    _rehashed_run,
    _run_with_configuration,
    _valid_run,
)


EPOCH = 1_790_640_000_000
ORDERS = {
    "H02": [
        ("H02-A-LONG", "A-USDT-SWAP", "LONG", "100", "2", "0000015006"),
        ("H02-A-SHORT", "A-USDT-SWAP", "SHORT", "200", "1", "0000025008"),
        ("H02-B-LONG", "B-USDT-SWAP", "LONG", "50", "1", "0000035006"),
        ("H02-B-SHORT", "B-USDT-SWAP", "SHORT", "200", "1", "0000045008"),
    ],
    "H04": [
        ("H04-ORDER-1", "A-USDT-SWAP", "SHORT", "105", "3", "H04-CLIENT-1"),
    ],
}


def _record(path, digest, case):
    raw = path.read_bytes()
    assert sha256(raw).hexdigest() == digest
    line = next(
        row for row in raw.decode("utf-8").splitlines() if row.startswith(case + "|")
    )
    fields = {}
    for field in line.split("|")[1:]:
        key, separator, value = field.partition("=")
        assert separator and key not in fields
        fields[key] = value
    assert line.startswith(case + "|")
    fields["case"] = case
    return fields


def _oracle_run(case):
    source = _record(INPUTS, INPUTS_SHA256, case)
    base = _valid_run()
    start = EPOCH
    end_offset = {"H02": 20_002, "H03": 100_001, "H04": 100_001}[case]
    end = start + end_offset
    delta = start - base.range_start_ms
    trade_rows = tuple(
        replace(row, bar_open_ms=row.bar_open_ms + delta) for row in base.trade_bars
    )
    mark_rows = tuple(
        replace(row, bar_open_ms=row.bar_open_ms + delta) for row in base.mark_bars
    )
    if end > trade_rows[-1].bar_open_ms + 60_000:
        for product in base.ordered_products:
            trade_rows += (
                replace(
                    next(
                        row
                        for row in trade_rows
                        if row.product_id == product and row.bar_open_ms == start
                    ),
                    bar_open_ms=start + 60_000,
                    source_sequence=max(
                        row.source_sequence
                        for row in trade_rows
                        if row.product_id == product
                    )
                    + 1,
                ),
            )
            mark_rows += (
                replace(
                    next(
                        row
                        for row in mark_rows
                        if row.product_id == product and row.bar_open_ms == start
                    ),
                    bar_open_ms=start + 60_000,
                    source_sequence=max(
                        row.source_sequence
                        for row in mark_rows
                        if row.product_id == product
                    )
                    + 1,
                ),
            )

    bars = {}
    product = None
    ids = {"A": "A-USDT-SWAP", "B": "B-USDT-SWAP"}
    for entry in source["bars"].split(";"):
        if ":" in entry:
            label, entry = entry.split(":", 1)
            product = ids[label]
        offset, raw_values = entry.split("=", 1)
        timestamp = start + (0 if offset == "E" else int(offset.removeprefix("E+")))
        bars[(product, timestamp)] = tuple(D(value) for value in raw_values.split("/"))

    def apply(rows, trade):
        output = []
        for row in rows:
            values = bars.get((row.product_id, row.bar_open_ms))
            if values is None:
                output.append(row)
                continue
            opening, high, low, close, volume = values
            output.append(
                replace(
                    row,
                    open=opening,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume if trade else None,
                )
            )
        return tuple(output)

    trade_rows, mark_rows = apply(trade_rows, True), apply(mark_rows, False)
    config_id = source["config"].split(":", 1)[0]
    config = _p2_configuration()
    config["config_id"] = config_id
    config["seed_effective_at"] = start - 1
    config["cash"] = "1000"
    seed_orders = []
    for owner_id, product_id, side, price, quantity, client_id in ORDERS.get(case, []):
        seed_orders.append(
            dict(
                intent_id=f"INTENT-{owner_id}",
                order_id=owner_id,
                client_order_id=client_id,
                strategy_id="SPIDER_GRID_ORIGINAL_V1",
                product_id=product_id,
                side=side,
                limit_price=price,
                reduce_only=False,
                original_quantity_contracts=quantity,
                filled_quantity_contracts="0",
                canceled_quantity_contracts="0",
                remaining_quantity_contracts=quantity,
                status="OPEN",
            )
        )
    config["orders"] = seed_orders
    running = "running:true" in source["policy_state"]
    policy_state = json.loads(
        _policy_cache(
            running=running,
            paused=False,
            online=True,
            ws_open=False,
            order_id=5 if case == "H02" else 1,
            capital={
                "total": "1000",
                "usdt": "1000",
                "avail": "1000",
                "earn": "0",
                "position": "0",
            },
        )
    )
    for row in policy_state["rows"]:
        row.update(
            active="true",
            leverage="1",
            歩差="1",
            單數="1",
            hold上限="0.8",
            hold下限="-0.8",
            hold="0",
        )
    spec_values = {
        "H02": {"A-USDT-SWAP": ("1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1")},
        "H03": {"A-USDT-SWAP": ("1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1")},
        "H04": {"A-USDT-SWAP": ("0.5", "0.5", "0.5"), "B-USDT-SWAP": ("1", "1", "1")},
    }[case]
    for product_config, market in zip(
        config["products"], policy_state["markets"], strict=True
    ):
        product = product_config["product_id"]
        tick, lot, minimum = spec_values[product]
        product_config["taker_fee_rate"] = "0.001"
        product_config["liquidation_fee_rate"] = "0.00602"
        product_config["specs"][0].update(
            contract_value="1",
            multiplier="1",
            price_tick=tick,
            quantity_step=lot,
            minimum_quantity=minimum,
        )
        product_config["tiers"][0]["rows"][0].update(
            maximum_contracts="1000",
            mmr="0.005",
            imr="0.1",
            max_leverage="10",
        )
        product_config["marks"][0].update(valid_to=end + 60_000, mark="100")
        market.update(
            ctVal="1",
            lotSz=lot,
            minSz=minimum,
            increment=tick,
            ratioHL="0",
            state="live",
        )
    if case == "H04":
        policy_state["rows"][0].update(歩差="0.5")
        policy_state["markets"][0]["price"] = "105"
    elif case == "H03":
        policy_state["markets"][0]["price"] = "100"
    if case == "H02":
        for seed, row in zip(seed_orders, ORDERS[case], strict=True):
            owner_id, product_id, side, price, quantity, client_id = row
            policy_state["replies"][owner_id] = dict(
                clOrdId=client_id,
                instId=product_id,
                state="new",
                side="buy" if side == "LONG" else "sell",
                px=price,
                sz=quantity,
                accFillSz="0",
            )
    config["positions"] = []
    config_bytes_run = _run_with_configuration(base, config)
    run = replace(
        config_bytes_run,
        run_id=f"oracle-{case.lower()}",
        account_key=source["account"],
        model_id=source.get("model", "OHLC4_OPEN_HIGH_LOW_CLOSE_V1"),
        policy_id="SPIDER_GRID_ORIGINAL_V1",
        policy_version="1",
        policy_source_sha256=source["policy"].split(":sha256:")[1],
        strategy_identity="SPIDER_GRID_ORIGINAL_V1",
        range_start_ms=start,
        range_end_ms=end,
        warmup_start_ms=start - 86_400_000,
        first_timer_ms=start + 5_000,
        trade_bars=trade_rows,
        mark_bars=mark_rows,
        trade_manifest=_manifest("P3_ORACLE_TRADE_V1", trade_rows, trade=True),
        mark_manifest=_manifest("P3_ORACLE_MARK_V1", mark_rows, trade=False),
        spec_before=tuple(
            replace(
                spec,
                effective_at_ms=start,
                price_tick=D(spec_values[spec.product_id][0]),
                quantity_step=D(spec_values[spec.product_id][1]),
                minimum_quantity=D(spec_values[spec.product_id][2]),
            )
            for spec in base.spec_before
        ),
        spec_after=tuple(
            replace(
                spec,
                effective_at_ms=end,
                price_tick=D(spec_values[spec.product_id][0]),
                quantity_step=D(spec_values[spec.product_id][1]),
                minimum_quantity=D(spec_values[spec.product_id][2]),
            )
            for spec in base.spec_after
        ),
        initial_policy_cache=canonical_bytes(policy_state),
        initial_account_state=_initial_account_state(
            cash="1000", orders=seed_orders, positions=[]
        ),
    )
    run = _rehashed_run(run, trade_rows=trade_rows, mark_rows=mark_rows)
    assert validate_historical_input(run)
    return source, run


def _case_answer(case):
    return _record(ANSWERS, ANSWERS_SHA256, case)


def _snapshot(composition, kind, snapshot_id, captured_at):
    fact = composition._codec.capture_snapshot(
        dict(
            schema_version="snapshot_request_v1",
            account_key=composition._account,
            snapshot_id=snapshot_id,
            snapshot_kind=kind,
            capture_mode="OWNER_CURRENT",
            captured_at=captured_at,
        )
    )
    return fact["immutable_payload"]


def _duplicate_execution_occurrence(composition, original_record):
    original = original_record["item"]["delivery"]
    duplicate = composition._codec.build_delivery(
        dict(
            schema_version="delivery_projection_v1",
            reference=dict(namespace="SOURCE", fact_id=original["source_fact_id"]),
            payload_kind="EXECUTION_FACT",
            occurrence_index=original["occurrence_index"] + 1,
            schedule_sequence=original["schedule_sequence"] + 1,
            visible_at=original["visible_at"],
        )
    )
    assert duplicate["delivery_id"] != original["delivery_id"]
    assert canonical_bytes(duplicate["immutable_payload"]) == canonical_bytes(
        original["immutable_payload"]
    )
    return duplicate


def _enqueue_policy_order(composition, visible_raw, event_kind, order):
    # The frozen case has a direct accepted source action; use the existing
    # private historical child builder and native SOURCE_GROUP scheduler.
    record = {
        "item": {
            "delivery": {
                "visible_at": visible_raw * 16 + 6,
                "delivery_id": f"P3_ORACLE_{event_kind}_{visible_raw}",
            }
        }
    }
    event = {"kind": event_kind, "orders": [order]}
    audit = [{"actions": [{"group_id": None, "status": "UNSUBMITTED"}]}]
    groups = composition._historical_order_children(record, [event], audit)
    assert len(groups) == 1
    assert composition._admit_set(groups)[0]["classification"] == "PENDING"
    return groups[0][0]


def test_frozen_h02_partial_duplicate_then_full_notice_closes_exactly():
    source, run = _oracle_run("H02")
    composition = _ReplayComposition._from_historical_run(run)
    evidence = []
    composition._evidence_callback = lambda kind, key, item: evidence.append(
        (kind, key, deepcopy(item))
    )
    start = run.range_start_ms
    assert composition._dispatch_due(start * 16 + 7)["classification"] == "SUCCESS"
    step0 = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        and record["item"]["node"]["step_index"] == 0
    )
    first = [
        fill
        for row in step0["result"]["historical_result"]["products"]
        for fill in row["fills"]
    ]
    assert [(fill["quantity_contracts"], fill["price"]) for fill in first] == [
        (D("1"), D("100"))
    ]
    target_id = first[0]["order_id"]
    state_after_first = composition._codec.inspect_state()
    assert (state_after_first["cash"], state_after_first["total_fees"]) == (
        D("999.9"),
        D("0.1"),
    )
    partial_notice = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
    )
    assert partial_notice["key"][0] == (start + 2) * 16 + 6
    duplicate_partial = _duplicate_execution_occurrence(composition, partial_notice)
    assert (
        composition._enqueue(dict(kind="DELIVERY", delivery=duplicate_partial))[
            "classification"
        ]
        == "PENDING"
    )
    assert (
        composition._dispatch_due(partial_notice["key"][0])["classification"]
        == "SUCCESS"
    )
    duplicate_partial_record = composition._records[
        (2, duplicate_partial["delivery_id"])
    ]
    assert duplicate_partial_record["result"]["classification"] == "SUCCESS"
    assert duplicate_partial_record["result"]["events"] == []
    assert any(
        kind == "CALLBACK_RESULT"
        and item["delivery_id"] == duplicate_partial["delivery_id"]
        and item["outcome"] == "CONSUMER_DEDUPLICATED"
        for kind, _, item in evidence
    )
    assert (
        composition._enqueue(dict(kind="DELIVERY", delivery=duplicate_partial))[
            "classification"
        ]
        == "SUCCESS"
    )
    partial_events = deepcopy(composition._policy.events)
    assert not any(event["kind"] in ("send", "cancel") for event in partial_events)
    state_after_partial_notice = composition._codec.inspect_state()
    assert (
        state_after_partial_notice["account_version"]
        == state_after_first["account_version"]
    )
    assert state_after_partial_notice["cash"] == state_after_first["cash"] == D("999.9")
    assert (
        state_after_partial_notice["total_fees"]
        == state_after_first["total_fees"]
        == D("0.1")
    )
    remaining_target = next(
        order
        for order in composition._codec.historical_working_orders()
        if order["order_id"] == target_id
    )
    assert remaining_target["remaining_quantity_contracts"] == D("1")
    assert composition._policy.events == partial_events

    assert (
        composition._dispatch_due((start + 20_000) * 16 + 8)["classification"]
        == "SUCCESS"
    )
    fills = [
        fill
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        for product in record["result"].get("historical_result", {}).get("products", [])
        for fill in product["fills"]
    ]
    assert [
        (fill["order_id"], fill["quantity_contracts"], fill["price"]) for fill in fills
    ] == [(target_id, D("1"), D("100")), (target_id, D("1"), D("100"))]
    state_after_full = composition._codec.inspect_state()
    assert (state_after_full["cash"], state_after_full["total_fees"]) == (
        D("999.8"),
        D("0.2"),
    )
    full_notice = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
        and record["item"]["delivery"]["immutable_payload"]["state"] == "filled"
    )
    assert full_notice["key"][0] == (start + 20_002) * 16 + 6
    duplicate_full = _duplicate_execution_occurrence(composition, full_notice)
    assert (
        composition._enqueue(dict(kind="DELIVERY", delivery=duplicate_full))[
            "classification"
        ]
        == "PENDING"
    )
    assert (
        composition._dispatch_due(full_notice["key"][0])["classification"] == "SUCCESS"
    )
    duplicate_full_record = composition._records[(2, duplicate_full["delivery_id"])]
    assert duplicate_full_record["result"]["classification"] == "SUCCESS"
    assert duplicate_full_record["result"]["events"] == []
    assert any(
        kind == "CALLBACK_RESULT"
        and item["delivery_id"] == duplicate_full["delivery_id"]
        and item["outcome"] == "CONSUMER_DEDUPLICATED"
        for kind, _, item in evidence
    )
    full_delivery = full_notice["item"]["delivery"]
    full_events = composition._policy.events
    audit = composition._audit[full_delivery["delivery_id"]]
    intents = [
        entry["event"]
        for entry in audit
        if entry["event"]["kind"] in ("send", "cancel")
    ]
    assert [event["kind"] for event in intents] == ["cancel", "send"]
    sent = [
        order
        for event in intents
        if event["kind"] == "send"
        for order in event["orders"]
    ]
    assert [(order["side"], D(order["sz"]), D(order["px"])) for order in sent] == [
        ("buy", D("1"), D("50")),
        ("sell", D("1"), D("200")),
    ]
    assert all(
        action["status"] == "UNSUBMITTED" and action["group_id"] is None
        for entry in audit
        for action in entry.get("actions", [])
    )
    assert [
        event["kind"]
        for event in full_events
        if event.get("kind") in ("send", "cancel")
    ] == [
        "cancel",
        "send",
    ]
    assert composition._codec.inspect_state() == state_after_full

    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    children = [
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "SOURCE_GROUP"
        and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"
    ]
    assert children == []
    assert (
        composition._polls[f"P3_POLL_{start + 20_000}"].observation.status
        == "IN_PROGRESS"
    )
    answer = _case_answer("H02")
    assert answer["endpoint"] == (
        "E+20002,owner_companion_A_and_B_grid_working,"
        "child_effects>=E+20003_UNSUBMITTED,pollE+20000_pending"
    )
    trading = _snapshot(composition, "TRADING", "H02_ENDPOINT_TRADING", endpoint)
    positions = _snapshot(composition, "POSITIONS", "H02_ENDPOINT_POSITIONS", endpoint)[
        "rows"
    ]
    owner_orders = _snapshot(
        composition, "OPEN_ORDERS", "H02_ENDPOINT_OPEN_ORDERS", endpoint
    )["rows"]
    state = composition._codec.inspect_state()
    assert (state["cash"], state["total_fees"], trading["equity"]) == (
        D("999.8"),
        D("0.2"),
        D("999.8"),
    )
    assert [
        (
            row["product_id"],
            row["position_contracts"],
            row["last_price"],
            row["notional_usd"],
        )
        for row in positions
    ] == [("A-USDT-SWAP", D("2"), D("100"), D("200"))]
    assert {row["client_order_id"] for row in owner_orders} == {
        "0000025008",
        "0000035006",
        "0000045008",
    }
    assert answer["S1"].startswith(
        "partial_execution1@100,cash999.9,fees0.1,remainder1"
    )
    assert (
        answer["S4"] == "full_execution1@100,cash/equity999.8,fees0.2,positionLONG2@100"
    )


def test_execution_notice_occurrence_rejects_forged_native_delivery_identity():
    for field, value in (
        ("delivery_id", "0" * 64),
        ("source_namespace", "SNAPSHOT"),
        ("payload_digest", "0" * 64),
    ):
        _, run = _oracle_run("H02")
        composition = _ReplayComposition._from_historical_run(run)
        assert (
            composition._dispatch_due(run.range_start_ms * 16 + 7)["classification"]
            == "SUCCESS"
        )
        original = next(
            record["item"]["delivery"]
            for record in composition._records.values()
            if record["item"]["kind"] == "DELIVERY"
            and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
        )
        duplicate = composition._codec.build_delivery(
            dict(
                schema_version="delivery_projection_v1",
                reference=dict(namespace="SOURCE", fact_id=original["source_fact_id"]),
                payload_kind="EXECUTION_FACT",
                occurrence_index=1,
                schedule_sequence=original["schedule_sequence"] + 1,
                visible_at=original["visible_at"],
            )
        )
        forged = deepcopy(duplicate)
        forged[field] = value
        result = composition._enqueue(dict(kind="DELIVERY", delivery=forged))
        assert result["classification"] == "TERMINAL"
        assert result["reason"] == "INVALID_SCHEMA"


def test_frozen_h03_preaccept_crossing_does_not_backfill_and_later_bar_fills():
    source, run = _oracle_run("H03")
    composition = _ReplayComposition._from_historical_run(run)
    start = run.range_start_ms
    crossing_at = (start + 40_000) * 16 + 8
    assert composition._dispatch_due(crossing_at)["classification"] == "SUCCESS"
    crossing = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        and record["item"]["node"]["bar_open_ms"] == start
        and record["item"]["node"]["step_index"] == 2
    )
    assert all(
        not row["fills"] for row in crossing["result"]["historical_result"]["products"]
    )

    group_item = _enqueue_policy_order(
        composition,
        start + 40_000,
        "send",
        dict(
            instId="A-USDT-SWAP",
            side="buy",
            ordType="limit",
            clOrdId="H03-ORDER-1",
            sz="1",
            px="95",
        ),
    )
    accepted_at = (start + 40_001) * 16 + 3
    assert group_item["group"]["group_effective_at"] == accepted_at
    assert composition._dispatch_due(accepted_at)["classification"] == "SUCCESS"
    assert group_item["group"]["members"][0]["kind"] == "INTENT"

    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    steps = [
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
    ]
    fills = [
        fill
        for record in steps
        for product in record["result"].get("historical_result", {}).get("products", [])
        for fill in product["fills"]
    ]
    assert [
        (
            fill["quantity_contracts"],
            fill["price"],
            fill["raw_time_ms"] if "raw_time_ms" in fill else None,
        )
        for fill in fills
    ] == [(D("1"), D("95"), None)]
    filled_step = next(
        record
        for record in steps
        if record["item"]["node"]["bar_open_ms"] == start + 60_000
        and record["item"]["node"]["step_index"] == 2
    )
    assert filled_step["result"]["historical_result"]["raw_time_ms"] == start + 100_000
    owner = composition._codec.inspect_state()
    trading = _snapshot(composition, "TRADING", "H03_ENDPOINT_TRADING", endpoint)
    positions = _snapshot(composition, "POSITIONS", "H03_ENDPOINT_POSITIONS", endpoint)[
        "rows"
    ]
    open_orders = _snapshot(
        composition, "OPEN_ORDERS", "H03_ENDPOINT_ORDERS", endpoint
    )["rows"]
    assert (
        owner["cash"],
        owner["gross_realized"],
        owner["total_fees"],
        trading["equity"],
    ) == (D("999.905"), D("0"), D("0.095"), D("998.905"))
    assert [
        (
            row["product_id"],
            row["position_contracts"],
            row["last_price"],
            row["notional_usd"],
        )
        for row in positions
    ] == [("A-USDT-SWAP", D("1"), D("94"), D("94"))]
    assert composition._codec.historical_working_orders() == []
    assert open_orders == []
    pending_notice = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
    )
    assert pending_notice["key"][0] == (start + 100_002) * 16 + 6
    assert pending_notice["result"]["classification"] == "PENDING"
    assert pending_notice["item"]["delivery"]["delivery_id"] not in composition._audit
    answer = _case_answer("H03")
    assert answer["S1"] == "E+40000_crossing_before_accept,no_fill"
    assert answer["S2"] == "acceptE+40001,no_backfill"
    assert answer["S3"].startswith("E+100000_fillLONG1@95,fee0.095,cash999.905")
    assert answer["endpoint"].startswith("E+100001,before_noticeE+100002")


def test_frozen_h04_fill_then_cancel_request_effect_ack_and_later_exclusion():
    source, run = _oracle_run("H04")
    composition = _ReplayComposition._from_historical_run(run)
    start = run.range_start_ms
    target = "H04-ORDER-1"

    assert composition._dispatch_due(start * 16 + 7)["classification"] == "SUCCESS"
    assert (
        composition._dispatch_due((start + 2) * 16 + 6)["classification"] == "SUCCESS"
    )
    assert (
        composition._dispatch_due((start + 20_000) * 16 + 8)["classification"]
        == "SUCCESS"
    )
    state_after_two = composition._codec.inspect_state()
    assert (state_after_two["cash"], state_after_two["total_fees"]) == (
        D("999.79"),
        D("0.21"),
    )
    assert composition._codec.historical_working_orders()[0][
        "remaining_quantity_contracts"
    ] == D("1")

    request_item = _enqueue_policy_order(
        composition,
        start + 39_999,
        "cancel",
        dict(
            ordId=target,
            instId="A-USDT-SWAP",
        ),
    )
    request_at = (start + 40_000) * 16 + 3
    assert request_item["group"]["group_effective_at"] == request_at
    assert composition._dispatch_due(request_at)["classification"] == "SUCCESS"
    assert (
        composition._records[(0, request_item["group"]["group_id"])]["result"][
            "group_result"
        ]["classification"]
        == "COMMITTED"
    )
    after_request = composition._codec.inspect_state()
    assert (after_request["cash"], after_request["total_fees"]) == (
        D("999.79"),
        D("0.21"),
    )
    assert composition._codec.historical_working_orders()[0][
        "remaining_quantity_contracts"
    ] == D("1")

    assert (
        composition._dispatch_due((start + 40_000) * 16 + 8)["classification"]
        == "SUCCESS"
    )
    step2 = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        and record["item"]["node"]["bar_open_ms"] == start
        and record["item"]["node"]["step_index"] == 2
    )
    fills = [
        fill
        for row in step2["result"]["historical_result"]["products"]
        for fill in row["fills"]
    ]
    assert [
        (fill["order_id"], fill["quantity_contracts"], fill["price"]) for fill in fills
    ] == [(target, D("0.5"), D("105"))]
    after_third = composition._codec.inspect_state()
    assert (after_third["cash"], after_third["total_fees"]) == (
        D("999.7375"),
        D("0.2625"),
    )

    effect_at = (start + 45_000) * 16 + 2
    effect = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "SOURCE_GROUP"
        and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"
    )
    assert effect["key"][0] == effect_at
    assert composition._dispatch_due(effect_at)["classification"] == "SUCCESS"
    assert effect["result"]["group_result"]["classification"] == "COMMITTED"
    assert composition._codec.historical_working_orders() == []
    after_effect = composition._codec.inspect_state()

    ack_at = (start + 45_002) * 16 + 6
    ack = next(
        record
        for record in composition._records.values()
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"
    )
    assert ack["key"][0] == ack_at
    assert ack["item"]["delivery"]["immutable_payload"]["order_id"] == target
    assert composition._dispatch_due(ack_at)["classification"] == "SUCCESS"
    assert ack["result"]["events"] == []
    assert composition._codec.inspect_state() == after_effect
    assert (
        composition._enqueue(dict(kind="DELIVERY", delivery=ack["item"]["delivery"]))[
            "classification"
        ]
        == "SUCCESS"
    )
    assert composition._codec.inspect_state() == after_effect

    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    all_fills = [
        fill
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        for product in record["result"].get("historical_result", {}).get("products", [])
        for fill in product["fills"]
    ]
    assert [(fill["quantity_contracts"], fill["price"]) for fill in all_fills] == [
        (D("1"), D("105")),
        (D("1"), D("105")),
        (D("0.5"), D("105")),
    ]
    assert composition._codec.historical_working_orders() == []
    owner = composition._codec.inspect_state()
    trading = _snapshot(composition, "TRADING", "H04_ENDPOINT_TRADING", endpoint)
    positions = _snapshot(composition, "POSITIONS", "H04_ENDPOINT_POSITIONS", endpoint)[
        "rows"
    ]
    open_orders = _snapshot(
        composition, "OPEN_ORDERS", "H04_ENDPOINT_ORDERS", endpoint
    )["rows"]
    assert (
        owner["cash"],
        owner["gross_realized"],
        owner["total_fees"],
        trading["equity"],
    ) == (D("999.7375"), D("0"), D("0.2625"), D("987.2375"))
    assert [
        (
            row["product_id"],
            row["position_contracts"],
            row["last_price"],
            row["notional_usd"],
        )
        for row in positions
    ] == [("A-USDT-SWAP", D("-2.5"), D("110"), D("275"))]
    assert open_orders == []
    fills_at_endpoint = [
        fill
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        for product in record["result"].get("historical_result", {}).get("products", [])
        for fill in product["fills"]
    ]
    assert sum((fill["quantity_contracts"] for fill in fills_at_endpoint), D("0")) == D(
        "2.5"
    )
    assert {fill["price"] for fill in fills_at_endpoint} == {D("105")}
    basis = sum(
        (fill["quantity_contracts"] * fill["price"] for fill in fills_at_endpoint),
        D("0"),
    )
    unrealized = trading["equity"] - owner["cash"]
    # Native snapshots omit basis and maintenance margin; derive these from
    # committed fills, the native position snapshot, and the admitted tier.
    configured_tier = json.loads(run.configuration_bytes)["products"][0]["tiers"][0][
        "rows"
    ][0]
    maintenance_margin = positions[0]["notional_usd"] * D(configured_tier["mmr"])
    assert (basis, unrealized, maintenance_margin) == (
        D("262.5"),
        D("-12.5"),
        D("1.375"),
    )
    answer = _case_answer("H04")
    assert answer["S1"].startswith("openingE_fillSHORT1@105,cash999.895")
    assert answer["S4"].startswith("E+40000_fill0.5@105,cash999.7375")
    assert answer["S5"] == "cancel_effectE+45000,CANCELED,remainder0"
    assert answer["S6"] == "ackE+45002,no_financial_change"
    assert answer["S7"] == "E+100000_later_cross_excluded"
    assert answer["endpoint"] == (
        "E+100001,cash999.7375,basis262.5,mark110,upl-12.5,equity987.2375,"
        "gross0,fees0.2625,mmr1.375,no_working_order"
    )
