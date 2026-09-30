"""Independent causal closure for the frozen historical oracle cases."""

from dataclasses import replace
from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256
import json

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
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
    "H05": [
        ("H05-A-REDUCE", "A-USDT-SWAP", "SHORT", "55", "1", "H05-A-REDUCE-CLIENT", True),
    ],
    "H10": [
        ("H10-ORDER-1", "A-USDT-SWAP", "LONG", "49900", "1", "H10-CLIENT-1"),
        ("H10-ORDER-2", "A-USDT-SWAP", "LONG", "49900", "1", "H10-CLIENT-2"),
    ],
    "H08": [
        ("H08-ORDER-1", "A-USDT-SWAP", "LONG", "99", "1", "H08-ORDER-1"),
    ],
    "H09": [
        ("H09-LONG", "A-USDT-SWAP", "LONG", "95", "1", "MANL"),
        ("H09-SHORT", "A-USDT-SWAP", "SHORT", "105", "1", "MANS"),
    ],
}


def _record(path, digest, case):
    raw = path.read_bytes()
    assert sha256(raw).hexdigest() == digest
    line = next(
        row for row in raw.decode("utf-8").splitlines() if row.startswith(case + "|")
    )
    fields = {}
    opaque_index = 0
    for field in line.split("|")[1:]:
        key, separator, value = field.partition("=")
        if separator:
            assert key not in fields
            fields[key] = value
        else:
            fields[f"_terms_{opaque_index}"] = field
            opaque_index += 1
    assert line.startswith(case + "|")
    fields["case"] = case
    return fields


def _oracle_run(case, *, model_id=None, h06_variant="A"):
    source = _record(INPUTS, INPUTS_SHA256, case)
    base = _valid_run()
    start = EPOCH
    end_offset = {
        "H02": 20_002, "H03": 100_001, "H04": 100_001,
        "H05": 20_004, "H09": 60_000, "H10": 1,
        "H06": 5_020, "H08": 60_000,
    }[case]
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
    products = []
    ids = {"A": "A-USDT-SWAP", "B": "B-USDT-SWAP"}
    for entry in source["bars"].split(";"):
        if ":" in entry:
            label, entry = entry.split(":", 1)
            if label in ("future_X=A", "future_Y=A"):
                continue
            assert set(label.split("/")) <= ids.keys(), f"unexpected frozen bar label: {label}"
            products = [ids[item] for item in label.split("/")]
        offset, raw_values = entry.split("=", 1)
        timestamp = start + (0 if offset == "E" else int(offset.removeprefix("E+")))
        for product in products:
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
    cash = {"H02": "1000", "H03": "1000", "H04": "1000", "H05": "20.15", "H06": "10000", "H08": "1000", "H09": "1000", "H10": "2001"}[case]
    config["cash"] = cash
    config["leverage"] = "1000" if case == "H10" else "10"
    seed_orders = []
    for row in ORDERS.get(case, []):
        owner_id, product_id, side, price, quantity, client_id, *extras = row
        seed_orders.append(
            dict(
                intent_id=f"INTENT-{owner_id}",
                order_id=owner_id,
                client_order_id=client_id,
                strategy_id="SPIDER_GRID_ORIGINAL_V1",
                product_id=product_id,
                side=side,
                limit_price=price,
                reduce_only=bool(extras[0]) if extras else False,
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
            active="false" if case in ("H08", "H09") else "true",
            leverage="1000" if case == "H10" else "10" if case == "H05" else "1",
            歩差="1",
            單數="1",
            hold上限="0.8",
            hold下限="-0.8",
            hold="0",
        )
    frozen_specs = {
        "H02": {"A-USDT-SWAP": ("1", "1", "1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
        "H03": {"A-USDT-SWAP": ("1", "1", "1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
        "H04": {"A-USDT-SWAP": ("1", "1", "0.5", "0.5", "0.5"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
        "H05": {"A-USDT-SWAP": ("2", "1", "1", "1", "1"), "B-USDT-SWAP": ("0.5", "1", "1", "1", "1")},
        "H06": {"A-USDT-SWAP": ("1", "1", "1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
        "H08": {"A-USDT-SWAP": ("1", "1", "1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
        "H09": {"A-USDT-SWAP": ("1", "1", "1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
        "H10": {"A-USDT-SWAP": ("0.01", "1", "1", "1", "1"), "B-USDT-SWAP": ("1", "1", "1", "1", "1")},
    }
    spec_values = frozen_specs[case]
    if case == "H04":
        tick_step_min = source["tick_step_min"]
        frozen_policy_steps = {
            {"A": "A-USDT-SWAP", "B": "B-USDT-SWAP"}[product]: tuple(values.split(","))
            for product, values in (part.split(":") for part in tick_step_min.split(";"))
        }
        assert frozen_policy_steps == {
            product: values[2:] for product, values in spec_values.items()
        }
    if case == "H05":
        assert source["config"].startswith("P3_ORACLE_CONFIG_H05_V1:")
    for product_config, market in zip(
        config["products"], policy_state["markets"], strict=True
    ):
        product = product_config["product_id"]
        expected_values = spec_values[product]
        contract_value, multiplier, tick, lot, minimum = spec_values[product]
        product_config["taker_fee_rate"] = "0.001"
        product_config["liquidation_fee_rate"] = "0.00602"
        product_config["specs"][0].update(
            contract_value=contract_value,
            multiplier=multiplier,
            price_tick=tick,
            quantity_step=lot,
            minimum_quantity=minimum,
        )
        if case == "H10" and product == "A-USDT-SWAP":
            product_config["tiers"][0]["rows"] = [
                dict(minimum_contracts="0", maximum_contracts="1000", mmr="0.004", imr="0.001", max_leverage="1000"),
                dict(minimum_contracts="1000.01", maximum_contracts="5000", mmr="0.005", imr="0.001", max_leverage="1000"),
            ]
        else:
            product_config["tiers"][0]["rows"][0].update(
                maximum_contracts="1000", mmr="0.005", imr="0.1", max_leverage="10",
            )
        default_mark = {"H05": {"A-USDT-SWAP": "50", "B-USDT-SWAP": "200"}, "H10": {"A-USDT-SWAP": "49900", "B-USDT-SWAP": "200"}}.get(case, {})
        product_config["marks"][0].update(valid_to=end + 60_000, mark=default_mark.get(product, "100"))
        market.update(
            ctVal=contract_value,
            lotSz=lot,
            minSz=minimum,
            increment=tick,
            ratioHL="0",
            state="live",
        )
        assert tuple(
            product_config["specs"][0][field]
            for field in ("contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity")
        ) == expected_values
        assert tuple(market[field] for field in ("ctVal", "increment", "lotSz", "minSz")) == (
            expected_values[0], expected_values[2], expected_values[3], expected_values[4]
        )
    if case == "H04":
        policy_state["rows"][0].update(歩差="0.5")
        policy_state["markets"][0]["price"] = "105"
    elif case in ("H03", "H08"):
        policy_state["markets"][0]["price"] = "100"
    elif case in ("H05", "H10"):
        policy_state["capital"].update(total=cash, usdt=cash, avail=cash)
        policy_state["markets"][0]["price"] = "50" if case == "H05" else "49900"
        policy_state["markets"][1]["price"] = "200"
    elif case == "H06":
        variants = {
            "A": {"A": ("LONG", "80"), "B": ("LONG", "40"), "shared_elapsed_ms": 6_980},
            "B": {"A": ("SHORT", "90"), "B": None, "shared_elapsed_ms": 5_981},
            "hold_boundary": {"A": ("SHORT", "80"), "B": None, "shared_elapsed_ms": 6_980},
            "elapsed_boundary": {"A": ("SHORT", "90"), "B": None, "shared_elapsed_ms": 5_980},
        }
        if h06_variant not in variants:
            raise AssertionError(f"unknown H06 variant: {h06_variant}")
        overlay = variants[h06_variant]
        policy_state["capital"].update(total=cash, usdt=cash, avail=cash, position="0")
        policy_state["replies"] = {}
        for product_ordinal, (product_label, product_id) in enumerate(ids.items(), start=1):
            spec = overlay[product_label]
            notional = "0" if spec is None else (spec[1] if spec[0] == "LONG" else f"-{spec[1]}")
            next(row for row in policy_state["rows"] if row["product_id"] == product_id).update(
                leverage="4", 歩差="1", 單數="1", hold=notional,
            )
            next(row for row in policy_state["markets"] if row["product_id"] == product_id).update(
                price="100", instIdCode=product_ordinal,
            )
            if spec is not None:
                policy_state["capital"]["position"] = str(
                    D(policy_state["capital"]["position"]) + D(notional)
                )
            for number, side in enumerate(("buy", "sell"), start=1):
                order_id = f"H06-GRID-{product_label}-{number}"
                policy_state["replies"][order_id] = dict(
                    clOrdId=f"H06-GRID-CLIENT-{product_label}-{number}",
                    instId=product_id, state="live", side=side, px="50" if side == "buy" else "200",
                    sz="1", accFillSz="0",
                )
        policy_state["shared_elapsed_ms"] = overlay["shared_elapsed_ms"]
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
    positions = []
    if case in ("H05", "H10"):
        product_id = "A-USDT-SWAP"
        quantity, entry = ("1", "50") if case == "H05" else ("1000", "49900")
        positions = [dict(product_id=product_id, side="LONG", quantity_contracts=quantity,
                          lots=[dict(seed_execution_id=f"{case}-SEED-EXECUTION", seed_sequence=0,
                                     strategy_id="SPIDER_GRID_ORIGINAL_V1", quantity_contracts=quantity,
                                     entry_price=entry)])]
    if case == "H06":
        variants = {
            "A": {"A": ("LONG", "80"), "B": ("LONG", "40")},
            "B": {"A": ("SHORT", "90"), "B": None},
            "hold_boundary": {"A": ("SHORT", "80"), "B": None},
            "elapsed_boundary": {"A": ("SHORT", "90"), "B": None},
        }
        for sequence, (label, value) in enumerate(variants[h06_variant].items()):
            if value is None:
                continue
            side, quantity = value
            product_id = ids[label]
            positions.append(dict(
                product_id=product_id, side=side, quantity_contracts=quantity,
                lots=[dict(seed_execution_id=f"H06-{h06_variant}-{label}-SEED", seed_sequence=sequence,
                           strategy_id="SPIDER_GRID_ORIGINAL_V1", quantity_contracts=quantity,
                           entry_price="100")],
            ))
    if case == "H08":
        positions = [dict(
            product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
            lots=[dict(seed_execution_id="H08-SEED-EXECUTION", seed_sequence=0,
                       strategy_id="SPIDER_GRID_ORIGINAL_V1", quantity_contracts="1",
                       entry_price="100")],
        )]
        policy_state["capital"].update(position="100")
        policy_state["rows"][0]["hold"] = "100"
        policy_state["markets"][0]["price"] = "100"
    config["positions"] = positions
    config_bytes_run = _run_with_configuration(base, config)
    run = replace(
        config_bytes_run,
        run_id=f"oracle-{case.lower()}" + (f"-{h06_variant.lower()}" if case == "H06" else ""),
        account_key=source["account"],
        model_id=model_id or source.get("model", "OHLC4_OPEN_HIGH_LOW_CLOSE_V1"),
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
                contract_value=D(spec_values[spec.product_id][0]),
                multiplier=D(spec_values[spec.product_id][1]),
                price_tick=D(spec_values[spec.product_id][2]),
                quantity_step=D(spec_values[spec.product_id][3]),
                minimum_quantity=D(spec_values[spec.product_id][4]),
            )
            for spec in base.spec_before
        ),
        spec_after=tuple(
            replace(
                spec,
                effective_at_ms=end,
                contract_value=D(spec_values[spec.product_id][0]),
                multiplier=D(spec_values[spec.product_id][1]),
                price_tick=D(spec_values[spec.product_id][2]),
                quantity_step=D(spec_values[spec.product_id][3]),
                minimum_quantity=D(spec_values[spec.product_id][4]),
            )
            for spec in base.spec_after
        ),
        initial_policy_cache=canonical_bytes(policy_state),
        initial_account_state=_initial_account_state(cash=cash, orders=seed_orders, positions=positions),
    )
    if case == "H06":
        run = replace(
            run,
            parameters=(("Clear", D("1")), ("TotalLimit", D("1")),
                        ("TotalLong", D("0.5")), ("defaultN", D("1"))),
            poll_profile="POLL_OPEN_ORDERS_FAILURE_V1",
        )
    run = _rehashed_run(run, trade_rows=trade_rows, mark_rows=mark_rows)
    assert {
        spec.product_id: (
            str(spec.contract_value), str(spec.multiplier), str(spec.price_tick),
            str(spec.quantity_step), str(spec.minimum_quantity),
        )
        for spec in run.spec_before
    } == spec_values
    assert {
        spec.product_id: (
            str(spec.contract_value), str(spec.multiplier), str(spec.price_tick),
            str(spec.quantity_step), str(spec.minimum_quantity),
        )
        for spec in run.spec_after
    } == spec_values
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


def _h08_frozen_timeline(source, start, visibility):
    root_id, root_offset = source["poll_root"].split("@E+")
    root = (root_id, start + int(root_offset))
    requests = {}
    for item in source["snapshot_requests"].split(";"):
        request, parent = item.split(":parent")
        request_id, offset = request.split("@E+")
        requests[request_id] = dict(
            request_id=request_id,
            issued_at_ms=start + int(offset),
            parent_request_id=parent,
        )
    captures = {}
    for item in source["captures"].split(";"):
        capture_id, as_of = item.split(":asofE+")
        captures[capture_id] = start + int(as_of)
    assert set(requests) == {"H08-OLD-REQUEST", "H08-NEW-REQUEST"}
    assert set(captures) == {"H08-OLD-POSITIONS", "H08-NEW-POSITIONS"}
    for request_id, capture_id in (
        ("H08-OLD-REQUEST", "H08-OLD-POSITIONS"),
        ("H08-NEW-REQUEST", "H08-NEW-POSITIONS"),
    ):
        requests[request_id]["capture_id"] = capture_id
        requests[request_id]["capture_at_ms"] = captures[capture_id]
    visibility_offsets = {
        "ordered": {"OLD": 20_007, "NEW": 20_008},
        "reversed": {"OLD": 20_008, "NEW": 20_007},
    }[visibility]
    for label, request_id in (("OLD", "H08-OLD-REQUEST"), ("NEW", "H08-NEW-REQUEST")):
        requests[request_id]["visible_at_ms"] = start + visibility_offsets[label]
    return root, requests


def _h08_expected_driver_trace(source, start, visibility):
    root_id, root_clock = source["poll_root"].split("@")
    root_ms = start + int(root_clock.removeprefix("E+"))
    requests = {}
    for item in source["snapshot_requests"].split(";"):
        identity, parent = item.split(":parent")
        request_id, clock = identity.split("@")
        requests[request_id] = (start + int(clock.removeprefix("E+")), parent)
    captures = {}
    for item in source["captures"].split(";"):
        capture_id, clock = item.split(":asof")
        captures[capture_id] = start + int(clock.removeprefix("E+"))
    visibility_ms = {
        "ordered": {"OLD": start + 20_007, "NEW": start + 20_008},
        "reversed": {"OLD": start + 20_008, "NEW": start + 20_007},
    }[visibility]
    old_at, old_parent = requests["H08-OLD-REQUEST"]
    new_at, new_parent = requests["H08-NEW-REQUEST"]
    assert old_parent == new_parent == root_id
    assert old_at < captures["H08-OLD-POSITIONS"]
    assert new_at < captures["H08-NEW-POSITIONS"]
    return [
        ("POLL", root_id, root_ms, None),
        ("SNAPSHOT", "H08-OLD-REQUEST", old_at, old_parent),
        ("CAPTURE", "H08-OLD-POSITIONS", captures["H08-OLD-POSITIONS"], "H08-OLD-REQUEST", visibility_ms["OLD"]),
        ("SNAPSHOT", "H08-NEW-REQUEST", new_at, new_parent),
        ("CAPTURE", "H08-NEW-POSITIONS", captures["H08-NEW-POSITIONS"], "H08-NEW-REQUEST", visibility_ms["NEW"]),
    ]


class _H08RequestDriver:
    def __init__(self, composition, frozen_root, frozen_requests):
        self.composition = composition
        self.root = frozen_root
        self.expected = frozen_requests
        self.issued = {}
        self.trace = []

    def issue_poll_root(self):
        request_id, issued_at_ms = self.root
        self._observe_issue_clock(issued_at_ms)
        assert request_id not in self.issued
        self.issued[request_id] = dict(request_id=request_id, issued_at_ms=issued_at_ms)
        self.trace.append(("POLL", request_id, issued_at_ms, None))

    def issue_snapshot_request(self, request_id):
        expected = self.expected[request_id]
        parent_id = expected["parent_request_id"]
        assert parent_id in self.issued
        assert request_id not in self.issued
        assert expected["issued_at_ms"] < expected["capture_at_ms"]
        self._observe_issue_clock(expected["issued_at_ms"])
        request = dict(expected)
        self.issued[request_id] = request
        self.trace.append(("SNAPSHOT", request_id, request["issued_at_ms"], parent_id))

    def _observe_issue_clock(self, issued_at_ms):
        observed_until = issued_at_ms * 16 + 4
        assert self.composition._dispatch_due(observed_until)["classification"] == "SUCCESS"
        assert self.composition._current_time >= observed_until

    def capture(self, request_id, sequence):
        assert request_id in self.issued
        issued = self.issued[request_id]
        capture_at_ms = issued["capture_at_ms"]
        assert self.composition._current_time < capture_at_ms * 16 + 5
        item = _h08_position_capture(self.composition, issued, sequence)
        assert self.composition._dispatch_due(capture_at_ms * 16 + 4)["classification"] == "SUCCESS"
        _h08_prepare_empty_delivery_plan(self.composition, item)
        assert self.composition._enqueue(item)["classification"] == "PENDING"
        assert self.composition._dispatch_due(capture_at_ms * 16 + 5)["classification"] == "SUCCESS"
        self.trace.append(("CAPTURE", issued["capture_id"], capture_at_ms, request_id, issued["visible_at_ms"]))
        return item


def _h08_position_capture(composition, issued_request, sequence):
    capture_id = issued_request["capture_id"]
    request = dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id=capture_id, snapshot_kind="POSITIONS",
        capture_mode="OWNER_CURRENT", captured_at=issued_request["capture_at_ms"] * 16 + 5,
    )
    projection = dict(
        schema_version="delivery_projection_v1",
        reference=dict(namespace="SNAPSHOT", fact_id=request["snapshot_id"]),
        payload_kind="POSITION_SNAPSHOT", occurrence_index=sequence,
        schedule_sequence=sequence, visible_at=issued_request["visible_at_ms"] * 16 + 6,
    )
    return dict(
        kind="SNAPSHOT_CAPTURE", capture_sequence=sequence,
        request=request, delivery_projection=projection,
    )


def _h08_prepare_empty_delivery_plan(composition, item):
    fact = composition._codec.capture_snapshot(item["request"])
    projection = deepcopy(item["delivery_projection"])
    projection["reference"] = fact["reference"]
    delivery = composition._codec.build_delivery(projection)
    delivery_id = delivery["delivery_id"]
    composition._callback_plans[delivery_id] = dict(
        delivery_id=delivery_id, expected_policy_events=[], financial_items=[], market_requests=[],
    )


def test_h08_request_driver_rejects_broken_request_causality():
    source = _record(INPUTS, INPUTS_SHA256, "H08")
    root, expected = _h08_frozen_timeline(source, EPOCH, "ordered")
    driver = _H08RequestDriver(None, root, deepcopy(expected))
    driver.issued[root[0]] = dict(request_id=root[0], issued_at_ms=root[1])
    driver.expected["H08-OLD-REQUEST"]["parent_request_id"] = "MISSING-PARENT"
    with pytest.raises(AssertionError):
        driver.issue_snapshot_request("H08-OLD-REQUEST")

    driver.expected = deepcopy(expected)
    driver.expected["H08-OLD-REQUEST"]["issued_at_ms"] = driver.expected[
        "H08-OLD-REQUEST"
    ]["capture_at_ms"]
    with pytest.raises(AssertionError):
        driver.issue_snapshot_request("H08-OLD-REQUEST")

    driver.expected = deepcopy(expected)
    driver.issued["H08-OLD-REQUEST"] = driver.expected["H08-OLD-REQUEST"]
    with pytest.raises(AssertionError):
        driver.issue_snapshot_request("H08-OLD-REQUEST")
    with pytest.raises(KeyError):
        driver.issue_snapshot_request("H08-WRONG-REQUEST")


def _h08_future_bar(source, future_label):
    entry = next(
        value for value in source["bars"].split(";")
        if value.startswith(f"{future_label}=")
    ).partition("=")[2]
    product, separator, row = entry.partition(":")
    offset, separator2, values = row.partition("=")
    assert (product, separator, offset, separator2) == ("A", ":", "E+60000", "=")
    opening, high, low, close, volume = (D(value) for value in values.split("/"))
    return opening, high, low, close, volume


def _h08_future_trace(run, future_bar):
    opening, high, low, future_close, volume = future_bar
    start = run.range_start_ms
    prefix_end = run.range_end_ms
    extended_end = prefix_end + 60_000
    configuration = json.loads(run.configuration_bytes)
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = extended_end + 60_000
    run = _run_with_configuration(run, configuration)
    run = replace(
        run, range_end_ms=extended_end,
        spec_after=tuple(replace(spec, effective_at_ms=extended_end) for spec in run.spec_after),
    )
    trade_rows = list(run.trade_bars)
    mark_rows = list(run.mark_bars)
    for product_id, values, bar_volume in (
        ("A-USDT-SWAP", (opening, high, low, future_close), volume),
        ("B-USDT-SWAP", (D("100"),) * 4, D("4")),
    ):
        trade_template = next(row for row in trade_rows
                              if row.product_id == product_id and row.bar_open_ms == start)
        mark_template = next(row for row in mark_rows
                             if row.product_id == product_id and row.bar_open_ms == start)
        trade_rows.append(replace(
            trade_template, bar_open_ms=prefix_end, open=values[0], high=values[1],
            low=values[2], close=values[3], volume=bar_volume,
            source_sequence=max(row.source_sequence for row in trade_rows
                                if row.product_id == product_id) + 1,
        ))
        mark_rows.append(replace(
            mark_template, bar_open_ms=prefix_end, open=values[0], high=values[1],
            low=values[2], close=values[3], volume=None,
            source_sequence=max(row.source_sequence for row in mark_rows
                                if row.product_id == product_id) + 1,
        ))
    trade_rows, mark_rows = tuple(trade_rows), tuple(mark_rows)
    updated = _rehashed_run(run, trade_rows=trade_rows, mark_rows=mark_rows)
    updated = replace(
        updated,
        trade_manifest=_manifest(updated.trade_manifest.source_id, updated.trade_bars, trade=True),
        mark_manifest=_manifest(updated.mark_manifest.source_id, updated.mark_bars, trade=False),
    )
    assert validate_historical_input(updated)
    return updated


def _h07_configuration():
    boundary = (EPOCH + 60_000) * 16
    configuration = _p2_configuration()
    configuration.update(config_id="P3_ORACLE_CONFIG_H07_V1", seed_effective_at=(EPOCH - 1) * 16,
                         cash="1000", leverage="10")
    configuration["orders"] = [dict(
        intent_id="H07-INTENT-1", order_id="H07-ORDER-1", client_order_id="H07-CLIENT-1",
        strategy_id="H07", product_id="A-USDT-SWAP", side="LONG", limit_price="103",
        reduce_only=False, original_quantity_contracts="1", filled_quantity_contracts="0",
        canceled_quantity_contracts="0", remaining_quantity_contracts="1", status="OPEN",
    )]
    for index, product in enumerate(configuration["products"]):
        product["taker_fee_rate"] = "0.001"
        product["liquidation_fee_rate"] = "0.00602"
        spec = product["specs"][0]
        spec.update(version="H07-SPEC-V1", valid_from=0, valid_to=None,
                    contract_value="1", multiplier="1", price_tick="1",
                    quantity_step="1", minimum_quantity="1")
        if index == 0:
            next_spec = {**spec, "version": "H07-SPEC-V2", "valid_from": boundary,
                         "valid_to": None, "price_tick": "5"}
            spec["valid_to"] = boundary
            product["specs"].append(next_spec)
        product["tiers"][0].update(valid_from=0, valid_to=None)
        product["tiers"][0]["rows"][0].update(
            maximum_contracts="1000", mmr="0.005", imr="0.1", max_leverage="10",
        )
        product["marks"] = [dict(valid_from=0, valid_to=2**63 - 1, mark="100")]
    return configuration


def _h07_node(codec, bar_open_ms, step_index, price):
    bars = []
    for index, product_id in enumerate(("A-USDT-SWAP", "B-USDT-SWAP")):
        value = D(price if index == 0 else "100")
        source_hash = sha256(f"H07:{product_id}:{bar_open_ms}:{step_index}".encode()).hexdigest()
        ohlc = dict(open=value, high=value, low=value, close=value,
                    confirmed=True, source_row_hash=source_hash)
        bars.append(dict(product_id=product_id,
                         trade={**ohlc, "volume_contracts": D("4")}, mark=dict(ohlc)))
    return codec.historical_market_step(dict(
        schema_version="historical_node_v1", model_id="OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
        model_version="1", run_contract_hash="a" * 64, bar_open_ms=bar_open_ms,
        bar_duration_ms=60_000, step_index=step_index, market_slippage_bps=D("0"),
        bars=bars, working_orders=codec.historical_working_orders(),
    ))


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


@pytest.mark.parametrize(
    "visibility,expected_hold",
    [("ordered", D("198")), ("reversed", D("99"))],
)
def test_frozen_h08_independent_position_snapshots_follow_delivery_order(visibility, expected_hold):
    source, run = _oracle_run("H08", model_id="OHLC4_OPEN_LOW_HIGH_CLOSE_V1")
    assert source["case"] == "H08"
    assert source["policy"] == (
        "SPIDER_GRID_ORIGINAL_V1@1:sha256:"
        "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec"
    )
    assert source["config"] == (
        "P3_ORACLE_CONFIG_BASE_V1:sha256:"
        "e4198c375c41c8c3e9b0a6d8aa7f836f4f5545e8f2b5abc712851c3b556009dd"
    )
    assert source["poll_root"] == "H08-POLL-ROOT@E+19997"
    assert source["snapshot_requests"] == (
        "H08-OLD-REQUEST@E+19998:parentH08-POLL-ROOT;"
        "H08-NEW-REQUEST@E+20000:parentH08-POLL-ROOT"
    )
    assert source["captures"] == (
        "H08-OLD-POSITIONS:asofE+19999;H08-NEW-POSITIONS:asofE+20001"
    )
    answer = _case_answer("H08")
    assert answer["market_fill"] == (
        "E+20000,H08-ORDER-1_LONG1@99,owner_contractsA1->2,"
        "execution_noticeE+20002_updates_policy_cache_mark99"
    )
    composition = _ReplayComposition._from_historical_run(run)
    start = run.range_start_ms
    timeline_root, expected_requests = _h08_frozen_timeline(source, start, visibility)
    driver = _H08RequestDriver(composition, timeline_root, expected_requests)
    driver.issue_poll_root()
    driver.issue_snapshot_request("H08-OLD-REQUEST")
    driver.capture("H08-OLD-REQUEST", sequence=100)
    driver.issue_snapshot_request("H08-NEW-REQUEST")
    driver.capture("H08-NEW-REQUEST", sequence=101)
    assert driver.trace == _h08_expected_driver_trace(source, start, visibility)
    timeline = [driver.issued["H08-OLD-REQUEST"], driver.issued["H08-NEW-REQUEST"]]
    old_visible = timeline[0]["visible_at_ms"] - start
    new_visible = timeline[1]["visible_at_ms"] - start
    owner_after_fill_and_capture = composition._codec.inspect_state()
    capture_records = {
        row["request_id"]: composition._records[(1, row["capture_id"])]
        for row in timeline
    }
    assert {
        request_id: (record["item"]["request"]["snapshot_id"],
                     (record["key"][0] - 5) // 16)
        for request_id, record in capture_records.items()
    } == {
        row["request_id"]: (row["capture_id"], row["capture_at_ms"])
        for row in timeline
    }
    fill_records = [
        (record, product, fill)
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        for product in record["result"].get("historical_result", {}).get("products", [])
        for fill in product["fills"]
        if fill["order_id"] == "H08-ORDER-1"
    ]
    assert len(fill_records) == 1
    fill_record, fill_product, fill = fill_records[0]
    notice = next(
        record for record in composition._records.values()
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["source_fact_id"] == fill["source_event_id"]
    )
    notice_delivery = notice["item"]["delivery"]
    assert (fill_product["product_id"], fill["quantity_contracts"], fill["price"]) == (
        "A-USDT-SWAP", D("1"), D("99"),
    )
    assert fill_record["result"]["historical_result"]["raw_time_ms"] == start + 20_000
    assert notice_delivery["source_fact_id"] == fill["source_event_id"]
    assert notice_delivery["immutable_payload"]["order_id"] == "H08-ORDER-1"
    assert notice_delivery["immutable_payload"]["fill_price"] == D("99")
    assert notice_delivery["visible_at"] == (start + 20_002) * 16 + 6
    assert D(str(composition._policy.markets["A-USDT-SWAP"]["price"])) == D("100")
    assert notice["result"]["classification"] == "PENDING"
    assert composition._dispatch_due(notice["key"][0] - 1)["classification"] == "SUCCESS"
    assert D(str(composition._policy.markets["A-USDT-SWAP"]["price"])) == D("100")
    assert composition._dispatch_due(notice["key"][0])["classification"] == "SUCCESS"
    assert composition._records[(2, notice_delivery["delivery_id"])]["result"]["classification"] == "SUCCESS"
    assert composition._policy.markets["A-USDT-SWAP"]["price"] == D("99")
    first_snapshot_visible = min(old_visible, new_visible)
    assert composition._dispatch_due((start + first_snapshot_visible) * 16 + 6)["classification"] == "SUCCESS"
    expected_first_hold = D("99") if first_snapshot_visible == old_visible else D("198")
    assert composition._policy.rows[0]["hold"] == expected_first_hold
    second_snapshot_visible = max(old_visible, new_visible)
    assert composition._dispatch_due((start + second_snapshot_visible) * 16 + 6)["classification"] == "SUCCESS"
    assert composition._policy.rows[0]["hold"] == expected_hold
    assert composition._policy.markets["A-USDT-SWAP"]["price"] == D("99")
    boundary_owner = composition._codec.inspect_state()
    assert boundary_owner["account_version"] == owner_after_fill_and_capture["account_version"]
    boundary_positions = _snapshot(
        composition, "POSITIONS", f"H08-{visibility}-BOUNDARY", (start + 20_008) * 16 + 6
    )["rows"]
    assert [(row["product_id"], row["position_contracts"]) for row in boundary_positions] == [
        ("A-USDT-SWAP", D("2")),
    ]
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    assert composition._policy.rows[0]["hold"] == D("198")
    endpoint_owner = composition._codec.inspect_state()
    endpoint_positions = _snapshot(
        composition, "POSITIONS", f"H08-{visibility}-ENDPOINT", run.range_end_ms * 16 + 6
    )["rows"]
    assert [(row["product_id"], row["position_contracts"]) for row in endpoint_positions] == [
        ("A-USDT-SWAP", D("2")),
    ]
    assert endpoint_owner["account_version"] >= boundary_owner["account_version"]
    assert endpoint_owner["cash"] == boundary_owner["cash"]

    snapshot_deliveries = {
        record["item"]["delivery"]["source_fact_id"]: record
        for record in composition._records.values()
        if record["item"]["kind"] == "DELIVERY"
        and record["item"]["delivery"]["source_fact_id"] in {
            "H08-OLD-POSITIONS", "H08-NEW-POSITIONS",
        }
    }
    for row in timeline:
        delivery_record = snapshot_deliveries[row["capture_id"]]
        delivery = delivery_record["item"]["delivery"]
        assert delivery["visible_at"] == row["visible_at_ms"] * 16 + 6
        assert delivery["snapshot_as_of"] == row["capture_at_ms"] * 16 + 5
        assert delivery_record["result"]["classification"] == "SUCCESS"
        expected_contracts = D("1") if row["capture_id"] == "H08-OLD-POSITIONS" else D("2")
        assert delivery["immutable_payload"]["rows"][0]["position_contracts"] == expected_contracts
    assert composition._codec.inspect_state()["account_version"] > 0
    assert composition._policy.rows[0]["active"] == "false"
    assert composition._policy.rows[0]["hold"] == D("198")
    control = _ReplayComposition._from_historical_run(run)
    assert control._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    assert composition._policy.events == control._policy.events


def test_frozen_h08_models_share_policy_prefix_and_timer_schedule():
    models = (
        ("OHLC4_OPEN_LOW_HIGH_CLOSE_V1", 20_000),
        ("OHLC4_OPEN_HIGH_LOW_CLOSE_V1", 40_000),
    )
    prefixes = []
    traces = []
    for model_id, fill_offset in models:
        _, run = _oracle_run("H08", model_id=model_id)
        composition = _ReplayComposition._from_historical_run(run)
        start = run.range_start_ms
        assert composition._dispatch_due((start + 20_001) * 16 + 5)["classification"] == "SUCCESS"
        prefixes.append(deepcopy(composition._policy.events))
        assert D(str(composition._policy.markets["A-USDT-SWAP"]["price"])) == D("100")
        assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
        timers = sorted(
            (record["raw_time_ms"], record["result"]["classification"])
            for record in composition._records.values()
            if record["item"]["kind"] == "HISTORICAL_TIMER"
        )
        assert timers == [
            (start + offset, "SUCCESS") for offset in range(5_000, 60_000, 5_000)
        ]
        fills = [
            (record["result"]["historical_result"]["raw_time_ms"], fill["order_id"],
             fill["quantity_contracts"], fill["price"])
            for record in composition._records.values()
            if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
            for product in record["result"].get("historical_result", {}).get("products", [])
            for fill in product["fills"]
        ]
        assert fills == [(start + fill_offset, "H08-ORDER-1", D("1"), D("99"))]
        final_positions = _snapshot(
            composition, "POSITIONS", f"H08-{model_id}-END", run.range_end_ms * 16 + 6
        )["rows"]
        assert [(row["product_id"], row["position_contracts"]) for row in final_positions] == [
            ("A-USDT-SWAP", D("2")),
        ]
        traces.append(composition)
    assert prefixes[0] == prefixes[1]
    assert traces[0]._policy.events == traces[1]._policy.events


def test_frozen_h08_future_x_y_bars_do_not_change_closed_interval_prefix():
    source, base = _oracle_run("H08", model_id="OHLC4_OPEN_LOW_HIGH_CLOSE_V1")
    future_x = _h08_future_trace(base, _h08_future_bar(source, "future_X"))
    future_y = _h08_future_trace(base, _h08_future_bar(source, "future_Y"))
    end = base.range_end_ms
    trade_prefix_x = tuple(row for row in future_x.trade_bars if row.bar_open_ms < end)
    trade_prefix_y = tuple(row for row in future_y.trade_bars if row.bar_open_ms < end)
    mark_prefix_x = tuple(row for row in future_x.mark_bars if row.bar_open_ms < end)
    mark_prefix_y = tuple(row for row in future_y.mark_bars if row.bar_open_ms < end)
    assert trade_prefix_x == trade_prefix_y
    assert mark_prefix_x == mark_prefix_y
    x_future = next(row for row in future_x.trade_bars
                    if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == end)
    y_future = next(row for row in future_y.trade_bars
                    if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == end)
    assert (x_future.open, x_future.high, x_future.low, x_future.close) == (
        D("100"), D("101"), D("99"), D("101"),
    )
    assert (y_future.open, y_future.high, y_future.low, y_future.close) == (
        D("100"), D("101"), D("99"), D("99"),
    )
    assert (x_future.open, x_future.high, x_future.low, x_future.volume) == (
        y_future.open, y_future.high, y_future.low, y_future.volume,
    )
    assert x_future.close != y_future.close
    for rows_x, rows_y in (
        (future_x.trade_bars, future_y.trade_bars),
        (future_x.mark_bars, future_y.mark_bars),
    ):
        by_key_x = {(row.product_id, row.bar_open_ms): row for row in rows_x}
        by_key_y = {(row.product_id, row.bar_open_ms): row for row in rows_y}
        assert by_key_x.keys() == by_key_y.keys()
        assert all(
            by_key_x[key] == by_key_y[key]
            for key in by_key_x
            if key != ("A-USDT-SWAP", end)
        )

    compositions = [
        _ReplayComposition._from_historical_run(run) for run in (future_x, future_y)
    ]
    for composition in compositions:
        assert composition._dispatch_due(end * 16 + 6)["classification"] == "SUCCESS"
        future_steps = [
            record for record in composition._records.values()
            if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
            and record["item"]["node"]["bar_open_ms"] == end
        ]
        assert len(future_steps) == 1
        assert future_steps[0]["result"]["classification"] == "PENDING"
    def normalized_steps(composition):
        return [
            (record["result"]["historical_result"]["raw_time_ms"],
             record["item"]["node"]["step_index"],
             tuple((bar["product_id"],
                    tuple((name, bar["trade"][name]) for name in ("open", "high", "low", "close", "volume_contracts")),
                    tuple((name, bar["mark"][name]) for name in ("open", "high", "low", "close")))
                   for bar in record["item"]["node"]["bars"]),
             tuple((product["product_id"], tuple(
                 (fill["order_id"], fill["quantity_contracts"], fill["price"])
                 for fill in product["fills"]
             )) for product in record["result"]["historical_result"]["products"]))
            for record in sorted(composition._records.values(), key=lambda item: item["key"])
            if (record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                    and "historical_result" in record["result"])
        ]
    assert normalized_steps(compositions[0]) == normalized_steps(compositions[1])
    assert compositions[0]._policy.events == compositions[1]._policy.events
    for composition in compositions:
        successful_timers = sorted(
            record["raw_time_ms"] for record in composition._records.values()
            if record["item"]["kind"] == "HISTORICAL_TIMER"
            and record["result"]["classification"] == "SUCCESS"
        )
        assert successful_timers == [
            future_x.range_start_ms + offset for offset in range(5_000, 60_000, 5_000)
        ]


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


def test_frozen_h05_shared_admission_reject_then_accept_after_a_close():
    source, run = _oracle_run("H05")
    answer = _case_answer("H05")
    assert source["cash"] == "20.15"
    assert "usedA10.11+proposedB10.1=20.21" in answer["B-ATTEMPT-1"]
    composition = _ReplayComposition._from_historical_run(run)
    start = run.range_start_ms
    assert composition._dispatch_due(start * 16 + 7)["classification"] == "SUCCESS"
    before_rejection = composition._codec.inspect_state()

    _enqueue_policy_order(
        composition, start, "send",
        dict(clOrdId="H05-B-ATTEMPT-1", instId="B-USDT-SWAP", side="buy",
             ordType="limit", sz="1", px="200"),
    )
    assert composition._dispatch_due((start + 1) * 16 + 3)["classification"] == "SUCCESS"
    rejected = [
        record["result"]["group_result"]
        for record in composition._records.values()
        if record["item"]["kind"] == "SOURCE_GROUP"
    ][0]
    assert rejected["classification"] == "REJECTED"
    assert [row["reason"] for row in rejected["rejections"]] == [
        "INSUFFICIENT_SHARED_EQUITY"
    ]
    after_rejection = composition._codec.inspect_state()
    assert after_rejection["account_version"] == before_rejection["account_version"]
    assert (after_rejection["cash"], after_rejection["total_fees"]) == (D("20.15"), D("0"))
    assert [row["order_id"] for row in composition._codec.historical_working_orders()] == [
        "H05-A-REDUCE"
    ]

    assert composition._dispatch_due((start + 20_000) * 16 + 8)["classification"] == "SUCCESS"
    close = next(
        record["result"]["historical_result"]
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        and record["item"]["node"]["step_index"] == 1
    )
    assert [
        (fill["order_id"], fill["quantity_contracts"], fill["price"])
        for row in close["products"] for fill in row["fills"]
    ] == [("H05-A-REDUCE", D("1"), D("55"))]
    after_close = composition._codec.inspect_state()
    assert (after_close["cash"], after_close["gross_realized"], after_close["total_fees"]) == (
        D("30.04"), D("10"), D("0.11")
    )
    assert composition._codec.historical_working_orders() == []

    _enqueue_policy_order(
        composition, start + 20_001, "send",
        dict(clOrdId="H05-B-ATTEMPT-2", instId="B-USDT-SWAP", side="buy",
             ordType="limit", sz="1", px="200"),
    )
    assert composition._dispatch_due((start + 20_002) * 16 + 3)["classification"] == "SUCCESS"
    accepted = [
        record["result"]["group_result"]
        for record in composition._records.values()
        if record["item"]["kind"] == "SOURCE_GROUP"
    ][1]
    assert accepted["classification"] == "COMMITTED"
    assert len(accepted["committed_references"]) == 1
    assert composition._dispatch_due((start + 20_004) * 16 + 6)["classification"] == "SUCCESS"
    state = composition._codec.inspect_state()
    assert (state["cash"], state["gross_realized"], state["total_fees"]) == (
        D("30.04"), D("10"), D("0.11")
    )
    assert [
        (row["side"], row["remaining_quantity_contracts"], row["limit_price"])
        for row in composition._codec.historical_working_orders()
    ] == [("LONG", D("1"), D("200"))]
    endpoint = (start + 20_004) * 16 + 6
    trading = _snapshot(composition, "TRADING", "H05-END-TRADING", endpoint)
    assert trading == {"available_equity": D("19.94"), "equity": D("30.04"), "outcome": "SUCCESS"}
    open_orders = _snapshot(composition, "OPEN_ORDERS", "H05-END-ORDERS", endpoint)
    assert [(row["client_order_id"], row["state"], row["limit_price"]) for row in open_orders["rows"]] == [
        ("H05-B-ATTEMPT-2", "live", D("200"))
    ]
    assert "creates_only_B-ORDER-2" in answer["B-ATTEMPT-2"]


def test_frozen_h10_first_candidate_triggers_immediate_risk_transition():
    source, run = _oracle_run("H10")
    answer = _case_answer("H10")
    assert source["orders"].startswith("H10-ORDER-1")
    assert answer["candidate2"] == "no_execution_no_fee_no_receipt"
    composition = _ReplayComposition._from_historical_run(run)
    start = run.range_start_ms
    assert [
        (row["order_id"], row["accepted_source_sequence"])
        for row in composition._codec.historical_working_orders()
    ] == [("H10-ORDER-1", 1), ("H10-ORDER-2", 2)]
    assert composition._dispatch_due(start * 16 + 7)["classification"] == "SUCCESS"
    step = next(
        record["result"]["historical_result"]
        for record in composition._records.values()
        if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        and record["item"]["node"]["step_index"] == 0
    )
    fills = [fill for product in step["products"] for fill in product["fills"]]
    assert [(fill["order_id"], fill["quantity_contracts"], fill["price"]) for fill in fills] == [
        ("H10-ORDER-1", D("1"), D("49900"))
    ]
    state = composition._codec.inspect_state()
    assert (state["cash"], state["gross_realized"], state["total_fees"], state["gate"]) == (
        D("1997.49702"), D("0"), D("3.50298"), "RUNNING"
    )
    endpoint = start * 16 + 7
    assert _snapshot(composition, "TRADING", "H10-END-TRADING", endpoint) == {
        "available_equity": D("1498.49702"),
        "equity": D("1997.49702"),
        "outcome": "SUCCESS",
    }
    positions = _snapshot(composition, "POSITIONS", "H10-END-POSITIONS", endpoint)
    assert [
        (row["product_id"], row["position_contracts"], row["notional_usd"])
        for row in positions["rows"]
    ] == [("A-USDT-SWAP", D("1000"), D("499000"))]
    assert _snapshot(composition, "OPEN_ORDERS", "H10-END-ORDERS", endpoint)["rows"] == []
    assert composition._codec.historical_working_orders() == []
    assert "liquidate1@49900" in answer["risk"]
    assert "total_fees3.50298" in answer["endpoint"]


def test_frozen_h09_named_ohlc_paths_preserve_execution_order_and_reports():
    source = _record(INPUTS, INPUTS_SHA256, "H09")
    answer = _case_answer("H09")
    model_cases = (
        ("OHLC4_OPEN_HIGH_LOW_CLOSE_V1", "H09-HIGH-LOW"),
        ("OHLC4_OPEN_LOW_HIGH_CLOSE_V1", "H09-LOW-HIGH"),
    )
    assert source["bars"] == "A:E=100/110/90/105/8;B:E=100/100/100/100/4"
    assert source["seed_orders"] == (
        "A:H09-LONG:client=MANL:LONG:qty1:px95;"
        "A:H09-SHORT:client=MANS:SHORT:qty1:px105"
    )
    assert source["models"] == "OHLC4_OPEN_HIGH_LOW_CLOSE_V1,OHLC4_OPEN_LOW_HIGH_CLOSE_V1"
    assert source["notices"] == "E+20002,E+40002"
    assert answer["OPEN_HIGH_LOW_CLOSE"].startswith("SHORT1@105@E+20000_then_LONG1@95@E+40000")
    assert answer["OPEN_LOW_HIGH_CLOSE"].startswith("LONG1@95@E+20000_then_SHORT1@105@E+40000")
    assert any("noticesE+20002/E+40002" in value for value in answer.values())

    reports = {}
    for model_id, report_id in model_cases:
        _, run = _oracle_run("H09", model_id=model_id)
        composition = _ReplayComposition._from_historical_run(run)
        start = run.range_start_ms
        endpoint = run.range_end_ms * 16 + 6
        assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"

        steps = [
            record for record in composition._records.values()
            if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
        ]
        journal = tuple(
            (fill["order_id"], record["result"]["historical_result"]["raw_time_ms"],
             fill["quantity_contracts"], fill["price"])
            for record in sorted(steps, key=lambda row: row["key"])
            for product in record["result"]["historical_result"]["products"]
            for fill in product["fills"]
        )
        expected = (
            (("H09-SHORT", start + 20_000, D("1"), D("105")),
             ("H09-LONG", start + 40_000, D("1"), D("95")))
            if model_id == "OHLC4_OPEN_HIGH_LOW_CLOSE_V1" else
            (("H09-LONG", start + 20_000, D("1"), D("95")),
             ("H09-SHORT", start + 40_000, D("1"), D("105")))
        )
        assert journal == expected

        notices = [
            record for record in composition._records.values()
            if record["item"]["kind"] == "DELIVERY"
            and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
        ]
        assert sorted(record["key"][0] for record in notices) == [
            (start + 20_002) * 16 + 6, (start + 40_002) * 16 + 6
        ]
        assert all(record["result"]["classification"] == "SUCCESS" for record in notices)
        assert all(row["active"] == "false" for row in composition._policy.rows)
        intent_groups = [
            record for record in composition._records.values()
            if record["item"]["kind"] == "SOURCE_GROUP"
            and any(member["kind"] == "INTENT" for member in record["item"]["group"]["members"])
        ]
        assert intent_groups == []
        assert not any(
            event.get("kind") in ("send", "cancel")
            for event in composition._policy.events
        )
        assert not any(
            entry["event"].get("kind") in ("send", "cancel")
            or entry.get("actions")
            for audit in composition._audit.values()
            for entry in audit
        )

        owner = composition._codec.inspect_state()
        trading = _snapshot(composition, "TRADING", f"{report_id}-TRADING", endpoint)
        positions = _snapshot(composition, "POSITIONS", f"{report_id}-POSITIONS", endpoint)["rows"]
        orders = _snapshot(composition, "OPEN_ORDERS", f"{report_id}-ORDERS", endpoint)["rows"]
        report = {
            "cash": owner["cash"],
            "equity": trading["equity"],
            "positions": positions,
            "open_orders": orders,
            "gross_realized": owner["gross_realized"],
            "fees": owner["total_fees"],
            "mark": composition._policy.markets["A-USDT-SWAP"]["price"],
            "journal": journal,
        }
        assert report == {
            "cash": D("1009.8"),
            "equity": D("1009.8"),
            "positions": [],
            "open_orders": [],
            "gross_realized": D("10"),
            "fees": D("0.2"),
            "mark": D("105"),
            "journal": expected,
        }
        reports[model_id] = report

    difference = {
        "first_fill": (reports[model_cases[0][0]]["journal"][0], reports[model_cases[1][0]]["journal"][0]),
        "last_fill": (reports[model_cases[0][0]]["journal"][1], reports[model_cases[1][0]]["journal"][1]),
    }
    assert difference == {
        "first_fill": (("H09-SHORT", EPOCH + 20_000, D("1"), D("105")),
                       ("H09-LONG", EPOCH + 20_000, D("1"), D("95"))),
        "last_fill": (("H09-LONG", EPOCH + 40_000, D("1"), D("95")),
                      ("H09-SHORT", EPOCH + 40_000, D("1"), D("105"))),
    }
    assert reports[model_cases[0][0]]["cash"] == reports[model_cases[1][0]]["cash"]
    assert any("each_endpointE+60000,cash1009.8,flat,gross10,fees0.2,mark105,equity1009.8" in value for value in answer.values())
    assert any("both_journals_required" in value for value in answer.values())


def test_h07_native_spec_migration_precedes_boundary_bar_and_rejects_raw_intent():
    source = _record(INPUTS, INPUTS_SHA256, "H07")
    answer = _record(ANSWERS, ANSWERS_SHA256, "H07")
    config_identity = (
        "P3_ORACLE_CONFIG_H07_V1|products=A-USDT-SWAP,B-USDT-SWAP|instIdCode=1,2|"
        "ctVal=1,1|multiplier=1,1|tick=1->5,1|qstep=1,1|min=1,1|leverage=10|"
        "fee=0.001|liqfee=0.00602|tier=0:1000:0.005:0.1:10|funding=DISABLED"
    )
    assert source["config"].endswith(sha256(config_identity.encode()).hexdigest())
    assert source["pre_spec"] == "tick1,qstep1,min1"
    assert source["activation"] == "E+60000"
    assert source["post_spec"] == "tick5,qstep1,min1"
    assert "no_retroactive_fill" in answer["preexisting103"]

    account = dict(venue="okx-scenario", environment="test", account="H07")
    codec = wire.ScenarioCodec(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", account, _h07_configuration()
    )

    def intent_group(identity, at, price):
        return dict(
            schema_version="scenario_group_v1", group_id=identity, account_key=account,
            ordering_contract_id="S_order_v1", group_effective_at=at, declared_member_count=1,
            members=[dict(
                kind="INTENT",
                stamp=dict(event_id=identity, effective_at=at, causal_parent_ids=[],
                           ordering_contract_id="S_order_v1", scenario_ordinal=60),
                payload=dict(intent_id=identity, client_order_id=identity, config_id="P3_ORACLE_CONFIG_H07_V1",
                             product_id="A-USDT-SWAP", strategy_id="H07", side="LONG",
                             order_type="LIMIT", quantity_contracts=D("1"), limit_price=D(price),
                             reduce_only=False, requested_at=at),
            )],
        )

    assert [(order["side"], order["limit_price"], order["accepted_at"])
            for order in codec.historical_working_orders()] == [
        ("LONG", D("103"), (EPOCH - 1) * 16),
    ]
    for step_index, (offset, phase) in enumerate(((0, 7), (20_000, 8), (40_000, 8))):
        result = _h07_node(codec, EPOCH, step_index, "105")
        assert (result["raw_time_ms"], result["effective_at"]) == (
            EPOCH + offset, (EPOCH + offset) * 16 + phase,
        )
        assert all(not product["fills"] for product in result["products"])
        assert result["owner_evidence"]["account_version"] == step_index + 1
        assert [(order["order_id"], order["limit_price"])
                for order in codec.historical_working_orders()] == [("H07-ORDER-1", D("103"))]

    close = _h07_node(codec, EPOCH, 3, "105")
    boundary = EPOCH + 60_000
    assert (close["raw_time_ms"], close["effective_at"]) == (boundary, boundary * 16 + 4)
    assert all(not product["fills"] for product in close["products"])
    # One spec migration at phase 0, then the mark context at the phase-4 close.
    assert close["owner_evidence"]["account_version"] == 5
    assert codec.historical_working_orders() == []

    opening = _h07_node(codec, boundary, 0, "110")
    assert (opening["raw_time_ms"], opening["effective_at"]) == (boundary, boundary * 16 + 7)
    assert all(not product["fills"] for product in opening["products"])
    assert opening["owner_evidence"]["account_version"] == 6
    for step_index in range(1, 4):
        result = _h07_node(codec, EPOCH + 60_000, step_index, "110")
        assert all(not product["fills"] for product in result["products"])
    final = codec.inspect_state()
    assert (final["cash"], final["gross_realized"], final["total_fees"]) == (
        D("1000"), D("0"), D("0"),
    )
    assert codec.historical_working_orders() == []
    trading = codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=account,
        snapshot_id="H07-TRADING-END", snapshot_kind="TRADING",
        capture_mode="OWNER_CURRENT", captured_at=(EPOCH + 120_000) * 16 + 6,
    ))["immutable_payload"]
    assert (trading["equity"], trading["available_equity"]) == (D("1000"), D("1000"))

    rejected_codec = wire.ScenarioCodec(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", account, _h07_configuration()
    )
    for step_index in range(3):
        _h07_node(rejected_codec, EPOCH, step_index, "105")
    _h07_node(rejected_codec, EPOCH, 3, "105")
    before_rejection = rejected_codec.inspect_state()
    rejected = rejected_codec.apply_group(
        intent_group("H07-RAW-103", (EPOCH + 60_000) * 16 + 6, "103")
    )
    assert (rejected["classification"], rejected["failure"], rejected["gate_after"]) == (
        "FAULT", "INVALID_BTC_INTENT", "FAILED",
    )
    after_rejection = rejected_codec.inspect_state()
    assert (after_rejection["cash"], after_rejection["gross_realized"],
            after_rejection["total_fees"], after_rejection["account_version"],
            after_rejection["orders_digest"], after_rejection["positions_digest"],
            after_rejection["reservations_digest"]) == (
        before_rejection["cash"], before_rejection["gross_realized"],
        before_rejection["total_fees"], before_rejection["account_version"],
        before_rejection["orders_digest"], before_rejection["positions_digest"],
        before_rejection["reservations_digest"],
    )
    assert rejected_codec.historical_working_orders() == []
