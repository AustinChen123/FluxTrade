"""Causal acceptance tests for the run-local paid endpoint on the native owner."""

from dataclasses import replace
from decimal import Decimal as D

from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_paid_endpoint import PaidEndpoint
from test_spider_execution_input import _input
from test_spider_historical_input import (
    _initial_account_state,
    _policy_cache,
    _p2_configuration,
    _rehashed_run,
    _run_with_configuration,
    _valid_run,
)


def _seeded_two_product_run(*, with_working_order: bool = False, late_fill_order: bool = False):
    run = _valid_run()
    configuration = _p2_configuration()
    configuration["cash"] = "10000"
    configuration["positions"] = [
        dict(
            product_id=product,
            side=side,
            quantity_contracts="1",
            lots=[
                dict(
                    seed_execution_id=f"PAID-SEED-{product}",
                    seed_sequence=sequence,
                    strategy_id=run.strategy_identity,
                    quantity_contracts="1",
                    entry_price="100",
                )
            ],
        )
        for sequence, (product, side) in enumerate(
            (("A-USDT-SWAP", "LONG"), ("B-USDT-SWAP", "SHORT"))
        )
    ]
    if with_working_order:
        configuration["orders"] = [
            dict(
                intent_id="PAID-SEED-INTENT-A",
                order_id="PAID-SEED-ORDER-A",
                client_order_id="PAID-SEED-CLIENT-A",
                strategy_id=run.strategy_identity,
                product_id="A-USDT-SWAP",
                side="LONG",
                limit_price="99" if late_fill_order else "50",
                reduce_only=False,
                original_quantity_contracts="0.2",
                filled_quantity_contracts="0",
                canceled_quantity_contracts="0",
                remaining_quantity_contracts="0.2",
                status="OPEN",
            )
        ]
    run = _run_with_configuration(run, configuration)
    if late_fill_order:
        run = _rehashed_run(
            run,
            trade_rows=tuple(
                replace(row, high=D("102"), low=D("98"), volume=D("8"))
                if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms
                else row
                for row in run.trade_bars
            ),
        )
    run = replace(
        run,
        initial_account_state=_initial_account_state(
            cash="10000",
            orders=configuration["orders"],
            positions=configuration["positions"],
        ),
        initial_policy_cache=_policy_cache(running=False),
    )
    return _input(run)


def _dispatch_paid(request):
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    endpoint = _require_paid_endpoint(composition)
    return composition, endpoint.result(composition)


def _require_paid_endpoint(composition: _ReplayComposition) -> PaidEndpoint:
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    return endpoint


def test_two_product_seeded_positions_close_through_native_paid_endpoint():
    request = _seeded_two_product_run()
    composition, result = _dispatch_paid(request)

    assert result["status"] == "PAID_FLAT"
    assert result["reasons"] == []
    assert result["pending_obligations"] == dict(
        positions=[],
        working_orders=[],
        pending_actions=[],
        pending_deliveries=[],
        pending_polls=[],
    )
    assert [action["role"] for action in result["actions"]] == ["EXIT", "EXIT"]
    exits = [action for action in result["actions"] if action["role"] == "EXIT"]
    assert [action["product_id"] for action in exits] == ["A-USDT-SWAP", "B-USDT-SWAP"]
    assert all(action["classification"] == "COMMITTED" for action in exits)
    assert all(action["order_id"] for action in exits)

    intent_records = [
        record
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"]["members"][0]["kind"] == "INTENT"
        and record["item"]["group"]["members"][0]["payload"].get("reduce_only") is True
    ]
    assert len(intent_records) == 2
    assert {record["item"]["group"]["members"][0]["payload"]["product_id"] for record in intent_records} == {
        "A-USDT-SWAP",
        "B-USDT-SWAP",
    }

    fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
        if fill["order_id"] in {action["order_id"] for action in exits}
    ]
    assert len(fills) == 2
    fills_by_order = {fill["order_id"]: fill for fill in fills}
    assert {fills_by_order[action["order_id"]]["quantity_contracts"] for action in exits} == {D("1")}
    assert {
        fills_by_order[action["order_id"]]["price"]
        for action in exits
        if action["product_id"] == "A-USDT-SWAP"
    } == {D("99.9")}
    assert {
        fills_by_order[action["order_id"]]["price"]
        for action in exits
        if action["product_id"] == "B-USDT-SWAP"
    } == {D("100.1")}

    trading = result["owner_evidence"]["trading_fact"]["immutable_payload"]
    # The fixed fixture uses contract value 1, 0.1% taker fees, 10 bps
    # slippage, and 1-contract opposing positions opened at 100.
    expected_fees = D("99.9") * D("0.001") + D("100.1") * D("0.001")
    expected_realized = (D("99.9") - D("100")) + (D("100") - D("100.1"))
    assert expected_fees == D("0.2")
    assert expected_realized == D("-0.2")
    assert trading["equity"] == D("10000") + expected_realized - expected_fees
    assert trading["available_equity"] == trading["equity"]

    _, repeated = _dispatch_paid(request)
    assert canonical_bytes(repeated) == canonical_bytes(result)


def test_short_cutoff_keeps_native_position_and_exit_obligation_incomplete():
    request = _seeded_two_product_run()
    request = replace(request, policy_end_ms=request.policy_end_ms + 59_000)
    _, result = _dispatch_paid(request)

    assert result["status"] == "INCOMPLETE"
    assert result["reasons"] == ["EXIT_PENDING", "RESIDUAL_POSITION"]
    assert [row["position_contracts"] for row in result["pending_obligations"]["positions"]] == [
        D("1"),
        D("-1"),
    ]
    assert [action["role"] for action in result["actions"]] == ["EXIT", "EXIT"]
    assert all(action["classification"] == "COMMITTED" for action in result["actions"])
    assert len(result["pending_obligations"]["working_orders"]) == 2
    assert len(result["pending_obligations"]["pending_actions"]) == 2
    assert result["settlement_end_ms"] == request.historical_input.range_end_ms
    assert result["owner_evidence"]["cutoff"] == request.historical_input.range_end_ms * 16 + 6


def test_financially_flat_short_cutoff_remains_incomplete_until_execution_notices():
    request = _seeded_two_product_run()
    request = replace(request, policy_end_ms=request.policy_end_ms + 30_000)
    _, result = _dispatch_paid(request)

    assert result["status"] == "INCOMPLETE"
    assert result["reasons"] == ["NOTIFICATION_PENDING"]
    assert result["pending_obligations"]["positions"] == []
    assert result["pending_obligations"]["working_orders"] == []
    assert result["pending_obligations"]["pending_actions"] == []
    assert len(result["pending_obligations"]["pending_deliveries"]) == 2
    assert all(action["role"] == "EXIT" and action["classification"] == "COMMITTED"
               for action in result["actions"])


def test_seeded_working_grid_order_is_canceled_before_native_exit():
    composition, result = _dispatch_paid(_seeded_two_product_run(with_working_order=True))

    assert result["status"] == "PAID_FLAT"
    drain = [action for action in result["actions"] if action["role"] == "DRAIN_CANCEL"]
    exits = [action for action in result["actions"] if action["role"] == "EXIT"]
    assert [action["classification"] for action in drain] == ["COMMITTED", "COMMITTED"]
    assert drain[0]["order_id"] == "PAID-SEED-ORDER-A"
    assert drain[1]["parent_action_id"] == drain[0]["action_id"]
    assert [action["product_id"] for action in exits] == ["A-USDT-SWAP", "B-USDT-SWAP"]
    assert result["pending_obligations"]["working_orders"] == []
    source_member_kinds = [
        record["item"]["group"]["members"][0]["kind"]
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
    ]
    assert "CANCEL_REQUEST" in source_member_kinds
    assert "CANCEL_EFFECT" in source_member_kinds
    cancel_acks = [
        record["item"]["delivery"]["immutable_payload"]
        for record in composition._records.values()
        if record["item"].get("kind") == "DELIVERY"
        and record["item"]["delivery"].get("payload_kind") == "TRANSPORT_ACK"
    ]
    assert cancel_acks == [dict(
        client_order_id="PAID-SEED-CLIENT-A",
        code="0",
        operation="CANCEL",
        order_id="PAID-SEED-ORDER-A",
        route="WS",
    )]


def test_cancel_pending_keeps_seeded_order_open_until_native_effect():
    request = _seeded_two_product_run(with_working_order=True)
    composition = _ReplayComposition._from_execution_run(request)

    before_effect = (request.policy_end_ms + 4_999) * 16 + 15
    assert composition._dispatch_due(before_effect)["classification"] == "SUCCESS"
    order = next(
        order
        for order in composition._codec.historical_working_orders()
        if order["order_id"] == "PAID-SEED-ORDER-A"
    )
    assert order["status"] == "OPEN"
    assert order["remaining_quantity_contracts"] == D("0.2")
    endpoint = _require_paid_endpoint(composition)
    cancel_actions = [action for action in endpoint.actions
                      if action["role"] == "DRAIN_CANCEL"]
    assert [action["classification"] for action in cancel_actions] == ["COMMITTED", "PENDING"]

    assert composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    result = endpoint.result(composition)
    assert result["status"] == "PAID_FLAT"
    assert not result["pending_obligations"]["working_orders"]


def test_partial_exit_uses_one_native_order_and_retains_exact_signed_remainder():
    request = _seeded_two_product_run()
    run = request.historical_input
    limited_volume = tuple(replace(row, volume=D("0.4")) for row in run.trade_bars)
    run = _rehashed_run(run, trade_rows=limited_volume)
    request = _input(run)
    composition, result = _dispatch_paid(request)

    exits = [action for action in result["actions"] if action["role"] == "EXIT"]
    assert len(exits) == 2
    fills_by_order = {}
    for record in composition._records.values():
        if record["item"].get("kind") != "HISTORICAL_MARKET_STEP":
            continue
        for product in record["result"]["historical_result"]["products"]:
            for fill in product["fills"]:
                if fill["order_id"] in {action["order_id"] for action in exits}:
                    fills_by_order.setdefault(fill["order_id"], []).append(
                        (product["product_id"], fill)
                    )

    assert {action["order_id"] for action in exits} == set(fills_by_order)
    assert all(len(fills_by_order[action["order_id"]]) == 2 for action in exits)
    assert all(
        fill["quantity_contracts"] == D("0.1")
        for order_fills in fills_by_order.values()
        for _, fill in order_fills
    )
    assert all(
        product == action["product_id"]
        for action in exits
        for product, _ in fills_by_order[action["order_id"]]
    )
    assert [row["position_contracts"] for row in result["pending_obligations"]["positions"]] == [
        D("0.8"),
        D("-0.8"),
    ]
    assert len(result["pending_obligations"]["working_orders"]) == 2
    assert all(order["cumulative_filled_size_contracts"] == D("0.2")
               for order in result["pending_obligations"]["working_orders"])
    assert len({action["product_id"] for action in exits}) == 2
    trading = result["owner_evidence"]["trading_fact"]["immutable_payload"]
    partial_fees = D("99.9") * D("0.001") * D("0.1") * 2 + D("100.1") * D("0.001") * D("0.1") * 2
    partial_realized = (D("99.9") - D("100")) * D("0.2") + (D("100") - D("100.1")) * D("0.2")
    assert trading["equity"] == D("10000") + partial_realized - partial_fees


def test_late_native_fill_during_cancel_pending_remains_owned_and_is_settled():
    request = _seeded_two_product_run(with_working_order=True, late_fill_order=True)
    request = replace(request, policy_end_ms=request.policy_end_ms + 39_000)
    composition = _ReplayComposition._from_execution_run(request)
    pre_effect_at = (request.policy_end_ms + 4_999) * 16 + 15
    assert composition._dispatch_due(pre_effect_at)["classification"] == "SUCCESS"
    endpoint = _require_paid_endpoint(composition)
    pending_cancel = [action for action in endpoint.actions
                      if action["role"] == "DRAIN_CANCEL"]
    assert [action["classification"] for action in pending_cancel] == ["COMMITTED", "PENDING"]
    assert pending_cancel[1]["parent_action_id"] == pending_cancel[0]["action_id"]
    pre_effect_fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
        if fill["order_id"] == "PAID-SEED-ORDER-A"
    ]
    assert len(pre_effect_fills) == 1

    assert composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    result = endpoint.result(composition)
    assert result["status"] == "INCOMPLETE"
    assert result["reasons"] == ["EXIT_PENDING", "RESIDUAL_POSITION"]
    assert [row["position_contracts"] for row in result["pending_obligations"]["positions"]] == [
        D("1.2"),
        D("-1"),
    ]
    assert all(action["classification"] == "COMMITTED" for action in result["actions"])
    assert result["pending_obligations"]["working_orders"][0]["original_size_contracts"] == D("1.2")
    late_fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
        if fill["order_id"] == "PAID-SEED-ORDER-A"
    ]
    assert len(late_fills) == 1
    assert late_fills[0]["quantity_contracts"] == D("0.2")
    assert any(action["role"] == "EXIT" and action["product_id"] == "A-USDT-SWAP"
               for action in result["actions"])
