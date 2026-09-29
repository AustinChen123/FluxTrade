use super::super::super::execution::ExecutionCandidate;
use super::super::super::group::{Applied, Completion, Group, Input, Reference};
use super::super::super::*;
use serde_json::{json, Value};

const PRODUCTS: &str = "BTC-USDT-SWAP ETH-USDT-SWAP SOL-USDT-SWAP BNB-USDT-SWAP XRP-USDT-SWAP DOGE-USDT-SWAP ARB-USDT-SWAP OP-USDT-SWAP NEAR-USDT-SWAP APT-USDT-SWAP SUI-USDT-SWAP ADA-USDT-SWAP";
fn products() -> Vec<&'static str> {
    PRODUCTS.split_whitespace().collect()
}
fn cash() -> Decimal {
    Decimal::new(1212, 1)
}

fn configured_owner() -> ScenarioAccount {
    let (seed, rows) = super::super::super::configured_tests::input(12);
    assert_eq!(
        (seed.config_id.as_str(), seed.effective_at, seed.cash),
        ("SYNTHETIC_P2_SCALE_12_V1", 500, cash())
    );
    assert!(seed.positions.is_empty() && seed.orders.is_empty());
    assert_eq!(
        rows.iter()
            .map(|r| r.product.0.as_ref())
            .collect::<Vec<_>>(),
        products()
    );
    ScenarioAccount::from_configured(&seed, Decimal::new(10, 0), rows).unwrap()
}

fn group_value(id: &str, event: &str, at: i64, kind: &str, payload: Value, ordinal: i64) -> Value {
    json!({"schema_version":"scenario_group_v1","group_id":id,
        "account_key":{"venue":"okx-scenario","environment":"test","account":"A"},
        "ordering_contract_id":"S_order_v1","group_effective_at":at,"declared_member_count":1,
        "members":[{"kind":kind,"payload":payload,"stamp":{"event_id":event,"effective_at":at,
            "causal_parent_ids":[],"ordering_contract_id":"S_order_v1","scenario_ordinal":ordinal}}]})
}
fn intent_group(owner: &ScenarioAccount, index: usize, at: i64) -> Group {
    let id = format!("I-{at}");
    let event = format!("event-{at}");
    let payload = json!({"intent_id":id,"client_order_id":format!("C-{at}"),
        "config_id":owner.config_id.clone(),"product_id":products()[index],"strategy_id":"section6",
        "side":"LONG","order_type":"LIMIT","quantity_contracts":"1","limit_price":"100",
        "reduce_only":false,"requested_at":at});
    super::decode_group(
        &group_value(&format!("G-{at}"), &event, at, "INTENT", payload, 60).to_string(),
        owner,
    )
    .unwrap()
}
fn execution_group(owner: &ScenarioAccount, order: &str) -> (Group, ExecutionCandidate) {
    let event = "event-514";
    let payload = json!({"namespace":"test","product_id":products()[0],"external_execution_id":"fill-514",
        "order_id":order,"side":"LONG","price":"100","quantity_contracts":"0.5",
        "liquidity":"SYNTHETIC_TAKER","matching_effective_at":514,"candidate_id":"candidate-514",
        "source_id":"source-514","visible_at":514,"expected_account_version":12,
        "expected_order_version":1,"spec_version":"scale-spec-v1","rule_data_version":"scale-tier-v1"});
    let group = super::decode_group(
        &group_value("G-514", event, 514, "EXECUTION", payload, 30).to_string(),
        owner,
    )
    .unwrap();
    let Input::Execution(candidate) = &group.members[0].input else {
        panic!("execution member")
    };
    let candidate = candidate.clone();
    (group, candidate)
}
fn duplicate(result: Applied) -> Completion {
    let Applied::Duplicate(completion) = result else {
        panic!("duplicate group")
    };
    completion
}

#[test]
fn frozen_section6_admission_rejection_and_514_partial_fill_are_causal() {
    let mut owner = configured_owner();
    let mut order_ids = Vec::new();
    for at in 501..=512 {
        let n = at - 500;
        assert_eq!(owner.state_version, (n - 1) as u64);
        let group = intent_group(&owner, (n - 1) as usize, at);
        let Applied::Fresh(completion) = owner.apply_group_observed(&group, |_| Ok(())).unwrap()
        else {
            panic!("fresh")
        };
        assert_eq!(completion.rejections, Vec::<(String, Fault)>::new());
        assert_eq!(
            completion.committed,
            vec![Reference::Source(format!("event-{at}"))]
        );
        assert_eq!(owner.state_version, n as u64);
        assert_eq!(owner.gate, Gate::Running);
        assert_eq!((owner.cash, owner.fees), (cash(), Decimal::ZERO));
        let event = format!("event-{at}");
        order_ids.push(
            owner
                .intent_results
                .values()
                .find_map(|r| r.delivery_order(&event))
                .unwrap()
                .order_id
                .clone(),
        );
        let snapshot = owner.reservation().unwrap();
        assert_eq!(snapshot.equity, cash());
        assert_eq!(snapshot.used_margin, Decimal::new(101 * n, 1));
        assert_eq!(snapshot.available_margin, Decimal::new(1212 - 101 * n, 1));
    }
    let reservation = owner.reservation().unwrap();
    assert_eq!(owner.orders.len(), 12);
    for (i, (product, order_id)) in products().iter().zip(&order_ids).enumerate() {
        let order = &owner.orders[order_id];
        assert_eq!(
            (
                order.facts.product.canonical_id(),
                order.facts.side,
                order.facts.price,
                order.facts.original,
                order.facts.filled,
                order.facts.remaining,
                order.facts.status.as_str(),
                order.version
            ),
            (
                *product,
                Side::Long,
                Decimal::new(100, 0),
                Decimal::ONE,
                Decimal::ZERO,
                Decimal::ONE,
                "OPEN",
                1
            )
        );
        let product_row = &reservation.products[i];
        assert_eq!(
            (
                product_row.product.0.as_ref(),
                product_row.position_value,
                product_row.long_remaining_value,
                product_row.short_remaining_value,
                product_row.exposure_margin
            ),
            (
                *product,
                Decimal::ZERO,
                Decimal::new(100, 0),
                Decimal::ZERO,
                Decimal::new(10, 0)
            )
        );
    }
    let mut expected_orders: Vec<_> = products()
        .into_iter()
        .map(str::to_string)
        .zip(order_ids.iter().cloned())
        .collect();
    expected_orders.sort();
    let actual_orders: Vec<_> = reservation
        .orders
        .iter()
        .map(|r| {
            (
                r.product.0.to_string(),
                r.order_id.clone(),
                r.side,
                r.remaining_contracts,
                r.remaining_base_exposure,
                r.order_loss,
                r.fee_hold,
            )
        })
        .collect();
    let expected_order_rows: Vec<_> = expected_orders
        .iter()
        .map(|(p, id)| {
            (
                p.to_string(),
                id.clone(),
                Side::Long,
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ZERO,
                Decimal::new(1, 1),
            )
        })
        .collect();
    assert_eq!(actual_orders, expected_order_rows);
    assert_eq!(reservation.orders.len(), 12);

    let rejected = intent_group(&owner, 0, 513);
    let before = owner.clone();
    let before_reservation = owner.reservation().unwrap();
    let mut risk_hook_calls = 0;
    let Applied::Fresh(rejection) = owner
        .apply_group_observed(&rejected, |_| {
            risk_hook_calls += 1;
            Ok(())
        })
        .unwrap()
    else {
        panic!("fresh rejection")
    };
    assert_eq!(
        rejection.rejections,
        vec![("event-513".into(), "INSUFFICIENT_SHARED_EQUITY")]
    );
    assert!(rejection.committed.is_empty());
    assert_eq!(risk_hook_calls, 0);
    assert_eq!(owner.state_version, 12);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.cash, before.cash);
    assert_eq!(owner.fees, before.fees);
    assert_eq!(owner.orders, before.orders);
    assert_eq!(owner.reservation().unwrap(), before_reservation);
    assert!(
        owner
            .transition
            .groups
            .contains_key(&("S_order_v1".into(), "G-513".into()))
            && owner.intent_results.contains_key("I-513")
    );
    let after_rejection = owner.clone();
    assert_eq!(
        duplicate(owner.apply_group_observed(&rejected, |_| Ok(())).unwrap()),
        rejection
    );
    assert_eq!(owner, after_rejection);

    let order_id = order_ids[0].clone();
    let (fill_group, candidate) = execution_group(&owner, &order_id);
    assert_eq!(owner.orders[&order_id].version, 1);
    let before_fill_reservation = owner.reservation().unwrap();
    let other_orders: BTreeMap<_, _> = owner
        .orders
        .iter()
        .filter(|(id, _)| *id != &order_id)
        .map(|(id, o)| (id.clone(), o.clone()))
        .collect();
    let Applied::Fresh(fill) = owner.apply_group_observed(&fill_group, |_| Ok(())).unwrap() else {
        panic!("fresh fill")
    };
    assert_eq!(fill.failure, None);
    assert_eq!(fill.committed, vec![Reference::Source("event-514".into())]);
    assert_eq!(
        (owner.cash, owner.fees, owner.gross_realized),
        (Decimal::new(12115, 2), Decimal::new(5, 2), Decimal::ZERO)
    );
    assert_eq!(owner.state_version, 13);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(
        owner.transition.lifecycle,
        risk_transition::Lifecycle::RiskStable
    );
    assert_eq!(owner.liquidation_ids().count(), 0);
    assert!(owner.pending_actions.is_empty());
    let order = &owner.orders[&order_id];
    assert_eq!(
        (
            order.facts.status.as_str(),
            order.facts.original,
            order.facts.filled,
            order.facts.remaining,
            order.version
        ),
        (
            "PARTIALLY_FILLED",
            Decimal::ONE,
            Decimal::new(5, 1),
            Decimal::new(5, 1),
            2
        )
    );
    let position = &owner.positions.btc().unwrap()[&Product(products()[0].into())];
    assert_eq!(
        (position.side, position.contracts, position.entry_basis),
        (Side::Long, Decimal::new(5, 1), Decimal::new(50, 0))
    );
    let settled = owner.reservation().unwrap();
    assert_eq!(
        (
            settled.equity,
            settled.maintenance_margin,
            settled.used_margin,
            settled.available_margin
        ),
        (
            Decimal::new(12115, 2),
            Decimal::new(25, 2),
            Decimal::new(12115, 2),
            Decimal::ZERO
        )
    );
    assert_eq!(settled.orders.len(), 12);
    assert_eq!(
        settled
            .orders
            .iter()
            .map(|r| (r.product.0.to_string(), r.order_id.clone()))
            .collect::<Vec<_>>(),
        expected_orders
    );
    for ((product, id), row) in expected_orders.iter().zip(&settled.orders) {
        let btc = product == products()[0];
        let quantity = if btc {
            Decimal::new(5, 1)
        } else {
            Decimal::ONE
        };
        let fee = if btc {
            Decimal::new(5, 2)
        } else {
            Decimal::new(1, 1)
        };
        assert_eq!(
            (row.product.0.as_ref(), row.order_id.as_str()),
            (product.as_str(), id.as_str())
        );
        assert_eq!(
            (
                row.side,
                row.remaining_contracts,
                row.remaining_base_exposure,
                row.order_loss,
                row.fee_hold
            ),
            (Side::Long, quantity, quantity, Decimal::ZERO, fee)
        );
        if !btc {
            assert_eq!(
                row,
                before_fill_reservation
                    .orders
                    .iter()
                    .find(|p| p.order_id == *id)
                    .unwrap()
            );
        }
    }
    let expected_products = products();
    assert_eq!(settled.products.len(), 12);
    assert_eq!(
        settled
            .products
            .iter()
            .map(|p| p.product.0.as_ref())
            .collect::<Vec<_>>(),
        expected_products
    );
    for (i, (product, row)) in expected_products.iter().zip(&settled.products).enumerate() {
        assert_eq!(row.product.0.as_ref(), *product);
        if i == 0 {
            assert_eq!(
                (
                    row.position_value,
                    row.long_remaining_value,
                    row.short_remaining_value,
                    row.exposure_margin
                ),
                (
                    Decimal::new(50, 0),
                    Decimal::new(50, 0),
                    Decimal::ZERO,
                    Decimal::new(10, 0)
                )
            );
        } else {
            assert_eq!(row, &before_fill_reservation.products[i]);
            assert_eq!(row.exposure_margin, Decimal::new(10, 0));
        }
    }
    for (id, old) in &other_orders {
        assert_eq!(&owner.orders[id], old);
    }
    assert_eq!(owner.execution_receipts.len(), 1);

    let receipt_before = owner.clone();
    let result =
        wire::result::GroupResult::from_outcome(&owner, &fill_group.group_id, 12, Ok(&fill))
            .unwrap()
            .canonical()
            .unwrap();
    assert_eq!(
        wire::result::GroupResult::from_outcome(&owner, &fill_group.group_id, 12, Ok(&fill))
            .unwrap()
            .canonical()
            .unwrap(),
        result
    );
    assert_eq!(owner, receipt_before);
    let mut delivery_store = delivery::store::Store::new(owner.key.clone());
    let snapshots = snapshot::Store::new(owner.key.clone());
    let projection = delivery::wire::decode_projection(
        &json!({"schema_version":"delivery_projection_v1",
        "reference":{"namespace":"SOURCE","fact_id":"event-514"},"payload_kind":"EXECUTION_FACT",
        "occurrence_index":0,"schedule_sequence":1,"visible_at":514})
        .to_string(),
    )
    .unwrap();
    let delivery = delivery_store
        .build(&owner, &snapshots, &projection)
        .unwrap();
    let _delivered = delivery.wire_json().unwrap().canonical().unwrap();
    assert_eq!(
        delivery_store
            .build(&owner, &snapshots, &projection)
            .unwrap(),
        delivery
    );
    assert_eq!(owner, receipt_before);

    let duplicate_group_before = owner.clone();
    assert_eq!(
        duplicate(owner.apply_group_observed(&fill_group, |_| Ok(())).unwrap()),
        fill
    );
    assert_eq!(owner, duplicate_group_before);
    assert_eq!(
        owner.execute(&candidate).unwrap(),
        execution::commit::Reply::Duplicate(
            owner.execution_receipts.values().next().unwrap().clone()
        )
    );
    assert_eq!(owner, duplicate_group_before);
    let last_group = intent_group(&owner, 11, 512);
    let last_result = owner.transition.groups[&("S_order_v1".into(), "G-512".into())].clone();
    assert_eq!(
        duplicate(owner.apply_group_observed(&last_group, |_| Ok(())).unwrap()),
        last_result
    );

    let bad_projection = delivery::wire::decode_projection(
        &json!({"schema_version":"delivery_projection_v1",
        "reference":{"namespace":"SOURCE","fact_id":"event-513"},"payload_kind":"TRANSPORT_ACK",
        "occurrence_index":0,"schedule_sequence":2,"visible_at":514,
        "transport":{"route":"WS","operation":"ORDER","client_order_id":"C-513","code":"0"}})
        .to_string(),
    )
    .unwrap();
    assert_eq!(
        delivery_store.build(&owner, &snapshots, &bad_projection),
        Err("INVALID_SCHEMA")
    );
    assert_eq!(owner, duplicate_group_before);
}
