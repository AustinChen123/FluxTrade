use super::super::super::execution::ExecutionCandidate;
use super::super::super::group::{Applied, Completion, Group, Input, Reference};
use super::super::super::*;
use ring::digest::{digest, SHA256};
use serde_json::{json, Value};

const PRODUCTS: &str = "BTC-USDT-SWAP ETH-USDT-SWAP SOL-USDT-SWAP BNB-USDT-SWAP XRP-USDT-SWAP DOGE-USDT-SWAP ARB-USDT-SWAP OP-USDT-SWAP NEAR-USDT-SWAP APT-USDT-SWAP SUI-USDT-SWAP ADA-USDT-SWAP";
const RESERVATION_PRODUCTS: [&str; 12] = [
    "ADA-USDT-SWAP",
    "APT-USDT-SWAP",
    "ARB-USDT-SWAP",
    "BNB-USDT-SWAP",
    "BTC-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "ETH-USDT-SWAP",
    "NEAR-USDT-SWAP",
    "OP-USDT-SWAP",
    "SOL-USDT-SWAP",
    "SUI-USDT-SWAP",
    "XRP-USDT-SWAP",
];
fn products() -> Vec<&'static str> {
    PRODUCTS.split_whitespace().collect()
}
fn cash() -> Decimal {
    Decimal::new(1212, 1)
}
struct Oracle(Vec<u8>);
impl Oracle {
    fn new(domain: &str) -> Self {
        let mut out = Self(Vec::new());
        out.text(domain);
        out
    }
    fn text(&mut self, value: &str) {
        self.integer(value.len() as i64);
        self.0.extend_from_slice(value.as_bytes());
    }
    fn integer(&mut self, value: i64) {
        self.0.extend_from_slice(&value.to_be_bytes());
    }
    fn optional_text(&mut self, value: Option<&str>) {
        if let Some(value) = value {
            self.0.push(1);
            self.text(value);
        } else {
            self.0.push(0);
        }
    }
    fn optional_integer(&mut self, value: Option<i64>) {
        if let Some(value) = value {
            self.0.push(1);
            self.integer(value);
        } else {
            self.0.push(0);
        }
    }
    fn account(&mut self) {
        for field in ["okx-scenario", "test", "A"] {
            self.text(field);
        }
        self.0.push(0);
    }
    fn hash(self) -> Hash {
        digest(&SHA256, &self.0).as_ref().try_into().unwrap()
    }
}
fn cancel_identity(domain: &str, event: &str, order: &str, phase: Option<i64>) -> Hash {
    let mut e = Oracle::new(domain);
    e.account();
    e.text(event);
    e.text(order);
    if let Some(phase) = phase {
        e.integer(phase);
    }
    e.hash()
}
fn delivery_identity_oracle() -> (Hash, Hash) {
    let mut id = Oracle::new("SCENARIO_DELIVERY_V1");
    id.account();
    id.text("SOURCE");
    id.text("event-516");
    id.text("TRANSPORT_ACK");
    id.integer(0);
    let mut payload = Oracle::new("SCENARIO_DELIVERY_PAYLOAD_V1");
    payload.account();
    payload.text("event-516");
    payload.text("SOURCE");
    payload.text("TRANSPORT_ACK");
    payload.integer(0);
    payload.integer(2);
    payload.text("TRANSPORT_ACK");
    for field in ["WS", "CANCEL", "C-501"] {
        payload.text(field);
    }
    payload.optional_text(None);
    payload.text("0");
    for _ in 0..5 {
        payload.optional_text(None);
    }
    payload.optional_integer(None);
    payload.optional_integer(None);
    payload.integer(517);
    payload.optional_text(None);
    (id.hash(), payload.hash())
}
fn hash_text(value: Hash) -> String {
    value.iter().map(|b| format!("{b:02x}")).collect()
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
fn configured_execution_candidate(
    owner: &ScenarioAccount,
    order: &str,
    event: &str,
    at: i64,
    account_version: u64,
    order_version: u64,
    execution_id: &str,
) -> ExecutionCandidate {
    let payload = json!({"namespace":"test","product_id":products()[0],
        "external_execution_id":execution_id,"order_id":order,"side":"LONG",
        "price":"100","quantity_contracts":"0.5","liquidity":"SYNTHETIC_TAKER",
        "matching_effective_at":at,"candidate_id":format!("candidate-{event}"),
        "source_id":format!("source-{event}"),"visible_at":at,
        "expected_account_version":account_version,"expected_order_version":order_version,
        "spec_version":"scale-spec-v1","rule_data_version":"scale-tier-v1"});
    let group = super::decode_group(
        &group_value(&format!("G-{event}"), event, at, "EXECUTION", payload, 30).to_string(),
        owner,
    )
    .unwrap();
    let Input::Execution(candidate) = &group.members[0].input else {
        panic!("execution")
    };
    candidate.clone()
}
fn cancel_group(owner: &ScenarioAccount, order: &str, effect: bool) -> Group {
    let (at, event, id, ordinal, parent, kind, payload) = if effect {
        (
            516,
            "event-516",
            "G-516",
            50,
            "event-515",
            "CANCEL_EFFECT",
            json!({"effects":[{"detecting_event_id":"event-515","target_order_id":order,
             "reason":"EXPLICIT_SCENARIO"}]}),
        )
    } else {
        (
            515,
            "event-515",
            "G-515",
            40,
            "event-514",
            "CANCEL_REQUEST",
            json!({"targets":[{"target_order_id":order,"reason":"EXPLICIT_SCENARIO"}]}),
        )
    };
    let mut value = group_value(id, event, at, kind, payload, ordinal);
    value["members"][0]["stamp"]["causal_parent_ids"] = json!([parent]);
    super::decode_group(&value.to_string(), owner).unwrap()
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

    let before515 = owner.clone();
    let request_group = cancel_group(&owner, &order_ids[0], false);
    let Input::Request(request) = request_group.members[0].input.clone() else {
        panic!("request")
    };
    let request_id = cancel_identity("CANCEL_REQUEST", "event-515", &order_ids[0], None);
    let effect_action_id = cancel_identity("CANCEL_ACTION", "event-515", &order_ids[0], Some(1));
    let delivery_action_id = cancel_identity("CANCEL_ACTION", "event-515", &order_ids[0], Some(2));
    assert_eq!(
        (
            request_group.members[0].stamp.event_id.as_str(),
            request_group.members[0].stamp.effective_at,
            request_group.members[0].stamp.causal_parent_ids.as_slice(),
            request_group.members[0].stamp.scenario_ordinal,
            request.targets.as_slice()
        ),
        (
            "event-515",
            515,
            &["event-514".to_owned()][..],
            40,
            &[(
                order_ids[0].clone(),
                risk_transition::cancel::Reason::ExplicitScenario
            )][..]
        )
    );
    let Applied::Fresh(requested) = owner
        .apply_group_observed(&request_group, |_| Ok(()))
        .unwrap()
    else {
        panic!("fresh request")
    };
    assert_eq!(
        requested.committed,
        vec![Reference::Source("event-515".into())]
    );
    assert!(requested.rejections.is_empty());
    assert_eq!(
        (owner.state_version, owner.transition.lifecycle),
        (14, risk_transition::Lifecycle::AwaitingCancelEffective)
    );
    let requested_order = &owner.orders[&order_ids[0]];
    assert_eq!(
        (
            requested_order.facts.status.as_str(),
            requested_order.facts.filled,
            requested_order.facts.canceled,
            requested_order.facts.remaining,
            requested_order.version
        ),
        (
            "PARTIALLY_FILLED",
            Decimal::new(5, 1),
            Decimal::ZERO,
            Decimal::new(5, 1),
            3
        )
    );
    let risk_transition::cancel::State::Requested(request_fact) = &requested_order.cancel else {
        panic!("request state")
    };
    assert_eq!(
        (
            request_fact.request_id,
            request_fact.effect_action_id,
            request_fact.delivery_action_id,
            request_fact.detecting_event_id.as_str(),
            request_fact.reason
        ),
        (
            request_id,
            effect_action_id,
            delivery_action_id,
            "event-515",
            risk_transition::cancel::Reason::ExplicitScenario
        )
    );
    let r515 = owner.reservation().unwrap();
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            r515.equity,
            r515.maintenance_margin,
            r515.orders.len(),
            r515.total_fee_hold,
            r515.used_margin,
            r515.available_margin
        ),
        (
            Decimal::new(12115, 2),
            Decimal::new(5, 2),
            Decimal::new(12115, 2),
            Decimal::new(25, 2),
            12,
            Decimal::new(115, 2),
            Decimal::new(12115, 2),
            Decimal::ZERO
        )
    );
    assert_eq!(
        r515.orders
            .iter()
            .map(|r| (
                r.product.0.as_ref(),
                r.order_id.as_str(),
                r.side,
                r.remaining_contracts,
                r.remaining_base_exposure,
                r.order_loss,
                r.fee_hold
            ))
            .collect::<Vec<_>>(),
        RESERVATION_PRODUCTS
            .iter()
            .map(|product| {
                let i = products().iter().position(|p| p == product).unwrap();
                let remaining = if i == 0 {
                    Decimal::new(5, 1)
                } else {
                    Decimal::ONE
                };
                (
                    *product,
                    order_ids[i].as_str(),
                    Side::Long,
                    remaining,
                    remaining,
                    Decimal::ZERO,
                    if i == 0 {
                        Decimal::new(5, 2)
                    } else {
                        Decimal::new(10, 2)
                    },
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        r515.products
            .iter()
            .map(|p| (
                p.product.0.as_ref(),
                p.position_value,
                p.long_remaining_value,
                p.short_remaining_value,
                p.exposure_margin
            ))
            .collect::<Vec<_>>(),
        products()
            .iter()
            .map(|p| (
                *p,
                if *p == "BTC-USDT-SWAP" {
                    Decimal::new(50, 0)
                } else {
                    Decimal::ZERO
                },
                if *p == "BTC-USDT-SWAP" {
                    Decimal::new(50, 0)
                } else {
                    Decimal::new(100, 0)
                },
                Decimal::ZERO,
                Decimal::new(10, 0)
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(owner.positions, before515.positions);
    let request_actions = owner.cancel_facts.actions.clone();
    assert_eq!(request_actions.len(), 2);
    let before_direct_request = owner.clone();
    let duplicate_request = owner.request_cancel(&request).unwrap();
    assert_eq!(owner, before_direct_request);
    assert_eq!(duplicate_request.receipts.len(), 1);
    let rr = &duplicate_request.receipts[0];
    let original_request_receipt = rr.clone();
    assert_eq!(rr.action_id, request_id);
    assert_eq!(rr.action_ids, vec![effect_action_id, delivery_action_id]);
    for (id, phase, outcome) in [(effect_action_id, 1, None), (delivery_action_id, 2, None)] {
        let action = &owner.cancel_facts.actions[&id];
        assert_eq!(
            (
                action.detecting_event_id.as_str(),
                action.target_order_id.as_str(),
                action.phase,
                action.outcome
            ),
            ("event-515", order_ids[0].as_str(), phase, outcome)
        );
    }
    assert_eq!(
        (
            rr.target_order_id.as_str(),
            rr.reason,
            rr.effective_at,
            rr.spec_version.as_str(),
            rr.rule_data_version.as_str()
        ),
        (
            order_ids[0].as_str(),
            risk_transition::cancel::Reason::ExplicitScenario,
            515,
            "scale-spec-v1",
            "scale-tier-v1"
        )
    );
    assert_eq!(rr.account_key, owner.key);
    assert_eq!(rr.before, before515.orders[&order_ids[0]].facts);
    assert_eq!(rr.after, before515.orders[&order_ids[0]].facts);
    assert_eq!(rr.reservation_before, rr.reservation_after);
    assert_eq!(
        rr.lifecycle_after,
        risk_transition::Lifecycle::AwaitingCancelEffective
    );
    assert_eq!(
        (
            rr.event_id.as_str(),
            rr.phase,
            rr.outcome,
            rr.account_version_before,
            rr.account_version_after,
            rr.order_version_before,
            rr.order_version_after
        ),
        (
            "event-515",
            0,
            risk_transition::cancel::Outcome::Requested,
            13,
            14,
            2,
            3
        )
    );
    let after_request = owner.clone();
    assert_eq!(
        duplicate(
            owner
                .apply_group_observed(&request_group, |_| Ok(()))
                .unwrap()
        ),
        requested
    );
    assert_eq!(owner, after_request);

    let effect_group = cancel_group(&owner, &order_ids[0], true);
    let Input::Effect(effect) = effect_group.members[0].input.clone() else {
        panic!("effect")
    };
    assert_eq!(
        (
            effect_group.members[0].stamp.event_id.as_str(),
            effect_group.members[0].stamp.effective_at,
            effect_group.members[0].stamp.causal_parent_ids.as_slice(),
            effect_group.members[0].stamp.scenario_ordinal,
            effect.effects.as_slice()
        ),
        (
            "event-516",
            516,
            &["event-515".to_owned()][..],
            50,
            &[(
                "event-515".into(),
                order_ids[0].clone(),
                risk_transition::cancel::Reason::ExplicitScenario
            )][..]
        )
    );
    let mut request_before_effect = owner.clone();
    let between = configured_execution_candidate(
        &request_before_effect,
        &order_ids[0],
        "event-between",
        516,
        14,
        3,
        "fill-between",
    );
    assert!(matches!(
        request_before_effect.execute(&between),
        Ok(execution::commit::Reply::Committed { .. })
    ));
    assert_eq!(request_before_effect.state_version, 15);
    assert_eq!(
        request_before_effect.orders[&order_ids[0]].facts.status,
        "FILLED"
    );
    assert_eq!(
        request_before_effect.orders[&order_ids[0]].facts.remaining,
        Decimal::ZERO
    );
    assert_eq!(request_before_effect.cash, Decimal::new(1211, 1));
    assert_eq!(request_before_effect.fees, Decimal::new(1, 1));
    let Applied::Fresh(effected) = owner
        .apply_group_observed(&effect_group, |_| Ok(()))
        .unwrap()
    else {
        panic!("fresh effect")
    };
    assert_eq!(
        effected.committed,
        vec![Reference::Source("event-516".into())]
    );
    assert!(effected.rejections.is_empty());
    assert_eq!(
        (owner.state_version, owner.transition.lifecycle),
        (15, risk_transition::Lifecycle::RiskStable)
    );
    let canceled_order = &owner.orders[&order_ids[0]];
    assert_eq!(
        (
            canceled_order.facts.status.as_str(),
            canceled_order.facts.filled,
            canceled_order.facts.canceled,
            canceled_order.facts.remaining,
            canceled_order.version
        ),
        (
            "CANCELED",
            Decimal::new(5, 1),
            Decimal::new(5, 1),
            Decimal::ZERO,
            4
        )
    );
    let risk_transition::cancel::State::EffectiveCanceled(effect_fact) = &canceled_order.cancel
    else {
        panic!("effective cancel state")
    };
    assert_eq!(
        (
            effect_fact.action_id,
            effect_fact.request.request_id,
            effect_fact.request.effect_action_id,
            effect_fact.request.delivery_action_id,
            effect_fact.request.detecting_event_id.as_str(),
            effect_fact.request.reason
        ),
        (
            effect_action_id,
            request_id,
            effect_action_id,
            delivery_action_id,
            "event-515",
            risk_transition::cancel::Reason::ExplicitScenario
        )
    );
    assert_eq!(canceled_order.facts.client_id, "C-501");
    let r516 = owner.reservation().unwrap();
    assert_eq!(
        (
            owner.cash,
            owner.fees,
            owner.gross_realized,
            r516.equity,
            r516.maintenance_margin,
            r516.total_fee_hold,
            r516.used_margin,
            r516.available_margin
        ),
        (
            Decimal::new(12115, 2),
            Decimal::new(5, 2),
            Decimal::ZERO,
            Decimal::new(12115, 2),
            Decimal::new(25, 2),
            Decimal::new(110, 2),
            Decimal::new(1161, 1),
            Decimal::new(505, 2)
        )
    );
    assert_eq!(r516.orders.len(), 11);
    assert_eq!(
        r516.orders
            .iter()
            .map(|r| (
                r.product.0.as_ref(),
                r.order_id.as_str(),
                r.side,
                r.remaining_contracts,
                r.remaining_base_exposure,
                r.order_loss,
                r.fee_hold
            ))
            .collect::<Vec<_>>(),
        RESERVATION_PRODUCTS
            .iter()
            .filter(|product| **product != products()[0])
            .map(|product| {
                let i = products().iter().position(|p| p == product).unwrap();
                (
                    *product,
                    order_ids[i].as_str(),
                    Side::Long,
                    Decimal::ONE,
                    Decimal::ONE,
                    Decimal::ZERO,
                    Decimal::new(10, 2),
                )
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        r516.products
            .iter()
            .map(|p| (
                p.product.0.as_ref(),
                p.position_value,
                p.long_remaining_value,
                p.short_remaining_value,
                p.exposure_margin
            ))
            .collect::<Vec<_>>(),
        products()
            .iter()
            .map(|p| (
                *p,
                if *p == "BTC-USDT-SWAP" {
                    Decimal::new(50, 0)
                } else {
                    Decimal::ZERO
                },
                if *p == "BTC-USDT-SWAP" {
                    Decimal::ZERO
                } else {
                    Decimal::new(100, 0)
                },
                Decimal::ZERO,
                if *p == "BTC-USDT-SWAP" {
                    Decimal::new(5, 0)
                } else {
                    Decimal::new(10, 0)
                }
            ))
            .collect::<Vec<_>>()
    );
    assert_eq!(owner.cancel_facts.actions.len(), 2);
    for id in &order_ids[1..] {
        assert_eq!(owner.orders[id], before515.orders[id]);
    }
    let before_direct_effect = owner.clone();
    let duplicate_effect = owner.effect_cancel(&effect).unwrap();
    assert_eq!(owner, before_direct_effect);
    let immutable_request = owner.request_cancel(&request).unwrap();
    assert_eq!(immutable_request.receipts, vec![original_request_receipt]);
    assert_eq!(owner, before_direct_effect);
    assert_eq!(duplicate_effect.receipts.len(), 1);
    let er = &duplicate_effect.receipts[0];
    assert_eq!(er.action_id, effect_action_id);
    assert_eq!(er.action_ids, vec![effect_action_id]);
    assert_eq!(
        (
            er.reason,
            er.effective_at,
            er.spec_version.as_str(),
            er.rule_data_version.as_str()
        ),
        (
            risk_transition::cancel::Reason::ExplicitScenario,
            516,
            "scale-spec-v1",
            "scale-tier-v1"
        )
    );
    assert_eq!(er.account_key, owner.key);
    assert_eq!(er.before, before515.orders[&order_ids[0]].facts);
    assert_eq!(er.after, owner.orders[&order_ids[0]].facts);
    assert_eq!(er.lifecycle_after, risk_transition::Lifecycle::RiskStable);
    assert_eq!(
        er.reservation_before,
        execution::FinancialSnapshot::BtcEth(r515.clone())
    );
    assert_eq!(
        er.reservation_after,
        execution::FinancialSnapshot::BtcEth(r516.clone())
    );
    assert_eq!(
        owner
            .cancel_facts
            .actions
            .keys()
            .copied()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([effect_action_id, delivery_action_id])
    );
    assert_eq!(
        (
            er.target_order_id.as_str(),
            er.after.order_id.as_str(),
            er.after.product.canonical_id()
        ),
        (order_ids[0].as_str(), order_ids[0].as_str(), products()[0])
    );
    assert_eq!(
        (
            er.event_id.as_str(),
            er.detecting_event_id.as_str(),
            er.phase,
            er.outcome,
            er.account_version_before,
            er.account_version_after,
            er.order_version_before,
            er.order_version_after
        ),
        (
            "event-516",
            "event-515",
            1,
            risk_transition::cancel::Outcome::EffectiveCanceled,
            14,
            15,
            3,
            4
        )
    );
    let outstanding: Vec<_> = owner
        .cancel_facts
        .actions
        .values()
        .filter(|a| a.phase == 2 && a.outcome.is_none())
        .collect();
    assert_eq!(outstanding.len(), 1);
    assert_eq!(outstanding[0].target_order_id, order_ids[0]);
    assert_eq!(
        owner
            .cancel_facts
            .actions
            .values()
            .find(|a| a.phase == 1)
            .unwrap()
            .outcome,
        Some(risk_transition::cancel::Outcome::EffectiveCanceled)
    );
    assert_eq!(owner.positions, before515.positions);

    let before517 = owner.clone();
    let ack_projection = delivery::wire::decode_projection(
        &json!({
        "schema_version":"delivery_projection_v1",
        "reference":{"namespace":"SOURCE","fact_id":"event-516"},
        "payload_kind":"TRANSPORT_ACK","occurrence_index":0,"schedule_sequence":2,
        "visible_at":517,"transport":{"route":"WS","operation":"CANCEL",
            "client_order_id":"C-501","code":"0"}})
        .to_string(),
    )
    .unwrap();
    let ack = delivery_store
        .build(&owner, &snapshots, &ack_projection)
        .unwrap();
    let ack_bytes = ack.wire_json().unwrap().canonical().unwrap();
    assert_eq!(
        ack_bytes,
        delivery_store
            .build(&owner, &snapshots, &ack_projection)
            .unwrap()
            .wire_json()
            .unwrap()
            .canonical()
            .unwrap()
    );
    let ack_json: Value = serde_json::from_str(&ack_bytes).unwrap();
    let (expected_delivery_id, expected_payload_digest) = delivery_identity_oracle();
    assert_eq!(ack.delivery_id, expected_delivery_id);
    assert_eq!(ack.payload_digest, expected_payload_digest);
    assert_eq!(ack_json["delivery_id"], hash_text(expected_delivery_id));
    assert_eq!(
        ack_json["payload_digest"],
        hash_text(expected_payload_digest)
    );
    assert_eq!(
        ack_json["account_key"],
        json!({
        "venue":"okx-scenario","environment":"test","account":"A"})
    );
    assert_eq!(ack_json["source_fact_id"], "event-516");
    assert_eq!(ack_json["source_namespace"], "SOURCE");
    assert_eq!(ack_json["payload_kind"], "TRANSPORT_ACK");
    assert_eq!(ack_json["occurrence_index"], 0);
    assert_eq!(ack_json["schedule_sequence"], 2);
    assert_eq!(ack_json["visible_at"], 517);
    assert_eq!(
        ack_json["immutable_payload"],
        json!({
        "route":"WS","operation":"CANCEL","client_order_id":"C-501","code":"0"})
    );
    assert_eq!(owner, before517);
    let request_ack = delivery::wire::decode_projection(
        &json!({"schema_version":"delivery_projection_v1",
        "reference":{"namespace":"SOURCE","fact_id":"event-515"},
        "payload_kind":"TRANSPORT_ACK","occurrence_index":0,"schedule_sequence":2,
        "visible_at":517,"transport":{"route":"WS","operation":"CANCEL",
            "client_order_id":"C-501","code":"0"}})
        .to_string(),
    )
    .unwrap();
    let mut independent_store = delivery::store::Store::new(owner.key.clone());
    let independent_owner = owner.clone();
    assert_eq!(
        independent_store.build(&independent_owner, &snapshots, &request_ack),
        Err("INVALID_SCHEMA")
    );
    assert_eq!(independent_owner, before517);
    let after_ack = owner.clone();
    assert_eq!(
        delivery_store
            .build(&owner, &snapshots, &ack_projection)
            .unwrap(),
        ack
    );
    assert_eq!(owner, after_ack);
    assert_eq!(
        duplicate(
            owner
                .apply_group_observed(&effect_group, |_| Ok(()))
                .unwrap()
        ),
        effected
    );
    assert_eq!(owner, after_ack);
    assert_eq!(
        owner.execute(&candidate).unwrap(),
        execution::commit::Reply::Duplicate(
            owner.execution_receipts.values().next().unwrap().clone()
        )
    );
    assert_eq!(owner, after_ack);
    assert_eq!(owner.gate, Gate::Running);
    assert!(!owner
        .transition
        .event_kinds
        .values()
        .any(|kind| *kind == source::Kind::EventC));
    let terminal_candidate =
        configured_execution_candidate(&owner, &order_ids[0], "event-518", 518, 15, 4, "fill-518");
    let mut expected_failed = owner.clone();
    expected_failed.gate = Gate::Failed("UNSUPPORTED_EXECUTION");
    assert_eq!(
        owner.execute(&terminal_candidate),
        Err("UNSUPPORTED_EXECUTION")
    );
    assert_eq!(owner, expected_failed);
}
