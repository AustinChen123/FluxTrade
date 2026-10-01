use super::super::tests::{d, fixture};
use super::*;
use cancel::{EffectInput, Reason, RequestInput};
use execution::commit::tests::{at, input};
pub(in super::super) fn mixed_owner() -> ScenarioAccount {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("200");
    marks[0].valid_to = 600;
    marks.push(Mark {
        product: Product::Btc,
        price: d("10000"),
        valid_from: 600,
        valid_to: 3000,
    });
    let mut a = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let id = admission::admit_execution_fixture(&mut a, Side::Short, d("1"), d("50000"));
    a.execute(&at(input(&a, &id, "X", "0.5", "50000"), 501))
        .unwrap();
    let request = a
        .request_cancel(&RequestInput {
            stamp: source::stamp("R", 502, 40),
            targets: vec![(id.clone(), Reason::ExplicitScenario)],
        })
        .unwrap();
    assert!(!request.receipts.is_empty());
    let effect = a
        .effect_cancel(&EffectInput {
            stamp: source::stamp("E", 503, 50),
            effects: vec![("R".into(), id, Reason::ExplicitScenario)],
        })
        .unwrap();
    assert!(!effect.receipts.is_empty());
    let (_, marks) = a.btc_context().unwrap();
    let context = context::tests::activation(&a, 600, context::tests::mark_rows(marks, 600));
    a.activate_context(&context).unwrap();
    a.effect_cancel(&EffectInput {
        stamp: source::stamp("release", 601, 50),
        effects: vec![("activate".into(), "O1".into(), Reason::MmrBreach)],
    })
    .unwrap();
    assert_eq!(a.intent_results.len(), 1);
    assert_eq!(a.execution_receipts.len(), 1);
    assert_eq!(a.cancel_facts.actions.len(), 4);
    assert_eq!(a.liquidation_ids().count(), 1);
    assert_eq!(a.transition.event_kinds.len(), 6);
    a
}
pub(in super::super) fn assert_inspection(a: &ScenarioAccount) {
    let before = a.clone();
    assert_eq!(a.inspect_state().unwrap(), a.inspect_state().unwrap());
    assert_eq!(a, &before);
}
#[test]
fn complete_literal_owner_vector() {
    let a = mixed_owner();
    let before = a.clone();
    let result = a.inspect_state().unwrap();
    assert_eq!(a, before);
    assert_eq!(result.basis.cash, a.cash);
    // Independent Python literal reconstruction: /private/tmp/spider-5a3b2b-oracle.py
    // Input: /private/tmp/spider-5a3b2b-literal.json; 8392 canonical bytes.
    // Prefix/segment ends: 1353, 1994, 3231, 7080, 7682, 8193, 8392.
    let expected = "e196f12a2114b554f3c831aafaca0c1e49b2f68bf7d0afe9ff244257c2e97dab";
    let hex = |hash: Hash| hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
    assert_eq!(hex(result.owner_state_digest), expected);
    for index in 0..6 {
        let mut order: Vec<_> = (0..6).collect();
        order.remove(index);
        assert_ne!(hex(stream(&a, &order, false)), expected);
        if index < 5 {
            let mut order: Vec<_> = (0..6).collect();
            order.swap(index, index + 1);
            assert_ne!(hex(stream(&a, &order, false)), expected);
        }
    }
    assert_ne!(hex(stream(&a, &[0, 1, 2, 3, 4, 5], true)), expected);
    assert_eq!(a, before);
    let mut detached = result.clone();
    detached.basis.cash = Decimal::ZERO;
    detached.owner_state_digest[0] ^= 1;
    assert_eq!(a.inspect_state().unwrap(), result);
    let mut excluded = a.clone();
    excluded.transition.accepted_stamp = None;
    excluded.transition.events.clear();
    excluded.transition.contexts.clear();
    excluded.transition.groups.clear();
    excluded.transition.admissions.clear();
    excluded.transition.batches.clear();
    excluded.transition.completed_episodes.clear();
    excluded.seed_intents.clear();
    excluded.seed_orders.clear();
    excluded.seed_executions.clear();
    let before = excluded.clone();
    assert_eq!(excluded.inspect_state().unwrap(), result);
    assert_eq!(excluded, before);
}
fn stream(a: &ScenarioAccount, order: &[usize], component_hashes: bool) -> Hash {
    let (basis, mut e) = a.current_evidence().unwrap();
    if component_hashes {
        e = Encoding::new("SCENARIO_OWNER_EVIDENCE_V1");
        e.account(&basis.account);
        e.text(basis.profile);
        e.text(&basis.config);
        e.integer(basis.version);
        e.hash(basis.context);
        e.text("RUNNING");
        e.optional_text(None);
        e.text("LIQUIDATED_INSOLVENT");
        decimals(&mut e, &[basis.cash, basis.gross, basis.fees]);
        for hash in [basis.positions, basis.orders, basis.reservations] {
            e.hash(hash);
        }
    }
    let segments: [fn(&ScenarioAccount, &mut Encoding) -> Result<(), Fault>; 6] = [
        ScenarioAccount::encode_inspection_intents,
        ScenarioAccount::encode_inspection_executions,
        |a, e| a.cancel_facts.encode_inspection_receipts(e),
        |a, e| a.cancel_facts.encode_inspection_actions(e),
        ScenarioAccount::encode_inspection_liquidations,
        ScenarioAccount::encode_inspection_sources,
    ];
    for index in order {
        segments[*index](a, &mut e).unwrap();
    }
    e.finish()
}
#[test]
fn source_tokens_ascending_literal_and_event_c_failure() {
    let mut a = mixed_owner();
    let rows = [
        ("a", source::Kind::Context, "CONTEXT"),
        ("b", source::Kind::Execution, "EXECUTION"),
        ("c", source::Kind::Intent, "INTENT"),
        ("d", source::Kind::CancelRequest, "CANCEL_REQUEST"),
        ("e", source::Kind::CancelEffect, "CANCEL_EFFECT"),
    ];
    a.transition.event_kinds = rows
        .iter()
        .rev()
        .map(|(id, kind, _)| (id.to_string(), *kind))
        .collect();
    let before = a.clone();
    let mut raw = receipt_vectors::Raw(Vec::new());
    raw.text("TEST_SOURCES");
    raw.num(5);
    for (id, _, kind) in rows {
        raw.text(id);
        raw.text(kind);
    }
    let mut e = Encoding::new("TEST_SOURCES");
    a.encode_inspection_sources(&mut e).unwrap();
    assert_eq!(e.finish(), raw.finish());
    assert_eq!(a, before);
    a.transition
        .event_kinds
        .insert("z".into(), source::Kind::EventC);
    let before = a.clone();
    assert_eq!(a.inspect_state(), Err("NATIVE_INVARIANT"));
    assert_eq!(a, before);
}
