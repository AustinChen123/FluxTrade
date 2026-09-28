use super::super::tests::d;
use super::*;
use inspection::receipt_vectors::Raw;
use risk_transition::{cancel::identity::Encoding, Lifecycle};
fn digest(a: &ScenarioAccount) -> Result<Hash, Fault> {
    let before = a.clone();
    let mut e = Encoding::new("TEST_EXECUTIONS");
    let result = a.encode_inspection_executions(&mut e).map(|_| e.finish());
    assert_eq!(a, &before);
    result
}
fn oracle(owner: &ScenarioAccount, keys: &[Hash], golden: bool, client: &str) -> Hash {
    let mut raw = Raw(Vec::new());
    raw.text("TEST_EXECUTIONS");
    raw.num(keys.len() as i64);
    for key in keys {
        let r = &owner.execution_receipts[key];
        raw.0.extend(*key);
        for s in ["okx-scenario", "test", "A"] {
            raw.text(s);
        }
        raw.0.push(0);
        raw.0.extend(r.execution_id);
        raw.0.extend(r.financial_payload_digest);
        raw.text(if golden { "event-G" } else { "event-1" });
        for n in [
            r.commit_sequence,
            u64::from(golden),
            1 + u64::from(golden),
            u64::from(golden),
            1 + u64::from(golden),
        ] {
            raw.num(n as i64);
        }
        for s in [
            if golden { "P_A" } else { "BTC-USDT-SWAP" },
            &r.order_id,
            if golden { "4" } else { "0.5" },
            if golden { "10" } else { "50000.1" },
            "USDT",
        ] {
            raw.text(s);
        }
        raw.decimals(&[r.fee_amount, r.realized_pnl_delta]);
        raw.num(r.cash_deltas.len() as i64);
        for (asset, value) in &r.cash_deltas {
            raw.text(asset);
            raw.decimals(&[*value]);
        }
        raw.order(&r.order_after);
        for n in [500, if golden { 501 } else { 500 }] {
            raw.num(n);
        }
        for s in [
            if golden { "1" } else { "0.01" },
            &r.spec_version,
            &r.rule_data_version,
            "RISK_STABLE",
        ] {
            raw.text(s);
        }
        raw.num(0);
        for s in [
            "EXECUTION_FACT",
            &r.order_id,
            if golden { "C" } else { "C1" },
            client,
            if golden { "P_A" } else { "BTC-USDT-SWAP" },
            "partially_filled",
            if golden { "buy" } else { "sell" },
            if golden { "10" } else { "50000.1" },
            if golden { "10" } else { "50000.1" },
            if golden { "10" } else { "2" },
            if golden { "4" } else { "1.5" },
            if golden { "1" } else { "0.01" },
        ] {
            raw.text(s);
        }
        raw.num(if golden { 501 } else { 500 });
        raw.num(if golden { 2 } else { 1 });
        raw.text(&r.spec_version);
        raw.text(&r.rule_data_version);
    }
    raw.finish()
}
#[test]
fn execution_vector_fields_order_exclusions_and_overflow() {
    let (mut owner, candidate) = tests::fixture_candidate();
    owner.execute(&candidate).unwrap();
    let key = *owner.execution_receipts.keys().next().unwrap();
    let receipt = owner.execution_receipts[&key].clone();
    owner.execution_receipts.insert([255; 32], receipt);
    let expected = digest(&owner).unwrap();
    assert_eq!(expected, oracle(&owner, &[key, [255; 32]], false, "C1"));
    assert_ne!(expected, oracle(&owner, &[[255; 32], key], false, "C1"));
    for change in [
        |r: &mut CommittedExecution| r.account_key.account.push('x'),
        |r: &mut CommittedExecution| r.execution_id[0] ^= 1,
        |r: &mut CommittedExecution| r.financial_payload_digest[0] ^= 1,
        |r: &mut CommittedExecution| r.event_id.push('x'),
        |r: &mut CommittedExecution| r.commit_sequence += 1,
        |r: &mut CommittedExecution| r.state_version_before += 1,
        |r: &mut CommittedExecution| r.state_version_after += 1,
        |r: &mut CommittedExecution| r.order_version_before += 1,
        |r: &mut CommittedExecution| r.order_version_after += 1,
        |r: &mut CommittedExecution| r.product = ProfileProduct::Pa,
        |r: &mut CommittedExecution| r.order_id.push('x'),
        |r: &mut CommittedExecution| r.quantity += Decimal::ONE,
        |r: &mut CommittedExecution| r.price += Decimal::ONE,
        |r: &mut CommittedExecution| r.fee_asset.push('x'),
        |r: &mut CommittedExecution| r.fee_amount += Decimal::ONE,
        |r: &mut CommittedExecution| r.realized_pnl_delta += Decimal::ONE,
        |r: &mut CommittedExecution| r.cash_deltas.push(("X".into(), d("1"))),
        |r: &mut CommittedExecution| r.order_after.client_id.push('x'),
        |r: &mut CommittedExecution| r.order_created_at += 1,
        |r: &mut CommittedExecution| r.execution_effective_at += 1,
        |r: &mut CommittedExecution| r.contract_value += Decimal::ONE,
        |r: &mut CommittedExecution| r.spec_version.push('x'),
        |r: &mut CommittedExecution| r.rule_data_version.push('x'),
        |r: &mut CommittedExecution| r.lifecycle_after = Lifecycle::LiquidatedFlat,
        |r: &mut CommittedExecution| r.pending_action_ids.push("action".into()),
    ] {
        let mut altered = owner.clone();
        change(altered.execution_receipts.values_mut().next().unwrap());
        assert_ne!(digest(&altered).unwrap(), expected);
    }
    let mut altered = owner.clone();
    let r = altered.execution_receipts.values_mut().next().unwrap();
    r.position_before = None;
    r.position_after = None;
    r.reservation_before = FinancialSnapshot::GoldenCancel(d("7"));
    r.reservation_after = FinancialSnapshot::GoldenCancel(d("8"));
    r.risk_decision_after = None;
    r.episode_after = None;
    assert_eq!(digest(&altered).unwrap(), expected);
    let receipt = altered.execution_receipts.values_mut().next().unwrap();
    receipt.commit_sequence = u64::MAX;
    assert_eq!(digest(&altered), Err("NATIVE_INVARIANT"));
}
#[test]
fn stored_vectors_hash_order_and_closed_golden_alias() {
    let (mut owner, candidate) = tests::fixture_candidate();
    owner.execute(&candidate).unwrap();
    let r = owner.execution_receipts.values_mut().next().unwrap();
    r.cash_deltas = vec![("A".into(), d("1")), ("B".into(), d("2"))];
    r.pending_action_ids = vec!["a".into(), "b".into()];
    let original = digest(&owner);
    owner
        .execution_receipts
        .values_mut()
        .next()
        .unwrap()
        .cash_deltas
        .reverse();
    assert_ne!(digest(&owner), original);
    owner
        .execution_receipts
        .values_mut()
        .next()
        .unwrap()
        .cash_deltas
        .reverse();
    owner
        .execution_receipts
        .values_mut()
        .next()
        .unwrap()
        .pending_action_ids
        .reverse();
    assert_ne!(digest(&owner), original);
    let receipt = owner.execution_receipts.values().next().unwrap().clone();
    owner.execution_receipts.insert([255; 32], receipt.clone());
    let original = digest(&owner);
    owner.execution_receipts.remove(&[255; 32]);
    owner.execution_receipts.insert([0; 32], receipt);
    assert_ne!(digest(&owner), original);
    let (mut seed, _, _) = super::super::tests::fixture();
    seed.positions.clear();
    seed.orders.clear();
    seed.cash = d("1000");
    let mut golden = ScenarioAccount::from_golden_cancel_seed(
        &seed,
        &super::super::golden_cancel::Config::frozen(),
    )
    .unwrap();
    admission::inspection_tests::golden_admit(&mut golden);
    let id = golden.orders.keys().next().unwrap().clone();
    let fill = commit::tests::input(&golden, &id, "G", "4", "10");
    golden.execute(&fill).unwrap();
    let key = *golden.execution_receipts.keys().next().unwrap();
    let expected = digest(&golden).unwrap();
    assert_eq!(expected, oracle(&golden, &[key], true, "0000015000"));
    assert_ne!(expected, oracle(&golden, &[key], true, "C"));
    let r = golden.execution_receipts.values_mut().next().unwrap();
    assert_eq!(r.policy_client(true).unwrap(), "0000015000");
    r.order_after.client_id = "not-C".into();
    assert_eq!(digest(&golden), Err("NATIVE_INVARIANT"));
}
