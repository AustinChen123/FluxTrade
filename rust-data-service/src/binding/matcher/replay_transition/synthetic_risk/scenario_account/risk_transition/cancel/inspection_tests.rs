use super::super::super::tests::{d, fixture};
use super::*;
use inspection::receipt_vectors::Raw;
fn owner() -> ScenarioAccount {
    let (seed, config, marks) = fixture();
    let mut a = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    a.request_cancel(&RequestInput {
        stamp: source::stamp("R", 501, 40),
        targets: vec![("O1".into(), Reason::ExplicitScenario)],
    })
    .unwrap();
    a.effect_cancel(&EffectInput {
        stamp: source::stamp("E", 502, 50),
        effects: vec![("R".into(), "O1".into(), Reason::ExplicitScenario)],
    })
    .unwrap();
    for map in [&mut a.cancel_facts.requests, &mut a.cancel_facts.effects] {
        let r = map.values().next().unwrap().clone();
        *map = BTreeMap::from([([0; 32], r.clone()), ([255; 32], r)]);
    }
    a
}
fn digest(a: &ScenarioAccount, actions: bool) -> Result<Hash, Fault> {
    let before = a.clone();
    let mut e = identity::Encoding::new("TEST_CANCEL");
    let result = if actions {
        a.cancel_facts.encode_inspection_actions(&mut e)
    } else {
        a.cancel_facts.encode_inspection_receipts(&mut e)
    };
    assert_eq!(a, &before);
    result.map(|_| e.finish())
}
fn oracle(a: &ScenarioAccount, keys: &[Hash]) -> Hash {
    let mut raw = Raw(Vec::new());
    raw.text("TEST_CANCEL");
    for (map, outcome) in [
        (&a.cancel_facts.requests, "REQUESTED"),
        (&a.cancel_facts.effects, "EFFECTIVE_CANCELED"),
    ] {
        raw.num(2);
        for key in keys {
            let s = &map[key];
            let r = &s.value;
            raw.0.extend(*key);
            raw.0.extend(s.digest);
            for s in ["okx-scenario", "test", "A"] {
                raw.text(s);
            }
            raw.0.push(0);
            raw.0.extend(r.action_id);
            raw.0.extend(r.payload_digest);
            for s in [&r.event_id, "R", "O1", "EXPLICIT_SCENARIO"] {
                raw.text(s);
            }
            raw.num(r.phase);
            raw.text(outcome);
            raw.num(r.effective_at);
            for n in [
                r.account_version_before,
                r.account_version_after,
                r.order_version_before,
                r.order_version_after,
            ] {
                raw.num(n as i64);
            }
            raw.order(&r.before);
            raw.order(&r.after);
            raw.text(
                if r.lifecycle_after == super::super::Lifecycle::RiskStable {
                    "RISK_STABLE"
                } else {
                    "AWAITING_CANCEL_EFFECTIVE"
                },
            );
            raw.text(&r.spec_version);
            raw.text(&r.rule_data_version);
            raw.num(r.action_ids.len() as i64);
            for id in &r.action_ids {
                raw.0.extend(*id);
            }
        }
    }
    raw.finish()
}
#[test]
fn receipt_vectors_fields_exclusions_and_order() {
    let a = owner();
    let expected = digest(&a, false).unwrap();
    assert_eq!(expected, oracle(&a, &[[0; 32], [255; 32]]));
    assert_ne!(expected, oracle(&a, &[[255; 32], [0; 32]]));
    for change in [
        |r: &mut Receipt| r.account_key.account.push('x'),
        |r: &mut Receipt| r.action_id[0] ^= 1,
        |r: &mut Receipt| r.payload_digest[0] ^= 1,
        |r: &mut Receipt| r.event_id.push('x'),
        |r: &mut Receipt| r.detecting_event_id.push('x'),
        |r: &mut Receipt| r.target_order_id.push('x'),
        |r: &mut Receipt| r.reason = Reason::MmrBreach,
        |r: &mut Receipt| r.phase += 1,
        |r: &mut Receipt| r.outcome = Outcome::EffectiveTooLate,
        |r: &mut Receipt| r.effective_at += 1,
        |r: &mut Receipt| r.account_version_before += 1,
        |r: &mut Receipt| r.account_version_after += 1,
        |r: &mut Receipt| r.order_version_before += 1,
        |r: &mut Receipt| r.order_version_after += 1,
        |r: &mut Receipt| r.before.client_id.push('x'),
        |r: &mut Receipt| r.after.client_id.push('x'),
        |r: &mut Receipt| r.lifecycle_after = super::super::Lifecycle::LiquidatedFlat,
        |r: &mut Receipt| r.spec_version.push('x'),
        |r: &mut Receipt| r.rule_data_version.push('x'),
        |r: &mut Receipt| r.action_ids.push([9; 32]),
    ] {
        for effect in [false, true] {
            let mut b = a.clone();
            let map = if effect {
                &mut b.cancel_facts.effects
            } else {
                &mut b.cancel_facts.requests
            };
            change(&mut map.get_mut(&[0; 32]).unwrap().value);
            assert_ne!(digest(&b, false).unwrap(), expected);
        }
    }
    let mut b = a.clone();
    for map in [&mut b.cancel_facts.requests, &mut b.cancel_facts.effects] {
        let r = &mut map.get_mut(&[0; 32]).unwrap().value;
        r.reservation_before = FinancialSnapshot::GoldenCancel(d("7"));
        r.reservation_after = FinancialSnapshot::GoldenCancel(d("8"));
        r.risk_after = None;
        r.episode_after = None;
    }
    b.cancel_facts.batches.clear();
    assert_eq!(digest(&b, false).unwrap(), expected);
    b.cancel_facts
        .requests
        .get_mut(&[0; 32])
        .unwrap()
        .value
        .action_ids
        .reverse();
    assert_ne!(digest(&b, false).unwrap(), expected);
    let r = &mut b.cancel_facts.requests.get_mut(&[0; 32]).unwrap().value;
    r.account_version_after = u64::MAX;
    assert_eq!(digest(&b, false), Err("NATIVE_INVARIANT"));
    for effect in [false, true] {
        let mut b = a.clone();
        let map = if effect {
            &mut b.cancel_facts.effects
        } else {
            &mut b.cancel_facts.requests
        };
        map.get_mut(&[0; 32]).unwrap().digest[0] ^= 1;
        assert_ne!(digest(&b, false).unwrap(), expected);
        let mut b = a.clone();
        let map = if effect {
            &mut b.cancel_facts.effects
        } else {
            &mut b.cancel_facts.requests
        };
        let r = map.remove(&[0; 32]).unwrap();
        map.insert([1; 32], r);
        assert_ne!(digest(&b, false).unwrap(), expected);
    }
}
#[test]
fn retained_actions_fields_optional_and_ascending_vector() {
    let mut a = owner();
    let mut actions: Vec<_> = a.cancel_facts.actions.values().cloned().collect();
    actions.sort_by_key(|r| r.phase);
    assert_eq!((actions[0].phase, actions[1].phase), (1, 2));
    assert_eq!(actions[0].outcome, Some(Outcome::EffectiveCanceled));
    assert_eq!(actions[1].outcome, None);
    a.cancel_facts.actions = BTreeMap::from([
        ([0; 32], actions[0].clone()),
        ([255; 32], actions[1].clone()),
    ]);
    let expected = digest(&a, true).unwrap();
    let oracle = |keys: [Hash; 2]| {
        let mut raw = Raw(Vec::new());
        raw.text("TEST_CANCEL");
        raw.num(2);
        for key in keys {
            raw.0.extend(key);
            raw.text("R");
            raw.text("O1");
            raw.num(if key == [0; 32] { 1 } else { 2 });
            raw.optional(if key == [0; 32] {
                Some("EFFECTIVE_CANCELED")
            } else {
                None
            });
        }
        raw.finish()
    };
    assert_eq!(expected, oracle([[0; 32], [255; 32]]));
    assert_ne!(expected, oracle([[255; 32], [0; 32]]));
    for change in [
        |r: &mut Action| r.detecting_event_id.push('x'),
        |r: &mut Action| r.target_order_id.push('x'),
        |r: &mut Action| r.phase += 1,
        |r: &mut Action| r.outcome = None,
        |r: &mut Action| r.outcome = Some(Outcome::Requested),
        |r: &mut Action| r.outcome = Some(Outcome::EffectiveTooLate),
    ] {
        let mut b = a.clone();
        change(b.cancel_facts.actions.get_mut(&[0; 32]).unwrap());
        assert_ne!(digest(&b, true).unwrap(), expected);
    }
    let action = a.cancel_facts.actions.remove(&[0; 32]).unwrap();
    a.cancel_facts.actions.insert([1; 32], action);
    assert_ne!(digest(&a, true).unwrap(), expected);
}
