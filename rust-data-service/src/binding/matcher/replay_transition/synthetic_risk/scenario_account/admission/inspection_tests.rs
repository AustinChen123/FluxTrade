use super::super::tests::{d, fixture};
use super::*;
use risk_transition::cancel::identity::Encoding;
use std::collections::BTreeMap;
pub(in super::super) fn reject_min_cash(owner: &mut ScenarioAccount) {
    let mut intent = fixture_intent(owner, "MIN-NEGATIVE-I", Side::Long, d("1"), d("50000"));
    intent.client_order_id = "MIN-NEGATIVE-C".into();
    intent.strategy_id = "min-cash-policy".into();
    intent.requested_at = 502;
    let before = owner.clone();
    let (_, result) = fixture_admit(owner, "negative", 502, &intent).unwrap();
    assert_eq!(
        (result.outcome, result.reason_code),
        (Outcome::Rejected, Some("MIN_CASH"))
    );
    let mut expected = before;
    expected.intent_results = owner.intent_results.clone();
    expected.transition = owner.transition.clone();
    assert_eq!(*owner, expected);
}
pub(in super::super) fn golden_admit(owner: &mut ScenarioAccount) {
    let mut intent = fixture_intent(owner, "I", Side::Long, d("10"), d("10"));
    intent.client_order_id = "C".into();
    fixture_admit(owner, "admit", 500, &intent).unwrap();
}
fn digest(a: &ScenarioAccount) -> Result<Hash, Fault> {
    let before = a.clone();
    let mut e = Encoding::new("TEST_INTENTS");
    let result = a.encode_inspection_intents(&mut e).map(|_| e.finish());
    assert_eq!(a, &before);
    result
}
#[test]
fn intent_fields_options_order_overflow_and_exclusions() {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    admit_execution_fixture(&mut owner, Side::Long, d("1"), d("50000"));
    let r = owner.intent_results.values().next().unwrap().clone();
    owner.intent_results.insert("z".into(), r);
    let expected = digest(&owner).unwrap();
    let mut raw = inspection::receipt_vectors::Raw(Vec::new());
    raw.text("TEST_INTENTS");
    raw.num(2);
    let header = raw.0.len();
    let mut ends = Vec::new();
    for key in ["execution-fixture", "z"] {
        let r = &owner.intent_results[key];
        raw.text(key);
        raw.text(&r.intent_id);
        raw.0.extend(r.canonical_payload_digest);
        raw.text("ACCEPTED");
        raw.optional(None);
        raw.optional(r.order_id.as_deref());
        raw.num(0);
        raw.num(1);
        raw.0.extend([0, 1]);
        raw.num(1);
        for s in [
            &r.spec_version,
            &r.rule_data_version,
            &r.created_at_event_id,
        ] {
            raw.text(s);
        }
        raw.0.push(1);
        raw.order(r.accepted_order.as_ref().unwrap());
        ends.push(raw.0.len());
    }
    let reversed = [&raw.0[..header], &raw.0[ends[0]..], &raw.0[header..ends[0]]].concat();
    assert_ne!(
        expected,
        inspection::receipt_vectors::Raw(reversed).finish()
    );
    assert_eq!(expected, raw.finish());
    for change in [
        |r: &mut AdmissionResult| r.intent_id.push('x'),
        |r: &mut AdmissionResult| r.canonical_payload_digest[0] ^= 1,
        |r: &mut AdmissionResult| r.outcome = Outcome::Rejected,
        |r: &mut AdmissionResult| r.reason_code = Some("MIN_CASH"),
        |r: &mut AdmissionResult| r.order_id = None,
        |r: &mut AdmissionResult| r.account_version_before += 1,
        |r: &mut AdmissionResult| r.account_version_after += 1,
        |r: &mut AdmissionResult| r.order_version_before = Some(7),
        |r: &mut AdmissionResult| r.order_version_after = None,
        |r: &mut AdmissionResult| r.spec_version.push('x'),
        |r: &mut AdmissionResult| r.rule_data_version.push('x'),
        |r: &mut AdmissionResult| r.created_at_event_id.push('x'),
        |r: &mut AdmissionResult| r.accepted_order = None,
    ] {
        let mut altered = owner.clone();
        alter_first_result(&mut altered, change);
        assert_ne!(digest(&altered).unwrap(), expected);
    }
    let mut altered = owner.clone();
    alter_first_result(&mut altered, |result| {
        result.evaluation = Evaluation::MinCash(d("123"));
        result.reservation_after = None;
    });
    assert_eq!(digest(&altered).unwrap(), expected);
    alter_first_result(&mut altered, |result| {
        result.order_version_after = Some(u64::MAX)
    });
    assert_eq!(digest(&altered), Err("NATIVE_INVARIANT"));
    let r = owner.intent_results.get("z").unwrap().clone();
    let original = owner
        .intent_results
        .get("execution-fixture")
        .unwrap()
        .clone();
    owner.intent_results.clear();
    owner
        .intent_results
        .insert("execution-fixture".into(), original);
    owner.intent_results.insert("a".into(), r);
    assert_ne!(digest(&owner).unwrap(), expected);
}

fn alter_first_result(owner: &mut ScenarioAccount, change: impl FnOnce(&mut AdmissionResult)) {
    let (key, mut result) = owner
        .intent_results
        .iter()
        .next()
        .map(|(key, result)| (key.clone(), result.clone()))
        .unwrap();
    change(&mut result);
    owner.intent_results.insert(key, result);
}

fn owned_rows(owner: &ScenarioAccount) -> BTreeMap<String, AdmissionResult> {
    owner
        .intent_results
        .iter()
        .map(|(key, result)| (key.clone(), result.clone()))
        .collect()
}

fn nonempty_owner() -> ScenarioAccount {
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    admit_execution_fixture(&mut owner, Side::Long, d("1"), d("50000"));
    owner
}

#[test]
fn intent_result_publication_duplicate_and_conflict_keep_owned_rows() {
    let mut owner = nonempty_owner();
    let mut intent = fixture_intent(&owner, "next-intent", Side::Long, d("1"), d("50000"));
    intent.client_order_id = "next-client".into();
    let before = owned_rows(&owner);
    let (_, published) = fixture_admit(&mut owner, "next-event", 501, &intent).unwrap();
    let mut expected = before.clone();
    expected.insert(published.intent_id.clone(), published.clone());
    assert_eq!(owned_rows(&owner), expected);

    let before_duplicate = owned_rows(&owner);
    assert_eq!(
        fixture_admit(&mut owner, "next-event", 501, &intent)
            .unwrap()
            .0,
        "duplicate"
    );
    assert_eq!(owned_rows(&owner), before_duplicate);
    let mut conflict = intent;
    conflict.client_order_id.push_str("-conflict");
    assert_eq!(
        fixture_admit(&mut owner, "next-event", 501, &conflict),
        Err("IDEMPOTENCY_KEY_CONFLICT")
    );
    assert_eq!(owned_rows(&owner), before_duplicate);
}

#[test]
fn rejected_admission_publishes_directly_to_nonempty_history() {
    let mut owner = nonempty_owner();
    let before = owned_rows(&owner);
    let intent = fixture_intent(&owner, "rejected-intent", Side::Long, d("1"), d("50000"));
    let envelope = Envelope {
        event_id: "negative",
        effective_at: 502,
        intent: &intent,
    };
    let stamp = source::stamp("negative", 502, 60);
    let result = owner
        .admit_stamped_with_restriction(&envelope, &stamp, true, |_| Ok(()))
        .unwrap()
        .result;
    assert_eq!(
        (result.outcome, result.reason_code),
        (Outcome::Rejected, Some("GROSS_RESPONSE_PENDING"))
    );
    let mut expected = before;
    expected.insert(result.intent_id.clone(), result);
    assert_eq!(owned_rows(&owner), expected);
}

#[test]
fn result_drafted_error_does_not_publish_intent_result() {
    let mut owner = nonempty_owner();
    let before = owned_rows(&owner);
    let intent = fixture_intent(&owner, "failed-intent", Side::Long, d("1"), d("50000"));
    let envelope = Envelope {
        event_id: "failed-event",
        effective_at: 501,
        intent: &intent,
    };
    assert_eq!(
        owner.admit_checked(&envelope, |stage| {
            if stage == PrepareStage::ResultDrafted {
                Err("INJECTED")
            } else {
                Ok(())
            }
        }),
        Err("INJECTED")
    );
    assert_eq!(owned_rows(&owner), before);
}

#[test]
fn every_pre_swap_admission_exit_preserves_nonempty_intent_rows() {
    for (index, stage) in [
        PrepareStage::Validated,
        PrepareStage::OrderDrafted,
        PrepareStage::ResultDrafted,
    ]
    .into_iter()
    .enumerate()
    {
        for panic in [false, true] {
            let mut owner = nonempty_owner();
            let before = owned_rows(&owner);
            let id = format!("failed-{index}-{panic}");
            let intent = fixture_intent(&owner, &id, Side::Long, d("1"), d("50000"));
            let envelope = Envelope {
                event_id: &id,
                effective_at: 501,
                intent: &intent,
            };
            let expected = if panic { "ADMISSION_PANIC" } else { "INJECTED" };
            assert_eq!(
                owner.admit_checked(&envelope, |current| {
                    if current == stage {
                        if panic {
                            panic!("injected hook panic");
                        }
                        return Err("INJECTED");
                    }
                    Ok(())
                }),
                Err(expected)
            );
            assert_eq!(owned_rows(&owner), before, "stage={stage:?} panic={panic}");
        }
    }
}

#[test]
fn nested_outer_rollback_and_later_failure_keep_committed_rows() {
    let mut owner = nonempty_owner();
    let committed = owned_rows(&owner);
    let mut outer = owner.clone();
    let outer_before = owned_rows(&outer);
    let intent = fixture_intent(&outer, "nested-failure", Side::Long, d("1"), d("50000"));
    let envelope = Envelope {
        event_id: "nested-event",
        effective_at: 501,
        intent: &intent,
    };
    assert_eq!(
        outer.admit_checked(&envelope, |stage| {
            if stage == PrepareStage::ResultDrafted {
                Err("NESTED_FAILURE")
            } else {
                Ok(())
            }
        }),
        Err("NESTED_FAILURE")
    );
    assert_eq!(owned_rows(&outer), outer_before);
    drop(outer);
    assert_eq!(owned_rows(&owner), committed);

    let later = fixture_intent(&owner, "later-failure", Side::Long, d("1"), d("50000"));
    let envelope = Envelope {
        event_id: "later-event",
        effective_at: 501,
        intent: &later,
    };
    assert_eq!(
        owner.admit_checked(&envelope, |stage| {
            if stage == PrepareStage::ResultDrafted {
                Err("LATER_FAILURE")
            } else {
                Ok(())
            }
        }),
        Err("LATER_FAILURE")
    );
    assert_eq!(owned_rows(&owner), committed);
}

#[test]
fn real_apply_group_publishes_intent_against_owned_reference() {
    let mut owner = nonempty_owner();
    let before = owned_rows(&owner);
    let intent = fixture_intent(&owner, "group-intent", Side::Long, d("1"), d("50000"));
    let stamp = source::stamp("group-event", 501, 60);
    let group = group::Group {
        group_id: "group-id".into(),
        account_key: owner.key.clone(),
        ordering_contract_id: stamp.ordering_contract_id.clone(),
        group_effective_at: stamp.effective_at,
        declared_member_count: 1,
        members: vec![group::Member {
            stamp,
            input: group::Input::Intent(intent.clone()),
        }],
    };
    owner.apply_group(&group).unwrap();
    let mut expected = before;
    expected.insert(
        "group-intent".into(),
        owner.intent_results["group-intent"].clone(),
    );
    assert_eq!(owned_rows(&owner), expected);
}
