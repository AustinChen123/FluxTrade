use super::super::tests::{d, fixture};
use super::*;
use risk_transition::cancel::identity::Encoding;
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
        change(altered.intent_results.values_mut().next().unwrap());
        assert_ne!(digest(&altered).unwrap(), expected);
    }
    let mut altered = owner.clone();
    let r = altered.intent_results.values_mut().next().unwrap();
    r.evaluation = Evaluation::MinCash(d("123"));
    r.reservation_after = None;
    assert_eq!(digest(&altered).unwrap(), expected);
    let result = altered.intent_results.values_mut().next().unwrap();
    result.order_version_after = Some(u64::MAX);
    assert_eq!(digest(&altered), Err("NATIVE_INVARIANT"));
    let r = owner.intent_results.remove("z").unwrap();
    owner.intent_results.insert("a".into(), r);
    assert_ne!(digest(&owner).unwrap(), expected);
}
