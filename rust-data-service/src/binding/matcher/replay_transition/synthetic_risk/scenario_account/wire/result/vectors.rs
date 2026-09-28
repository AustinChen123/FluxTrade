use super::*;

fn value() -> GroupResult {
    GroupResult {
        classification: "FAULT",
        group_id: "G".into(),
        group_digest: Some([1; 32]),
        references: vec![
            Reference::Source("A".into()),
            Reference::Liquidation([2; 32]),
        ],
        rejections: vec![("B".into(), "MIN_CASH"), ("C".into(), "RUN_TERMINAL")],
        failure: Some("INJECTED"),
        before: 1,
        after: 3,
        gate: Gate::Failed("FAILED_REASON"),
        lifecycle: Lifecycle::RiskStable,
        owner_digest: [3; 32],
    }
}
// Independent byte construction: no production Encoding, JSON writer or digest helper.
fn raw_text(bytes: &mut Vec<u8>, s: &str) {
    bytes.extend((s.len() as i64).to_be_bytes());
    bytes.extend(s.as_bytes());
}
fn oracle(class: &str, present: bool) -> Hash {
    let mut b = Vec::new();
    for s in ["SCENARIO_GROUP_RESULT_V1", "group_result_v1", class, "G"] {
        raw_text(&mut b, s);
    }
    b.push(u8::from(present));
    if present {
        b.extend([1; 32]);
    }
    b.extend(2i64.to_be_bytes());
    for s in ["SOURCE", "A", "LIQUIDATION", &"02".repeat(32)] {
        raw_text(&mut b, s);
    }
    b.extend(2i64.to_be_bytes());
    for s in ["B", "MIN_CASH", "C", "RUN_TERMINAL"] {
        raw_text(&mut b, s);
    }
    b.push(u8::from(present));
    if present {
        raw_text(&mut b, "INJECTED");
    }
    b.extend(1i64.to_be_bytes());
    b.extend(3i64.to_be_bytes());
    raw_text(&mut b, if present { "FAILED" } else { "RUNNING" });
    b.push(u8::from(present));
    if present {
        raw_text(&mut b, "FAILED_REASON");
    }
    raw_text(&mut b, "RISK_STABLE");
    b.extend([3; 32]);
    ring::digest::digest(&ring::digest::SHA256, &b)
        .as_ref()
        .try_into()
        .unwrap()
}
#[test]
fn independent_nonempty_vectors_and_optional_json_fields() {
    for class in ["COMMITTED", "REJECTED", "FAULT"] {
        for present in [false, true] {
            let mut r = value();
            r.classification = class;
            if !present {
                r.group_digest = None;
                r.failure = None;
                r.gate = Gate::Running;
            }
            assert_eq!(r.encode().unwrap().finish(), oracle(class, present));
            let expected = format!(concat!(
                "{{\"account_version_after\":3,\"account_version_before\":1,\"classification\":\"{}\",",
                "\"committed_references\":[{{\"fact_id\":\"A\",\"namespace\":\"SOURCE\"}},",
                "{{\"fact_id\":\"{}\",\"namespace\":\"LIQUIDATION\"}}],{}\"gate_after\":\"{}\",{}{}",
                "\"group_id\":\"G\",\"lifecycle_after\":\"RISK_STABLE\",\"owner_state_digest\":\"{}\",",
                "\"rejections\":[{{\"event_id\":\"B\",\"reason\":\"MIN_CASH\"}},",
                "{{\"event_id\":\"C\",\"reason\":\"RUN_TERMINAL\"}}],\"result_digest\":\"{}\",\"schema_version\":\"group_result_v1\"}}"
            ), class, "02".repeat(32), if present {"\"failure\":\"INJECTED\","} else {""},
                if present {"FAILED"} else {"RUNNING"}, if present {"\"gate_failure\":\"FAILED_REASON\","} else {""},
                if present {format!("\"group_digest\":\"{}\",", "01".repeat(32))} else {String::new()},
                "03".repeat(32), oracle(class,present).iter().map(|b| format!("{b:02x}")).collect::<String>());
            assert_eq!(r.canonical().unwrap(), expected);
        }
    }
}
#[test]
fn each_result_field_and_list_order_affects_evidence() {
    let base = value();
    let changes: Vec<Box<dyn Fn(&mut GroupResult)>> = vec![
        Box::new(|r| r.classification = "REJECTED"),
        Box::new(|r| r.group_id.push('x')),
        Box::new(|r| r.group_digest = Some([4; 32])),
        Box::new(|r| r.references.swap(0, 1)),
        Box::new(|r| r.references.pop().map(|_| ()).unwrap()),
        Box::new(|r| r.references[0] = Reference::Source("Z".into())),
        Box::new(|r| r.references[1] = Reference::Liquidation([4; 32])),
        Box::new(|r| r.rejections.swap(0, 1)),
        Box::new(|r| r.rejections[0].0.push('x')),
        Box::new(|r| r.rejections[0].1 = "OTHER"),
        Box::new(|r| r.failure = Some("OTHER")),
        Box::new(|r| r.before += 1),
        Box::new(|r| r.after += 1),
        Box::new(|r| r.gate = Gate::Failed("OTHER")),
        Box::new(|r| r.lifecycle = Lifecycle::LiquidatedInsolvent),
        Box::new(|r| r.owner_digest[0] ^= 1),
    ];
    for change in changes {
        let mut changed = base.clone();
        change(&mut changed);
        assert_ne!(
            changed.encode().unwrap().finish(),
            base.encode().unwrap().finish()
        );
        assert_ne!(changed.canonical().unwrap(), base.canonical().unwrap());
    }
}
