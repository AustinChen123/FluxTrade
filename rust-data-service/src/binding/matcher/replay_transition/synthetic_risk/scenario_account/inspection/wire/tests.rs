use super::*;

#[test]
fn detached_inspection_three_profiles_and_checked_boundary() {
    let (seed, _, _) = super::super::super::tests::fixture();
    for profile in [
        "SYNTHETIC_BTC_ETH_V1",
        "SYNTHETIC_GOLDEN_CANCEL_V1",
        "SYNTHETIC_MIN_CASH_V1",
    ] {
        let mut owner =
            super::super::super::wire::profiles::construct(profile, seed.key.clone(), None)
                .unwrap();
        for gate in [Gate::Running, Gate::Failed("EXACT_REASON")] {
            owner.gate = gate.clone();
            let before = owner.clone();
            let inspection = owner.inspect_state().unwrap();
            let mut output = inspection.wire_json().unwrap();
            let hex = |h: Hash| h.iter().map(|b| format!("{b:02x}")).collect::<String>();
            let b = &inspection.basis;
            let mut expected = serde_json::json!({"schema_version":"inspect_state_v1",
                "account_key":{"venue":"okx-scenario","environment":"test","account":"A"},
                "profile_id":profile,"config_id":"scenario-v1","account_version":0,
                "valuation_context_id":hex(b.context),"gate":if gate == Gate::Running {"RUNNING"} else {"FAILED"},
                "lifecycle":"RISK_STABLE","cash":if profile == "SYNTHETIC_BTC_ETH_V1" {"10000"} else {"1000"},
                "gross_realized":"0","total_fees":"0","positions_digest":hex(b.positions),
                "orders_digest":hex(b.orders),"reservations_digest":hex(b.reservations),
                "owner_state_digest":hex(inspection.owner_state_digest)});
            if gate != Gate::Running {
                expected["gate_failure"] = serde_json::json!("EXACT_REASON");
            }
            assert_eq!(output.canonical().unwrap(), expected.to_string());
            assert_eq!(
                output.canonical(),
                inspection.wire_json().unwrap().canonical()
            );
            let Json::Object(ref mut rows) = output else {
                panic!("object")
            };
            assert_eq!(rows["profile_id"], string(profile));
            assert_eq!(rows["account_version"], number(0).unwrap());
            assert_eq!(rows["cash"], decimal(owner.cash));
            assert_eq!(
                rows["owner_state_digest"],
                hash(owner.owner_evidence_digest().unwrap())
            );
            assert_eq!(
                rows.get("gate_failure").cloned(),
                if gate == Gate::Running {
                    None
                } else {
                    Some(string("EXACT_REASON"))
                }
            );
            rows.clear();
            assert_ne!(output, inspection.wire_json().unwrap());
            assert_eq!(owner, before);
        }
        owner.state_version = u64::MAX;
        assert_eq!(owner.inspect_state(), Err("NATIVE_INVARIANT"));
    }
    assert_eq!(number(u64::MAX), Err("NATIVE_INVARIANT"));
}
