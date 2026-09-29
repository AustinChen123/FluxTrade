use super::super::super::tests::d;
use super::*;
use inspection::receipt_vectors::Raw;
fn digest(a: &ScenarioAccount) -> Result<Hash, Fault> {
    let before = a.clone();
    let mut e = Encoding::new("TEST_LIQUIDATIONS");
    let result = a.encode_inspection_liquidations(&mut e).map(|_| e.finish());
    assert_eq!(a, &before);
    result
}
#[test]
fn two_step_oracle_fields_commit_order_faults_and_exclusions() {
    let mut a = tests::source_draft(
        &tests::anchor(Side::Long, "1001", "50000", "3000", 500, "49900"),
        501,
    );
    a.liquidation_loop(|_| Ok(())).unwrap();
    assert_eq!(a.transition.liquidations.len(), 2);
    let expected = digest(&a).unwrap();
    let mut raw = Raw(Vec::new());
    raw.text("TEST_LIQUIDATIONS");
    raw.num(2);
    for (i, r) in a.transition.liquidations.iter().enumerate() {
        for s in ["okx-scenario", "test", "A"] {
            raw.text(s);
        }
        raw.0.push(0);
        raw.0.extend(r.liquidation_id);
        raw.0.extend(r.canonical_payload_digest);
        raw.text(&r.trigger_event_id);
        raw.num(1 + i as i64);
        for s in [
            "BTC-USDT-SWAP",
            "LONG",
            "SHORT",
            if i == 0 { "1" } else { "1000" },
            if i == 0 { "0.01" } else { "10" },
            "49900",
            "SyntheticLiquidation",
            "0.00602",
        ] {
            raw.text(s);
        }
        raw.decimals(&[r.fee, r.gross_realized_delta, r.cash_delta]);
        for n in [
            r.account_version_before,
            r.account_version_after,
            r.commit_sequence_before,
            r.commit_sequence,
        ] {
            raw.num(n as i64);
        }
        raw.text(&r.spec_version);
        raw.text(&r.rule_data_version);
        raw.0.extend(r.risk_action_episode_id);
        raw.optional(r.escalation_event_id.as_deref());
        raw.optional(r.release_event_id.as_deref());
        raw.0.extend(r.active_context_id);
        raw.text(if i == 0 {
            "CONTINUE_LIQUIDATION"
        } else {
            "LIQUIDATED_INSOLVENT"
        });
        raw.optional(if i == 0 {
            None
        } else {
            Some("LIQUIDATED_INSOLVENT")
        });
    }
    assert_eq!(expected, raw.finish());
    for change in [
        |r: &mut Receipt| r.account_key.account.push('x'),
        |r: &mut Receipt| r.liquidation_id[0] ^= 1,
        |r: &mut Receipt| r.canonical_payload_digest[0] ^= 1,
        |r: &mut Receipt| r.trigger_event_id.push('x'),
        |r: &mut Receipt| r.step_index += 1,
        |r: &mut Receipt| r.product = Product::Eth,
        |r: &mut Receipt| r.position_side_before = Side::Short,
        |r: &mut Receipt| r.execution_side = Side::Long,
        |r: &mut Receipt| r.contracts += Decimal::ONE,
        |r: &mut Receipt| r.base_quantity += Decimal::ONE,
        |r: &mut Receipt| r.mark += Decimal::ONE,
        |r: &mut Receipt| r.fee_rate += Decimal::ONE,
        |r: &mut Receipt| r.fee += Decimal::ONE,
        |r: &mut Receipt| r.gross_realized_delta += Decimal::ONE,
        |r: &mut Receipt| r.cash_delta += Decimal::ONE,
        |r: &mut Receipt| r.account_version_before += 1,
        |r: &mut Receipt| r.account_version_after += 1,
        |r: &mut Receipt| r.commit_sequence_before += 1,
        |r: &mut Receipt| r.commit_sequence += 1,
        |r: &mut Receipt| r.spec_version.push('x'),
        |r: &mut Receipt| r.rule_data_version.push('x'),
        |r: &mut Receipt| r.risk_action_episode_id[0] ^= 1,
        |r: &mut Receipt| r.escalation_event_id = Some("escalation".into()),
        |r: &mut Receipt| r.release_event_id = Some("release".into()),
        |r: &mut Receipt| r.active_context_id[0] ^= 1,
        |r: &mut Receipt| r.post_step_decision = StepDecision::RiskStable,
        |r: &mut Receipt| r.resulting_lifecycle = Some(Lifecycle::LiquidatedFlat),
    ] {
        let mut b = a.clone();
        change(&mut b.transition.liquidations[0]);
        assert_ne!(digest(&b).unwrap(), expected);
    }
    let mut b = a.clone();
    b.transition.liquidations.reverse();
    assert_ne!(digest(&b).unwrap(), expected);
    for policy in [
        FeePolicy::BtcEthTradingTaker,
        FeePolicy::GoldenCancelTradingTaker,
    ] {
        let mut b = a.clone();
        b.transition.liquidations[0].fee_policy = policy;
        assert_eq!(digest(&b), Err("NATIVE_INVARIANT"));
    }
    let mut b = a.clone();
    b.transition.liquidations[0].commit_sequence = u64::MAX;
    assert_eq!(digest(&b), Err("NATIVE_INVARIANT"));
    let mut b = a.clone();
    let r = &mut b.transition.liquidations[0];
    r.position_before.entry_basis = d("7");
    r.position_after = None;
    r.valuation_before.equity = d("8");
    r.valuation_after.equity = d("9");
    r.reservation_before.equity = d("10");
    r.reservation_after.equity = d("11");
    assert_eq!(digest(&b).unwrap(), expected);
}

#[test]
fn configured_fee_policy_has_distinct_discriminator_at_same_rate() {
    let mut owner = tests::source_draft(
        &tests::anchor(Side::Long, "1001", "50000", "3000", 500, "49900"),
        501,
    );
    owner.liquidation_loop(|_| Ok(())).unwrap();
    let original = owner.transition.liquidations[0].clone();
    assert_eq!(original.fee_rate, Decimal::new(602, 5));
    let original_digest = original.digest().unwrap();
    let original_inspection = digest(&owner).unwrap();

    let mut configured = original;
    configured.fee_policy = FeePolicy::ConfiguredLiquidation(Decimal::new(602, 5));
    assert_eq!(configured.fee_rate, Decimal::new(602, 5));
    assert_ne!(configured.digest().unwrap(), original_digest);
    owner.transition.liquidations[0] = configured;
    assert_ne!(digest(&owner).unwrap(), original_inspection);
}
