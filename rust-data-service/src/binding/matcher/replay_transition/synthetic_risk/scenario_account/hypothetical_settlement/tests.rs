use super::super::tests::{d, fixture};
use super::*;

fn opening(sequence: u64) -> OpeningIdentity {
    OpeningIdentity {
        source_id: format!("X{sequence}"),
        strategy_id: "strategy".into(),
        execution_id: hash_fields(&[Some(sequence.to_string())]),
        sequence,
    }
}

#[test]
fn closed_fee_policies_have_exact_rates_and_amounts() {
    for (policy, rate, fee) in [
        (FeePolicy::BtcEthTradingTaker, "0.001", "0.04"),
        (FeePolicy::GoldenCancelTradingTaker, "0.01", "0.4"),
        (FeePolicy::SyntheticLiquidation, "0.00602", "0.2408"),
    ] {
        assert_eq!(policy.rate(), d(rate));
        assert_eq!(fee_amount(d("4"), d("10"), policy), Ok(d(fee)));
    }
}

#[test]
fn origin_spec_preserves_fractional_fifo_under_active_v2_for_both_fee_policies() {
    let (_, config, _) = fixture();
    let (v1, _) = config.resolve(Product::Btc, 1999).unwrap();
    let (v2, _) = config.resolve(Product::Btc, 2000).unwrap();
    for (side, opposing, pnl) in [
        (Side::Long, Side::Short, "0.0135"),
        (Side::Short, Side::Long, "-0.0135"),
    ] {
        for (policy, fee, cash) in [
            (
                FeePolicy::BtcEthTradingTaker,
                "0.750015",
                if side == Side::Long {
                    "-0.736515"
                } else {
                    "-0.763515"
                },
            ),
            (
                FeePolicy::SyntheticLiquidation,
                "4.5150903",
                if side == Side::Long {
                    "-4.5015903"
                } else {
                    "-4.5285903"
                },
            ),
        ] {
            let first = calculate(
                None,
                side,
                d("1"),
                d("50000.1"),
                (&config, v1),
                FeePolicy::BtcEthTradingTaker,
                Some(&opening(0)),
            )
            .unwrap();
            let opened = calculate(
                first.position.as_ref(),
                side,
                d("2"),
                d("50000.1"),
                (&config, v1),
                FeePolicy::BtcEthTradingTaker,
                Some(&opening(1)),
            )
            .unwrap();
            let before = opened.position.clone();
            let closed = calculate(
                opened.position.as_ref(),
                opposing,
                d("1.5"),
                d("50001"),
                (&config, v2),
                policy,
                None,
            )
            .unwrap();
            assert_eq!(
                (closed.gross_realized_delta, closed.fee, closed.cash_delta),
                (d(pnl), d(fee), d(cash))
            );
            let remaining = closed.position.unwrap();
            assert_eq!(
                (remaining.contracts, remaining.entry_basis),
                (d("1.5"), d("750.0015"))
            );
            assert_eq!(remaining.lots.len(), 1);
            let lot = &remaining.lots[0];
            assert_eq!(
                (
                    &lot.origin_spec_version,
                    lot.execution_id,
                    lot.source.seed_sequence
                ),
                (&"spec-v1".to_string(), opening(1).execution_id, 1)
            );
            assert_eq!(
                (lot.source.entry, lot.source.contracts, lot.base_quantity),
                (d("50000.1"), d("1.5"), d("0.015"))
            );
            assert_eq!(opened.position, before);
            assert_eq!(
                calculate(
                    opened.position.as_ref(),
                    opposing,
                    d("1"),
                    d("50000.1"),
                    (&config, v2),
                    policy,
                    None
                ),
                Err("INVALID_HYPOTHETICAL_EXECUTION")
            );
            let v2_open = calculate(
                Some(&remaining),
                side,
                d("1"),
                d("50001"),
                (&config, v2),
                policy,
                Some(&opening(2)),
            )
            .unwrap()
            .position
            .unwrap();
            assert_eq!(v2_open.lots[0].origin_spec_version, "spec-v1");
            assert_eq!(v2_open.lots[1].origin_spec_version, "spec-v2");
        }
    }
    assert_eq!(FeePolicy::BtcEthTradingTaker.rate(), d("0.001"));
    assert_eq!(FeePolicy::SyntheticLiquidation.rate(), d("0.00602"));
}

#[test]
fn missing_unknown_and_corrupt_origin_fail_closed_without_mutation() {
    let (mut seed, config, marks) = fixture();
    seed.positions[0].lots[0].entry = d("50000.1");
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let original = &owner.positions.btc().unwrap()[&Product::Btc];
    assert!(original
        .lots
        .iter()
        .all(|lot| lot.origin_spec_version == "spec-v1"));
    let (active, _) = config.resolve(Product::Btc, 2000).unwrap();
    for (version, error) in [
        ("", "INVALID_LOT_ORIGIN_SPEC"),
        ("unknown", "INVALID_LOT_ORIGIN_SPEC"),
        ("event-limit-neutral", "INVALID_LOT_ORIGIN_SPEC"),
        ("spec-v2", "INVALID_FIFO_POSITION"),
    ] {
        let mut corrupted = original.clone();
        corrupted.lots[0].origin_spec_version = version.into();
        let before = corrupted.clone();
        assert_eq!(
            calculate(
                Some(&corrupted),
                Side::Short,
                d("1"),
                d("50001"),
                (&config, active),
                FeePolicy::SyntheticLiquidation,
                None
            ),
            Err(error)
        );
        assert_eq!(corrupted, before);
    }
    for mutate in [
        |lot: &mut EntryLot| lot.source.entry = d("50000.01"),
        |lot: &mut EntryLot| lot.source.contracts = d("1.001"),
        |lot: &mut EntryLot| lot.base_quantity = d("0.1"),
    ] {
        let mut corrupted = original.clone();
        mutate(&mut corrupted.lots[0]);
        assert_eq!(
            calculate(
                Some(&corrupted),
                Side::Short,
                d("1"),
                d("50001"),
                (&config, active),
                FeePolicy::BtcEthTradingTaker,
                None
            ),
            Err("INVALID_FIFO_POSITION")
        );
    }
}

#[test]
fn long_and_short_fifo_golden_traces_are_pure_and_exact() {
    let spec = frozen_spec(Product::Btc, false);
    let (_, config, _) = fixture();
    for (side, rows, ending_cash) in [
        (
            Side::Long,
            [
                ("1", "50000", "0", "0.5", "9999.5"),
                ("2", "50000.1", "0", "1.000002", "9998.499998"),
                ("1", "50001", "0.01", "0.50001", "9998.009988"),
                ("2", "49999.9", "-0.004", "0.999998", "9997.00599"),
            ],
            "9997.00599",
        ),
        (
            Side::Short,
            [
                ("2", "50000", "0", "1", "9999"),
                ("1", "49999.5", "0", "0.499995", "9998.500005"),
                ("1.5", "49900", "1.5", "0.7485", "9999.251505"),
                ("1.5", "50100", "-1.505", "0.7515", "9996.995005"),
            ],
            "9996.995005",
        ),
    ] {
        let mut current = None;
        let mut cash = d("10000");
        for (index, (quantity, price, realized, fee, expected_cash)) in rows.into_iter().enumerate()
        {
            let before = current.clone();
            let direction = if index < 2 {
                side
            } else if side == Side::Long {
                Side::Short
            } else {
                Side::Long
            };
            let identity = opening(index as u64);
            let result = calculate(
                current.as_ref(),
                direction,
                d(quantity),
                d(price),
                (&config, &spec),
                FeePolicy::BtcEthTradingTaker,
                (index < 2).then_some(&identity),
            )
            .unwrap();
            assert_eq!(current, before);
            assert_eq!(
                (result.gross_realized_delta, result.fee),
                (d(realized), d(fee))
            );
            cash = add(cash, result.cash_delta).unwrap();
            assert_eq!(cash, d(expected_cash));
            if index == 2 && side == Side::Short {
                let lot = &result.position.as_ref().unwrap().lots[0];
                assert_eq!(
                    (lot.source.contracts, lot.base_quantity, lot.source.entry),
                    (d("0.5"), d("0.005"), d("50000"))
                );
                assert_eq!(
                    (lot.execution_id, lot.source.seed_sequence),
                    (opening(0).execution_id, 0)
                );
            }
            current = result.position;
        }
        assert_eq!(cash, d(ending_cash));
        assert!(current.is_none());
    }
}

#[test]
fn reducing_stress_uses_actual_fee_and_preserves_input() {
    let (mut seed, config, mut marks) = fixture();
    seed.positions[0].lots.truncate(1);
    seed.positions[0].contracts = d("1");
    seed.cash = d("40");
    seed.orders.clear();
    marks[0].price = d("50000");
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let before = owner.clone();
    let initial = owner.reservation().unwrap();
    assert_eq!(
        add(initial.equity, -initial.maintenance_margin),
        Ok(d("38"))
    );
    for (price, equity, excess, improves) in [
        ("50000", "39.75", "38.75", true),
        ("49800", "38.751", "37.751", false),
        ("42200", "0.789", "-0.211", false),
    ] {
        let draft = calculate(
            owner.positions.btc().unwrap().get(&Product::Btc),
            Side::Short,
            d("0.5"),
            d(price),
            (&config, &frozen_spec(Product::Btc, false)),
            FeePolicy::BtcEthTradingTaker,
            None,
        )
        .unwrap();
        let position = draft.position.unwrap();
        let mut projection = owner.projection().unwrap();
        projection.cash = add(projection.cash, draft.cash_delta).unwrap();
        projection.positions[0].contracts = position.contracts;
        projection.positions[0].lots[0].contracts = position.contracts;
        let value = config.evaluate(&projection, &marks).unwrap();
        assert_eq!(
            (
                value.equity,
                add(value.equity, -value.maintenance_margin).unwrap()
            ),
            (d(equity), d(excess))
        );
        assert_eq!(
            value.equity > value.maintenance_margin && d(excess) > d("38"),
            improves
        );
        assert_eq!(owner, before);
    }
}

#[test]
fn invalid_and_nonexact_hypotheticals_never_mutate_inputs() {
    let (seed, config, marks) = fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let position = &owner.positions.btc().unwrap()[&Product::Btc];
    let spec = frozen_spec(Product::Btc, false);
    let (_, config, _) = fixture();
    for (quantity, price) in [
        ("4", "50000"),
        ("0", "50000"),
        ("-1", "50000"),
        ("0.001", "50000"),
        ("1", "0"),
        ("1", "50000.01"),
        ("1", "-1"),
    ] {
        assert!(calculate(
            Some(position),
            Side::Short,
            d(quantity),
            d(price),
            (&config, &spec),
            FeePolicy::BtcEthTradingTaker,
            None
        )
        .is_err());
    }
    let mut reversed = position.clone();
    reversed.lots.reverse();
    assert!(calculate(
        Some(&reversed),
        Side::Short,
        d("1"),
        d("50000"),
        (&config, &spec),
        FeePolicy::BtcEthTradingTaker,
        None
    )
    .is_err());
    assert!(calculate(
        None,
        Side::Long,
        d("1"),
        d("50000"),
        (&config, &spec),
        FeePolicy::BtcEthTradingTaker,
        None
    )
    .is_err());
    assert!(calculate(
        Some(position),
        Side::Long,
        d("1"),
        d("50000"),
        (&config, &spec),
        FeePolicy::BtcEthTradingTaker,
        Some(&opening(0))
    )
    .is_err());
    assert!(calculate(
        None,
        Side::Long,
        d("200"),
        Decimal::MAX,
        (&config, &spec),
        FeePolicy::BtcEthTradingTaker,
        Some(&opening(0))
    )
    .is_err());
    assert_eq!(
        fee_amount(
            d("0.0000000000000000000000000001"),
            d("1"),
            FeePolicy::BtcEthTradingTaker
        ),
        Err("DECIMAL_PRECISION_LOSS")
    );
    assert_eq!(
        owner,
        ScenarioAccount::from_seed(&seed, &config, &marks).unwrap()
    );
}

#[test]
fn strict_stress_threshold_equalities_remain_visible_to_future_admission() {
    for (cash, mark, price, equity, mmr, prior_excess) in [
        ("40", "49950", "49800", "38.501", "0.999", "37.502"),
        ("3.248", "50000", "49600", "1", "1", "1.248"),
    ] {
        let (mut seed, config, mut marks) = fixture();
        seed.cash = d(cash);
        seed.orders.clear();
        marks[0].price = d(mark);
        seed.positions[0].contracts = d("1");
        seed.positions[0].lots.truncate(1);
        let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        let before = owner.reservation().unwrap();
        assert_eq!(
            add(before.equity, -before.maintenance_margin),
            Ok(d(prior_excess))
        );
        let draft = calculate(
            owner.positions.btc().unwrap().get(&Product::Btc),
            Side::Short,
            d("0.5"),
            d(price),
            (&config, &frozen_spec(Product::Btc, false)),
            FeePolicy::BtcEthTradingTaker,
            None,
        )
        .unwrap();
        let mut projection = owner.projection().unwrap();
        projection.cash = add(projection.cash, draft.cash_delta).unwrap();
        projection.positions[0].contracts = d("0.5");
        projection.positions[0].lots[0].contracts = d("0.5");
        let after = config.evaluate(&projection, &marks).unwrap();
        assert_eq!(
            (after.equity, after.maintenance_margin),
            (d(equity), d(mmr))
        );
        assert!(
            !(after.equity > after.maintenance_margin
                && add(after.equity, -after.maintenance_margin).unwrap() > d(prior_excess))
        );
    }
}
