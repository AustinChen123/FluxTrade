use super::*;

#[test]
fn configured_fee_fifo_reduction_is_exact_for_long_and_short() {
    for (
        side,
        order_side,
        order_price,
        pnl1,
        cash1,
        pnl2,
        cash2,
        used0,
        avail0,
        hold0,
        loss1,
        used1,
        avail1,
        hold1,
    ) in [
        (
            Side::Long,
            Side::Short,
            "100",
            "7.5",
            "1006.555",
            "-3",
            "1003.231",
            "94.2",
            "905.8",
            "1.2",
            "7.5",
            "23.55",
            "975.505",
            "0.3",
        ),
        (
            Side::Short,
            Side::Long,
            "110",
            "-7.5",
            "991.555",
            "3",
            "994.231",
            "94.32",
            "905.68",
            "1.32",
            "7.5",
            "23.58",
            "975.475",
            "0.33",
        ),
    ] {
        let (mut seed, mut rows) = super::super::super::super::configured_tests::input(1);
        let product = rows[0].product.clone();
        seed.cash = d("1000");
        rows[0].taker_fee = d("0.002");
        rows[0].specs[0].contract_value = d("2");
        rows[0].specs[0].multiplier = d("1.5");
        rows[0].marks[0].price = d("105");
        seed.positions.push(SeedPosition {
            product: product.clone(),
            side,
            contracts: d("2"),
            lots: [
                ("FIFO-LOT-1", 0, "fifo-a", "1", "100"),
                ("FIFO-LOT-2", 1, "fifo-b", "1", "110"),
            ]
            .into_iter()
            .map(|(id, sequence, strategy_id, contracts, entry)| SeedLot {
                seed_execution_id: id.into(),
                seed_sequence: sequence,
                strategy_id: strategy_id.into(),
                contracts: d(contracts),
                entry: d(entry),
            })
            .collect(),
        });
        let prototype = fixture().0.orders[0].clone();
        seed.orders.push(configured_order(
            &prototype,
            "FIFO-REDUCE",
            &product,
            order_side,
            order_price,
            "2",
        ));
        seed.orders[0].reduce_only = true;
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), rows).unwrap();
        let initial = owner.reservation().unwrap();
        assert_eq!(
            (
                owner.positions.btc().unwrap()[&product].entry_basis,
                initial.orders.len(),
                initial.products.len(),
                initial.total_fee_hold,
                initial.total_order_loss,
                initial.used_margin,
                initial.available_margin
            ),
            (d("630"), 1, 1, d(hold0), d("30"), d(used0), d(avail0))
        );
        let before_order = owner.orders["FIFO-REDUCE"].facts.clone();

        let first = configured_input(&owner, "FIFO-REDUCE", "FIFO-X1", "1.5", "105", 501);
        let receipt1 = committed(&mut owner, &first);
        assert_eq!(
            (
                receipt1.realized_pnl_delta,
                receipt1.fee_amount,
                owner.cash,
                owner.fees,
                owner.gross_realized
            ),
            (d(pnl1), d("0.945"), d(cash1), d("0.945"), d(pnl1))
        );
        let position = &owner.positions.btc().unwrap()[&product];
        assert_eq!(
            (
                position.side,
                position.contracts,
                position.entry_basis,
                position.lots.len()
            ),
            (side, d("0.5"), d("165"), 1)
        );
        assert_eq!(
            (
                position.lots[0].source.seed_execution_id.as_str(),
                position.lots[0].source.seed_sequence,
                position.lots[0].source.strategy_id.as_str(),
                position.lots[0].source.entry,
                position.lots[0].source.contracts,
                position.lots[0].origin_spec_version.as_str(),
                position.lots[0].base_quantity
            ),
            (
                "FIFO-LOT-2",
                1,
                "fifo-b",
                d("110"),
                d("0.5"),
                "scale-spec-v1",
                d("1.5")
            )
        );
        assert_eq!(
            (
                owner.orders["FIFO-REDUCE"].facts.filled,
                owner.orders["FIFO-REDUCE"].facts.remaining,
                owner.orders["FIFO-REDUCE"].facts.status.as_str()
            ),
            (d("1.5"), d("0.5"), "PARTIALLY_FILLED")
        );
        let mid = owner.reservation().unwrap();
        assert_eq!(
            (
                mid.orders.len(),
                mid.total_fee_hold,
                mid.total_order_loss,
                mid.used_margin,
                mid.available_margin,
                mid.maintenance_margin
            ),
            (1, d(hold1), d(loss1), d(used1), d(avail1), d("0.7875"))
        );
        assert_eq!(
            (
                mid.products[0].position_value,
                mid.products[0].exposure_margin
            ),
            (d("157.5"), d("15.75"))
        );
        assert_eq!(
            (owner.state_version, owner.gate.clone()),
            (1, Gate::Running)
        );
        let committed1 = owner.clone();
        assert_eq!(
            owner.execute(&first),
            Ok(Reply::Duplicate(receipt1.clone()))
        );
        assert_eq!(owner, committed1);

        let second = configured_input(&owner, "FIFO-REDUCE", "FIFO-X2", "0.5", "108", 502);
        let receipt2 = committed(&mut owner, &second);
        assert_eq!(
            (
                receipt2.realized_pnl_delta,
                receipt2.fee_amount,
                owner.cash,
                owner.fees,
                owner.gross_realized
            ),
            (
                d(pnl2),
                d("0.324"),
                d(cash2),
                d("1.269"),
                d("4.5")
                    * if side == Side::Long {
                        Decimal::ONE
                    } else {
                        -Decimal::ONE
                    }
            )
        );
        assert!(receipt2.position_after.is_none());
        assert!(!owner.positions.btc().unwrap().contains_key(&product));
        assert_eq!(
            (
                owner.orders["FIFO-REDUCE"].facts.filled,
                owner.orders["FIFO-REDUCE"].facts.remaining,
                owner.orders["FIFO-REDUCE"].facts.status.as_str()
            ),
            (d("2"), d("0"), "FILLED")
        );
        assert_eq!(
            owner.orders["FIFO-REDUCE"].facts.original,
            before_order.original
        );
        let flat = owner.reservation().unwrap();
        assert_eq!(
            (
                flat.orders.len(),
                flat.total_fee_hold,
                flat.total_order_loss,
                flat.used_margin,
                flat.available_margin,
                flat.maintenance_margin
            ),
            (0, d("0"), d("0"), d("0"), d(cash2), d("0"))
        );
        assert_eq!(
            (owner.state_version, owner.gate.clone()),
            (2, Gate::Running)
        );
        let committed2 = owner.clone();
        assert_eq!(owner.execute(&second), Ok(Reply::Duplicate(receipt2)));
        assert_eq!(owner, committed2);
    }
}
