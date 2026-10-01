use super::*;

#[test]
fn product_a_fee_controls_shared_product_b_admission() {
    for (rate, expected_fee, expected_cash, available, outcome, post_available) in [
        ("0", "0", "20.1", "10.1", "accepted", "0"),
        ("0.002", "0.2", "19.9", "9.9", "rejected", "-0.2"),
    ] {
        let (mut seed, mut rows) = super::super::super::super::configured_tests::input(2);
        let a = rows[0].product.clone();
        let b = rows[1].product.clone();
        rows[0].taker_fee = d(rate);
        rows[1].taker_fee = d("0.001");
        seed.cash = d("20.1");
        let prototype = fixture().0.orders[0].clone();
        seed.orders.push(SeedOrder {
            intent_id: "A-INTENT".into(),
            order_id: "A-ORDER".into(),
            client_id: "A-CLIENT".into(),
            product: ProfileProduct::BtcEth(a.clone()),
            side: Side::Long,
            price: d("100"),
            reduce_only: false,
            original: d("1"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "OPEN".into(),
            ..prototype
        });
        let mut owner = ScenarioAccount::from_configured(&seed, d("10"), rows).unwrap();
        let fill = super::super::super::super::execution::commit::tests::configured_input(
            &owner,
            "A-ORDER",
            &format!("A-FILL-{rate}"),
            "1",
            "100",
            501,
        );
        let execution = owner.execute(&fill).unwrap();
        let receipt = match execution {
            super::super::super::super::execution::commit::Reply::Committed {
                receipt,
                terminal_reason: None,
            } => receipt,
            other => panic!("unexpected configured fill: {other:?}"),
        };
        assert_eq!(
            (owner.fees, owner.cash, owner.gross_realized),
            (d(expected_fee), d(expected_cash), d("0"))
        );
        assert_eq!(
            (
                owner.state_version,
                owner.commit_sequence,
                owner.gate.clone()
            ),
            (1, 1, Gate::Running)
        );
        assert_eq!(owner.execution_receipts.len(), 1);
        assert_eq!(owner.orders["A-ORDER"].version, 1);
        assert_eq!(owner.orders["A-ORDER"].facts.status, "FILLED");
        assert_eq!(
            (
                owner.orders["A-ORDER"].facts.filled,
                owner.orders["A-ORDER"].facts.remaining
            ),
            (d("1"), d("0"))
        );
        let pos = &owner.positions.btc().unwrap()[&a];
        assert_eq!(
            (pos.side, pos.contracts, pos.entry_basis),
            (Side::Long, d("1"), d("100"))
        );
        assert!(!owner.positions.btc().unwrap().contains_key(&b));
        let risk = owner.reservation().unwrap();
        assert_eq!(
            (
                risk.total_fee_hold,
                risk.used_margin,
                risk.available_margin,
                risk.equity,
                risk.maintenance_margin
            ),
            (d("0"), d("10"), d(available), d(expected_cash), d("0.5"))
        );
        let after_fill = owner.clone();
        assert_eq!(
            owner.execute(&fill),
            Ok(super::super::super::super::execution::commit::Reply::Duplicate(receipt))
        );
        assert_eq!(owner, after_fill);

        let a_order = owner.orders["A-ORDER"].clone();
        let receipts = owner.execution_receipts.clone();
        let mut intent = admission::fixture_intent(
            &owner,
            &format!("B-INTENT-{rate}"),
            Side::Long,
            d("1"),
            d("100"),
        );
        intent.product = ProfileProduct::BtcEth(b.clone());
        let reply = owner
            .admit(&admission::Envelope {
                event_id: &format!("B-ADMIT-{rate}"),
                effective_at: 502,
                intent: &intent,
            })
            .unwrap();
        let admission::Evaluation::BtcEth(evidence) = &reply.result.evaluation else {
            panic!("BTC/ETH admission evidence expected");
        };
        let post = evidence.post_reservation.as_ref().unwrap();
        assert_eq!(
            (
                post.equity,
                post.total_fee_hold,
                post.used_margin,
                post.available_margin
            ),
            (d(expected_cash), d("0.1"), d("20.1"), d(post_available))
        );
        assert!(evidence.stress.is_none());
        assert_eq!(reply.result.account_version_before, 1);
        assert_eq!(
            reply.result.account_version_after,
            if outcome == "accepted" { 2 } else { 1 }
        );
        assert_eq!(owner.orders["A-ORDER"], a_order);
        assert_eq!(owner.execution_receipts, receipts);
        assert_eq!(
            (
                owner.cash,
                owner.fees,
                owner.gross_realized,
                owner.gate.clone()
            ),
            (d(expected_cash), d(expected_fee), d("0"), Gate::Running)
        );
        assert_eq!(owner.positions.btc().unwrap()[&a].contracts, d("1"));
        assert!(!owner.positions.btc().unwrap().contains_key(&b));
        if outcome == "accepted" {
            assert_eq!(reply.kind, admission::ReplyKind::Accepted);
            assert_eq!(reply.result.reason_code, None);
            let id = reply.result.order_id.as_ref().unwrap();
            assert_ne!(id, "A-ORDER");
            let order = &owner.orders[id].facts;
            assert_eq!(
                (
                    order.product.clone(),
                    order.side,
                    order.original,
                    order.remaining,
                    order.status.as_str()
                ),
                (
                    ProfileProduct::BtcEth(b.clone()),
                    Side::Long,
                    d("1"),
                    d("1"),
                    "OPEN"
                )
            );
            assert_eq!(owner.orders[id].version, 1);
            assert_eq!(owner.orders.len(), 2);
            assert_eq!(owner.reservation().unwrap(), *post);
        } else {
            assert_eq!(reply.kind, admission::ReplyKind::Rejected);
            assert_eq!(reply.result.reason_code, Some("INSUFFICIENT_SHARED_EQUITY"));
            assert!(reply.result.accepted_order.is_none() && reply.result.order_id.is_none());
            assert_eq!(owner.orders.len(), 1);
            let actual = owner.reservation().unwrap();
            assert_eq!(
                (
                    actual.used_margin,
                    actual.available_margin,
                    actual.total_fee_hold
                ),
                (d("10"), d(available), d("0"))
            );
        }
    }
}
