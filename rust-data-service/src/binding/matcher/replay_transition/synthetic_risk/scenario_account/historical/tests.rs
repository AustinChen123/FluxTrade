use super::*;
use std::str::FromStr;

fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn bar() -> BarPair {
    BarPair {
        product: Product::Btc,
        bar_open_ms: 0,
        bar_duration_ms: BAR_MS,
        trade_ohlc: [d("100"), d("120"), d("80"), d("110")],
        mark_ohlc: [d("101"), d("121"), d("81"), d("111")],
        volume_contracts: d("10.5"),
        confirmed: true,
        source_row_hash: [7; 32],
    }
}

#[test]
fn frozen_paths_and_product_local_four_step_capacity_are_exact() {
    let b = bar();
    let ohlc = product_steps(&b, Model::OpenHighLowClose, 0, d("1"), d("0.5"), d("1"), 1).unwrap();
    let olhc = product_steps(&b, Model::OpenLowHighClose, 0, d("1"), d("0.5"), d("1"), 1).unwrap();
    assert_eq!(ohlc.trade_price, d("120"));
    assert_eq!(olhc.trade_price, d("80"));
    assert_eq!(ohlc.capacity, d("3"));
    assert_eq!(ohlc.discarded_volume, d("0.5"));
    assert_eq!(
        product_steps(&b, Model::OpenHighLowClose, 0, d("1"), d("0.5"), d("1"), 2)
            .unwrap()
            .capacity,
        d("2")
    );
}

#[test]
fn crossing_fraction_orders_exactly_and_execution_identity_is_deterministic() {
    let mut b = bar();
    b.trade_ohlc = [d("90"), d("110"), d("80"), d("100")];
    let step = product_steps(&b, Model::OpenHighLowClose, 0, d("1"), d("1"), d("1"), 1).unwrap();
    let order = |id: &str, limit: &str, seq| WorkingOrderMeta {
        order_id: id.into(),
        product: Product::Btc,
        order_version: 0,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: seq,
        kind: OrderKind::Limit,
        side: Side::Short,
        limit_price: Some(d(limit)),
        risk_cancel_pending: false,
    };
    let orders = [order("later", "102", 1), order("first", "100", 2)];
    let candidates = product_candidates(
        &step,
        &orders,
        d("2"),
        Model::OpenHighLowClose,
        [9; 32],
        d("0"),
        |_| Ok(1),
    )
    .unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|c| c.order_id.as_str())
            .collect::<Vec<_>>(),
        ["first", "later"]
    );
    assert_eq!(
        candidates[0].trigger.compare(Fraction {
            numerator: d("10"),
            denominator: d("20")
        }),
        std::cmp::Ordering::Equal
    );
    assert_eq!(
        candidates[0].execution_id,
        derived_execution_id([9; 32], Model::OpenHighLowClose, [7; 32], 1, "first", 1)
    );
    assert_eq!(
        candidates[0].execution_id,
        derived_execution_id([9; 32], Model::OpenHighLowClose, [7; 32], 1, "first", 1)
    );
}

#[test]
fn h03_acceptance_boundary_does_not_backfill_the_opening_segment() {
    let mut b = bar();
    b.trade_ohlc = [d("105"), d("110"), d("90"), d("100")];
    let order = WorkingOrderMeta {
        order_id: "H03-ORDER".into(),
        product: Product::Btc,
        order_version: 0,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: 1,
        kind: OrderKind::Limit,
        side: Side::Long,
        limit_price: Some(d("95")),
        risk_cancel_pending: false,
    };
    let open = product_steps(&b, Model::OpenLowHighClose, 0, d("1"), d("1"), d("1"), 0).unwrap();
    let later = product_steps(&b, Model::OpenLowHighClose, 0, d("1"), d("1"), d("1"), 1).unwrap();
    assert!(product_candidates(
        &open,
        std::slice::from_ref(&order),
        d("1"),
        Model::OpenLowHighClose,
        [3; 32],
        d("0"),
        |_| Ok(1)
    )
    .unwrap()
    .is_empty());
    let fills = product_candidates(
        &later,
        &[order],
        d("1"),
        Model::OpenLowHighClose,
        [3; 32],
        d("0"),
        |_| Ok(1),
    )
    .unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price, d("95"));
}

#[test]
fn h09_frozen_paths_reverse_short_then_long_order() {
    let mut b = bar();
    b.trade_ohlc = [d("100"), d("110"), d("90"), d("100")];
    let short = WorkingOrderMeta {
        order_id: "H09-SHORT".into(),
        product: Product::Btc,
        order_version: 0,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: 1,
        kind: OrderKind::Limit,
        side: Side::Short,
        limit_price: Some(d("105")),
        risk_cancel_pending: false,
    };
    let long = WorkingOrderMeta {
        order_id: "H09-LONG".into(),
        product: Product::Btc,
        order_version: 0,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: 2,
        kind: OrderKind::Limit,
        side: Side::Long,
        limit_price: Some(d("95")),
        risk_cancel_pending: false,
    };
    let sequence = |model| {
        let mut orders = vec![short.clone(), long.clone()];
        let mut fills = Vec::new();
        for index in 0..4 {
            let step = product_steps(&b, model, 0, d("1"), d("1"), d("1"), index).unwrap();
            let candidates =
                product_candidates(&step, &orders, d("1"), model, [6; 32], d("0"), |_| Ok(1))
                    .unwrap();
            if let Some(candidate) = candidates.first() {
                fills.push(candidate.order_id.clone());
                orders.retain(|order| order.order_id != candidate.order_id);
            }
        }
        fills
    };
    assert_eq!(sequence(Model::OpenHighLowClose), ["H09-SHORT", "H09-LONG"]);
    assert_eq!(sequence(Model::OpenLowHighClose), ["H09-LONG", "H09-SHORT"]);
}

#[test]
fn h04_explicit_cancel_request_remains_fill_eligible_but_risk_cancel_does_not() {
    let mut b = bar();
    b.trade_ohlc = [d("100"), d("110"), d("90"), d("100")];
    let step = product_steps(&b, Model::OpenHighLowClose, 0, d("1"), d("1"), d("1"), 1).unwrap();
    let explicit = WorkingOrderMeta {
        order_id: "H04-ORDER".into(),
        product: Product::Btc,
        order_version: 1,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: 1,
        kind: OrderKind::Limit,
        side: Side::Short,
        limit_price: Some(d("105")),
        risk_cancel_pending: false,
    };
    let matches = |order: &WorkingOrderMeta| {
        product_candidates(
            &step,
            std::slice::from_ref(order),
            d("1"),
            Model::OpenHighLowClose,
            [8; 32],
            d("0"),
            |_| Ok(1),
        )
        .unwrap()
    };
    assert_eq!(matches(&explicit).len(), 1);
    let mut risk = explicit;
    risk.risk_cancel_pending = true;
    assert!(matches(&risk).is_empty());
}

#[test]
fn h10_risk_cancel_metadata_suppresses_second_same_node_candidate() {
    let mut b = bar();
    b.trade_ohlc = [d("100"), d("110"), d("90"), d("100")];
    let step = product_steps(&b, Model::OpenHighLowClose, 0, d("1"), d("1"), d("1"), 1).unwrap();
    let order = |id: &str, risk_cancel_pending| WorkingOrderMeta {
        order_id: id.into(),
        product: Product::Btc,
        order_version: 0,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: 1,
        kind: OrderKind::Limit,
        side: Side::Short,
        limit_price: Some(d("105")),
        risk_cancel_pending,
    };
    let candidates = product_candidates(
        &step,
        &[order("H10-ORDER-1", false), order("H10-ORDER-2", true)],
        d("2"),
        Model::OpenHighLowClose,
        [10; 32],
        d("0"),
        |_| Ok(1),
    )
    .unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|c| c.order_id.as_str())
            .collect::<Vec<_>>(),
        ["H10-ORDER-1"]
    );
}

#[test]
fn market_cost_rounds_adversely_to_tick_and_rejects_nonpositive_sell_cost() {
    let mut b = bar();
    b.trade_ohlc = [d("100"), d("120"), d("80"), d("110")];
    let step = product_steps(&b, Model::OpenHighLowClose, 0, d("1"), d("0.5"), d("1"), 1).unwrap();
    assert_eq!(market_price(&step, Side::Long, d("1")), Ok(d("120.5")));
    assert_eq!(market_price(&step, Side::Short, d("1")), Ok(d("119.5")));
    assert_eq!(
        market_price(&step, Side::Short, d("10000")),
        Err("INVALID_HISTORICAL_COST")
    );
    let market = WorkingOrderMeta {
        order_id: "market".into(),
        product: Product::Btc,
        order_version: 0,
        status: "OPEN".into(),
        remaining: d("1"),
        accepted_at: 0,
        accepted_source_sequence: 1,
        kind: OrderKind::Market,
        side: Side::Long,
        limit_price: None,
        risk_cancel_pending: false,
    };
    let fills = product_candidates(
        &step,
        &[market],
        d("1"),
        Model::OpenHighLowClose,
        [2; 32],
        d("1"),
        |_| Ok(1),
    )
    .unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price, d("120.5"));
}
