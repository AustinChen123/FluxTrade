use super::*;
use std::str::FromStr;

fn d(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn owner() -> ScenarioAccount {
    let (mut seed, products) = configured_tests::input(2);
    seed.cash = d("1000");
    seed.positions = vec![SeedPosition {
        product: products[0].product.clone(),
        side: Side::Long,
        contracts: d("1"),
        lots: vec![SeedLot {
            seed_execution_id: "GROSS-RESPONSE-SEED".into(),
            seed_sequence: 0,
            strategy_id: "gross-response-strategy".into(),
            contracts: d("1"),
            entry: d("100"),
        }],
    }];
    ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap()
}

fn gross_historical_node(
    owner: &ScenarioAccount,
    bar_open_ms: i64,
    step_index: u8,
) -> historical::NodeInput {
    let (scenario, _) = owner.btc_context().unwrap();
    historical::NodeInput {
        model_id: "OHLC4_OPEN_HIGH_LOW_CLOSE_V1".into(),
        model_version: "1".into(),
        run_contract_hash: [44; 32],
        step_index,
        market_slippage_bps: Decimal::ZERO,
        bars: scenario
            .products()
            .into_iter()
            .enumerate()
            .map(|(index, product)| historical::BarPair {
                product,
                bar_open_ms,
                bar_duration_ms: 60_000,
                trade_ohlc: [d("100"); 4],
                mark_ohlc: [d("100"); 4],
                volume_contracts: Decimal::ZERO,
                confirmed: true,
                trade_source_row_hash: [50 + index as u8; 32],
                mark_source_row_hash: [80 + index as u8; 32],
            })
            .collect(),
        working_orders: Vec::new(),
    }
}

#[test]
fn gross_profile_historical_mark_context_keeps_policy_identity() {
    let (mut seed, mut products) = configured_tests::input(2);
    seed.effective_at = 0;
    for product in &mut products {
        for mark in &mut product.marks {
            mark.valid_to = i64::MAX;
        }
    }
    let mut owner =
        ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap();
    let result = owner
        .historical_market_step(&gross_historical_node(&owner, 1_000, 0))
        .unwrap();
    assert!(owner.validate_context(result.effective_at).is_ok());
}

#[test]
fn gross_profile_historical_spec_boundary_keeps_policy_identity() {
    let (mut seed, mut products) = configured_tests::input(2);
    seed.effective_at = 0;
    let boundary = 60_000 * 16;
    let first = &mut products[0].specs[0];
    let prior = first.clone();
    first.interval = Interval {
        from: 0,
        to: Some(boundary),
    };
    products[0].specs.push(Spec {
        version: "gross-spec-v2".into(),
        interval: Interval {
            from: boundary,
            to: None,
        },
        ..prior
    });
    for product in &mut products {
        for mark in &mut product.marks {
            mark.valid_to = i64::MAX;
        }
    }
    let mut owner =
        ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap();
    let result = owner
        .historical_market_step(&gross_historical_node(&owner, 0, 3))
        .unwrap();
    assert!(owner.validate_context(result.effective_at).is_ok());
}

fn owner_at_gross_ratio_18() -> ScenarioAccount {
    let (mut seed, products) = configured_tests::input(2);
    seed.cash = d("1000");
    seed.positions = products
        .iter()
        .enumerate()
        .map(|(sequence, product)| {
            let side = if sequence == 0 {
                Side::Long
            } else {
                Side::Short
            };
            SeedPosition {
                product: product.product.clone(),
                side,
                contracts: d("9"),
                lots: vec![SeedLot {
                    seed_execution_id: format!("GROSS-18-SEED-{sequence}"),
                    seed_sequence: sequence as u64,
                    strategy_id: "gross-response-strategy".into(),
                    contracts: d("9"),
                    entry: d("100"),
                }],
            }
        })
        .collect();
    ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap()
}

fn owner_with_working_orders() -> ScenarioAccount {
    let (mut seed, mut products) = configured_tests::input(2);
    products.reverse();
    seed.cash = d("1000");
    seed.positions = vec![SeedPosition {
        product: Product::Btc,
        side: Side::Long,
        contracts: d("1"),
        lots: vec![SeedLot {
            seed_execution_id: "GROSS-RESPONSE-SEED".into(),
            seed_sequence: 0,
            strategy_id: "gross-response-strategy".into(),
            contracts: d("1"),
            entry: d("100"),
        }],
    }];
    seed.orders = vec![
        SeedOrder {
            intent_id: "I-ORDER-A".into(),
            order_id: "ORDER-A".into(),
            client_id: "C-ORDER-A".into(),
            strategy_id: "gross-response-strategy".into(),
            product: ProfileProduct::BtcEth(Product::Btc),
            side: Side::Short,
            price: d("100"),
            reduce_only: true,
            original: d("1"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("1"),
            status: "OPEN".into(),
        },
        SeedOrder {
            intent_id: "I-ORDER-Z".into(),
            order_id: "ORDER-Z".into(),
            client_id: "C-ORDER-Z".into(),
            strategy_id: "gross-response-strategy".into(),
            product: ProfileProduct::BtcEth(Product("ETH-USDT-SWAP".into())),
            side: Side::Long,
            price: d("100"),
            reduce_only: false,
            original: d("0.5"),
            filled: Decimal::ZERO,
            canceled: Decimal::ZERO,
            remaining: d("0.5"),
            status: "OPEN".into(),
        },
    ];
    ScenarioAccount::from_configured_profile(&seed, d("10"), products, true).unwrap()
}

fn grouped_intent(
    owner: &ScenarioAccount,
    group_id: &str,
    at: i64,
    intent_id: &str,
    product: Product,
    side: Side,
    quantity: &str,
    reduce_only: bool,
) -> group::Group {
    let intent = admission::fixture_intent_for_product_role(
        owner,
        intent_id,
        ProfileProduct::BtcEth(product),
        side,
        d(quantity),
        d("100"),
        reduce_only,
    );
    let stamp = risk_transition::cancel::Stamp {
        event_id: intent_id.into(),
        effective_at: at,
        source_sequence: None,
        causal_parent_ids: Vec::new(),
        ordering_contract_id: "S_order_v1".into(),
        scenario_ordinal: 60,
    };
    group::Group {
        group_id: group_id.into(),
        account_key: owner.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: at,
        declared_member_count: 1,
        members: vec![group::Member {
            stamp,
            input: group::Input::Intent(intent),
        }],
    }
}

fn admission_ready(owner: &ScenarioAccount) -> bool {
    gross_response::project(owner)
        .unwrap()
        .object(
            &["current_admission_state_ready"],
            &[
                "schema_version",
                "account_key",
                "profile_id",
                "account_version",
                "valuation_context_id",
                "valuation_effective_at",
                "owner_state_digest",
                "gate",
                "lifecycle",
                "gross_admission",
                "maintenance_margin",
                "available_margin",
                "products",
                "orders",
            ],
        )
        .unwrap()["current_admission_state_ready"]
        .boolean()
        .unwrap()
}

#[test]
fn gross_response_projection_is_same_owner_detached_and_closed() {
    let owner = owner();
    let before = owner.clone();
    let state = gross_response::project(&owner).unwrap();
    let fields = state
        .object(
            &[
                "schema_version",
                "account_key",
                "profile_id",
                "account_version",
                "valuation_context_id",
                "valuation_effective_at",
                "owner_state_digest",
                "gate",
                "lifecycle",
                "gross_admission",
                "maintenance_margin",
                "available_margin",
                "current_admission_state_ready",
                "products",
                "orders",
            ],
            &[],
        )
        .unwrap();
    assert_eq!(
        fields["schema_version"].text().unwrap(),
        "spider_gross_response_native_state_v1"
    );
    assert_eq!(
        fields["profile_id"].text().unwrap(),
        "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1"
    );
    assert_eq!(fields["account_version"].integer(true).unwrap(), 0);
    assert_eq!(
        fields["valuation_context_id"].hash().unwrap(),
        owner.valuation_context_id
    );
    assert_eq!(
        fields["owner_state_digest"].hash().unwrap(),
        owner.owner_evidence_digest().unwrap()
    );
    assert_eq!(
        fields["current_admission_state_ready"].boolean().unwrap(),
        true
    );
    let products = fields["products"].array().unwrap();
    assert_eq!(products.len(), 2);
    let first = products[0]
        .object(
            &[
                "product_id",
                "signed_contracts",
                "gross",
                "mark",
                "spec_version",
                "tier_version",
                "contract_value",
                "multiplier",
                "quantity_step",
                "minimum_quantity",
                "taker_fee_rate",
                "closing_capacity",
                "lots",
            ],
            &[],
        )
        .unwrap();
    assert_eq!(first["signed_contracts"].decimal().unwrap(), d("1"));
    assert_eq!(first["gross"].decimal().unwrap(), d("100"));
    assert_eq!(first["closing_capacity"].decimal().unwrap(), d("1"));
    assert_eq!(first["lots"].array().unwrap().len(), 1);
    assert_eq!(
        first["lots"].array().unwrap()[0]
            .object(&["strategy_id", "contracts"], &[])
            .unwrap()["strategy_id"]
            .text()
            .unwrap(),
        "gross-response-strategy"
    );
    assert!(fields["orders"].array().unwrap().is_empty());
    assert_eq!(owner, before);

    let working = owner_with_working_orders();
    let working_before = working.clone();
    let working_state = gross_response::project(&working).unwrap();
    let working_orders = working_state
        .object(
            &[
                "schema_version",
                "account_key",
                "profile_id",
                "account_version",
                "valuation_context_id",
                "valuation_effective_at",
                "owner_state_digest",
                "gate",
                "lifecycle",
                "gross_admission",
                "maintenance_margin",
                "available_margin",
                "current_admission_state_ready",
                "products",
                "orders",
            ],
            &[],
        )
        .unwrap()["orders"]
        .array()
        .unwrap();
    assert_eq!(working_orders.len(), 2);
    assert_eq!(
        working_orders[0]
            .object(
                &["order_id"],
                &[
                    "client_order_id",
                    "strategy_id",
                    "product_id",
                    "side",
                    "order_type",
                    "reduce_only",
                    "remaining_contracts",
                    "status",
                    "cancel_pending",
                    "verified_reducing"
                ]
            )
            .unwrap()["order_id"]
            .text()
            .unwrap(),
        "ORDER-A"
    );
    assert_eq!(
        working_orders[0]
            .object(
                &["order_id"],
                &[
                    "client_order_id",
                    "strategy_id",
                    "product_id",
                    "side",
                    "order_type",
                    "reduce_only",
                    "remaining_contracts",
                    "status",
                    "cancel_pending",
                    "verified_reducing"
                ]
            )
            .unwrap()["verified_reducing"]
            .boolean()
            .unwrap(),
        true
    );
    assert_eq!(
        working_orders[1]
            .object(
                &["order_id"],
                &[
                    "client_order_id",
                    "strategy_id",
                    "product_id",
                    "side",
                    "order_type",
                    "reduce_only",
                    "remaining_contracts",
                    "status",
                    "cancel_pending",
                    "verified_reducing"
                ]
            )
            .unwrap()["verified_reducing"]
            .boolean()
            .unwrap(),
        false
    );
    let projected_products = working_state
        .object(
            &[
                "schema_version",
                "account_key",
                "profile_id",
                "account_version",
                "valuation_context_id",
                "valuation_effective_at",
                "owner_state_digest",
                "gate",
                "lifecycle",
                "gross_admission",
                "maintenance_margin",
                "available_margin",
                "current_admission_state_ready",
                "products",
                "orders",
            ],
            &[],
        )
        .unwrap()["products"]
        .array()
        .unwrap();
    assert_eq!(
        projected_products
            .iter()
            .map(|row| {
                row.object(
                    &["product_id"],
                    &[
                        "signed_contracts",
                        "gross",
                        "mark",
                        "spec_version",
                        "tier_version",
                        "contract_value",
                        "multiplier",
                        "quantity_step",
                        "minimum_quantity",
                        "taker_fee_rate",
                        "closing_capacity",
                        "lots",
                    ],
                )
                .unwrap()["product_id"]
                    .text()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>(),
        vec!["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    );
    let btc = projected_products
        .iter()
        .find(|row| {
            row.object(
                &["product_id"],
                &[
                    "signed_contracts",
                    "gross",
                    "mark",
                    "spec_version",
                    "tier_version",
                    "contract_value",
                    "multiplier",
                    "quantity_step",
                    "minimum_quantity",
                    "taker_fee_rate",
                    "closing_capacity",
                    "lots",
                ],
            )
            .unwrap()["product_id"]
                .text()
                .unwrap()
                == "BTC-USDT-SWAP"
        })
        .unwrap();
    assert_eq!(
        btc.object(
            &["closing_capacity"],
            &[
                "product_id",
                "signed_contracts",
                "gross",
                "mark",
                "spec_version",
                "tier_version",
                "contract_value",
                "multiplier",
                "quantity_step",
                "minimum_quantity",
                "taker_fee_rate",
                "lots",
            ],
        )
        .unwrap()["closing_capacity"]
            .decimal()
            .unwrap(),
        Decimal::ZERO
    );
    assert_eq!(working, working_before);

    let mut legacy = owner.clone();
    legacy.profile = match legacy.profile {
        ProfileContext::BtcEthScenario {
            scenario,
            marks,
            min_cash_profile,
            p1_liquidation_profile,
            ..
        } => ProfileContext::BtcEthScenario {
            scenario,
            marks,
            min_cash_profile,
            p1_liquidation_profile,
            gross_admission_profile: false,
        },
        _ => unreachable!(),
    };
    assert_eq!(
        gross_response::project(&legacy),
        Err("UNSUPPORTED_GROSS_PROFILE")
    );
}

#[test]
fn restricted_group_rejects_only_risk_intent_and_preserves_verified_reduction() {
    let mut owner = owner_at_gross_ratio_18();
    let before = owner.clone();
    let initial_projection = gross_admission::Projection::current(&owner).unwrap();
    assert_eq!(initial_projection.effective_equity, d("1000"));
    assert_eq!(initial_projection.gross, d("1800"));
    let risky = grouped_intent(
        &owner,
        "GROSS-RESTRICTED-RISK",
        500,
        "GROSS-RISK-INTENT",
        Product::Btc,
        Side::Long,
        "1",
        false,
    );
    let result = owner
        .apply_group_observed_with_restriction(&risky, true, |_| Ok(()))
        .unwrap();
    let group::Applied::Fresh(completion) = result else {
        panic!("first group must be fresh")
    };
    assert_eq!(
        completion.rejections,
        vec![("GROSS-RISK-INTENT".into(), "GROSS_RESPONSE_PENDING")]
    );
    assert!(completion.committed.is_empty());
    assert_eq!(owner.cash, before.cash);
    assert_eq!(owner.fees, before.fees);
    assert_eq!(owner.positions, before.positions);
    assert_eq!(owner.orders, before.orders);
    assert_eq!(owner.gate, Gate::Running);
    assert_eq!(owner.state_version, before.state_version);

    let reducing = grouped_intent(
        &owner,
        "GROSS-RESTRICTED-REDUCE",
        501,
        "GROSS-REDUCE-INTENT",
        Product::Btc,
        Side::Short,
        "0.5",
        true,
    );
    let result = owner
        .apply_group_observed_with_restriction(&reducing, true, |_| Ok(()))
        .unwrap();
    let group::Applied::Fresh(completion) = result else {
        panic!("first reducing group must be fresh")
    };
    assert!(completion.rejections.is_empty());
    assert_eq!(
        completion.committed,
        vec![group::Reference::Source("GROSS-REDUCE-INTENT".into())]
    );
    assert_eq!(owner.orders.len(), 1);
    assert_eq!(owner.state_version, before.state_version + 1);

    let unrestricted_risk = grouped_intent(
        &owner,
        "GROSS-UNRESTRICTED-RISK",
        502,
        "GROSS-UNRESTRICTED-RISK-INTENT",
        Product::Btc,
        Side::Long,
        "0.5",
        false,
    );
    let unrestricted = owner
        .apply_group_observed_with_restriction(&unrestricted_risk, false, |_| Ok(()))
        .unwrap();
    let group::Applied::Fresh(unrestricted) = unrestricted else {
        panic!("unrestricted control group must be fresh")
    };
    assert!(unrestricted.rejections.is_empty());
    assert_eq!(unrestricted.committed.len(), 1);
    assert_eq!(owner.orders.len(), 2);
    let duplicate = owner
        .apply_group_observed_with_restriction(&reducing, false, |_| Ok(()))
        .unwrap();
    let group::Applied::Duplicate(duplicate) = duplicate else {
        panic!("same group must return its original receipt")
    };
    assert!(duplicate.rejections.is_empty());
    assert_eq!(owner.orders.len(), 2);
    assert_eq!(owner.gate, Gate::Running);
}

#[test]
fn gross_projection_readiness_tracks_native_admission_boundaries() {
    let ready = owner();
    assert!(admission_ready(&ready));
    let ready_projection = gross_admission::Projection::current(&ready).unwrap();
    let ready_reservation = ready.reservation().unwrap();
    assert!(ready_projection.effective_equity > ready_reservation.maintenance_margin);
    assert!(ready_reservation.available_margin >= Decimal::ZERO);

    let (mut cap_seed, cap_products) = configured_tests::input(2);
    cap_seed.cash = d("1000");
    cap_seed.positions = vec![
        SeedPosition {
            product: cap_products[0].product.clone(),
            side: Side::Long,
            contracts: d("11"),
            lots: vec![SeedLot {
                seed_execution_id: "CAP-SEED-A".into(),
                seed_sequence: 0,
                strategy_id: "gross-response-strategy".into(),
                contracts: d("11"),
                entry: d("100"),
            }],
        },
        SeedPosition {
            product: cap_products[1].product.clone(),
            side: Side::Short,
            contracts: d("11"),
            lots: vec![SeedLot {
                seed_execution_id: "CAP-SEED-B".into(),
                seed_sequence: 1,
                strategy_id: "gross-response-strategy".into(),
                contracts: d("11"),
                entry: d("100"),
            }],
        },
    ];
    let over_cap =
        ScenarioAccount::from_configured_profile(&cap_seed, d("10"), cap_products, true).unwrap();
    let cap_projection = gross_admission::Projection::current(&over_cap).unwrap();
    assert_eq!(cap_projection.effective_equity, d("1000"));
    assert_eq!(cap_projection.worst_gross, d("2200"));
    assert!(!admission_ready(&over_cap));

    let (mut margin_seed, margin_products) = configured_tests::input(2);
    margin_seed.cash = d("1000");
    margin_seed.positions = margin_products
        .iter()
        .enumerate()
        .map(|(sequence, product)| {
            let side = if sequence == 0 {
                Side::Long
            } else {
                Side::Short
            };
            SeedPosition {
                product: product.product.clone(),
                side,
                contracts: d("7.5"),
                lots: vec![SeedLot {
                    seed_execution_id: format!("MARGIN-SEED-{sequence}"),
                    seed_sequence: sequence as u64,
                    strategy_id: "gross-response-strategy".into(),
                    contracts: d("7.5"),
                    entry: d("100"),
                }],
            }
        })
        .collect();
    let margin_limited =
        ScenarioAccount::from_configured_profile(&margin_seed, d("1"), margin_products, true)
            .unwrap();
    let margin_projection = gross_admission::Projection::current(&margin_limited).unwrap();
    let margin_reservation = margin_limited.reservation().unwrap();
    assert_eq!(margin_projection.effective_equity, d("1000"));
    assert_eq!(margin_projection.worst_gross, d("1500"));
    assert!(margin_projection.worst_gross < d("2") * margin_projection.effective_equity);
    assert!(margin_projection.effective_equity > margin_reservation.maintenance_margin);
    assert!(margin_reservation.available_margin < Decimal::ZERO);
    assert!(!admission_ready(&margin_limited));

    let mut market_pending = owner();
    let reduction = admission::historical_reducing_market_fixture(
        &market_pending,
        "READINESS-REDUCE".into(),
        "READINESS-REDUCE-CLIENT".into(),
        "BTC-USDT-SWAP",
        Side::Short,
        d("0.5"),
    );
    let stamp = risk_transition::cancel::Stamp {
        event_id: "READINESS-REDUCE-EVENT".into(),
        effective_at: 500,
        source_sequence: Some(1),
        causal_parent_ids: Vec::new(),
        ordering_contract_id: "HISTORICAL_ORDER_V1".into(),
        scenario_ordinal: 60,
    };
    let market_group = group::Group {
        group_id: "READINESS-REDUCE-GROUP".into(),
        account_key: market_pending.key.clone(),
        ordering_contract_id: "HISTORICAL_ORDER_V1".into(),
        group_effective_at: 500,
        declared_member_count: 1,
        members: vec![group::Member {
            stamp,
            input: group::Input::Intent(reduction),
        }],
    };
    let market_result = market_pending
        .apply_group_observed_with_restriction(&market_group, false, |_| Ok(()))
        .unwrap();
    let group::Applied::Fresh(market_result) = market_result else {
        panic!("first market group must be fresh")
    };
    assert!(market_result.failure.is_none());
    assert!(market_result.rejections.is_empty());
    assert_eq!(market_pending.gate, Gate::Running);
    assert_eq!(
        market_pending.transition.lifecycle,
        risk_transition::Lifecycle::RiskStable
    );
    assert_eq!(market_pending.orders.len(), 1);
    let market_order = market_pending.orders.values().next().unwrap();
    assert_eq!(
        market_pending.admitted_order_type(&market_order.facts),
        admission::OrderType::Market
    );
    assert_eq!(market_order.facts.remaining, d("0.5"));
    let market_projection = gross_admission::Projection::current(&market_pending).unwrap();
    assert_eq!(market_projection.equity_floor, None);
    let market_wire = market_projection.wire_json(&market_pending).unwrap();
    let market_fields = market_wire
        .object(
            &[
                "schema_version",
                "risk_policy_id",
                "bound_scope",
                "account_key",
                "account_version",
                "valuation_context_id",
                "cap",
                "effective_equity",
                "gross",
                "net",
                "worst_gross",
                "order_loss_hold",
                "fee_hold",
                "equity_floor",
                "gross_ratio",
                "bound_reason",
            ],
            &[],
        )
        .unwrap();
    assert_eq!(
        market_fields["bound_reason"].text().unwrap(),
        "MARKET_ORDER_PENDING"
    );
    assert!(!admission_ready(&market_pending));

    let mut cancel_pending = owner_with_working_orders();
    cancel_pending
        .request_cancel(&risk_transition::cancel::RequestInput {
            stamp: risk_transition::cancel::Stamp {
                event_id: "READINESS-CANCEL-REQUEST".into(),
                effective_at: 501,
                source_sequence: None,
                causal_parent_ids: Vec::new(),
                ordering_contract_id: "S_order_v1".into(),
                scenario_ordinal: 40,
            },
            targets: vec![(
                "ORDER-Z".into(),
                risk_transition::cancel::Reason::ExplicitScenario,
            )],
        })
        .unwrap();
    assert_eq!(
        cancel_pending.transition.lifecycle,
        risk_transition::Lifecycle::AwaitingCancelEffective
    );
    assert!(!admission_ready(&cancel_pending));

    let (mut failed_seed, mut failed_products) = configured_tests::input(2);
    failed_seed.cash = d("1000");
    failed_seed.positions.push(SeedPosition {
        product: failed_products[0].product.clone(),
        side: Side::Long,
        contracts: d("1"),
        lots: vec![SeedLot {
            seed_execution_id: "FAILED-READINESS-SEED".into(),
            seed_sequence: 0,
            strategy_id: "gross-response-strategy".into(),
            contracts: d("1"),
            entry: d("100"),
        }],
    });
    for product in &mut failed_products {
        let mut prior = product.marks[0].clone();
        prior.valid_to = 501;
        let next = Mark {
            price: d("101"),
            valid_from: 501,
            valid_to: 3000,
            ..prior.clone()
        };
        product.marks = vec![prior, next];
    }
    let mut failed =
        ScenarioAccount::from_configured_profile(&failed_seed, d("10"), failed_products, true)
            .unwrap();
    let (scenario, marks) = failed.btc_context().unwrap();
    let context = context::tests::activation(&failed, 501, context::tests::mark_rows(marks, 501));
    assert!(scenario.configured.is_some());
    assert_eq!(
        failed.activate_context_checked(&context, |stage| {
            if stage == context::Stage::AfterSwap {
                Err("GROSS-READINESS-TEST-FAULT")
            } else {
                Ok(())
            }
        }),
        Err("GROSS-READINESS-TEST-FAULT")
    );
    assert_eq!(failed.gate, Gate::Failed("GROSS-READINESS-TEST-FAULT"));
    assert!(!admission_ready(&failed));

    let (mut terminal_seed, mut terminal_products) = configured_tests::input(2);
    terminal_seed.cash = d("50");
    terminal_seed.positions.push(SeedPosition {
        product: terminal_products[0].product.clone(),
        side: Side::Long,
        contracts: d("1"),
        lots: vec![SeedLot {
            seed_execution_id: "TERMINAL-READINESS-SEED".into(),
            seed_sequence: 0,
            strategy_id: "gross-response-strategy".into(),
            contracts: d("1"),
            entry: d("100"),
        }],
    });
    for product in &mut terminal_products {
        let mut prior = product.marks[0].clone();
        prior.valid_to = 501;
        let next = Mark {
            price: d("50"),
            valid_from: 501,
            valid_to: 3000,
            ..prior.clone()
        };
        product.marks = vec![prior, next];
    }
    let mut terminal =
        ScenarioAccount::from_configured_profile(&terminal_seed, d("10"), terminal_products, true)
            .unwrap();
    let (_, terminal_marks) = terminal.btc_context().unwrap();
    let terminal_context = context::tests::activation(
        &terminal,
        501,
        context::tests::mark_rows(terminal_marks, 501),
    );
    terminal.activate_context(&terminal_context).unwrap();
    assert!(matches!(
        terminal.transition.lifecycle,
        risk_transition::Lifecycle::LiquidatedFlat
            | risk_transition::Lifecycle::LiquidatedInsolvent
    ));
    assert!(!admission_ready(&terminal));
}
