//! Detached same-owner evidence for the run-local gross response.
use super::*;
use risk_transition::Lifecycle;

const STATE_SCHEMA: &str = "spider_gross_response_native_state_v1";

pub(super) fn project(owner: &ScenarioAccount) -> Result<wire::Json, Fault> {
    use wire::{account, decimal, fields, hash, number, string, Json};

    if !owner.is_gross_admission_profile() {
        return Err("UNSUPPORTED_GROSS_PROFILE");
    }
    let (scenario, marks) = owner.btc_context()?;
    if scenario.configured.is_none() {
        return Err("UNSUPPORTED_GROSS_PROFILE");
    }
    let valuation = owner.projection()?;
    let effective_at = valuation.effective_at;
    let reservation = owner.reservation()?;
    let gross = gross_admission::Projection::current(owner)?;
    let admission = gross.wire_json(owner)?;
    let admission_ready = gross_admission::current_state_ready(owner, &gross, &reservation)?;

    let positions = owner.positions.btc()?;
    let mut configured_products = scenario.products();
    configured_products.sort_by(|left, right| product_id(left).cmp(product_id(right)));
    let mut products = Vec::with_capacity(configured_products.len());
    for product in configured_products {
        let (spec, tier) = scenario.resolve(&product, effective_at)?;
        let mark = marks
            .iter()
            .find(|row| {
                row.product == product
                    && row.valid_from <= effective_at
                    && effective_at < row.valid_to
            })
            .ok_or("MARK_COVERAGE_MISSING")?;
        let position = positions.get(&product);
        let (signed_contracts, side, lots, position_contracts) = match position {
            Some(position) => {
                let signed = if position.side == Side::Long {
                    position.contracts
                } else {
                    -position.contracts
                };
                let lots = position
                    .lots
                    .iter()
                    .map(|lot| {
                        fields(vec![
                            ("strategy_id", string(&lot.source.strategy_id)),
                            ("contracts", decimal(lot.source.contracts)),
                        ])
                    })
                    .collect();
                (signed, Some(position.side), lots, position.contracts)
            }
            None => (Decimal::ZERO, None, Vec::new(), Decimal::ZERO),
        };
        let product_value = reservation
            .products
            .iter()
            .find(|row| row.product == product)
            .map_or(Decimal::ZERO, |row| row.position_value);
        let closing_capacity = match side {
            Some(side) => {
                gross_admission::closing_capacity(owner, &product, side, position_contracts)?
            }
            None => Decimal::ZERO,
        };
        let fee_rate = hypothetical_settlement::configured_fee_policy(scenario, &product)?.rate();
        products.push(fields(vec![
            ("product_id", string(product_id(&product))),
            ("signed_contracts", decimal(signed_contracts)),
            ("gross", decimal(product_value)),
            ("mark", decimal(mark.price)),
            ("spec_version", string(&spec.version)),
            ("tier_version", string(&tier.version)),
            ("contract_value", decimal(spec.contract_value)),
            ("multiplier", decimal(spec.multiplier)),
            ("quantity_step", decimal(spec.lot)),
            ("minimum_quantity", decimal(spec.minimum)),
            ("taker_fee_rate", decimal(fee_rate)),
            ("closing_capacity", decimal(closing_capacity)),
            ("lots", Json::Array(lots)),
        ]));
    }

    let mut working_orders = owner
        .orders
        .values()
        .filter_map(
            |order| match order.facts.projects_remainder("NATIVE_INVARIANT") {
                Ok(true) => Some(Ok(order)),
                Ok(false) => None,
                Err(fault) => Some(Err(fault)),
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    working_orders.sort_by(|left, right| {
        (
            left.facts.product.canonical_id(),
            left.facts.order_id.as_str(),
        )
            .cmp(&(
                right.facts.product.canonical_id(),
                right.facts.order_id.as_str(),
            ))
    });
    let mut orders = Vec::with_capacity(working_orders.len());
    for order in working_orders {
        let product = order.facts.product.btc()?;
        let order_type = owner.admitted_order_type(&order.facts);
        let cancel_pending = matches!(order.cancel, risk_transition::cancel::State::Requested(_));
        let verified_reducing = if order.facts.reduce_only {
            if let Some(position) = positions.get(product) {
                order.facts.side != position.side
                    && gross_admission::committed_reducing(owner, product, position.side)?
                        <= position.contracts
            } else {
                false
            }
        } else {
            false
        };
        orders.push(fields(vec![
            ("order_id", string(&order.facts.order_id)),
            ("client_order_id", string(&order.facts.client_id)),
            ("strategy_id", string(&order.facts.strategy_id)),
            ("product_id", string(order.facts.product.canonical_id())),
            (
                "side",
                string(if order.facts.side == Side::Long {
                    "LONG"
                } else {
                    "SHORT"
                }),
            ),
            (
                "order_type",
                string(match order_type {
                    admission::OrderType::Limit => "LIMIT",
                    admission::OrderType::Market => "MARKET",
                    admission::OrderType::Unsupported => return Err("NATIVE_INVARIANT"),
                }),
            ),
            ("reduce_only", Json::Bool(order.facts.reduce_only)),
            ("remaining_contracts", decimal(order.facts.remaining)),
            ("status", string(&order.facts.status)),
            ("cancel_pending", Json::Bool(cancel_pending)),
            ("verified_reducing", Json::Bool(verified_reducing)),
        ]));
    }
    let lifecycle = match owner.transition.lifecycle {
        Lifecycle::RiskStable => "RISK_STABLE",
        Lifecycle::AwaitingCancelEffective => "AWAITING_CANCEL_EFFECTIVE",
        Lifecycle::LiquidatedFlat => "LIQUIDATED_FLAT",
        Lifecycle::LiquidatedInsolvent => "LIQUIDATED_INSOLVENT",
    };
    let gate = match &owner.gate {
        Gate::Running => string("RUNNING"),
        Gate::Failed(reason) => fields(vec![
            ("status", string("FAILED")),
            ("reason", string(reason)),
        ]),
    };
    Ok(fields(vec![
        ("schema_version", string(STATE_SCHEMA)),
        ("account_key", account(&owner.key)),
        (
            "profile_id",
            string("SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1"),
        ),
        ("account_version", number(owner.state_version)?),
        ("valuation_context_id", hash(owner.valuation_context_id)),
        ("valuation_effective_at", number(effective_at)?),
        ("owner_state_digest", hash(owner.owner_evidence_digest()?)),
        ("gate", gate),
        ("lifecycle", string(lifecycle)),
        ("gross_admission", admission),
        (
            "maintenance_margin",
            decimal(reservation.maintenance_margin),
        ),
        ("available_margin", decimal(reservation.available_margin)),
        ("current_admission_state_ready", Json::Bool(admission_ready)),
        ("products", Json::Array(products)),
        ("orders", Json::Array(orders)),
    ]))
}
