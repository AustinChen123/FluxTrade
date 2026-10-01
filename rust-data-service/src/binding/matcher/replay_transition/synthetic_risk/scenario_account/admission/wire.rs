//! Input conversion beside private admission fields; no admission decision.
use super::super::wire::{optional, Json};
use super::*;

pub(in super::super) fn decode(
    value: &Json,
    owner: &ScenarioAccount,
) -> Result<OrderIntent, Fault> {
    let rows = value.object(
        &[
            "intent_id",
            "client_order_id",
            "config_id",
            "product_id",
            "strategy_id",
            "side",
            "order_type",
            "quantity_contracts",
            "reduce_only",
            "requested_at",
        ],
        &["limit_price"],
    )?;
    let limit_price = optional(rows, "limit_price")
        .map(Json::decimal)
        .transpose()?;
    let order_type = match (rows["order_type"].text()?, limit_price.is_some()) {
        ("LIMIT", true) => OrderType::Limit,
        ("MARKET", false) => OrderType::Market,
        _ => return Err("INVALID_SCHEMA"),
    };
    Ok(OrderIntent {
        intent_id: rows["intent_id"].id()?,
        client_order_id: rows["client_order_id"].id()?,
        account_key: owner.key.clone(),
        config_id: rows["config_id"].id()?,
        product: owner.resolve_wire_product(&rows["product_id"])?,
        strategy_id: rows["strategy_id"].id()?,
        side: rows["side"].side()?,
        order_type,
        quantity: rows["quantity_contracts"].decimal()?,
        limit_price,
        reduce_only: rows["reduce_only"].boolean()?,
        requested_at: rows["requested_at"].integer(false)?,
    })
}
