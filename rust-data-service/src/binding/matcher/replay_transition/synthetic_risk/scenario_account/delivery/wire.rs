//! Representation and existing pre-identity validation, never fact resolution.
use super::super::wire::{decode, optional, Json};
use super::*;
mod output;
use identity::{Projection, Reference};
#[cfg(test)]
mod tests;

fn token(value: &Json, choices: &[&'static str]) -> Result<&'static str, Fault> {
    let value = value.text()?;
    choices
        .iter()
        .copied()
        .find(|s| *s == value)
        .ok_or("INVALID_SCHEMA")
}
pub(in super::super) fn decode_projection(input: &str) -> Result<Projection, Fault> {
    let value = decode(input)?;
    let r = value.object(
        &[
            "schema_version",
            "reference",
            "payload_kind",
            "occurrence_index",
            "schedule_sequence",
            "visible_at",
        ],
        &["continuation_id", "transport"],
    )?;
    let reference = r["reference"].object(&["namespace", "fact_id"], &[])?;
    let projection = Projection {
        schema_version: r["schema_version"].text()?.into(),
        reference: Reference {
            namespace: token(
                &reference["namespace"],
                &["SOURCE", "LIQUIDATION", "SNAPSHOT"],
            )?,
            fact_id: reference["fact_id"].id()?,
        },
        kind: token(
            &r["payload_kind"],
            &[
                "EXECUTION_FACT",
                "TRANSPORT_ACK",
                "MARKET_SNAPSHOT",
                "EARN_SNAPSHOT",
                "TRADING_SNAPSHOT",
                "POSITION_SNAPSHOT",
                "OPEN_ORDER_SNAPSHOT",
            ],
        )?,
        occurrence: r["occurrence_index"].integer(true)?,
        sequence: r["schedule_sequence"].integer(true)?,
        visible_at: r["visible_at"].integer(true)?,
        continuation: optional(r, "continuation_id").map(Json::id).transpose()?,
        transport: optional(r, "transport").map(transport).transpose()?,
    };
    if projection.kind == "TRANSPORT_ACK"
        && r.get("transport")
            .is_some_and(|value| matches!(value, Json::Null))
    {
        return Err("INVALID_SCHEMA");
    }
    projection.validate()?;
    Ok(projection)
}
fn transport(value: &Json) -> Result<Transport, Fault> {
    let r = value.object(
        &["route", "operation", "client_order_id", "code"],
        &[
            "order_id",
            "message",
            "product_id",
            "side",
            "limit_price",
            "size_contracts",
        ],
    )?;
    Ok(Transport {
        route: token(&r["route"], &["REST", "WS"])?,
        operation: token(&r["operation"], &["ORDER", "CANCEL"])?,
        client: r["client_order_id"].id()?,
        code: r["code"].text()?.into(),
        order: optional(r, "order_id").map(Json::id).transpose()?,
        message: optional(r, "message")
            .map(|v| v.text().map(String::from))
            .transpose()?,
        product: optional(r, "product_id").map(Json::id).transpose()?,
        side: optional(r, "side")
            .map(|v| match v.text()? {
                "buy" => Ok(Side::Long),
                "sell" => Ok(Side::Short),
                _ => Err("INVALID_SCHEMA"),
            })
            .transpose()?,
        price: optional(r, "limit_price").map(Json::decimal).transpose()?,
        size: optional(r, "size_contracts")
            .map(Json::decimal)
            .transpose()?,
    })
}
