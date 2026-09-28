//! Input conversion beside immutable execution templates; no settlement.
use super::super::wire::{optional, Json};
use super::*;

pub(in super::super) fn decode(
    value: &Json,
    account: &AccountKey,
    event_id: &str,
) -> Result<ExecutionCandidate, Fault> {
    let rows = value.object(
        &[
            "namespace",
            "product_id",
            "external_execution_id",
            "order_id",
            "side",
            "price",
            "quantity_contracts",
            "liquidity",
            "matching_effective_at",
            "candidate_id",
            "source_id",
            "visible_at",
            "expected_account_version",
            "expected_order_version",
            "spec_version",
            "rule_data_version",
        ],
        &["fee_asset", "reported_fee"],
    )?;
    if rows["liquidity"].text()? != "SYNTHETIC_TAKER" {
        return Err("INVALID_SCHEMA");
    }
    Ok(ExecutionCandidate {
        template: ExecutionTemplate {
            key: ExternalExecutionKey {
                account: account.clone(),
                namespace: rows["namespace"].id()?,
                product: rows["product_id"].product()?,
                external_id: rows["external_execution_id"].id()?,
            },
            order_id: rows["order_id"].id()?,
            side: rows["side"].side()?,
            price: rows["price"].decimal()?,
            quantity: rows["quantity_contracts"].decimal()?,
            liquidity: LiquidityRole::SyntheticTaker,
            fee_asset: optional(rows, "fee_asset").map(Json::id).transpose()?,
            fee_amount: optional(rows, "reported_fee")
                .map(Json::decimal)
                .transpose()?,
            matching_effective_at: rows["matching_effective_at"].integer(false)?,
        },
        candidate_id: rows["candidate_id"].id()?,
        event_id: event_id.into(),
        source_id: rows["source_id"].id()?,
        visible_at: rows["visible_at"].integer(false)?,
        expected_account_version: rows["expected_account_version"]
            .integer(true)?
            .try_into()
            .map_err(|_| "INVALID_SCHEMA")?,
        expected_order_version: rows["expected_order_version"]
            .integer(true)?
            .try_into()
            .map_err(|_| "INVALID_SCHEMA")?,
        spec_version: rows["spec_version"].id()?,
        rule_data_version: rows["rule_data_version"].id()?,
    })
}
