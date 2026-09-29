//! Input conversion beside immutable execution templates; no settlement.
use super::super::wire::{optional, Json};
use super::*;

#[cfg(test)]
mod output_tests {
    use super::*;
    #[test]
    fn golden_saved_alias_and_checked_version() {
        let (seed, _, _) = super::super::super::tests::fixture();
        let mut owner = super::super::super::wire::profiles::construct(
            "SYNTHETIC_GOLDEN_CANCEL_V1",
            seed.key,
            None,
        )
        .unwrap();
        admission::inspection_tests::golden_admit(&mut owner);
        let id = owner.orders.keys().next().unwrap().clone();
        owner
            .execute(&commit::tests::input(&owner, &id, "G", "4", "10"))
            .unwrap();
        let before = owner.clone();
        let r = owner.execution_receipts.values().next().unwrap();
        assert_eq!(owner, before);
        let mut bad = r.clone();
        bad.state_version_after = i64::MAX as u64 + 1;
        assert_eq!(bad.delivery_json("0000015000"), Err("NATIVE_INVARIANT"));
        bad.state_version_after = 2;
        bad.order_after.status = "OPEN".into();
        assert_eq!(bad.delivery_json("0000015000"), Err("NATIVE_INVARIANT"));
    }
}

impl CommittedExecution {
    pub(in super::super) fn delivery_json(&self, client: &str) -> Result<Json, Fault> {
        use super::super::wire::{decimal, fields, number, string};
        let o = &self.order_after;
        let state = match o.status.as_str() {
            "PARTIALLY_FILLED" => "partially_filled",
            "FILLED" => "filled",
            _ => return Err("NATIVE_INVARIANT"),
        };
        Ok(fields(vec![
            ("order_id", string(&o.order_id)),
            ("owner_client_order_id", string(&o.client_id)),
            ("policy_client_order_id", string(client)),
            ("product_id", string(o.product.canonical_id())),
            ("state", string(state)),
            ("side", string(delivery::side(o.side))),
            ("limit_price", decimal(o.price)),
            ("fill_price", decimal(self.price)),
            ("original_size_contracts", decimal(o.original)),
            ("cumulative_filled_size_contracts", decimal(o.filled)),
            ("contract_value", decimal(self.contract_value)),
            (
                "execution_effective_at",
                number(self.execution_effective_at)?,
            ),
            ("commit_account_version", number(self.state_version_after)?),
            ("spec_version", string(&self.spec_version)),
            ("rule_data_version", string(&self.rule_data_version)),
        ]))
    }
}

pub(in super::super) fn decode(
    value: &Json,
    owner: &ScenarioAccount,
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
                account: owner.key.clone(),
                namespace: rows["namespace"].id()?,
                product: owner.resolve_wire_product(&rows["product_id"])?,
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
        canonical_execution_id: None,
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
