//! Detached source-average-cost view over the configured owner's actual receipts.
use super::*;

const MODEL_ID: &str = "SOURCE_WEIGHTED_OPEN_COST_V1";
const SEED_FEE_PROVENANCE: &str = "MODELED_CONFIGURED_TAKER_RATE_V1";

#[derive(Clone, Copy)]
struct CostState {
    quantity: Decimal,
    average: Decimal,
}

fn checked(value: Option<Decimal>) -> Result<Decimal, Fault> {
    value.ok_or("NATIVE_INVARIANT")
}

fn signed_quantity(position: Option<&ProductPosition>) -> Result<Decimal, Fault> {
    let Some(position) = position else {
        return Ok(Decimal::ZERO);
    };
    let sign = if position.side == Side::Long {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    position.lots.iter().try_fold(Decimal::ZERO, |sum, lot| {
        if lot.base_quantity <= Decimal::ZERO || lot.source.contracts <= Decimal::ZERO {
            return Err("NATIVE_INVARIANT");
        }
        checked(sum.checked_add(checked(lot.base_quantity.checked_mul(sign))?))
    })
}

fn seed_state(position: Option<&ProductPosition>, fee_rate: Decimal) -> Result<CostState, Fault> {
    let quantity = signed_quantity(position)?;
    if quantity == Decimal::ZERO {
        if position.is_some_and(|p| p.entry_basis != Decimal::ZERO || !p.lots.is_empty()) {
            return Err("NATIVE_INVARIANT");
        }
        return Ok(CostState {
            quantity,
            average: Decimal::ZERO,
        });
    }
    let basis = position.ok_or("NATIVE_INVARIANT")?.entry_basis;
    if basis <= Decimal::ZERO || fee_rate < Decimal::ZERO {
        return Err("NATIVE_INVARIANT");
    }
    let sign = if quantity > Decimal::ZERO {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    let multiplier = checked(Decimal::ONE.checked_add(checked(sign.checked_mul(fee_rate))?))?;
    let average = checked(checked(basis.checked_div(quantity.abs()))?.checked_mul(multiplier))?;
    Ok(CostState { quantity, average })
}

fn apply_receipt(state: &mut CostState, receipt: &SourceCostReceipt) -> Result<(), Fault> {
    let before = signed_quantity(receipt.before.as_ref())?;
    let after = signed_quantity(receipt.after.as_ref())?;
    if before != state.quantity || receipt.price <= Decimal::ZERO || receipt.fee < Decimal::ZERO {
        return Err("NATIVE_INVARIANT");
    }
    let delta = checked(after.checked_sub(before))?;
    if delta == Decimal::ZERO
        || (before.is_sign_positive() != after.is_sign_positive()
            && before != Decimal::ZERO
            && after != Decimal::ZERO)
    {
        return Err("NATIVE_INVARIANT");
    }
    if after == Decimal::ZERO {
        state.average = Decimal::ZERO;
    } else if before == Decimal::ZERO || before.is_sign_positive() == delta.is_sign_positive() {
        let fee_sign = if delta > Decimal::ZERO {
            Decimal::ONE
        } else {
            -Decimal::ONE
        };
        let delta_cost = checked(delta.abs().checked_mul(receipt.price))?;
        let fee_cost = checked(fee_sign.checked_mul(receipt.fee))?;
        let cost = checked(state.quantity.abs().checked_mul(state.average))?;
        let numerator = checked(checked(cost.checked_add(delta_cost))?.checked_add(fee_cost))?;
        state.average = checked(numerator.checked_div(after.abs()))?;
    }
    state.quantity = after;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn d(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    fn projection_row(value: &Value, product: &str) -> Value {
        value["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["product_id"] == product)
            .unwrap()
            .clone()
    }

    #[test]
    fn source_projection_uses_seed_lots_configured_fees_and_is_read_only() {
        let (mut seed, mut products) = configured_tests::input(2);
        seed.cash = d("1000000");
        seed.positions = products
            .iter()
            .enumerate()
            .map(|(index, row)| SeedPosition {
                product: row.product.clone(),
                side: if index == 0 { Side::Long } else { Side::Short },
                contracts: d("3"),
                lots: vec![
                    SeedLot {
                        seed_execution_id: format!("SEED-{index}-0"),
                        seed_sequence: index as u64 * 2,
                        strategy_id: "source-cost-test".into(),
                        contracts: d("1"),
                        entry: d(if index == 0 { "10" } else { "30" }),
                    },
                    SeedLot {
                        seed_execution_id: format!("SEED-{index}-1"),
                        seed_sequence: index as u64 * 2 + 1,
                        strategy_id: "source-cost-test".into(),
                        contracts: d("2"),
                        entry: d(if index == 0 { "20" } else { "40" }),
                    },
                ],
            })
            .collect();
        for (product, (cv, multiplier, rate)) in products
            .iter_mut()
            .zip([("2", "3", "0.01"), ("0.5", "4", "0.02")])
        {
            product.taker_fee = d(rate);
            for spec in &mut product.specs {
                spec.contract_value = d(cv);
                spec.multiplier = d(multiplier);
            }
        }
        let owner = ScenarioAccount::from_configured(&seed, d("10"), products).unwrap();
        let before = owner.clone();
        let first = owner
            .historical_source_cost_projection()
            .unwrap()
            .canonical()
            .unwrap();
        let second = owner
            .historical_source_cost_projection()
            .unwrap()
            .canonical()
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(owner, before);
        let value: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(value["schema_version"], "spider_source_cost_projection_v1");
        assert_eq!(value["model_id"], MODEL_ID);
        assert_eq!(value["account_version"], 0);
        assert_eq!(value["seed_fee_provenance"], SEED_FEE_PROVENANCE);
        assert_eq!(value["rows"].as_array().unwrap().len(), 2);
        assert_eq!(value["rows"][0]["product_id"], "BTC-USDT-SWAP");
        assert_eq!(value["rows"][1]["product_id"], "ETH-USDT-SWAP");
        let long = projection_row(&value, "BTC-USDT-SWAP");
        let short = projection_row(&value, "ETH-USDT-SWAP");
        assert_eq!(long["signed_base_quantity"], "18");
        assert_eq!(short["signed_base_quantity"], "-6");
        assert_eq!(
            long["average_open_price"],
            d("300")
                .checked_div(d("18"))
                .unwrap()
                .checked_mul(d("1.01"))
                .unwrap()
                .normalize()
                .to_string()
        );
        assert_eq!(
            short["average_open_price"],
            d("220")
                .checked_div(d("6"))
                .unwrap()
                .checked_mul(d("0.98"))
                .unwrap()
                .normalize()
                .to_string()
        );
        assert_eq!(long["estimated_exit_fee_rate"], "0.01");
        assert_eq!(short["estimated_exit_fee_rate"], "0.02");
    }
}

impl ScenarioAccount {
    pub(super) fn historical_source_cost_projection(&self) -> Result<wire::Json, Fault> {
        let (scenario, _) = self.btc_context()?;
        let configured = scenario
            .configured
            .as_ref()
            .ok_or("UNSUPPORTED_COST_PROFILE")?;

        let mut receipts = self
            .execution_receipts
            .values()
            .map(execution::CommittedExecution::source_cost_receipt)
            .collect::<Result<Vec<_>, _>>()?;
        receipts.extend(self.transition.source_cost_receipts()?);
        receipts.sort_by_key(|receipt| receipt.sequence);
        let mut last_sequence = None;
        for receipt in &receipts {
            if last_sequence == Some(receipt.sequence) {
                return Err("NATIVE_INVARIANT");
            }
            last_sequence = Some(receipt.sequence);
        }

        let positions = self.positions.btc()?;
        let mut products: Vec<_> = configured.iter().collect();
        products.sort_by(|left, right| left.product.0.cmp(&right.product.0));
        let mut rows = Vec::with_capacity(products.len());
        for product in products {
            let product_receipts: Vec<_> = receipts
                .iter()
                .filter(|receipt| receipt.product == product.product)
                .collect();
            let initial = product_receipts
                .first()
                .map(|receipt| receipt.before.as_ref())
                .unwrap_or_else(|| positions.get(&product.product));
            let mut state = seed_state(initial, product.taker_fee)?;
            for receipt in product_receipts {
                apply_receipt(&mut state, receipt)?;
            }
            let current = signed_quantity(positions.get(&product.product))?;
            if state.quantity != current {
                return Err("NATIVE_INVARIANT");
            }
            rows.push(wire::fields(vec![
                ("product_id", wire::string(product.product.0.as_ref())),
                ("signed_base_quantity", wire::decimal(current)),
                ("average_open_price", wire::decimal(state.average)),
                ("estimated_exit_fee_rate", wire::decimal(product.taker_fee)),
            ]));
        }
        Ok(wire::fields(vec![
            (
                "schema_version",
                wire::string("spider_source_cost_projection_v1"),
            ),
            ("model_id", wire::string(MODEL_ID)),
            ("account_key", wire::account(&self.key)),
            ("account_version", wire::number(self.state_version)?),
            (
                "valuation_context_id",
                wire::hash(self.valuation_context_id),
            ),
            (
                "lifecycle",
                wire::string(inspection::lifecycle(self.transition.lifecycle)),
            ),
            ("seed_fee_provenance", wire::string(SEED_FEE_PROVENANCE)),
            ("rows", wire::Json::Array(rows)),
        ]))
    }
}
