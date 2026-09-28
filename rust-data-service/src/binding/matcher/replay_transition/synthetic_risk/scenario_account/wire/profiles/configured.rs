use super::*;

pub(super) fn construct(input: &str, key: AccountKey) -> Result<ScenarioAccount, Fault> {
    let value = super::super::decode(input).map_err(|_| "INVALID_SCHEMA")?;
    let rows = value.object(
        &[
            "schema_version",
            "config_id",
            "seed_effective_at",
            "cash",
            "leverage",
            "products",
            "positions",
            "orders",
        ],
        &[],
    )?;
    if rows["schema_version"].text()? != "synthetic_multi_product_config_v1"
        || !rows["positions"].array()?.is_empty()
        || !rows["orders"].array()?.is_empty()
    {
        return Err("INVALID_SCHEMA");
    }
    let seed = CleanSeed {
        key,
        config_id: rows["config_id"].id()?,
        effective_at: rows["seed_effective_at"].integer(false)?,
        cash: rows["cash"].decimal()?,
        positions: Vec::new(),
        orders: Vec::new(),
    };
    let products = rows["products"]
        .array()?
        .iter()
        .map(product)
        .collect::<Result<Vec<_>, _>>()?;
    ScenarioAccount::from_configured_empty(&seed, rows["leverage"].decimal()?, products)
        .map_err(|_| "INVALID_SCHEMA")
}

fn product(value: &Json) -> Result<ConfiguredProduct, Fault> {
    let rows = value.object(
        &[
            "product_id",
            "instrument_code",
            "taker_fee_rate",
            "liquidation_fee_rate",
            "specs",
            "tiers",
            "marks",
        ],
        &[],
    )?;
    let product = Product(rows["product_id"].id()?.into());
    Ok(ConfiguredProduct {
        product: product.clone(),
        instrument_code: rows["instrument_code"].integer(false)?,
        taker_fee: rows["taker_fee_rate"].decimal()?,
        liquidation_fee: rows["liquidation_fee_rate"].decimal()?,
        specs: rows["specs"]
            .array()?
            .iter()
            .map(|v| spec(v, &product))
            .collect::<Result<_, _>>()?,
        tiers: rows["tiers"]
            .array()?
            .iter()
            .map(|v| tier_version(v, &product))
            .collect::<Result<_, _>>()?,
        marks: rows["marks"]
            .array()?
            .iter()
            .map(|v| mark(v, &product))
            .collect::<Result<_, _>>()?,
    })
}
fn interval(rows: &std::collections::BTreeMap<String, Json>) -> Result<Interval, Fault> {
    Ok(Interval {
        from: rows["valid_from"].integer(false)?,
        to: match &rows["valid_to"] {
            Json::Null => None,
            value => Some(value.integer(false)?),
        },
    })
}
fn spec(value: &Json, product: &Product) -> Result<Spec, Fault> {
    let rows = value.object(
        &[
            "version",
            "valid_from",
            "valid_to",
            "contract_value",
            "multiplier",
            "price_tick",
            "quantity_step",
            "minimum_quantity",
        ],
        &[],
    )?;
    Ok(Spec {
        product: product.clone(),
        version: rows["version"].id()?,
        interval: interval(rows)?,
        contract_value: rows["contract_value"].decimal()?,
        multiplier: rows["multiplier"].decimal()?,
        tick: rows["price_tick"].decimal()?,
        lot: rows["quantity_step"].decimal()?,
        minimum: rows["minimum_quantity"].decimal()?,
    })
}
fn tier_version(value: &Json, product: &Product) -> Result<TierVersion, Fault> {
    let rows = value.object(&["version", "valid_from", "valid_to", "rows"], &[])?;
    Ok(TierVersion {
        product: product.clone(),
        version: rows["version"].id()?,
        interval: interval(rows)?,
        tiers: rows["rows"]
            .array()?
            .iter()
            .map(tier_row)
            .collect::<Result<_, _>>()?,
    })
}
fn tier_row(value: &Json) -> Result<Tier, Fault> {
    let rows = value.object(
        &[
            "minimum_contracts",
            "maximum_contracts",
            "mmr",
            "imr",
            "max_leverage",
        ],
        &[],
    )?;
    Ok(Tier {
        minimum: rows["minimum_contracts"].decimal()?,
        maximum: rows["maximum_contracts"].decimal()?,
        mmr: rows["mmr"].decimal()?,
        imr: rows["imr"].decimal()?,
        max_leverage: rows["max_leverage"].decimal()?,
    })
}
fn mark(value: &Json, product: &Product) -> Result<Mark, Fault> {
    let rows = value.object(&["valid_from", "valid_to", "mark"], &[])?;
    Ok(Mark {
        product: product.clone(),
        valid_from: rows["valid_from"].integer(false)?,
        valid_to: rows["valid_to"].integer(false)?,
        price: rows["mark"].decimal()?,
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    fn product(i: usize) -> String {
        format!(
            r#"{{"product_id":"ASSET-{i}","instrument_code":{},"taker_fee_rate":"0.001","liquidation_fee_rate":"0.002","specs":[{{"version":"spec-{i}-a","valid_from":0,"valid_to":40,"contract_value":"1","multiplier":"1","price_tick":"0.1","quantity_step":"0.01","minimum_quantity":"0.01"}},{{"version":"spec-{i}-b","valid_from":40,"valid_to":null,"contract_value":"1","multiplier":"1","price_tick":"0.1","quantity_step":"0.01","minimum_quantity":"0.01"}}],"tiers":[{{"version":"tier-{i}-a","valid_from":0,"valid_to":40,"rows":[{{"minimum_contracts":"0","maximum_contracts":"100","mmr":"0.01","imr":"0.02","max_leverage":"10"}}]}},{{"version":"tier-{i}-b","valid_from":40,"valid_to":null,"rows":[{{"minimum_contracts":"0","maximum_contracts":"100","mmr":"0.01","imr":"0.02","max_leverage":"10"}}]}}],"marks":[{{"valid_from":0,"valid_to":40,"mark":"1"}},{{"valid_from":40,"valid_to":100,"mark":"2"}}]}}"#,
            i + 1
        )
    }

    fn config(count: usize, reverse: bool) -> String {
        let products = (0..count)
            .map(|i| if reverse { count - i - 1 } else { i })
            .map(product)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"schema_version":"synthetic_multi_product_config_v1","config_id":"cfg-a","seed_effective_at":50,"cash":"1000","leverage":"5","products":[{products}],"positions":[],"orders":[]}}"#
        )
    }

    fn key() -> AccountKey {
        AccountKey {
            venue: "test-venue".into(),
            environment: "paper".into(),
            account: "account-a".into(),
            subaccount: None,
        }
    }

    fn edit(input: &str, old: &str, new: &str) -> String {
        assert!(input.contains(old), "missing fixture token: {old}");
        input.replacen(old, new, 1)
    }

    fn rejected(input: String) {
        assert!(serde_json::from_str::<Box<serde_json::value::RawValue>>(&input).is_ok());
        assert_eq!(construct(&input, key()), Err("INVALID_SCHEMA"));
    }

    #[test]
    fn accepts_product_counts_preserves_array_order_and_ignores_object_key_order() {
        for count in [1, 2, 12] {
            let owner = construct(&config(count, false), key()).unwrap();
            assert!(owner.positions.is_empty() && owner.orders.is_empty());
            assert_eq!(owner.key, key());
            assert_eq!(owner.seed_effective_at, 50);
            assert_eq!(owner.cash, Decimal::from(1000));
            assert_eq!(owner.reservation().unwrap().products.len(), count);
        }
        let input = config(2, false);
        let moved = edit(&input, r#""config_id":"cfg-a","#, r#""#);
        let moved = edit(
            &moved,
            r#""orders":[]}"#,
            r#""orders":[],"config_id":"cfg-a"}"#,
        );
        let ordered = construct(&input, key()).unwrap();
        let reordered = construct(&moved, key()).unwrap();
        assert_eq!(ordered.valuation_context_id, reordered.valuation_context_id);
        let reversed = construct(&config(2, true), key()).unwrap();
        assert_ne!(ordered.valuation_context_id, reversed.valuation_context_id);
        assert_eq!(
            reversed.reservation().unwrap().products[0].product,
            Product("ASSET-1".into())
        );
    }

    #[test]
    fn strict_objects_reject_missing_unknown_duplicate_and_wrong_typed_fields() {
        let input = config(1, false);
        let object_markers = [
            (r#""config_id":"cfg-a""#, r#""config_id":"cfg-a","#),
            (r#""product_id":"ASSET-0""#, r#""product_id":"ASSET-0","#),
            (r#""version":"spec-0-a""#, r#""version":"spec-0-a","#),
            (r#""version":"tier-0-a""#, r#""version":"tier-0-a","#),
            (r#""minimum_contracts":"0""#, r#""minimum_contracts":"0","#),
            (r#""mark":"1""#, r#","mark":"1""#),
        ];
        for (pair, missing) in object_markers {
            rejected(edit(&input, missing, ""));
            let unknown = format!("{pair},\"extra\":null");
            rejected(edit(&input, pair, &unknown));
            let duplicate = format!("{pair},{pair}");
            rejected(edit(&input, pair, &duplicate));
        }
        for (old, new) in [
            (
                r#""schema_version":"synthetic_multi_product_config_v1""#,
                r#""schema_version":1"#,
            ),
            (r#""seed_effective_at":50"#, r#""seed_effective_at":"50""#),
            (r#""cash":"1000""#, r#""cash":1000"#),
            (r#""leverage":"5""#, r#""leverage":[]"#),
            (r#""instrument_code":1"#, r#""instrument_code":"1""#),
            (r#""valid_from":0"#, r#""valid_from":false"#),
            (r#""valid_to":null"#, r#""valid_to":false"#),
            (r#""contract_value":"1""#, r#""contract_value":1"#),
            (
                r#""rows":[{"minimum_contracts":"0","maximum_contracts":"100","mmr":"0.01","imr":"0.02","max_leverage":"10"}]"#,
                r#""rows":{"minimum_contracts":"0","maximum_contracts":"100","mmr":"0.01","imr":"0.02","max_leverage":"10"}"#,
            ),
            (r#""mmr":"0.01""#, r#""mmr":0.01"#),
            (r#""mark":"1""#, r#""mark":"1.0""#),
            (r#""valid_to":100"#, r#""valid_to":null"#),
            (r#""positions":[]"#, r#""positions":[{}]"#),
            (r#""orders":[]"#, r#""orders":null"#),
        ] {
            rejected(edit(&input, old, new));
        }
        rejected(edit(
            &input,
            r#""taker_fee_rate":"0.001""#,
            r#""taker_fee_rate":"0.0010""#,
        ));
    }

    #[test]
    fn constructor_semantics_fail_closed_for_products_timelines_and_seed_coverage() {
        let input = config(2, false);
        for (old, new) in [
            (r#""product_id":"ASSET-1""#, r#""product_id":"ASSET-0""#),
            (r#""instrument_code":2"#, r#""instrument_code":1"#),
            (r#""valid_to":40"#, r#""valid_to":39"#),
        ] {
            rejected(edit(&input, old, new));
        }
        let uncovered = edit(
            &input,
            r#""valid_from":40,"valid_to":100"#,
            r#""valid_from":60,"valid_to":100"#,
        );
        rejected(uncovered);
    }
}
