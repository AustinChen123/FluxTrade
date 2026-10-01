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
    if rows["schema_version"].text()? != "synthetic_multi_product_config_v1" {
        return Err("INVALID_SCHEMA");
    }
    let seed = CleanSeed {
        key,
        config_id: rows["config_id"].id()?,
        effective_at: rows["seed_effective_at"].integer(false)?,
        cash: rows["cash"].decimal()?,
        positions: rows["positions"]
            .array()?
            .iter()
            .map(position)
            .collect::<Result<_, _>>()?,
        orders: rows["orders"]
            .array()?
            .iter()
            .map(order)
            .collect::<Result<_, _>>()?,
    };
    let products = rows["products"]
        .array()?
        .iter()
        .map(product)
        .collect::<Result<Vec<_>, _>>()?;
    ScenarioAccount::from_configured(&seed, rows["leverage"].decimal()?, products)
        .map_err(|_| "INVALID_SCHEMA")
}

fn position(value: &Json) -> Result<SeedPosition, Fault> {
    let rows = value.object(&["product_id", "side", "quantity_contracts", "lots"], &[])?;
    Ok(SeedPosition {
        product: Product(rows["product_id"].id()?.into()),
        side: rows["side"].side()?,
        contracts: rows["quantity_contracts"].decimal()?,
        lots: rows["lots"]
            .array()?
            .iter()
            .map(seed_lot)
            .collect::<Result<_, _>>()?,
    })
}
fn seed_lot(value: &Json) -> Result<SeedLot, Fault> {
    let rows = value.object(
        &[
            "seed_execution_id",
            "seed_sequence",
            "strategy_id",
            "quantity_contracts",
            "entry_price",
        ],
        &[],
    )?;
    Ok(SeedLot {
        seed_execution_id: rows["seed_execution_id"].id()?,
        seed_sequence: u64::try_from(rows["seed_sequence"].integer(true)?)
            .map_err(|_| "INVALID_SCHEMA")?,
        strategy_id: rows["strategy_id"].id()?,
        contracts: rows["quantity_contracts"].decimal()?,
        entry: rows["entry_price"].decimal()?,
    })
}
fn order(value: &Json) -> Result<SeedOrder, Fault> {
    let rows = value.object(
        &[
            "intent_id",
            "order_id",
            "client_order_id",
            "strategy_id",
            "product_id",
            "side",
            "limit_price",
            "reduce_only",
            "original_quantity_contracts",
            "filled_quantity_contracts",
            "canceled_quantity_contracts",
            "remaining_quantity_contracts",
            "status",
        ],
        &[],
    )?;
    Ok(SeedOrder {
        intent_id: rows["intent_id"].id()?,
        order_id: rows["order_id"].id()?,
        client_id: rows["client_order_id"].id()?,
        strategy_id: rows["strategy_id"].id()?,
        product: ProfileProduct::BtcEth(Product(rows["product_id"].id()?.into())),
        side: rows["side"].side()?,
        price: rows["limit_price"].decimal()?,
        reduce_only: rows["reduce_only"].boolean()?,
        original: rows["original_quantity_contracts"].decimal()?,
        filled: rows["filled_quantity_contracts"].decimal()?,
        canceled: rows["canceled_quantity_contracts"].decimal()?,
        remaining: rows["remaining_quantity_contracts"].decimal()?,
        status: rows["status"].text()?.into(),
    })
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

    fn lot(id: &str, sequence: &str, quantity: &str, entry: &str) -> String {
        format!(
            r#"{{"seed_execution_id":"{id}","seed_sequence":{sequence},"strategy_id":"strategy-a","quantity_contracts":"{quantity}","entry_price":"{entry}"}}"#
        )
    }
    fn position(product: &str, side: &str, quantity: &str, lots: &[String]) -> String {
        format!(
            r#"{{"product_id":"{product}","side":"{side}","quantity_contracts":"{quantity}","lots":[{}]}}"#,
            lots.join(",")
        )
    }
    fn seed_order(
        id: &str,
        product: &str,
        side: &str,
        reduce: bool,
        original: &str,
        filled: &str,
        remaining: &str,
        status: &str,
    ) -> String {
        format!(
            r#"{{"intent_id":"intent-{id}","order_id":"order-{id}","client_order_id":"client-{id}","strategy_id":"strategy-a","product_id":"{product}","side":"{side}","limit_price":"2","reduce_only":{reduce},"original_quantity_contracts":"{original}","filled_quantity_contracts":"{filled}","canceled_quantity_contracts":"0","remaining_quantity_contracts":"{remaining}","status":"{status}"}}"#
        )
    }
    fn seeded(input: &str, positions: &[String], orders: &[String]) -> String {
        edit(
            input,
            r#""positions":[],"orders":[]"#,
            &format!(
                "\"positions\":[{}],\"orders\":[{}]",
                positions.join(","),
                orders.join(",")
            ),
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

    fn reject_position(input: &str, position: String) {
        rejected(seeded(input, &[position], &[]));
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
    fn seeded_positions_orders_and_fifo_are_canonical() {
        let input = config(2, false);
        let a = vec![
            lot("exec-a", "0", "0.5", "2"),
            lot("exec-b", "1", "0.5", "2"),
        ];
        let b = vec![lot("exec-c", "2", "0.5", "2")];
        let positions = vec![
            position("ASSET-0", "LONG", "1", &a),
            position("ASSET-1", "SHORT", "0.5", &b),
        ];
        let orders = vec![
            seed_order("open", "ASSET-0", "LONG", false, "0.5", "0", "0.5", "OPEN"),
            seed_order(
                "partial",
                "ASSET-1",
                "LONG",
                true,
                "0.5",
                "0.2",
                "0.3",
                "PARTIALLY_FILLED",
            ),
        ];
        let owner = construct(&seeded(&input, &positions, &orders), key()).unwrap();
        let reversed = construct(
            &seeded(
                &input,
                &[
                    positions[1].clone(),
                    position("ASSET-0", "LONG", "1", &[a[1].clone(), a[0].clone()]),
                ],
                &[orders[1].clone(), orders[0].clone()],
            ),
            key(),
        )
        .unwrap();
        assert_eq!(owner, reversed);
        let evidence = |owner: &ScenarioAccount| {
            (
                owner.reservation().unwrap(),
                owner.inspect_state().unwrap().wire_json().unwrap(),
            )
        };
        assert_eq!(evidence(&owner), evidence(&reversed));
        let positions = reversed.positions.btc().unwrap();
        assert_eq!(positions[&Product("ASSET-0".into())].side, Side::Long);
        assert_eq!(positions[&Product("ASSET-1".into())].side, Side::Short);
        assert_eq!(
            positions[&Product("ASSET-0".into())]
                .lots
                .iter()
                .map(|lot| lot.source.seed_sequence)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(reversed.orders["order-open"].facts.status, "OPEN");
        assert_eq!(
            reversed.orders["order-partial"].facts.status,
            "PARTIALLY_FILLED"
        );
    }
    #[test]
    fn seed_wire_errors_fail_before_owner_exposure() {
        let input = config(2, false);
        let lots = vec![lot("exec-a", "0", "1", "2")];
        let position_json = position("ASSET-0", "LONG", "1", &lots);
        let order = seed_order("open", "ASSET-0", "LONG", false, "0.5", "0", "0.5", "OPEN");
        let base = seeded(
            &input,
            std::slice::from_ref(&position_json),
            std::slice::from_ref(&order),
        );
        for (old, new) in [
            (r#""side":"LONG""#, r#""side":"BUY""#),
            (r#""status":"OPEN""#, r#""status":"NEW""#),
            (
                r#""original_quantity_contracts":"0.5""#,
                r#""original_quantity_contracts":"0.6""#,
            ),
            (r#""reduce_only":false"#, r#""reduce_only":true"#),
            (r#""entry_price":"2""#, r#""entry_price":"2000""#),
        ] {
            rejected(edit(&base, old, new));
        }
        reject_position(&input, position("UNKNOWN", "LONG", "1", &lots));
        for (quantity, lots) in [
            ("1", vec![lot("exec-a", "0", "0.5", "2")]),
            ("1", vec![lot("exec-a", "1", "1", "2")]),
            ("1", vec![lot("exec-a", "-1", "1", "2")]),
            ("1", vec![lot("exec-a", "9223372036854775808", "1", "2")]),
            (
                "1",
                vec![
                    lot("exec-a", "0", "0.5", "2"),
                    lot("exec-a", "1", "0.5", "2"),
                ],
            ),
            (
                "1",
                vec![
                    lot("exec-a", "0", "0.5", "2"),
                    lot("exec-b", "0", "0.5", "2"),
                ],
            ),
        ] {
            reject_position(&input, position("ASSET-0", "LONG", quantity, &lots));
        }
        let second = seed_order(
            "second", "ASSET-0", "LONG", false, "0.5", "0", "0.5", "OPEN",
        );
        for (field, prefix) in [
            ("intent_id", "intent"),
            ("order_id", "order"),
            ("client_order_id", "client"),
        ] {
            let duplicate = second.replace(
                &format!(r#""{field}":"{prefix}-second""#),
                &format!(r#""{field}":"{prefix}-open""#),
            );
            rejected(seeded(
                &input,
                std::slice::from_ref(&position_json),
                &[order.clone(), duplicate],
            ));
        }
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
