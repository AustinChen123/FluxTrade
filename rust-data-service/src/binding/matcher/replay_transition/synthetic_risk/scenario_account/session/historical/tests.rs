use super::*;
use serde_json::{json, Value};

fn configured_session() -> Session {
    let configuration = r#"{"schema_version":"synthetic_multi_product_config_v1","config_id":"codec-test","seed_effective_at":50,"cash":"10","leverage":"1","products":[{"product_id":"WIRE-X","instrument_code":1,"taker_fee_rate":"0","liquidation_fee_rate":"0","specs":[{"version":"s1","valid_from":0,"valid_to":null,"contract_value":"1","multiplier":"1","price_tick":"1","quantity_step":"1","minimum_quantity":"1"}],"tiers":[{"version":"t1","valid_from":0,"valid_to":null,"rows":[{"minimum_contracts":"0","maximum_contracts":"100","mmr":"0.005","imr":"0.1","max_leverage":"10"}]}],"marks":[{"valid_from":0,"valid_to":9223372036854775807,"mark":"1"}]}],"positions":[],"orders":[]}"#;
    Session::new(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        r#"{"venue":"okx-scenario","environment":"test","account":"A"}"#,
        Some(configuration),
    )
    .unwrap()
}

fn node() -> Value {
    json!({
        "schema_version":"historical_node_v1",
        "model_id":"OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
        "model_version":"1",
        "run_contract_hash":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "bar_open_ms":3,
        "bar_duration_ms":60000,
        "step_index":0,
        "market_slippage_bps":"0",
        "bars":[{"product_id":"WIRE-X",
            "trade":{"open":"1","high":"1","low":"1","close":"1","volume_contracts":"0","confirmed":true,"source_row_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "mark":{"open":"1","high":"1","low":"1","close":"1","confirmed":true,"source_row_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}],
        "working_orders":[]
    })
}

#[test]
fn strict_node_wire_routes_through_configured_owner_and_keeps_rejections_read_only() {
    let mut session = configured_session();
    let before = session.owner.clone();
    let mut invalid = node();
    invalid["bars"][0]["product_id"] = json!("UNKNOWN");
    assert_eq!(
        session.historical_market_step(&invalid.to_string()),
        Err(BoundaryError::Input("INVALID_SCHEMA"))
    );
    assert_eq!(session.owner, before);

    let mut invalid = node();
    invalid["extra"] = json!(true);
    assert_eq!(
        session.historical_market_step(&invalid.to_string()),
        Err(BoundaryError::Input("INVALID_SCHEMA"))
    );
    assert_eq!(session.owner, before);

    let result: Value =
        serde_json::from_str(&session.historical_market_step(&node().to_string()).unwrap())
            .unwrap();
    assert_eq!(result["schema_version"], "historical_node_result_v1");
    assert_eq!(result["raw_time_ms"], 3);
    assert_eq!(result["effective_at"], 55);
    assert_eq!(result["products"][0]["product_id"], "WIRE-X");
    assert!(result["products"][0]["fills"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(
        result["owner_evidence"]["schema_version"],
        "inspect_state_v1"
    );
}
