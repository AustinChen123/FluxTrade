use super::policy::field;
use super::*;
use prost::Message;

fn key() -> CaptureKey {
    CaptureKey {
        bytes: vec![0x5a; 32],
        version: "test_v1".into(),
    }
}

#[test]
fn notification_keeps_exact_negative_bracket_price_and_hashes_identity() {
    let payload = super::super::super::protocol::ExchangeOrderNotification {
        template_id: 352,
        user_tag: Some("client-secret-sentinel".into()),
        price: Some(-3.25),
        basket_id: Some("basket-secret-sentinel".into()),
        ..Default::default()
    }
    .encode_to_vec();
    let capture = capture_fields(352, &payload, &key());
    let text = serde_json::to_string(&capture.fields).unwrap();
    assert!(text.contains("-3.25"));
    assert!(text.contains("raw_bits"));
    assert!(!text.contains("secret-sentinel"));
    assert_eq!(capture.decode_status, "ok");
}

#[test]
fn order_request_response_order_snapshot_and_pnl_allowlisted_scalars_are_retained() {
    let key = key();
    let update = super::super::super::protocol::RithmicOrderNotification {
        template_id: 351,
        price: Some(-2.5),
        quantity: Some(3),
        ..Default::default()
    }
    .encode_to_vec();
    let request = super::super::super::protocol::RequestNewOrder {
        template_id: 312,
        user_tag: Some("client-id-secret".into()),
        symbol: Some("MNQZ6".into()),
        quantity: Some(2),
        price: Some(20001.25),
        ..Default::default()
    }
    .encode_to_vec();
    let response = super::super::super::protocol::ResponseNewOrder {
        template_id: 313,
        user_msg: vec!["request-key-secret".into()],
        rp_code: vec!["provider-text-secret".into()],
        basket_id: Some("basket-id-secret".into()),
        ..Default::default()
    }
    .encode_to_vec();
    let pnl = super::super::super::protocol::InstrumentPnLPositionUpdate {
        template_id: 450,
        day_pnl: Some(-8.75),
        net_quantity: Some(1),
        ..Default::default()
    }
    .encode_to_vec();

    let records = [
        capture_fields(351, &update, &key),
        capture_fields(312, &request, &key),
        capture_fields(313, &response, &key),
        capture_fields(450, &pnl, &key),
    ];
    let text = serde_json::to_string(&records).unwrap();
    assert!(text.contains("-2.5"));
    assert!(text.contains("20001.25"));
    assert!(text.contains("MNQZ6"));
    assert!(text.contains("-8.75"));
    assert!(!text.contains("secret"));
    assert!(!text.contains("provider-text"));
    assert!(text.contains("hmac_sha256"));
}

#[test]
fn unknown_text_is_counted_and_never_copied() {
    let payload = super::super::super::protocol::ResponsePnLPositionSnapshot {
        template_id: 403,
        user_msg: vec!["opaque-sentinel".into()],
        rp_code: vec!["provider-text-sentinel".into()],
    }
    .encode_to_vec();
    let capture = capture_fields(403, &payload, &key());
    let text = serde_json::to_string(&capture.fields).unwrap();
    assert!(!text.contains("sentinel"));
    assert!(capture.omitted_field_count >= 1);
    assert!(capture.fields.is_empty());
    // A field whose number is safe in other templates remains unknown in
    // this template, even when its bytes look like ordinary ASCII.
    let mut with_unknown = payload;
    with_unknown.extend_from_slice(&[0xa2, 0xe1, 0x35, 0x13]); // field 110100, len 19
    with_unknown.extend_from_slice(b"PASSWORDSENTINEL!!!");
    let capture = capture_fields(403, &with_unknown, &key());
    let text = serde_json::to_string(&capture.fields).unwrap();
    assert!(!text.contains("PASSWORDSENTINEL"));
    assert!(capture.fields.is_empty());
}

#[test]
fn oversized_field_number_cannot_alias_a_safe_text_field() {
    // A field number above protobuf's maximum that truncates to 110100.
    let oversized = ((1u64 << 32) + 110100) << 3 | 2;
    let mut payload = vec![0x98, 0xb6, 0x4b, 0x93, 0x03]; // template id 403
    let mut value = oversized;
    while value >= 0x80 {
        payload.push((value as u8) | 0x80);
        value >>= 7;
    }
    payload.push(value as u8);
    payload.extend_from_slice(&[3, b'B', b'A', b'D']);
    let capture = capture_fields(403, &payload, &key());
    assert_eq!(capture.decode_status, "malformed");
    assert!(capture.fields.is_empty());
}

#[test]
fn each_template_rejects_fields_reviewed_only_for_other_templates() {
    for (template, field_number) in [
        (403, 110100),
        (313, 110100),
        (315, 110101),
        (317, 150100),
        (321, 150101),
        (331, 157124),
        (3505, 110100),
        (312, 157124),
        (314, 157124),
        (351, 157124),
        (352, 157124),
        (450, 157124),
        (451, 157954),
    ] {
        assert!(
            field(template, field_number).is_none(),
            "{template}/{field_number}"
        );
    }
}

#[test]
fn template_allowlists_omit_user_text_and_schema_absent_fields() {
    for template in [308, 312, 314, 316, 320, 330, 3504, 351, 352, 402, 450, 451] {
        assert!(
            field(template, 132760).is_none(),
            "user_msg/template={template}"
        );
    }
    for (template, field_number) in [
        (351, 153647),  // only ExchangeOrderNotification has TP exchange id
        (352, 112027),  // only RithmicOrderNotification has release-pending quantity
        (314, 154119),  // modify requests have no user tag
        (316, 110100),  // cancel requests carry no symbol
        (316, 110101),  // cancel requests carry no exchange
        (316, 154119),  // cancel requests have no user tag
        (3504, 154119), // exit requests have no user tag
        (450, 132760),  // instrument PNL updates have no user message
        (451, 100749),  // only instrument PNL updates have a product code
        (451, 154434),  // account PNL updates have no average open fill price
    ] {
        assert!(
            field(template, field_number).is_none(),
            "schema-absent field {field_number}/template={template}"
        );
    }
}

#[test]
fn bracket_trailing_trigger_ticks_remain_signed_integers() {
    for ticks in [0, 5, -5] {
        let payload = super::super::super::protocol::RequestBracketOrder {
            template_id: 330,
            trailing_stop_trigger_ticks: Some(ticks),
            ..Default::default()
        }
        .encode_to_vec();
        let capture = capture_fields(330, &payload, &key());
        let tick = capture.fields.iter().find(|value| {
            value.get("name").and_then(Value::as_str) == Some("trailing_stop_trigger_ticks")
        });
        assert_eq!(
            tick.and_then(|value| value.get("value"))
                .and_then(Value::as_i64),
            Some(i64::from(ticks))
        );
    }
}

#[test]
fn template_451_string_pnl_is_not_misread_as_template_450_float() {
    let payload = super::super::super::protocol::AccountPnLPositionUpdate {
        template_id: 451,
        day_pnl: Some("-10.25".into()),
        ..Default::default()
    }
    .encode_to_vec();
    let capture = capture_fields(451, &payload, &key());
    assert!(!serde_json::to_string(&capture.fields)
        .unwrap()
        .contains("day_pnl"));
}

#[test]
fn template_450_retains_the_exact_float_bits() {
    let price = -8.75_f64;
    let payload = super::super::super::protocol::InstrumentPnLPositionUpdate {
        template_id: 450,
        day_pnl: Some(price),
        ..Default::default()
    }
    .encode_to_vec();
    let capture = capture_fields(450, &payload, &key());
    let value = capture
        .fields
        .iter()
        .find(|value| value.get("name").and_then(Value::as_str) == Some("day_pnl"));
    assert_eq!(
        value
            .and_then(|value| value.get("raw_bits"))
            .and_then(Value::as_str),
        Some(format!("0x{:016x}", price.to_bits()).as_str())
    );
}

#[test]
fn malformed_wire_clears_partial_values() {
    let capture = capture_fields(352, &[0x80], &key());
    assert_eq!(capture.decode_status, "malformed");
    assert!(capture.fields.is_empty());
}
