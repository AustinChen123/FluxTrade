use super::{codec, protocol, session::ensure_success};
use anyhow::{ensure, Context, Result};

const FRONT_MONTH_REQUEST: i32 = 113;
const FRONT_MONTH_RESPONSE: i32 = 114;

pub(crate) fn request(request_key: &str, root_symbol: &str, exchange: &str) -> Result<Vec<u8>> {
    for (name, value) in [
        ("request key", request_key),
        ("root symbol", root_symbol),
        ("exchange", exchange),
    ] {
        ensure!(
            !value.trim().is_empty(),
            "Rithmic front-month {name} must not be empty"
        );
    }

    codec::encode(&protocol::RequestFrontMonthContract {
        template_id: FRONT_MONTH_REQUEST,
        user_msg: vec![request_key.to_string()],
        symbol: Some(root_symbol.to_string()),
        exchange: Some(exchange.to_string()),
        need_updates: Some(false),
    })
}

pub(crate) fn decode_response(payload: &[u8], request_key: &str) -> Result<String> {
    ensure!(
        codec::template_id(payload)? == FRONT_MONTH_RESPONSE,
        "unexpected Rithmic front-month response template"
    );
    let response: protocol::ResponseFrontMonthContract = codec::decode(payload)?;
    ensure!(
        response.user_msg.first().map(String::as_str) == Some(request_key),
        "Rithmic front-month response request key mismatch"
    );
    ensure_success(&response.rp_code)?;

    required_text(response.trading_symbol, "trading symbol")
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic front-month {field}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_maps_root_contract_without_subscribing() {
        let payload = request("front-month", "NQ", "CME").unwrap();
        let request: protocol::RequestFrontMonthContract = codec::decode(&payload).unwrap();

        assert_eq!(request.template_id, FRONT_MONTH_REQUEST);
        assert_eq!(request.user_msg, ["front-month"]);
        assert_eq!(request.symbol.as_deref(), Some("NQ"));
        assert_eq!(request.exchange.as_deref(), Some("CME"));
        assert_eq!(request.need_updates, Some(false));
    }

    #[test]
    fn response_validation_matrix_fails_closed() {
        let valid = response(FRONT_MONTH_RESPONSE, "front-month", vec!["0"], Some("NQU6"));
        assert_eq!(decode_response(&valid, "front-month").unwrap(), "NQU6");

        for payload in [
            response(115, "front-month", vec!["0"], Some("NQU6")),
            response(FRONT_MONTH_RESPONSE, "other", vec!["0"], Some("NQU6")),
            response(FRONT_MONTH_RESPONSE, "front-month", vec![], Some("NQU6")),
            response(FRONT_MONTH_RESPONSE, "front-month", vec!["7"], None),
            response(FRONT_MONTH_RESPONSE, "front-month", vec!["0"], None),
        ] {
            assert!(decode_response(&payload, "front-month").is_err());
        }
    }

    fn response(
        template_id: i32,
        request_key: &str,
        rp_code: Vec<&str>,
        trading_symbol: Option<&str>,
    ) -> Vec<u8> {
        codec::encode(&protocol::ResponseFrontMonthContract {
            template_id,
            user_msg: vec![request_key.to_string()],
            rp_code: rp_code.into_iter().map(str::to_string).collect(),
            trading_symbol: trading_symbol.map(str::to_string),
            ..Default::default()
        })
        .unwrap()
    }
}
