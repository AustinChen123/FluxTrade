use super::{codec, protocol, session::ensure_success};
use anyhow::{ensure, Context, Result};

const FRONT_MONTH_REQUEST: i32 = 113;
const FRONT_MONTH_RESPONSE: i32 = 114;
const FRONT_MONTH_UPDATE: i32 = 159;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrontMonthEvent {
    CurrentVerified,
    RolloverRequired(String),
}

pub(crate) fn request(request_key: &str, root_symbol: &str, exchange: &str) -> Result<Vec<u8>> {
    request_with_updates(request_key, root_symbol, exchange, false)
}

pub(crate) fn request_with_updates(
    request_key: &str,
    root_symbol: &str,
    exchange: &str,
    need_updates: bool,
) -> Result<Vec<u8>> {
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
        need_updates: Some(need_updates),
    })
}

pub(crate) fn decode_live_event(
    payload: &[u8],
    request_key: &str,
    root_symbol: &str,
    exchange: &str,
    configured_symbol: &str,
) -> Result<Option<FrontMonthEvent>> {
    let trading_symbol = match codec::template_id(payload)? {
        FRONT_MONTH_RESPONSE => Some(decode_response(
            payload,
            request_key,
            root_symbol,
            exchange,
        )?),
        FRONT_MONTH_UPDATE => {
            let update: protocol::FrontMonthContractUpdate = codec::decode(payload)?;
            ensure_required_identity(update.symbol.as_deref(), root_symbol, "root symbol")?;
            ensure_required_identity(update.exchange.as_deref(), exchange, "exchange")?;
            ensure_required_identity(
                update.trading_exchange.as_deref(),
                exchange,
                "trading exchange",
            )?;
            ensure_confirmed_front_month_status(update.is_front_month_symbol)?;
            Some(validated_trading_symbol(
                update.trading_symbol,
                root_symbol,
            )?)
        }
        _ => None,
    };
    let Some(trading_symbol) = trading_symbol else {
        return Ok(None);
    };
    ensure!(
        !configured_symbol.trim().is_empty(),
        "configured Rithmic trading symbol must not be empty"
    );
    Ok(Some(
        if trading_symbol.eq_ignore_ascii_case(configured_symbol) {
            FrontMonthEvent::CurrentVerified
        } else {
            FrontMonthEvent::RolloverRequired(trading_symbol)
        },
    ))
}

pub(crate) fn decode_response(
    payload: &[u8],
    request_key: &str,
    root_symbol: &str,
    exchange: &str,
) -> Result<String> {
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
    ensure_optional_identity(response.symbol.as_deref(), root_symbol, "root symbol")?;
    ensure_optional_identity(response.exchange.as_deref(), exchange, "exchange")?;
    ensure_optional_identity(
        response.trading_exchange.as_deref(),
        exchange,
        "trading exchange",
    )?;
    ensure_confirmed_front_month_status(response.is_front_month_symbol)?;
    validated_trading_symbol(response.trading_symbol, root_symbol)
}

fn ensure_confirmed_front_month_status(status: Option<bool>) -> Result<()> {
    ensure!(
        status == Some(true),
        "Rithmic response did not confirm front-month status"
    );
    Ok(())
}

fn ensure_optional_identity(actual: Option<&str>, expected: &str, field: &str) -> Result<()> {
    ensure!(
        !expected.trim().is_empty(),
        "missing expected Rithmic {field}"
    );
    if let Some(actual) = actual {
        ensure!(
            actual.eq_ignore_ascii_case(expected),
            "Rithmic front-month {field} mismatch"
        );
    }
    Ok(())
}

fn ensure_required_identity(actual: Option<&str>, expected: &str, field: &str) -> Result<()> {
    let actual = actual.with_context(|| format!("missing Rithmic front-month {field}"))?;
    ensure_optional_identity(Some(actual), expected, field)
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic front-month {field}"))
}

fn validated_trading_symbol(value: Option<String>, root_symbol: &str) -> Result<String> {
    ensure!(
        !root_symbol.is_empty() && root_symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()),
        "invalid expected Rithmic root symbol"
    );
    let symbol = required_text(value, "trading symbol")?;
    ensure!(
        symbol.bytes().all(|byte| byte.is_ascii_alphanumeric())
            && symbol.len() == root_symbol.len() + 2,
        "invalid Rithmic front-month trading symbol"
    );
    let (actual_root, suffix) = symbol.split_at(root_symbol.len());
    ensure!(
        actual_root.eq_ignore_ascii_case(root_symbol)
            && matches!(
                suffix.as_bytes(),
                [month, year]
                    if matches!(
                        month.to_ascii_uppercase(),
                        b'F' | b'G' | b'H' | b'J' | b'K' | b'M' | b'N' | b'Q' | b'U' | b'V' | b'X' | b'Z'
                    ) && year.is_ascii_digit()
            ),
        "invalid Rithmic front-month trading symbol"
    );
    Ok(symbol)
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
    fn live_request_subscribes_for_authoritative_updates() {
        let payload = request_with_updates("front-month", "MNQ", "CME", true).unwrap();
        let request: protocol::RequestFrontMonthContract = codec::decode(&payload).unwrap();

        assert_eq!(request.need_updates, Some(true));
    }

    #[test]
    fn live_event_matrix_verifies_current_and_flags_rollover() {
        let current = response(
            FRONT_MONTH_RESPONSE,
            "front-month",
            vec!["0"],
            Some("MNQU6"),
        );
        assert_eq!(
            decode_live_event(&current, "front-month", "MNQ", "CME", "MNQU6").unwrap(),
            Some(FrontMonthEvent::CurrentVerified)
        );

        let changed = codec::encode(&protocol::FrontMonthContractUpdate {
            template_id: FRONT_MONTH_UPDATE,
            symbol: Some("MNQ".to_string()),
            exchange: Some("CME".to_string()),
            trading_symbol: Some("MNQZ6".to_string()),
            trading_exchange: Some("CME".to_string()),
            is_front_month_symbol: Some(true),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_live_event(&changed, "front-month", "MNQ", "CME", "MNQU6").unwrap(),
            Some(FrontMonthEvent::RolloverRequired("MNQZ6".to_string()))
        );

        let unrelated = codec::encode(&protocol::LastTrade {
            template_id: 150,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_live_event(&unrelated, "front-month", "MNQ", "CME", "MNQU6").unwrap(),
            None
        );

        for status in [None, Some(false)] {
            let update = codec::encode(&protocol::FrontMonthContractUpdate {
                template_id: FRONT_MONTH_UPDATE,
                symbol: Some("MNQ".to_string()),
                exchange: Some("CME".to_string()),
                trading_symbol: Some("MNQU6".to_string()),
                trading_exchange: Some("CME".to_string()),
                is_front_month_symbol: status,
                ..Default::default()
            })
            .unwrap();
            assert!(decode_live_event(&update, "front-month", "MNQ", "CME", "MNQU6").is_err());

            let response = response_with_status(
                FRONT_MONTH_RESPONSE,
                "front-month",
                vec!["0"],
                Some("MNQU6"),
                status,
            );
            assert!(decode_live_event(&response, "front-month", "MNQ", "CME", "MNQU6").is_err());
        }
    }

    #[test]
    fn live_update_requires_complete_instrument_identity() {
        for (symbol, exchange, trading_exchange) in [
            (None, Some("CME"), Some("CME")),
            (Some("MNQ"), None, Some("CME")),
            (Some("MNQ"), Some("CME"), None),
            (Some("NQ"), Some("CME"), Some("CME")),
            (Some("MNQ"), Some("NYMEX"), Some("CME")),
            (Some("MNQ"), Some("CME"), Some("NYMEX")),
        ] {
            let payload = codec::encode(&protocol::FrontMonthContractUpdate {
                template_id: FRONT_MONTH_UPDATE,
                symbol: symbol.map(str::to_string),
                exchange: exchange.map(str::to_string),
                trading_symbol: Some("MNQU6".to_string()),
                trading_exchange: trading_exchange.map(str::to_string),
                ..Default::default()
            })
            .unwrap();

            assert!(decode_live_event(&payload, "front-month", "MNQ", "CME", "MNQU6").is_err());
        }
    }

    #[test]
    fn trading_symbol_validation_matrix_rejects_wrong_root_and_unsafe_text() {
        for invalid in ["MNQ", "ESU6", "MNQA6", "MNQU66", "MNQU\n"] {
            assert!(validated_trading_symbol(Some(invalid.to_string()), "MNQ").is_err());
        }
        assert_eq!(
            validated_trading_symbol(Some("mnqu6".to_string()), "MNQ").unwrap(),
            "mnqu6"
        );
        assert_eq!(
            validated_trading_symbol(Some("6EU6".to_string()), "6E").unwrap(),
            "6EU6"
        );
    }

    #[test]
    fn response_validation_matrix_fails_closed() {
        let valid = response(FRONT_MONTH_RESPONSE, "front-month", vec!["0"], Some("NQU6"));
        assert_eq!(
            decode_response(&valid, "front-month", "NQ", "CME").unwrap(),
            "NQU6"
        );

        for payload in [
            response(115, "front-month", vec!["0"], Some("NQU6")),
            response(FRONT_MONTH_RESPONSE, "other", vec!["0"], Some("NQU6")),
            response(FRONT_MONTH_RESPONSE, "front-month", vec![], Some("NQU6")),
            response(FRONT_MONTH_RESPONSE, "front-month", vec!["7"], None),
            response(FRONT_MONTH_RESPONSE, "front-month", vec!["0"], None),
        ] {
            assert!(decode_response(&payload, "front-month", "NQ", "CME").is_err());
        }
        assert!(decode_response(&valid, "front-month", "", "CME").is_err());
        assert!(decode_response(&valid, "front-month", "NQ", "").is_err());

        for (symbol, exchange, trading_exchange) in [
            (Some("ES"), Some("CME"), Some("CME")),
            (Some("NQ"), Some("NYMEX"), Some("CME")),
            (Some("NQ"), Some("CME"), Some("NYMEX")),
        ] {
            let payload = codec::encode(&protocol::ResponseFrontMonthContract {
                template_id: FRONT_MONTH_RESPONSE,
                user_msg: vec!["front-month".to_string()],
                rp_code: vec!["0".to_string()],
                symbol: symbol.map(str::to_string),
                exchange: exchange.map(str::to_string),
                trading_exchange: trading_exchange.map(str::to_string),
                trading_symbol: Some("NQU6".to_string()),
                is_front_month_symbol: Some(true),
                ..Default::default()
            })
            .unwrap();
            assert!(decode_response(&payload, "front-month", "NQ", "CME").is_err());
        }
    }

    fn response(
        template_id: i32,
        request_key: &str,
        rp_code: Vec<&str>,
        trading_symbol: Option<&str>,
    ) -> Vec<u8> {
        response_with_status(
            template_id,
            request_key,
            rp_code,
            trading_symbol,
            Some(true),
        )
    }

    fn response_with_status(
        template_id: i32,
        request_key: &str,
        rp_code: Vec<&str>,
        trading_symbol: Option<&str>,
        is_front_month_symbol: Option<bool>,
    ) -> Vec<u8> {
        codec::encode(&protocol::ResponseFrontMonthContract {
            template_id,
            user_msg: vec![request_key.to_string()],
            rp_code: rp_code.into_iter().map(str::to_string).collect(),
            trading_symbol: trading_symbol.map(str::to_string),
            is_front_month_symbol,
            ..Default::default()
        })
        .unwrap()
    }
}
