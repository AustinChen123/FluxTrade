use super::{codec, protocol, session::ensure_success};
use anyhow::{ensure, Result};

const MARKET_DATA_REQUEST: i32 = 100;
const MARKET_DATA_RESPONSE: i32 = 101;

#[derive(Clone, Copy)]
pub(crate) enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
}

pub(crate) fn last_trade_request(
    exchange: &str,
    symbol: &str,
    action: SubscriptionAction,
) -> Result<Vec<u8>> {
    ensure!(
        !exchange.trim().is_empty(),
        "Rithmic exchange must not be empty"
    );
    ensure!(
        !symbol.trim().is_empty(),
        "Rithmic symbol must not be empty"
    );

    codec::encode(&protocol::RequestMarketDataUpdate {
        template_id: MARKET_DATA_REQUEST,
        user_msg: vec![],
        symbol: Some(symbol.to_string()),
        exchange: Some(exchange.to_string()),
        request: Some(match action {
            SubscriptionAction::Subscribe => {
                protocol::request_market_data_update::Request::Subscribe as i32
            }
            SubscriptionAction::Unsubscribe => {
                protocol::request_market_data_update::Request::Unsubscribe as i32
            }
        }),
        update_bits: Some(protocol::request_market_data_update::UpdateBits::LastTrade as u32),
    })
}

pub(crate) fn accept_market_data_response(payload: &[u8]) -> Result<()> {
    ensure!(
        codec::template_id(payload)? == MARKET_DATA_RESPONSE,
        "unexpected Rithmic market-data response template"
    );
    let response: protocol::ResponseMarketDataUpdate = codec::decode(payload)?;
    ensure_success(&response.rp_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_trade_subscription_action_matrix() {
        for (action, expected) in [
            (
                SubscriptionAction::Subscribe,
                protocol::request_market_data_update::Request::Subscribe,
            ),
            (
                SubscriptionAction::Unsubscribe,
                protocol::request_market_data_update::Request::Unsubscribe,
            ),
        ] {
            let payload = last_trade_request("CME", "NQU6", action).unwrap();
            let request: protocol::RequestMarketDataUpdate = codec::decode(&payload).unwrap();

            assert_eq!(request.template_id, MARKET_DATA_REQUEST);
            assert_eq!(request.exchange.as_deref(), Some("CME"));
            assert_eq!(request.symbol.as_deref(), Some("NQU6"));
            assert_eq!(request.request, Some(expected as i32));
            assert_eq!(
                request.update_bits,
                Some(protocol::request_market_data_update::UpdateBits::LastTrade as u32)
            );
        }
    }

    #[test]
    fn market_data_request_and_response_validation_matrix() {
        assert!(last_trade_request("", "NQU6", SubscriptionAction::Subscribe).is_err());
        assert!(last_trade_request("CME", "", SubscriptionAction::Subscribe).is_err());

        for (codes, succeeds) in [
            (vec!["0".to_string()], true),
            (vec!["9".to_string()], false),
            (vec![], false),
        ] {
            let payload = codec::encode(&protocol::ResponseMarketDataUpdate {
                template_id: MARKET_DATA_RESPONSE,
                user_msg: vec![],
                rp_code: codes,
            })
            .unwrap();
            assert_eq!(accept_market_data_response(&payload).is_ok(), succeeds);
        }
    }
}
