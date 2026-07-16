use super::{codec, protocol, session::ensure_success};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

const MARKET_DATA_REQUEST: i32 = 100;
const MARKET_DATA_RESPONSE: i32 = 101;
const LAST_TRADE: i32 = 150;
const LAST_TRADE_PRESENT: u32 = 1;

#[derive(Clone, Copy)]
pub(crate) enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Aggressor {
    Buy,
    Sell,
}

#[derive(Debug, PartialEq)]
pub(crate) struct LastTradeUpdate {
    pub exchange: String,
    pub symbol: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub aggressor: Option<Aggressor>,
    pub ssboe: i32,
    pub usecs: i32,
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

pub(crate) fn decode_last_trade(payload: &[u8]) -> Result<LastTradeUpdate> {
    ensure!(
        codec::template_id(payload)? == LAST_TRADE,
        "unexpected Rithmic last-trade template"
    );
    let trade: protocol::LastTrade = codec::decode(payload)?;
    ensure!(
        trade.presence_bits.unwrap_or_default() & LAST_TRADE_PRESENT != 0,
        "Rithmic update does not contain a last trade"
    );

    let exchange = required_text(trade.exchange, "exchange")?;
    let symbol = required_text(trade.symbol, "symbol")?;
    let price = trade.trade_price.context("missing Rithmic trade price")?;
    ensure!(
        price.is_finite() && price > 0.0,
        "invalid Rithmic trade price"
    );
    let price = Decimal::from_str(&price.to_string()).context("invalid Rithmic trade price")?;
    let quantity = trade.trade_size.context("missing Rithmic trade size")?;
    ensure!(quantity > 0, "invalid Rithmic trade size");
    let ssboe = trade.ssboe.context("missing Rithmic trade ssboe")?;
    let usecs = trade.usecs.context("missing Rithmic trade usecs")?;
    ensure!(ssboe >= 0, "invalid Rithmic trade ssboe");
    ensure!(
        (0..1_000_000).contains(&usecs),
        "invalid Rithmic trade usecs"
    );

    Ok(LastTradeUpdate {
        exchange,
        symbol,
        price,
        quantity: Decimal::from(quantity),
        aggressor: match trade.aggressor {
            None => None,
            Some(value) if value == protocol::last_trade::TransactionType::Buy as i32 => {
                Some(Aggressor::Buy)
            }
            Some(value) if value == protocol::last_trade::TransactionType::Sell as i32 => {
                Some(Aggressor::Sell)
            }
            Some(_) => anyhow::bail!("unknown Rithmic trade aggressor"),
        },
        ssboe,
        usecs,
    })
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic trade {field}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

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

    #[test]
    fn decodes_last_trade_without_float_financial_values() {
        let payload = last_trade_payload();

        assert_eq!(
            decode_last_trade(&payload).unwrap(),
            LastTradeUpdate {
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                price: dec!(29784.75),
                quantity: dec!(2),
                aggressor: Some(Aggressor::Buy),
                ssboe: 1_784_243_600,
                usecs: 123_456,
            }
        );
    }

    #[test]
    fn last_trade_validation_matrix_fails_closed() {
        let valid = || protocol::LastTrade {
            template_id: LAST_TRADE,
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            presence_bits: Some(LAST_TRADE_PRESENT),
            trade_price: Some(29_784.75),
            trade_size: Some(1),
            aggressor: Some(protocol::last_trade::TransactionType::Sell as i32),
            ssboe: Some(1_784_243_600),
            usecs: Some(999_999),
            ..Default::default()
        };

        let invalid = [
            protocol::LastTrade {
                template_id: 149,
                ..valid()
            },
            protocol::LastTrade {
                presence_bits: Some(0),
                ..valid()
            },
            protocol::LastTrade {
                exchange: None,
                ..valid()
            },
            protocol::LastTrade {
                symbol: Some(" ".to_string()),
                ..valid()
            },
            protocol::LastTrade {
                trade_price: None,
                ..valid()
            },
            protocol::LastTrade {
                trade_price: Some(f64::NAN),
                ..valid()
            },
            protocol::LastTrade {
                trade_price: Some(0.0),
                ..valid()
            },
            protocol::LastTrade {
                trade_size: Some(0),
                ..valid()
            },
            protocol::LastTrade {
                aggressor: Some(99),
                ..valid()
            },
            protocol::LastTrade {
                ssboe: Some(-1),
                ..valid()
            },
            protocol::LastTrade {
                usecs: Some(-1),
                ..valid()
            },
            protocol::LastTrade {
                usecs: Some(1_000_000),
                ..valid()
            },
        ];

        for trade in invalid {
            let payload = codec::encode(&trade).unwrap();
            assert!(decode_last_trade(&payload).is_err());
        }
    }

    fn last_trade_payload() -> Vec<u8> {
        codec::encode(&protocol::LastTrade {
            template_id: LAST_TRADE,
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            presence_bits: Some(LAST_TRADE_PRESENT),
            trade_price: Some(29_784.75),
            trade_size: Some(2),
            aggressor: Some(protocol::last_trade::TransactionType::Buy as i32),
            ssboe: Some(1_784_243_600),
            usecs: Some(123_456),
            ..Default::default()
        })
        .unwrap()
    }
}
