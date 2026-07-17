use super::{codec, protocol, session::ensure_success};
use anyhow::{ensure, Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

const MARKET_DATA_REQUEST: i32 = 100;
const MARKET_DATA_RESPONSE: i32 = 101;
const LAST_TRADE: i32 = 150;
const LAST_TRADE_PRESENT: u32 = 1;
const REJECT: i32 = 75;

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
    pub timestamp: i64,
    pub is_snapshot: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) enum MarketDataEvent {
    SubscriptionAccepted,
    LastTrade(LastTradeUpdate),
    Rejected {
        user_messages: Vec<String>,
        response_codes: Vec<String>,
    },
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
        timestamp: epoch_millis(ssboe, usecs),
        is_snapshot: trade.is_snapshot.unwrap_or(false),
    })
}

pub(crate) fn decode_market_data_event(payload: &[u8]) -> Result<MarketDataEvent> {
    match codec::template_id(payload)? {
        MARKET_DATA_RESPONSE => {
            accept_market_data_response(payload)?;
            Ok(MarketDataEvent::SubscriptionAccepted)
        }
        LAST_TRADE => decode_last_trade(payload).map(MarketDataEvent::LastTrade),
        REJECT => {
            let reject: protocol::Reject = codec::decode(payload)?;
            Ok(MarketDataEvent::Rejected {
                user_messages: reject.user_msg,
                response_codes: reject.rp_code,
            })
        }
        template_id => anyhow::bail!("unsupported Rithmic market-data template {template_id}"),
    }
}

fn required_text(value: Option<String>, field: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("missing Rithmic trade {field}"))
}

fn epoch_millis(ssboe: i32, usecs: i32) -> i64 {
    i64::from(ssboe) * 1_000 + i64::from(usecs) / 1_000
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
                timestamp: 1_784_243_600_123,
                is_snapshot: false,
            }
        );
    }

    #[test]
    fn market_data_event_template_matrix() {
        let accepted = codec::encode(&protocol::ResponseMarketDataUpdate {
            template_id: MARKET_DATA_RESPONSE,
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            decode_market_data_event(&accepted).unwrap(),
            MarketDataEvent::SubscriptionAccepted
        );
        assert!(matches!(
            decode_market_data_event(&last_trade_payload()).unwrap(),
            MarketDataEvent::LastTrade(_)
        ));
        let rejected = codec::encode(&protocol::Reject {
            template_id: REJECT,
            user_msg: vec!["subscription".to_string()],
            rp_code: vec!["permission-denied".to_string()],
        })
        .unwrap();
        assert_eq!(
            decode_market_data_event(&rejected).unwrap(),
            MarketDataEvent::Rejected {
                user_messages: vec!["subscription".to_string()],
                response_codes: vec!["permission-denied".to_string()],
            }
        );

        for payload in [
            codec::encode(&protocol::ResponseMarketDataUpdate {
                template_id: MARKET_DATA_RESPONSE,
                rp_code: vec!["9".to_string()],
                ..Default::default()
            })
            .unwrap(),
            codec::encode(&protocol::RequestLogout {
                template_id: 12,
                ..Default::default()
            })
            .unwrap(),
        ] {
            assert!(decode_market_data_event(&payload).is_err());
        }
    }

    #[test]
    fn epoch_timestamp_conversion_truncates_sub_millisecond_precision() {
        assert_eq!(epoch_millis(0, 0), 0);
        assert_eq!(epoch_millis(1_784_243_600, 123_456), 1_784_243_600_123);
        assert_eq!(epoch_millis(i32::MAX, 999_999), 2_147_483_647_999);
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
