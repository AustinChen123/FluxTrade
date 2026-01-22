use crate::connector::ExchangeConnector;
use crate::connector::ws::WebSocketManager;
use crate::model::{Candlestick, Trade, OrderBook};
use anyhow::{Result, Context};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{info, warn, error};
use futures_util::SinkExt;
use chrono::DateTime;

pub struct BackpackConnector {
    exchange_id: String,
}

impl BackpackConnector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            exchange_id: "BACKPACK".to_string(),
        }
    }

    fn parse_iso8601_to_ms(iso: &str) -> Result<i64> {
        // Backpack returns "2026-01-22T21:19:00" without timezone, 
        // assuming UTC based on typical exchange behavior.
        // We add 'Z' to make it RFC3339 compatible if it lacks it.
        let rfc = if iso.ends_with('Z') { iso.to_string() } else { format!("{}Z", iso) };
        let dt = DateTime::parse_from_rfc3339(&rfc)
            .context("Failed to parse ISO8601 timestamp")?;
        Ok(dt.timestamp_millis())
    }
}

#[async_trait]
impl ExchangeConnector for BackpackConnector {
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn subscribe_trades(&mut self, symbols: &[String], tx: mpsc::Sender<Trade>) -> Result<()> {
        let url = "wss://ws.backpack.exchange/";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();
        
        let args: Vec<String> = symbols.iter()
            .map(|s| format!("trade.{}", s))
            .collect();

        info!("Subscribing to Backpack trades: {:?}", args);

        tokio::spawn(async move {
            let res = ws_manager.connect_with_retry(
                |mut ws| {
                    let args = args.clone();
                    async move {
                        let sub = json!({
                            "method": "SUBSCRIBE",
                            "params": args
                        });
                        ws.send(Message::Text(sub.to_string().into())).await.map_err(|e| anyhow::anyhow!(e))?;
                        Ok((ws, Ok(())))
                    }
                },
                |msg| {
                    let tx = tx.clone();
                    let connector_id = connector_id.clone();
                    async move {
                        if let Message::Text(text) = msg {
                            let v: Value = serde_json::from_str(&text)?;
                            if let Some(data) = v.get("data") {
                                if data.get("e") == Some(&Value::String("trade".to_string())) {
                                    let trade = Trade {
                                        id: data.get("t").context("t")?.as_i64().context("t")?.to_string(),
                                        product_id: format!("{}:{}-PERP", connector_id, data.get("s").context("s")?.as_str().context("s")?),
                                        price: data.get("p").context("p")?.as_str().context("p")?.parse::<Decimal>()?,
                                        quantity: data.get("q").context("q")?.as_str().context("q")?.parse::<Decimal>()?,
                                        side: if data.get("m").context("m")?.as_bool().context("m")? { "sell".to_string() } else { "buy".to_string() },
                                        timestamp: data.get("T").context("T")?.as_i64().context("T")? / 1000, // micro to milli
                                    };
                                    if let Err(e) = trade.validate() {
                                        warn!("Invalid Backpack trade: {}", e);
                                    } else {
                                        tx.send(trade).await.ok();
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                }
            ).await;

            if let Err(e) = res {
                error!("Backpack trades subscription failed: {}", e);
            }
        });
        
        Ok(())
    }

    async fn subscribe_orderbook(&mut self, _symbols: &[String], _tx: mpsc::Sender<OrderBook>) -> Result<()> {
        Ok(())
    }

    async fn subscribe_candles(&mut self, symbols: &[String], timeframe: &str, tx: mpsc::Sender<Candlestick>) -> Result<()> {
        let url = "wss://ws.backpack.exchange/";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();
        
        let args: Vec<String> = symbols.iter()
            .map(|s| format!("kline.{}.{}", timeframe, s))
            .collect();
        
        let timeframe_str = timeframe.to_string();

        info!("Subscribing to Backpack candles: {:?}", args);

        tokio::spawn(async move {
            let res = ws_manager.connect_with_retry(
                |mut ws| {
                    let args = args.clone();
                    async move {
                        let sub = json!({
                            "method": "SUBSCRIBE",
                            "params": args
                        });
                        ws.send(Message::Text(sub.to_string().into())).await.map_err(|e| anyhow::anyhow!(e))?;
                        Ok((ws, Ok(())))
                    }
                },
                |msg| {
                    let tx = tx.clone();
                    let connector_id = connector_id.clone();
                    let timeframe_str = timeframe_str.clone();
                    async move {
                        if let Message::Text(text) = msg {
                            let v: Value = serde_json::from_str(&text)?;
                            if let Some(data) = v.get("data") {
                                if data.get("e") == Some(&Value::String("kline".to_string())) {
                                    let ts_str = data.get("t").context("t")?.as_str().context("t not str")?;
                                    let timestamp = BackpackConnector::parse_iso8601_to_ms(ts_str)?;

                                    let candle = Candlestick {
                                        product_id: format!("{}:{}-PERP", connector_id, data.get("s").context("s")?.as_str().context("s")?),
                                        timeframe: timeframe_str, // Use the cloned String
                                        timestamp,
                                        open: data.get("o").context("o")?.as_str().context("o")?.parse::<Decimal>()?,
                                        high: data.get("h").context("h")?.as_str().context("h")?.parse::<Decimal>()?,
                                        low: data.get("l").context("l")?.as_str().context("l")?.parse::<Decimal>()?,
                                        close: data.get("c").context("c")?.as_str().context("c")?.parse::<Decimal>()?,
                                        volume: data.get("v").context("v")?.as_str().context("v")?.parse::<Decimal>()?,
                                    };
                                    if let Err(e) = candle.validate() {
                                        warn!("Invalid Backpack candle: {}", e);
                                    } else {
                                        tx.send(candle).await.ok();
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                }
            ).await;

            if let Err(e) = res {
                error!("Backpack candles subscription failed: {}", e);
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_backpack_parse_iso8601() {
        let ts = BackpackConnector::parse_iso8601_to_ms("2026-01-22T21:19:00").unwrap();
        // 2026-01-22 21:19:00 UTC
        assert_eq!(ts, 1769116740000i64);
    }

    #[test]
    fn test_backpack_parse_trade() {
        // Trade data sample from real test
        let data = json!({
            "E": 1769116699776278i64,
            "T": 1769116699772000i64,
            "a": "28354754146",
            "b": "28354756627",
            "e": "trade",
            "m": false,
            "p": "128.57",
            "q": "0.72",
            "s": "SOL_USDC",
            "t": 379172947i64
        });
        
        // Mocking the behavior inside subscribe_trades closure
        let trade = Trade {
            id: data.get("t").unwrap().as_i64().unwrap().to_string(),
            product_id: format!("BACKPACK:{}-PERP", data.get("s").unwrap().as_str().unwrap()),
            price: data.get("p").unwrap().as_str().unwrap().parse::<Decimal>().unwrap(),
            quantity: data.get("q").unwrap().as_str().unwrap().parse::<Decimal>().unwrap(),
            side: if data.get("m").unwrap().as_bool().unwrap() { "sell".to_string() } else { "buy".to_string() },
            timestamp: data.get("T").unwrap().as_i64().unwrap() / 1000,
        };

        assert_eq!(trade.product_id, "BACKPACK:SOL_USDC-PERP");
        assert_eq!(trade.side, "buy");
        assert_eq!(trade.timestamp, 1769116699772i64);
    }
}
