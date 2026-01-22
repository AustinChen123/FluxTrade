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

pub struct BybitConnector {
    exchange_id: String,
}

impl BybitConnector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            exchange_id: "BYBIT".to_string(),
        }
    }

    #[allow(dead_code)]
    fn parse_kline(&self, topic: &str, data: &Value) -> Result<Candlestick> {
        let parts: Vec<&str> = topic.split('.').collect();
        let timeframe = parts.get(1).context("Missing timeframe in topic")?.to_string();
        let symbol = parts.get(2).context("Missing symbol in topic")?;

        Ok(Candlestick {
            product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
            timeframe,
            timestamp: data.get("start").context("start")?.as_i64().context("start not i64")?,
            open: data.get("open").context("open")?.as_str().context("open not str")?.parse::<Decimal>()?,
            high: data.get("high").context("high")?.as_str().context("high not str")?.parse::<Decimal>()?,
            low: data.get("low").context("low")?.as_str().context("low not str")?.parse::<Decimal>()?,
            close: data.get("close").context("close")?.as_str().context("close not str")?.parse::<Decimal>()?,
            volume: data.get("volume").context("volume")?.as_str().context("volume not str")?.parse::<Decimal>()?,
        })
    }

    #[allow(dead_code)]
    fn parse_trade(&self, data: &Value) -> Result<Trade> {
        let symbol = data.get("s").context("s")?.as_str().context("s not str")?;
        
        Ok(Trade {
            id: data.get("i").context("i")?.as_str().context("i not str")?.to_string(),
            product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
            price: data.get("p").context("p")?.as_str().context("p not str")?.parse::<Decimal>()?,
            quantity: data.get("v").context("v")?.as_str().context("v not str")?.parse::<Decimal>()?,
            side: data.get("S").context("S")?.as_str().context("S not str")?.to_lowercase(),
            timestamp: data.get("T").context("T")?.as_i64().context("T not i64")?,
        })
    }
}

#[async_trait]
impl ExchangeConnector for BybitConnector {
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn subscribe_trades(&mut self, symbols: &[String], tx: mpsc::Sender<Trade>) -> Result<()> {
        let url = "wss://stream.bybit.com/v5/public/linear";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();
        
        let args: Vec<String> = symbols.iter()
            .map(|s| format!("publicTrade.{}", s))
            .collect();

        info!("Subscribing to Bybit trades: {:?}", args);

        tokio::spawn(async move {
            let res = ws_manager.connect_with_retry(
                |mut ws| {
                    let args = args.clone();
                    async move {
                        let sub = json!({
                            "op": "subscribe",
                            "args": args
                        });
                        let res = ws.send(Message::Text(sub.to_string().into())).await.map_err(|e| anyhow::anyhow!(e));
                        Ok((ws, res))
                    }
                },
                |msg| {
                    let tx = tx.clone();
                    let connector_id = connector_id.clone();
                    async move {
                        if let Message::Text(text) = msg {
                            let v: Value = serde_json::from_str(&text)?;
                            if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                                if topic.starts_with("publicTrade") {
                                    if let Some(data_list) = v.get("data").and_then(|d| d.as_array()) {
                                        for data in data_list {
                                            let trade = Trade {
                                                id: data.get("i").context("i")?.as_str().context("i")?.to_string(),
                                                product_id: format!("{}:{}-PERP", connector_id, data.get("s").context("s")?.as_str().context("s")?),
                                                price: data.get("p").context("p")?.as_str().context("p")?.parse::<Decimal>()?,
                                                quantity: data.get("v").context("v")?.as_str().context("v")?.parse::<Decimal>()?,
                                                side: data.get("S").context("S")?.as_str().context("S")?.to_lowercase(),
                                                timestamp: data.get("T").context("T")?.as_i64().context("T")?,
                                            };
                                            if let Err(e) = trade.validate() {
                                                warn!("Invalid Bybit trade: {}", e);
                                            } else {
                                                tx.send(trade).await.ok();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                }
            ).await;

            if let Err(e) = res {
                error!("Bybit trades subscription failed: {}", e);
            }
        });
        
        Ok(())
    }

    async fn subscribe_orderbook(&mut self, _symbols: &[String], _tx: mpsc::Sender<OrderBook>) -> Result<()> {
        Ok(())
    }

    async fn subscribe_candles(&mut self, symbols: &[String], timeframe: &str, tx: mpsc::Sender<Candlestick>) -> Result<()> {
        let bybit_tf = match timeframe {
            "1m" => "1",
            "3m" => "3",
            "5m" => "5",
            "15m" => "15",
            "30m" => "30",
            "1h" => "60",
            "2h" => "120",
            "4h" => "240",
            "6h" => "360",
            "12h" => "720",
            "1d" => "D",
            _ => timeframe,
        };

        let url = "wss://stream.bybit.com/v5/public/linear";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();
        
        let args: Vec<String> = symbols.iter()
            .map(|s| format!("kline.{}.{}", bybit_tf, s))
            .collect();

        info!("Subscribing to Bybit candles: {:?}", args);

        tokio::spawn(async move {
            let res = ws_manager.connect_with_retry(
                |mut ws| {
                    let args = args.clone();
                    async move {
                        let sub = json!({
                            "op": "subscribe",
                            "args": args
                        });
                        let res = ws.send(Message::Text(sub.to_string().into())).await.map_err(|e| anyhow::anyhow!(e));
                        Ok((ws, res))
                    }
                },
                |msg| {
                    let tx = tx.clone();
                    let connector_id = connector_id.clone();
                    async move {
                        if let Message::Text(text) = msg {
                            let v: Value = serde_json::from_str(&text)?;
                            if let Some(topic) = v.get("topic").and_then(|t| t.as_str()) {
                                if topic.starts_with("kline") {
                                    if let Some(data_list) = v.get("data").and_then(|d| d.as_array()) {
                                        let parts: Vec<&str> = topic.split('.').collect();
                                        let symbol = parts.get(2).unwrap_or(&"UNKNOWN");
                                        let tf = parts.get(1).unwrap_or(&"1");
                                        
                                        for data in data_list {
                                            let candle = Candlestick {
                                                product_id: format!("{}:{}-PERP", connector_id, symbol),
                                                timeframe: tf.to_string(),
                                                timestamp: data.get("start").context("start")?.as_i64().context("start")?,
                                                open: data.get("open").context("open")?.as_str().context("open")?.parse::<Decimal>()?,
                                                high: data.get("high").context("high")?.as_str().context("high")?.parse::<Decimal>()?,
                                                low: data.get("low").context("low")?.as_str().context("low")?.parse::<Decimal>()?,
                                                close: data.get("close").context("close")?.as_str().context("close")?.parse::<Decimal>()?,
                                                volume: data.get("volume").context("volume")?.as_str().context("volume")?.parse::<Decimal>()?,
                                            };
                                            if let Err(e) = candle.validate() {
                                                warn!("Invalid Bybit candle: {}", e);
                                            } else {
                                                tx.send(candle).await.ok();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(())
                    }
                }
            ).await;

            if let Err(e) = res {
                error!("Bybit candles subscription failed: {}", e);
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
    fn test_bybit_parse_kline() {
        let connector = BybitConnector::new();
        let topic = "kline.1.BTCUSDT";
        let data = json!({
            "start": 1672324800000i64,
            "end": 1672324859999i64,
            "interval": "1",
            "open": "16599.4",
            "close": "16599.3",
            "high": "16599.4",
            "low": "16599.3",
            "confirm": false,
            "volume": "0.1",
            "turnover": "1659.93"
        });

        let candle = connector.parse_kline(topic, &data).unwrap();
        assert_eq!(candle.product_id, "BYBIT:BTCUSDT-PERP");
        assert_eq!(candle.timeframe, "1");
        assert_eq!(candle.close, "16599.3".parse::<Decimal>().unwrap());
    }

    #[test]
    fn test_bybit_parse_trade() {
        let connector = BybitConnector::new();
        let data = json!({
            "T": 1672324988881i64,
            "s": "BTCUSDT",
            "S": "Buy",
            "p": "16599.4",
            "v": "0.1",
            "i": "trade_id_123",
            "BT": false
        });

        let trade = connector.parse_trade(&data).unwrap();
        assert_eq!(trade.product_id, "BYBIT:BTCUSDT-PERP");
        assert_eq!(trade.side, "buy");
        assert_eq!(trade.price, "16599.4".parse::<Decimal>().unwrap());
    }
}
