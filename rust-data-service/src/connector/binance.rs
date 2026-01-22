use crate::connector::ws::WebSocketManager;
use crate::connector::ExchangeConnector;
use crate::model::{Candlestick, OrderBook, Trade};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

#[allow(dead_code)]
pub struct BinanceConnector {
    ws_manager: WebSocketManager,
    exchange_id: String,
}

impl BinanceConnector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ws_manager: WebSocketManager::new("wss://fstream.binance.com/ws"),
            exchange_id: "BINANCE".to_string(),
        }
    }

    #[allow(dead_code)]
    fn parse_kline(&self, v: &Value) -> Result<Candlestick> {
        let k = v.get("k").context("Missing 'k' field in kline")?;
        let symbol = v
            .get("s")
            .context("Missing 's'")?
            .as_str()
            .context("s not string")?;

        Ok(Candlestick {
            product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
            timeframe: k
                .get("i")
                .context("Missing 'i'")?
                .as_str()
                .context("i not string")?
                .to_string(),
            timestamp: k
                .get("t")
                .context("Missing 't'")?
                .as_i64()
                .context("t not i64")?,
            open: k
                .get("o")
                .context("Missing 'o'")?
                .as_str()
                .context("o not string")?
                .parse::<Decimal>()?,
            high: k
                .get("h")
                .context("Missing 'h'")?
                .as_str()
                .context("h not string")?
                .parse::<Decimal>()?,
            low: k
                .get("l")
                .context("Missing 'l'")?
                .as_str()
                .context("l not string")?
                .parse::<Decimal>()?,
            close: k
                .get("c")
                .context("Missing 'c'")?
                .as_str()
                .context("c not string")?
                .parse::<Decimal>()?,
            volume: k
                .get("v")
                .context("Missing 'v'")?
                .as_str()
                .context("v not string")?
                .parse::<Decimal>()?,
        })
    }

    #[allow(dead_code)]
    fn parse_trade(&self, v: &Value) -> Result<Trade> {
        let symbol = v
            .get("s")
            .context("Missing 's'")?
            .as_str()
            .context("s not string")?;

        Ok(Trade {
            id: v
                .get("a")
                .context("Missing 'a'")?
                .as_i64()
                .context("a not i64")?
                .to_string(),
            product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
            price: v
                .get("p")
                .context("Missing 'p'")?
                .as_str()
                .context("p not string")?
                .parse::<Decimal>()?,
            quantity: v
                .get("q")
                .context("Missing 'q'")?
                .as_str()
                .context("q not string")?
                .parse::<Decimal>()?,
            side: if v
                .get("m")
                .context("Missing 'm'")?
                .as_bool()
                .context("m not bool")?
            {
                "sell".to_string()
            } else {
                "buy".to_string()
            },
            timestamp: v
                .get("T")
                .context("Missing 'T'")?
                .as_i64()
                .context("T not i64")?,
        })
    }
}

#[async_trait]
impl ExchangeConnector for BinanceConnector {
    async fn connect(&mut self) -> Result<()> {
        // In Binance, subscriptions are often part of the URL or sent as messages.
        // For simplicity in this base manager, we'll handle actual subscription in the loop if needed.
        Ok(())
    }

    async fn subscribe_trades(
        &mut self,
        symbols: &[String],
        tx: mpsc::Sender<Trade>,
    ) -> Result<()> {
        let streams = symbols
            .iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect::<Vec<_>>()
            .join("/");

        let url = format!("wss://fstream.binance.com/stream?streams={}", streams);
        let ws_manager = WebSocketManager::new(&url);

        // We need a static-like way to use parse_trade or clone the connector
        // Since Connector is just a config holder, we use a simple helper
        let exchange_id = self.exchange_id.clone();

        info!("Subscribing to Binance trades: {}", streams);

        tokio::spawn(async move {
            let res = ws_manager
                .connect_with_retry(
                    |ws| async { Ok((ws, Ok(()))) },
                    |msg| {
                        let tx = tx.clone();
                        let exchange_id = exchange_id.clone();
                        async move {
                            if let Message::Text(text) = msg {
                                let v: Value = serde_json::from_str(&text)?;
                                if let Some(data) = v.get("data") {
                                    // Extract data and use a local logic or helper
                                    if data.get("e") == Some(&Value::String("aggTrade".to_string()))
                                    {
                                        let symbol =
                                            data.get("s").context("s")?.as_str().context("s")?;
                                        let trade = Trade {
                                            id: data
                                                .get("a")
                                                .context("a")?
                                                .as_i64()
                                                .context("a")?
                                                .to_string(),
                                            product_id: format!("{}:{}-PERP", exchange_id, symbol),
                                            price: data
                                                .get("p")
                                                .context("p")?
                                                .as_str()
                                                .context("p")?
                                                .parse::<Decimal>()?,
                                            quantity: data
                                                .get("q")
                                                .context("q")?
                                                .as_str()
                                                .context("q")?
                                                .parse::<Decimal>()?,
                                            side: if data
                                                .get("m")
                                                .context("m")?
                                                .as_bool()
                                                .context("m")?
                                            {
                                                "sell".to_string()
                                            } else {
                                                "buy".to_string()
                                            },
                                            timestamp: data
                                                .get("T")
                                                .context("T")?
                                                .as_i64()
                                                .context("T")?,
                                        };
                                        if let Err(e) = trade.validate() {
                                            warn!("Invalid trade received: {}", e);
                                        } else {
                                            tx.send(trade).await.ok();
                                        }
                                    }
                                }
                            }
                            Ok(())
                        }
                    },
                )
                .await;

            if let Err(e) = res {
                error!("Binance trades subscription failed: {}", e);
            }
        });

        Ok(())
    }

    async fn subscribe_orderbook(
        &mut self,
        _symbols: &[String],
        _tx: mpsc::Sender<OrderBook>,
    ) -> Result<()> {
        // Implementation for orderbook
        Ok(())
    }

    async fn subscribe_candles(
        &mut self,
        symbols: &[String],
        timeframe: &str,
        tx: mpsc::Sender<Candlestick>,
    ) -> Result<()> {
        let streams = symbols
            .iter()
            .map(|s| format!("{}@kline_{}", s.to_lowercase(), timeframe))
            .collect::<Vec<_>>()
            .join("/");

        let url = format!("wss://fstream.binance.com/stream?streams={}", streams);
        let ws_manager = WebSocketManager::new(&url);
        let exchange_id = self.exchange_id.clone();
        let timeframe_str = timeframe.to_string();

        info!("Subscribing to Binance candles: {}", streams);

        tokio::spawn(async move {
            let res = ws_manager
                .connect_with_retry(
                    |ws| async { Ok((ws, Ok(()))) },
                    |msg| {
                        let tx = tx.clone();
                        let exchange_id = exchange_id.clone();
                        let timeframe_str = timeframe_str.clone();
                        async move {
                            if let Message::Text(text) = msg {
                                let v: Value = serde_json::from_str(&text)?;
                                if let Some(data) = v.get("data") {
                                    if data.get("e") == Some(&Value::String("kline".to_string())) {
                                        let k = data.get("k").context("k")?;
                                        let candle = Candlestick {
                                            product_id: format!(
                                                "{}:{}-PERP",
                                                exchange_id,
                                                data.get("s")
                                                    .context("s")?
                                                    .as_str()
                                                    .context("s")?
                                            ),
                                            timeframe: timeframe_str,
                                            timestamp: k
                                                .get("t")
                                                .context("t")?
                                                .as_i64()
                                                .context("t")?,
                                            open: k
                                                .get("o")
                                                .context("o")?
                                                .as_str()
                                                .context("o")?
                                                .parse::<Decimal>()?,
                                            high: k
                                                .get("h")
                                                .context("h")?
                                                .as_str()
                                                .context("h")?
                                                .parse::<Decimal>()?,
                                            low: k
                                                .get("l")
                                                .context("l")?
                                                .as_str()
                                                .context("l")?
                                                .parse::<Decimal>()?,
                                            close: k
                                                .get("c")
                                                .context("c")?
                                                .as_str()
                                                .context("c")?
                                                .parse::<Decimal>()?,
                                            volume: k
                                                .get("v")
                                                .context("v")?
                                                .as_str()
                                                .context("v")?
                                                .parse::<Decimal>()?,
                                        };
                                        if let Err(e) = candle.validate() {
                                            warn!("Invalid candle received: {}", e);
                                        } else {
                                            tx.send(candle).await.ok();
                                        }
                                    }
                                }
                            }
                            Ok(())
                        }
                    },
                )
                .await;

            if let Err(e) = res {
                error!("Binance candles subscription failed: {}", e);
            }
        });

        Ok(())
    }

    async fn fetch_recent_candles(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: u32,
    ) -> Result<Vec<Candlestick>> {
        let url = format!(
            "https://fapi.binance.com/fapi/v1/klines?symbol={}&interval={}&limit={}",
            symbol, timeframe, limit
        );
        let client = reqwest::Client::new();
        let res = client.get(url).send().await?.json::<Value>().await?;

        let mut candles = Vec::new();
        if let Some(arr) = res.as_array() {
            for k in arr {
                let candle = Candlestick {
                    product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
                    timeframe: timeframe.to_string(),
                    timestamp: k[0].as_i64().context("t")?,
                    open: k[1].as_str().context("o")?.parse()?,
                    high: k[2].as_str().context("h")?.parse()?,
                    low: k[3].as_str().context("l")?.parse()?,
                    close: k[4].as_str().context("c")?.parse()?,
                    volume: k[5].as_str().context("v")?.parse()?,
                };
                candles.push(candle);
            }
        }
        Ok(candles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_binance_parse_kline() {
        let connector = BinanceConnector::new();
        let kline_json = json!({
            "e": "kline",
            "E": 123456789,
            "s": "BTCUSDT",
            "k": {
                "t": 1600000000000i64,
                "T": 1600000059999i64,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 100,
                "L": 200,
                "o": "50000.00",
                "c": "50500.00",
                "h": "51000.00",
                "l": "49000.00",
                "v": "10.5",
                "n": 10,
                "x": false,
                "q": "500000.00",
                "V": "5.0",
                "Q": "250000.00",
                "B": "0"
            }
        });

        let candle = connector.parse_kline(&kline_json).unwrap();
        assert_eq!(candle.product_id, "BINANCE:BTCUSDT-PERP");
        assert_eq!(candle.timeframe, "1m");
        assert_eq!(candle.open, "50000.00".parse::<Decimal>().unwrap());
    }

    #[test]
    fn test_binance_parse_trade() {
        let connector = BinanceConnector::new();
        let trade_json = json!({
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 12345,
            "p": "50000.00",
            "q": "0.100",
            "f": 100,
            "l": 105,
            "T": 1600000000000i64,
            "m": true,
            "M": true
        });

        let trade = connector.parse_trade(&trade_json).unwrap();
        assert_eq!(trade.product_id, "BINANCE:BTCUSDT-PERP");
        assert_eq!(trade.price, "50000.00".parse::<Decimal>().unwrap());
        assert_eq!(trade.side, "sell");
    }
}
