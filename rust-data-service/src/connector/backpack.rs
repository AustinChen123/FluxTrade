use crate::connector::ws::WebSocketManager;
use crate::connector::ExchangeConnector;
use crate::model::{AccountUpdate, Candlestick, OrderBook, PositionUpdate, Trade, UserStreamEvent};
use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::DateTime;
use futures_util::SinkExt;
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

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

    fn sign(instruction: &str, timestamp: &str, window: &str, secret: &str) -> Result<String> {
        let payload = format!(
            "instruction={}&timestamp={}&window={}",
            instruction, timestamp, window
        );

        let secret_bytes = BASE64.decode(secret).context("Failed to decode secret")?;
        let key_pair = Ed25519KeyPair::from_seed_unchecked(&secret_bytes)
            .or_else(|_| Ed25519KeyPair::from_pkcs8(&secret_bytes))
            .map_err(|_| anyhow::anyhow!("Invalid Ed25519 secret key"))?;

        let signature = key_pair.sign(payload.as_bytes());
        Ok(BASE64.encode(signature.as_ref()))
    }

    #[allow(dead_code)]
    pub async fn cancel_all_orders(&self) -> Result<()> {
        let api_key = std::env::var("EXCHANGE_API_KEY").context("EXCHANGE_API_KEY not set")?;
        let secret = std::env::var("EXCHANGE_SECRET").context("EXCHANGE_SECRET not set")?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();
        let window = "5000";
        let instruction = "cancelAllOrders";

        let signature_base64 = Self::sign(instruction, &timestamp, window, &secret)?;

        info!("Watchdog: Executing cancelAllOrders on Backpack...");

        let client = reqwest::Client::new();
        let response = client
            .delete("https://api.backpack.exchange/api/v1/orders")
            .query(&[
                ("instruction", instruction),
                ("timestamp", &timestamp),
                ("window", window),
            ])
            .header("X-API-Key", api_key)
            .header("X-Timestamp", timestamp)
            .header("X-Window", window)
            .header("X-Signature", signature_base64)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            info!("Watchdog: Successfully cancelled all orders on Backpack.");
        } else {
            let text = response.text().await?;
            error!("Watchdog: Failed to cancel orders: {}", text);
            anyhow::bail!("Failed to cancel orders: {}", text);
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn subscribe_user_stream(&self, tx: mpsc::Sender<UserStreamEvent>) -> Result<()> {
        let api_key = std::env::var("EXCHANGE_API_KEY").context("EXCHANGE_API_KEY not set")?;
        let secret = std::env::var("EXCHANGE_SECRET").context("EXCHANGE_SECRET not set")?;

        let url = "wss://ws.backpack.exchange/";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();

        info!("Subscribing to Backpack User Stream");

        tokio::spawn(async move {
            let res = ws_manager
                .connect_with_retry(
                    |mut ws| {
                        let api_key = api_key.clone();
                        let secret = secret.clone();
                        async move {
                            let timestamp = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis()
                                .to_string();
                            let window = "5000";
                            let instruction = "subscribe";

                            let signature =
                                match Self::sign(instruction, &timestamp, window, &secret) {
                                    Ok(s) => s,
                                    Err(e) => return Err(anyhow::anyhow!(e)),
                                };

                            let sub = json!({
                                "method": "SUBSCRIBE",
                                "params": ["account.update"],
                                "signature": [api_key, signature, timestamp, window]
                            });

                            ws.send(Message::Text(sub.to_string().into()))
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;
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
                                    if data.get("e")
                                        == Some(&Value::String("account.update".to_string()))
                                    {
                                        let timestamp =
                                            v.get("T").and_then(|t| t.as_i64()).unwrap_or(0); // Assuming T is common

                                        // Process Balances
                                        if let Some(balances) =
                                            data.get("B").and_then(|b| b.as_object())
                                        {
                                            for (asset, info) in balances {
                                                let available = info
                                                    .get("available")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("0");
                                                let locked = info
                                                    .get("locked")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("0");

                                                let avail_dec: Decimal =
                                                    available.parse().unwrap_or(Decimal::ZERO);
                                                let locked_dec: Decimal =
                                                    locked.parse().unwrap_or(Decimal::ZERO);

                                                let update = AccountUpdate {
                                                    exchange: connector_id.clone(),
                                                    asset: asset.to_string(),
                                                    balance: avail_dec + locked_dec,
                                                    timestamp,
                                                };
                                                tx.send(UserStreamEvent::Account(update))
                                                    .await
                                                    .ok();
                                            }
                                        }

                                        // Process Positions
                                        if let Some(positions) =
                                            data.get("P").and_then(|p| p.as_array())
                                        {
                                            for p in positions {
                                                let symbol = p
                                                    .get("s")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or_default();
                                                let amount = p
                                                    .get("n")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("0"); // n = net size
                                                let entry_price = p
                                                    .get("e")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("0"); // e = entry
                                                let upnl = p
                                                    .get("u")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("0"); // u = upnl

                                                let update = PositionUpdate {
                                                    exchange: connector_id.clone(),
                                                    symbol: symbol.to_string(),
                                                    amount: amount.parse().unwrap_or(Decimal::ZERO),
                                                    entry_price: entry_price
                                                        .parse()
                                                        .unwrap_or(Decimal::ZERO),
                                                    unrealized_pnl: upnl
                                                        .parse()
                                                        .unwrap_or(Decimal::ZERO),
                                                    timestamp,
                                                };
                                                tx.send(UserStreamEvent::Position(update))
                                                    .await
                                                    .ok();
                                            }
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
                error!("Backpack User Stream failed: {}", e);
            }
        });

        Ok(())
    }

    fn parse_iso8601_to_ms(iso: &str) -> Result<i64> {
        // Backpack returns "2026-01-22T21:19:00" or similar.

        // We ensure it is treated as UTC.

        let rfc = if iso.contains('Z') || iso.contains('+') {
            iso.to_string()
        } else {
            format!("{}Z", iso)
        };

        let dt = rfc
            .parse::<DateTime<chrono::Utc>>()
            .context("Failed to parse ISO8601 timestamp")?;

        Ok(dt.timestamp_millis())
    }
}

#[async_trait]
impl ExchangeConnector for BackpackConnector {
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn subscribe_trades(
        &mut self,
        symbols: &[String],
        tx: mpsc::Sender<Trade>,
    ) -> Result<()> {
        let url = "wss://ws.backpack.exchange/";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();

        let args: Vec<String> = symbols.iter().map(|s| format!("trade.{}", s)).collect();

        info!("Subscribing to Backpack trades: {:?}", args);

        tokio::spawn(async move {
            let res = ws_manager
                .connect_with_retry(
                    |mut ws| {
                        let args = args.clone();
                        async move {
                            let sub = json!({
                                "method": "SUBSCRIBE",
                                "params": args
                            });
                            ws.send(Message::Text(sub.to_string().into()))
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;
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
                                            id: data
                                                .get("t")
                                                .context("t")?
                                                .as_i64()
                                                .context("t")?
                                                .to_string(),
                                            product_id: format!(
                                                "{}:{}-PERP",
                                                connector_id,
                                                data.get("s")
                                                    .context("s")?
                                                    .as_str()
                                                    .context("s")?
                                            ),
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
                                                .context("T")?
                                                / 1000, // micro to milli
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
                    },
                )
                .await;

            if let Err(e) = res {
                error!("Backpack trades subscription failed: {}", e);
            }
        });

        Ok(())
    }

    async fn subscribe_orderbook(
        &mut self,
        _symbols: &[String],
        _tx: mpsc::Sender<OrderBook>,
    ) -> Result<()> {
        Ok(())
    }

    async fn subscribe_candles(
        &mut self,
        symbols: &[String],
        timeframe: &str,
        tx: mpsc::Sender<Candlestick>,
    ) -> Result<()> {
        let url = "wss://ws.backpack.exchange/";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();

        let args: Vec<String> = symbols
            .iter()
            .map(|s| format!("kline.{}.{}", timeframe, s))
            .collect();

        let timeframe_str = timeframe.to_string();

        info!("Subscribing to Backpack candles: {:?}", args);

        tokio::spawn(async move {
            let res = ws_manager
                .connect_with_retry(
                    |mut ws| {
                        let args = args.clone();
                        async move {
                            let sub = json!({
                                "method": "SUBSCRIBE",
                                "params": args
                            });
                            ws.send(Message::Text(sub.to_string().into()))
                                .await
                                .map_err(|e| anyhow::anyhow!(e))?;
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
                                        let ts_str = data
                                            .get("t")
                                            .context("t")?
                                            .as_str()
                                            .context("t not str")?;
                                        let timestamp =
                                            BackpackConnector::parse_iso8601_to_ms(ts_str)?;

                                        let candle = Candlestick {
                                            product_id: format!(
                                                "{}:{}-PERP",
                                                connector_id,
                                                data.get("s")
                                                    .context("s")?
                                                    .as_str()
                                                    .context("s")?
                                            ),
                                            timeframe: timeframe_str, // Use the cloned String
                                            timestamp,
                                            open: data
                                                .get("o")
                                                .context("o")?
                                                .as_str()
                                                .context("o")?
                                                .parse::<Decimal>()?,
                                            high: data
                                                .get("h")
                                                .context("h")?
                                                .as_str()
                                                .context("h")?
                                                .parse::<Decimal>()?,
                                            low: data
                                                .get("l")
                                                .context("l")?
                                                .as_str()
                                                .context("l")?
                                                .parse::<Decimal>()?,
                                            close: data
                                                .get("c")
                                                .context("c")?
                                                .as_str()
                                                .context("c")?
                                                .parse::<Decimal>()?,
                                            volume: data
                                                .get("v")
                                                .context("v")?
                                                .as_str()
                                                .context("v")?
                                                .parse::<Decimal>()?,
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
                    },
                )
                .await;

            if let Err(e) = res {
                error!("Backpack candles subscription failed: {}", e);
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
        // Backpack: GET /api/v1/klines
        let url = format!(
            "https://api.backpack.exchange/api/v1/klines?symbol={}&interval={}&limit={}",
            symbol, timeframe, limit
        );
        let client = reqwest::Client::new();
        let res = client.get(url).send().await?.json::<Value>().await?;

        let mut candles = Vec::new();
        if let Some(arr) = res.as_array() {
            for k in arr {
                // Backpack klines array: [timestamp, open, high, low, close, volume, close_timestamp]
                // Note: timestamps are ISO strings in REST too? No, usually numbers.
                // Let's check docs again or handle both.
                let ts = if let Some(s) = k[0].as_str() {
                    BackpackConnector::parse_iso8601_to_ms(s)?
                } else {
                    k[0].as_i64().context("t")?
                };

                let candle = Candlestick {
                    product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
                    timeframe: timeframe.to_string(),
                    timestamp: ts,
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
            price: data
                .get("p")
                .unwrap()
                .as_str()
                .unwrap()
                .parse::<Decimal>()
                .unwrap(),
            quantity: data
                .get("q")
                .unwrap()
                .as_str()
                .unwrap()
                .parse::<Decimal>()
                .unwrap(),
            side: if data.get("m").unwrap().as_bool().unwrap() {
                "sell".to_string()
            } else {
                "buy".to_string()
            },
            timestamp: data.get("T").unwrap().as_i64().unwrap() / 1000,
        };

        assert_eq!(trade.product_id, "BACKPACK:SOL_USDC-PERP");
        assert_eq!(trade.side, "buy");
        assert_eq!(trade.timestamp, 1769116699772i64);
    }
}
