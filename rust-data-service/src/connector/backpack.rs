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
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

const DEFAULT_MARKET_DATA_SYMBOLS: &str = "BTC_USDC_PERP,SOL_USDC_PERP";

#[derive(Debug)]
pub(crate) struct BackpackTaskFailure {
    task: &'static str,
    stable_error_code: &'static str,
    safe_cause: &'static str,
    source: Option<anyhow::Error>,
}

impl BackpackTaskFailure {
    pub(crate) fn task_error(task: &'static str, source: anyhow::Error) -> Self {
        Self {
            task,
            stable_error_code: "backpack_stream_task_failed",
            safe_cause: "Backpack stream task failed",
            source: Some(source),
        }
    }

    pub(crate) fn unexpected_exit(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "backpack_stream_task_exited",
            safe_cause: "Backpack stream task exited unexpectedly",
            source: None,
        }
    }

    pub(crate) fn panicked(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "backpack_stream_task_panicked",
            safe_cause: "Backpack stream task panicked",
            source: None,
        }
    }

    pub(crate) fn cancelled(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "backpack_stream_task_cancelled",
            safe_cause: "Backpack stream task was cancelled",
            source: None,
        }
    }

    fn join_failed(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "backpack_stream_task_join_failed",
            safe_cause: "Backpack stream task join failed",
            source: None,
        }
    }

    fn monitor_closed() -> Self {
        Self {
            task: "task_monitor",
            stable_error_code: "backpack_task_monitor_closed",
            safe_cause: "Backpack task monitor closed unexpectedly",
            source: None,
        }
    }

    pub(crate) fn task(&self) -> &'static str {
        self.task
    }

    pub(crate) fn stable_error_code(&self) -> &'static str {
        self.stable_error_code
    }

    pub(crate) fn safe_cause(&self) -> &'static str {
        self.safe_cause
    }
}

impl std::fmt::Display for BackpackTaskFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.safe_cause)
    }
}

impl std::error::Error for BackpackTaskFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
}

fn product_id_from_market_symbol(symbol: &str) -> Result<String> {
    let contract = symbol
        .strip_suffix("_PERP")
        .context("unsupported Backpack market type")?;
    let parts = contract.split('_').collect::<Vec<_>>();
    anyhow::ensure!(
        parts.len() == 2
            && parts
                .iter()
                .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_alphanumeric())),
        "invalid Backpack perpetual market symbol: {symbol}"
    );
    Ok(format!("BACKPACK:{contract}-PERP"))
}

pub(crate) fn resolve_market_data_symbols(configured: Option<&str>) -> Result<Vec<String>> {
    let raw = configured.unwrap_or(DEFAULT_MARKET_DATA_SYMBOLS);
    let symbols = raw
        .split(',')
        .map(str::trim)
        .filter(|symbol| !symbol.is_empty())
        .map(str::to_uppercase)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !symbols.is_empty(),
        "BACKPACK_MARKET_DATA_SYMBOLS must contain at least one symbol"
    );
    anyhow::ensure!(
        symbols.len()
            == symbols
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
        "BACKPACK_MARKET_DATA_SYMBOLS must not contain duplicates"
    );
    for symbol in &symbols {
        product_id_from_market_symbol(symbol)?;
    }
    Ok(symbols)
}

async fn subscribe_market_data(
    connector: &mut impl ExchangeConnector,
    symbols: &[String],
    trade_tx: mpsc::Sender<Trade>,
    candle_tx: mpsc::Sender<Candlestick>,
) -> Result<()> {
    connector.subscribe_trades(symbols, trade_tx).await?;
    connector.subscribe_candles(symbols, "1m", candle_tx).await
}

pub(crate) fn preflight_user_stream_credentials(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<bool> {
    const NAMES: [&str; 2] = ["EXCHANGE_API_KEY", "EXCHANGE_SECRET"];
    let values = NAMES.map(lookup);
    let mut present = 0;
    for (name, value) in NAMES.into_iter().zip(&values) {
        let Some(value) = value else {
            continue;
        };
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        anyhow::ensure!(
            trimmed == value,
            "{name} must not contain surrounding whitespace"
        );
        present += 1;
    }
    if present == 0 {
        return Ok(false);
    }
    anyhow::ensure!(
        present == NAMES.len(),
        "optional credentials must be provided together: {}",
        NAMES.join(", ")
    );
    Ok(true)
}

pub(crate) async fn run(
    symbols: Vec<String>,
    trade_tx: mpsc::Sender<Trade>,
    candle_tx: mpsc::Sender<Candlestick>,
    user_tx: mpsc::Sender<UserStreamEvent>,
    user_stream_enabled: bool,
) -> Result<()> {
    let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
    let mut connector = BackpackConnector::with_task_exit_tx(task_exit_tx);
    info!(symbols = ?symbols, "Starting Backpack Connector");
    subscribe_market_data(&mut connector, &symbols, trade_tx, candle_tx).await?;

    if user_stream_enabled {
        connector.subscribe_user_stream(user_tx).await?;
    } else {
        info!("Backpack API Key/Secret not found, skipping User Data Stream");
    }

    await_task_exit(&mut task_exit_rx).await
}

async fn await_task_exit(
    task_exit_rx: &mut mpsc::UnboundedReceiver<BackpackTaskFailure>,
) -> Result<()> {
    match task_exit_rx.recv().await {
        Some(failure) => Err(failure.into()),
        None => Err(BackpackTaskFailure::monitor_closed().into()),
    }
}

pub struct BackpackConnector {
    exchange_id: String,
    task_exit_tx: Option<mpsc::UnboundedSender<BackpackTaskFailure>>,
}

impl BackpackConnector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            exchange_id: "BACKPACK".to_string(),
            task_exit_tx: None,
        }
    }

    fn with_task_exit_tx(task_exit_tx: mpsc::UnboundedSender<BackpackTaskFailure>) -> Self {
        Self {
            task_exit_tx: Some(task_exit_tx),
            ..Self::new()
        }
    }

    fn spawn_task<F>(&self, task_name: &'static str, future: F) -> AbortHandle
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let task = tokio::spawn(future);
        let abort_handle = task.abort_handle();
        let task_exit_tx = self.task_exit_tx.clone();
        tokio::spawn(async move {
            let failure = match task.await {
                Ok(Ok(())) => BackpackTaskFailure::unexpected_exit(task_name),
                Ok(Err(error)) => BackpackTaskFailure::task_error(task_name, error),
                Err(error) if error.is_panic() => BackpackTaskFailure::panicked(task_name),
                Err(error) if error.is_cancelled() => BackpackTaskFailure::cancelled(task_name),
                Err(_) => BackpackTaskFailure::join_failed(task_name),
            };
            if let Some(task_exit_tx) = task_exit_tx {
                let _ = task_exit_tx.send(failure);
            } else {
                error!(
                    task = failure.task(),
                    stable_error_code = failure.stable_error_code(),
                    safe_cause = failure.safe_cause(),
                    "Backpack connector task failed"
                );
            }
        });
        abort_handle
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

        let _user_stream = self.spawn_task("user_stream", async move {
            ws_manager
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
                .await
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

async fn forward_backpack_kline(
    timeframe: &str,
    data: &Value,
    tx: &mpsc::Sender<Candlestick>,
) -> Result<()> {
    match data.get("X").and_then(Value::as_bool) {
        Some(false) => return Ok(()),
        Some(true) => {}
        None => anyhow::bail!("Backpack kline close flag is missing or invalid"),
    }
    let timestamp = BackpackConnector::parse_iso8601_to_ms(
        data.get("t").context("t")?.as_str().context("t not str")?,
    )?;
    let candle = Candlestick {
        product_id: product_id_from_market_symbol(
            data.get("s").context("s")?.as_str().context("s")?,
        )?,
        timeframe: timeframe.to_string(),
        timestamp,
        open: data.get("o").context("o")?.as_str().context("o")?.parse()?,
        high: data.get("h").context("h")?.as_str().context("h")?.parse()?,
        low: data.get("l").context("l")?.as_str().context("l")?.parse()?,
        close: data.get("c").context("c")?.as_str().context("c")?.parse()?,
        volume: data.get("v").context("v")?.as_str().context("v")?.parse()?,
    };
    if let Err(error) = candle.validate() {
        warn!("Invalid Backpack candle: {}", error);
    } else {
        tx.send(candle).await.ok();
    }
    Ok(())
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

        let args: Vec<String> = symbols.iter().map(|s| format!("trade.{}", s)).collect();

        info!("Subscribing to Backpack trades: {:?}", args);

        let _trades = self.spawn_task("trades", async move {
            ws_manager
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
                                            product_id: product_id_from_market_symbol(
                                                data.get("s")
                                                    .context("s")?
                                                    .as_str()
                                                    .context("s")?,
                                            )?,
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
                .await
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

        let args: Vec<String> = symbols
            .iter()
            .map(|s| format!("kline.{}.{}", timeframe, s))
            .collect();

        let timeframe_str = timeframe.to_string();

        info!("Subscribing to Backpack candles: {:?}", args);

        let _candles = self.spawn_task("candles", async move {
            ws_manager
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
                        let timeframe_str = timeframe_str.clone();
                        async move {
                            if let Message::Text(text) = msg {
                                let v: Value = serde_json::from_str(&text)?;
                                if let Some(data) = v.get("data") {
                                    if data.get("e") == Some(&Value::String("kline".to_string())) {
                                        forward_backpack_kline(&timeframe_str, data, &tx).await?;
                                    }
                                }
                            }
                            Ok(())
                        }
                    },
                )
                .await
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
    use async_trait::async_trait;
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn backpack_owner_preflights_its_optional_user_stream_key_pair() {
        let lookup = |key: Option<&str>, secret: Option<&str>| {
            preflight_user_stream_credentials(|name| match name {
                "EXCHANGE_API_KEY" => key.map(str::to_string),
                "EXCHANGE_SECRET" => secret.map(str::to_string),
                _ => unreachable!(),
            })
        };

        assert!(!lookup(None, None).unwrap());
        assert!(!lookup(Some(""), Some("   ")).unwrap());
        assert!(lookup(Some("backpack-key"), Some("backpack-secret")).unwrap());
        for (key, secret) in [
            (Some("backpack-key"), None),
            (None, Some("backpack-secret")),
            (Some("   "), Some("backpack-secret")),
        ] {
            assert_eq!(
                lookup(key, secret).unwrap_err().to_string(),
                "optional credentials must be provided together: EXCHANGE_API_KEY, EXCHANGE_SECRET"
            );
        }
        assert_eq!(
            lookup(Some(" backpack-key "), Some("backpack-secret"))
                .unwrap_err()
                .to_string(),
            "EXCHANGE_API_KEY must not contain surrounding whitespace"
        );
        assert_eq!(
            lookup(Some("backpack-key"), Some(" backpack-secret "))
                .unwrap_err()
                .to_string(),
            "EXCHANGE_SECRET must not contain surrounding whitespace"
        );
    }

    #[test]
    fn backpack_market_data_symbols_are_provider_owned_and_fail_closed() {
        assert_eq!(
            resolve_market_data_symbols(None).unwrap(),
            ["BTC_USDC_PERP", "SOL_USDC_PERP"]
        );
        assert_eq!(
            resolve_market_data_symbols(Some(" sui_usdc_perp,eth_usdc_perp ")).unwrap(),
            ["SUI_USDC_PERP", "ETH_USDC_PERP"]
        );

        for raw in [
            "",
            "   ",
            "BTCUSDT",
            "BTC_USDC",
            "BTC_USDC_RFQ",
            "BTC_USDC_IPERP",
            "BTC_USDC_PERP,BTC_USDC_PERP",
            "BTC-_USDC_PERP",
        ] {
            assert!(resolve_market_data_symbols(Some(raw)).is_err(), "{raw}");
        }
    }

    #[test]
    fn backpack_perpetual_market_symbol_projects_to_canonical_product_identity() {
        assert_eq!(
            product_id_from_market_symbol("SOL_USDC_PERP").unwrap(),
            "BACKPACK:SOL_USDC-PERP"
        );
        for unsupported in ["SOL_USDC", "SOL_USDC_RFQ", "SOL_USDC_IPERP"] {
            assert!(product_id_from_market_symbol(unsupported).is_err());
        }
    }

    #[derive(Default)]
    struct RecordingConnector {
        trade_symbols: Vec<String>,
        candle_symbols: Vec<String>,
        candle_timeframe: String,
    }

    #[async_trait]
    impl ExchangeConnector for RecordingConnector {
        async fn connect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn subscribe_trades(
            &mut self,
            symbols: &[String],
            _tx: mpsc::Sender<Trade>,
        ) -> Result<()> {
            self.trade_symbols = symbols.to_vec();
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
            _tx: mpsc::Sender<Candlestick>,
        ) -> Result<()> {
            self.candle_symbols = symbols.to_vec();
            self.candle_timeframe = timeframe.to_string();
            Ok(())
        }

        async fn fetch_recent_candles(
            &self,
            _symbol: &str,
            _timeframe: &str,
            _limit: u32,
        ) -> Result<Vec<Candlestick>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn backpack_market_data_subscriptions_receive_the_exact_configured_symbols() {
        let symbols = vec!["SUI_USDC_PERP".to_string(), "ETH_USDC_PERP".to_string()];
        let (trade_tx, _) = mpsc::channel(1);
        let (candle_tx, _) = mpsc::channel(1);
        let mut connector = RecordingConnector::default();

        subscribe_market_data(&mut connector, &symbols, trade_tx, candle_tx)
            .await
            .unwrap();

        assert_eq!(connector.trade_symbols, symbols);
        assert_eq!(connector.candle_symbols, symbols);
        assert_eq!(connector.candle_timeframe, "1m");
    }

    #[tokio::test]
    async fn internal_task_error_reaches_the_backpack_runtime_owner() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BackpackConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("trades", async {
            Err(anyhow::anyhow!("provider failure sentinel"))
        });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();

        let failure = error.downcast_ref::<BackpackTaskFailure>().unwrap();
        assert_eq!(failure.task(), "trades");
        assert_eq!(failure.stable_error_code(), "backpack_stream_task_failed");
        assert_eq!(failure.safe_cause(), "Backpack stream task failed");
        assert_eq!(
            error.chain().map(ToString::to_string).collect::<Vec<_>>(),
            ["Backpack stream task failed", "provider failure sentinel"]
        );
    }

    #[tokio::test]
    async fn clean_internal_task_exit_is_not_treated_as_healthy() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BackpackConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("candles", async { Ok(()) });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();

        let failure = error.downcast_ref::<BackpackTaskFailure>().unwrap();
        assert_eq!(failure.task(), "candles");
        assert_eq!(failure.stable_error_code(), "backpack_stream_task_exited");
        assert_eq!(
            failure.safe_cause(),
            "Backpack stream task exited unexpectedly"
        );
    }

    #[tokio::test]
    async fn panicked_internal_task_has_a_fixed_safe_error() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BackpackConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("user_stream", async {
            panic!("provider panic payload sentinel")
        });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();
        let failure = error.downcast_ref::<BackpackTaskFailure>().unwrap();
        assert_eq!(failure.task(), "user_stream");
        assert_eq!(failure.stable_error_code(), "backpack_stream_task_panicked");
        let rendered = error.to_string();
        assert_eq!(rendered, "Backpack stream task panicked");
        assert!(!rendered.contains("provider panic payload sentinel"));
    }

    #[tokio::test]
    async fn cancelled_internal_task_has_a_fixed_safe_error() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BackpackConnector::with_task_exit_tx(task_exit_tx);
        let task = connector.spawn_task("trades", async {
            std::future::pending::<Result<()>>().await
        });
        task.abort();

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();

        let failure = error.downcast_ref::<BackpackTaskFailure>().unwrap();
        assert_eq!(failure.task(), "trades");
        assert_eq!(
            failure.stable_error_code(),
            "backpack_stream_task_cancelled"
        );
        assert_eq!(failure.safe_cause(), "Backpack stream task was cancelled");
    }

    #[tokio::test]
    async fn closed_task_monitor_is_not_treated_as_healthy() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        drop(task_exit_tx);

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();
        let failure = error.downcast_ref::<BackpackTaskFailure>().unwrap();
        assert_eq!(failure.task(), "task_monitor");
        assert_eq!(failure.stable_error_code(), "backpack_task_monitor_closed");
        assert_eq!(
            failure.safe_cause(),
            "Backpack task monitor closed unexpectedly"
        );
    }

    #[test]
    fn production_owner_does_not_hide_or_double_log_final_task_failures() {
        let source = include_str!("backpack.rs");
        let production = source.split_once("#[cfg(test)]").unwrap().0;
        assert!(!production.contains("std::future::pending::<()>().await;"));
        assert_eq!(production.matches("tokio::spawn").count(), 2);
        assert_eq!(production.matches("self.spawn_task(").count(), 3);
        for monitored in [
            "self.spawn_task(\"user_stream\"",
            "self.spawn_task(\"trades\"",
            "self.spawn_task(\"candles\"",
        ] {
            assert!(production.contains(monitored), "{monitored}");
        }
        for legacy in [
            "Backpack User Stream failed:",
            "Backpack trades subscription failed:",
            "Backpack candles subscription failed:",
        ] {
            assert!(!production.contains(legacy), "{legacy}");
        }
        assert_eq!(production.matches("forward_backpack_kline(").count(), 2);
    }

    #[test]
    fn test_backpack_parse_iso8601() {
        let ts = BackpackConnector::parse_iso8601_to_ms("2026-01-22T21:19:00").unwrap();
        // 2026-01-22 21:19:00 UTC
        assert_eq!(ts, 1769116740000i64);
    }

    #[tokio::test]
    async fn backpack_forwards_only_provider_closed_klines() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut data = json!({
            "e": "kline",
            "s": "SOL_USDC_PERP",
            "t": "2026-01-22T21:19:00",
            "o": "128.50",
            "h": "129.00",
            "l": "128.25",
            "c": "128.75",
            "v": "10.5",
            "X": false
        });

        forward_backpack_kline("1m", &data, &tx).await.unwrap();
        forward_backpack_kline("1m", &data, &tx).await.unwrap();
        assert!(rx.try_recv().is_err());

        data["X"] = json!(true);
        forward_backpack_kline("1m", &data, &tx).await.unwrap();
        let candle = rx.try_recv().unwrap();
        assert_eq!(candle.product_id, "BACKPACK:SOL_USDC-PERP");
        assert_eq!(candle.timeframe, "1m");
        assert_eq!(candle.timestamp, 1769116740000i64);
        assert_eq!(candle.open, "128.50".parse::<Decimal>().unwrap());
        assert_eq!(candle.high, "129.00".parse::<Decimal>().unwrap());
        assert_eq!(candle.low, "128.25".parse::<Decimal>().unwrap());
        assert_eq!(candle.close, "128.75".parse::<Decimal>().unwrap());
        assert_eq!(candle.volume, "10.5".parse::<Decimal>().unwrap());
        assert!(rx.try_recv().is_err());

        for invalid in [Value::Null, json!("true")] {
            data["X"] = invalid;
            let error = forward_backpack_kline("1m", &data, &tx).await.unwrap_err();
            assert_eq!(
                error.to_string(),
                "Backpack kline close flag is missing or invalid"
            );
            assert!(rx.try_recv().is_err());
        }
        data.as_object_mut().unwrap().remove("X");
        assert_eq!(
            forward_backpack_kline("1m", &data, &tx)
                .await
                .unwrap_err()
                .to_string(),
            "Backpack kline close flag is missing or invalid"
        );
        assert!(rx.try_recv().is_err());
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
            "s": "SOL_USDC_PERP",
            "t": 379172947i64
        });

        // Mocking the behavior inside subscribe_trades closure
        let trade = Trade {
            id: data.get("t").unwrap().as_i64().unwrap().to_string(),
            product_id: product_id_from_market_symbol(data.get("s").unwrap().as_str().unwrap())
                .unwrap(),
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
