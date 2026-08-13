use crate::connector::ws::WebSocketManager;
use crate::connector::ExchangeConnector;
use crate::model::{AccountUpdate, Candlestick, OrderBook, PositionUpdate, Trade, UserStreamEvent};
use anyhow::{Context, Result};
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json::Value;
use std::env;
use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

const LISTEN_KEY_ACQUIRED_MESSAGE: &str = "Obtained Binance listen key";

#[derive(Debug)]
pub(crate) struct BinanceTaskFailure {
    task: &'static str,
    stable_error_code: &'static str,
    safe_cause: &'static str,
    source: Option<anyhow::Error>,
}

impl BinanceTaskFailure {
    pub(crate) fn task_error(task: &'static str, source: anyhow::Error) -> Self {
        Self {
            task,
            stable_error_code: "binance_stream_task_failed",
            safe_cause: "Binance stream task failed",
            source: Some(source),
        }
    }

    pub(crate) fn unexpected_exit(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "binance_stream_task_exited",
            safe_cause: "Binance stream task exited unexpectedly",
            source: None,
        }
    }

    pub(crate) fn panicked(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "binance_stream_task_panicked",
            safe_cause: "Binance stream task panicked",
            source: None,
        }
    }

    pub(crate) fn cancelled(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "binance_stream_task_cancelled",
            safe_cause: "Binance stream task was cancelled",
            source: None,
        }
    }

    fn join_failed(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "binance_stream_task_join_failed",
            safe_cause: "Binance stream task join failed",
            source: None,
        }
    }

    fn monitor_closed() -> Self {
        Self {
            task: "task_monitor",
            stable_error_code: "binance_task_monitor_closed",
            safe_cause: "Binance task monitor closed unexpectedly",
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

impl std::fmt::Display for BinanceTaskFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.safe_cause)
    }
}

impl std::error::Error for BinanceTaskFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
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
    let Some(value) = lookup("BINANCE_API_KEY") else {
        return Ok(false);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }
    anyhow::ensure!(
        trimmed == value,
        "BINANCE_API_KEY must not contain surrounding whitespace"
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
    let mut connector = BinanceConnector::with_task_exit_tx(task_exit_tx);
    info!(symbols = ?symbols, "Starting Binance Connector");
    subscribe_market_data(&mut connector, &symbols, trade_tx, candle_tx).await?;

    if user_stream_enabled {
        connector.subscribe_user_stream(user_tx).await?;
    } else {
        info!("BINANCE_API_KEY not found, skipping User Data Stream");
    }

    await_task_exit(&mut task_exit_rx).await
}

async fn await_task_exit(
    task_exit_rx: &mut mpsc::UnboundedReceiver<BinanceTaskFailure>,
) -> Result<()> {
    match task_exit_rx.recv().await {
        Some(failure) => Err(failure.into()),
        None => Err(BinanceTaskFailure::monitor_closed().into()),
    }
}

#[allow(dead_code)]
pub struct BinanceConnector {
    ws_manager: WebSocketManager,
    exchange_id: String,
    http_client: reqwest::Client,
    base_url: String,
    task_exit_tx: Option<mpsc::UnboundedSender<BinanceTaskFailure>>,
}

fn parse_trade_from_json(v: &Value, exchange_id: &str) -> Result<Trade> {
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
        product_id: format!("{}:{}-PERP", exchange_id, symbol),
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

impl BinanceConnector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            ws_manager: WebSocketManager::new("wss://fstream.binance.com/ws"),
            exchange_id: "BINANCE".to_string(),
            http_client: reqwest::Client::new(),
            base_url: "https://fapi.binance.com".to_string(),
            task_exit_tx: None,
        }
    }

    fn with_task_exit_tx(task_exit_tx: mpsc::UnboundedSender<BinanceTaskFailure>) -> Self {
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
                Ok(Ok(())) => BinanceTaskFailure::unexpected_exit(task_name),
                Ok(Err(error)) => BinanceTaskFailure::task_error(task_name, error),
                Err(error) if error.is_panic() => BinanceTaskFailure::panicked(task_name),
                Err(error) if error.is_cancelled() => BinanceTaskFailure::cancelled(task_name),
                Err(_) => BinanceTaskFailure::join_failed(task_name),
            };
            if let Some(task_exit_tx) = task_exit_tx {
                let _ = task_exit_tx.send(failure);
            } else {
                error!(
                    task = failure.task(),
                    stable_error_code = failure.stable_error_code(),
                    safe_cause = failure.safe_cause(),
                    "Binance connector task failed"
                );
            }
        });
        abort_handle
    }

    async fn keep_listen_key_alive(
        http_client: reqwest::Client,
        base_url: String,
        api_key: String,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(1800));
        loop {
            interval.tick().await;
            let url = format!("{base_url}/fapi/v1/listenKey");
            match http_client
                .put(&url)
                .header("X-MBX-APIKEY", &api_key)
                .send()
                .await
            {
                Ok(_) => info!("Refreshed Binance ListenKey"),
                Err(error) => error!("Failed to refresh ListenKey: {error}"),
            }
        }
    }

    async fn get_listen_key(&self, api_key: &str) -> Result<String> {
        let url = format!("{}/fapi/v1/listenKey", self.base_url);
        let res = self
            .http_client
            .post(&url)
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await?
            .json::<Value>()
            .await?;

        res.get("listenKey")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .context("Failed to get listenKey from response")
    }

    #[allow(dead_code)]
    pub async fn subscribe_user_stream(&self, tx: mpsc::Sender<UserStreamEvent>) -> Result<()> {
        let api_key = env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;
        let listen_key = self.get_listen_key(&api_key).await?;
        info!("{LISTEN_KEY_ACQUIRED_MESSAGE}");

        let http_client = self.http_client.clone();
        let base_url = self.base_url.clone();
        let api_key_keep = api_key.clone();
        let _keepalive = self.spawn_task(
            "listen_key_keepalive",
            Self::keep_listen_key_alive(http_client, base_url, api_key_keep),
        );

        // Connect to WS
        let url = format!("wss://fstream.binance.com/ws/{}", listen_key);
        let ws_manager = WebSocketManager::new(&url);
        let exchange_id = self.exchange_id.clone();

        info!("Subscribing to Binance User Stream");

        let _user_stream = self.spawn_task("user_stream", async move {
            ws_manager
                .connect_with_retry(
                    |ws| async { Ok((ws, Ok(()))) },
                    |msg| {
                        let tx = tx.clone();
                        let exchange_id = exchange_id.clone();
                        async move {
                            if let Message::Text(text) = msg {
                                let v: Value = serde_json::from_str(&text)?;
                                if let Some(event) = v.get("e").and_then(|e| e.as_str()) {
                                    if event == "ACCOUNT_UPDATE" {
                                        if let Some(a) = v.get("a") {
                                            // Process Balances
                                            if let Some(balances) =
                                                a.get("B").and_then(|b| b.as_array())
                                            {
                                                for b in balances {
                                                    let asset = b
                                                        .get("a")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or_default();
                                                    let wallet_balance = b
                                                        .get("wb")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("0");
                                                    let update = AccountUpdate {
                                                        exchange: exchange_id.clone(),
                                                        asset: asset.to_string(),
                                                        balance: wallet_balance
                                                            .parse()
                                                            .unwrap_or(Decimal::ZERO),
                                                        timestamp: v
                                                            .get("E")
                                                            .and_then(|t| t.as_i64())
                                                            .unwrap_or(0),
                                                    };
                                                    tx.send(UserStreamEvent::Account(update))
                                                        .await
                                                        .ok();
                                                }
                                            }
                                            // Process Positions
                                            if let Some(positions) =
                                                a.get("P").and_then(|p| p.as_array())
                                            {
                                                for p in positions {
                                                    let symbol = p
                                                        .get("s")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or_default();
                                                    let amount = p
                                                        .get("pa")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("0");
                                                    let entry_price = p
                                                        .get("ep")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("0");
                                                    let upnl = p
                                                        .get("up")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("0");

                                                    let update = PositionUpdate {
                                                        exchange: exchange_id.clone(),
                                                        symbol: symbol.to_string(),
                                                        amount: amount
                                                            .parse()
                                                            .unwrap_or(Decimal::ZERO),
                                                        entry_price: entry_price
                                                            .parse()
                                                            .unwrap_or(Decimal::ZERO),
                                                        unrealized_pnl: upnl
                                                            .parse()
                                                            .unwrap_or(Decimal::ZERO),
                                                        timestamp: v
                                                            .get("E")
                                                            .and_then(|t| t.as_i64())
                                                            .unwrap_or(0),
                                                    };
                                                    tx.send(UserStreamEvent::Position(update))
                                                        .await
                                                        .ok();
                                                }
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
        parse_trade_from_json(v, &self.exchange_id)
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

        let _trades = self.spawn_task("trades", async move {
            ws_manager
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
                                        let trade = parse_trade_from_json(data, &exchange_id)?;
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
                .await
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

        let _candles = self.spawn_task("candles", async move {
            ws_manager
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
    fn binance_owner_preflights_its_optional_user_stream_key() {
        for value in [None, Some(""), Some("   ")] {
            assert!(!preflight_user_stream_credentials(|_| value.map(str::to_string)).unwrap());
        }
        assert!(preflight_user_stream_credentials(|_| Some("binance-key".to_string())).unwrap());
        assert_eq!(
            preflight_user_stream_credentials(|_| Some(" binance-key ".to_string()))
                .unwrap_err()
                .to_string(),
            "BINANCE_API_KEY must not contain surrounding whitespace"
        );
    }

    #[derive(Default)]
    struct RecordingConnector {
        calls: Vec<(&'static str, Vec<String>, String)>,
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
            self.calls.push(("trades", symbols.to_vec(), String::new()));
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
            self.calls
                .push(("candles", symbols.to_vec(), timeframe.to_string()));
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
    async fn binance_owner_subscribes_exact_symbols_in_runtime_order() {
        let symbols = vec!["BTCUSDT".to_string(), "SOLUSDC".to_string()];
        let (trade_tx, _) = mpsc::channel(1);
        let (candle_tx, _) = mpsc::channel(1);
        let mut connector = RecordingConnector::default();

        subscribe_market_data(&mut connector, &symbols, trade_tx, candle_tx)
            .await
            .unwrap();

        assert_eq!(
            connector.calls,
            vec![
                ("trades", symbols.clone(), String::new()),
                ("candles", symbols, "1m".to_string()),
            ]
        );
    }

    #[test]
    fn binance_listen_key_log_is_fixed_and_cannot_render_the_key() {
        assert_eq!(LISTEN_KEY_ACQUIRED_MESSAGE, "Obtained Binance listen key");
        assert!(!LISTEN_KEY_ACQUIRED_MESSAGE.contains("{}"));
        let source = include_str!("binance.rs");
        assert!(source.contains("info!(\"{LISTEN_KEY_ACQUIRED_MESSAGE}\")"));
        let forbidden = ["Obtained Binance ", "ListenKey", ":"].concat();
        assert!(!source.contains(&forbidden));
    }

    #[test]
    fn generic_main_delegates_binance_runtime_to_the_connector_composition_owner() {
        let main_product = include_str!("../main.rs")
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;
        let runtime_product = include_str!("live_runtime.rs")
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;

        assert!(main_product.contains("connector::live_runtime::LiveRuntime"));
        assert!(!main_product.contains("connector::binance::run("));
        assert!(!main_product.contains("BinanceConnector::new"));
        assert_eq!(runtime_product.matches("super::binance::run(").count(), 1);
    }

    #[tokio::test]
    async fn internal_task_error_reaches_the_binance_runtime_owner() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BinanceConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("trades", async {
            Err(anyhow::anyhow!("provider failure sentinel"))
        });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();

        let failure = error.downcast_ref::<BinanceTaskFailure>().unwrap();
        assert_eq!(failure.task(), "trades");
        assert_eq!(failure.stable_error_code(), "binance_stream_task_failed");
        assert_eq!(failure.safe_cause(), "Binance stream task failed");
        assert_eq!(
            error.chain().map(ToString::to_string).collect::<Vec<_>>(),
            ["Binance stream task failed", "provider failure sentinel"]
        );
    }

    #[tokio::test]
    async fn clean_internal_task_exit_is_not_treated_as_healthy() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BinanceConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("candles", async { Ok(()) });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();

        let failure = error.downcast_ref::<BinanceTaskFailure>().unwrap();
        assert_eq!(failure.task(), "candles");
        assert_eq!(failure.stable_error_code(), "binance_stream_task_exited");
        assert_eq!(
            failure.safe_cause(),
            "Binance stream task exited unexpectedly"
        );
    }

    #[tokio::test]
    async fn panicked_internal_task_has_a_fixed_safe_error() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BinanceConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("user_stream", async {
            panic!("provider panic payload sentinel")
        });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();
        let failure = error.downcast_ref::<BinanceTaskFailure>().unwrap();
        assert_eq!(failure.task(), "user_stream");
        assert_eq!(failure.stable_error_code(), "binance_stream_task_panicked");
        let rendered = error.to_string();
        assert_eq!(rendered, "Binance stream task panicked");
        assert!(!rendered.contains("provider panic payload sentinel"));
    }

    #[tokio::test]
    async fn cancelled_internal_task_has_a_fixed_safe_error() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BinanceConnector::with_task_exit_tx(task_exit_tx);
        let task = connector.spawn_task("listen_key_keepalive", async {
            std::future::pending::<Result<()>>().await
        });
        task.abort();

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();

        let failure = error.downcast_ref::<BinanceTaskFailure>().unwrap();
        assert_eq!(failure.task(), "listen_key_keepalive");
        assert_eq!(failure.stable_error_code(), "binance_stream_task_cancelled");
        assert_eq!(failure.safe_cause(), "Binance stream task was cancelled");
    }

    #[test]
    fn production_owner_does_not_hide_behind_an_unconditional_pending_future() {
        let source = include_str!("binance.rs");
        let production = source.split_once("#[cfg(test)]").unwrap().0;
        assert!(!production.contains("std::future::pending::<()>().await;"));
    }

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
