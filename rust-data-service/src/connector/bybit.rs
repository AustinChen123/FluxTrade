use crate::connector::ws::WebSocketManager;
use crate::connector::ExchangeConnector;
use crate::model::{Candlestick, OrderBook, Trade};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::SinkExt;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use std::future::Future;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{error, info, warn};

#[derive(Debug)]
pub(crate) struct BybitTaskFailure {
    task: &'static str,
    stable_error_code: &'static str,
    safe_cause: &'static str,
    source: Option<anyhow::Error>,
}

impl BybitTaskFailure {
    pub(crate) fn task_error(task: &'static str, source: anyhow::Error) -> Self {
        Self {
            task,
            stable_error_code: "bybit_stream_task_failed",
            safe_cause: "Bybit stream task failed",
            source: Some(source),
        }
    }

    pub(crate) fn unexpected_exit(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "bybit_stream_task_exited",
            safe_cause: "Bybit stream task exited unexpectedly",
            source: None,
        }
    }

    pub(crate) fn panicked(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "bybit_stream_task_panicked",
            safe_cause: "Bybit stream task panicked",
            source: None,
        }
    }

    pub(crate) fn cancelled(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "bybit_stream_task_cancelled",
            safe_cause: "Bybit stream task was cancelled",
            source: None,
        }
    }

    fn join_failed(task: &'static str) -> Self {
        Self {
            task,
            stable_error_code: "bybit_stream_task_join_failed",
            safe_cause: "Bybit stream task join failed",
            source: None,
        }
    }

    fn monitor_closed() -> Self {
        Self {
            task: "task_monitor",
            stable_error_code: "bybit_task_monitor_closed",
            safe_cause: "Bybit task monitor closed unexpectedly",
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

impl std::fmt::Display for BybitTaskFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.safe_cause)
    }
}

impl std::error::Error for BybitTaskFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
}

pub(crate) async fn run(
    symbols: Vec<String>,
    trade_tx: mpsc::Sender<Trade>,
    candle_tx: mpsc::Sender<Candlestick>,
) -> Result<()> {
    let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
    let mut connector = BybitConnector::with_task_exit_tx(task_exit_tx);
    info!(symbols = ?symbols, "Starting Bybit Connector");
    connector.subscribe_trades(&symbols, trade_tx).await?;
    connector
        .subscribe_candles(&symbols, "1m", candle_tx)
        .await?;
    await_task_exit(&mut task_exit_rx).await
}

async fn await_task_exit(
    task_exit_rx: &mut mpsc::UnboundedReceiver<BybitTaskFailure>,
) -> Result<()> {
    match task_exit_rx.recv().await {
        Some(failure) => Err(failure.into()),
        None => Err(BybitTaskFailure::monitor_closed().into()),
    }
}

pub struct BybitConnector {
    exchange_id: String,
    task_exit_tx: Option<mpsc::UnboundedSender<BybitTaskFailure>>,
}

impl BybitConnector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            exchange_id: "BYBIT".to_string(),
            task_exit_tx: None,
        }
    }

    fn with_task_exit_tx(task_exit_tx: mpsc::UnboundedSender<BybitTaskFailure>) -> Self {
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
                Ok(Ok(())) => BybitTaskFailure::unexpected_exit(task_name),
                Ok(Err(error)) => BybitTaskFailure::task_error(task_name, error),
                Err(error) if error.is_panic() => BybitTaskFailure::panicked(task_name),
                Err(error) if error.is_cancelled() => BybitTaskFailure::cancelled(task_name),
                Err(_) => BybitTaskFailure::join_failed(task_name),
            };
            if let Some(task_exit_tx) = task_exit_tx {
                let _ = task_exit_tx.send(failure);
            } else {
                error!(
                    task = failure.task(),
                    stable_error_code = failure.stable_error_code(),
                    safe_cause = failure.safe_cause(),
                    "Bybit connector task failed"
                );
            }
        });
        abort_handle
    }

    #[allow(dead_code)]
    fn parse_trade(&self, data: &Value) -> Result<Trade> {
        let symbol = data.get("s").context("s")?.as_str().context("s not str")?;

        Ok(Trade {
            id: data
                .get("i")
                .context("i")?
                .as_str()
                .context("i not str")?
                .to_string(),
            product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
            price: data
                .get("p")
                .context("p")?
                .as_str()
                .context("p not str")?
                .parse::<Decimal>()?,
            quantity: data
                .get("v")
                .context("v")?
                .as_str()
                .context("v not str")?
                .parse::<Decimal>()?,
            side: data
                .get("S")
                .context("S")?
                .as_str()
                .context("S not str")?
                .to_lowercase(),
            timestamp: data.get("T").context("T")?.as_i64().context("T not i64")?,
        })
    }
}

async fn forward_bybit_kline(
    connector_id: &str,
    topic: &str,
    data: &Value,
    tx: &mpsc::Sender<Candlestick>,
) -> Result<()> {
    match data.get("confirm").and_then(Value::as_bool) {
        Some(false) => return Ok(()),
        Some(true) => {}
        None => anyhow::bail!("Bybit kline close flag is missing or invalid"),
    }
    let parts: Vec<&str> = topic.split('.').collect();
    let symbol = parts.get(2).unwrap_or(&"UNKNOWN");
    let timeframe = parts.get(1).unwrap_or(&"1");
    let candle = Candlestick {
        product_id: format!("{}:{}-PERP", connector_id, symbol),
        timeframe: timeframe.to_string(),
        timestamp: data
            .get("start")
            .context("start")?
            .as_i64()
            .context("start")?,
        open: data
            .get("open")
            .context("open")?
            .as_str()
            .context("open")?
            .parse()?,
        high: data
            .get("high")
            .context("high")?
            .as_str()
            .context("high")?
            .parse()?,
        low: data
            .get("low")
            .context("low")?
            .as_str()
            .context("low")?
            .parse()?,
        close: data
            .get("close")
            .context("close")?
            .as_str()
            .context("close")?
            .parse()?,
        volume: data
            .get("volume")
            .context("volume")?
            .as_str()
            .context("volume")?
            .parse()?,
    };
    if let Err(error) = candle.validate() {
        warn!("Invalid Bybit candle: {}", error);
    } else {
        tx.send(candle).await.ok();
    }
    Ok(())
}

#[async_trait]
impl ExchangeConnector for BybitConnector {
    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn subscribe_trades(
        &mut self,
        symbols: &[String],
        tx: mpsc::Sender<Trade>,
    ) -> Result<()> {
        let url = "wss://stream.bybit.com/v5/public/linear";
        let ws_manager = WebSocketManager::new(url);
        let connector_id = self.exchange_id.clone();

        let args: Vec<String> = symbols
            .iter()
            .map(|s| format!("publicTrade.{}", s))
            .collect();

        info!("Subscribing to Bybit trades: {:?}", args);

        self.spawn_task("trades", async move {
            ws_manager
                .connect_with_retry(
                    |mut ws| {
                        let args = args.clone();
                        async move {
                            let sub = json!({
                                "op": "subscribe",
                                "args": args
                            });
                            let res = ws
                                .send(Message::Text(sub.to_string().into()))
                                .await
                                .map_err(|e| anyhow::anyhow!(e));
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
                                        if let Some(data_list) =
                                            v.get("data").and_then(|d| d.as_array())
                                        {
                                            for data in data_list {
                                                let trade = Trade {
                                                    id: data
                                                        .get("i")
                                                        .context("i")?
                                                        .as_str()
                                                        .context("i")?
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
                                                        .get("v")
                                                        .context("v")?
                                                        .as_str()
                                                        .context("v")?
                                                        .parse::<Decimal>()?,
                                                    side: data
                                                        .get("S")
                                                        .context("S")?
                                                        .as_str()
                                                        .context("S")?
                                                        .to_lowercase(),
                                                    timestamp: data
                                                        .get("T")
                                                        .context("T")?
                                                        .as_i64()
                                                        .context("T")?,
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

        let args: Vec<String> = symbols
            .iter()
            .map(|s| format!("kline.{}.{}", bybit_tf, s))
            .collect();

        info!("Subscribing to Bybit candles: {:?}", args);

        self.spawn_task("candles", async move {
            ws_manager
                .connect_with_retry(
                    |mut ws| {
                        let args = args.clone();
                        async move {
                            let sub = json!({
                                "op": "subscribe",
                                "args": args
                            });
                            let res = ws
                                .send(Message::Text(sub.to_string().into()))
                                .await
                                .map_err(|e| anyhow::anyhow!(e));
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
                                        if let Some(data_list) =
                                            v.get("data").and_then(|d| d.as_array())
                                        {
                                            for data in data_list {
                                                forward_bybit_kline(
                                                    &connector_id,
                                                    topic,
                                                    data,
                                                    &tx,
                                                )
                                                .await?;
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

    async fn fetch_recent_candles(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: u32,
    ) -> Result<Vec<Candlestick>> {
        // Bybit V5: GET /v5/market/kline
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
        let url = format!(
            "https://api.bybit.com/v5/market/kline?category=linear&symbol={}&interval={}&limit={}",
            symbol, bybit_tf, limit
        );
        let client = reqwest::Client::new();
        let res = client.get(url).send().await?.json::<Value>().await?;

        let mut candles = Vec::new();
        if let Some(list) = res
            .get("result")
            .and_then(|r| r.get("list"))
            .and_then(|l| l.as_array())
        {
            for k in list {
                // Bybit returns: [start, open, high, low, close, volume, turnover]
                let candle = Candlestick {
                    product_id: format!("{}:{}-PERP", self.exchange_id, symbol),
                    timeframe: timeframe.to_string(),
                    timestamp: k[0].as_str().context("t")?.parse()?,
                    open: k[1].as_str().context("o")?.parse()?,
                    high: k[2].as_str().context("h")?.parse()?,
                    low: k[3].as_str().context("l")?.parse()?,
                    close: k[4].as_str().context("c")?.parse()?,
                    volume: k[5].as_str().context("v")?.parse()?,
                };
                candles.push(candle);
            }
        }
        // Bybit returns newest first, we want oldest first for consistency
        candles.reverse();
        Ok(candles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test]
    async fn final_internal_task_error_reaches_the_connector_owner() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BybitConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("trades", async {
            Err(anyhow::anyhow!("provider error sentinel"))
        });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();
        let failure = error.downcast_ref::<BybitTaskFailure>().unwrap();
        assert_eq!(failure.task(), "trades");
        assert_eq!(failure.stable_error_code(), "bybit_stream_task_failed");
        assert_eq!(failure.safe_cause(), "Bybit stream task failed");
        assert_eq!(error.to_string(), "Bybit stream task failed");
        assert!(!error.to_string().contains("provider error sentinel"));
    }

    #[tokio::test]
    async fn clean_internal_task_exit_is_not_treated_as_healthy() {
        let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
        let connector = BybitConnector::with_task_exit_tx(task_exit_tx);
        let _task = connector.spawn_task("candles", async { Ok(()) });

        let error =
            tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                .await
                .unwrap()
                .unwrap_err();
        let failure = error.downcast_ref::<BybitTaskFailure>().unwrap();
        assert_eq!(failure.task(), "candles");
        assert_eq!(failure.stable_error_code(), "bybit_stream_task_exited");
        assert_eq!(
            failure.safe_cause(),
            "Bybit stream task exited unexpectedly"
        );
    }

    #[tokio::test]
    async fn panic_and_cancellation_have_fixed_safe_errors() {
        for (kind, expected_code, expected_cause) in [
            (
                "panic",
                "bybit_stream_task_panicked",
                "Bybit stream task panicked",
            ),
            (
                "cancel",
                "bybit_stream_task_cancelled",
                "Bybit stream task was cancelled",
            ),
        ] {
            let (task_exit_tx, mut task_exit_rx) = mpsc::unbounded_channel();
            let connector = BybitConnector::with_task_exit_tx(task_exit_tx);
            let task = if kind == "panic" {
                connector.spawn_task("trades", async {
                    panic!("provider panic payload sentinel")
                })
            } else {
                connector.spawn_task("trades", async {
                    std::future::pending::<Result<()>>().await
                })
            };
            if kind == "cancel" {
                task.abort();
            }

            let error =
                tokio::time::timeout(Duration::from_secs(1), await_task_exit(&mut task_exit_rx))
                    .await
                    .unwrap()
                    .unwrap_err();
            let failure = error.downcast_ref::<BybitTaskFailure>().unwrap();
            assert_eq!(failure.stable_error_code(), expected_code);
            assert_eq!(failure.safe_cause(), expected_cause);
            assert_eq!(error.to_string(), expected_cause);
            assert!(!error
                .to_string()
                .contains("provider panic payload sentinel"));
        }
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
        let failure = error.downcast_ref::<BybitTaskFailure>().unwrap();
        assert_eq!(failure.task(), "task_monitor");
        assert_eq!(failure.stable_error_code(), "bybit_task_monitor_closed");
        assert_eq!(
            failure.safe_cause(),
            "Bybit task monitor closed unexpectedly"
        );
    }

    #[test]
    fn production_owner_does_not_hide_or_double_log_final_task_failures() {
        let source = include_str!("bybit.rs");
        let production = source.split_once("#[cfg(test)]").unwrap().0;
        let main = include_str!("../main.rs")
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;
        let runtime = include_str!("live_runtime.rs")
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;
        assert_eq!(production.matches("tokio::spawn").count(), 2);
        assert_eq!(production.matches("self.spawn_task(").count(), 2);
        for monitored in ["self.spawn_task(\"trades\"", "self.spawn_task(\"candles\""] {
            assert!(production.contains(monitored), "{monitored}");
        }
        for legacy in [
            "Bybit trades subscription failed:",
            "Bybit candles subscription failed:",
        ] {
            assert!(!production.contains(legacy), "{legacy}");
        }
        assert!(!main.contains("crate::connector::bybit::run("));
        assert!(!main.contains("run_bybit_connector"));
        assert!(!main.contains("BybitConnector::new"));
        assert_eq!(runtime.matches("super::bybit::run(").count(), 1);
        assert_eq!(production.matches("forward_bybit_kline(").count(), 2);
    }

    #[tokio::test]
    async fn bybit_forwards_only_provider_closed_klines() {
        let (tx, mut rx) = mpsc::channel(4);
        let topic = "kline.1.BTCUSDT";
        let mut data = json!({
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

        forward_bybit_kline("BYBIT", topic, &data, &tx)
            .await
            .unwrap();
        forward_bybit_kline("BYBIT", topic, &data, &tx)
            .await
            .unwrap();
        assert!(rx.try_recv().is_err());

        data["confirm"] = json!(true);
        forward_bybit_kline("BYBIT", topic, &data, &tx)
            .await
            .unwrap();
        let candle = rx.try_recv().unwrap();
        assert_eq!(candle.product_id, "BYBIT:BTCUSDT-PERP");
        assert_eq!(candle.timeframe, "1");
        assert_eq!(candle.timestamp, 1672324800000i64);
        assert_eq!(candle.open, "16599.4".parse::<Decimal>().unwrap());
        assert_eq!(candle.high, "16599.4".parse::<Decimal>().unwrap());
        assert_eq!(candle.low, "16599.3".parse::<Decimal>().unwrap());
        assert_eq!(candle.close, "16599.3".parse::<Decimal>().unwrap());
        assert_eq!(candle.volume, "0.1".parse::<Decimal>().unwrap());
        assert!(rx.try_recv().is_err());

        for invalid in [Value::Null, json!("true")] {
            data["confirm"] = invalid;
            let error = forward_bybit_kline("BYBIT", topic, &data, &tx)
                .await
                .unwrap_err();
            assert_eq!(
                error.to_string(),
                "Bybit kline close flag is missing or invalid"
            );
            assert!(rx.try_recv().is_err());
        }
        data.as_object_mut().unwrap().remove("confirm");
        assert_eq!(
            forward_bybit_kline("BYBIT", topic, &data, &tx)
                .await
                .unwrap_err()
                .to_string(),
            "Bybit kline close flag is missing or invalid"
        );
        assert!(rx.try_recv().is_err());
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
