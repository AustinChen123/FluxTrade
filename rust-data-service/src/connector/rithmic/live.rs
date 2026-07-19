use super::{
    bar::MinuteBarBuilder,
    config,
    market::{self, MarketDataEvent, SubscriptionAction},
    session::Plant,
    transport::{self, ReconnectPolicy},
};
use crate::model::{validate_product_id, Candlestick};
use anyhow::{ensure, Context, Result};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const FORWARD_QUEUE_CAPACITY: usize = 60;

pub(crate) struct LiveConfig {
    runtime: config::RuntimeConfig,
    startup: Vec<u8>,
    policy: ReconnectPolicy,
    product_id: String,
    exchange: String,
    symbol: String,
}

pub(crate) fn configure(
    profile: &str,
    product_id: String,
    exchange: String,
    symbol: String,
) -> Result<LiveConfig> {
    validate_instrument(&product_id, &exchange, &symbol)?;
    let runtime = config::load(profile, Plant::Ticker)?;
    let startup = market::last_trade_request(&exchange, &symbol, SubscriptionAction::Subscribe)?;
    let policy = ReconnectPolicy::new(INITIAL_BACKOFF, MAX_BACKOFF)?;

    Ok(LiveConfig {
        runtime,
        startup,
        policy,
        product_id,
        exchange,
        symbol,
    })
}

pub(crate) async fn run(config: LiveConfig, candle_tx: mpsc::Sender<Candlestick>) -> Result<()> {
    let (forward_tx, forward_rx) = mpsc::channel(FORWARD_QUEUE_CAPACITY);
    let handler = Arc::new(Mutex::new(LivePayloadHandler {
        builder: MinuteBarBuilder::new(config.product_id, config.exchange, config.symbol)?,
        candle_tx: forward_tx,
    }));
    let reconnect_handler = Arc::clone(&handler);
    let payload_handler = Arc::clone(&handler);

    let transport = transport::run_with_reconnect(
        &config.runtime.url,
        config.runtime.login,
        RESPONSE_TIMEOUT,
        config.policy,
        vec![config.startup],
        move || {
            reconnect_handler
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic live handler lock poisoned"))?
                .reset();
            Ok(())
        },
        move |payload| {
            payload_handler
                .lock()
                .map_err(|_| anyhow::anyhow!("Rithmic live handler lock poisoned"))?
                .handle(&payload)
        },
    );

    tokio::select! {
        result = transport => result,
        result = forward_candles(forward_rx, candle_tx) => result,
    }
}

struct LivePayloadHandler {
    builder: MinuteBarBuilder,
    candle_tx: mpsc::Sender<Candlestick>,
}

impl LivePayloadHandler {
    fn reset(&mut self) {
        self.builder.reset();
    }

    fn handle(&mut self, payload: &[u8]) -> Result<()> {
        match market::decode_market_data_event(payload)? {
            MarketDataEvent::LastTrade(trade) => {
                if let Some(candle) = self.builder.push(&trade)? {
                    self.candle_tx
                        .try_send(candle)
                        .map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => {
                                anyhow::anyhow!("Rithmic candle forwarding queue exhausted")
                            }
                            mpsc::error::TrySendError::Closed(_) => {
                                anyhow::anyhow!("Rithmic candle forwarding queue closed")
                            }
                        })?;
                }
                Ok(())
            }
            MarketDataEvent::SubscriptionAccepted | MarketDataEvent::LastTradeCleared => Ok(()),
            MarketDataEvent::Rejected { .. } => {
                anyhow::bail!("Rithmic market-data subscription was rejected")
            }
        }
    }
}

async fn forward_candles(
    mut source: mpsc::Receiver<Candlestick>,
    destination: mpsc::Sender<Candlestick>,
) -> Result<()> {
    while let Some(candle) = source.recv().await {
        destination
            .send(candle)
            .await
            .context("Rithmic candle destination is closed")?;
    }
    anyhow::bail!("Rithmic candle forwarding queue closed")
}

fn validate_instrument(product_id: &str, exchange: &str, symbol: &str) -> Result<()> {
    validate_product_id(product_id)?;
    ensure!(
        product_id.starts_with("RITHMIC:"),
        "Rithmic live product ID must use RITHMIC venue"
    );
    ensure!(
        !exchange.trim().is_empty(),
        "Rithmic exchange must not be empty"
    );
    ensure!(
        !symbol.trim().is_empty(),
        "Rithmic symbol must not be empty"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::rithmic::{
        codec, config::RuntimeConfig, protocol, session::LoginParameters,
    };
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message, WebSocketStream};

    #[test]
    fn explicit_instrument_identity_validation_matrix() {
        assert!(validate_instrument("RITHMIC:NQ-202609", "CME", "NQU6").is_ok());

        for (product_id, exchange, symbol) in [
            ("CME:NQ-202609", "CME", "NQU6"),
            ("RITHMIC:NQ", "CME", "NQU6"),
            ("RITHMIC:NQ-202609", "", "NQU6"),
            ("RITHMIC:NQ-202609", "CME", ""),
        ] {
            assert!(validate_instrument(product_id, exchange, symbol).is_err());
        }
    }

    #[test]
    fn live_payload_handler_emits_completed_canonical_minute() {
        let (mut handler, mut candle_rx) = handler();

        handler.handle(&last_trade(1_800_000_001)).unwrap();
        handler.handle(&last_trade(1_800_000_061)).unwrap();

        let candle = candle_rx.try_recv().unwrap();
        assert_eq!(candle.product_id, "RITHMIC:NQ-202609");
        assert_eq!(candle.timestamp, 1_800_000_000_000);
        assert_eq!(candle.timeframe, "1m");
    }

    #[test]
    fn live_payload_handler_rejects_subscription_failure() {
        let (mut handler, _) = handler();
        let rejected = codec::encode(&protocol::Reject {
            template_id: 75,
            rp_code: vec!["permission-denied".to_string()],
            ..Default::default()
        })
        .unwrap();

        assert!(handler.handle(&rejected).is_err());
    }

    #[test]
    fn reconnect_reset_discards_partial_minute() {
        let (mut handler, mut candle_rx) = handler();
        handler.handle(&last_trade(1_800_000_001)).unwrap();
        handler.reset();
        handler.handle(&last_trade(1_800_000_061)).unwrap();
        assert!(candle_rx.try_recv().is_err());

        handler.handle(&last_trade(1_800_000_121)).unwrap();
        assert_eq!(candle_rx.try_recv().unwrap().timestamp, 1_800_000_060_000);
    }

    #[tokio::test]
    async fn reconnect_replays_subscription_and_discards_pre_disconnect_partial_minute() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let mut socket = serve_handshake(&listener).await;
                send(&mut socket, heartbeat_response()).await;
                assert_template(socket.next().await.unwrap().unwrap(), 100);
                send(
                    &mut socket,
                    codec::encode(&protocol::ResponseMarketDataUpdate {
                        template_id: 101,
                        rp_code: vec!["0".to_string()],
                        ..Default::default()
                    })
                    .unwrap(),
                )
                .await;

                if attempt == 0 {
                    send(&mut socket, last_trade(1_800_000_001)).await;
                } else {
                    send(&mut socket, last_trade(1_800_000_061)).await;
                    send(&mut socket, last_trade(1_800_000_121)).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        });
        let startup =
            market::last_trade_request("CME", "NQU6", SubscriptionAction::Subscribe).unwrap();
        let config = LiveConfig {
            runtime: RuntimeConfig {
                url,
                login: LoginParameters::new(
                    "test-user".to_string(),
                    "test-password".to_string(),
                    "test-system".to_string(),
                    "FluxTrade".to_string(),
                    "0.1.0".to_string(),
                    Plant::Ticker,
                )
                .unwrap(),
            },
            startup,
            policy: ReconnectPolicy::new(Duration::from_millis(1), Duration::from_millis(10))
                .unwrap(),
            product_id: "RITHMIC:NQ-202609".to_string(),
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
        };
        let (candle_tx, mut candle_rx) = mpsc::channel(2);
        let connector = tokio::spawn(run(config, candle_tx));

        let candle = timeout(Duration::from_secs(2), candle_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(candle.timestamp, 1_800_000_060_000);
        assert!(candle_rx.try_recv().is_err());

        connector.abort();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn forwarder_waits_for_bounded_destination_capacity() {
        let (source_tx, source_rx) = mpsc::channel(1);
        let (destination_tx, mut destination_rx) = mpsc::channel(1);
        destination_tx.send(candle(1)).await.unwrap();
        source_tx.send(candle(2)).await.unwrap();

        let forwarder = tokio::spawn(forward_candles(source_rx, destination_tx));
        tokio::task::yield_now().await;
        assert!(!forwarder.is_finished());
        assert_eq!(destination_rx.recv().await.unwrap().timestamp, 1);
        assert_eq!(destination_rx.recv().await.unwrap().timestamp, 2);

        drop(source_tx);
        assert!(forwarder.await.unwrap().is_err());
    }

    #[test]
    fn full_forwarding_queue_fails_closed_without_dropping_buffered_candle() {
        let (mut handler, mut candle_rx) = handler_with_capacity(1);
        handler.handle(&last_trade(1_800_000_001)).unwrap();
        handler.handle(&last_trade(1_800_000_061)).unwrap();

        let error = handler.handle(&last_trade(1_800_000_121)).unwrap_err();
        assert!(error.to_string().contains("queue exhausted"));
        assert_eq!(candle_rx.try_recv().unwrap().timestamp, 1_800_000_000_000);
        assert!(candle_rx.try_recv().is_err());
    }

    fn handler() -> (LivePayloadHandler, mpsc::Receiver<Candlestick>) {
        handler_with_capacity(FORWARD_QUEUE_CAPACITY)
    }

    fn handler_with_capacity(capacity: usize) -> (LivePayloadHandler, mpsc::Receiver<Candlestick>) {
        let (candle_tx, candle_rx) = mpsc::channel(capacity);
        (
            LivePayloadHandler {
                builder: MinuteBarBuilder::new(
                    "RITHMIC:NQ-202609".to_string(),
                    "CME".to_string(),
                    "NQU6".to_string(),
                )
                .unwrap(),
                candle_tx,
            },
            candle_rx,
        )
    }

    fn candle(timestamp: i64) -> Candlestick {
        Candlestick {
            product_id: "RITHMIC:NQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp,
            open: rust_decimal_macros::dec!(1),
            high: rust_decimal_macros::dec!(1),
            low: rust_decimal_macros::dec!(1),
            close: rust_decimal_macros::dec!(1),
            volume: rust_decimal_macros::dec!(1),
        }
    }

    fn last_trade(ssboe: i32) -> Vec<u8> {
        codec::encode(&protocol::LastTrade {
            template_id: 150,
            presence_bits: Some(1),
            exchange: Some("CME".to_string()),
            symbol: Some("NQU6".to_string()),
            trade_price: Some(29_784.75),
            trade_size: Some(1),
            ssboe: Some(ssboe),
            usecs: Some(0),
            ..Default::default()
        })
        .unwrap()
    }

    async fn serve_handshake(listener: &TcpListener) -> WebSocketStream<TcpStream> {
        let (stream, _) = listener.accept().await.unwrap();
        let mut discovery = accept_async(stream).await.unwrap();
        assert_template(discovery.next().await.unwrap().unwrap(), 16);
        send(
            &mut discovery,
            codec::encode(&protocol::ResponseRithmicSystemInfo {
                template_id: 17,
                rp_code: vec!["0".to_string()],
                system_name: vec!["test-system".to_string()],
                ..Default::default()
            })
            .unwrap(),
        )
        .await;

        let (stream, _) = listener.accept().await.unwrap();
        let mut login = accept_async(stream).await.unwrap();
        assert_template(login.next().await.unwrap().unwrap(), 10);
        send(
            &mut login,
            codec::encode(&protocol::ResponseLogin {
                template_id: 11,
                rp_code: vec!["0".to_string()],
                heartbeat_interval: Some(30.0),
                ..Default::default()
            })
            .unwrap(),
        )
        .await;
        assert_template(login.next().await.unwrap().unwrap(), 18);
        login
    }

    fn heartbeat_response() -> Vec<u8> {
        codec::encode(&protocol::ResponseHeartbeat {
            template_id: 19,
            rp_code: vec!["0".to_string()],
            ..Default::default()
        })
        .unwrap()
    }

    async fn send(socket: &mut WebSocketStream<TcpStream>, payload: Vec<u8>) {
        socket.send(Message::Binary(payload.into())).await.unwrap();
    }

    fn assert_template(message: Message, expected: i32) {
        let Message::Binary(payload) = message else {
            panic!("expected binary Rithmic message");
        };
        assert_eq!(codec::template_id(&payload).unwrap(), expected);
    }
}
