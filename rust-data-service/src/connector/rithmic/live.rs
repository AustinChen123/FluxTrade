use super::{
    bar::MinuteBarBuilder,
    config,
    front_month::{self, FrontMonthEvent},
    market::{self, MarketDataEvent, SubscriptionAction},
    session::Plant,
    transport::{self, ConnectionPreparation, ReconnectPolicy},
};
use crate::model::validate_product_id;
use crate::AggregationSourceEvent;
use anyhow::{ensure, Context, Result};
use chrono::{Datelike, Utc};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tracing::info;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const FORWARD_QUEUE_CAPACITY: usize = 60;

pub(crate) struct LiveConfig {
    runtime: config::RuntimeConfig,
    startup: Vec<Vec<u8>>,
    policy: ReconnectPolicy,
    product_id: String,
    root_symbol: String,
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
    let root_symbol = dated_product_root(&product_id)?;
    let runtime = config::load(profile, Plant::Ticker)?;
    let startup = vec![
        front_month::request_with_updates("live-front-month", &root_symbol, &exchange, true)?,
        market::last_trade_request(&exchange, &symbol, SubscriptionAction::Subscribe)?,
    ];
    let policy = ReconnectPolicy::new(INITIAL_BACKOFF, MAX_BACKOFF)?;

    Ok(LiveConfig {
        runtime,
        startup,
        policy,
        product_id,
        root_symbol,
        exchange,
        symbol,
    })
}

pub(crate) async fn run(
    config: LiveConfig,
    aggregation_source_tx: mpsc::Sender<AggregationSourceEvent>,
) -> Result<()> {
    let (forward_tx, forward_rx) = mpsc::channel(FORWARD_QUEUE_CAPACITY);
    let (front_month_gate_tx, front_month_gate_rx) = watch::channel(FrontMonthGateState::Idle);
    let product_id = config.product_id.clone();
    let handler = Arc::new(Mutex::new(LivePayloadHandler {
        builder: MinuteBarBuilder::new(
            config.product_id,
            config.exchange.clone(),
            config.symbol.clone(),
        )?,
        candle_tx: forward_tx.clone(),
        observed_last_trade: false,
        root_symbol: config.root_symbol,
        exchange: config.exchange,
        symbol: config.symbol,
        front_month_gate: front_month_gate_tx,
    }));
    let lifecycle_handler = Arc::clone(&handler);
    let payload_handler = Arc::clone(&handler);

    let transport = transport::run_with_reconnect(
        &config.runtime.url,
        config.runtime.login,
        RESPONSE_TIMEOUT,
        config.policy,
        config.startup,
        move |preparation| {
            prepare_live_connection(&lifecycle_handler, &forward_tx, &product_id, preparation)
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
        result = forward_events(forward_rx, aggregation_source_tx) => result,
        result = enforce_front_month_deadline(front_month_gate_rx) => result,
    }
}

fn prepare_live_connection(
    handler: &Arc<Mutex<LivePayloadHandler>>,
    forward_tx: &mpsc::Sender<AggregationSourceEvent>,
    product_id: &str,
    preparation: ConnectionPreparation,
) -> Result<()> {
    let mut handler = handler
        .lock()
        .map_err(|_| anyhow::anyhow!("Rithmic live handler lock poisoned"))?;
    match preparation {
        ConnectionPreparation::Startup => {
            handler.reset();
            forward_tx
                .try_send(AggregationSourceEvent::ResetProduct(product_id.to_string()))
                .context("Rithmic aggregation reset queue is unavailable")?;
        }
        ConnectionPreparation::Retry => handler.suspend(),
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrontMonthGateState {
    Idle,
    Awaiting { deadline: Instant },
    Verified,
}

async fn enforce_front_month_deadline(
    mut state_rx: watch::Receiver<FrontMonthGateState>,
) -> Result<()> {
    loop {
        let observed = *state_rx.borrow_and_update();
        match observed {
            FrontMonthGateState::Idle | FrontMonthGateState::Verified => {
                state_rx
                    .changed()
                    .await
                    .context("Rithmic front-month gate closed")?
            }
            FrontMonthGateState::Awaiting { deadline } => {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {
                        ensure!(
                            *state_rx.borrow() != observed,
                            "Rithmic front-month verification timed out"
                        );
                    }
                    changed = state_rx.changed() => {
                        changed.context("Rithmic front-month gate closed")?;
                    }
                }
            }
        }
    }
}

struct LivePayloadHandler {
    builder: MinuteBarBuilder,
    candle_tx: mpsc::Sender<AggregationSourceEvent>,
    observed_last_trade: bool,
    root_symbol: String,
    exchange: String,
    symbol: String,
    front_month_gate: watch::Sender<FrontMonthGateState>,
}

impl LivePayloadHandler {
    fn suspend(&mut self) {
        self.front_month_gate
            .send_replace(FrontMonthGateState::Idle);
    }

    fn reset(&mut self) {
        self.builder.reset();
        self.observed_last_trade = false;
        self.front_month_gate
            .send_replace(FrontMonthGateState::Awaiting {
                deadline: Instant::now() + RESPONSE_TIMEOUT,
            });
    }

    fn handle(&mut self, payload: &[u8]) -> Result<()> {
        if let Some(event) = front_month::decode_live_event(
            payload,
            "live-front-month",
            &self.root_symbol,
            &self.exchange,
            &self.symbol,
        )? {
            return match event {
                FrontMonthEvent::CurrentVerified => {
                    self.front_month_gate
                        .send_replace(FrontMonthGateState::Verified);
                    info!(
                        symbol = self.symbol,
                        "Rithmic configured contract is current front month"
                    );
                    Ok(())
                }
                FrontMonthEvent::RolloverRequired(next_symbol) => anyhow::bail!(
                    "rithmic_rollover_required: configured_symbol={} next_symbol={next_symbol}",
                    self.symbol
                ),
            };
        }
        match market::decode_market_data_event(payload)? {
            MarketDataEvent::LastTrade(trade) => {
                let completed = self.builder.push(&trade)?;
                if *self.front_month_gate.borrow() != FrontMonthGateState::Verified {
                    return Ok(());
                }
                if !self.observed_last_trade {
                    info!(
                        is_snapshot = trade.is_snapshot,
                        "Rithmic first LastTrade update received"
                    );
                    self.observed_last_trade = true;
                }
                if let Some(candle) = completed {
                    info!(
                        timestamp = candle.timestamp,
                        "Rithmic completed minute candle"
                    );
                    self.candle_tx
                        .try_send(AggregationSourceEvent::Candle(candle))
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
            MarketDataEvent::SubscriptionAccepted => {
                info!("Rithmic market-data subscription accepted");
                Ok(())
            }
            MarketDataEvent::LastTradeCleared | MarketDataEvent::LastTradeUnchanged => Ok(()),
            MarketDataEvent::Rejected { response_codes, .. } => {
                anyhow::bail!(
                    "Rithmic market-data subscription was rejected: {}",
                    response_codes.join(",")
                )
            }
        }
    }
}

async fn forward_events(
    mut source: mpsc::Receiver<AggregationSourceEvent>,
    destination: mpsc::Sender<AggregationSourceEvent>,
) -> Result<()> {
    while let Some(event) = source.recv().await {
        destination
            .send(event)
            .await
            .context("Rithmic aggregation event destination is closed")?;
    }
    anyhow::bail!("Rithmic candle forwarding queue closed")
}

fn validate_instrument(product_id: &str, exchange: &str, symbol: &str) -> Result<()> {
    validate_instrument_for_year(product_id, exchange, symbol, Utc::now().year())
}

fn validate_instrument_for_year(
    product_id: &str,
    exchange: &str,
    symbol: &str,
    current_year: i32,
) -> Result<()> {
    validate_product_id(product_id)?;
    ensure!(
        product_id.starts_with("RITHMIC:"),
        "Rithmic live product ID must use RITHMIC venue"
    );
    ensure!(
        exchange.eq_ignore_ascii_case("CME"),
        "Rithmic live market-data validation currently supports CME only"
    );
    ensure!(
        !symbol.trim().is_empty(),
        "Rithmic symbol must not be empty"
    );
    let expiry_year = product_id
        .rsplit_once('-')
        .and_then(|(_, expiry)| expiry.get(..4))
        .and_then(|year| year.parse::<i32>().ok())
        .context("Rithmic product ID has an invalid expiry year")?;
    ensure!(
        (current_year - 1..=current_year + 1).contains(&expiry_year),
        "Rithmic product ID expiry year is outside the live validation window"
    );
    ensure!(
        dated_product_root(product_id)?.eq_ignore_ascii_case("MNQ"),
        "Rithmic live market-data validation currently supports MNQ only"
    );
    ensure!(
        expected_native_symbol(product_id)?.eq_ignore_ascii_case(symbol),
        "Rithmic product ID and native symbol identify different contracts"
    );
    Ok(())
}

fn expected_native_symbol(product_id: &str) -> Result<String> {
    let instrument = product_id
        .strip_prefix("RITHMIC:")
        .context("Rithmic live product ID must use RITHMIC venue")?;
    let (root, expiry) = instrument
        .rsplit_once('-')
        .context("Rithmic live product ID must identify a dated future")?;
    let month_code = match expiry
        .get(4..)
        .context("Rithmic product ID has an invalid expiry month")?
    {
        "01" => 'F',
        "02" => 'G',
        "03" => 'H',
        "04" => 'J',
        "05" => 'K',
        "06" => 'M',
        "07" => 'N',
        "08" => 'Q',
        "09" => 'U',
        "10" => 'V',
        "11" => 'X',
        "12" => 'Z',
        _ => anyhow::bail!("Rithmic product ID has an invalid expiry month"),
    };
    let year_code = expiry
        .chars()
        .nth(3)
        .context("Rithmic product ID has an invalid expiry year")?;
    Ok(format!("{root}{month_code}{year_code}"))
}

fn dated_product_root(product_id: &str) -> Result<String> {
    let instrument = product_id
        .strip_prefix("RITHMIC:")
        .context("Rithmic live product ID must use RITHMIC venue")?;
    let (root, _) = instrument
        .rsplit_once('-')
        .context("Rithmic live product ID must identify a dated future")?;
    ensure!(
        !root.is_empty(),
        "Rithmic dated product root must not be empty"
    );
    Ok(root.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::rithmic::{
        codec, config::RuntimeConfig, protocol, session::LoginParameters,
    };
    use crate::model::Candlestick;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;
    use tokio_tungstenite::{accept_async, tungstenite::protocol::Message, WebSocketStream};

    #[test]
    fn explicit_instrument_identity_validation_matrix() {
        assert!(validate_instrument_for_year("RITHMIC:MNQ-202609", "CME", "MNQU6", 2026).is_ok());

        for (product_id, exchange, symbol) in [
            ("CME:MNQ-202609", "CME", "MNQU6"),
            ("RITHMIC:MNQ", "CME", "MNQU6"),
            ("RITHMIC:MNQ-202609", "", "MNQU6"),
            ("RITHMIC:MNQ-202609", "NYMEX", "MNQU6"),
            ("RITHMIC:NQ-202609", "CME", "NQU6"),
            ("RITHMIC:MNQ-202609", "CME", ""),
            ("RITHMIC:MNQ-202612", "CME", "MNQU6"),
            ("RITHMIC:MNQ-202609", "CME", "MNQZ6"),
            ("RITHMIC:MNQ-203609", "CME", "MNQU9"),
        ] {
            assert!(validate_instrument_for_year(product_id, exchange, symbol, 2026).is_err());
        }
    }

    #[test]
    fn live_payload_handler_emits_completed_canonical_minute() {
        let (mut handler, mut candle_rx) = handler();

        handler.handle(&front_month_response("MNQU6")).unwrap();
        handler.handle(&last_trade(1_800_000_001)).unwrap();
        handler.handle(&last_trade(1_800_000_061)).unwrap();

        let candle = event_candle(candle_rx.try_recv().unwrap());
        assert_eq!(candle.product_id, "RITHMIC:MNQ-202609");
        assert_eq!(candle.timestamp, 1_800_000_000_000);
        assert_eq!(candle.timeframe, "1m");
    }

    #[test]
    fn live_payload_handler_front_month_state_matrix_fails_closed() {
        let (mut unverified, mut unverified_rx) = handler();
        unverified.handle(&last_trade(1_800_000_001)).unwrap();
        assert!(unverified_rx.try_recv().is_err());

        let (mut current, _) = handler();
        current.handle(&front_month_response("MNQU6")).unwrap();
        assert_eq!(
            *current.front_month_gate.borrow(),
            FrontMonthGateState::Verified
        );

        let (mut changed, _) = handler();
        let error = changed.handle(&front_month_update("MNQZ6")).unwrap_err();
        assert!(error.to_string().contains("rithmic_rollover_required"));
        assert_ne!(
            *changed.front_month_gate.borrow(),
            FrontMonthGateState::Verified
        );
    }

    #[test]
    fn preverification_trades_are_retained_but_not_published() {
        let (mut handler, mut candle_rx) = handler();

        handler.handle(&last_trade(1_800_000_001)).unwrap();
        assert!(candle_rx.try_recv().is_err());

        handler.handle(&front_month_response("MNQU6")).unwrap();
        handler.handle(&last_trade(1_800_000_061)).unwrap();

        assert_eq!(
            event_candle(candle_rx.try_recv().unwrap()).timestamp,
            1_800_000_000_000
        );
    }

    #[tokio::test]
    async fn front_month_deadline_expires_without_market_payload() {
        let (_state_tx, state_rx) = watch::channel(FrontMonthGateState::Awaiting {
            deadline: Instant::now() + Duration::from_millis(10),
        });

        let error = timeout(
            Duration::from_secs(1),
            enforce_front_month_deadline(state_rx),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("front-month verification timed out"));
    }

    #[tokio::test]
    async fn front_month_deadline_starts_only_when_startup_is_prepared() {
        let (state_tx, state_rx) = watch::channel(FrontMonthGateState::Idle);
        let watchdog = tokio::spawn(enforce_front_month_deadline(state_rx));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!watchdog.is_finished());

        state_tx.send_replace(FrontMonthGateState::Awaiting {
            deadline: Instant::now() + Duration::from_millis(10),
        });
        let error = timeout(Duration::from_secs(1), watchdog)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("front-month verification timed out"));
    }

    #[tokio::test]
    async fn reconnect_wait_can_outlive_previous_front_month_deadline() {
        let (handler, _) = handler();
        let state_rx = handler.front_month_gate.subscribe();
        handler
            .front_month_gate
            .send_replace(FrontMonthGateState::Awaiting {
                deadline: Instant::now() + Duration::from_millis(10),
            });
        let handler = Arc::new(Mutex::new(handler));
        let (forward_tx, mut forward_rx) = mpsc::channel(1);
        let watchdog = tokio::spawn(enforce_front_month_deadline(state_rx));
        tokio::task::yield_now().await;

        prepare_live_connection(
            &handler,
            &forward_tx,
            "RITHMIC:MNQ-202609",
            ConnectionPreparation::Retry,
        )
        .unwrap();
        assert!(forward_rx.try_recv().is_err());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!watchdog.is_finished());

        prepare_live_connection(
            &handler,
            &forward_tx,
            "RITHMIC:MNQ-202609",
            ConnectionPreparation::Startup,
        )
        .unwrap();
        assert!(matches!(
            forward_rx.try_recv().unwrap(),
            AggregationSourceEvent::ResetProduct(ref product_id)
                if product_id == "RITHMIC:MNQ-202609"
        ));
        handler
            .lock()
            .unwrap()
            .handle(&front_month_response("MNQU6"))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!watchdog.is_finished());
        watchdog.abort();
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
        handler.handle(&front_month_response("MNQU6")).unwrap();
        handler.handle(&last_trade(1_800_000_001)).unwrap();
        handler.reset();
        assert_ne!(
            *handler.front_month_gate.borrow(),
            FrontMonthGateState::Verified
        );
        handler.handle(&front_month_response("MNQU6")).unwrap();
        handler.handle(&last_trade(1_800_000_061)).unwrap();
        assert!(candle_rx.try_recv().is_err());

        handler.handle(&last_trade(1_800_000_121)).unwrap();
        assert_eq!(
            event_candle(candle_rx.try_recv().unwrap()).timestamp,
            1_800_000_060_000
        );
    }

    #[tokio::test]
    async fn reconnect_replays_subscription_and_discards_pre_disconnect_partial_minute() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let mut socket = serve_handshake(&listener).await;
                send(&mut socket, heartbeat_response()).await;
                assert_template(socket.next().await.unwrap().unwrap(), 113);
                send(&mut socket, front_month_response("MNQU6")).await;
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
        let startup = vec![
            front_month::request_with_updates("live-front-month", "MNQ", "CME", true).unwrap(),
            market::last_trade_request("CME", "MNQU6", SubscriptionAction::Subscribe).unwrap(),
        ];
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
            product_id: "RITHMIC:MNQ-202609".to_string(),
            root_symbol: "MNQ".to_string(),
            exchange: "CME".to_string(),
            symbol: "MNQU6".to_string(),
        };
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let connector = tokio::spawn(run(config, event_tx));

        let first = timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = event_rx.recv().await.unwrap();
        let third = event_rx.recv().await.unwrap();
        assert!(matches!(
            first,
            AggregationSourceEvent::ResetProduct(ref product_id)
                if product_id == "RITHMIC:MNQ-202609"
        ));
        assert!(matches!(
            second,
            AggregationSourceEvent::ResetProduct(ref product_id)
                if product_id == "RITHMIC:MNQ-202609"
        ));
        assert_eq!(event_candle(third).timestamp, 1_800_000_060_000);
        assert!(event_rx.try_recv().is_err());

        connector.abort();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn forwarder_waits_for_bounded_destination_capacity() {
        let (source_tx, source_rx) = mpsc::channel(1);
        let (destination_tx, mut destination_rx) = mpsc::channel(1);
        destination_tx
            .send(AggregationSourceEvent::Candle(candle(1)))
            .await
            .unwrap();
        source_tx
            .send(AggregationSourceEvent::Candle(candle(2)))
            .await
            .unwrap();

        let forwarder = tokio::spawn(forward_events(source_rx, destination_tx));
        tokio::task::yield_now().await;
        assert!(!forwarder.is_finished());
        assert_eq!(
            event_candle(destination_rx.recv().await.unwrap()).timestamp,
            1
        );
        assert_eq!(
            event_candle(destination_rx.recv().await.unwrap()).timestamp,
            2
        );

        drop(source_tx);
        assert!(forwarder.await.unwrap().is_err());
    }

    #[test]
    fn full_forwarding_queue_fails_closed_without_dropping_buffered_candle() {
        let (mut handler, mut candle_rx) = handler_with_capacity(1);
        handler.handle(&front_month_response("MNQU6")).unwrap();
        handler.handle(&last_trade(1_800_000_001)).unwrap();
        handler.handle(&last_trade(1_800_000_061)).unwrap();

        let error = handler.handle(&last_trade(1_800_000_121)).unwrap_err();
        assert!(error.to_string().contains("queue exhausted"));
        assert_eq!(
            event_candle(candle_rx.try_recv().unwrap()).timestamp,
            1_800_000_000_000
        );
        assert!(candle_rx.try_recv().is_err());
    }

    fn handler() -> (LivePayloadHandler, mpsc::Receiver<AggregationSourceEvent>) {
        handler_with_capacity(FORWARD_QUEUE_CAPACITY)
    }

    fn handler_with_capacity(
        capacity: usize,
    ) -> (LivePayloadHandler, mpsc::Receiver<AggregationSourceEvent>) {
        let (candle_tx, candle_rx) = mpsc::channel(capacity);
        let (front_month_gate, _) = watch::channel(FrontMonthGateState::Awaiting {
            deadline: Instant::now() + RESPONSE_TIMEOUT,
        });
        (
            LivePayloadHandler {
                builder: MinuteBarBuilder::new(
                    "RITHMIC:MNQ-202609".to_string(),
                    "CME".to_string(),
                    "MNQU6".to_string(),
                )
                .unwrap(),
                candle_tx,
                observed_last_trade: false,
                root_symbol: "MNQ".to_string(),
                exchange: "CME".to_string(),
                symbol: "MNQU6".to_string(),
                front_month_gate,
            },
            candle_rx,
        )
    }

    fn event_candle(event: AggregationSourceEvent) -> Candlestick {
        match event {
            AggregationSourceEvent::Candle(candle) => candle,
            AggregationSourceEvent::ResetProduct(product_id) => {
                panic!("expected candle event, received reset for {product_id}")
            }
        }
    }

    fn candle(timestamp: i64) -> Candlestick {
        Candlestick {
            product_id: "RITHMIC:MNQ-202609".to_string(),
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
            symbol: Some("MNQU6".to_string()),
            trade_price: Some(29_784.75),
            trade_size: Some(1),
            ssboe: Some(ssboe),
            usecs: Some(0),
            ..Default::default()
        })
        .unwrap()
    }

    fn front_month_response(symbol: &str) -> Vec<u8> {
        codec::encode(&protocol::ResponseFrontMonthContract {
            template_id: 114,
            user_msg: vec!["live-front-month".to_string()],
            rp_code: vec!["0".to_string()],
            symbol: Some("MNQ".to_string()),
            exchange: Some("CME".to_string()),
            trading_symbol: Some(symbol.to_string()),
            trading_exchange: Some("CME".to_string()),
            is_front_month_symbol: Some(true),
            ..Default::default()
        })
        .unwrap()
    }

    fn front_month_update(symbol: &str) -> Vec<u8> {
        codec::encode(&protocol::FrontMonthContractUpdate {
            template_id: 159,
            symbol: Some("MNQ".to_string()),
            exchange: Some("CME".to_string()),
            trading_symbol: Some(symbol.to_string()),
            trading_exchange: Some("CME".to_string()),
            is_front_month_symbol: Some(true),
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
