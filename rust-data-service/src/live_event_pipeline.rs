use crate::aggregator::CandleAggregator;
use crate::model::{self, UserStreamEvent};
use crate::publisher::PublishSender;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[cfg_attr(not(feature = "rithmic"), allow(dead_code))]
#[derive(Debug)]
pub(crate) enum AggregationSourceEvent {
    Candle(model::Candlestick),
    ResetProduct(String),
}

/// Receive live connector events, update derived candles, and forward them to Redis.
pub(crate) async fn run_event_loop(
    mut trade_rx: mpsc::Receiver<model::Trade>,
    mut candle_rx: mpsc::Receiver<model::Candlestick>,
    mut user_rx: mpsc::Receiver<UserStreamEvent>,
    mut aggregation_source_rx: mpsc::Receiver<AggregationSourceEvent>,
    pub_sender: PublishSender,
) -> anyhow::Result<()> {
    let mut aggregator = CandleAggregator::new();
    let mut trade_open = true;
    let mut candle_open = true;
    let mut user_open = true;
    let mut aggregation_source_open = true;

    info!("Event loop started");

    loop {
        tokio::select! {
            msg = trade_rx.recv(), if trade_open => {
                match msg {
                    Some(trade) => {
                        if let Err(e) = pub_sender.publish_trade(&trade).await {
                            warn!("Failed to send trade to publisher: {}", e);
                        }
                    }
                    None => {
                        info!("Trade channel closed");
                        trade_open = false;
                    }
                }
            }

            msg = candle_rx.recv(), if candle_open => {
                match msg {
                    Some(candle) => {
                        publish_and_aggregate_candle(&mut aggregator, &pub_sender, candle).await;
                    }
                    None => {
                        info!("Candle channel closed");
                        candle_open = false;
                    }
                }
            }

            msg = user_rx.recv(), if user_open => {
                match msg {
                    Some(event) => {
                        match event {
                            UserStreamEvent::Account(update) => {
                                if let Err(e) = pub_sender.publish_account_update(&update).await {
                                    warn!("Failed to send account update to publisher: {}", e);
                                }
                            }
                            UserStreamEvent::Position(update) => {
                                if let Err(e) = pub_sender.publish_position_update(&update).await {
                                    warn!("Failed to send position update to publisher: {}", e);
                                }
                            }
                        }
                    }
                    None => {
                        info!("User stream channel closed");
                        user_open = false;
                    }
                }
            }

            event = aggregation_source_rx.recv(), if aggregation_source_open => {
                match event {
                    Some(AggregationSourceEvent::Candle(candle)) => {
                        publish_and_aggregate_candle(&mut aggregator, &pub_sender, candle).await;
                    }
                    Some(AggregationSourceEvent::ResetProduct(product_id)) => {
                        aggregator.reset_product(&product_id);
                        info!(product_id, "Aggregation state reset after source reconnect");
                    }
                    None => {
                        aggregation_source_open = false;
                    }
                }
            }
        }

        if !trade_open && !candle_open && !user_open && !aggregation_source_open {
            info!("All event channels closed, event loop exiting");
            return Ok(());
        }
    }
}

async fn publish_and_aggregate_candle(
    aggregator: &mut CandleAggregator,
    pub_sender: &PublishSender,
    candle: model::Candlestick,
) {
    if let Err(e) = pub_sender.publish_candle(&candle).await {
        warn!("Failed to send candle to publisher: {}", e);
    }

    for target_timeframe in ["5m", "15m"] {
        let can_derive = CandleAggregator::can_aggregate(&candle.timeframe, target_timeframe);
        match can_derive {
            Ok(true) if candle.timeframe != target_timeframe => {}
            Ok(_) => continue,
            Err(e) => {
                warn!(
                    "Invalid source/target timeframe pair {} -> {}: {}",
                    candle.timeframe, target_timeframe, e
                );
                continue;
            }
        }
        match aggregator.add_candle(&candle, target_timeframe) {
            Ok(Some(completed)) => {
                if let Err(e) = pub_sender.publish_candle(&completed).await {
                    warn!(
                        "Failed to send {} candle to publisher: {}",
                        target_timeframe, e
                    );
                }
            }
            Ok(None) => {}
            Err(e) => warn!(
                "Failed to aggregate {} -> {} candle: {}",
                candle.timeframe, target_timeframe, e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::{create_publish_channel, PublishMessage};
    use std::time::Duration;

    #[test]
    fn generic_main_contains_no_event_pipeline_implementation() {
        let main_source = include_str!("main.rs");
        let event_loop_definition = ["async fn run", "_event_loop"].concat();
        let candle_definition = ["async fn publish", "_and_aggregate_candle"].concat();
        let owner_call = ["live_event_pipeline", "::run_event_loop"].concat();
        assert!(!main_source.contains(&event_loop_definition));
        assert!(!main_source.contains(&candle_definition));
        assert_eq!(main_source.matches(&owner_call).count(), 1);
    }

    #[tokio::test]
    async fn test_event_loop_exits_on_channel_close() {
        let (trade_tx, trade_rx) = mpsc::channel(10);
        let (candle_tx, candle_rx) = mpsc::channel(10);
        let (user_tx, user_rx) = mpsc::channel(10);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, _pub_rx) = create_publish_channel(10);

        drop(trade_tx);
        drop(candle_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_event_loop(
                trade_rx,
                candle_rx,
                user_rx,
                aggregation_reset_rx,
                pub_sender,
            ),
        )
        .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn event_loop_keeps_serving_remaining_channels() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(1);
        drop(trade_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_reset_rx,
            pub_sender,
        ));
        candle_tx
            .send(model::Candlestick {
                product_id: "RITHMIC:NQ-202609".to_string(),
                timeframe: "1m".to_string(),
                timestamp: 1_800_000_000_000,
                open: rust_decimal_macros::dec!(100),
                high: rust_decimal_macros::dec!(101),
                low: rust_decimal_macros::dec!(99),
                close: rust_decimal_macros::dec!(100),
                volume: rust_decimal_macros::dec!(1),
            })
            .await
            .unwrap();

        assert!(matches!(
            pub_rx.recv().await,
            Some(PublishMessage::Candle(_))
        ));
        drop(candle_tx);
        assert!(event_loop.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn event_loop_forwards_trade_account_and_position_events() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(2);
        let (aggregation_tx, aggregation_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(3);
        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_rx,
            pub_sender,
        ));

        trade_tx
            .send(model::Trade {
                id: "trade-1".to_string(),
                product_id: "BINANCE:BTCUSDT-PERP".to_string(),
                price: rust_decimal_macros::dec!(100),
                quantity: rust_decimal_macros::dec!(2),
                side: "buy".to_string(),
                timestamp: 1_800_000_000_000,
            })
            .await
            .unwrap();
        user_tx
            .send(UserStreamEvent::Account(model::AccountUpdate {
                exchange: "binance".to_string(),
                asset: "USDT".to_string(),
                balance: rust_decimal_macros::dec!(1000),
                timestamp: 1_800_000_000_001,
            }))
            .await
            .unwrap();
        user_tx
            .send(UserStreamEvent::Position(model::PositionUpdate {
                exchange: "binance".to_string(),
                symbol: "BTCUSDT".to_string(),
                amount: rust_decimal_macros::dec!(2),
                entry_price: rust_decimal_macros::dec!(100),
                unrealized_pnl: rust_decimal_macros::dec!(3),
                timestamp: 1_800_000_000_002,
            }))
            .await
            .unwrap();
        drop(trade_tx);
        drop(candle_tx);
        drop(user_tx);
        drop(aggregation_tx);

        let mut kinds = Vec::new();
        for _ in 0..3 {
            let message = tokio::time::timeout(Duration::from_secs(1), pub_rx.recv())
                .await
                .unwrap()
                .unwrap();
            kinds.push(match message {
                PublishMessage::Trade(trade) if trade.id == "trade-1" => "trade",
                PublishMessage::AccountUpdate(update) if update.asset == "USDT" => "account",
                PublishMessage::PositionUpdate(update) if update.symbol == "BTCUSDT" => "position",
                unexpected => panic!("unexpected forwarded message: {unexpected:?}"),
            });
        }
        kinds.sort_unstable();
        assert_eq!(kinds, ["account", "position", "trade"]);
        assert!(event_loop.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn event_loop_accepts_closed_5m_source_candles() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(4);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(8);
        drop(trade_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_reset_rx,
            pub_sender,
        ));
        for index in 0..4 {
            candle_tx
                .send(model::Candlestick {
                    product_id: "RITHMIC:NQ-202609".to_string(),
                    timeframe: "5m".to_string(),
                    timestamp: 1_800_000_000_000 + index * 5 * 60 * 1000,
                    open: rust_decimal_macros::dec!(100),
                    high: rust_decimal_macros::dec!(101),
                    low: rust_decimal_macros::dec!(99),
                    close: rust_decimal_macros::dec!(100),
                    volume: rust_decimal_macros::dec!(1),
                })
                .await
                .unwrap();
        }
        drop(candle_tx);

        let mut published = Vec::new();
        while let Some(message) = pub_rx.recv().await {
            if let PublishMessage::Candle(candle) = message {
                published.push(candle);
            }
        }
        assert!(event_loop.await.unwrap().is_ok());
        assert_eq!(
            published
                .iter()
                .map(|candle| candle.timeframe.as_str())
                .collect::<Vec<_>>(),
            ["5m", "5m", "5m", "5m", "15m"]
        );
    }

    #[tokio::test]
    async fn closed_publisher_is_nonfatal_for_live_event_forwarding() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_tx, aggregation_rx) = mpsc::channel(1);
        let (pub_sender, pub_rx) = create_publish_channel(1);
        drop(pub_rx);
        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_rx,
            pub_sender,
        ));

        trade_tx
            .send(model::Trade {
                id: "trade-closed-publisher".to_string(),
                product_id: "BINANCE:BTCUSDT-PERP".to_string(),
                price: rust_decimal_macros::dec!(100),
                quantity: rust_decimal_macros::dec!(1),
                side: "sell".to_string(),
                timestamp: 1_800_000_000_000,
            })
            .await
            .unwrap();
        drop(trade_tx);
        drop(candle_tx);
        drop(user_tx);
        drop(aggregation_tx);

        assert!(tokio::time::timeout(Duration::from_secs(1), event_loop)
            .await
            .unwrap()
            .unwrap()
            .is_ok());
    }

    #[tokio::test]
    async fn event_loop_applies_rithmic_reset_before_post_reconnect_candles() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_source_tx, aggregation_source_rx) = mpsc::channel(8);
        let (pub_sender, mut pub_rx) = create_publish_channel(16);
        drop(trade_tx);
        drop(candle_tx);
        drop(user_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_source_rx,
            pub_sender,
        ));
        let base_ts = 1_800_000_000_000;
        let candle = |minute: i64| model::Candlestick {
            product_id: "RITHMIC:NQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: rust_decimal_macros::dec!(100),
            high: rust_decimal_macros::dec!(101),
            low: rust_decimal_macros::dec!(99),
            close: rust_decimal_macros::dec!(100),
            volume: rust_decimal_macros::dec!(1),
        };
        aggregation_source_tx
            .send(AggregationSourceEvent::Candle(candle(2)))
            .await
            .unwrap();
        aggregation_source_tx
            .send(AggregationSourceEvent::ResetProduct(
                "RITHMIC:NQ-202609".to_string(),
            ))
            .await
            .unwrap();
        for minute in 5..=10 {
            aggregation_source_tx
                .send(AggregationSourceEvent::Candle(candle(minute)))
                .await
                .unwrap();
        }
        drop(aggregation_source_tx);

        let mut derived = Vec::new();
        while let Some(message) = pub_rx.recv().await {
            if let PublishMessage::Candle(candle) = message {
                if candle.timeframe != "1m" {
                    derived.push(candle);
                }
            }
        }
        assert!(event_loop.await.unwrap().is_ok());
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].timeframe, "5m");
        assert_eq!(derived[0].timestamp, base_ts + 5 * 60_000);
        assert_eq!(derived[0].volume, rust_decimal_macros::dec!(5));
    }

    #[tokio::test]
    async fn event_loop_preserves_shorter_source_that_cannot_form_5m_exactly() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(1);
        drop(trade_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_reset_rx,
            pub_sender,
        ));
        candle_tx
            .send(model::Candlestick {
                product_id: "RITHMIC:NQ-202609".to_string(),
                timeframe: "2m".to_string(),
                timestamp: 1_800_000_000_000,
                open: rust_decimal_macros::dec!(100),
                high: rust_decimal_macros::dec!(101),
                low: rust_decimal_macros::dec!(99),
                close: rust_decimal_macros::dec!(100),
                volume: rust_decimal_macros::dec!(1),
            })
            .await
            .unwrap();
        drop(candle_tx);

        let published = pub_rx.recv().await.expect("source candle should publish");
        assert!(matches!(
            published,
            PublishMessage::Candle(candle) if candle.timeframe == "2m"
        ));
        assert!(event_loop.await.unwrap().is_ok());
    }
}
