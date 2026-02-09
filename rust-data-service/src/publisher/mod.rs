use crate::model::{AccountUpdate, Candlestick, PositionUpdate, Trade};
use anyhow::Result;
use redis::AsyncCommands;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Messages that can be sent to the publisher task via the channel.
#[derive(Debug)]
pub enum PublishMessage {
    Trade(Trade),
    Candle(Candlestick),
    AccountUpdate(AccountUpdate),
    PositionUpdate(PositionUpdate),
}

/// A cloneable sender handle for publishing messages without holding a mutex.
/// Connectors use this to send messages to the dedicated publisher task.
#[derive(Clone)]
pub struct PublishSender {
    tx: mpsc::Sender<PublishMessage>,
}

impl PublishSender {
    /// Send a trade to be published. Returns error if the channel is full or closed.
    pub async fn publish_trade(&self, trade: &Trade) -> Result<()> {
        self.tx
            .try_send(PublishMessage::Trade(trade.clone()))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("Publisher channel full, dropping trade message")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("Publisher channel closed")
                }
            })
    }

    /// Send a candle to be published.
    pub async fn publish_candle(&self, candle: &Candlestick) -> Result<()> {
        self.tx
            .try_send(PublishMessage::Candle(candle.clone()))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("Publisher channel full, dropping candle message")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("Publisher channel closed")
                }
            })
    }

    /// Send an account update to be published.
    pub async fn publish_account_update(&self, update: &AccountUpdate) -> Result<()> {
        self.tx
            .try_send(PublishMessage::AccountUpdate(update.clone()))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("Publisher channel full, dropping account update")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("Publisher channel closed")
                }
            })
    }

    /// Send a position update to be published.
    pub async fn publish_position_update(&self, update: &PositionUpdate) -> Result<()> {
        self.tx
            .try_send(PublishMessage::PositionUpdate(update.clone()))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("Publisher channel full, dropping position update")
                }
                mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("Publisher channel closed")
                }
            })
    }
}

pub struct RedisPublisher {
    client: redis::Client,
    conn: Option<redis::aio::MultiplexedConnection>,
}

/// Default channel capacity for the publisher task.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;

/// Create a publisher channel pair: (sender for connectors, receiver for the publisher task).
pub fn create_publish_channel(capacity: usize) -> (PublishSender, mpsc::Receiver<PublishMessage>) {
    let (tx, rx) = mpsc::channel(capacity);
    (PublishSender { tx }, rx)
}

impl RedisPublisher {
    pub fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self { client, conn: None })
    }

    pub async fn connect(&mut self) -> Result<()> {
        let conn = self.client.get_multiplexed_async_connection().await?;
        self.conn = Some(conn);
        Ok(())
    }

    /// Run the publisher as a dedicated task, consuming messages from the channel.
    /// This task owns the Redis connection exclusively — no mutex needed.
    /// Returns Ok(()) when the channel is closed (all senders dropped).
    /// Returns Err on unrecoverable Redis errors.
    pub async fn run(&mut self, mut rx: mpsc::Receiver<PublishMessage>) -> Result<()> {
        self.connect().await?;
        info!("Publisher task started, consuming from channel");

        let mut consecutive_errors: u32 = 0;
        let max_consecutive_errors: u32 = 10;

        while let Some(msg) = rx.recv().await {
            let result = match msg {
                PublishMessage::Trade(trade) => self.publish_trade(&trade).await,
                PublishMessage::Candle(candle) => self.publish_candle(&candle).await,
                PublishMessage::AccountUpdate(update) => {
                    self.update_account_balance(&update).await
                }
                PublishMessage::PositionUpdate(update) => self.update_position(&update).await,
            };

            match result {
                Ok(()) => {
                    consecutive_errors = 0;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    error!(
                        "Publisher error ({}/{}): {}",
                        consecutive_errors, max_consecutive_errors, e
                    );
                    if consecutive_errors >= max_consecutive_errors {
                        error!("Publisher exceeded max consecutive errors, exiting task");
                        return Err(anyhow::anyhow!(
                            "Publisher task failed: {} consecutive errors",
                            consecutive_errors
                        ));
                    }
                    // Attempt reconnect on error
                    if let Err(re) = self.connect().await {
                        warn!("Publisher reconnect failed: {}", re);
                    }
                }
            }
        }

        info!("Publisher channel closed, task exiting");
        Ok(())
    }

    pub async fn update_account_balance(&mut self, update: &AccountUpdate) -> Result<()> {
        self.ensure_connected().await?;
        if let Some(conn) = &mut self.conn {
            let key = format!("account:balance:{}", update.asset);
            // Set the balance key (String)
            let _: () = conn.set(&key, update.balance.to_string()).await?;

            // Also publish to the stream channel
            let payload = serde_json::to_string(update)?;
            let _: () = conn.publish("stream.user.updates", payload).await?;
        }
        Ok(())
    }

    pub async fn update_position(&mut self, update: &PositionUpdate) -> Result<()> {
        self.ensure_connected().await?;
        if let Some(conn) = &mut self.conn {
            let key = format!("account:positions:{}", update.symbol);
            // Set the position hash
            let _: () = conn
                .hset_multiple(
                    &key,
                    &[
                        ("size", update.amount.to_string()),
                        ("entry_price", update.entry_price.to_string()),
                        ("pnl", update.unrealized_pnl.to_string()),
                    ],
                )
                .await?;

            // Also publish to the stream channel
            let payload = serde_json::to_string(update)?;
            let _: () = conn.publish("stream.user.updates", payload).await?;
        }
        Ok(())
    }

    pub async fn publish_candle(&mut self, candle: &Candlestick) -> Result<()> {
        // Stream Key: stream:market:{exchange}:{symbol}
        // product_id is "EXCHANGE:SYMBOL-PERP"
        let parts: Vec<&str> = candle.product_id.split(':').collect();
        let exchange = parts[0].to_lowercase();
        let symbol = parts[1].replace("-PERP", "").to_lowercase();
        let tf = candle.timeframe.to_lowercase();
        let topic = format!("stream:market:{}:{}:{}", exchange, symbol, tf);

        self.publish(&topic, candle).await
    }

    pub async fn publish_trade(&mut self, trade: &Trade) -> Result<()> {
        let parts: Vec<&str> = trade.product_id.split(':').collect();
        let exchange = parts[0].to_lowercase();
        let symbol = parts[1].replace("-PERP", "").to_lowercase();
        let topic = format!("stream:market:{}:{}", exchange, symbol);

        self.publish(&topic, trade).await
    }

    async fn ensure_connected(&mut self) -> Result<()> {
        if self.conn.is_none() {
            info!("Redis connection missing, reconnecting...");
            self.connect().await?;
        }
        Ok(())
    }

    async fn publish<T: Serialize>(&mut self, topic: &str, data: &T) -> Result<()> {
        let payload = serde_json::to_string(data)?;

        self.ensure_connected().await?;

        if let Some(conn) = &mut self.conn {
            // debug!("XADD to {}: {}", topic, payload);
            let items = [("json", payload)];
            // MAXLEN ~ 100000
            let maxlen = redis::streams::StreamMaxlen::Approx(100000);

            match conn
                .xadd_maxlen::<&str, &str, &str, String, String>(topic, maxlen, "*", &items)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("Redis XADD error: {}. Invalidating connection.", e);
                    self.conn = None; // Invalidate for next attempt
                    anyhow::bail!("Redis XADD failed: {}", e);
                }
            }
        } else {
            anyhow::bail!("Redis connection lost");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_publish_channel() {
        let (sender, _rx) = create_publish_channel(100);
        // Sender should be cloneable
        let _sender2 = sender.clone();
    }

    #[tokio::test]
    async fn test_publish_sender_trade() {
        let (sender, mut rx) = create_publish_channel(10);
        let trade = Trade {
            id: "1".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            price: rust_decimal_macros::dec!(50000),
            quantity: rust_decimal_macros::dec!(0.1),
            side: "buy".to_string(),
            timestamp: 1600000000,
        };

        sender.publish_trade(&trade).await.unwrap();

        let msg = rx.recv().await.unwrap();
        match msg {
            PublishMessage::Trade(t) => {
                assert_eq!(t.id, "1");
                assert_eq!(t.product_id, "BINANCE:BTCUSDT-PERP");
            }
            _ => panic!("Expected Trade message"),
        }
    }

    #[tokio::test]
    async fn test_publish_sender_candle() {
        let (sender, mut rx) = create_publish_channel(10);
        let candle = Candlestick {
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1600000000,
            open: rust_decimal_macros::dec!(50000),
            high: rust_decimal_macros::dec!(51000),
            low: rust_decimal_macros::dec!(49000),
            close: rust_decimal_macros::dec!(50500),
            volume: rust_decimal_macros::dec!(10),
        };

        sender.publish_candle(&candle).await.unwrap();

        let msg = rx.recv().await.unwrap();
        match msg {
            PublishMessage::Candle(c) => {
                assert_eq!(c.product_id, "BINANCE:BTCUSDT-PERP");
                assert_eq!(c.timeframe, "1m");
            }
            _ => panic!("Expected Candle message"),
        }
    }

    #[tokio::test]
    async fn test_publish_sender_backpressure() {
        // Create a channel with capacity 1
        let (sender, _rx) = create_publish_channel(1);
        let trade = Trade {
            id: "1".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            price: rust_decimal_macros::dec!(50000),
            quantity: rust_decimal_macros::dec!(0.1),
            side: "buy".to_string(),
            timestamp: 1600000000,
        };

        // First send should succeed
        sender.publish_trade(&trade).await.unwrap();
        // Second send should fail (channel full, no one consuming)
        let result = sender.publish_trade(&trade).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("channel full"));
    }

    #[tokio::test]
    async fn test_publish_sender_closed_channel() {
        let (sender, rx) = create_publish_channel(10);
        // Drop receiver to close the channel
        drop(rx);

        let trade = Trade {
            id: "1".to_string(),
            product_id: "BINANCE:BTCUSDT-PERP".to_string(),
            price: rust_decimal_macros::dec!(50000),
            quantity: rust_decimal_macros::dec!(0.1),
            side: "buy".to_string(),
            timestamp: 1600000000,
        };

        let result = sender.publish_trade(&trade).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("channel closed"));
    }
}
