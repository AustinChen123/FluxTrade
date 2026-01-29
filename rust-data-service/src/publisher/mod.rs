use crate::model::{AccountUpdate, Candlestick, PositionUpdate, Trade};
use anyhow::Result;
use redis::AsyncCommands;
use serde::Serialize;
use tracing::{error, info};

pub struct RedisPublisher {
    client: redis::Client,
    conn: Option<redis::aio::MultiplexedConnection>,
}

impl RedisPublisher {
    #[allow(dead_code)]
    pub fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self { client, conn: None })
    }

    #[allow(dead_code)]
    pub async fn connect(&mut self) -> Result<()> {
        let conn = self.client.get_multiplexed_async_connection().await?;
        self.conn = Some(conn);
        Ok(())
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub async fn update_position(&mut self, update: &PositionUpdate) -> Result<()> {
        self.ensure_connected().await?;
        if let Some(conn) = &mut self.conn {
            let key = format!("account:positions:{}", update.symbol);
            // Set the position hash
            let _: () = conn.hset_multiple(
                &key,
                &[
                    ("size", update.amount.to_string()),
                    ("entry_price", update.entry_price.to_string()),
                    ("pnl", update.unrealized_pnl.to_string()),
                ],
            ).await?;

            // Also publish to the stream channel
            let payload = serde_json::to_string(update)?;
            let _: () = conn.publish("stream.user.updates", payload).await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
            
            match conn.xadd_maxlen::<&str, &str, &str, String, String>(topic, maxlen, "*", &items).await {
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
