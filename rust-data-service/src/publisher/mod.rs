use crate::model::{Candlestick, Trade};
use anyhow::Result;
use redis::AsyncCommands;
use serde::Serialize;
use tracing::{debug, error};

pub struct RedisPublisher {
    client: redis::Client,
    conn: Option<redis::aio::MultiplexedConnection>,
}

impl RedisPublisher {
    #[allow(dead_code)]
    pub fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self {
            client,
            conn: None,
        } )
    }

    #[allow(dead_code)]
    pub async fn connect(&mut self) -> Result<()> {
        let conn = self.client.get_multiplexed_async_connection().await?;
        self.conn = Some(conn);
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn publish_candle(&mut self, candle: &Candlestick) -> Result<()> {
        // Topic: market_data.<exchange>.<symbol>.<timeframe>
        // product_id is "EXCHANGE:SYMBOL-PERP"
        let parts: Vec<&str> = candle.product_id.split(':').collect();
        let exchange = parts[0].to_lowercase();
        let symbol = parts[1].replace("-PERP", "").to_lowercase();
        let topic = format!("market_data.{}.{}.{}", exchange, symbol, candle.timeframe);
        
        self.publish(&topic, candle).await
    }

    #[allow(dead_code)]
    pub async fn publish_trade(&mut self, trade: &Trade) -> Result<()> {
        let parts: Vec<&str> = trade.product_id.split(':').collect();
        let exchange = parts[0].to_lowercase();
        let symbol = parts[1].replace("-PERP", "").to_lowercase();
        let topic = format!("market_data.{}.{}.trades", exchange, symbol);
        
        self.publish(&topic, trade).await
    }

    async fn publish<T: Serialize>(&mut self, topic: &str, data: &T) -> Result<()> {
        let payload = serde_json::to_string(data)?;
        if let Some(conn) = &mut self.conn {
            debug!("Publishing to {}: {}", topic, payload);
            conn.publish::<&str, String, ()>(topic, payload).await?;
        } else {
            error!("Redis not connected");
            anyhow::bail!("Redis not connected");
        }
        Ok(())
    }
}
