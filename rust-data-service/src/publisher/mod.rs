use crate::model::{Candlestick, Trade};
use anyhow::Result;
use redis::AsyncCommands;
use serde::Serialize;
use tracing::{debug, error, info};

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
                debug!("Publishing to {}: {}", topic, payload);
                match conn.publish::<&str, String, ()>(topic, payload).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("Redis publish error: {}. Invalidating connection.", e);
                        self.conn = None; // Invalidate for next attempt
                        anyhow::bail!("Redis publish failed: {}", e);
                    }
                }
                    } else {
                        anyhow::bail!("Redis connection lost");
                    }
                }
            }
