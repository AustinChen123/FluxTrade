use crate::connector::backpack::BackpackConnector;
use redis::AsyncCommands;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};

pub struct Watchdog {
    redis_client: redis::Client,
    backpack: BackpackConnector,
    missing_count: u32,
}

impl Watchdog {
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let redis_client = redis::Client::open(redis_url)?;
        Ok(Self {
            redis_client,
            backpack: BackpackConnector::new(),
            missing_count: 0,
        })
    }

    pub async fn run(mut self) {
        info!("⚔️ Watchdog Active: Monitoring heartbeat:python");
        
        // Connect to Redis
        let mut conn = match self.redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Watchdog failed to connect to Redis: {}. Exiting Watchdog.", e);
                return;
            }
        };

        loop {
            // 1. Check Heartbeat
            // We expect the value to be a timestamp (ms) string
            let heartbeat_res: redis::RedisResult<Option<String>> = conn.get("heartbeat:python").await;

            let mut trigger = false;

            match heartbeat_res {
                Ok(Some(ts_str)) => {
                    self.missing_count = 0; // Reset missing count
                    if let Ok(ts) = ts_str.parse::<i64>() {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;
                        
                        if now - ts > 5000 {
                            warn!("Watchdog: Heartbeat stale (age: {}ms)", now - ts);
                            trigger = true;
                        }
                    } else {
                        warn!("Watchdog: Invalid heartbeat format: {}", ts_str);
                        // Don't trigger on format error immediately, but maybe counts as missing?
                        // Let's treat as missing
                        self.missing_count += 1;
                    }
                }
                Ok(None) => {
                    self.missing_count += 1;
                    warn!("Watchdog: Heartbeat missing (count: {})", self.missing_count);
                }
                Err(e) => {
                    warn!("Watchdog: Redis error reading heartbeat: {}", e);
                    // Don't increment missing count on Redis error to avoid false positive due to network blip?
                    // Or do? If Redis is down, we can't see Python.
                    // But we can't write to Lock System either.
                    // Let's pause.
                }
            }

            if self.missing_count > 5 {
                trigger = true;
            }

            if trigger {
                error!("🚨 WATCHDOG TRIGGERED: Python heartbeat failure!");
                
                // 1. Lock System
                if let Err(e) = conn.set::<_, _, ()>("system:state", "LOCKDOWN").await {
                     error!("Watchdog failed to set system:state: {}", e);
                }
                
                // 2. Alert
                if let Err(e) = conn.publish::<_, _, ()>("system:alert", "⚠️ Emergency Stop Triggered").await {
                    error!("Watchdog failed to publish alert: {}", e);
                }
                
                // 3. KILL (Cancel Orders)
                // This is the most critical part
                match self.backpack.cancel_all_orders().await {
                    Ok(_) => info!("Watchdog: Kill switch executed successfully."),
                    Err(e) => error!("Watchdog: FAILED to execute Kill Switch: {}", e),
                }

                // Sleep a bit to avoid rapid firing loop
                sleep(Duration::from_secs(5)).await;
                // Reset missing count? Or keep triggering until fixed?
                // If we reset, we re-evaluate.
                self.missing_count = 0; 
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}
