use crate::connector::backpack::BackpackConnector;
use crate::environment::RuntimeEnvironment;
use anyhow::Context;
use redis::AsyncCommands;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};

pub(crate) enum EmergencyMitigation {
    LockdownOnly,
    Backpack(BackpackConnector),
    #[cfg(feature = "rithmic")]
    Rithmic {
        profile: String,
        account_id: String,
    },
}

impl EmergencyMitigation {
    async fn run(&self) -> anyhow::Result<()> {
        match self {
            Self::LockdownOnly => Ok(()),
            Self::Backpack(connector) => connector.cancel_all_orders().await,
            #[cfg(feature = "rithmic")]
            Self::Rithmic {
                profile,
                account_id,
            } => {
                let result =
                    crate::connector::rithmic::emergency::mitigate(profile, account_id).await?;
                info!(
                    cancelled_orders = result.cancelled_orders,
                    exited_positions = result.exited_positions,
                    "Rithmic emergency mitigation verified flat"
                );
                Ok(())
            }
        }
    }
}

pub struct Watchdog {
    redis_client: redis::Client,
    mitigation: EmergencyMitigation,
    missing_count: u32,
    environment: RuntimeEnvironment,
}

impl Watchdog {
    pub(crate) fn new(
        redis_url: &str,
        environment: RuntimeEnvironment,
        mitigation: EmergencyMitigation,
    ) -> anyhow::Result<Self> {
        let redis_client = redis::Client::open(redis_url)?;
        Ok(Self {
            redis_client,
            mitigation,
            missing_count: 0,
            environment,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let heartbeat_key = self.environment.key("heartbeat:python");
        let system_state_key = self.environment.key("system:state");
        let alert_key = self.environment.key("system:alert");
        info!(
            environment = self.environment.identity(),
            heartbeat_key, "Watchdog active"
        );

        // Connect to Redis
        let mut conn = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .context("Watchdog failed to connect to Redis")?;

        loop {
            // 1. Check Heartbeat
            // We expect the value to be a timestamp (ms) string
            let heartbeat_res: redis::RedisResult<Option<String>> = conn.get(&heartbeat_key).await;

            let mut trigger = false;

            match heartbeat_res {
                Ok(Some(ts_str)) => {
                    self.missing_count = 0; // Reset missing count
                    if let Ok(ts) = ts_str.parse::<i64>() {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
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
                    warn!(
                        "Watchdog: Heartbeat missing (count: {})",
                        self.missing_count
                    );
                }
                Err(e) => {
                    return Err(e).context("Watchdog failed to read heartbeat from Redis");
                }
            }

            if self.missing_count > 5 {
                trigger = true;
            }

            if trigger {
                error!("🚨 WATCHDOG TRIGGERED: Python heartbeat failure!");

                // 1. Lock System
                let lockdown_result = conn
                    .set::<_, _, ()>(&system_state_key, "LOCKDOWN")
                    .await
                    .context("Watchdog failed to persist LOCKDOWN");

                // 2. KILL (Cancel Orders)
                // This is the most critical part
                let kill_result = if self.environment.allows_external_kill() {
                    let result = self
                        .mitigation
                        .run()
                        .await
                        .context("Watchdog failed to execute external kill switch");
                    if result.is_ok() {
                        info!("Watchdog: Kill switch executed successfully.");
                    }
                    result
                } else {
                    info!(
                        environment = self.environment.identity(),
                        "Watchdog skipped external kill outside live environment"
                    );
                    Ok(())
                };

                // 3. Alert
                let alert_result = conn
                    .publish::<_, _, ()>(&alert_key, "⚠️ Emergency Stop Triggered")
                    .await
                    .context("Watchdog failed to publish alert");

                lockdown_result?;
                kill_result?;
                alert_result?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    #[ignore = "requires an isolated Redis provided through B3_TEST_REDIS_URL"]
    async fn test_timeout_locks_only_test_environment() {
        let redis_url = std::env::var("B3_TEST_REDIS_URL")
            .expect("B3_TEST_REDIS_URL must point to an isolated Redis");
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let test_environment = RuntimeEnvironment::new("test").unwrap();
        let live_environment = RuntimeEnvironment::new("live").unwrap();
        let test_heartbeat_key = test_environment.key("heartbeat:python");
        let test_state_key = test_environment.key("system:state");
        let live_state_key = live_environment.key("system:state");

        conn.set::<_, _, ()>(&live_state_key, "OK").await.unwrap();
        conn.set::<_, _, ()>(&test_heartbeat_key, "0")
            .await
            .unwrap();

        let watchdog = Watchdog::new(
            &redis_url,
            test_environment,
            EmergencyMitigation::LockdownOnly,
        )
        .unwrap();
        let task = tokio::spawn(watchdog.run());
        timeout(Duration::from_secs(3), async {
            loop {
                let state: Option<String> = conn.get(&test_state_key).await.unwrap();
                if state.as_deref() == Some("LOCKDOWN") {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap();

        let live_state: String = conn.get(&live_state_key).await.unwrap();
        assert_eq!(live_state, "OK");
        assert!(!task.is_finished(), "test watchdog must remain active");
        task.abort();
        let _: () = conn
            .del(&[test_heartbeat_key, test_state_key, live_state_key])
            .await
            .unwrap();
    }
}
