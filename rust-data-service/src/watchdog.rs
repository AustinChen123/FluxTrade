use crate::connector::emergency::EmergencyMitigation;
use crate::environment::RuntimeEnvironment;
use anyhow::Context;
use redis::AsyncCommands;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};

const HEARTBEAT_STALE_AFTER_MS: i64 = 5_000;
const HEARTBEAT_MISSING_LIMIT: u32 = 5;

#[derive(Debug, Default)]
struct HeartbeatMonitor {
    armed: bool,
    missing_count: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum HeartbeatObservation {
    WaitingForInitial,
    Healthy { newly_armed: bool },
    Missing { count: u32 },
    Invalid { count: u32 },
    TriggerMissing { count: u32 },
    TriggerInvalid { count: u32 },
    TriggerStale { age_ms: i64 },
}

impl HeartbeatMonitor {
    fn observe(&mut self, value: Option<&str>, now_ms: i64) -> HeartbeatObservation {
        let Some(raw_timestamp) = value else {
            if !self.armed {
                self.missing_count = 0;
                return HeartbeatObservation::WaitingForInitial;
            }
            self.missing_count = self.missing_count.saturating_add(1);
            return if self.missing_count > HEARTBEAT_MISSING_LIMIT {
                HeartbeatObservation::TriggerMissing {
                    count: self.missing_count,
                }
            } else {
                HeartbeatObservation::Missing {
                    count: self.missing_count,
                }
            };
        };

        let Ok(timestamp_ms) = raw_timestamp.parse::<i64>() else {
            self.missing_count = self.missing_count.saturating_add(1);
            return if self.missing_count > HEARTBEAT_MISSING_LIMIT {
                HeartbeatObservation::TriggerInvalid {
                    count: self.missing_count,
                }
            } else {
                HeartbeatObservation::Invalid {
                    count: self.missing_count,
                }
            };
        };

        self.missing_count = 0;
        let age_ms = now_ms.saturating_sub(timestamp_ms);
        if age_ms > HEARTBEAT_STALE_AFTER_MS {
            return HeartbeatObservation::TriggerStale { age_ms };
        }

        let newly_armed = !self.armed;
        self.armed = true;
        HeartbeatObservation::Healthy { newly_armed }
    }
}

pub struct Watchdog {
    redis_client: redis::Client,
    mitigation: EmergencyMitigation,
    heartbeat_monitor: HeartbeatMonitor,
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
            heartbeat_monitor: HeartbeatMonitor::default(),
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

            let heartbeat_value =
                heartbeat_res.context("Watchdog failed to read heartbeat from Redis")?;
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let observation = self
                .heartbeat_monitor
                .observe(heartbeat_value.as_deref(), now_ms);
            let trigger = match observation {
                HeartbeatObservation::WaitingForInitial => false,
                HeartbeatObservation::Healthy { newly_armed } => {
                    if newly_armed {
                        info!("Watchdog armed after the first fresh Python heartbeat");
                    }
                    false
                }
                HeartbeatObservation::Missing { count } => {
                    warn!("Watchdog: Heartbeat missing (count: {})", count);
                    false
                }
                HeartbeatObservation::Invalid { count } => {
                    warn!("Watchdog: Invalid heartbeat format (count: {})", count);
                    false
                }
                HeartbeatObservation::TriggerMissing { count } => {
                    warn!("Watchdog: Heartbeat missing (count: {})", count);
                    true
                }
                HeartbeatObservation::TriggerInvalid { count } => {
                    warn!("Watchdog: Invalid heartbeat format (count: {})", count);
                    true
                }
                HeartbeatObservation::TriggerStale { age_ms } => {
                    warn!("Watchdog: Heartbeat stale (age: {}ms)", age_ms);
                    true
                }
            };

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
                self.heartbeat_monitor.missing_count = 0;
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[test]
    fn missing_heartbeat_waits_until_the_first_fresh_observation() {
        let mut monitor = HeartbeatMonitor::default();

        for _ in 0..12 {
            assert_eq!(
                monitor.observe(None, 10_000),
                HeartbeatObservation::WaitingForInitial
            );
        }

        assert!(!monitor.armed);
        assert_eq!(monitor.missing_count, 0);
    }

    #[test]
    fn first_fresh_heartbeat_arms_the_existing_missing_threshold() {
        let mut monitor = HeartbeatMonitor::default();

        assert_eq!(
            monitor.observe(Some("10000"), 10_100),
            HeartbeatObservation::Healthy { newly_armed: true }
        );
        for count in 1..=5 {
            assert_eq!(
                monitor.observe(None, 10_100 + i64::from(count)),
                HeartbeatObservation::Missing { count }
            );
        }
        assert_eq!(
            monitor.observe(None, 10_106),
            HeartbeatObservation::TriggerMissing { count: 6 }
        );
    }

    #[test]
    fn stale_or_malformed_existing_heartbeat_never_bypasses_safety() {
        let mut stale = HeartbeatMonitor::default();
        assert_eq!(
            stale.observe(Some("1000"), 10_000),
            HeartbeatObservation::TriggerStale { age_ms: 9_000 }
        );

        let mut malformed = HeartbeatMonitor::default();
        for count in 1..=5 {
            assert_eq!(
                malformed.observe(Some("invalid"), 10_000),
                HeartbeatObservation::Invalid { count }
            );
        }
        assert_eq!(
            malformed.observe(Some("invalid"), 10_000),
            HeartbeatObservation::TriggerInvalid { count: 6 }
        );
    }

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
