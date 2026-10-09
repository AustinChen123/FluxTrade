use crate::connector::emergency::EmergencyMitigation;
use crate::environment::RuntimeEnvironment;
use anyhow::Context;
use redis::AsyncCommands;
use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info, warn};

const HEARTBEAT_STALE_AFTER_MS: i64 = 5_000;
const HEARTBEAT_MISSING_LIMIT: u32 = 5;
const PLANNED_RESTART_GRACE: Duration = Duration::from_secs(30);

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

#[derive(Debug)]
struct PlannedRestart {
    boot_id: String,
    observed_at_ms: i64,
    deadline: Instant,
}

#[derive(Debug, Default)]
struct PlannedRestartGrace {
    active: Option<PlannedRestart>,
    seen_clean_boot_ids: HashSet<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum GraceUpdate {
    None,
    Started,
    Retained,
    Cancelled,
}

#[derive(Debug, PartialEq, Eq)]
struct HeartbeatDecision {
    trigger: bool,
    missing_suppressed: bool,
    grace_expired: bool,
}

fn decide_heartbeat_action(
    observation: &HeartbeatObservation,
    grace: &PlannedRestartGrace,
    now: Instant,
) -> HeartbeatDecision {
    let heartbeat_missing = matches!(
        observation,
        HeartbeatObservation::WaitingForInitial
            | HeartbeatObservation::Missing { .. }
            | HeartbeatObservation::TriggerMissing { .. }
    );
    let grace_expired = heartbeat_missing && grace.missing_expired(now);
    if grace_expired {
        return HeartbeatDecision {
            trigger: true,
            missing_suppressed: false,
            grace_expired: true,
        };
    }
    if heartbeat_missing && grace.missing_is_suppressed(now) {
        return HeartbeatDecision {
            trigger: false,
            missing_suppressed: true,
            grace_expired: false,
        };
    }
    let trigger = matches!(
        observation,
        HeartbeatObservation::TriggerMissing { .. }
            | HeartbeatObservation::TriggerInvalid { .. }
            | HeartbeatObservation::TriggerStale { .. }
    );
    HeartbeatDecision {
        trigger,
        missing_suppressed: false,
        grace_expired: false,
    }
}

impl PlannedRestartGrace {
    fn observe_marker(
        &mut self,
        marker: Option<&str>,
        observed_at_ms: i64,
        now: Instant,
    ) -> GraceUpdate {
        let parsed = marker.and_then(parse_boot_marker);
        let Some((state, boot_id)) = parsed else {
            return if self.active.take().is_some() {
                GraceUpdate::Cancelled
            } else {
                GraceUpdate::None
            };
        };

        match state.as_str() {
            "UNCLEAN" => GraceUpdate::Retained,
            "CLEAN" => {
                if self
                    .active
                    .as_ref()
                    .is_some_and(|restart| restart.boot_id == boot_id)
                {
                    return GraceUpdate::Retained;
                }
                if !self.seen_clean_boot_ids.insert(boot_id.clone()) {
                    return GraceUpdate::Retained;
                }
                self.active = Some(PlannedRestart {
                    boot_id,
                    observed_at_ms,
                    deadline: now + PLANNED_RESTART_GRACE,
                });
                GraceUpdate::Started
            }
            _ => unreachable!("parse_boot_marker accepts only CLEAN or UNCLEAN"),
        }
    }

    fn confirm_restart_progress(&mut self, heartbeat: Option<&str>, now_ms: i64) -> bool {
        let Some(restart) = self.active.as_ref() else {
            return false;
        };
        let Some(raw_timestamp) = heartbeat else {
            return false;
        };
        let Ok(timestamp_ms) = raw_timestamp.parse::<i64>() else {
            return false;
        };
        let age_ms = now_ms.saturating_sub(timestamp_ms);
        if timestamp_ms <= restart.observed_at_ms || age_ms > HEARTBEAT_STALE_AFTER_MS {
            return false;
        }
        self.active = None;
        true
    }

    fn missing_is_suppressed(&self, now: Instant) -> bool {
        self.active
            .as_ref()
            .is_some_and(|restart| now < restart.deadline)
    }

    fn missing_expired(&self, now: Instant) -> bool {
        self.active
            .as_ref()
            .is_some_and(|restart| now >= restart.deadline)
    }
}

fn parse_boot_marker(marker: &str) -> Option<(String, String)> {
    let payload: serde_json::Value = serde_json::from_str(marker).ok()?;
    let state = payload.get("state")?.as_str()?;
    let boot_id = payload.get("boot_id")?.as_str()?;
    if !matches!(state, "CLEAN" | "UNCLEAN") || boot_id.is_empty() {
        return None;
    }
    Some((state.to_owned(), boot_id.to_owned()))
}

pub struct Watchdog {
    redis_client: redis::Client,
    mitigation: EmergencyMitigation,
    heartbeat_monitor: HeartbeatMonitor,
    planned_restart_grace: PlannedRestartGrace,
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
            planned_restart_grace: PlannedRestartGrace::default(),
            environment,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let heartbeat_key = self.environment.key("heartbeat:python");
        let boot_state_key = self.environment.key("system:engine_boot_state");
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
            let boot_state_res: redis::RedisResult<Option<String>> =
                conn.get(&boot_state_key).await;
            let boot_state =
                boot_state_res.context("Watchdog failed to read engine boot state from Redis")?;
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let grace_update = self.planned_restart_grace.observe_marker(
                boot_state.as_deref(),
                now_ms,
                Instant::now(),
            );
            match grace_update {
                GraceUpdate::Started => info!("Watchdog observed planned Python shutdown; bounded restart grace started"),
                GraceUpdate::Cancelled => warn!("Watchdog planned restart grace cancelled because boot evidence is unavailable or invalid"),
                GraceUpdate::None | GraceUpdate::Retained => {}
            }
            let observation = self
                .heartbeat_monitor
                .observe(heartbeat_value.as_deref(), now_ms);
            let restart_confirmed = self
                .planned_restart_grace
                .confirm_restart_progress(heartbeat_value.as_deref(), now_ms);
            if restart_confirmed {
                info!("Watchdog observed fresh Python heartbeat after planned restart");
            }
            let now = Instant::now();
            let decision = decide_heartbeat_action(&observation, &self.planned_restart_grace, now);
            if decision.grace_expired {
                error!("Watchdog planned restart grace expired without a fresh Python heartbeat");
            }
            if decision.missing_suppressed {
                warn!("Watchdog heartbeat missing during bounded planned restart grace");
            }
            if let HeartbeatObservation::Healthy { newly_armed } = &observation {
                if *newly_armed {
                    info!("Watchdog armed after the first fresh Python heartbeat");
                }
            }
            match &observation {
                HeartbeatObservation::Missing { count }
                | HeartbeatObservation::TriggerMissing { count }
                    if !decision.missing_suppressed && !decision.grace_expired =>
                {
                    warn!("Watchdog: Heartbeat missing (count: {})", count);
                }
                HeartbeatObservation::Invalid { count }
                | HeartbeatObservation::TriggerInvalid { count } => {
                    warn!("Watchdog: Invalid heartbeat format (count: {})", count);
                }
                HeartbeatObservation::TriggerStale { age_ms } => {
                    warn!("Watchdog: Heartbeat stale (age: {}ms)", age_ms);
                }
                _ => {}
            }
            let trigger = decision.trigger;

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

    #[test]
    fn planned_restart_requires_valid_clean_marker_and_never_renews_same_id() {
        let start = Instant::now();
        let mut grace = PlannedRestartGrace::default();

        assert_eq!(
            grace.observe_marker(
                Some(r#"{"state":"CLEAN","boot_id":"boot-a"}"#),
                1_000,
                start
            ),
            GraceUpdate::Started
        );
        assert_eq!(
            grace.observe_marker(
                Some(r#"{"state":"CLEAN","boot_id":"boot-a"}"#),
                2_000,
                start + Duration::from_secs(20)
            ),
            GraceUpdate::Retained
        );
        assert!(grace.missing_is_suppressed(start + Duration::from_secs(29)));
        assert!(!grace.missing_is_suppressed(start + Duration::from_secs(30)));
        assert!(grace.missing_expired(start + Duration::from_secs(30)));

        assert_eq!(
            grace.observe_marker(
                Some(r#"{"state":"CLEAN","boot_id":"boot-a"}"#),
                3_000,
                start + Duration::from_secs(31)
            ),
            GraceUpdate::Retained
        );
        assert!(grace.missing_expired(start + Duration::from_secs(31)));
    }

    #[test]
    fn planned_restart_keeps_deadline_across_new_unclean_marker_and_needs_new_heartbeat() {
        let start = Instant::now();
        let mut grace = PlannedRestartGrace::default();
        grace.observe_marker(
            Some(r#"{"state":"CLEAN","boot_id":"boot-a"}"#),
            10_000,
            start,
        );

        assert_eq!(
            grace.observe_marker(
                Some(r#"{"state":"UNCLEAN","boot_id":"boot-b"}"#),
                12_000,
                start + Duration::from_secs(10)
            ),
            GraceUpdate::Retained
        );
        assert!(!grace.confirm_restart_progress(Some("9999"), 12_001));
        assert!(grace.active.is_some());
        assert!(grace.confirm_restart_progress(Some("12001"), 12_001));
        assert!(grace.active.is_none());
    }

    #[test]
    fn missing_or_invalid_boot_evidence_cancels_grace() {
        for marker in [
            None,
            Some("not-json"),
            Some(r#"{"state":"CLEAN","boot_id":""}"#),
            Some(r#"{"state":"UNKNOWN","boot_id":"boot-a"}"#),
        ] {
            let mut grace = PlannedRestartGrace::default();
            grace.observe_marker(
                Some(r#"{"state":"CLEAN","boot_id":"boot-a"}"#),
                1_000,
                Instant::now(),
            );
            assert_eq!(
                grace.observe_marker(marker, 1_100, Instant::now()),
                GraceUpdate::Cancelled
            );
            assert!(!grace.missing_is_suppressed(Instant::now()));
        }
    }

    #[test]
    fn integrated_decision_expires_unarmed_grace_and_never_masks_present_failures() {
        let start = Instant::now();
        let mut grace = PlannedRestartGrace::default();
        grace.observe_marker(
            Some(r#"{"state":"CLEAN","boot_id":"boot-a"}"#),
            10_000,
            start,
        );

        let before_deadline = decide_heartbeat_action(
            &HeartbeatObservation::WaitingForInitial,
            &grace,
            start + Duration::from_secs(29),
        );
        assert_eq!(
            before_deadline,
            HeartbeatDecision {
                trigger: false,
                missing_suppressed: true,
                grace_expired: false,
            }
        );
        let at_deadline = decide_heartbeat_action(
            &HeartbeatObservation::WaitingForInitial,
            &grace,
            start + Duration::from_secs(30),
        );
        assert_eq!(
            at_deadline,
            HeartbeatDecision {
                trigger: true,
                missing_suppressed: false,
                grace_expired: true,
            }
        );

        let stale = decide_heartbeat_action(
            &HeartbeatObservation::TriggerStale { age_ms: 6_000 },
            &grace,
            start + Duration::from_secs(5),
        );
        assert_eq!(
            stale,
            HeartbeatDecision {
                trigger: true,
                missing_suppressed: false,
                grace_expired: false,
            }
        );
        let malformed = decide_heartbeat_action(
            &HeartbeatObservation::TriggerInvalid { count: 6 },
            &grace,
            start + Duration::from_secs(5),
        );
        assert_eq!(
            malformed,
            HeartbeatDecision {
                trigger: true,
                missing_suppressed: false,
                grace_expired: false,
            }
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

    #[tokio::test]
    #[ignore = "requires an isolated Redis provided through B3_TEST_REDIS_URL"]
    async fn clean_boot_marker_suppresses_missing_heartbeat_only_during_restart_grace() {
        let redis_url = std::env::var("B3_TEST_REDIS_URL")
            .expect("B3_TEST_REDIS_URL must point to an isolated Redis");
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let environment = RuntimeEnvironment::new("test").unwrap();
        let heartbeat_key = environment.key("heartbeat:python");
        let boot_state_key = environment.key("system:engine_boot_state");
        let system_state_key = environment.key("system:state");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        conn.set::<_, _, ()>(&system_state_key, "OK").await.unwrap();
        conn.set::<_, _, ()>(&heartbeat_key, now_ms.to_string())
            .await
            .unwrap();
        conn.set::<_, _, ()>(
            &boot_state_key,
            r#"{"state":"CLEAN","boot_id":"isolated-restart-test"}"#,
        )
        .await
        .unwrap();
        let watchdog =
            Watchdog::new(&redis_url, environment, EmergencyMitigation::LockdownOnly).unwrap();
        let task = tokio::spawn(watchdog.run());
        sleep(Duration::from_millis(1_200)).await;
        let _: () = conn.del(&heartbeat_key).await.unwrap();
        sleep(Duration::from_millis(1_200)).await;
        conn.set::<_, _, ()>(
            &boot_state_key,
            r#"{"state":"UNCLEAN","boot_id":"next-isolated-restart-test"}"#,
        )
        .await
        .unwrap();
        sleep(Duration::from_secs(6)).await;

        let state: String = conn.get(&system_state_key).await.unwrap();
        assert_eq!(state, "OK");
        timeout(Duration::from_secs(25), async {
            loop {
                let state: Option<String> = conn.get(&system_state_key).await.unwrap();
                if state.as_deref() == Some("LOCKDOWN") {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("planned restart deadline must trigger without a new missing window");
        task.abort();
        let _: () = conn
            .del(&[heartbeat_key, boot_state_key, system_state_key])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires an isolated Redis provided through B3_TEST_REDIS_URL"]
    async fn fresh_restart_heartbeat_completes_grace_without_clearing_existing_lockdown() {
        let redis_url = std::env::var("B3_TEST_REDIS_URL")
            .expect("B3_TEST_REDIS_URL must point to an isolated Redis");
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let environment = RuntimeEnvironment::new("test").unwrap();
        let heartbeat_key = environment.key("heartbeat:python");
        let boot_state_key = environment.key("system:engine_boot_state");
        let system_state_key = environment.key("system:state");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        conn.set::<_, _, ()>(&system_state_key, "OK").await.unwrap();
        conn.set::<_, _, ()>(&heartbeat_key, now_ms.to_string())
            .await
            .unwrap();
        conn.set::<_, _, ()>(
            &boot_state_key,
            r#"{"state":"CLEAN","boot_id":"success-restart-a"}"#,
        )
        .await
        .unwrap();

        let watchdog = Watchdog::new(
            &redis_url,
            environment.clone(),
            EmergencyMitigation::LockdownOnly,
        )
        .unwrap();
        let task = tokio::spawn(watchdog.run());
        sleep(Duration::from_millis(1_200)).await;
        let _: () = conn.del(&heartbeat_key).await.unwrap();
        sleep(Duration::from_secs(6)).await;
        conn.set::<_, _, ()>(
            &boot_state_key,
            r#"{"state":"UNCLEAN","boot_id":"success-restart-b"}"#,
        )
        .await
        .unwrap();
        let resumed_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        conn.set::<_, _, ()>(&heartbeat_key, resumed_at_ms.to_string())
            .await
            .unwrap();
        sleep(Duration::from_millis(1_200)).await;

        let state: String = conn.get(&system_state_key).await.unwrap();
        assert_eq!(state, "OK");
        assert!(
            !task.is_finished(),
            "watchdog remains active after recovery"
        );
        task.abort();
        let _ = task.await;

        conn.set::<_, _, ()>(&system_state_key, "LOCKDOWN")
            .await
            .unwrap();
        conn.set::<_, _, ()>(&heartbeat_key, resumed_at_ms.to_string())
            .await
            .unwrap();
        conn.set::<_, _, ()>(
            &boot_state_key,
            r#"{"state":"CLEAN","boot_id":"locked-restart-c"}"#,
        )
        .await
        .unwrap();
        let watchdog =
            Watchdog::new(&redis_url, environment, EmergencyMitigation::LockdownOnly).unwrap();
        let task = tokio::spawn(watchdog.run());
        sleep(Duration::from_millis(1_200)).await;
        let state: String = conn.get(&system_state_key).await.unwrap();
        assert_eq!(state, "LOCKDOWN");
        task.abort();
        let _ = task.await;
        let _: () = conn
            .del(&[heartbeat_key, boot_state_key, system_state_key])
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires an isolated Redis provided through B3_TEST_REDIS_URL"]
    async fn boot_marker_read_error_keeps_watchdog_supervised_failure_behavior() {
        let redis_url = std::env::var("B3_TEST_REDIS_URL")
            .expect("B3_TEST_REDIS_URL must point to an isolated Redis");
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let environment = RuntimeEnvironment::new("test").unwrap();
        let heartbeat_key = environment.key("heartbeat:python");
        let boot_state_key = environment.key("system:engine_boot_state");
        let system_state_key = environment.key("system:state");
        conn.set::<_, _, ()>(&system_state_key, "OK").await.unwrap();
        conn.set::<_, _, ()>(&heartbeat_key, "0").await.unwrap();
        conn.rpush::<_, _, ()>(&boot_state_key, "invalid-type")
            .await
            .unwrap();

        let watchdog =
            Watchdog::new(&redis_url, environment, EmergencyMitigation::LockdownOnly).unwrap();
        let task = tokio::spawn(watchdog.run());
        let result = timeout(Duration::from_secs(3), task)
            .await
            .expect("watchdog must exit on boot-marker Redis read error")
            .expect("watchdog task must not panic");
        let error = result.expect_err("wrong-type boot marker must fail the watchdog read");
        assert!(error
            .to_string()
            .contains("Watchdog failed to read engine boot state from Redis"));
        let state: String = conn.get(&system_state_key).await.unwrap();
        assert_eq!(state, "OK");
        let _: () = conn
            .del(&[heartbeat_key, boot_state_key, system_state_key])
            .await
            .unwrap();
    }
}
