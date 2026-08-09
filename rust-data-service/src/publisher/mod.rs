use crate::environment::RuntimeEnvironment;
use crate::model::{validate_product_id, AccountUpdate, Candlestick, PositionUpdate, Trade};
use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout, Duration, MissedTickBehavior};
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
    environment: RuntimeEnvironment,
}

const LIVENESS_INTERVAL: Duration = Duration::from_secs(1);
const LIVENESS_TTL_MILLIS: i64 = 5_000;
const LIVENESS_OPERATION_TIMEOUT: Duration = Duration::from_secs(1);
const LIVENESS_FAILURE_LIMIT: u32 = 5;

#[cfg(test)]
static LIVENESS_FAILURE_PROBE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[derive(Debug, Default)]
struct LivenessFailureState {
    consecutive_failures: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum LivenessTransition {
    None,
    FailureStarted,
    Recovered(u32),
    Fatal,
}

impl LivenessFailureState {
    fn succeeded(&mut self) -> LivenessTransition {
        if self.consecutive_failures == 0 {
            return LivenessTransition::None;
        }
        let attempts = self.consecutive_failures;
        self.consecutive_failures = 0;
        LivenessTransition::Recovered(attempts)
    }

    fn failed(&mut self) -> LivenessTransition {
        self.consecutive_failures += 1;
        match self.consecutive_failures {
            1 => LivenessTransition::FailureStarted,
            LIVENESS_FAILURE_LIMIT.. => LivenessTransition::Fatal,
            _ => LivenessTransition::None,
        }
    }
}

fn observe_liveness_result(state: &mut LivenessFailureState, result: Result<()>) -> Result<()> {
    match result {
        Ok(()) => {
            if let LivenessTransition::Recovered(attempts) = state.succeeded() {
                info!(
                    component = "data_publisher",
                    operation = "renew_liveness",
                    stage = "redis_write",
                    disposition = "recovered",
                    failed_attempts = attempts,
                    "Publisher liveness renewal recovered"
                );
            }
            Ok(())
        }
        Err(error) => {
            #[cfg(test)]
            LIVENESS_FAILURE_PROBE.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match state.failed() {
                LivenessTransition::FailureStarted => {
                    warn!(
                        component = "data_publisher",
                        operation = "renew_liveness",
                        stage = "redis_write",
                        disposition = "retry",
                        failed_attempts = 1,
                        error = %error,
                        "Publisher liveness renewal failed"
                    );
                    Ok(())
                }
                LivenessTransition::Fatal => {
                    Err(error).context("Publisher liveness renewal failed five consecutive times")
                }
                LivenessTransition::None => Ok(()),
                LivenessTransition::Recovered(_) => unreachable!(),
            }
        }
    }
}

/// Default channel capacity for the publisher task.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 10_000;

/// Create a publisher channel pair: (sender for connectors, receiver for the publisher task).
pub fn create_publish_channel(capacity: usize) -> (PublishSender, mpsc::Receiver<PublishMessage>) {
    let (tx, rx) = mpsc::channel(capacity);
    (PublishSender { tx }, rx)
}

impl RedisPublisher {
    pub fn new(url: &str, environment: RuntimeEnvironment) -> Result<Self> {
        let client = redis::Client::open(url)?;
        Ok(Self {
            client,
            conn: None,
            environment,
        })
    }

    fn liveness_key(environment: &RuntimeEnvironment) -> String {
        environment.key("heartbeat:data-publisher")
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
        info!("Publisher task started, consuming from channel");

        let mut consecutive_errors: u32 = 0;
        let max_consecutive_errors: u32 = 10;
        let mut liveness_state = LivenessFailureState::default();
        let mut liveness_interval = interval(LIVENESS_INTERVAL);
        liveness_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = liveness_interval.tick() => {
                    let result = self.renew_liveness().await;
                    if result.is_err() {
                        self.conn = None;
                    }
                    observe_liveness_result(&mut liveness_state, result)?;
                }
                message = rx.recv() => {
                    let Some(msg) = message else {
                        info!("Publisher channel closed, task exiting");
                        return Ok(());
                    };
                    let result = match msg {
                        PublishMessage::Trade(trade) => self.publish_trade(&trade).await,
                        PublishMessage::Candle(candle) => self.publish_candle(&candle).await,
                        PublishMessage::AccountUpdate(update) => self.update_account_balance(&update).await,
                        PublishMessage::PositionUpdate(update) => self.update_position(&update).await,
                    };
                    match result {
                        Ok(()) => consecutive_errors = 0,
                        Err(e) => {
                            consecutive_errors += 1;
                            error!("Publisher error ({}/{}): {}", consecutive_errors, max_consecutive_errors, e);
                            if consecutive_errors >= max_consecutive_errors {
                                error!("Publisher exceeded max consecutive errors, exiting task");
                                return Err(anyhow::anyhow!("Publisher task failed: {} consecutive errors", consecutive_errors));
                            }
                            if let Err(re) = self.connect().await {
                                warn!("Publisher reconnect failed: {}", re);
                            }
                        }
                    }
                }
            }
        }
    }

    async fn renew_liveness(&mut self) -> Result<()> {
        let key = Self::liveness_key(&self.environment);
        timeout(LIVENESS_OPERATION_TIMEOUT, async {
            self.ensure_connected().await?;
            let conn = self.conn.as_mut().context("Redis connection lost")?;
            let (seconds, microseconds): (i64, i64) = redis::cmd("TIME")
                .query_async(conn)
                .await
                .context("Redis TIME failed for publisher liveness")?;
            let current_millis = seconds
                .checked_mul(1_000)
                .and_then(|value| value.checked_add(microseconds / 1_000))
                .context("Redis TIME overflowed publisher liveness timestamp")?;
            let expires_at_millis = current_millis
                .checked_add(LIVENESS_TTL_MILLIS)
                .context("Publisher liveness expiry timestamp overflowed")?;
            let _: () = redis::cmd("SET")
                .arg(key)
                .arg("alive")
                .arg("PXAT")
                .arg(expires_at_millis)
                .query_async(conn)
                .await
                .context("Redis SET PXAT failed for publisher liveness")?;
            Result::<()>::Ok(())
        })
        .await
        .context("Publisher liveness renewal timed out")??;
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
        let topic = market_stream_key(&candle.product_id, Some(&candle.timeframe))?;

        self.publish(&topic, candle).await
    }

    pub async fn publish_trade(&mut self, trade: &Trade) -> Result<()> {
        let topic = market_stream_key(&trade.product_id, None)?;

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

fn market_stream_key(product_id: &str, timeframe: Option<&str>) -> Result<String> {
    validate_product_id(product_id)?;
    let (venue, instrument) = product_id.split_once(':').expect("validated product_id");
    let symbol = instrument.strip_suffix("-PERP").unwrap_or(instrument);
    let mut key = format!(
        "stream:market:{}:{}",
        venue.to_ascii_lowercase(),
        symbol.to_ascii_lowercase()
    );
    if let Some(timeframe) = timeframe {
        key.push(':');
        key.push_str(&timeframe.to_ascii_lowercase());
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::RuntimeEnvironment;
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, timeout, Duration, Instant};
    use tracing_subscriber::prelude::*;

    #[derive(Clone)]
    struct CaptureLayer(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields =
                BTreeMap::from([("level".to_string(), event.metadata().level().to_string())]);
            event.record(&mut EventVisitor(&mut fields));
            self.0.lock().unwrap().push(fields);
        }
    }

    struct EventVisitor<'a>(&'a mut BTreeMap<String, String>);

    impl tracing::field::Visit for EventVisitor<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(
                field.name().to_string(),
                format!("{value:?}").trim_matches('"').to_string(),
            );
        }
    }

    #[test]
    fn test_create_publish_channel() {
        let (sender, _rx) = create_publish_channel(100);
        // Sender should be cloneable
        let _sender2 = sender.clone();
    }

    #[test]
    fn publisher_liveness_keys_are_environment_scoped() {
        let first = RuntimeEnvironment::new("publisher-test-a").unwrap();
        let second = RuntimeEnvironment::new("publisher-test-b").unwrap();

        assert_eq!(
            RedisPublisher::liveness_key(&first),
            "fluxtrade:publisher-test-a:heartbeat:data-publisher"
        );
        assert_ne!(
            RedisPublisher::liveness_key(&first),
            RedisPublisher::liveness_key(&second)
        );
    }

    #[test]
    fn liveness_failure_state_has_exact_threshold_and_success_reset() {
        let mut state = LivenessFailureState::default();
        for expected in [
            LivenessTransition::FailureStarted,
            LivenessTransition::None,
            LivenessTransition::None,
            LivenessTransition::None,
            LivenessTransition::Fatal,
        ] {
            assert_eq!(state.failed(), expected);
        }
        assert_eq!(state.succeeded(), LivenessTransition::Recovered(5));
        assert_eq!(state.failed(), LivenessTransition::FailureStarted);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires an isolated Redis provided through DATA_PUBLISHER_TEST_REDIS_URL"]
    async fn publisher_liveness_renews_recovers_fails_at_threshold_and_expires() {
        let redis_url = std::env::var("DATA_PUBLISHER_TEST_REDIS_URL")
            .expect("DATA_PUBLISHER_TEST_REDIS_URL must point to an isolated Redis");
        let unique = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let environment = RuntimeEnvironment::new(format!("publisher-test-{unique}")).unwrap();
        let key = RedisPublisher::liveness_key(&environment);
        let client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut admin = bounded_redis(client.get_multiplexed_async_connection()).await;
        let _: () = bounded_redis(admin.del(&key)).await;

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(CaptureLayer(Arc::clone(&events)));
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let (sender, rx) = create_publish_channel(1);
        let mut publisher = RedisPublisher::new(&redis_url, environment.clone()).unwrap();
        LIVENESS_FAILURE_PROBE.store(0, std::sync::atomic::Ordering::SeqCst);
        let task = tokio::spawn(async move { publisher.run(rx).await });
        wait_for_lease(&mut admin, &key, Duration::from_secs(3)).await;
        let original_pttl: i64 = bounded_redis(admin.pttl(&key)).await;

        let _: () = bounded_redis(
            redis::cmd("CLIENT")
                .arg("PAUSE")
                .arg(20000)
                .arg("WRITE")
                .query_async(&mut admin),
        )
        .await;
        wait_for_failure_count(1, Duration::from_secs(3)).await;
        assert!(!task.is_finished(), "one renewal failure must be nonfatal");
        let _: () =
            bounded_redis(redis::cmd("CLIENT").arg("UNPAUSE").query_async(&mut admin)).await;
        let captured = wait_for_liveness_events(&events, 2, Duration::from_secs(3)).await;
        sleep(Duration::from_millis(original_pttl as u64 + 250)).await;
        wait_for_lease(&mut admin, &key, Duration::from_secs(4)).await;
        let renewed_pttl: i64 = bounded_redis(admin.pttl(&key)).await;
        assert!((3_000..=5_000).contains(&renewed_pttl));
        assert_liveness_transitions(&captured);
        events.lock().unwrap().clear();

        drop(sender);
        timeout(Duration::from_secs(3), task)
            .await
            .expect("publisher must stop after normal channel close")
            .unwrap()
            .unwrap();
        let value: Option<String> = bounded_redis(admin.get(&key)).await;
        let exit_pttl: i64 = bounded_redis(admin.pttl(&key)).await;
        assert_eq!(value.as_deref(), Some("alive"));
        assert!(exit_pttl > 0);
        wait_for_absence(&mut admin, &key, Duration::from_secs(7)).await;

        let threshold_environment =
            RuntimeEnvironment::new(format!("publisher-threshold-{unique}")).unwrap();
        let threshold_key = RedisPublisher::liveness_key(&threshold_environment);
        let (_sender, rx) = create_publish_channel(1);
        let mut publisher = RedisPublisher::new(&redis_url, threshold_environment).unwrap();
        LIVENESS_FAILURE_PROBE.store(0, std::sync::atomic::Ordering::SeqCst);
        let task = tokio::spawn(async move { publisher.run(rx).await });
        wait_for_lease(&mut admin, &threshold_key, Duration::from_secs(3)).await;
        let _: () = bounded_redis(
            redis::cmd("CLIENT")
                .arg("PAUSE")
                .arg(20000)
                .arg("WRITE")
                .query_async(&mut admin),
        )
        .await;
        wait_for_failure_count(4, Duration::from_secs(7)).await;
        assert!(
            !task.is_finished(),
            "publisher must survive four timeout windows"
        );
        let error = timeout(Duration::from_secs(4), task)
            .await
            .expect("publisher must fail at the bounded threshold")
            .unwrap()
            .unwrap_err();
        let task_completed = Instant::now();
        assert!(error.to_string().contains("five consecutive"));
        tokio::time::sleep_until(
            task_completed + Duration::from_millis(LIVENESS_TTL_MILLIS as u64 + 1_000),
        )
        .await;
        let _: () =
            bounded_redis(redis::cmd("CLIENT").arg("UNPAUSE").query_async(&mut admin)).await;
        sleep(Duration::from_millis(250)).await;
        let threshold_key_exists: bool = bounded_redis(admin.exists(&threshold_key)).await;
        assert!(!threshold_key_exists);
    }

    fn assert_liveness_transitions(events: &[BTreeMap<String, String>]) {
        assert_eq!(events.len(), 2);
        let warning = &events[0];
        assert_eq!(warning["level"], "WARN");
        assert_eq!(warning["message"], "Publisher liveness renewal failed");
        assert_eq!(warning["component"], "data_publisher");
        assert_eq!(warning["operation"], "renew_liveness");
        assert_eq!(warning["stage"], "redis_write");
        assert_eq!(warning["disposition"], "retry");
        assert_eq!(warning["failed_attempts"], "1");
        assert_eq!(warning["error"], "Publisher liveness renewal timed out");

        let recovery = &events[1];
        assert_eq!(recovery["level"], "INFO");
        assert_eq!(recovery["message"], "Publisher liveness renewal recovered");
        assert_eq!(recovery["component"], "data_publisher");
        assert_eq!(recovery["operation"], "renew_liveness");
        assert_eq!(recovery["stage"], "redis_write");
        assert_eq!(recovery["disposition"], "recovered");
        assert_eq!(recovery["failed_attempts"], "1");
        assert!(!recovery.contains_key("error"));
    }

    async fn wait_for_failure_count(expected: u32, limit: Duration) {
        let deadline = Instant::now() + limit;
        while LIVENESS_FAILURE_PROBE.load(std::sync::atomic::Ordering::SeqCst) < expected {
            assert!(
                Instant::now() < deadline,
                "publisher failure probe timed out"
            );
            sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            LIVENESS_FAILURE_PROBE.load(std::sync::atomic::Ordering::SeqCst),
            expected
        );
    }

    async fn wait_for_liveness_events(
        events: &Arc<Mutex<Vec<BTreeMap<String, String>>>>,
        expected: usize,
        limit: Duration,
    ) -> Vec<BTreeMap<String, String>> {
        let deadline = Instant::now() + limit;
        loop {
            let captured: Vec<_> = events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| {
                    event.get("component").map(String::as_str) == Some("data_publisher")
                })
                .cloned()
                .collect();
            if captured.len() >= expected {
                return captured;
            }
            assert!(
                Instant::now() < deadline,
                "publisher liveness events timed out"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_lease(
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
        limit: Duration,
    ) {
        let deadline = Instant::now() + limit;
        loop {
            let value: Option<String> = bounded_redis(conn.get(key)).await;
            let ttl: i64 = bounded_redis(conn.ttl(key)).await;
            if value.as_deref() == Some("alive") && (1..=5).contains(&ttl) {
                return;
            }
            assert!(Instant::now() < deadline, "publisher lease was not renewed");
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_absence(
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
        limit: Duration,
    ) {
        let deadline = Instant::now() + limit;
        while bounded_redis(conn.exists(key)).await {
            assert!(Instant::now() < deadline, "publisher lease did not expire");
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn bounded_redis<T>(future: impl Future<Output = redis::RedisResult<T>>) -> T {
        timeout(Duration::from_secs(3), future)
            .await
            .expect("isolated Redis operation timed out")
            .unwrap()
    }

    #[test]
    fn dated_future_stream_key_and_payload_preserve_product_id() {
        let candle = Candlestick {
            product_id: "RITHMIC:MNQ-202509".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 1_704_067_200_000,
            open: rust_decimal_macros::dec!(20000),
            high: rust_decimal_macros::dec!(20000.25),
            low: rust_decimal_macros::dec!(19999.75),
            close: rust_decimal_macros::dec!(20000),
            volume: rust_decimal_macros::dec!(10),
        };

        assert_eq!(
            market_stream_key(&candle.product_id, Some(&candle.timeframe)).unwrap(),
            "stream:market:rithmic:mnq-202509:1m"
        );
        let payload = serde_json::to_string(&candle).unwrap();
        let decoded: Candlestick = serde_json::from_str(&payload).unwrap();
        assert_eq!(decoded.product_id, "RITHMIC:MNQ-202509");
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
