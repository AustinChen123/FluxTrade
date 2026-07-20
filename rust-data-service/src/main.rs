mod aggregator;
mod connector;
mod environment;
mod historical;
mod model;
mod publisher;
mod watchdog;

use crate::aggregator::CandleAggregator;
use crate::connector::backpack::BackpackConnector;
use crate::connector::binance::BinanceConnector;
use crate::connector::bybit::BybitConnector;
use crate::connector::ExchangeConnector;
use crate::model::UserStreamEvent;
use crate::publisher::{
    create_publish_channel, PublishSender, RedisPublisher, DEFAULT_CHANNEL_CAPACITY,
};

#[cfg(feature = "rithmic")]
use anyhow::Context;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info, warn, Level};

/// Maximum consecutive failures before triggering graceful shutdown.
const MAX_TASK_FAILURES: u32 = 3;

/// Backoff base duration for task restarts.
const RESTART_BACKOFF_BASE: Duration = Duration::from_secs(2);

/// Maximum backoff duration for task restarts.
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Runs the real-time data collection service
    Live {
        /// Optional: Comma separated exchanges to enable (e.g. binance,backpack)
        #[arg(short, long)]
        exchange: Option<String>,

        /// Optional: Comma separated symbols to subscribe (e.g. BTCUSDT,SOLUSDC)
        #[arg(short, long)]
        symbol: Option<String>,

        #[cfg(feature = "rithmic")]
        /// Credential profile under [rithmic.<profile>]
        #[arg(
            long,
            requires_all = ["rithmic_product_id", "rithmic_exchange", "rithmic_symbol"]
        )]
        rithmic_profile: Option<String>,

        #[cfg(feature = "rithmic")]
        /// Canonical dated product ID (e.g. RITHMIC:NQ-202609)
        #[arg(long, requires = "rithmic_profile")]
        rithmic_product_id: Option<String>,

        #[cfg(feature = "rithmic")]
        /// Rithmic exchange code (e.g. CME)
        #[arg(long, requires = "rithmic_profile")]
        rithmic_exchange: Option<String>,

        #[cfg(feature = "rithmic")]
        /// Exact Rithmic native contract symbol (e.g. NQU6)
        #[arg(long, requires = "rithmic_profile")]
        rithmic_symbol: Option<String>,
    },

    /// Downloads historical data
    Backfill {
        /// Exchange to download from (binance, bybit)
        #[arg(short, long)]
        exchange: String,

        /// Symbol to download (e.g., BTCUSDT)
        #[arg(short, long)]
        symbol: String,

        /// Start date (YYYY-MM-DD)
        #[arg(long)]
        start: String,

        /// End date (YYYY-MM-DD)
        #[arg(long)]
        end: String,

        /// Timeframe (1m, 5m, 1h, 1d) - Defaults to 1m
        #[arg(long, default_value = "1m")]
        timeframe: String,
    },

    #[cfg(feature = "rithmic")]
    /// Downloads exact Rithmic 1m bars into the candle database.
    RithmicHistory {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        product_id: String,
        #[arg(long)]
        exchange: String,
        #[arg(long)]
        symbol: String,
        /// Inclusive UTC epoch milliseconds, aligned to a minute boundary.
        #[arg(long)]
        start_ms: i64,
        /// Exclusive UTC epoch milliseconds, aligned to a minute boundary.
        #[arg(long)]
        end_ms: i64,
    },

    #[cfg(feature = "rithmic")]
    /// Reads remote ORDER/PNL ledger snapshots without changing orders.
    RithmicLedgerSnapshot {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        account_id: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    // Explicitly install CryptoProvider for rustls 0.23+
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let cli = Cli::parse();

    match cli.command.unwrap_or(Commands::Live {
        exchange: None,
        symbol: None,
        #[cfg(feature = "rithmic")]
        rithmic_profile: None,
        #[cfg(feature = "rithmic")]
        rithmic_product_id: None,
        #[cfg(feature = "rithmic")]
        rithmic_exchange: None,
        #[cfg(feature = "rithmic")]
        rithmic_symbol: None,
    }) {
        Commands::Live {
            exchange,
            symbol,
            #[cfg(feature = "rithmic")]
            rithmic_profile,
            #[cfg(feature = "rithmic")]
            rithmic_product_id,
            #[cfg(feature = "rithmic")]
            rithmic_exchange,
            #[cfg(feature = "rithmic")]
            rithmic_symbol,
        } => {
            run_live_mode(
                exchange,
                symbol,
                #[cfg(feature = "rithmic")]
                rithmic_profile,
                #[cfg(feature = "rithmic")]
                rithmic_product_id,
                #[cfg(feature = "rithmic")]
                rithmic_exchange,
                #[cfg(feature = "rithmic")]
                rithmic_symbol,
            )
            .await?
        }

        Commands::Backfill {
            exchange,
            symbol,
            start,
            end,
            timeframe,
        } => run_backfill_mode(exchange, symbol, start, end, timeframe).await?,

        #[cfg(feature = "rithmic")]
        Commands::RithmicHistory {
            profile,
            product_id,
            exchange,
            symbol,
            start_ms,
            end_ms,
        } => {
            let inserted = crate::connector::rithmic::history_runtime::run(
                &profile,
                &product_id,
                &exchange,
                &symbol,
                start_ms,
                end_ms,
            )
            .await?;
            info!(inserted, "Rithmic history backfill completed");
        }

        #[cfg(feature = "rithmic")]
        Commands::RithmicLedgerSnapshot {
            profile,
            account_id,
        } => {
            let snapshot =
                crate::connector::rithmic::ledger_runtime::run(&profile, account_id.as_deref())
                    .await?;
            info!(
                account_currency = snapshot.account.currency.as_deref().unwrap_or("unknown"),
                orders = snapshot.orders.len(),
                positions = snapshot.positions.len(),
                has_account_summary = snapshot.account_summary.is_some(),
                "Rithmic remote ledger snapshot completed"
            );
        }
    }

    Ok(())
}

async fn run_backfill_mode(
    exchange: String,
    symbol: String,
    start: String,
    end: String,
    timeframe: String,
) -> anyhow::Result<()> {
    info!(
        "Starting Backfill for {} on {} ({}) from {} to {}",
        symbol, exchange, timeframe, start, end
    );

    crate::historical::run_backfill(exchange, symbol, start, end, timeframe).await
}

/// Identifier for supervised tasks, used in logging and restart logic.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskId {
    Watchdog,
    Publisher,
    EventLoop,
    Connector(String),
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskId::Watchdog => write!(f, "watchdog"),
            TaskId::Publisher => write!(f, "publisher"),
            TaskId::EventLoop => write!(f, "event-loop"),
            TaskId::Connector(name) => write!(f, "connector:{}", name),
        }
    }
}

fn task_exit_requires_shutdown(task_id: &TaskId) -> bool {
    matches!(
        task_id,
        TaskId::Watchdog | TaskId::Publisher | TaskId::EventLoop
    ) || matches!(task_id, TaskId::Connector(name) if name == "rithmic")
}

/// Track failure counts per task for restart decisions.
struct TaskFailureTracker {
    failures: std::collections::HashMap<String, u32>,
    max_failures: u32,
}

#[cfg(feature = "rithmic")]
struct RithmicLiveArgs {
    profile: String,
    product_id: String,
    exchange: String,
    symbol: String,
}

#[cfg(feature = "rithmic")]
fn resolve_rithmic_live_args(
    enabled_exchanges: &str,
    profile: Option<String>,
    product_id: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
) -> anyhow::Result<Option<RithmicLiveArgs>> {
    let enabled_count = enabled_exchanges
        .split(',')
        .filter(|value| value.trim().eq_ignore_ascii_case("rithmic"))
        .count();
    anyhow::ensure!(
        enabled_count <= 1,
        "Rithmic exchange must not be enabled more than once"
    );
    if enabled_count == 0 {
        anyhow::ensure!(
            profile.is_none() && product_id.is_none() && exchange.is_none() && symbol.is_none(),
            "Rithmic options require --exchange rithmic"
        );
        return Ok(None);
    }

    Ok(Some(RithmicLiveArgs {
        profile: profile.context("--rithmic-profile is required")?,
        product_id: product_id.context("--rithmic-product-id is required")?,
        exchange: exchange.context("--rithmic-exchange is required")?,
        symbol: symbol.context("--rithmic-symbol is required")?,
    }))
}

impl TaskFailureTracker {
    fn new(max_failures: u32) -> Self {
        Self {
            failures: std::collections::HashMap::new(),
            max_failures,
        }
    }

    /// Record a failure. Returns true if the task should be restarted,
    /// false if max failures exceeded.
    fn record_failure(&mut self, task_id: &str) -> bool {
        let count = self.failures.entry(task_id.to_string()).or_insert(0);
        *count += 1;
        *count <= self.max_failures
    }

    /// Reset failure count for a task (on successful restart).
    #[allow(dead_code)]
    fn reset(&mut self, task_id: &str) {
        self.failures.remove(task_id);
    }

    /// Get current failure count for a task.
    fn get_failures(&self, task_id: &str) -> u32 {
        *self.failures.get(task_id).unwrap_or(&0)
    }

    /// Calculate backoff duration based on failure count.
    fn backoff_duration(&self, task_id: &str) -> Duration {
        let failures = self.get_failures(task_id);
        let backoff = RESTART_BACKOFF_BASE * 2u32.saturating_pow(failures.saturating_sub(1));
        std::cmp::min(backoff, RESTART_BACKOFF_MAX)
    }
}

async fn run_live_mode(
    exchange_opt: Option<String>,
    symbol_opt: Option<String>,
    #[cfg(feature = "rithmic")] rithmic_profile: Option<String>,
    #[cfg(feature = "rithmic")] rithmic_product_id: Option<String>,
    #[cfg(feature = "rithmic")] rithmic_exchange: Option<String>,
    #[cfg(feature = "rithmic")] rithmic_symbol: Option<String>,
) -> anyhow::Result<()> {
    info!("FluxTrade Data Service Starting (Live Mode)...");
    let runtime_environment = crate::environment::RuntimeEnvironment::from_env()?;
    info!(
        environment = runtime_environment.identity(),
        "Runtime environment identity resolved"
    );

    let redis_host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let redis_port = std::env::var("REDIS_PORT").unwrap_or_else(|_| "6379".into());
    let redis_url = match std::env::var("REDIS_PASSWORD") {
        Ok(pw) if !pw.is_empty() => format!("redis://:{}@{}:{}", pw, redis_host, redis_port),
        _ => format!("redis://{}:{}", redis_host, redis_port),
    };

    let channel_capacity = std::env::var("PUBLISHER_CHANNEL_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CHANNEL_CAPACITY);

    // Create the publish channel (Task 2: publisher channel pattern)
    let (pub_sender, pub_rx) = create_publish_channel(channel_capacity);

    let (trade_tx, trade_rx) = mpsc::channel(1000);
    let (candle_tx, candle_rx) = mpsc::channel(1000);
    let (user_tx, user_rx) = mpsc::channel(100);

    let enabled_exchanges = exchange_opt.unwrap_or_else(|| {
        std::env::var("EXCHANGE_ENABLED").unwrap_or_else(|_| "binance,bybit,backpack".into())
    });

    #[cfg(feature = "rithmic")]
    let rithmic_args = resolve_rithmic_live_args(
        &enabled_exchanges,
        rithmic_profile,
        rithmic_product_id,
        rithmic_exchange,
        rithmic_symbol,
    )?;
    #[cfg(feature = "rithmic")]
    let mut rithmic_config = rithmic_args
        .map(|args| {
            crate::connector::rithmic::live::configure(
                &args.profile,
                args.product_id,
                args.exchange,
                args.symbol,
            )
        })
        .transpose()?;

    let symbols_str = symbol_opt.unwrap_or_else(|| "BTCUSDT,SOLUSDC".into());
    let symbols: Vec<String> = symbols_str
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    // --- Supervised task set (Task 1: task supervision) ---
    let mut join_set: JoinSet<(TaskId, anyhow::Result<()>)> = JoinSet::new();
    let mut tracker = TaskFailureTracker::new(MAX_TASK_FAILURES);

    // Spawn Watchdog task
    let watchdog_redis_url = redis_url.clone();
    let watchdog_environment = runtime_environment.clone();
    join_set.spawn(async move {
        let result = match crate::watchdog::Watchdog::new(&watchdog_redis_url, watchdog_environment)
        {
            Ok(wd) => wd.run().await,
            Err(e) => Err(anyhow::anyhow!("Failed to initialize Watchdog: {}", e)),
        };
        (TaskId::Watchdog, result)
    });
    info!("Supervised task spawned: watchdog");

    // Spawn Publisher task (Task 2: dedicated publisher with channel)
    let publisher_redis_url = redis_url.clone();
    join_set.spawn(async move {
        let result = match RedisPublisher::new(&publisher_redis_url) {
            Ok(mut publisher) => publisher.run(pub_rx).await,
            Err(e) => Err(e),
        };
        (TaskId::Publisher, result)
    });
    info!(
        "Supervised task spawned: publisher (channel capacity: {})",
        channel_capacity
    );

    // Spawn Connector tasks
    for ex in enabled_exchanges.split(',') {
        let trade_tx = trade_tx.clone();
        let candle_tx = candle_tx.clone();
        let user_tx = user_tx.clone();
        let symbols = symbols.clone();
        let exchange_name = ex.trim().to_lowercase();

        match exchange_name.as_str() {
            "binance" => {
                join_set.spawn(async move {
                    let result = run_binance_connector(symbols, trade_tx, candle_tx, user_tx).await;
                    (TaskId::Connector("binance".to_string()), result)
                });
                info!("Supervised task spawned: connector:binance");
            }
            "bybit" => {
                join_set.spawn(async move {
                    let result = run_bybit_connector(symbols, trade_tx, candle_tx).await;
                    (TaskId::Connector("bybit".to_string()), result)
                });
                info!("Supervised task spawned: connector:bybit");
            }
            "backpack" => {
                join_set.spawn(async move {
                    let result =
                        run_backpack_connector(symbols, trade_tx, candle_tx, user_tx).await;
                    (TaskId::Connector("backpack".to_string()), result)
                });
                info!("Supervised task spawned: connector:backpack");
            }
            #[cfg(feature = "rithmic")]
            "rithmic" => {
                let config = rithmic_config.take().expect("Rithmic configuration loaded");
                join_set.spawn(async move {
                    let result = crate::connector::rithmic::live::run(config, candle_tx).await;
                    (TaskId::Connector("rithmic".to_string()), result)
                });
                info!("Supervised task spawned: connector:rithmic");
            }
            _ => warn!("Unknown exchange in EXCHANGE_ENABLED: {}", ex),
        }
    }

    // Drop the extra sender clones so channels close properly when connectors exit
    drop(trade_tx);
    drop(candle_tx);
    drop(user_tx);

    // Spawn the main event loop (aggregation + forwarding to publisher channel)
    let event_pub_sender = pub_sender.clone();
    join_set.spawn(async move {
        let result = run_event_loop(trade_rx, candle_rx, user_rx, event_pub_sender).await;
        (TaskId::EventLoop, result)
    });
    info!("Supervised task spawned: event-loop");

    // --- Supervisor loop ---
    info!(
        "Supervisor active. Monitoring {} tasks. Press Ctrl+C to shutdown.",
        join_set.len()
    );

    // Store context needed for task restarts
    let redis_url_for_restart = redis_url.clone();
    let environment_for_restart = runtime_environment;
    let symbols_for_restart = symbols;
    let exchanges_for_restart = enabled_exchanges;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal, stopping all tasks...");
                join_set.shutdown().await;
                break;
            }

            result = join_set.join_next() => {
                match result {
                    None => {
                        // All tasks have exited
                        info!("All supervised tasks have exited");
                        break;
                    }
                    Some(Ok((task_id, task_result))) => {
                        match task_result {
                            Ok(()) => {
                                if task_exit_requires_shutdown(&task_id) {
                                    error!("Critical task '{}' exited unexpectedly", task_id);
                                    join_set.shutdown().await;
                                    return Err(anyhow::anyhow!(
                                        "Critical task '{}' exited unexpectedly",
                                        task_id
                                    ));
                                }
                                info!("Task '{}' completed normally", task_id);
                            }
                            Err(ref e) => {
                                error!("Task '{}' failed: {:#}", task_id, e);
                                if task_exit_requires_shutdown(&task_id) {
                                    join_set.shutdown().await;
                                    return Err(anyhow::anyhow!(
                                        "Critical task '{}' failed: {}",
                                        task_id,
                                        e
                                    ));
                                }
                                let task_key = task_id.to_string();
                                let should_restart = tracker.record_failure(&task_key);
                                let failures = tracker.get_failures(&task_key);

                                if should_restart {
                                    let backoff = tracker.backoff_duration(&task_key);
                                    warn!(
                                        "Task '{}' will restart in {:?} (failure {}/{})",
                                        task_id, backoff, failures, MAX_TASK_FAILURES
                                    );
                                    tokio::time::sleep(backoff).await;

                                    // Restart the failed task
                                    restart_task(
                                        &task_id,
                                        &mut join_set,
                                        &redis_url_for_restart,
                                        &environment_for_restart,
                                        &pub_sender,
                                        &symbols_for_restart,
                                        &exchanges_for_restart,
                                    );
                                } else {
                                    error!(
                                        "Task '{}' exceeded max failures ({}). Initiating graceful shutdown.",
                                        task_id, MAX_TASK_FAILURES
                                    );
                                    join_set.shutdown().await;
                                    return Err(anyhow::anyhow!(
                                        "Critical task '{}' failed {} times, shutting down",
                                        task_id,
                                        failures
                                    ));
                                }
                            }
                        }
                    }
                    Some(Err(join_err)) => {
                        // Task panicked or was cancelled
                        if join_err.is_panic() {
                            error!("A supervised task panicked: {:?}", join_err);
                            error!("Panic in supervised task is unrecoverable, shutting down");
                            join_set.shutdown().await;
                            return Err(anyhow::anyhow!("Task panic: {:?}", join_err));
                        } else {
                            // Task was cancelled (e.g. by shutdown)
                            info!("A task was cancelled during shutdown");
                        }
                    }
                }
            }
        }
    }

    // Graceful cleanup
    info!("Closing all connections...");
    info!("FluxTrade Data Service stopped.");

    Ok(())
}

/// Restart a failed task by spawning a new instance into the JoinSet.
/// Note: connector restarts create new mpsc senders but those feed into
/// the existing event loop. The publisher and event-loop are not trivially
/// restartable because their channel receivers are consumed, so they
/// trigger a full shutdown instead.
fn restart_task(
    task_id: &TaskId,
    join_set: &mut JoinSet<(TaskId, anyhow::Result<()>)>,
    redis_url: &str,
    environment: &crate::environment::RuntimeEnvironment,
    _pub_sender: &PublishSender,
    _symbols: &[String],
    _exchanges: &str,
) {
    match task_id {
        TaskId::Watchdog => {
            let redis_url = redis_url.to_string();
            let environment = environment.clone();
            join_set.spawn(async move {
                let result = match crate::watchdog::Watchdog::new(&redis_url, environment) {
                    Ok(wd) => wd.run().await,
                    Err(e) => Err(anyhow::anyhow!("Watchdog init failed: {}", e)),
                };
                (TaskId::Watchdog, result)
            });
            info!("Restarted task: watchdog");
        }
        TaskId::Publisher | TaskId::EventLoop => {
            // Publisher and EventLoop hold consumed channel receivers.
            // They cannot be trivially restarted without recreating the whole pipeline.
            // We log this and let the supervisor trigger a full shutdown via the
            // failure tracker exceeding max failures.
            error!(
                "Task '{}' cannot be restarted (channel receiver consumed). \
                 This will count toward max failures and eventually trigger shutdown.",
                task_id
            );
        }
        TaskId::Connector(name) => {
            // Connectors can't be restarted here because their mpsc Senders
            // (trade_tx, candle_tx, user_tx) were moved into the original spawn.
            // The WebSocketManager inside each connector already has its own
            // retry/reconnect logic, so connector task exits are rare.
            // We log and let the failure tracker decide.
            warn!(
                "Connector '{}' exited and cannot be trivially restarted from supervisor. \
                 The connector's internal WebSocket retry logic should handle most reconnections.",
                name
            );
        }
    }
}

/// Run the main event loop: receives trades/candles/user events from connectors,
/// runs aggregation, and forwards to the publisher channel.
async fn run_event_loop(
    mut trade_rx: mpsc::Receiver<model::Trade>,
    mut candle_rx: mpsc::Receiver<model::Candlestick>,
    mut user_rx: mpsc::Receiver<UserStreamEvent>,
    pub_sender: PublishSender,
) -> anyhow::Result<()> {
    let mut aggregator = CandleAggregator::new();
    let mut trade_open = true;
    let mut candle_open = true;
    let mut user_open = true;

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
                        // Publish 1m candle
                        if let Err(e) = pub_sender.publish_candle(&candle).await {
                            warn!("Failed to send candle to publisher: {}", e);
                        }

                        // Aggregate to 5m and 15m
                        if let Some(c5) = aggregator.add_candle(&candle, "5m") {
                            if let Err(e) = pub_sender.publish_candle(&c5).await {
                                warn!("Failed to send 5m candle to publisher: {}", e);
                            }
                        }

                        if let Some(c15) = aggregator.add_candle(&candle, "15m") {
                            if let Err(e) = pub_sender.publish_candle(&c15).await {
                                warn!("Failed to send 15m candle to publisher: {}", e);
                            }
                        }
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
        }

        if !trade_open && !candle_open && !user_open {
            info!("All event channels closed, event loop exiting");
            return Ok(());
        }
    }
}

/// Run the Binance connector: subscribes to trades, candles, and user stream.
async fn run_binance_connector(
    symbols: Vec<String>,
    trade_tx: mpsc::Sender<model::Trade>,
    candle_tx: mpsc::Sender<model::Candlestick>,
    user_tx: mpsc::Sender<UserStreamEvent>,
) -> anyhow::Result<()> {
    let mut conn = BinanceConnector::new();
    info!("Starting Binance Connector...");

    if let Err(e) = conn.subscribe_trades(&symbols, trade_tx).await {
        error!("Binance trades error: {}", e);
        return Err(e);
    }

    if let Err(e) = conn.subscribe_candles(&symbols, "1m", candle_tx).await {
        error!("Binance candles error: {}", e);
        return Err(e);
    }

    // Start User Stream if API Key is present
    if std::env::var("BINANCE_API_KEY").is_ok() {
        if let Err(e) = conn.subscribe_user_stream(user_tx).await {
            error!("Binance user stream error: {}", e);
            return Err(e);
        }
    } else {
        info!("BINANCE_API_KEY not found, skipping User Data Stream");
    }

    // Keep the task alive — connector internal tasks handle the WebSocket loops.
    // We use a pending future that will only resolve if cancelled.
    std::future::pending::<()>().await;
    Ok(())
}

/// Run the Bybit connector: subscribes to trades and candles.
async fn run_bybit_connector(
    symbols: Vec<String>,
    trade_tx: mpsc::Sender<model::Trade>,
    candle_tx: mpsc::Sender<model::Candlestick>,
) -> anyhow::Result<()> {
    let mut conn = BybitConnector::new();
    info!("Starting Bybit Connector...");

    if let Err(e) = conn.subscribe_trades(&symbols, trade_tx).await {
        error!("Bybit trades error: {}", e);
        return Err(e);
    }

    if let Err(e) = conn.subscribe_candles(&symbols, "1m", candle_tx).await {
        error!("Bybit candles error: {}", e);
        return Err(e);
    }

    std::future::pending::<()>().await;
    Ok(())
}

/// Run the Backpack connector: subscribes to trades, candles, and user stream.
async fn run_backpack_connector(
    _symbols: Vec<String>,
    trade_tx: mpsc::Sender<model::Trade>,
    candle_tx: mpsc::Sender<model::Candlestick>,
    user_tx: mpsc::Sender<UserStreamEvent>,
) -> anyhow::Result<()> {
    let mut conn = BackpackConnector::new();
    info!("Starting Backpack Connector...");

    // Backpack symbols often use underscore
    let backpack_symbols = vec!["BTC_USDC".to_string(), "SOL_USDC".to_string()];

    if let Err(e) = conn.subscribe_trades(&backpack_symbols, trade_tx).await {
        error!("Backpack trades error: {}", e);
        return Err(e);
    }

    if let Err(e) = conn
        .subscribe_candles(&backpack_symbols, "1m", candle_tx)
        .await
    {
        error!("Backpack candles error: {}", e);
        return Err(e);
    }

    // Start User Stream if API Key is present
    if std::env::var("EXCHANGE_API_KEY").is_ok() && std::env::var("EXCHANGE_SECRET").is_ok() {
        if let Err(e) = conn.subscribe_user_stream(user_tx).await {
            error!("Backpack user stream error: {}", e);
            return Err(e);
        }
    } else {
        info!("Backpack API Key/Secret not found, skipping User Data Stream");
    }

    std::future::pending::<()>().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_id_display() {
        assert_eq!(TaskId::Watchdog.to_string(), "watchdog");
        assert_eq!(TaskId::Publisher.to_string(), "publisher");
        assert_eq!(TaskId::EventLoop.to_string(), "event-loop");
        assert_eq!(
            TaskId::Connector("binance".to_string()).to_string(),
            "connector:binance"
        );
    }

    #[test]
    fn rithmic_task_exit_policy_fails_closed() {
        for task_id in [TaskId::Connector("binance".to_string())] {
            assert!(!task_exit_requires_shutdown(&task_id));
        }
        for task_id in [TaskId::Watchdog, TaskId::Publisher, TaskId::EventLoop] {
            assert!(task_exit_requires_shutdown(&task_id));
        }
        assert!(task_exit_requires_shutdown(&TaskId::Connector(
            "rithmic".to_string()
        )));
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn rithmic_live_arguments_fail_closed_before_startup() {
        assert!(resolve_rithmic_live_args("binance", None, None, None, None)
            .unwrap()
            .is_none());
        assert!(resolve_rithmic_live_args(
            "rithmic",
            Some("lucid".to_string()),
            Some("RITHMIC:NQ-202609".to_string()),
            Some("CME".to_string()),
            Some("NQU6".to_string()),
        )
        .unwrap()
        .is_some());

        for args in [
            ("rithmic", None, None, None, None),
            ("rithmic,rithmic", None, None, None, None),
            (
                "binance",
                Some("lucid".to_string()),
                Some("RITHMIC:NQ-202609".to_string()),
                Some("CME".to_string()),
                Some("NQU6".to_string()),
            ),
        ] {
            assert!(resolve_rithmic_live_args(args.0, args.1, args.2, args.3, args.4).is_err());
        }
    }

    #[test]
    fn test_failure_tracker_basic() {
        let mut tracker = TaskFailureTracker::new(3);

        // First 3 failures should allow restart
        assert!(tracker.record_failure("watchdog"));
        assert!(tracker.record_failure("watchdog"));
        assert!(tracker.record_failure("watchdog"));
        // 4th failure exceeds max
        assert!(!tracker.record_failure("watchdog"));
    }

    #[test]
    fn test_failure_tracker_independent_tasks() {
        let mut tracker = TaskFailureTracker::new(2);

        assert!(tracker.record_failure("watchdog"));
        assert!(tracker.record_failure("publisher"));
        assert!(tracker.record_failure("watchdog"));
        // watchdog at 2, publisher at 1
        assert!(!tracker.record_failure("watchdog")); // 3rd > max(2)
        assert!(tracker.record_failure("publisher")); // 2nd <= max(2)
    }

    #[test]
    fn test_failure_tracker_backoff() {
        let tracker = TaskFailureTracker::new(5);

        // No failures yet: base duration
        let d = tracker.backoff_duration("task");
        assert_eq!(d, RESTART_BACKOFF_BASE);
    }

    #[test]
    fn test_failure_tracker_backoff_exponential() {
        let mut tracker = TaskFailureTracker::new(10);
        tracker.record_failure("task"); // 1 failure
        let d1 = tracker.backoff_duration("task");
        assert_eq!(d1, RESTART_BACKOFF_BASE); // 2^0 * base

        tracker.record_failure("task"); // 2 failures
        let d2 = tracker.backoff_duration("task");
        assert_eq!(d2, RESTART_BACKOFF_BASE * 2); // 2^1 * base

        tracker.record_failure("task"); // 3 failures
        let d3 = tracker.backoff_duration("task");
        assert_eq!(d3, RESTART_BACKOFF_BASE * 4); // 2^2 * base
    }

    #[test]
    fn test_failure_tracker_backoff_capped() {
        let mut tracker = TaskFailureTracker::new(20);
        for _ in 0..15 {
            tracker.record_failure("task");
        }
        let d = tracker.backoff_duration("task");
        assert!(d <= RESTART_BACKOFF_MAX);
    }

    #[test]
    fn test_failure_tracker_reset() {
        let mut tracker = TaskFailureTracker::new(3);
        tracker.record_failure("watchdog");
        tracker.record_failure("watchdog");
        assert_eq!(tracker.get_failures("watchdog"), 2);

        tracker.reset("watchdog");
        assert_eq!(tracker.get_failures("watchdog"), 0);
        // After reset, can fail again
        assert!(tracker.record_failure("watchdog"));
    }

    #[tokio::test]
    async fn test_event_loop_exits_on_channel_close() {
        let (_trade_tx, trade_rx) = mpsc::channel(10);
        let (_candle_tx, candle_rx) = mpsc::channel(10);
        let (_user_tx, user_rx) = mpsc::channel(10);
        let (pub_sender, _pub_rx) = create_publish_channel(10);

        // Drop all senders to close the channels
        drop(_trade_tx);
        drop(_candle_tx);
        drop(_user_tx);

        // Event loop should exit gracefully when all channels are closed
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_event_loop(trade_rx, candle_rx, user_rx, pub_sender),
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
        let (pub_sender, mut pub_rx) = create_publish_channel(1);
        drop(trade_tx);
        drop(user_tx);

        let event_loop = tokio::spawn(run_event_loop(trade_rx, candle_rx, user_rx, pub_sender));
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
            Some(crate::publisher::PublishMessage::Candle(_))
        ));
        drop(candle_tx);
        assert!(event_loop.await.unwrap().is_ok());
    }
}
