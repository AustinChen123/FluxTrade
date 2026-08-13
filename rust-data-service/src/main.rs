mod aggregator;
mod connector;
mod environment;
mod historical;
mod model;
mod publisher;
mod watchdog;

use crate::aggregator::CandleAggregator;
use crate::model::UserStreamEvent;
use crate::publisher::{
    create_publish_channel, PublishSender, RedisPublisher, DEFAULT_CHANNEL_CAPACITY,
};

use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use std::process::ExitCode;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info, warn, Level};

#[cfg_attr(not(feature = "rithmic"), allow(dead_code))]
#[derive(Debug)]
pub(crate) enum AggregationSourceEvent {
    Candle(model::Candlestick),
    ResetProduct(String),
}

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
            requires_all = [
                "rithmic_account_id",
                "rithmic_product_id",
                "rithmic_exchange",
                "rithmic_symbol"
            ]
        )]
        rithmic_profile: Option<String>,

        #[cfg(feature = "rithmic")]
        /// Exact account ID used for emergency ledger reconciliation.
        #[arg(long, requires = "rithmic_profile")]
        rithmic_account_id: Option<String>,

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
    /// Downloads closed exact Rithmic 1m bars into the candle database.
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
    /// Downloads closed exact Rithmic 1m bars into an atomic CSV file.
    RithmicHistoryExport {
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
        #[arg(long)]
        output: std::path::PathBuf,
    },

    #[cfg(feature = "rithmic")]
    /// Reads remote ORDER/PNL ledger snapshots without changing orders.
    RithmicLedgerSnapshot {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        account_id: Option<String>,
    },

    #[cfg(feature = "rithmic")]
    /// Resolves one root symbol to Rithmic's current front-month contract.
    RithmicFrontMonth {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        root_symbol: String,
        #[arg(long)]
        exchange: String,
        #[arg(long)]
        exclusive_session: bool,
        #[arg(long, default_value_t = 15)]
        timeout_seconds: u64,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    dotenv().ok();

    initialize_process_diagnostics();

    // Explicitly install CryptoProvider for rustls 0.23+
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    match run_application().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            report_terminal_failure(&error);
            ExitCode::FAILURE
        }
    }
}

fn initialize_process_diagnostics() {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(Level::DEBUG)
        .init();
    install_sanitized_panic_hook();
}

fn install_sanitized_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let (source_file, source_line, source_column) =
            info.location().map_or(("unknown", 0, 0), |location| {
                (
                    std::path::Path::new(location.file())
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .unwrap_or("unknown"),
                    location.line(),
                    location.column(),
                )
            });
        warn!(
            component = %"runtime", task = %"unknown", operation = %"panic",
            stage = %"panic_hook", template_id = %"unknown", payload_len = %"unknown",
            stable_error_code = %"panic_observed", disposition = %"continue_unwind",
            state_effect = %"unwinding", safe_cause = %"panic payload suppressed",
            source_file = %source_file, source_line, source_column,
            "FluxTrade panic observed"
        );
    }));
}

async fn run_application() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Commands::Live {
        exchange: None,
        symbol: None,
        #[cfg(feature = "rithmic")]
        rithmic_profile: None,
        #[cfg(feature = "rithmic")]
        rithmic_account_id: None,
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
            rithmic_account_id,
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
                rithmic_account_id,
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
        Commands::RithmicHistoryExport {
            profile,
            product_id,
            exchange,
            symbol,
            start_ms,
            end_ms,
            output,
        } => {
            let exported = crate::connector::rithmic::history_runtime::export_csv(
                &profile,
                &product_id,
                &exchange,
                &symbol,
                start_ms,
                end_ms,
                &output,
            )
            .await?;
            info!(
                exported,
                output = %output.display(),
                "Rithmic history CSV export completed"
            );
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

        #[cfg(feature = "rithmic")]
        Commands::RithmicFrontMonth {
            profile,
            root_symbol,
            exchange,
            exclusive_session,
            timeout_seconds,
        } => {
            anyhow::ensure!(
                (1..=120).contains(&timeout_seconds),
                "--timeout-seconds must be between 1 and 120"
            );
            let trading_symbol = crate::connector::rithmic::front_month_runtime::run(
                &profile,
                &root_symbol,
                &exchange,
                exclusive_session,
                std::time::Duration::from_secs(timeout_seconds),
            )
            .await?;
            println!("{trading_symbol}");
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

fn supervised_task_exit_error(task_id: &TaskId, task_result: anyhow::Result<()>) -> anyhow::Error {
    match task_result {
        Ok(()) => {
            SupervisedFailure::new(task_id, SupervisedFailureKind::UnexpectedExit, None).into()
        }
        Err(error) => {
            SupervisedFailure::new(task_id, SupervisedFailureKind::TaskError, Some(error)).into()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SupervisedFailureKind {
    TaskError,
    UnexpectedExit,
    CancelledJoin,
    PanickedJoin,
    OtherJoin,
}

impl SupervisedFailureKind {
    fn diagnostic(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::TaskError => (
                "task_exit",
                "supervised_task_failed",
                "supervised task failed",
            ),
            Self::UnexpectedExit => (
                "task_exit",
                "supervised_task_exited",
                "supervised task exited unexpectedly",
            ),
            Self::CancelledJoin => (
                "task_join",
                "supervised_task_cancelled",
                "supervised task was cancelled",
            ),
            Self::PanickedJoin => (
                "task_join",
                "supervised_task_panicked",
                "supervised task panicked",
            ),
            Self::OtherJoin => (
                "task_join",
                "supervised_task_join_failed",
                "supervised task join failed",
            ),
        }
    }
}

#[derive(Debug)]
struct SupervisedFailure {
    task: String,
    kind: SupervisedFailureKind,
    source: Option<anyhow::Error>,
}

impl SupervisedFailure {
    fn new(task: &TaskId, kind: SupervisedFailureKind, source: Option<anyhow::Error>) -> Self {
        Self {
            task: task.to_string(),
            kind,
            source,
        }
    }
}

impl std::fmt::Display for SupervisedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.kind.diagnostic().2)
    }
}

impl std::error::Error for SupervisedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
}

fn supervised_join_error(error: tokio::task::JoinError) -> anyhow::Error {
    let kind = classify_join_failure(error.is_cancelled(), error.is_panic());
    SupervisedFailure {
        task: "unknown".to_string(),
        kind,
        source: None,
    }
    .into()
}

fn classify_join_failure(cancelled: bool, panicked: bool) -> SupervisedFailureKind {
    if cancelled {
        SupervisedFailureKind::CancelledJoin
    } else if panicked {
        SupervisedFailureKind::PanickedJoin
    } else {
        SupervisedFailureKind::OtherJoin
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TerminalDiagnostic {
    component: &'static str,
    task: String,
    operation: &'static str,
    stage: &'static str,
    template_id: String,
    payload_len: String,
    stable_error_code: &'static str,
    disposition: &'static str,
    state_effect: &'static str,
    safe_cause: &'static str,
}

fn terminal_diagnostic(error: &anyhow::Error) -> TerminalDiagnostic {
    let supervisor = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<SupervisedFailure>());
    let task = supervisor.map_or_else(|| "unknown".to_string(), |failure| failure.task.clone());
    if let Some(failure) = crate::connector::terminal::classify(error) {
        return TerminalDiagnostic {
            component: failure.component,
            task,
            operation: failure.operation,
            stage: failure.stage,
            template_id: failure.template_id,
            payload_len: failure.payload_len,
            stable_error_code: failure.stable_error_code,
            disposition: failure.disposition,
            state_effect: failure.state_effect,
            safe_cause: failure.safe_cause,
        };
    }
    let (stage, stable_error_code, safe_cause) = supervisor
        .map(|failure| failure.kind.diagnostic())
        .unwrap_or((
            "unknown",
            "terminal_failure",
            "terminal failure details unavailable",
        ));
    TerminalDiagnostic {
        component: "unknown",
        task,
        operation: "unknown",
        stage,
        template_id: "unknown".to_string(),
        payload_len: "unknown".to_string(),
        stable_error_code,
        disposition: "fatal_service_exit",
        state_effect: "process_exit",
        safe_cause,
    }
}

fn report_terminal_failure(error: &anyhow::Error) {
    let diagnostic = terminal_diagnostic(error);
    error!(
        component = %diagnostic.component, task = %diagnostic.task,
        operation = %diagnostic.operation, stage = %diagnostic.stage,
        template_id = %diagnostic.template_id, payload_len = %diagnostic.payload_len,
        stable_error_code = %diagnostic.stable_error_code, disposition = %diagnostic.disposition,
        state_effect = %diagnostic.state_effect, safe_cause = %diagnostic.safe_cause,
        "FluxTrade terminal failure"
    );
}

#[cfg(test)]
#[derive(Clone)]
struct CaptureLayer {
    events: std::sync::Arc<std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>>,
}

#[cfg(test)]
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != Level::ERROR {
            return;
        }
        let mut fields = std::collections::BTreeMap::new();
        event.record(&mut FieldVisitor(&mut fields));
        self.events.lock().unwrap().push(fields);
    }
}

#[cfg(test)]
struct FieldVisitor<'a>(&'a mut std::collections::BTreeMap<String, String>);

#[cfg(test)]
impl tracing::field::Visit for FieldVisitor<'_> {
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

#[cfg(test)]
fn capture_error_events(
    operation: impl FnOnce(),
) -> Vec<std::collections::BTreeMap<String, String>> {
    use tracing_subscriber::prelude::*;

    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer {
        events: std::sync::Arc::clone(&events),
    });
    tracing::subscriber::with_default(subscriber, operation);
    let captured = events.lock().unwrap().clone();
    captured
}

#[cfg(test)]
fn capture_terminal_event(error: &anyhow::Error) -> std::collections::BTreeMap<String, String> {
    let mut captured = capture_error_events(|| report_terminal_failure(error));
    assert_eq!(captured.len(), 1);
    captured.pop().unwrap()
}

async fn run_live_mode(
    exchange_opt: Option<String>,
    symbol_opt: Option<String>,
    #[cfg(feature = "rithmic")] rithmic_profile: Option<String>,
    #[cfg(feature = "rithmic")] rithmic_account_id: Option<String>,
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
    let (aggregation_source_tx, aggregation_source_rx) = mpsc::channel(1000);

    let live_runtime = crate::connector::live_runtime::LiveRuntime::prepare(
        crate::connector::live_runtime::LiveRuntimeOptions::new(
            exchange_opt,
            symbol_opt,
            #[cfg(feature = "rithmic")]
            rithmic_profile,
            #[cfg(feature = "rithmic")]
            rithmic_account_id,
            #[cfg(feature = "rithmic")]
            rithmic_product_id,
            #[cfg(feature = "rithmic")]
            rithmic_exchange,
            #[cfg(feature = "rithmic")]
            rithmic_symbol,
        ),
    )?;

    // --- Supervised task set (Task 1: task supervision) ---
    let mut join_set: JoinSet<(TaskId, anyhow::Result<()>)> = JoinSet::new();
    // Spawn Watchdog task
    let watchdog_redis_url = redis_url.clone();
    let watchdog_environment = runtime_environment.clone();
    let execution_venue = non_empty_env("EXCHANGE_ID");
    let watchdog_mitigation = crate::connector::emergency::resolve(
        &watchdog_environment,
        execution_venue.as_deref(),
        live_runtime.watchdog_identity(),
    )?;
    join_set.spawn(async move {
        let result = match crate::watchdog::Watchdog::new(
            &watchdog_redis_url,
            watchdog_environment,
            watchdog_mitigation,
        ) {
            Ok(wd) => wd.run().await,
            Err(e) => Err(anyhow::anyhow!("Failed to initialize Watchdog: {}", e)),
        };
        (TaskId::Watchdog, result)
    });
    info!("Supervised task spawned: watchdog");

    // Spawn Publisher task (Task 2: dedicated publisher with channel)
    let publisher_redis_url = redis_url.clone();
    let publisher_environment = runtime_environment;
    join_set.spawn(async move {
        let result = match RedisPublisher::new(&publisher_redis_url, publisher_environment) {
            Ok(mut publisher) => publisher.run(pub_rx).await,
            Err(e) => Err(e),
        };
        (TaskId::Publisher, result)
    });
    info!(
        "Supervised task spawned: publisher (channel capacity: {})",
        channel_capacity
    );

    live_runtime.spawn(
        &mut join_set,
        trade_tx.clone(),
        candle_tx.clone(),
        user_tx.clone(),
        aggregation_source_tx.clone(),
    );

    // Drop the extra sender clones so channels close properly when connectors exit
    drop(trade_tx);
    drop(candle_tx);
    drop(user_tx);
    drop(aggregation_source_tx);

    // Spawn the main event loop (aggregation + forwarding to publisher channel)
    let event_pub_sender = pub_sender.clone();
    join_set.spawn(async move {
        let result = run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_source_rx,
            event_pub_sender,
        )
        .await;
        (TaskId::EventLoop, result)
    });
    info!("Supervised task spawned: event-loop");

    // --- Supervisor loop ---
    info!(
        "Supervisor active. Monitoring {} tasks. Press Ctrl+C to shutdown.",
        join_set.len()
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping all tasks...");
            join_set.shutdown().await;
        }

        result = join_set.join_next() => {
            match result {
                None => {
                    info!("All supervised tasks have exited");
                }
                Some(Ok((task_id, task_result))) => {
                    let error = supervised_task_exit_error(&task_id, task_result);
                    join_set.shutdown().await;
                    return Err(error);
                }
                Some(Err(join_err)) => {
                    join_set.shutdown().await;
                    return Err(supervised_join_error(join_err));
                }
            }
        }
    }

    // Graceful cleanup
    info!("Closing all connections...");
    info!("FluxTrade Data Service stopped.");

    Ok(())
}

/// Run the main event loop: receives trades/candles/user events from connectors,
/// runs aggregation, and forwards to the publisher channel.
async fn run_event_loop(
    mut trade_rx: mpsc::Receiver<model::Trade>,
    mut candle_rx: mpsc::Receiver<model::Candlestick>,
    mut user_rx: mpsc::Receiver<UserStreamEvent>,
    mut aggregation_source_rx: mpsc::Receiver<AggregationSourceEvent>,
    pub_sender: PublishSender,
) -> anyhow::Result<()> {
    let mut aggregator = CandleAggregator::new();
    let mut trade_open = true;
    let mut candle_open = true;
    let mut user_open = true;
    let mut aggregation_source_open = true;

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
                        publish_and_aggregate_candle(&mut aggregator, &pub_sender, candle).await;
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

            event = aggregation_source_rx.recv(), if aggregation_source_open => {
                match event {
                    Some(AggregationSourceEvent::Candle(candle)) => {
                        publish_and_aggregate_candle(&mut aggregator, &pub_sender, candle).await;
                    }
                    Some(AggregationSourceEvent::ResetProduct(product_id)) => {
                        aggregator.reset_product(&product_id);
                        info!(product_id, "Aggregation state reset after source reconnect");
                    }
                    None => {
                        aggregation_source_open = false;
                    }
                }
            }
        }

        if !trade_open && !candle_open && !user_open && !aggregation_source_open {
            info!("All event channels closed, event loop exiting");
            return Ok(());
        }
    }
}

async fn publish_and_aggregate_candle(
    aggregator: &mut CandleAggregator,
    pub_sender: &PublishSender,
    candle: model::Candlestick,
) {
    if let Err(e) = pub_sender.publish_candle(&candle).await {
        warn!("Failed to send candle to publisher: {}", e);
    }

    for target_timeframe in ["5m", "15m"] {
        let can_derive = CandleAggregator::can_aggregate(&candle.timeframe, target_timeframe);
        match can_derive {
            Ok(true) if candle.timeframe != target_timeframe => {}
            Ok(_) => continue,
            Err(e) => {
                warn!(
                    "Invalid source/target timeframe pair {} -> {}: {}",
                    candle.timeframe, target_timeframe, e
                );
                continue;
            }
        }
        match aggregator.add_candle(&candle, target_timeframe) {
            Ok(Some(completed)) => {
                if let Err(e) = pub_sender.publish_candle(&completed).await {
                    warn!(
                        "Failed to send {} candle to publisher: {}",
                        target_timeframe, e
                    );
                }
            }
            Ok(None) => {}
            Err(e) => warn!(
                "Failed to aggregate {} -> {} candle: {}",
                candle.timeframe, target_timeframe, e
            ),
        }
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    normalized_optional_value(std::env::var(name).ok())
}

fn normalized_optional_value(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::process::Command;
    use std::sync::Mutex;
    use std::time::Duration;

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    const PANIC_CHILD_ENV: &str = "FLUXTRADE_PANIC_HOOK_SUBPROCESS";
    const PANIC_CHILD_VALUE: &str = "fluxtrade-panic-hook-child-v1";
    const PANIC_SENTINEL: &str = "panic-provider-payload-sentinel";

    struct EnvVarGuard {
        name: &'static str,
        prior: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn remove(name: &'static str) -> Self {
            let prior = std::env::var_os(name);
            std::env::remove_var(name);
            Self { name, prior }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.prior.take() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

    fn assert_generic_terminal_fields(fields: &std::collections::BTreeMap<String, String>) {
        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "unknown");
        assert_eq!(fields["task"], "unknown");
        assert_eq!(fields["operation"], "unknown");
        assert_eq!(fields["stage"], "unknown");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "terminal_failure");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "terminal failure details unavailable");
    }

    #[test]
    fn terminal_reporter_emits_ten_independent_fields_and_fixed_message() {
        assert_generic_terminal_fields(&capture_terminal_event(&anyhow::anyhow!("secret")));
    }

    #[test]
    fn panic_hook_subprocess_child() {
        if std::env::var(PANIC_CHILD_ENV).as_deref() != Ok(PANIC_CHILD_VALUE) {
            return;
        }

        initialize_process_diagnostics();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("child runtime should build")
            .block_on(async {
                let panicked = tokio::spawn(async { panic!("{PANIC_SENTINEL}") });
                let error = supervised_join_error(panicked.await.unwrap_err());
                report_terminal_failure(&error);
            });
    }

    #[test]
    fn panic_hook_subprocess_suppresses_payload_and_reports_once() {
        let output = Command::new(std::env::current_exe().expect("test executable should exist"))
            .args([
                "tests::panic_hook_subprocess_child",
                "--exact",
                "--nocapture",
            ])
            .env(PANIC_CHILD_ENV, PANIC_CHILD_VALUE)
            .output()
            .expect("panic-hook child should run");

        for stream in [&output.stdout, &output.stderr] {
            assert!(
                !stream
                    .windows(PANIC_SENTINEL.len())
                    .any(|bytes| bytes == PANIC_SENTINEL.as_bytes()),
                "panic payload leaked in raw subprocess output"
            );
        }
        assert!(output.status.success(), "child failed: {output:?}");
        let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
        let combined = format!("{stdout}\n{stderr}");
        assert_eq!(combined.matches("FluxTrade panic observed").count(), 1);
        assert_eq!(combined.matches(" WARN ").count(), 1);
        assert_eq!(combined.matches("FluxTrade terminal failure").count(), 1);
        assert!(!combined.contains("Error:"), "Result termination leaked");

        let warning = combined
            .lines()
            .find(|line| line.contains("FluxTrade panic observed"))
            .expect("panic warning should be present");
        assert!(warning.contains(" WARN "), "wrong severity: {warning}");
        for field in [
            "component=runtime",
            "task=unknown",
            "operation=panic",
            "stage=panic_hook",
            "template_id=unknown",
            "payload_len=unknown",
            "stable_error_code=panic_observed",
            "disposition=continue_unwind",
            "state_effect=unwinding",
            "safe_cause=panic payload suppressed",
            "source_file=main.rs",
        ] {
            assert!(warning.contains(field), "missing {field}: {warning}");
        }
        let numeric_field = |name: &str| {
            warning
                .split_whitespace()
                .find_map(|field| field.strip_prefix(name))
                .unwrap_or_else(|| panic!("missing {name}: {warning}"))
                .parse::<u32>()
                .unwrap_or_else(|_| panic!("non-numeric {name}: {warning}"))
        };
        assert!(numeric_field("source_line=") > 0);
        assert!(numeric_field("source_column=") > 0);

        let terminal = combined
            .lines()
            .find(|line| line.contains("FluxTrade terminal failure"))
            .expect("terminal event should be present");
        assert!(terminal.contains(" ERROR "), "wrong severity: {terminal}");
    }

    #[test]
    fn terminal_diagnostic_classification_matrix_has_exact_structs() {
        let generic = |task: &str, stage, stable_error_code, safe_cause| TerminalDiagnostic {
            component: "unknown",
            task: task.to_string(),
            operation: "unknown",
            stage,
            template_id: "unknown".to_string(),
            payload_len: "unknown".to_string(),
            stable_error_code,
            disposition: "fatal_service_exit",
            state_effect: "process_exit",
            safe_cause,
        };
        let direct_supervisor = |kind| {
            anyhow::Error::new(SupervisedFailure {
                task: "unknown".to_string(),
                kind,
                source: None,
            })
        };
        let mut cases = vec![
            (
                "generic",
                anyhow::anyhow!("generic-secret"),
                generic(
                    "unknown",
                    "unknown",
                    "terminal_failure",
                    "terminal failure details unavailable",
                ),
            ),
            (
                "TaskError",
                supervised_task_exit_error(
                    &TaskId::Connector("binance".to_string()),
                    Err(anyhow::anyhow!("task-secret")),
                ),
                generic(
                    "connector:binance",
                    "task_exit",
                    "supervised_task_failed",
                    "supervised task failed",
                ),
            ),
            (
                "UnexpectedExit",
                supervised_task_exit_error(&TaskId::EventLoop, Ok(())),
                generic(
                    "event-loop",
                    "task_exit",
                    "supervised_task_exited",
                    "supervised task exited unexpectedly",
                ),
            ),
        ];
        for (name, failure, code, cause) in [
            (
                "BinanceTaskError",
                crate::connector::binance::BinanceTaskFailure::task_error(
                    "trades",
                    anyhow::anyhow!("provider-secret"),
                ),
                "binance_stream_task_failed",
                "Binance stream task failed",
            ),
            (
                "BinanceUnexpectedExit",
                crate::connector::binance::BinanceTaskFailure::unexpected_exit("trades"),
                "binance_stream_task_exited",
                "Binance stream task exited unexpectedly",
            ),
            (
                "BinancePanicked",
                crate::connector::binance::BinanceTaskFailure::panicked("trades"),
                "binance_stream_task_panicked",
                "Binance stream task panicked",
            ),
            (
                "BinanceCancelled",
                crate::connector::binance::BinanceTaskFailure::cancelled("trades"),
                "binance_stream_task_cancelled",
                "Binance stream task was cancelled",
            ),
        ] {
            cases.push((
                name,
                supervised_task_exit_error(
                    &TaskId::Connector("binance".to_string()),
                    Err(failure.into()),
                ),
                TerminalDiagnostic {
                    component: "binance",
                    task: "connector:binance".to_string(),
                    operation: "stream_task",
                    stage: "trades",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: code,
                    disposition: "fatal_service_exit",
                    state_effect: "process_exit",
                    safe_cause: cause,
                },
            ));
        }
        for (name, failure, code, cause) in [
            (
                "BybitTaskError",
                crate::connector::bybit::BybitTaskFailure::task_error(
                    "trades",
                    anyhow::anyhow!("provider-secret"),
                ),
                "bybit_stream_task_failed",
                "Bybit stream task failed",
            ),
            (
                "BybitUnexpectedExit",
                crate::connector::bybit::BybitTaskFailure::unexpected_exit("trades"),
                "bybit_stream_task_exited",
                "Bybit stream task exited unexpectedly",
            ),
            (
                "BybitPanicked",
                crate::connector::bybit::BybitTaskFailure::panicked("trades"),
                "bybit_stream_task_panicked",
                "Bybit stream task panicked",
            ),
            (
                "BybitCancelled",
                crate::connector::bybit::BybitTaskFailure::cancelled("trades"),
                "bybit_stream_task_cancelled",
                "Bybit stream task was cancelled",
            ),
        ] {
            cases.push((
                name,
                supervised_task_exit_error(
                    &TaskId::Connector("bybit".to_string()),
                    Err(failure.into()),
                ),
                TerminalDiagnostic {
                    component: "bybit",
                    task: "connector:bybit".to_string(),
                    operation: "stream_task",
                    stage: "trades",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: code,
                    disposition: "fatal_service_exit",
                    state_effect: "process_exit",
                    safe_cause: cause,
                },
            ));
        }
        for (name, failure, code, cause) in [
            (
                "BackpackTaskError",
                crate::connector::backpack::BackpackTaskFailure::task_error(
                    "trades",
                    anyhow::anyhow!("provider-secret"),
                ),
                "backpack_stream_task_failed",
                "Backpack stream task failed",
            ),
            (
                "BackpackUnexpectedExit",
                crate::connector::backpack::BackpackTaskFailure::unexpected_exit("trades"),
                "backpack_stream_task_exited",
                "Backpack stream task exited unexpectedly",
            ),
            (
                "BackpackPanicked",
                crate::connector::backpack::BackpackTaskFailure::panicked("trades"),
                "backpack_stream_task_panicked",
                "Backpack stream task panicked",
            ),
            (
                "BackpackCancelled",
                crate::connector::backpack::BackpackTaskFailure::cancelled("trades"),
                "backpack_stream_task_cancelled",
                "Backpack stream task was cancelled",
            ),
        ] {
            cases.push((
                name,
                supervised_task_exit_error(
                    &TaskId::Connector("backpack".to_string()),
                    Err(failure.into()),
                ),
                TerminalDiagnostic {
                    component: "backpack",
                    task: "connector:backpack".to_string(),
                    operation: "stream_task",
                    stage: "trades",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: code,
                    disposition: "fatal_service_exit",
                    state_effect: "process_exit",
                    safe_cause: cause,
                },
            ));
        }
        for (name, kind, code, cause) in [
            (
                "CancelledJoin",
                SupervisedFailureKind::CancelledJoin,
                "supervised_task_cancelled",
                "supervised task was cancelled",
            ),
            (
                "PanickedJoin",
                SupervisedFailureKind::PanickedJoin,
                "supervised_task_panicked",
                "supervised task panicked",
            ),
            (
                "OtherJoin",
                SupervisedFailureKind::OtherJoin,
                "supervised_task_join_failed",
                "supervised task join failed",
            ),
        ] {
            cases.push((
                name,
                direct_supervisor(kind),
                generic("unknown", "task_join", code, cause),
            ));
        }

        #[cfg(feature = "rithmic")]
        {
            let mut failure = crate::connector::rithmic::PayloadFailure::new(
                crate::connector::rithmic::PayloadFailureKind::MarketDecode,
            );
            failure.attach_transport(Some(151), 777);
            let source = anyhow::Error::new(failure)
                .context("payload boundary")
                .context("connector boundary");
            cases.push((
                "RithmicPayload",
                supervised_task_exit_error(&TaskId::Connector("rithmic".to_string()), Err(source)),
                TerminalDiagnostic {
                    component: "rithmic",
                    task: "connector:rithmic".to_string(),
                    operation: "handle_payload",
                    stage: "market_decode",
                    template_id: "151".to_string(),
                    payload_len: "777".to_string(),
                    stable_error_code: "malformed_market_payload",
                    disposition: "fatal_service_exit",
                    state_effect: "none",
                    safe_cause: "market payload validation failed",
                },
            ));
            cases.push((
                "HandshakeReject",
                crate::connector::rithmic::handshake_rejection_with_contexts(),
                TerminalDiagnostic {
                    component: "rithmic",
                    task: "unknown".to_string(),
                    operation: "handshake",
                    stage: "handshake",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: "rithmic_handshake_rejected",
                    disposition: "fatal_service_exit",
                    state_effect: "session_failed",
                    safe_cause: "Rithmic handshake rejected",
                },
            ));
        }

        for (name, error, expected) in cases {
            assert_eq!(terminal_diagnostic(&error), expected, "{name}");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn binance_wrapper_propagates_then_reports_exactly_one_error_without_network_poll() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _key = EnvVarGuard::remove("BINANCE_API_KEY");
        let (trade_tx, _) = mpsc::channel(1);
        let (candle_tx, _) = mpsc::channel(1);
        let (user_tx, _) = mpsc::channel(1);
        let mut events = capture_error_events(|| {
            let future =
                crate::connector::binance::run(Vec::new(), trade_tx, candle_tx, user_tx, true);
            let source = future.now_or_never().unwrap().unwrap_err();
            let error =
                supervised_task_exit_error(&TaskId::Connector("binance".to_string()), Err(source));
            report_terminal_failure(&error);
        });

        assert_eq!(events.len(), 1);
        let fields = events.pop().unwrap();
        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "unknown");
        assert_eq!(fields["task"], "connector:binance");
        assert_eq!(fields["operation"], "unknown");
        assert_eq!(fields["stage"], "task_exit");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "supervised_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "supervised task failed");
    }

    #[test]
    fn binance_internal_task_failure_reports_exact_safe_terminal_fields() {
        let source = anyhow::anyhow!("provider failure sentinel");
        let failure = crate::connector::binance::BinanceTaskFailure::task_error("trades", source);
        let error = supervised_task_exit_error(
            &TaskId::Connector("binance".to_string()),
            Err(failure.into()),
        );
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "binance");
        assert_eq!(fields["task"], "connector:binance");
        assert_eq!(fields["operation"], "stream_task");
        assert_eq!(fields["stage"], "trades");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "binance_stream_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "Binance stream task failed");
        assert!(fields
            .values()
            .all(|value| !value.contains("provider failure sentinel")));
    }

    #[test]
    fn backpack_internal_task_failure_reports_exact_safe_terminal_fields() {
        let source = anyhow::anyhow!("provider failure sentinel");
        let failure =
            crate::connector::backpack::BackpackTaskFailure::task_error("candles", source);
        let error = supervised_task_exit_error(
            &TaskId::Connector("backpack".to_string()),
            Err(failure.into()),
        );
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "backpack");
        assert_eq!(fields["task"], "connector:backpack");
        assert_eq!(fields["operation"], "stream_task");
        assert_eq!(fields["stage"], "candles");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "backpack_stream_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "Backpack stream task failed");
        assert!(fields
            .values()
            .all(|value| !value.contains("provider failure sentinel")));
    }

    #[test]
    fn bybit_internal_task_failure_reports_exact_safe_terminal_fields() {
        let source = anyhow::anyhow!("provider failure sentinel");
        let failure = crate::connector::bybit::BybitTaskFailure::task_error("candles", source);
        let error = supervised_task_exit_error(
            &TaskId::Connector("bybit".to_string()),
            Err(failure.into()),
        );
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "bybit");
        assert_eq!(fields["task"], "connector:bybit");
        assert_eq!(fields["operation"], "stream_task");
        assert_eq!(fields["stage"], "candles");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "bybit_stream_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "Bybit stream task failed");
        assert!(fields
            .values()
            .all(|value| !value.contains("provider failure sentinel")));
    }

    #[tokio::test]
    async fn actual_join_errors_select_safe_terminal_classifications() {
        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        let cancelled = supervised_join_error(cancelled.await.unwrap_err());
        let cancelled_fields = capture_terminal_event(&cancelled);
        assert_eq!(
            cancelled_fields["stable_error_code"],
            "supervised_task_cancelled"
        );

        let panicked = tokio::spawn(async { panic!("panic-payload-sentinel") });
        let panicked = supervised_join_error(panicked.await.unwrap_err());
        let panicked_fields = capture_terminal_event(&panicked);
        assert_eq!(
            panicked_fields["stable_error_code"],
            "supervised_task_panicked"
        );
        assert!(panicked_fields
            .values()
            .all(|value| !value.contains("panic-payload-sentinel")));
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn typed_payload_metadata_reaches_supervised_terminal_event_through_contexts() {
        let mut failure = crate::connector::rithmic::PayloadFailure::new(
            crate::connector::rithmic::PayloadFailureKind::MarketDecode,
        );
        failure.attach_transport(Some(151), 777);
        let source = anyhow::Error::new(failure)
            .context("payload boundary")
            .context("connector boundary");
        let error =
            supervised_task_exit_error(&TaskId::Connector("rithmic".to_string()), Err(source));
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "rithmic");
        assert_eq!(fields["task"], "connector:rithmic");
        assert_eq!(fields["operation"], "handle_payload");
        assert_eq!(fields["stage"], "market_decode");
        assert_eq!(fields["template_id"], "151");
        assert_eq!(fields["payload_len"], "777");
        assert_eq!(fields["stable_error_code"], "malformed_market_payload");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "none");
        assert_eq!(fields["safe_cause"], "market payload validation failed");
    }

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
    fn emergency_mitigation_is_connector_owned() {
        let main_source = include_str!("main.rs");
        let watchdog_source = include_str!("watchdog.rs");
        let owner_source = include_str!("connector/emergency.rs");
        let main_product = main_source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;
        let watchdog_product = watchdog_source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;

        assert!(!main_product.contains("BackpackConnector"));
        assert!(!main_product.contains("fn resolve_emergency_mitigation"));
        assert!(!watchdog_product.contains("crate::connector::backpack"));
        assert!(!watchdog_product.contains("crate::connector::rithmic"));
        assert!(owner_source.contains("pub(crate) fn resolve"));
        assert!(owner_source.contains("pub(crate) async fn run"));
    }

    #[test]
    fn connector_terminal_diagnostics_are_connector_owned() {
        let main_source = include_str!("main.rs");
        let owner_source = include_str!("connector/terminal.rs");
        let main_product = main_source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;

        for provider_detail in [
            "BinanceTaskFailure",
            "BackpackTaskFailure",
            "BybitTaskFailure",
            "PayloadFailure",
            "is_handshake_rejection",
        ] {
            assert!(!main_product.contains(provider_detail));
            assert!(owner_source.contains(provider_detail));
        }
        assert!(owner_source.contains("pub(crate) fn classify"));
    }

    #[test]
    fn connector_live_runtime_composition_is_connector_owned() {
        let main_source = include_str!("main.rs");
        let owner_source = include_str!("connector/live_runtime.rs");
        let main_product = main_source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .unwrap()
            .0;

        for (main_detail, owner_detail) in [
            ("connector::binance::run", "super::binance::run"),
            ("connector::bybit::run", "super::bybit::run"),
            ("connector::backpack::run", "super::backpack::run"),
            (
                "preflight_user_stream_credentials",
                "preflight_user_stream_credentials",
            ),
            ("resolve_market_data_symbols", "resolve_market_data_symbols"),
            ("resolve_live_options", "resolve_live_options"),
        ] {
            assert!(!main_product.contains(main_detail));
            assert!(owner_source.contains(owner_detail));
        }
        assert!(owner_source.contains("pub(crate) struct LiveRuntime"));
        assert!(owner_source.contains("pub(crate) fn prepare"));
        assert!(owner_source.contains("pub(crate) fn spawn"));
    }

    #[test]
    fn every_supervised_task_exit_is_fatal() {
        for task_id in [
            TaskId::Watchdog,
            TaskId::Publisher,
            TaskId::EventLoop,
            TaskId::Connector("binance".to_string()),
            TaskId::Connector("bybit".to_string()),
            TaskId::Connector("backpack".to_string()),
            TaskId::Connector("rithmic".to_string()),
        ] {
            let clean = supervised_task_exit_error(&task_id, Ok(()));
            assert_eq!(
                terminal_diagnostic(&clean).stable_error_code,
                "supervised_task_exited"
            );
            let failed = supervised_task_exit_error(&task_id, Err(anyhow::anyhow!("secret")));
            assert_eq!(
                terminal_diagnostic(&failed).stable_error_code,
                "supervised_task_failed"
            );
        }
        for (cancelled, panicked, expected) in [
            (true, false, "supervised_task_cancelled"),
            (false, true, "supervised_task_panicked"),
            (false, false, "supervised_task_join_failed"),
        ] {
            assert_eq!(
                classify_join_failure(cancelled, panicked).diagnostic().1,
                expected
            );
        }
    }

    #[test]
    fn supervised_task_failure_preserves_the_complete_error_chain() {
        let source = anyhow::anyhow!("unsupported Rithmic market-data template 151")
            .context("Rithmic payload handler failed");
        let error =
            supervised_task_exit_error(&TaskId::Connector("rithmic".to_string()), Err(source));

        assert_eq!(
            error.chain().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "supervised task failed",
                "Rithmic payload handler failed",
                "unsupported Rithmic market-data template 151",
            ]
        );
        assert_eq!(terminal_diagnostic(&error).task, "connector:rithmic");
    }

    #[test]
    fn optional_environment_values_are_trimmed() {
        assert_eq!(normalized_optional_value(None), None);
        assert_eq!(normalized_optional_value(Some(String::new())), None);
        assert_eq!(normalized_optional_value(Some("  ".to_string())), None);
        assert_eq!(
            normalized_optional_value(Some(" key ".to_string())),
            Some("key".to_string())
        );
    }

    #[test]
    fn generic_main_contains_no_venue_credential_schema_or_pair_validator() {
        let production = include_str!("main.rs")
            .split_once("#[cfg(test)]\nmod tests")
            .unwrap()
            .0;
        for forbidden in [
            "BINANCE_API_KEY",
            "EXCHANGE_API_KEY",
            "EXCHANGE_SECRET",
            "optional_credentials_present",
        ] {
            assert!(!production.contains(forbidden), "{forbidden}");
        }
    }

    #[tokio::test]
    async fn test_event_loop_exits_on_channel_close() {
        let (_trade_tx, trade_rx) = mpsc::channel(10);
        let (_candle_tx, candle_rx) = mpsc::channel(10);
        let (_user_tx, user_rx) = mpsc::channel(10);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, _pub_rx) = create_publish_channel(10);

        // Drop all senders to close the channels
        drop(_trade_tx);
        drop(_candle_tx);
        drop(_user_tx);
        drop(aggregation_reset_tx);

        // Event loop should exit gracefully when all channels are closed
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            run_event_loop(
                trade_rx,
                candle_rx,
                user_rx,
                aggregation_reset_rx,
                pub_sender,
            ),
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
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(1);
        drop(trade_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_reset_rx,
            pub_sender,
        ));
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

    #[tokio::test]
    async fn event_loop_accepts_closed_5m_source_candles() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(4);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(8);
        drop(trade_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_reset_rx,
            pub_sender,
        ));
        for index in 0..4 {
            candle_tx
                .send(model::Candlestick {
                    product_id: "RITHMIC:NQ-202609".to_string(),
                    timeframe: "5m".to_string(),
                    timestamp: 1_800_000_000_000 + index * 5 * 60 * 1000,
                    open: rust_decimal_macros::dec!(100),
                    high: rust_decimal_macros::dec!(101),
                    low: rust_decimal_macros::dec!(99),
                    close: rust_decimal_macros::dec!(100),
                    volume: rust_decimal_macros::dec!(1),
                })
                .await
                .unwrap();
        }
        drop(candle_tx);

        let mut published = Vec::new();
        while let Some(message) = pub_rx.recv().await {
            if let crate::publisher::PublishMessage::Candle(candle) = message {
                published.push(candle);
            }
        }
        assert!(event_loop.await.unwrap().is_ok());
        assert_eq!(
            published
                .iter()
                .filter(|candle| candle.timeframe == "5m")
                .count(),
            4
        );
        assert_eq!(
            published
                .iter()
                .filter(|candle| candle.timeframe == "15m")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn event_loop_applies_rithmic_reset_before_post_reconnect_candles() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_source_tx, aggregation_source_rx) = mpsc::channel(8);
        let (pub_sender, mut pub_rx) = create_publish_channel(16);
        drop(trade_tx);
        drop(candle_tx);
        drop(user_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_source_rx,
            pub_sender,
        ));
        let base_ts = 1_800_000_000_000;
        let candle = |minute: i64| model::Candlestick {
            product_id: "RITHMIC:NQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: base_ts + minute * 60_000,
            open: rust_decimal_macros::dec!(100),
            high: rust_decimal_macros::dec!(101),
            low: rust_decimal_macros::dec!(99),
            close: rust_decimal_macros::dec!(100),
            volume: rust_decimal_macros::dec!(1),
        };
        aggregation_source_tx
            .send(AggregationSourceEvent::Candle(candle(2)))
            .await
            .unwrap();
        aggregation_source_tx
            .send(AggregationSourceEvent::ResetProduct(
                "RITHMIC:NQ-202609".to_string(),
            ))
            .await
            .unwrap();
        for minute in 5..=10 {
            aggregation_source_tx
                .send(AggregationSourceEvent::Candle(candle(minute)))
                .await
                .unwrap();
        }
        drop(aggregation_source_tx);

        let mut derived = Vec::new();
        while let Some(message) = pub_rx.recv().await {
            if let crate::publisher::PublishMessage::Candle(candle) = message {
                if candle.timeframe != "1m" {
                    derived.push(candle);
                }
            }
        }
        assert!(event_loop.await.unwrap().is_ok());
        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].timeframe, "5m");
        assert_eq!(derived[0].timestamp, base_ts + 5 * 60_000);
        assert_eq!(derived[0].volume, rust_decimal_macros::dec!(5));
    }

    #[tokio::test]
    async fn event_loop_preserves_shorter_source_that_cannot_form_5m_exactly() {
        let (trade_tx, trade_rx) = mpsc::channel(1);
        let (candle_tx, candle_rx) = mpsc::channel(1);
        let (user_tx, user_rx) = mpsc::channel(1);
        let (aggregation_reset_tx, aggregation_reset_rx) = mpsc::channel(1);
        let (pub_sender, mut pub_rx) = create_publish_channel(1);
        drop(trade_tx);
        drop(user_tx);
        drop(aggregation_reset_tx);

        let event_loop = tokio::spawn(run_event_loop(
            trade_rx,
            candle_rx,
            user_rx,
            aggregation_reset_rx,
            pub_sender,
        ));
        candle_tx
            .send(model::Candlestick {
                product_id: "RITHMIC:NQ-202609".to_string(),
                timeframe: "2m".to_string(),
                timestamp: 1_800_000_000_000,
                open: rust_decimal_macros::dec!(100),
                high: rust_decimal_macros::dec!(101),
                low: rust_decimal_macros::dec!(99),
                close: rust_decimal_macros::dec!(100),
                volume: rust_decimal_macros::dec!(1),
            })
            .await
            .unwrap();
        drop(candle_tx);

        let published = pub_rx.recv().await.expect("source candle should publish");
        assert!(matches!(
            published,
            crate::publisher::PublishMessage::Candle(candle)
                if candle.timeframe == "2m"
        ));
        assert!(event_loop.await.unwrap().is_ok());
    }
}
