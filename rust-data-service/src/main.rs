mod aggregator;
mod connector;
mod environment;
mod historical;
mod live_event_pipeline;
mod model;
mod publisher;
mod runtime_supervisor;
mod watchdog;

use crate::publisher::{create_publish_channel, RedisPublisher, DEFAULT_CHANNEL_CAPACITY};
use crate::runtime_supervisor::{
    initialize_process_diagnostics, report_terminal_failure, supervise, TaskId,
};

use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use std::process::ExitCode;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::info;

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
        let result = live_event_pipeline::run_event_loop(
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

    supervise(join_set).await?;

    // Graceful cleanup
    info!("Closing all connections...");
    info!("FluxTrade Data Service stopped.");

    Ok(())
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
mod tests {}
