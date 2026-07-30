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

use anyhow::Context;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
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
        Ok(()) => anyhow::anyhow!("Critical task '{}' exited unexpectedly", task_id),
        Err(error) => anyhow::anyhow!("Critical task '{}' failed: {}", task_id, error),
    }
}

#[cfg(feature = "rithmic")]
#[derive(Clone)]
struct RithmicLiveArgs {
    profile: String,
    account_id: String,
    product_id: String,
    exchange: String,
    symbol: String,
}

#[cfg(feature = "rithmic")]
fn resolve_rithmic_live_args(
    enabled_exchanges: &str,
    profile: Option<String>,
    account_id: Option<String>,
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
            profile.is_none()
                && account_id.is_none()
                && product_id.is_none()
                && exchange.is_none()
                && symbol.is_none(),
            "Rithmic options require --exchange rithmic"
        );
        return Ok(None);
    }

    Ok(Some(RithmicLiveArgs {
        profile: profile.context("--rithmic-profile is required")?,
        account_id: account_id.context("--rithmic-account-id is required")?,
        product_id: product_id.context("--rithmic-product-id is required")?,
        exchange: exchange.context("--rithmic-exchange is required")?,
        symbol: symbol.context("--rithmic-symbol is required")?,
    }))
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

    let enabled_exchanges_raw = exchange_opt
        .or_else(|| non_empty_env("EXCHANGE_ENABLED"))
        .unwrap_or_else(|| "binance,bybit,backpack".into());
    let enabled_exchanges = validate_enabled_exchanges(&enabled_exchanges_raw)?;
    #[cfg(feature = "rithmic")]
    let enabled_exchanges_csv = enabled_exchanges.join(",");
    let (binance_user_stream_enabled, backpack_user_stream_enabled) =
        preflight_user_stream_credentials(&enabled_exchanges, |name| std::env::var(name).ok())?;

    #[cfg(feature = "rithmic")]
    let rithmic_args = resolve_rithmic_live_args(
        &enabled_exchanges_csv,
        rithmic_profile.or_else(|| non_empty_env("RITHMIC_PROFILE")),
        rithmic_account_id.or_else(|| non_empty_env("RITHMIC_ACCOUNT_ID")),
        rithmic_product_id.or_else(|| non_empty_env("RITHMIC_PRODUCT_ID")),
        rithmic_exchange.or_else(|| non_empty_env("RITHMIC_EXCHANGE")),
        rithmic_symbol.or_else(|| non_empty_env("RITHMIC_SYMBOL")),
    )?;
    #[cfg(feature = "rithmic")]
    let mut rithmic_config = rithmic_args
        .as_ref()
        .map(|args| {
            crate::connector::rithmic::live::configure(
                &args.profile,
                args.product_id.clone(),
                args.exchange.clone(),
                args.symbol.clone(),
            )
        })
        .transpose()?;

    let symbols_str = symbol_opt
        .or_else(|| non_empty_env("MARKET_DATA_SYMBOLS"))
        .unwrap_or_else(|| "BTCUSDT,SOLUSDC".into());
    let symbols = parse_unique_csv("MARKET_DATA_SYMBOLS", &symbols_str, str::to_uppercase)?;

    // --- Supervised task set (Task 1: task supervision) ---
    let mut join_set: JoinSet<(TaskId, anyhow::Result<()>)> = JoinSet::new();
    // Spawn Watchdog task
    let watchdog_redis_url = redis_url.clone();
    let watchdog_environment = runtime_environment;
    let execution_venue = non_empty_env("EXCHANGE_ID");
    #[cfg(feature = "rithmic")]
    let rithmic_watchdog_identity = rithmic_args
        .as_ref()
        .map(|args| (args.profile.as_str(), args.account_id.as_str()));
    #[cfg(not(feature = "rithmic"))]
    let rithmic_watchdog_identity = None;
    let watchdog_mitigation = resolve_emergency_mitigation(
        &watchdog_environment,
        execution_venue.as_deref(),
        rithmic_watchdog_identity,
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
    for exchange_name in &enabled_exchanges {
        let trade_tx = trade_tx.clone();
        let candle_tx = candle_tx.clone();
        let user_tx = user_tx.clone();
        let symbols = symbols.clone();

        match exchange_name.as_str() {
            "binance" => {
                join_set.spawn(async move {
                    let result = run_binance_connector(
                        symbols,
                        trade_tx,
                        candle_tx,
                        user_tx,
                        binance_user_stream_enabled,
                    )
                    .await;
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
                    let result = run_backpack_connector(
                        symbols,
                        trade_tx,
                        candle_tx,
                        user_tx,
                        backpack_user_stream_enabled,
                    )
                    .await;
                    (TaskId::Connector("backpack".to_string()), result)
                });
                info!("Supervised task spawned: connector:backpack");
            }
            #[cfg(feature = "rithmic")]
            "rithmic" => {
                let config = rithmic_config.take().expect("Rithmic configuration loaded");
                let aggregation_source_tx = aggregation_source_tx.clone();
                join_set.spawn(async move {
                    let result =
                        crate::connector::rithmic::live::run(config, aggregation_source_tx).await;
                    (TaskId::Connector("rithmic".to_string()), result)
                });
                info!("Supervised task spawned: connector:rithmic");
            }
            _ => unreachable!("validated exchange: {exchange_name}"),
        }
    }

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
                    error!("{:#}", error);
                    join_set.shutdown().await;
                    return Err(error);
                }
                Some(Err(join_err)) => {
                    error!("Supervised task join failed: {:?}", join_err);
                    join_set.shutdown().await;
                    return Err(anyhow::anyhow!("Supervised task join failed: {:?}", join_err));
                }
            }
        }
    }

    // Graceful cleanup
    info!("Closing all connections...");
    info!("FluxTrade Data Service stopped.");

    Ok(())
}

fn resolve_emergency_mitigation(
    environment: &crate::environment::RuntimeEnvironment,
    execution_venue: Option<&str>,
    rithmic_identity: Option<(&str, &str)>,
) -> anyhow::Result<crate::watchdog::EmergencyMitigation> {
    #[cfg(not(feature = "rithmic"))]
    let _ = rithmic_identity;
    if !environment.allows_external_kill() {
        return Ok(crate::watchdog::EmergencyMitigation::LockdownOnly);
    }
    let venue = execution_venue
        .context("EXCHANGE_ID must be set explicitly in live")?
        .trim()
        .to_ascii_lowercase();
    match venue.as_str() {
        "backpack" => Ok(crate::watchdog::EmergencyMitigation::Backpack(
            BackpackConnector::new(),
        )),
        #[cfg(feature = "rithmic")]
        "rithmic" => {
            let (profile, account_id) =
                rithmic_identity.context("Rithmic watchdog requires live Rithmic configuration")?;
            Ok(crate::watchdog::EmergencyMitigation::Rithmic {
                profile: profile.to_string(),
                account_id: account_id.to_string(),
            })
        }
        _ => anyhow::bail!("unsupported emergency execution venue: {venue}"),
    }
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

fn parse_unique_csv(
    name: &str,
    value: &str,
    canonicalize: fn(&str) -> String,
) -> anyhow::Result<Vec<String>> {
    let values: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(canonicalize)
        .collect();
    if values.is_empty() {
        anyhow::bail!("{name} must contain at least one value");
    }
    let unique: std::collections::HashSet<&str> = values.iter().map(String::as_str).collect();
    if unique.len() != values.len() {
        anyhow::bail!("{name} must not contain duplicate values");
    }
    Ok(values)
}

fn validate_enabled_exchanges(value: &str) -> anyhow::Result<Vec<String>> {
    let exchanges = parse_unique_csv("EXCHANGE_ENABLED", value, str::to_lowercase)?;
    for exchange in &exchanges {
        match exchange.as_str() {
            "binance" | "bybit" | "backpack" => {}
            #[cfg(feature = "rithmic")]
            "rithmic" => {}
            _ => anyhow::bail!("unsupported or unavailable exchange: {exchange}"),
        }
    }
    Ok(exchanges)
}

fn optional_credentials_present(credentials: &[(&str, Option<String>)]) -> anyhow::Result<bool> {
    let mut present = 0;
    for (name, value) in credentials {
        let Some(value) = value else {
            continue;
        };
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        anyhow::ensure!(
            trimmed == value,
            "{name} must not contain surrounding whitespace"
        );
        present += 1;
    }
    if present == 0 {
        return Ok(false);
    }
    if present != credentials.len() {
        let names = credentials
            .iter()
            .map(|(name, _)| *name)
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!("optional credentials must be provided together: {names}");
    }
    Ok(true)
}

fn preflight_user_stream_credentials(
    enabled_exchanges: &[String],
    lookup: impl Fn(&str) -> Option<String>,
) -> anyhow::Result<(bool, bool)> {
    let binance_enabled = enabled_exchanges.iter().any(|value| value == "binance");
    let backpack_enabled = enabled_exchanges.iter().any(|value| value == "backpack");
    let binance_user_stream = binance_enabled
        && optional_credentials_present(&[("BINANCE_API_KEY", lookup("BINANCE_API_KEY"))])?;
    let backpack_user_stream = backpack_enabled
        && optional_credentials_present(&[
            ("EXCHANGE_API_KEY", lookup("EXCHANGE_API_KEY")),
            ("EXCHANGE_SECRET", lookup("EXCHANGE_SECRET")),
        ])?;
    Ok((binance_user_stream, backpack_user_stream))
}

/// Run the Binance connector: subscribes to trades, candles, and user stream.
async fn run_binance_connector(
    symbols: Vec<String>,
    trade_tx: mpsc::Sender<model::Trade>,
    candle_tx: mpsc::Sender<model::Candlestick>,
    user_tx: mpsc::Sender<UserStreamEvent>,
    user_stream_enabled: bool,
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

    // Credential completeness is validated before any connector task is spawned.
    if user_stream_enabled {
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
    user_stream_enabled: bool,
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

    // Credential completeness is validated before any connector task is spawned.
    if user_stream_enabled {
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
    use crate::environment::RuntimeEnvironment;
    use std::time::Duration;

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
    fn non_live_watchdog_never_builds_external_mitigation() {
        let mitigation = resolve_emergency_mitigation(
            &RuntimeEnvironment::new("test").unwrap(),
            Some("backpack"),
            None,
        )
        .unwrap();

        assert!(matches!(
            mitigation,
            crate::watchdog::EmergencyMitigation::LockdownOnly
        ));
    }

    #[test]
    fn live_watchdog_requires_supported_explicit_execution_venue() {
        let environment = RuntimeEnvironment::new("live").unwrap();

        assert!(resolve_emergency_mitigation(&environment, None, None)
            .err()
            .unwrap()
            .to_string()
            .contains("EXCHANGE_ID"));
        assert!(
            resolve_emergency_mitigation(&environment, Some("binance"), None)
                .err()
                .unwrap()
                .to_string()
                .contains("unsupported emergency execution venue")
        );
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn live_rithmic_watchdog_preserves_exact_profile_and_account() {
        let mitigation = resolve_emergency_mitigation(
            &RuntimeEnvironment::new("live").unwrap(),
            Some("RITHMIC"),
            Some(("profile-a", "TEST_ACCOUNT_001")),
        )
        .unwrap();

        match mitigation {
            crate::watchdog::EmergencyMitigation::Rithmic {
                profile,
                account_id,
            } => {
                assert_eq!(profile, "profile-a");
                assert_eq!(account_id, "TEST_ACCOUNT_001");
            }
            _ => panic!("expected Rithmic emergency mitigation"),
        }
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
            assert!(supervised_task_exit_error(&task_id, Ok(()))
                .to_string()
                .contains("exited unexpectedly"));
            assert!(
                supervised_task_exit_error(&task_id, Err(anyhow::anyhow!("test failure")))
                    .to_string()
                    .contains("test failure")
            );
        }
    }

    #[test]
    fn production_runtime_csv_values_fail_closed() {
        assert!(parse_unique_csv("MARKET_DATA_SYMBOLS", " , ", str::to_uppercase).is_err());
        assert_eq!(
            parse_unique_csv("MARKET_DATA_SYMBOLS", " btcusdt, mnqu6 ", str::to_uppercase,)
                .unwrap(),
            vec!["BTCUSDT", "MNQU6"]
        );
        assert!(parse_unique_csv("MARKET_DATA_SYMBOLS", "mnqu6,MNQU6", str::to_uppercase).is_err());
        assert!(validate_enabled_exchanges("unknown").is_err());
        assert_eq!(
            validate_enabled_exchanges("BINANCE,bybit").unwrap(),
            vec!["binance", "bybit"]
        );
        assert!(validate_enabled_exchanges("binance,BINANCE").is_err());
    }

    #[test]
    fn optional_credentials_require_all_clean_non_empty_values() {
        assert_eq!(normalized_optional_value(None), None);
        assert_eq!(normalized_optional_value(Some(String::new())), None);
        assert_eq!(normalized_optional_value(Some("  ".to_string())), None);
        assert_eq!(
            normalized_optional_value(Some(" key ".to_string())),
            Some("key".to_string())
        );
        assert!(!optional_credentials_present(&[("key", None), ("secret", None),]).unwrap());
        assert!(optional_credentials_present(&[
            ("key", Some("key".to_string())),
            ("secret", Some("secret".to_string())),
        ])
        .unwrap());
        assert!(optional_credentials_present(&[
            ("key", Some("key".to_string())),
            ("secret", None),
        ])
        .is_err());
        assert!(optional_credentials_present(&[
            ("key", Some(" key ".to_string())),
            ("secret", Some("secret".to_string())),
        ])
        .is_err());
        assert!(optional_credentials_present(&[
            ("key", Some(" ".to_string())),
            ("secret", Some("secret".to_string())),
        ])
        .is_err());
    }

    #[test]
    fn user_stream_credentials_are_preflighted_for_enabled_exchanges() {
        let enabled = vec!["binance".to_string(), "backpack".to_string()];
        let complete = std::collections::HashMap::from([
            ("BINANCE_API_KEY", "binance-key"),
            ("EXCHANGE_API_KEY", "backpack-key"),
            ("EXCHANGE_SECRET", "backpack-secret"),
        ]);
        assert_eq!(
            preflight_user_stream_credentials(&enabled, |name| {
                complete.get(name).map(|value| (*value).to_string())
            })
            .unwrap(),
            (true, true)
        );

        let partial = std::collections::HashMap::from([("EXCHANGE_API_KEY", "backpack-key")]);
        assert!(preflight_user_stream_credentials(&enabled, |name| {
            partial.get(name).map(|value| (*value).to_string())
        })
        .is_err());

        let public_only = preflight_user_stream_credentials(&enabled, |_| None).unwrap();
        assert_eq!(public_only, (false, false));

        let binance_only = vec!["binance".to_string()];
        assert_eq!(
            preflight_user_stream_credentials(&binance_only, |name| {
                partial.get(name).map(|value| (*value).to_string())
            })
            .unwrap(),
            (false, false)
        );
    }

    #[cfg(not(feature = "rithmic"))]
    #[test]
    fn rithmic_requires_a_rithmic_enabled_build() {
        assert!(validate_enabled_exchanges("rithmic").is_err());
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn rithmic_live_arguments_fail_closed_before_startup() {
        assert!(
            resolve_rithmic_live_args("binance", None, None, None, None, None)
                .unwrap()
                .is_none()
        );
        let args = resolve_rithmic_live_args(
            "rithmic",
            Some("lucid".to_string()),
            Some("ACCOUNT".to_string()),
            Some("RITHMIC:NQ-202609".to_string()),
            Some("CME".to_string()),
            Some("NQU6".to_string()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(args.account_id, "ACCOUNT");

        for args in [
            ("rithmic", None, None, None, None, None),
            ("rithmic,rithmic", None, None, None, None, None),
            (
                "binance",
                Some("lucid".to_string()),
                Some("ACCOUNT".to_string()),
                Some("RITHMIC:NQ-202609".to_string()),
                Some("CME".to_string()),
                Some("NQU6".to_string()),
            ),
        ] {
            assert!(
                resolve_rithmic_live_args(args.0, args.1, args.2, args.3, args.4, args.5).is_err()
            );
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
