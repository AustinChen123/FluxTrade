mod aggregator;
mod connector;
mod historical;
mod model;
mod publisher;
mod watchdog;

use crate::aggregator::CandleAggregator;
use crate::connector::backpack::BackpackConnector;
use crate::connector::binance::BinanceConnector;
use crate::connector::bybit::BybitConnector;
use crate::connector::ExchangeConnector;
use crate::publisher::RedisPublisher;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{error, info, warn, Level};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Runs the real-time data collection service
    Live,
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

    match cli.command.unwrap_or(Commands::Live) {
        Commands::Live => run_live_mode().await?,
        Commands::Backfill {
            exchange,
            symbol,
            start,
            end,
        } => run_backfill_mode(exchange, symbol, start, end).await?,
    }

    Ok(())
}

async fn run_backfill_mode(
    exchange: String,
    symbol: String,
    start: String,
    end: String,
) -> anyhow::Result<()> {
    info!(
        "Starting Backfill for {} on {} from {} to {}",
        symbol, exchange, start, end
    );
    crate::historical::run_backfill(exchange, symbol, start, end).await
}

async fn run_live_mode() -> anyhow::Result<()> {
    info!("🚀 FluxTrade Data Service Starting (Live Mode)...");

    let redis_url = format!(
        "redis://{}:{}",
        std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        std::env::var("REDIS_PORT").unwrap_or_else(|_| "6379".into())
    );

    let mut publisher = RedisPublisher::new(&redis_url)?;
    publisher.connect().await?;
    let publisher = Arc::new(Mutex::new(publisher));

    let (trade_tx, mut trade_rx) = mpsc::channel(1000);
    let (candle_tx, mut candle_rx) = mpsc::channel(1000);

    // Start Watchdog
    let watchdog_redis_url = redis_url.clone();
    tokio::spawn(async move {
        match crate::watchdog::Watchdog::new(&watchdog_redis_url) {
            Ok(wd) => wd.run().await,
            Err(e) => error!("Failed to initialize Watchdog: {}", e),
        }
    });

    // 1. Start Aggregator Task (Optional for now, but wired up)
    let mut aggregator = CandleAggregator::new();

    // 2. Start Data Collection Pipeline
    let enabled_exchanges =
        std::env::var("EXCHANGE_ENABLED").unwrap_or_else(|_| "binance,bybit,backpack".into());
    let symbols = vec!["BTCUSDT".to_string(), "SOLUSDC".to_string()]; // Default symbols

    for ex in enabled_exchanges.split(',') {
        let trade_tx = trade_tx.clone();
        let candle_tx = candle_tx.clone();
        let symbols = symbols.clone();

        match ex.trim().to_lowercase().as_str() {
            "binance" => {
                tokio::spawn(async move {
                    let mut conn = BinanceConnector::new();
                    info!("Starting Binance Connector...");
                    if let Err(e) = conn.subscribe_trades(&symbols, trade_tx).await {
                        error!("Binance trades error: {}", e);
                    }
                    if let Err(e) = conn.subscribe_candles(&symbols, "1m", candle_tx).await {
                        error!("Binance candles error: {}", e);
                    }
                });
            }
            "bybit" => {
                tokio::spawn(async move {
                    let mut conn = BybitConnector::new();
                    info!("Starting Bybit Connector...");
                    if let Err(e) = conn.subscribe_trades(&symbols, trade_tx).await {
                        error!("Bybit trades error: {}", e);
                    }
                    if let Err(e) = conn.subscribe_candles(&symbols, "1m", candle_tx).await {
                        error!("Bybit candles error: {}", e);
                    }
                });
            }
            "backpack" => {
                tokio::spawn(async move {
                    let mut conn = BackpackConnector::new();
                    info!("Starting Backpack Connector...");
                    // Backpack symbols often use underscore
                    let backpack_symbols = vec!["BTC_USDC".to_string(), "SOL_USDC".to_string()];
                    if let Err(e) = conn.subscribe_trades(&backpack_symbols, trade_tx).await {
                        error!("Backpack trades error: {}", e);
                    }
                    if let Err(e) = conn
                        .subscribe_candles(&backpack_symbols, "1m", candle_tx)
                        .await
                    {
                        error!("Backpack candles error: {}", e);
                    }
                });
            }
            _ => warn!("Unknown exchange in EXCHANGE_ENABLED: {}", ex),
        }
    }

    // 3. Main Loop: Forward to Redis and Aggregator
    loop {
        tokio::select! {
            msg = trade_rx.recv() => {
                if let Some(trade) = msg {
                    let mut pub_lock = publisher.lock().await;
                    if let Err(e) = pub_lock.publish_trade(&trade).await {
                        error!("Failed to publish trade: {}", e);
                    }
                }
            }
            msg = candle_rx.recv() => {
                if let Some(candle) = msg {
                    let mut pub_lock = publisher.lock().await;
                    // Publish 1m candle
                    if let Err(e) = pub_lock.publish_candle(&candle).await {
                        error!("Failed to publish candle: {}", e);
                    }

                    // Aggregate to 5m and 15m
                    if let Some(c5) = aggregator.add_candle(&candle, "5m") {
                        if let Err(e) = pub_lock.publish_candle(&c5).await {
                            error!("Failed to publish 5m candle: {}", e);
                        }
                    }
                    if let Some(c15) = aggregator.add_candle(&candle, "15m") {
                        if let Err(e) = pub_lock.publish_candle(&c15).await {
                            error!("Failed to publish 15m candle: {}", e);
                        }
                    }
                }
            }
        }
    }
}