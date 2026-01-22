mod connector;
mod model;

use crate::connector::binance::BinanceConnector;
use crate::connector::ExchangeConnector;
use tokio::sync::mpsc;
use tracing::{info, error, Level};
use dotenvy::dotenv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    
    // Explicitly install CryptoProvider for rustls 0.23+
    rustls::crypto::ring::default_provider().install_default()
        .expect("Failed to install crypto provider");

    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();
    
    info!("Starting FluxTrade Data Service Test Mode...");

    let api_key = std::env::var("BINANCE_API_KEY").unwrap_or_default();
    if api_key.is_empty() {
        error!("BINANCE_API_KEY not found in .env");
    } else {
        info!("BINANCE_API_KEY loaded successfully.");
    }

    let mut connector = BinanceConnector::new();
    
    let (trade_tx, mut trade_rx) = mpsc::channel(100);
    let (candle_tx, mut candle_rx) = mpsc::channel(100);

    info!("Subscribing to BTCUSDT trades and 1m candles...");
    connector.subscribe_trades(&["BTCUSDT".to_string()], trade_tx).await?;
    connector.subscribe_candles(&["BTCUSDT".to_string()], "1m", candle_tx).await?;

    let mut count = 0;
    let mut timeout = tokio::time::interval(std::time::Duration::from_secs(30));
    timeout.tick().await; // first tick is immediate

    loop {
        tokio::select! {
            msg = trade_rx.recv() => {
                if let Some(trade) = msg {
                    info!("Received Trade: {} {} @ {}", trade.product_id, trade.quantity, trade.price);
                    count += 1;
                }
            }
            msg = candle_rx.recv() => {
                if let Some(candle) = msg {
                    info!("Received Candle: {} Close: {}", candle.product_id, candle.close);
                    count += 1;
                }
            }
            _ = timeout.tick() => {
                info!("Test timeout reached.");
                break;
            }
        }
        if count >= 10 {
            info!("Received 10 messages, test successful!");
            break;
        }
    }

    Ok(())
}
