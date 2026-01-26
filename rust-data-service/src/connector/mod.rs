use crate::model::{Candlestick, OrderBook, Trade};
use anyhow::Result;
pub mod backpack;
pub mod binance;
pub mod bybit;
pub mod ws;

use async_trait::async_trait;
use tokio::sync::mpsc;

#[async_trait]
#[allow(dead_code)]
pub trait ExchangeConnector {
    async fn connect(&mut self) -> Result<()>;

    // Subscribes to channels and streams data via the provided Sender
    async fn subscribe_trades(&mut self, symbols: &[String], tx: mpsc::Sender<Trade>)
        -> Result<()>;
    async fn subscribe_orderbook(
        &mut self,
        symbols: &[String],
        tx: mpsc::Sender<OrderBook>,
    ) -> Result<()>;
    async fn subscribe_candles(
        &mut self,
        symbols: &[String],
        timeframe: &str,
        tx: mpsc::Sender<Candlestick>,
    ) -> Result<()>;

    // REST API for fetching recent history (catch-up)
    async fn fetch_recent_candles(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: u32,
    ) -> Result<Vec<Candlestick>>;
}
