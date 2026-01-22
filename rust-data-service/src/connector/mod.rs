use crate::model::{Candlestick, OrderBook, Trade};
use anyhow::Result;
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
}
