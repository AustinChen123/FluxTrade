# Rust Data Service API Reference

This document details the internal data structures and traits used in the `rust-data-service`. While users rarely interact with this directly, it is useful for contributors adding new exchanges.

## Data Models (`src::model`)

All models derive `Debug, Clone, Serialize, Deserialize` and use `rust_decimal::Decimal` for precision.

### `Candlestick`
Represents an aggregated OHLCV bar.

```rust
pub struct Candlestick {
    pub product_id: String, // "EXCHANGE:SYMBOL-PERP"
    pub timeframe: String,  // "1m"
    pub timestamp: i64,     // ms
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}
```

### `UserStreamEvent` (Enum)
Events received from authenticated user data streams.

```rust
pub enum UserStreamEvent {
    Account(AccountUpdate),
    Position(PositionUpdate),
    OrderUpdate(OrderUpdate), // Reserved
}
```

### `AccountUpdate`
Balance changes.

```rust
pub struct AccountUpdate {
    pub exchange: String,
    pub asset: String,      // "USDT"
    pub balance: Decimal,   // Wallet Balance
    pub timestamp: i64,
}
```

---

## Connectors (`src::connector`)

### `ExchangeConnector` (Trait)
The interface that every exchange adapter must implement.

```rust
#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    // 1. Initial Connection logic
    async fn connect(&mut self) -> Result<()>;

    // 2. Public Market Data
    async fn subscribe_trades(&mut self, symbols: &[String], tx: mpsc::Sender<Trade>) -> Result<()>;
    async fn subscribe_candles(&mut self, symbols: &[String], timeframe: &str, tx: mpsc::Sender<Candlestick>) -> Result<()>;
    
    // 3. Private User Data (Authenticated)
    async fn subscribe_user_stream(&self, tx: mpsc::Sender<UserStreamEvent>) -> Result<()>;

    // 4. REST Backfill
    async fn fetch_recent_candles(&self, symbol: &str, timeframe: &str, limit: u32) -> Result<Vec<Candlestick>>;
}
```
