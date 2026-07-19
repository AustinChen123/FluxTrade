use super::{
    config,
    history::{self, HistoryEvent, HistoryMinuteBar, HistoryPageDecoder},
    session::Plant,
    transport::{self, ConnectionEvent},
};
use crate::model::{validate_product_id, Candlestick};
use anyhow::{ensure, Context, Result};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, QueryBuilder};
use std::time::Duration;
use tracing::info;

const MINUTE_MS: i64 = 60_000;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const PAGE_TIMEOUT: Duration = Duration::from_secs(30);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const REQUEST_KEY: &str = "fluxtrade-history";

pub(crate) async fn run(
    profile: &str,
    product_id: &str,
    exchange: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<usize> {
    validate_product_id(product_id)?;
    ensure!(
        product_id.starts_with("RITHMIC:"),
        "Rithmic history product ID must use RITHMIC venue"
    );
    let (start_index, finish_index) = request_bounds(start_ms, end_ms)?;
    let runtime = config::load(profile, Plant::History)?;
    let bars = tokio::time::timeout(
        DOWNLOAD_TIMEOUT,
        download(
            runtime,
            product_id,
            exchange,
            symbol,
            start_index,
            finish_index,
        ),
    )
    .await
    .context("Rithmic history download timed out")??;
    persist_exact(&bars).await
}

async fn download(
    runtime: config::RuntimeConfig,
    product_id: &str,
    exchange: &str,
    symbol: &str,
    start_index: i32,
    finish_index: i32,
) -> Result<Vec<Candlestick>> {
    let mut connection = transport::connect(&runtime.url, runtime.login, RESPONSE_TIMEOUT).await?;
    let mut decoder = HistoryPageDecoder::new(REQUEST_KEY, exchange, symbol, finish_index)?;
    let mut next_start = Some(start_index);
    let mut bars = Vec::new();
    wait_for_heartbeat(&mut connection).await?;
    info!("Rithmic history heartbeat confirmed");

    while let Some(page_start) = next_start {
        info!(page_start, finish_index, "requesting Rithmic history page");
        connection
            .send_payload(history::minute_bar_replay_request(
                REQUEST_KEY,
                exchange,
                symbol,
                page_start,
                finish_index,
            )?)
            .await?;

        let (page_bars, following) = tokio::time::timeout(PAGE_TIMEOUT, async {
            let mut page_bars = Vec::new();
            loop {
                match connection.next_event().await? {
                    ConnectionEvent::HeartbeatConfirmed => {}
                    ConnectionEvent::Payload(payload) => match decoder.decode(&payload)? {
                        HistoryEvent::Bar(bar) => page_bars.push(to_candle(product_id, bar)?),
                        HistoryEvent::PageEnded { next_start } => {
                            return Ok::<_, anyhow::Error>((page_bars, next_start));
                        }
                    },
                }
            }
        })
        .await
        .context("Rithmic history page timed out")??;
        ensure!(
            following.is_none_or(|next| next > page_start),
            "Rithmic history pagination did not advance"
        );
        info!(
            page_start,
            received_bars = page_bars.len(),
            next_start = following,
            "Rithmic history page completed"
        );
        bars.extend(page_bars);
        next_start = following;
    }

    Ok(bars)
}

async fn wait_for_heartbeat(connection: &mut transport::RithmicConnection) -> Result<()> {
    let event = tokio::time::timeout(RESPONSE_TIMEOUT, connection.next_event())
        .await
        .context("Rithmic history heartbeat timed out")??;
    match event {
        ConnectionEvent::HeartbeatConfirmed => Ok(()),
        ConnectionEvent::Payload(_) => {
            anyhow::bail!("Rithmic history payload arrived before heartbeat confirmation")
        }
    }
}

fn request_bounds(start_ms: i64, end_ms: i64) -> Result<(i32, i32)> {
    ensure!(start_ms >= 0, "Rithmic history start must not be negative");
    ensure!(end_ms > start_ms, "Rithmic history end must follow start");
    ensure!(
        start_ms % MINUTE_MS == 0 && end_ms % MINUTE_MS == 0,
        "Rithmic history bounds must align to minute boundaries"
    );
    let start_index = start_ms / 1_000;
    let final_end = end_ms / 1_000;
    Ok((
        i32::try_from(start_index).context("Rithmic history start exceeds protocol range")?,
        i32::try_from(final_end).context("Rithmic history end exceeds protocol range")?,
    ))
}

fn to_candle(product_id: &str, bar: HistoryMinuteBar) -> Result<Candlestick> {
    let timestamp = bar
        .end_timestamp
        .checked_sub(MINUTE_MS)
        .context("Rithmic history bar ends before one minute")?;
    let candle = Candlestick {
        product_id: product_id.to_string(),
        timeframe: "1m".to_string(),
        timestamp,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
    };
    candle.validate()?;
    Ok(candle)
}

async fn persist_exact(candles: &[Candlestick]) -> Result<usize> {
    if candles.is_empty() {
        return Ok(0);
    }
    let pool = database_pool().await?;
    let product_id = &candles[0].product_id;
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM product WHERE id = $1)")
        .bind(product_id)
        .fetch_one(&pool)
        .await?;
    ensure!(
        exists,
        "Rithmic history product must exist before backfill: {product_id}"
    );

    let mut inserted = 0_u64;
    for chunk in candles.chunks(1_000) {
        let mut query = QueryBuilder::new(
            "INSERT INTO candlestick (product_id, timeframe, timestamp, open, high, low, close, volume) ",
        );
        query.push_values(chunk, |mut row, candle| {
            row.push_bind(&candle.product_id)
                .push_bind(&candle.timeframe)
                .push_bind(candle.timestamp)
                .push_bind(candle.open)
                .push_bind(candle.high)
                .push_bind(candle.low)
                .push_bind(candle.close)
                .push_bind(candle.volume);
        });
        query.push(" ON CONFLICT (product_id, timeframe, timestamp) DO NOTHING");
        inserted += query.build().execute(&pool).await?.rows_affected();
    }
    Ok(inserted as usize)
}

async fn database_pool() -> Result<Pool<Postgres>> {
    let url = format!(
        "postgres://{}:{}@{}:{}/{}",
        std::env::var("POSTGRES_USER").unwrap_or_else(|_| "fluxtrade".to_string()),
        std::env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| "fluxtrade".to_string()),
        std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "localhost".to_string()),
        std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string()),
        std::env::var("POSTGRES_DB").unwrap_or_else(|_| "fluxtrade".to_string()),
    );
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn request_range_matrix_is_explicit_and_minute_aligned() {
        assert_eq!(request_bounds(0, 120_000).unwrap(), (0, 120));
        for (start, end) in [(-60_000, 0), (0, 0), (1, 60_000), (0, 60_001)] {
            assert!(request_bounds(start, end).is_err());
        }
    }

    #[test]
    fn history_bar_end_maps_to_canonical_candle_start() {
        let candle = to_candle(
            "RITHMIC:NQ-202609",
            HistoryMinuteBar {
                exchange: "CME".to_string(),
                symbol: "NQU6".to_string(),
                end_timestamp: 120_000,
                open: dec!(100),
                high: dec!(102),
                low: dec!(99),
                close: dec!(101),
                volume: dec!(4),
            },
        )
        .unwrap();

        assert_eq!(candle.timestamp, 60_000);
        assert_eq!(candle.timeframe, "1m");
    }
}
