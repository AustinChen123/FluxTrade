use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::model::Candlestick;

pub async fn run_backfill(
    exchange: String,
    symbol: String,
    start_date: String,
    end_date: String,
) -> Result<()> {
    // 1. Parse Dates
    let start = parse_date(&start_date)?;
    let end = parse_date(&end_date)?;
    let start_ts = start.timestamp_millis();
    let end_ts = end.timestamp_millis();

    if start_ts >= end_ts {
        return Err(anyhow!("Start date must be before end date"));
    }

    // 2. Connect to DB
    let database_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        std::env::var("POSTGRES_USER").unwrap_or("fluxtrade".to_string()),
        std::env::var("POSTGRES_PASSWORD").unwrap_or("fluxtrade".to_string()),
        std::env::var("POSTGRES_HOST").unwrap_or("localhost".to_string()),
        std::env::var("POSTGRES_PORT").unwrap_or("5432".to_string()),
        std::env::var("POSTGRES_DB").unwrap_or("fluxtrade".to_string())
    );

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;

    info!("Connected to Database");

    // 3. Setup Downloader
    let client = Client::new();
    let downloader = match exchange.to_lowercase().as_str() {
        "binance" => Arc::new(BinanceDownloader::new(client)),
        _ => return Err(anyhow!("Unsupported exchange: {}", exchange)),
    };

    // 4. Generate Chunks (1000 candles per chunk for safety)
    // 1m candles = 60000 ms
    let interval_ms = 60000;
    let limit = 1000;
    let chunk_duration = interval_ms * limit;
    
    let mut chunks = Vec::new();
    let mut current_start = start_ts;
    while current_start < end_ts {
        let mut current_end = current_start + chunk_duration - 1; // inclusive end
        if current_end > end_ts {
            current_end = end_ts;
        }
        chunks.push((current_start, current_end));
        current_start = current_end + 1;
    }

    info!("Generated {} chunks to download", chunks.len());

    // 5. Concurrent Download & Insert
    // Use a semaphore to limit concurrent requests to avoid rate limits
    let semaphore = Arc::new(Semaphore::new(5)); 
    
    // We will stream the tasks
    let product_id = format!("{}:{}-PERP", exchange.to_uppercase(), symbol.to_uppercase());

    let results = futures_util::stream::iter(chunks)
        .map(|(c_start, c_end)| {
            let downloader = downloader.clone();
            let pool = pool.clone();
            let product_id = product_id.clone();
            let semaphore = semaphore.clone();
            
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                process_chunk(&downloader, &pool, &product_id, c_start, c_end).await
            })
        })
        .buffer_unordered(5); // Parallel execution

    results.for_each(|res| async {
        match res {
            Ok(Ok(count)) => info!("Chunk finished. Inserted {} candles", count),
            Ok(Err(e)) => error!("Chunk failed: {}", e),
            Err(e) => error!("Task join error: {}", e),
        }
    }).await;

    info!("Backfill completed!");
    Ok(())
}

async fn process_chunk(
    downloader: &BinanceDownloader,
    pool: &Pool<Postgres>,
    product_id: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<usize> {
    let candles = downloader
        .fetch_candles(product_id, start_ts, end_ts)
        .await?;
    
    if candles.is_empty() {
        return Ok(0);
    }

    // Bulk Insert
    // Using simple unnest approach or batched insert
    let mut query_builder = sqlx::QueryBuilder::new(
        "INSERT INTO candlestick (product_id, timeframe, timestamp, open, high, low, close, volume) "
    );
    
    query_builder.push_values(&candles, |mut b, candle| {
        b.push_bind(candle.product_id.clone())
            .push_bind(candle.timeframe.clone())
            .push_bind(candle.timestamp)
            .push_bind(candle.open)
            .push_bind(candle.high)
            .push_bind(candle.low)
            .push_bind(candle.close)
            .push_bind(candle.volume);
    });
    
    query_builder.push(" ON CONFLICT (product_id, timeframe, timestamp) DO NOTHING");

    let query = query_builder.build();
    let result = query.execute(pool).await?;
    
    Ok(result.rows_affected() as usize)
}

fn parse_date(date_str: &str) -> Result<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")?;
    // Default to midnight UTC
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap()))
}

struct BinanceDownloader {
    client: Client,
    base_url: String,
}

impl BinanceDownloader {
    fn new(client: Client) -> Self {
        Self {
            client,
            base_url: "https://fapi.binance.com".to_string(),
        }
    }

    async fn fetch_candles(&self, product_id: &str, start_time: i64, end_time: i64) -> Result<Vec<Candlestick>> {
        // product_id is like BINANCE:BTCUSDT-PERP
        // Extract symbol: BTCUSDT
        let parts: Vec<&str> = product_id.split(':').collect();
        let symbol_part = parts.get(1).ok_or(anyhow!("Invalid product_id"))?;
        let symbol = symbol_part.replace("-PERP", "");

        let url = format!("{}/fapi/v1/klines", self.base_url);
        
        let params = [
            ("symbol", symbol.as_str()),
            ("interval", "1m"),
            ("startTime", &start_time.to_string()),
            ("endTime", &end_time.to_string()),
            ("limit", "1500"),
        ];

        let resp = self.client.get(&url)
            .query(&params)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await?;
            return Err(anyhow!("Binance API Error {}: {}", status, text));
        }

        let raw_data: Vec<Vec<Value>> = resp.json().await?;
        
        let mut candles = Vec::new();
        for k in raw_data {
            // [open_time, open, high, low, close, volume, ...]
            if k.len() < 6 { continue; }
            
            let timestamp = k[0].as_i64().ok_or(anyhow!("Invalid timestamp"))?;
            let open = Decimal::from_str(k[1].as_str().unwrap_or("0"))?;
            let high = Decimal::from_str(k[2].as_str().unwrap_or("0"))?;
            let low = Decimal::from_str(k[3].as_str().unwrap_or("0"))?;
            let close = Decimal::from_str(k[4].as_str().unwrap_or("0"))?;
            let volume = Decimal::from_str(k[5].as_str().unwrap_or("0"))?;

            candles.push(Candlestick {
                product_id: product_id.to_string(),
                timeframe: "1m".to_string(),
                timestamp,
                open,
                high,
                low,
                close,
                volume,
            });
        }

        Ok(candles)
    }
}
