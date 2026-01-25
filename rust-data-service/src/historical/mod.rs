use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use zip::ZipArchive;

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

    let product_id = format!("{}:{}-PERP", exchange.to_uppercase(), symbol.to_uppercase());

    // 3. Ensure Product and Exchange exist in DB
    ensure_product_exists(&pool, &exchange, &symbol, &product_id).await?;

    // 4. Setup Downloader
    let client = Client::new();
    let downloader = match exchange.to_lowercase().as_str() {
        "binance" => Arc::new(BinanceDownloader::new(client)),
        _ => return Err(anyhow!("Unsupported exchange: {}", exchange)),
    };

    // 5. Hybrid Logic: Determine S3 vs REST
    let now = Utc::now();
    let first_day_current_month = NaiveDate::from_ymd_opt(now.year(), now.month(), 1).unwrap();
    let s3_boundary_ts = Utc.from_utc_datetime(&first_day_current_month.and_hms_opt(0, 0, 0).unwrap()).timestamp_millis();

    let semaphore = Arc::new(Semaphore::new(5));

    // S3 Backfill (Monthly)
    if start_ts < s3_boundary_ts {
        let s3_end = end_ts.min(s3_boundary_ts - 1);
        info!("Starting S3 backfill from {} to {}", start_ts, s3_end);
        
        let curr_month_start = NaiveDate::from_ymd_opt(start.year(), start.month(), 1).unwrap();

        let mut s3_tasks = Vec::new();
        
        // We want to cover all months from start_ts up to s3_end
        let s3_end_date = NaiveDate::from_ymd_opt(
            if s3_end == s3_boundary_ts - 1 {
                let prev_month = first_day_current_month.pred_opt().unwrap_or(first_day_current_month);
                prev_month.year()
            } else {
                let dt = Utc.timestamp_millis_opt(s3_end).unwrap();
                dt.year()
            },
            if s3_end == s3_boundary_ts - 1 {
                let prev_month = first_day_current_month.pred_opt().unwrap_or(first_day_current_month);
                prev_month.month()
            } else {
                let dt = Utc.timestamp_millis_opt(s3_end).unwrap();
                dt.month() as u32
            },
            1
        ).unwrap();

        let mut curr = curr_month_start;
        while curr <= s3_end_date {
            let downloader = downloader.clone();
            let pool = pool.clone();
            let symbol = symbol.clone();
            let semaphore = semaphore.clone();
            let year = curr.year();
            let month = curr.month();

            s3_tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                process_s3_month(&downloader, &pool, &symbol, year, month, start_ts, end_ts).await
            }));

            // Increment month
            if curr.month() == 12 {
                curr = NaiveDate::from_ymd_opt(curr.year() + 1, 1, 1).unwrap();
            } else {
                curr = NaiveDate::from_ymd_opt(curr.year(), curr.month() + 1, 1).unwrap();
            }
        }

        for task in s3_tasks {
            match task.await? {
                Ok(count) => if count > 0 { info!("S3 Month finished. Inserted {} candles", count) },
                Err(e) => error!("S3 Task failed: {}", e),
            }
        }
    }

    // REST Backfill (Chunks) for the remaining range
    if end_ts >= s3_boundary_ts {
        let rest_start = start_ts.max(s3_boundary_ts);
        info!("Starting REST backfill from {} to {}", rest_start, end_ts);

        let interval_ms = 60000;
        let limit = 1000;
        let chunk_duration = interval_ms * limit;
        
        let mut chunks = Vec::new();
        let mut current_start = rest_start;
        while current_start < end_ts {
            let mut current_end = current_start + chunk_duration - 1;
            if current_end > end_ts {
                current_end = end_ts;
            }
            chunks.push((current_start, current_end));
            current_start = current_end + 1;
        }

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
            .buffer_unordered(5);

        results.for_each(|res| async {
            match res {
                Ok(Ok(count)) => info!("REST Chunk finished. Inserted {} candles", count),
                Ok(Err(e)) => error!("REST Chunk failed: {}", e),
                Err(e) => error!("Task join error: {}", e),
            }
        }).await;
    }

    info!("Backfill completed!");
    Ok(())
}

async fn process_s3_month(
    downloader: &BinanceDownloader,
    pool: &Pool<Postgres>,
    symbol: &str,
    year: i32,
    month: u32,
    start_ts: i64,
    end_ts: i64,
) -> Result<usize> {
    let mut candles = downloader.fetch_from_s3(symbol, year, month).await?;
    if candles.is_empty() {
        return Ok(0);
    }

    // Filter by range
    candles.retain(|c| c.timestamp >= start_ts && c.timestamp <= end_ts);
    
    if candles.is_empty() {
        return Ok(0);
    }

    insert_candles(pool, &mut candles).await
}

async fn process_chunk(
    downloader: &BinanceDownloader,
    pool: &Pool<Postgres>,
    product_id: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<usize> {
    let mut candles = downloader
        .fetch_candles(product_id, start_ts, end_ts)
        .await?;
    
    if candles.is_empty() {
        return Ok(0);
    }

    insert_candles(pool, &mut candles).await
}

async fn insert_candles(
    pool: &Pool<Postgres>,
    candles: &mut Vec<Candlestick>,
) -> Result<usize> {
    if candles.is_empty() {
        return Ok(0);
    }

    // Sort and Fill Gaps
    candles.sort_by_key(|c| c.timestamp);
    let mut filled_candles = Vec::new();
    filled_candles.push(candles[0].clone());
    
    for i in 1..candles.len() {
        let curr = &candles[i];
        let (prev_close, last_ts) = {
            let last = filled_candles.last().unwrap();
            (last.close, last.timestamp)
        };
        
        let mut expected_ts = last_ts + 60000;
        while expected_ts < curr.timestamp {
            filled_candles.push(Candlestick {
                product_id: curr.product_id.clone(),
                timeframe: "1m".to_string(),
                timestamp: expected_ts,
                open: prev_close,
                high: prev_close,
                low: prev_close,
                close: prev_close,
                volume: Decimal::ZERO,
            });
            expected_ts += 60000;
        }
        filled_candles.push(curr.clone());
    }

    // Bulk Insert in batches of 1000
    let mut total_affected = 0;
    for chunk in filled_candles.chunks(1000) {
        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO candlestick (product_id, timeframe, timestamp, open, high, low, close, volume) "
        );
        
        query_builder.push_values(chunk, |mut b, candle| {
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
        total_affected += result.rows_affected();
    }
    
    Ok(total_affected as usize)
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

    pub async fn fetch_from_s3(&self, symbol: &str, year: i32, month: u32) -> Result<Vec<Candlestick>> {
        let url = format!(
            "https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{year}-{month:02}.zip",
            symbol = symbol,
            year = year,
            month = month
        );
        
        info!("Fetching S3 Archive: {}", url);
        
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                warn!("S3 Archive not found: {}", url);
                return Ok(Vec::new());
            }
            return Err(anyhow!("S3 Download Error {}: {}", resp.status(), url));
        }
        
        let bytes = resp.bytes().await?;
        let cursor = Cursor::new(bytes);
        let mut archive = ZipArchive::new(cursor)?;
        
        let mut candles = Vec::new();
        let product_id = format!("BINANCE:{}-PERP", symbol);

        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            if file.name().ends_with(".csv") {
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(file);
                
                for result in rdr.records() {
                    let record = result?;
                    if record.len() < 6 { continue; }
                    
                    let timestamp = record[0].parse::<i64>()?;
                    let open = Decimal::from_str(&record[1])?;
                    let high = Decimal::from_str(&record[2])?;
                    let low = Decimal::from_str(&record[3])?;
                    let close = Decimal::from_str(&record[4])?;
                    let volume = Decimal::from_str(&record[5])?;
                    
                    candles.push(Candlestick {
                        product_id: product_id.clone(),
                        timeframe: "1m".to_string(),
                        timestamp,
                        open,
                        high,
                        low,
                        close,
                        volume,
                    });
                }
            }
        }
        
        Ok(candles)
    }
}

async fn ensure_product_exists(
    pool: &Pool<Postgres>,
    exchange: &str,
    symbol: &str,
    product_id: &str,
) -> Result<()> {
    // 1. Ensure exchange exists
    sqlx::query("INSERT INTO exchange (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING")
        .bind(exchange.to_uppercase())
        .bind(exchange.to_lowercase())
        .execute(pool)
        .await?;

    // 2. Parse base/quote from symbol
    let (base, quote) = if symbol.ends_with("USDT") {
        (symbol.replace("USDT", ""), "USDT".to_string())
    } else if symbol.ends_with("USDC") {
        (symbol.replace("USDC", ""), "USDC".to_string())
    } else {
        (symbol.to_string(), "USDT".to_string())
    };

    // 3. Ensure product exists
    sqlx::query("INSERT INTO product (id, exchange_id, base_asset, quote_asset) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING")
        .bind(product_id)
        .bind(exchange.to_uppercase())
        .bind(base)
        .bind(quote)
        .execute(pool)
        .await?;

    info!("Product verified/registered: {}", product_id);
    Ok(())
}
