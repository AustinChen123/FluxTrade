use super::{
    config,
    history::{self, HistoryEvent, HistoryMinuteBar, HistoryPageDecoder},
    session::Plant,
    transport::{self, ConnectionEvent},
};
use crate::model::{validate_product_id, Candlestick};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, QueryBuilder};
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tracing::{info, warn};

const MINUTE_MS: i64 = 60_000;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const PAGE_TIMEOUT: Duration = Duration::from_secs(30);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const REQUEST_KEY: &str = "fluxtrade-history";
const MAX_SOURCE_CONTRACT_CHARS: usize = 64;
const MAX_HISTORY_RANGE_MS: i64 = 7 * 24 * 60 * MINUTE_MS;
static PARTIAL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) async fn run(
    profile: &str,
    product_id: &str,
    exchange: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<usize> {
    let candles = load_closed(
        profile,
        product_id,
        exchange,
        symbol,
        start_ms,
        end_ms,
        Utc::now().timestamp_millis(),
    )
    .await?;
    persist_exact(&candles).await
}

pub(crate) async fn export_csv(
    profile: &str,
    product_id: &str,
    exchange: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    output: &Path,
) -> Result<usize> {
    preflight_csv_export(symbol, output)?;
    let candles = load_closed(
        profile,
        product_id,
        exchange,
        symbol,
        start_ms,
        end_ms,
        Utc::now().timestamp_millis(),
    )
    .await?;
    persist_csv_exact(&candles, symbol, output)?;
    Ok(candles.len())
}

async fn load_closed(
    profile: &str,
    product_id: &str,
    exchange: &str,
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    now_ms: i64,
) -> Result<ClosedHistoryBatch> {
    validate_product_id(product_id)?;
    ensure!(
        product_id.starts_with("RITHMIC:"),
        "Rithmic history product ID must use RITHMIC venue"
    );
    let window = ClosedHistoryWindow::new(start_ms, end_ms, now_ms)?;
    let runtime = config::load(profile, Plant::History)?;
    let bars = tokio::time::timeout(
        DOWNLOAD_TIMEOUT,
        download(runtime, product_id, exchange, symbol, window),
    )
    .await
    .context("Rithmic history download timed out")??;
    ClosedHistoryBatch::new(bars, product_id, window)
}

async fn download(
    runtime: config::RuntimeConfig,
    product_id: &str,
    exchange: &str,
    symbol: &str,
    window: ClosedHistoryWindow,
) -> Result<Vec<Candlestick>> {
    let mut connection = transport::connect(&runtime.url, runtime.login, RESPONSE_TIMEOUT).await?;
    let mut decoder = HistoryPageDecoder::new(REQUEST_KEY, exchange, symbol, window.finish_index)?;
    let mut next_start = Some(window.start_index);
    let mut bars = Vec::new();
    wait_for_heartbeat(&mut connection).await?;
    info!("Rithmic history heartbeat confirmed");

    while let Some(page_start) = next_start {
        info!(
            page_start,
            finish_index = window.finish_index,
            "requesting Rithmic history page"
        );
        connection
            .send_payload(history::minute_bar_replay_request(
                REQUEST_KEY,
                exchange,
                symbol,
                page_start,
                window.finish_index,
            )?)
            .await?;

        let (page_bars, following) = tokio::time::timeout(PAGE_TIMEOUT, async {
            let mut accumulator = HistoryPageAccumulator::new(product_id, window);
            loop {
                match connection.next_event().await? {
                    ConnectionEvent::HeartbeatConfirmed => {}
                    ConnectionEvent::Payload(payload) => {
                        if let PageAccumulation::Complete(next_start) =
                            accumulator.apply(decoder.decode(&payload)?)?
                        {
                            return Ok::<_, anyhow::Error>((accumulator.into_bars(), next_start));
                        }
                    }
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

#[derive(Debug, PartialEq)]
enum PageAccumulation {
    Continue,
    Complete(Option<i32>),
}

struct HistoryPageAccumulator<'a> {
    product_id: &'a str,
    window: ClosedHistoryWindow,
    bars: Vec<Candlestick>,
}

impl<'a> HistoryPageAccumulator<'a> {
    fn new(product_id: &'a str, window: ClosedHistoryWindow) -> Self {
        Self {
            product_id,
            window,
            bars: Vec::new(),
        }
    }

    fn apply(&mut self, event: HistoryEvent) -> Result<PageAccumulation> {
        match event {
            HistoryEvent::Bar(bar) => {
                if self.window.contains_bar_end(bar.end_timestamp) {
                    self.bars.push(to_candle(self.product_id, bar)?);
                }
                Ok(PageAccumulation::Continue)
            }
            HistoryEvent::PageEnded { next_start } => Ok(PageAccumulation::Complete(next_start)),
        }
    }

    fn into_bars(self) -> Vec<Candlestick> {
        self.bars
    }
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

#[derive(Clone, Copy, Debug)]
struct ClosedHistoryWindow {
    start_ms: i64,
    end_ms: i64,
    closed_cutoff_ms: i64,
    start_index: i32,
    finish_index: i32,
}

impl ClosedHistoryWindow {
    fn new(start_ms: i64, end_ms: i64, now_ms: i64) -> Result<Self> {
        ensure!(start_ms >= 0, "Rithmic history start must not be negative");
        ensure!(end_ms > start_ms, "Rithmic history end must follow start");
        ensure!(
            start_ms % MINUTE_MS == 0 && end_ms % MINUTE_MS == 0,
            "Rithmic history bounds must align to minute boundaries"
        );
        ensure!(
            end_ms - start_ms <= MAX_HISTORY_RANGE_MS,
            "Rithmic history range must not exceed 7 days; split the request"
        );
        ensure!(now_ms >= 0, "current time must not be negative");
        let closed_cutoff_ms = now_ms - now_ms % MINUTE_MS;
        ensure!(
            end_ms <= closed_cutoff_ms,
            "Rithmic history range includes an unclosed minute"
        );
        let start_index = i32::try_from(start_ms / 1_000)
            .context("Rithmic history start exceeds protocol range")?;
        let finish_index =
            i32::try_from(end_ms / 1_000).context("Rithmic history end exceeds protocol range")?;
        Ok(Self {
            start_ms,
            end_ms,
            closed_cutoff_ms,
            start_index,
            finish_index,
        })
    }

    fn contains_bar_end(self, end_timestamp: i64) -> bool {
        end_timestamp > self.start_ms && end_timestamp <= self.end_ms
    }
}

fn validate_closed_batch(
    candles: &[Candlestick],
    product_id: &str,
    window: ClosedHistoryWindow,
) -> Result<()> {
    let mut previous_timestamp = None;
    for candle in candles {
        candle.validate()?;
        ensure!(
            candle.product_id == product_id,
            "Rithmic history candle product mismatch"
        );
        ensure!(
            candle.timeframe == "1m",
            "Rithmic history candle timeframe mismatch"
        );
        ensure!(
            candle.timestamp >= window.start_ms && candle.timestamp < window.end_ms,
            "Rithmic history candle is outside the requested range"
        );
        ensure!(
            candle.timestamp % MINUTE_MS == 0,
            "Rithmic history candle timestamp is not minute-aligned"
        );
        let bar_end = candle
            .timestamp
            .checked_add(MINUTE_MS)
            .context("Rithmic history candle end overflow")?;
        ensure!(
            bar_end <= window.end_ms,
            "Rithmic history candle crosses the requested range"
        );
        ensure!(
            bar_end <= window.closed_cutoff_ms,
            "Rithmic history returned an unclosed candle"
        );
        ensure!(
            previous_timestamp.is_none_or(|previous| candle.timestamp > previous),
            "Rithmic history candles must be strictly increasing"
        );
        previous_timestamp = Some(candle.timestamp);
    }
    Ok(())
}

struct ClosedHistoryBatch {
    candles: Vec<Candlestick>,
}

impl ClosedHistoryBatch {
    fn new(
        candles: Vec<Candlestick>,
        product_id: &str,
        window: ClosedHistoryWindow,
    ) -> Result<Self> {
        validate_closed_batch(&candles, product_id, window)?;
        Ok(Self { candles })
    }

    fn len(&self) -> usize {
        self.candles.len()
    }
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

async fn persist_exact(batch: &ClosedHistoryBatch) -> Result<usize> {
    let candles = &batch.candles;
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

fn persist_csv_exact(
    batch: &ClosedHistoryBatch,
    source_contract: &str,
    output: &Path,
) -> Result<()> {
    let candles = &batch.candles;
    ensure!(
        !candles.is_empty(),
        "Rithmic history CSV export returned no closed candles"
    );
    let source_contract = source_contract.trim();
    preflight_csv_export(source_contract, output)?;
    let partial = unique_partial_path(output)?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&partial)
        .with_context(|| {
            format!(
                "failed to create Rithmic history partial CSV: {}",
                partial.display()
            )
        })?;

    let write_result = write_csv(file, candles, source_contract)
        .and_then(|file| {
            file.sync_all()
                .context("failed to sync Rithmic history CSV")
        })
        .and_then(|_| {
            fs::hard_link(&partial, output).with_context(|| {
                format!(
                    "failed to publish Rithmic history CSV: {}",
                    output.display()
                )
            })?;
            if let Err(error) = fs::remove_file(&partial) {
                warn!(
                    path = %partial.display(),
                    %error,
                    "Rithmic history CSV published but partial link cleanup failed"
                );
            }
            Ok(())
        });
    if write_result.is_err() {
        if let Err(error) = fs::remove_file(&partial) {
            warn!(
                path = %partial.display(),
                %error,
                "Rithmic history CSV export failed and partial cleanup also failed"
            );
        }
    }
    write_result
}

fn preflight_csv_export(source_contract: &str, output: &Path) -> Result<()> {
    validate_source_contract(source_contract.trim())?;
    ensure!(
        !output.as_os_str().is_empty(),
        "Rithmic history CSV output path must not be empty"
    );
    ensure!(
        output.file_name().is_some(),
        "Rithmic history CSV output must include a file name"
    );

    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_metadata = fs::metadata(parent).with_context(|| {
        format!(
            "failed to inspect Rithmic history CSV parent: {}",
            parent.display()
        )
    })?;
    ensure!(
        parent_metadata.is_dir(),
        "Rithmic history CSV parent is not a directory: {}",
        parent.display()
    );

    match fs::symlink_metadata(output) {
        Ok(_) => anyhow::bail!(
            "Rithmic history CSV output already exists: {}",
            output.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect Rithmic history CSV output: {}",
                output.display()
            )
        }),
    }
}

fn validate_source_contract(source_contract: &str) -> Result<()> {
    ensure!(
        !source_contract.is_empty(),
        "Rithmic history source contract must not be empty"
    );
    ensure!(
        source_contract.chars().count() <= MAX_SOURCE_CONTRACT_CHARS,
        "Rithmic history source contract must not exceed {MAX_SOURCE_CONTRACT_CHARS} characters"
    );
    Ok(())
}

fn unique_partial_path(output: &Path) -> Result<PathBuf> {
    let file_name = output
        .file_name()
        .context("Rithmic history CSV output must include a file name")?;
    let timestamp = Utc::now()
        .timestamp_nanos_opt()
        .context("current time is outside the supported CSV export range")?;
    let sequence = PARTIAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let mut partial_name = OsString::from(".");
    partial_name.push(file_name);
    partial_name.push(format!(
        ".{}.{}.{}.partial",
        std::process::id(),
        timestamp,
        sequence
    ));
    Ok(output.with_file_name(partial_name))
}

fn write_csv<W: Write>(writer: W, candles: &[Candlestick], source_contract: &str) -> Result<W> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    writer.write_record([
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source_contract",
    ])?;
    for candle in candles {
        writer.write_record([
            candle.timestamp.to_string(),
            candle.open.to_string(),
            candle.high.to_string(),
            candle.low.to_string(),
            candle.close.to_string(),
            candle.volume.to_string(),
            source_contract.to_string(),
        ])?;
    }
    writer.flush()?;
    writer
        .into_inner()
        .map_err(|error| error.into_error())
        .context("failed to finalize Rithmic history CSV")
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
        let window = ClosedHistoryWindow::new(0, 120_000, 179_999).unwrap();
        assert_eq!(window.start_index, 0);
        assert_eq!(window.finish_index, 120);
        assert_eq!(window.closed_cutoff_ms, 120_000);
        assert!(ClosedHistoryWindow::new(0, MAX_HISTORY_RANGE_MS, MAX_HISTORY_RANGE_MS).is_ok());

        for (start, end, now) in [
            (-60_000, 0, 120_000),
            (0, 0, 120_000),
            (1, 60_000, 120_000),
            (0, 60_001, 120_000),
            (0, 120_000, 119_999),
            (
                0,
                MAX_HISTORY_RANGE_MS + MINUTE_MS,
                MAX_HISTORY_RANGE_MS + MINUTE_MS,
            ),
        ] {
            assert!(ClosedHistoryWindow::new(start, end, now).is_err());
        }
    }

    #[test]
    fn closed_batch_validation_matrix() {
        let window = ClosedHistoryWindow::new(60_000, 180_000, 180_999).unwrap();
        let candle = |timestamp| Candlestick {
            product_id: "RITHMIC:NQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp,
            open: dec!(100),
            high: dec!(102),
            low: dec!(99),
            close: dec!(101),
            volume: dec!(4),
        };

        assert!(validate_closed_batch(
            &[candle(60_000), candle(120_000)],
            "RITHMIC:NQ-202609",
            window
        )
        .is_ok());
        for invalid in [
            vec![candle(0)],
            vec![candle(61_000)],
            vec![candle(180_000)],
            vec![candle(120_000), candle(60_000)],
            vec![candle(60_000), candle(60_000)],
        ] {
            assert!(validate_closed_batch(&invalid, "RITHMIC:NQ-202609", window).is_err());
        }
        assert!(validate_closed_batch(&[candle(60_000)], "RITHMIC:ES-202609", window).is_err());
    }

    #[test]
    fn requested_bar_end_matrix_excludes_boundary_and_overshoot_bars() {
        let window = ClosedHistoryWindow::new(60_000, 180_000, 180_999).unwrap();
        assert!(!window.contains_bar_end(60_000));
        assert!(window.contains_bar_end(120_000));
        assert!(window.contains_bar_end(180_000));
        assert!(!window.contains_bar_end(240_000));
    }

    #[test]
    fn production_page_accumulator_ignores_boundary_overshoot() {
        let window = ClosedHistoryWindow::new(60_000, 180_000, 180_999).unwrap();
        let bar = |end_timestamp| HistoryMinuteBar {
            exchange: "CME".to_string(),
            symbol: "NQU6".to_string(),
            end_timestamp,
            open: dec!(100),
            high: dec!(102),
            low: dec!(99),
            close: dec!(101),
            volume: dec!(4),
        };
        let mut accumulator = HistoryPageAccumulator::new("RITHMIC:NQ-202609", window);

        assert_eq!(
            accumulator.apply(HistoryEvent::Bar(bar(120_000))).unwrap(),
            PageAccumulation::Continue
        );
        assert_eq!(
            accumulator.apply(HistoryEvent::Bar(bar(240_000))).unwrap(),
            PageAccumulation::Continue
        );
        assert_eq!(
            accumulator
                .apply(HistoryEvent::PageEnded { next_start: None })
                .unwrap(),
            PageAccumulation::Complete(None)
        );
        let candles = accumulator.into_bars();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].timestamp, 60_000);
    }

    #[test]
    fn csv_uses_importable_decimal_schema_and_source_contract() {
        let candle = Candlestick {
            product_id: "RITHMIC:NQ-202609".to_string(),
            timeframe: "1m".to_string(),
            timestamp: 60_000,
            open: dec!(100.25),
            high: dec!(101.50),
            low: dec!(99.75),
            close: dec!(101.00),
            volume: dec!(4),
        };
        let output = write_csv(Vec::new(), &[candle], "NQU6").unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "timestamp,open,high,low,close,volume,source_contract\n\
             60000,100.25,101.50,99.75,101.00,4,NQU6\n"
        );
    }

    #[test]
    fn csv_publish_is_atomic_and_refuses_to_replace_existing_output() {
        let nonce = Utc::now().timestamp_nanos_opt().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "fluxtrade-rithmic-history-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let output = directory.join("history.csv");
        let window = ClosedHistoryWindow::new(0, 60_000, 60_999).unwrap();
        let batch = ClosedHistoryBatch::new(
            vec![Candlestick {
                product_id: "RITHMIC:NQ-202609".to_string(),
                timeframe: "1m".to_string(),
                timestamp: 0,
                open: dec!(100),
                high: dec!(101),
                low: dec!(99),
                close: dec!(100.5),
                volume: dec!(4),
            }],
            "RITHMIC:NQ-202609",
            window,
        )
        .unwrap();

        persist_csv_exact(&batch, "NQU6", &output).unwrap();
        assert!(output.is_file());
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        let original = fs::read(&output).unwrap();
        assert!(persist_csv_exact(&batch, "NQU6", &output).is_err());
        assert_eq!(fs::read(&output).unwrap(), original);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);

        fs::remove_file(output).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn csv_preflight_rejects_invalid_targets_before_download() {
        let nonce = Utc::now().timestamp_nanos_opt().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "fluxtrade-rithmic-preflight-history-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let output = directory.join("history.csv");
        let non_directory = directory.join("not-a-directory");
        fs::write(&non_directory, b"file").unwrap();

        assert!(preflight_csv_export("NQU6", &output).is_ok());
        assert!(preflight_csv_export("", &output).is_err());
        assert!(preflight_csv_export(&"N".repeat(65), &output).is_err());
        assert!(preflight_csv_export("NQU6", Path::new("")).is_err());
        assert!(preflight_csv_export("NQU6", &non_directory.join("history.csv")).is_err());
        assert!(preflight_csv_export("NQU6", &directory.join("missing/history.csv")).is_err());

        fs::write(&output, b"existing").unwrap();
        assert!(preflight_csv_export("NQU6", &output).is_err());

        fs::remove_file(output).unwrap();
        fs::remove_file(non_directory).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[tokio::test]
    async fn csv_export_preflights_before_loading_credentials() {
        let nonce = Utc::now().timestamp_nanos_opt().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "fluxtrade-rithmic-export-preflight-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let output = directory.join("history.csv");
        fs::write(&output, b"existing").unwrap();

        let error = export_csv("", "RITHMIC:NQ-202609", "CME", "NQU6", 0, 60_000, &output)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("output already exists"));

        fs::remove_file(output).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn csv_empty_batch_fails_without_creating_output() {
        let nonce = Utc::now().timestamp_nanos_opt().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "fluxtrade-rithmic-empty-history-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let output = directory.join("history.csv");
        let window = ClosedHistoryWindow::new(0, 60_000, 60_999).unwrap();
        let batch = ClosedHistoryBatch::new(Vec::new(), "RITHMIC:NQ-202609", window).unwrap();

        assert!(persist_csv_exact(&batch, "NQU6", &output).is_err());
        assert!(!output.exists());
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);

        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn csv_publish_ignores_stale_partial_and_matches_importer_contract() {
        let nonce = Utc::now().timestamp_nanos_opt().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "fluxtrade-rithmic-stale-history-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let output = directory.join("history.csv");
        let stale_partial = directory.join(".history.csv.partial");
        fs::write(&stale_partial, b"stale").unwrap();
        let window = ClosedHistoryWindow::new(0, 60_000, 60_999).unwrap();
        let batch = ClosedHistoryBatch::new(
            vec![Candlestick {
                product_id: "RITHMIC:NQ-202609".to_string(),
                timeframe: "1m".to_string(),
                timestamp: 0,
                open: dec!(100),
                high: dec!(101),
                low: dec!(99),
                close: dec!(100.5),
                volume: dec!(4),
            }],
            "RITHMIC:NQ-202609",
            window,
        )
        .unwrap();

        assert!(persist_csv_exact(&batch, &"N".repeat(64), &output).is_ok());
        assert_eq!(fs::read(&stale_partial).unwrap(), b"stale");
        assert!(validate_source_contract(&"N".repeat(65)).is_err());

        fs::remove_file(output).unwrap();
        fs::remove_file(stale_partial).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn csv_publish_does_not_replace_dangling_symlink() {
        use std::os::unix::fs::symlink;

        let nonce = Utc::now().timestamp_nanos_opt().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "fluxtrade-rithmic-symlink-history-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let output = directory.join("history.csv");
        symlink("missing.csv", &output).unwrap();
        let window = ClosedHistoryWindow::new(0, 60_000, 60_999).unwrap();
        let batch = ClosedHistoryBatch::new(
            vec![Candlestick {
                product_id: "RITHMIC:NQ-202609".to_string(),
                timeframe: "1m".to_string(),
                timestamp: 0,
                open: dec!(100),
                high: dec!(101),
                low: dec!(99),
                close: dec!(100.5),
                volume: dec!(4),
            }],
            "RITHMIC:NQ-202609",
            window,
        )
        .unwrap();

        assert!(persist_csv_exact(&batch, "NQU6", &output).is_err());
        assert!(fs::symlink_metadata(&output)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);

        fs::remove_file(output).unwrap();
        fs::remove_dir(directory).unwrap();
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
