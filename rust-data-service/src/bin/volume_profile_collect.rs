//! One-shot staging collector only; not scheduled, enabled or deployed.
#[cfg(unix)]
mod app {
    use anyhow::{ensure, Result};
    use clap::Parser;
    use fluxtrade_core::volume_profile::mvp::*;
    use fluxtrade_core::volume_profile::{
        binance_spot::PRODUCT_ID,
        checkpoint::Identity,
        collector::{self, Reason, Report},
        compressed_page,
        store::Store,
        transport::Transport,
        work_policy::{Budget, Failure, RetrySchedule},
        Window,
    };
    use serde::Serialize;
    use std::{
        path::PathBuf,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    #[derive(Parser)]
    #[command(
        about = "One-shot BTCUSDT spot staging; one completed UTC hour. No scheduler/deployment.",
        after_help = "Fixed: 20s/attempt, 200 requests, 25MiB body bytes, 5min; 3 retries (250..4000ms); 2MiB raw/manifest, 2112KiB ZIP. Root/job must be trusted and single-worker. Exit 75 requires external rescheduling; never retry Deferred before retry_after_seconds."
    )]
    pub struct Args {
        #[arg(long)]
        staging_root: PathBuf,
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        start_ms: i64,
        #[arg(long)]
        end_ms: i64,
    }

    fn window(start: i64, end: i64, now: i64) -> Result<Window> {
        ensure!(
            start >= 0 && start % HOUR == 0 && end % HOUR == 0,
            "unaligned UTC hour"
        );
        ensure!(
            start.checked_add(HOUR) == Some(end),
            "expected exactly one UTC hour"
        );
        ensure!(end <= now, "hour is not completed");
        Ok(Window::new(start, end)?)
    }
    #[derive(Serialize)]
    struct Output<'a> {
        reason: &'static str,
        status: Option<u16>,
        failure: Option<&'static str>,
        requests: u64,
        response_bytes: u64,
        retry_after_seconds: Option<u64>,
        job_id: &'a str,
        start_ms: i64,
        end_ms: i64,
        product: &'static str,
        config_sha256: String,
    }
    fn report(args: &Args, value: Report) -> Result<(String, u8)> {
        let (reason, code, status, failure, delay) = match value.reason {
            Reason::Complete => ("complete", 0, None, None, None),
            Reason::Budget => ("budget", 75, None, None, None),
            Reason::Deferred(s) => ("deferred", 75, Some(429), None, Some(s)),
            Reason::RetriesExhausted(d, f) => ("retries_exhausted", 75, d.status, f, None),
            Reason::Failed(d, f) => ("failed", 1, d.status, f, None),
        };
        let failure = failure.map(|f| match f {
            Failure::Timeout => "timeout",
            Failure::Connection => "connection",
            Failure::Decode => "decode",
            Failure::Protocol => "protocol",
        });
        let line = serde_json::to_string(&Output {
            reason,
            status,
            failure,
            requests: value.requests,
            response_bytes: value.response_bytes,
            retry_after_seconds: delay,
            job_id: &args.job_id,
            start_ms: args.start_ms,
            end_ms: args.end_ms,
            product: PRODUCT_ID,
            config_sha256: config_hex(),
        })?;
        Ok((line, code))
    }
    async fn collect(args: &Args) -> Result<(String, u8)> {
        let began = Instant::now();
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let window = window(args.start_ms, args.end_ms, now)?;
        let identity = Identity::new(args.job_id.clone(), window, GRID.into(), config_hash())?;
        let root = rustix::fs::open(
            &args.staging_root,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let store = Store::open(
            &root,
            identity,
            compressed_page::Limits::new(ARCHIVE, RAW)?,
            MANIFEST,
        )?;
        let mut source = Transport::new(Duration::from_millis(TIMEOUT_MS), RAW)?;
        let mut budget = Budget::new(WORK).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        let retries = RetrySchedule::new(MAX_RETRIES, BASE_DELAY_MS, MAX_DELAY_MS)?;
        let result = collector::round(
            &store,
            &mut source,
            &mut budget,
            retries,
            |ms| tokio::time::sleep(Duration::from_millis(ms)),
            || u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX),
        )
        .await?;
        report(args, result)
    }
    pub async fn main() -> u8 {
        let args = match Args::try_parse() {
            Ok(args) => args,
            Err(error) => {
                let _ = error.print();
                return u8::from(error.use_stderr());
            }
        };
        match collect(&args).await {
            Ok((line, code)) => {
                println!("{line}");
                code
            }
            Err(error) => {
                eprintln!("volume_profile_collect: {error}");
                1
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use fluxtrade_core::volume_profile::{self, binance_spot, work_policy};
        fn args() -> Vec<&'static str> {
            "collector --staging-root /unused --job-id job --start-ms 0 --end-ms 3600000"
                .split_whitespace()
                .collect()
        }
        #[test]
        fn arguments_and_hour_matrix() {
            assert!(Args::try_parse_from(args()).is_ok());
            for index in [1, 3, 5, 7] {
                let mut missing = args();
                missing.drain(index..index + 2);
                assert!(Args::try_parse_from(missing).is_err());
            }
            for flag in "--endpoint --symbol --api-key --credentials --auth --proxy --rithmic"
                .split_whitespace()
            {
                let mut bad = args();
                bad.extend([flag, "x"]);
                assert!(Args::try_parse_from(bad).is_err());
            }
            for (start, end, now, valid) in [
                (0, HOUR, HOUR, true),
                (HOUR, 2 * HOUR, 2 * HOUR, true),
                (-HOUR, 0, HOUR, false),
                (1, HOUR, HOUR, false),
                (0, HOUR + 1, 2 * HOUR, false),
                (0, 2 * HOUR, 2 * HOUR, false),
                (0, HOUR, HOUR - 1, false),
                (0, 0, HOUR, false),
                (i64::MAX / HOUR * HOUR, 0, i64::MAX, false),
            ] {
                assert_eq!(window(start, end, now).is_ok(), valid);
            }
        }
        #[test]
        fn stable_config_and_report_matrix() {
            use Reason::*;
            assert!(CONFIG.is_ascii());
            assert_eq!(
                config_hex(),
                "4c354eaa172499d024460f50f9c75cf286aea5fa52044923f7612cbeefbb2746"
            );
            let args = Args::try_parse_from(args()).unwrap();
            for (key, value) in [
                ("algorithm", volume_profile::ALGORITHM_VERSION),
                ("product", PRODUCT_ID),
                ("endpoint", binance_spot::ENDPOINT),
                ("grid_id", GRID),
                ("origin", "0"),
                ("step", "10"),
                ("unit", "USDT"),
            ] {
                assert!(CONFIG.lines().any(|line| line == format!("{key}={value}")));
            }
            for (key, value) in [
                ("timeout_ms", TIMEOUT_MS),
                ("requests", WORK.requests),
                ("response_bytes", WORK.response_bytes),
                ("elapsed_ms", WORK.elapsed_ms),
                ("max_retries", u64::from(MAX_RETRIES)),
                ("base_delay_ms", BASE_DELAY_MS),
                ("max_delay_ms", MAX_DELAY_MS),
                ("raw_bytes", RAW as u64),
                ("archive_bytes", ARCHIVE as u64),
                ("manifest_bytes", MANIFEST),
                ("page_limit", binance_spot::PAGE_LIMIT as u64),
            ] {
                assert!(CONFIG.lines().any(|line| line == format!("{key}={value}")));
            }
            let protocol = work_policy::failure(Failure::Protocol, None);
            let exhausted = RetriesExhausted(work_policy::http(500, &[]), None);
            let failed = Failed(protocol, Some(Failure::Protocol));
            for (reason, name, code, status, failure, delay) in [
                (Complete, "complete", 0, None, None, None),
                (Budget, "budget", 75, None, None, None),
                (Deferred(0), "deferred", 75, Some(429), None, Some(0)),
                (Deferred(7), "deferred", 75, Some(429), None, Some(7)),
                (exhausted, "retries_exhausted", 75, Some(500), None, None),
                (failed, "failed", 1, None, Some("protocol"), None),
            ] {
                let result = Report {
                    reason,
                    requests: 2,
                    response_bytes: 17,
                };
                let (line, exit) = report(&args, result).unwrap();
                assert_eq!(exit, code);
                assert!(!line.contains('\n'));
                let value: serde_json::Value = serde_json::from_str(&line).unwrap();
                assert_eq!(
                    value,
                    serde_json::json!({"reason":name,"status":status,"failure":failure,
                    "requests":2,"response_bytes":17,"retry_after_seconds":delay,"job_id":"job",
                    "start_ms":0,"end_ms":HOUR,"product":PRODUCT_ID,"config_sha256":config_hex()})
                );
            }
        }
    }
}
#[cfg(unix)]
#[tokio::main]
async fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(app::main().await)
}
#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("volume_profile_collect requires Unix staging support");
    std::process::ExitCode::FAILURE
}
