//! One-shot public evidence acquisition, never a scheduler/publication/cleanup owner.
#[cfg(unix)]
mod app {
    use anyhow::{ensure, Result};
    use clap::Parser;
    use fluxtrade_core::volume_profile::{
        binance_kline, kline_evidence::Identity, kline_fetch, mvp, Window,
    };
    use std::{
        io::Write,
        path::PathBuf,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    #[derive(Parser)]
    #[command(
        about = "One-shot public Binance daily-kline evidence fetch; no scheduling or cleanup.",
        after_help = "Trusted existing staging root, single worker. At most four 3-second attempts, 30-second work budget. Exit 75 requires external scheduling; honor retry_after_seconds. No DB publication."
    )]
    struct Args {
        #[arg(long)]
        staging_root: PathBuf,
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        start_ms: i64,
    }
    fn identity(args: &Args) -> Result<Identity> {
        ensure!(
            args.staging_root.is_absolute(),
            "absolute staging root required"
        );
        let end = args
            .start_ms
            .checked_add(24 * mvp::HOUR)
            .ok_or_else(|| anyhow::anyhow!("daily overflow"))?;
        Identity::new(args.job_id.clone(), Window::new(args.start_ms, end)?)
    }
    async fn execute(args: Args) -> Result<(kline_fetch::Report, Identity)> {
        let origin = Instant::now();
        let identity = identity(&args)?;
        let root = rustix::fs::open(
            &args.staging_root,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let mut source = binance_kline::Transport::new(Duration::from_secs(3))?;
        let report = kline_fetch::run(
            &root,
            &identity,
            &mut source,
            || {
                Ok(i64::try_from(
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
                )?)
            },
            || u64::try_from(origin.elapsed().as_millis()).unwrap_or(u64::MAX),
            |ms| tokio::time::sleep(Duration::from_millis(ms)),
        )
        .await?;
        Ok((report, identity))
    }
    fn emit(
        result: Result<(kline_fetch::Report, Identity)>,
        output: &mut impl Write,
        errors: &mut impl Write,
    ) -> u8 {
        if let Ok((report, identity)) = result {
            if writeln!(output, "{}", report.json(&identity)).is_ok() {
                return report.exit_code();
            }
        }
        let _ = writeln!(errors, "volume_profile_kline_fetch: failed");
        1
    }
    pub async fn main() -> u8 {
        let args = match Args::try_parse() {
            Ok(args) => args,
            Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
                print!("{error}");
                return 0;
            }
            Err(_) => {
                eprintln!("volume_profile_kline_fetch: invalid arguments");
                return 1;
            }
        };
        emit(
            execute(args).await,
            &mut std::io::stdout().lock(),
            &mut std::io::stderr().lock(),
        )
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn arguments_identity_and_sanitized_error() {
            let base = [
                "fetch",
                "--staging-root",
                "/SECRET",
                "--job-id",
                "daily",
                "--start-ms",
                "0",
            ];
            assert!(identity(&Args::try_parse_from(base).unwrap()).is_ok());
            for flag in [
                "--endpoint",
                "--proxy",
                "--api-key",
                "--auth",
                "--rithmic",
                "--end-ms",
                "--config",
            ] {
                assert!(Args::try_parse_from(base.into_iter().chain([flag, "SECRET"])).is_err());
            }
            for index in [1, 3, 5] {
                let mut args = base.to_vec();
                args.drain(index..index + 2);
                assert!(Args::try_parse_from(args).is_err());
            }
            let (mut output, mut errors) = (Vec::new(), Vec::new());
            assert_eq!(
                emit(
                    Err(anyhow::anyhow!("SECRET raw path")),
                    &mut output,
                    &mut errors
                ),
                1
            );
            assert!(output.is_empty());
            assert_eq!(errors, b"volume_profile_kline_fetch: failed\n");
            let mut args = Args::try_parse_from(base).unwrap();
            args.staging_root = "relative".into();
            assert!(identity(&args).is_err());
            args.staging_root = "/trusted".into();
            args.start_ms = 1;
            assert!(identity(&args).is_err());
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
    eprintln!("volume_profile_kline_fetch requires Unix");
    std::process::ExitCode::FAILURE
}
