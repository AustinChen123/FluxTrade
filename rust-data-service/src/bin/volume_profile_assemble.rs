//! Offline assembly only, from immutable kline evidence and recovered hourly stores;
//! success is not network-source verification, DB publication or cleanup.
#[cfg(unix)]
mod app {
    use anyhow::{ensure, Result};
    use clap::Parser;
    use fluxtrade_core::volume_profile::{
        checkpoint::Identity, compressed_page, daily, kline_evidence, mvp, store::Store, Window,
    };
    use std::{
        ffi::OsString,
        io::Write,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    const DAY: i64 = 24 * mvp::HOUR;
    #[derive(Parser)]
    #[command(
        about = "Offline daily profile assembly from immutable kline evidence and 24 completed staging hours.",
        after_help = "Source bytes and first observation belong to the immutable evidence store. No stdin input is consumed. Success is not network-source verification, DB publication or cleanup. Root/job directories must be trusted and single-worker. No network request is made."
    )]
    struct Args {
        #[arg(long)]
        staging_root: PathBuf,
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        start_ms: i64,
    }
    fn window(args: &Args, now: i64) -> Result<Window> {
        daily::hourly_staging_job_id(&args.job_id, args.start_ms)?;
        ensure!(args.start_ms % DAY == 0, "invalid daily alignment");
        let end = args
            .start_ms
            .checked_add(DAY)
            .ok_or_else(|| anyhow::anyhow!("daily overflow"))?;
        ensure!(end <= now, "incomplete day");
        Ok(Window::new(args.start_ms, end)?)
    }
    fn assemble(args: &Args, now: &mut impl FnMut() -> Result<i64>) -> Result<Vec<u8>> {
        let invocation_time = now()?;
        let day = window(args, invocation_time)?;
        let flags = rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC;
        let root = rustix::fs::open(&args.staging_root, flags, rustix::fs::Mode::empty())?;
        let evidence = kline_evidence::recover(
            &root,
            &kline_evidence::Identity::new(args.job_id.clone(), day)?,
        )?;
        ensure!(
            evidence.observed_at_ms() <= invocation_time,
            "future source evidence"
        );
        let config = mvp::config_hash();
        let mut recovered = Vec::with_capacity(24);
        for hour in 0..24 {
            let start = day.start_ms() + hour * mvp::HOUR;
            let job = daily::hourly_staging_job_id(&args.job_id, start)?;
            // Existing-hour requirement only; Directory's descriptor-relative
            // NOFOLLOW opens remain the safety boundary, not this existence check.
            let _existing =
                rustix::fs::openat(&root, job.as_str(), flags, rustix::fs::Mode::empty())?;
            let identity = Identity::new(
                job,
                Window::new(start, start + mvp::HOUR)?,
                mvp::GRID.into(),
                config,
            )?;
            let store = Store::open(
                &root,
                identity,
                compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW)?,
                mvp::MANIFEST,
            )?;
            recovered.push(store.recover()?);
        }
        let mut bytes = daily::assemble(
            &args.job_id,
            day,
            config,
            &recovered,
            evidence.raw(),
            evidence.observed_at_ms(),
        )?
        .to_bytes()?;
        bytes.push(b'\n');
        Ok(bytes)
    }
    fn run<I, T>(
        args: I,
        output: &mut impl Write,
        errors: &mut impl Write,
        mut now: impl FnMut() -> Result<i64>,
    ) -> u8
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let args = match Args::try_parse_from(args) {
            Ok(args) => args,
            Err(error)
                if matches!(
                    error.kind(),
                    clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
                ) =>
            {
                return u8::from(write!(output, "{error}").is_err());
            }
            Err(_) => {
                let _ = writeln!(errors, "volume_profile_assemble: invalid arguments");
                return 1;
            }
        };
        match assemble(&args, &mut now) {
            Ok(bytes) if output.write_all(&bytes).is_ok() => 0,
            _ => {
                let _ = writeln!(errors, "volume_profile_assemble: assembly failed");
                1
            }
        }
    }
    pub fn main() -> u8 {
        run(
            std::env::args_os(),
            &mut std::io::stdout().lock(),
            &mut std::io::stderr().lock(),
            || {
                Ok(i64::try_from(
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
                )?)
            },
        )
    }
    #[cfg(test)]
    mod tests {
        include!("volume_profile_assemble/tests.rs");
    }
}
#[cfg(unix)]
fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(app::main())
}
#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("volume_profile_assemble requires Unix staging support");
    std::process::ExitCode::FAILURE
}
