//! Offline assembly only. Controlled fetch owner attests stdin provenance;
//! success is not network-source verification, DB publication or cleanup.
#[cfg(unix)]
mod app {
    use anyhow::{ensure, Result};
    use clap::Parser;
    use fluxtrade_core::volume_profile::{
        checkpoint::Identity, compressed_page, daily, mvp, store::Store, Window,
    };
    use std::{
        ffi::OsString,
        io::{Read, Write},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    const DAY: i64 = 24 * mvp::HOUR;
    const KLINE_LIMIT: u64 = 65_536;
    #[derive(Parser)]
    #[command(
        about = "Offline daily profile assembly from 24 completed staging hours and kline JSON on stdin.",
        after_help = "Official stdin provenance belongs to the controlled fetch owner. Success is not network-source verification, DB publication or cleanup. Root/job directories must be trusted and single-worker. No network request is made."
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
    fn assemble(
        args: &Args,
        input: &mut impl Read,
        now: &mut impl FnMut() -> Result<i64>,
    ) -> Result<Vec<u8>> {
        let day = window(args, now()?)?;
        let mut raw = Vec::new();
        input.take(KLINE_LIMIT + 1).read_to_end(&mut raw)?;
        ensure!(raw.len() as u64 <= KLINE_LIMIT, "kline input limit");
        let observed = now()?; // Sample only after the complete bounded stdin read.
        let flags = rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC;
        let root = rustix::fs::open(&args.staging_root, flags, rustix::fs::Mode::empty())?;
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
        let mut bytes =
            daily::assemble(&args.job_id, day, config, &recovered, &raw, observed)?.to_bytes()?;
        bytes.push(b'\n');
        Ok(bytes)
    }
    fn run<I, T>(
        args: I,
        input: &mut impl Read,
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
        match assemble(&args, input, &mut now) {
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
            &mut std::io::stdin().lock(),
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
