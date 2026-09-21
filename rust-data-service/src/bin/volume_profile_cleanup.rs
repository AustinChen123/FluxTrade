//! Explicit offline raw cleanup. Caller must exclusively own the entire daily family.
#[cfg(unix)]
mod app {
    use anyhow::{ensure, Result};
    use clap::Parser;
    use fluxtrade_core::volume_profile::{cleanup, mvp, Window};
    use std::{io::Write, path::PathBuf};
    #[derive(Parser)]
    #[command(
        about = "Explicit offline raw cleanup; exclusive staging-root/daily-family ownership required.",
        after_help = "No DB publication or cleanup scheduling. Retains manifests, marker, tombstones and directories. First call requires exact assembler stdout SHA-256 (JSON plus one LF)."
    )]
    struct Args {
        #[arg(long)]
        staging_root: PathBuf,
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        start_ms: i64,
        #[arg(long)]
        expected_content_sha256: String,
        #[arg(long)]
        expected_handoff_sha256: Option<String>,
    }
    fn execute(args: Args) -> Result<cleanup::Report> {
        ensure!(args.staging_root.is_absolute(), "absolute root required");
        let end = args
            .start_ms
            .checked_add(24 * mvp::HOUR)
            .ok_or_else(|| anyhow::anyhow!("day overflow"))?;
        let root = rustix::fs::open(
            &args.staging_root,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        cleanup::run(
            &root,
            &args.job_id,
            Window::new(args.start_ms, end)?,
            &args.expected_content_sha256,
            args.expected_handoff_sha256.as_deref(),
        )
    }
    pub fn main() -> u8 {
        let args = match Args::try_parse() {
            Ok(args) => args,
            Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
                print!("{error}");
                return 0;
            }
            Err(_) => {
                eprintln!("volume_profile_cleanup: invalid arguments");
                return 1;
            }
        };
        if let Ok(report) = execute(args) {
            if let Ok(bytes) = serde_json::to_vec(&report) {
                if std::io::stdout()
                    .lock()
                    .write_all(&[bytes.as_slice(), b"\n"].concat())
                    .is_ok()
                {
                    return 0;
                }
            }
        }
        eprintln!("volume_profile_cleanup: failed");
        1
    }
}
#[cfg(unix)]
fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(app::main())
}
#[cfg(not(unix))]
fn main() -> std::process::ExitCode {
    eprintln!("volume_profile_cleanup requires Unix");
    std::process::ExitCode::FAILURE
}
