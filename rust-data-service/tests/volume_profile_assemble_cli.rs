#![cfg(unix)]
use fluxtrade_core::volume_profile::{
    checkpoint::{CheckpointProgress, Identity},
    compressed_page, daily, mvp,
    store::Store,
    Window,
};
use std::{
    fs::File,
    io::Write,
    process::{Command, Stdio},
};

#[test]
fn actual_executable_replays_source_pair_with_canonical_lf() {
    let staging = tempfile::tempdir().unwrap();
    let root = File::open(staging.path()).unwrap();
    let mut hours = Vec::new();
    for hour in 0..24 {
        let start = hour * mvp::HOUR;
        let identity = Identity::new(
            daily::hourly_staging_job_id("daily", start).unwrap(),
            Window::new(start, start + mvp::HOUR).unwrap(),
            mvp::GRID.into(),
            mvp::config_hash(),
        )
        .unwrap();
        let store = Store::open(
            &root,
            identity,
            compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW).unwrap(),
            mvp::MANIFEST,
        )
        .unwrap();
        store
            .append(0, b"[]", CheckpointProgress::EmptyPage)
            .unwrap();
        hours.push(store.recover().unwrap());
    }
    let end = 24 * mvp::HOUR;
    let raw = serde_json::to_vec(&serde_json::json!([[
        0,
        "0",
        "0",
        "0",
        "0",
        "0",
        end - 1,
        "0",
        0,
        "0",
        "0",
        "0"
    ]]))
    .unwrap();
    let mut expected = daily::assemble(
        "daily",
        Window::new(0, end).unwrap(),
        mvp::config_hash(),
        &hours,
        &raw,
        end,
    )
    .unwrap()
    .to_bytes()
    .unwrap();
    expected.push(b'\n');
    fluxtrade_core::volume_profile::kline_evidence::persist(
        &root,
        fluxtrade_core::volume_profile::kline_evidence::Identity::new(
            "daily".into(),
            Window::new(0, end).unwrap(),
        )
        .unwrap(),
        &raw,
        end,
    )
    .unwrap();
    for _ in 0..2 {
        let mut child = Command::new(env!("CARGO_BIN_EXE_volume_profile_assemble"))
            .arg("--staging-root")
            .arg(staging.path())
            .args(["--job-id", "daily", "--start-ms", "0"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        if let Err(error) = stdin.write_all(b"SECRET ignored stdin") {
            assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe, "{error}");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        if child.try_wait().unwrap().is_none() {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("assembler consumed stdin or exceeded deadline");
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert_eq!(output.stdout, expected);
        assert_eq!(output.stdout.iter().filter(|&&b| b == b'\n').count(), 1);
    }
}
