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
    for _ in 0..2 {
        let mut child = Command::new(env!("CARGO_BIN_EXE_volume_profile_assemble"))
            .arg("--staging-root")
            .arg(staging.path())
            .args([
                "--job-id",
                "daily",
                "--start-ms",
                "0",
                "--source-available-at-ms",
                &end.to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&raw).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        assert_eq!(output.stdout, expected);
        assert_eq!(output.stdout.iter().filter(|&&b| b == b'\n').count(), 1);
    }
}
