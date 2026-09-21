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
    let wire: serde_json::Value = serde_json::from_slice(&expected).unwrap();
    let content = wire["content_sha256"].as_str().unwrap();
    let handoff: String = ring::digest::digest(&ring::digest::SHA256, &expected)
        .as_ref()
        .iter()
        .flat_map(|b| {
            [
                char::from(b"0123456789abcdef"[(b >> 4) as usize]),
                char::from(b"0123456789abcdef"[(b & 15) as usize]),
            ]
        })
        .collect();
    let command = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_volume_profile_cleanup"));
        command.arg("--staging-root").arg(staging.path()).args([
            "--job-id",
            "daily",
            "--start-ms",
            "0",
            "--expected-content-sha256",
            content,
        ]);
        command
    };
    let missing = bounded_cleanup(&mut command());
    assert!(!missing.status.success());
    assert!(missing.stdout.is_empty());
    assert_eq!(missing.stderr, b"volume_profile_cleanup: failed\n");
    let link = staging.path().join("root-link");
    std::os::unix::fs::symlink(staging.path(), &link).unwrap();
    let mut symlink = Command::new(env!("CARGO_BIN_EXE_volume_profile_cleanup"));
    symlink.arg("--staging-root").arg(&link).args([
        "--job-id",
        "daily",
        "--start-ms",
        "0",
        "--expected-content-sha256",
        content,
        "--expected-handoff-sha256",
        &handoff,
    ]);
    assert!(!bounded_cleanup(&mut symlink).status.success());
    let first = bounded_cleanup(command().args(["--expected-handoff-sha256", &handoff]));
    assert!(first.status.success());
    assert!(first.stderr.is_empty());
    let second = bounded_cleanup(&mut command());
    assert!(second.status.success());
    assert_eq!(second.stdout, first.stdout);
    assert!(second.stderr.is_empty());
    let report: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(report["reason"], "COMPLETE");
    assert_eq!(report["referenced_pages"], 24);
    assert!(first.stdout.ends_with(b"\n"));
    let forbidden = bounded_cleanup(command().args(["--raw-path", "SECRET"]));
    assert!(!forbidden.status.success());
    assert_eq!(
        forbidden.stderr,
        b"volume_profile_cleanup: invalid arguments\n"
    );
}

fn bounded_cleanup(command: &mut Command) -> std::process::Output {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _stdin = child.stdin.take().unwrap(); // Keep EOF unavailable throughout execution.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    if child.try_wait().unwrap().is_none() {
        child.kill().unwrap();
        child.wait().unwrap();
        panic!("cleanup deadline");
    }
    child.wait_with_output().unwrap()
}
