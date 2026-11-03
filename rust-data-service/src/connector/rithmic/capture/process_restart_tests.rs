use super::super::super::*;

#[test]
fn fresh_os_process_restart_keeps_committed_capture_records_once() {
    const MODE: &str = "F4_CAPTURE_RESTART_CHILD";
    if let Ok(mode) = std::env::var(MODE) {
        let root = std::path::PathBuf::from(std::env::var_os("F4_CAPTURE_RESTART_ROOT").unwrap());
        std::env::set_var(PATH_ENV, root.join("capture"));
        std::env::set_var(KEY_FILE_ENV, root.join("capture.key"));
        std::env::set_var(KEY_VERSION_ENV, "restart_test");
        if mode == "blocked_shutdown" || mode == "dead_shutdown" {
            let state = if mode == "blocked_shutdown" {
                TestWriterState::Blocked
            } else {
                TestWriterState::WriteFailing
            };
            let owner = test_owner_writer(&root.join("capture"), state);
            initialize_process_capture_with(|| super::duplicate_capture_handle(&owner.capture));
            before_action(
                process_capture(),
                Direction::Outbound,
                Some(312),
                0,
                303,
                &[],
                || (),
            );
            if mode == "blocked_shutdown" {
                before_action(
                    process_capture(),
                    Direction::Inbound,
                    Some(450),
                    0,
                    303,
                    &[],
                    || (),
                );
                before_action(
                    process_capture(),
                    Direction::Outbound,
                    Some(312),
                    0,
                    404,
                    &[],
                    || (),
                );
                assert_eq!(process_capture().counts().0, 3);
                shutdown_process_capture();
                std::mem::forget(owner);
                println!("BLOCKED_WRITER_SHUTDOWN_RETURNED");
            } else {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                while !process_capture().health_counts().4 && std::time::Instant::now() < deadline {
                    std::thread::yield_now();
                }
                assert!(
                    process_capture().health_counts().4,
                    "writer failure was not observed"
                );
                while !*process_capture()
                    .counters
                    .admission_closed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "writer did not close admission"
                    );
                    std::thread::yield_now();
                }
                before_action(
                    process_capture(),
                    Direction::Outbound,
                    Some(312),
                    0,
                    404,
                    &[],
                    || (),
                );
                assert_eq!(process_capture().health_counts(), (1, 0, 2, 2, true));
                shutdown_process_capture();
                println!("DEAD_WRITER_RECOVERY_SHUTDOWN_RETURNED");
            }
            use std::io::Write;
            std::io::stdout().flush().unwrap();
            return;
        }
        initialize_process_capture();
        let connection_id = if mode == "first" { 101 } else { 202 };
        process_capture().attempt(Direction::Outbound, Some(312), 0, connection_id, &[]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while process_capture().counters.written.load(Ordering::Relaxed) != 1
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(
            process_capture().counters.written.load(Ordering::Relaxed),
            1
        );
        println!("CAPTURE_COMMITTED_{mode}");
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        if mode == "first" {
            std::thread::park();
        }
        return;
    }

    use std::io::{BufRead, BufReader};
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};
    let _external_tmpdir = std::env::var("TMPDIR").expect("agent TMPDIR must be configured");
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let capture_root = root.join("capture");
    std::fs::create_dir(&capture_root).unwrap();
    std::fs::set_permissions(&capture_root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let key_path = root.join("capture.key");
    std::fs::write(&key_path, [0x27; 32]).unwrap();
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let test_name = std::thread::current().name().unwrap().to_string();
    let mut first = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(&test_name)
        .arg("--nocapture")
        .env(MODE, "first")
        .env("F4_CAPTURE_RESTART_ROOT", root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut first_output = BufReader::new(first.stdout.take().unwrap());
    let mut line = String::new();
    while !line.contains("CAPTURE_COMMITTED_first") {
        line.clear();
        assert_ne!(
            first_output.read_line(&mut line).unwrap(),
            0,
            "child exited before commit"
        );
    }
    first.kill().unwrap();
    first.wait().unwrap();

    let mut restarted = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(&test_name)
        .arg("--nocapture")
        .env(MODE, "restart")
        .env("F4_CAPTURE_RESTART_ROOT", root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut restarted_output = BufReader::new(restarted.stdout.take().unwrap());
    line.clear();
    while !line.contains("CAPTURE_COMMITTED_restart") {
        line.clear();
        assert_ne!(
            restarted_output.read_line(&mut line).unwrap(),
            0,
            "restarted child exited before commit"
        );
    }
    assert!(restarted.wait().unwrap().success());

    for (mode, marker) in [
        ("blocked_shutdown", "BLOCKED_WRITER_SHUTDOWN_RETURNED"),
        ("dead_shutdown", "DEAD_WRITER_RECOVERY_SHUTDOWN_RETURNED"),
    ] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(&test_name)
            .arg("--nocapture")
            .env(MODE, mode)
            .env("F4_CAPTURE_RESTART_ROOT", root)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        line.clear();
        while !line.contains(marker) {
            line.clear();
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "child exited before {marker}"
            );
        }
        assert!(child.wait().unwrap().success());
    }

    let records: Vec<_> = std::fs::read_dir(&capture_root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().unwrap().is_dir())
        .flat_map(|entry| std::fs::read_dir(entry.path()).unwrap())
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name() == "capture.jsonl")
        .flat_map(|entry| {
            std::fs::read_to_string(entry.path())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(records.len(), 2);
    let mut ids: Vec<_> = records
        .iter()
        .map(|record| record["connection_id"].as_u64().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, [101, 202]);
}
