use std::process::Command;

const TERMINAL_MARKER: &str = "FluxTrade terminal failure";
const SENTINEL: &str = "synthetic-provider-secret";

#[test]
fn unsupported_exchange_emits_one_sanitized_terminal_record() {
    let output = Command::new(env!("CARGO_BIN_EXE_rust-data-service"))
        .args(["live", "--exchange", SENTINEL, "--symbol", "BTCUSDT"])
        .env("FLUXTRADE_ENVIRONMENT", "test")
        .output()
        .expect("binary should run");
    for stream in [&output.stdout, &output.stderr] {
        assert!(!stream.contains(&0x1b), "ANSI escape leaked: {stream:?}");
    }
    let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(stdout.matches(TERMINAL_MARKER).count(), 1);
    for field in [
        "component=unknown",
        "task=unknown",
        "operation=unknown",
        "stage=unknown",
        "template_id=unknown",
        "payload_len=unknown",
        "stable_error_code=terminal_failure",
        "disposition=fatal_service_exit",
        "state_effect=process_exit",
        "safe_cause=terminal failure details unavailable",
    ] {
        assert!(stdout.contains(field), "missing {field}: {stdout}");
    }
    for stream in [&stdout, &stderr] {
        assert!(
            !stream.contains("Error:"),
            "Result termination leaked: {stream}"
        );
        assert!(!stream.contains(SENTINEL), "sentinel leaked: {stream}");
    }
    assert!(!stderr.contains(TERMINAL_MARKER));
}

#[test]
fn help_exits_without_terminal_record() {
    let output = Command::new(env!("CARGO_BIN_EXE_rust-data-service"))
        .arg("--help")
        .output()
        .expect("binary should run");

    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(TERMINAL_MARKER));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(TERMINAL_MARKER));
}
