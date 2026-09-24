use super::*;
use fluxtrade_core::volume_profile::checkpoint::CheckpointProgress;
use serde_json::{json, Value};
use std::fs::File;

fn args(root: &std::path::Path) -> Vec<OsString> {
    vec![
        "assemble".into(),
        "--staging-root".into(),
        root.as_os_str().into(),
        "--job-id".into(),
        "daily".into(),
        "--start-ms".into(),
        "0".into(),
    ]
}
fn kline() -> Vec<u8> {
    serde_json::to_vec(&json!([[
        0,
        "0",
        "0",
        "0",
        "0",
        "0",
        DAY - 1,
        "0",
        0,
        "0",
        "0",
        "0"
    ]]))
    .unwrap()
}
fn identity(hour: i64) -> Identity {
    Identity::new(
        daily::hourly_staging_job_id("daily", hour * mvp::HOUR).unwrap(),
        Window::new(hour * mvp::HOUR, (hour + 1) * mvp::HOUR).unwrap(),
        mvp::GRID.into(),
        mvp::config_hash(),
    )
    .unwrap()
}
fn store(root: &File, hour: i64) -> Store {
    Store::open(
        root,
        identity(hour),
        compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW).unwrap(),
        mvp::MANIFEST,
    )
    .unwrap()
}
fn seed() -> tempfile::TempDir {
    let directory = tempfile::tempdir().unwrap();
    let root = File::open(directory.path()).unwrap();
    for hour in 0..24 {
        store(&root, hour)
            .append(0, b"[]", CheckpointProgress::EmptyPage)
            .unwrap();
    }
    kline_evidence::persist(
        &root,
        kline_evidence::Identity::new("daily".into(), Window::new(0, DAY).unwrap()).unwrap(),
        &kline(),
        DAY + 17,
    )
    .unwrap();
    directory
}
fn invoke(root: &std::path::Path, now: i64) -> (u8, Vec<u8>, Vec<u8>) {
    let (mut output, mut errors) = (Vec::new(), Vec::new());
    let code = run(args(root), &mut output, &mut errors, || Ok(now));
    (code, output, errors)
}

#[test]
fn arguments_day_and_removed_input_flags() {
    let root = std::path::Path::new("/SECRET_path");
    for flag in [
        "--endpoint",
        "--symbol",
        "--api-key",
        "--credentials",
        "--auth",
        "--proxy",
        "--rithmic",
        "--config",
        "--end-ms",
        "--source-available-at-ms",
        "--input",
        "--raw-path",
    ] {
        let mut argv = args(root);
        argv.extend([flag.into(), "SECRET".into()]);
        let (mut output, mut errors) = (Vec::new(), Vec::new());
        assert_eq!(run(argv, &mut output, &mut errors, || Ok(DAY)), 1);
        assert!(output.is_empty());
        assert_eq!(errors, b"volume_profile_assemble: invalid arguments\n");
    }
    for index in [1, 3, 5] {
        let mut argv = args(root);
        argv.drain(index..index + 2);
        assert!(Args::try_parse_from(argv).is_err());
    }
    let mut parsed = Args::try_parse_from(args(root)).unwrap();
    for (start, now, valid) in [
        (0, DAY, true),
        (DAY, 2 * DAY, true),
        (0, DAY - 1, false),
        (1, 2 * DAY, false),
        (-DAY, DAY, false),
        (i64::MAX / DAY * DAY, i64::MAX, false),
    ] {
        parsed.start_ms = start;
        assert_eq!(window(&parsed, now).is_ok(), valid);
    }
    parsed.start_ms = 0;
    parsed.job_id = "../bad".into();
    assert!(window(&parsed, DAY).is_err());
}

#[test]
fn immutable_evidence_produces_exact_restart_bytes() {
    let directory = seed();
    let root = File::open(directory.path()).unwrap();
    let hours: Vec<_> = (0..24)
        .map(|i| store(&root, i).recover().unwrap())
        .collect();
    let mut expected = daily::assemble(
        "daily",
        Window::new(0, DAY).unwrap(),
        mvp::config_hash(),
        &hours,
        &kline(),
        DAY + 17,
    )
    .unwrap()
    .to_bytes()
    .unwrap();
    expected.push(b'\n');
    for now in [DAY + 17, DAY + 100, DAY * 2] {
        let (code, output, errors) = invoke(directory.path(), now);
        assert_eq!(code, 0);
        assert!(errors.is_empty());
        assert_eq!(output, expected);
        assert_eq!(
            serde_json::from_slice::<Value>(&output).unwrap()["source_available_at_ms"],
            DAY + 17
        );
    }
}

#[test]
fn bad_evidence_or_hours_never_emit_partial_output() {
    for mode in [
        "missing_evidence",
        "corrupt_evidence",
        "future_evidence",
        "wrong_evidence_identity",
        "missing_hour",
        "corrupt_hour",
        "nonterminal",
        "identity",
        "oversize",
    ] {
        let directory = seed();
        let evidence = directory.path().join("daily/daily-kline-evidence.json");
        let job = daily::hourly_staging_job_id("daily", 0).unwrap();
        let path = directory.path().join(job).join("manifest.json");
        match mode {
            "missing_evidence" => {
                std::fs::rename(&evidence, evidence.with_extension("saved")).unwrap();
            }
            "corrupt_evidence" => std::fs::write(&evidence, b"SECRET_raw").unwrap(),
            "wrong_evidence_identity" => {
                let mut value: Value =
                    serde_json::from_slice(&std::fs::read(&evidence).unwrap()).unwrap();
                value["identity"]["config_sha256"] = json!("b".repeat(64));
                std::fs::write(&evidence, serde_json::to_vec(&value).unwrap()).unwrap();
            }
            "missing_hour" => {
                std::fs::rename(path.parent().unwrap(), directory.path().join("saved-hour"))
                    .unwrap();
            }
            "corrupt_hour" => std::fs::write(&path, b"SECRET_bad_manifest").unwrap(),
            "nonterminal" => {
                let manifest =
                    fluxtrade_core::volume_profile::checkpoint::Manifest::new(identity(0), vec![]);
                std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            }
            "identity" => {
                let mut value: Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                value["identity"]["grid_id"] = json!("wrong");
                std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            }
            "oversize" => std::fs::write(&evidence, vec![b' '; 400_000]).unwrap(),
            _ => (),
        }
        let (code, output, errors) = invoke(
            directory.path(),
            if mode == "future_evidence" {
                DAY
            } else {
                DAY + 17
            },
        );
        assert_eq!(code, 1, "{mode}");
        assert!(output.is_empty());
        assert_eq!(errors, b"volume_profile_assemble: assembly failed\n");
    }
}

#[test]
fn symlink_root_hour_and_evidence_are_rejected() {
    for mode in ["root", "hour", "evidence"] {
        let directory = seed();
        let links = tempfile::tempdir().unwrap();
        let target = if mode == "root" {
            links.path().join("root")
        } else if mode == "hour" {
            directory
                .path()
                .join(daily::hourly_staging_job_id("daily", 0).unwrap())
        } else {
            directory.path().join("daily/daily-kline-evidence.json")
        };
        if mode != "root" {
            std::fs::rename(&target, links.path().join("saved")).unwrap();
        }
        std::os::unix::fs::symlink(
            if mode == "root" {
                directory.path().to_path_buf()
            } else {
                links.path().join("saved")
            },
            &target,
        )
        .unwrap();
        let (code, output, _) = invoke(
            if mode == "root" {
                &target
            } else {
                directory.path()
            },
            DAY + 17,
        );
        assert_eq!(code, 1);
        assert!(output.is_empty());
    }
}
