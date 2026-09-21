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
        "--source-available-at-ms".into(),
        (DAY + 17).to_string().into(),
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
    directory
}
fn invoke(root: &std::path::Path, mut raw: &[u8]) -> (u8, Vec<u8>, Vec<u8>) {
    let (mut output, mut errors) = (Vec::new(), Vec::new());
    let code = run(args(root), &mut raw, &mut output, &mut errors, || {
        Ok(DAY + 17)
    });
    (code, output, errors)
}

#[test]
fn arguments_completed_day_and_safe_error_output() {
    let root = std::path::Path::new("/SECRET_path_marker");
    assert!(Args::try_parse_from(args(root)).is_ok());
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
    ] {
        let mut argv = args(root);
        argv.extend([flag.into(), "SECRET_value_marker".into()]);
        let (mut output, mut errors) = (Vec::new(), Vec::new());
        assert_eq!(
            run(argv, &mut &b"[]"[..], &mut output, &mut errors, || Ok(DAY)),
            1
        );
        assert!(output.is_empty());
        assert_eq!(errors, b"volume_profile_assemble: invalid arguments\n");
    }
    for index in [1, 3, 5, 7] {
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
    parsed.job_id = "../SECRET".into();
    assert!(window(&parsed, DAY).is_err());
    let (code, output, errors) = invoke(root, b"SECRET_raw");
    assert_eq!(code, 1);
    assert!(output.is_empty());
    assert_eq!(errors, b"volume_profile_assemble: assembly failed\n");
}

#[test]
fn real_store_hours_match_direct_assembly_and_observation_is_after_read() {
    let directory = seed();
    let raw = kline();
    let root = File::open(directory.path()).unwrap();
    let hours: Vec<_> = (0..24)
        .map(|i| store(&root, i).recover().unwrap())
        .collect();
    let mut expected = daily::assemble(
        "daily",
        Window::new(0, DAY).unwrap(),
        mvp::config_hash(),
        &hours,
        &raw,
        DAY + 17,
    )
    .unwrap()
    .to_bytes()
    .unwrap();
    expected.push(b'\n');
    let (code, output, errors) = invoke(directory.path(), &raw);
    assert_eq!(code, 0);
    assert!(errors.is_empty());
    assert_eq!(output, expected);
    assert_eq!(
        serde_json::from_slice::<Value>(&output).unwrap()["source_available_at_ms"],
        DAY + 17
    );
    struct RecordingReader<'a> {
        cursor: std::io::Cursor<Vec<u8>>,
        eof: &'a std::cell::Cell<bool>,
    }
    impl Read for RecordingReader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let count = self.cursor.read(buffer)?;
            if count == 0 && !buffer.is_empty() {
                self.eof.set(true);
            }
            Ok(count)
        }
    }
    let eof = std::cell::Cell::new(false);
    let mut input = RecordingReader {
        cursor: std::io::Cursor::new(raw),
        eof: &eof,
    };
    let mut calls = 0;
    let result = assemble(
        &Args::try_parse_from(args(directory.path())).unwrap(),
        &mut input,
        &mut || {
            calls += 1;
            assert!(eof.get(), "wall clock must follow the actual stdin EOF");
            Ok(DAY + 1000)
        },
    )
    .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(result, expected);
    assert_eq!(input.cursor.position(), input.cursor.get_ref().len() as u64);
}

#[test]
fn observation_bounds_and_restart_determinism() {
    let directory = seed();
    let raw = kline();
    let mut outputs = Vec::new();
    for now in [DAY + 17, DAY + 100, DAY * 2] {
        let (mut output, mut errors) = (Vec::new(), Vec::new());
        assert_eq!(
            run(
                args(directory.path()),
                &mut raw.as_slice(),
                &mut output,
                &mut errors,
                || Ok(now)
            ),
            0
        );
        assert!(errors.is_empty());
        assert!(output.ends_with(b"\n"));
        outputs.push(output);
    }
    assert!(outputs.windows(2).all(|pair| pair[0] == pair[1]));
    for (observed, now) in [
        (DAY - 1, DAY + 17),
        (DAY + 18, DAY + 17),
        (253_402_300_800_000, i64::MAX),
    ] {
        let mut argv = args(directory.path());
        argv[8] = observed.to_string().into();
        let (mut output, mut errors) = (Vec::new(), Vec::new());
        assert_eq!(
            run(argv, &mut raw.as_slice(), &mut output, &mut errors, || Ok(
                now
            )),
            1
        );
        assert!(output.is_empty());
        assert_eq!(errors, b"volume_profile_assemble: assembly failed\n");
    }
    for invalid in ["SECRET", "1.5", "9223372036854775808"] {
        let mut argv = args(directory.path());
        argv[8] = invalid.into();
        let (mut output, mut errors) = (Vec::new(), Vec::new());
        assert_eq!(
            run(argv, &mut raw.as_slice(), &mut output, &mut errors, || Ok(
                DAY
            )),
            1
        );
        assert!(output.is_empty());
        assert_eq!(errors, b"volume_profile_assemble: invalid arguments\n");
    }
}

#[test]
fn missing_corrupt_nonterminal_wrong_identity_and_input_limits_have_no_output() {
    for mode in [
        "missing",
        "corrupt",
        "nonterminal",
        "identity",
        "oversize",
        "malformed",
    ] {
        let directory = seed();
        let job = daily::hourly_staging_job_id("daily", 0).unwrap();
        let path = directory.path().join(job).join("manifest.json");
        match mode {
            "missing" => {
                std::fs::rename(
                    path.parent().unwrap(),
                    directory.path().join("not-the-hour"),
                )
                .unwrap();
            }
            "corrupt" => std::fs::write(&path, b"SECRET_bad_manifest").unwrap(),
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
            _ => (),
        }
        let raw = match mode {
            "oversize" => vec![b' '; 65_537],
            "malformed" => b"SECRET_raw_marker".to_vec(),
            _ => kline(),
        };
        let (code, output, errors) = invoke(directory.path(), &raw);
        assert_eq!(code, 1, "{mode}");
        assert!(output.is_empty());
        assert_eq!(errors, b"volume_profile_assemble: assembly failed\n");
    }
}

#[test]
fn symlink_root_and_job_are_rejected() {
    let directory = seed();
    let links = tempfile::tempdir().unwrap();
    let root_link = links.path().join("root");
    std::os::unix::fs::symlink(directory.path(), &root_link).unwrap();
    assert_eq!(invoke(&root_link, &kline()).0, 1);
    let job = daily::hourly_staging_job_id("daily", 0).unwrap();
    let original = directory.path().join(&job);
    let renamed = directory.path().join("saved");
    std::fs::rename(&original, &renamed).unwrap();
    std::os::unix::fs::symlink(&renamed, &original).unwrap();
    let (code, output, _) = invoke(directory.path(), &kline());
    assert_eq!(code, 1);
    assert!(output.is_empty());
}
