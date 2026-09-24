use super::super::work_policy;
use super::*;
use std::{
    collections::VecDeque,
    fs::{self, File},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

const DAY: i64 = 86_400_000;
const RAW: &[u8] = b"[[0,\"0\",\"0\",\"0\",\"0\",\"0\",86399999,\"0\",0,\"0\",\"0\",\"0\"]]";
fn identity() -> Identity {
    Identity::new("daily".into(), Window::new(0, DAY).unwrap()).unwrap()
}

#[tokio::test]
async fn any_cleanup_marker_blocks_recovery_before_source_or_clocks() {
    for present in [false, true] {
        for kind in ["valid", "partial", "directory", "symlink"] {
            let temp = tempfile::tempdir().unwrap();
            let root = File::open(temp.path()).unwrap();
            if present {
                kline_evidence::persist(&root, identity(), RAW, DAY).unwrap();
            } else {
                fs::create_dir(temp.path().join("daily")).unwrap();
            }
            let marker = temp
                .path()
                .join("daily")
                .join(super::super::cleanup::MARKER);
            match kind {
                "directory" => fs::create_dir(&marker).unwrap(),
                "symlink" => {
                    std::os::unix::fs::symlink(temp.path().join("missing"), &marker).unwrap()
                }
                "valid" => {
                    let value = serde_json::json!({"schema":1,"identity":identity(),"content_sha256":"a".repeat(64),"handoff_sha256":"b".repeat(64),
                        "hours":(0..24).map(|_|serde_json::json!({"manifest_sha256":"c".repeat(64),"page_count":1})).collect::<Vec<_>>()});
                    fs::write(&marker, serde_json::to_vec(&value).unwrap()).unwrap();
                }
                _ => fs::write(&marker, b"partial marker").unwrap(),
            }
            let mut source = Fake::new(vec![]);
            let error = run(
                &root,
                &identity(),
                &mut source,
                || panic!("retired wall"),
                || panic!("retired elapsed"),
                |_| async { panic!("retired wait") },
            )
            .await
            .unwrap_err();
            assert_eq!(error.to_string(), "daily source retired");
            assert_eq!(source.calls.load(Ordering::SeqCst), 0);
            assert!(kline_evidence::recover(&root, &identity()).is_err());
            assert!(kline_evidence::recover_optional(&root, &identity()).is_err());
            assert!(kline_evidence::persist(&root, identity(), RAW, DAY).is_err());
            assert_eq!(
                temp.path()
                    .join("daily")
                    .join(kline_evidence::FILE)
                    .exists(),
                present
            );
        }
    }
}

#[tokio::test]
async fn ack_unknown_visible_artifact_is_confirmed_without_refetch() {
    use super::super::directory::{Directory, Phase};
    let seed = tempfile::tempdir().unwrap();
    let seed_root = File::open(seed.path()).unwrap();
    kline_evidence::persist(&seed_root, identity(), RAW, DAY).unwrap();
    let bytes = fs::read(seed.path().join("daily").join(kline_evidence::FILE)).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let directory = Directory::open(&root, "daily").unwrap();
    assert!(directory
        .replace_with(kline_evidence::FILE, &bytes, |phase| {
            anyhow::ensure!(phase != Phase::BeforeDirectorySync, "ACK unknown");
            Ok(())
        })
        .is_err());
    let mut source = Fake::new(vec![]);
    let report = run(
        &root,
        &identity(),
        &mut source,
        || panic!("clock"),
        || panic!("elapsed"),
        |_| async { panic!("wait") },
    )
    .await
    .unwrap();
    assert_eq!(report.reason, Reason::Existing);
    assert_eq!(report.observed_at_ms, Some(DAY));
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn byte_budget_and_transport_failures_preserve_empty_staging() {
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let mut oversized = status(500, &[]);
    oversized.response_bytes = 4 * binance_kline::RAW_LIMIT as u64;
    let mut source = Fake::new(vec![oversized]);
    let report = run(
        &root,
        &identity(),
        &mut source,
        || Ok(DAY),
        || 0,
        |_| async { panic!("budget wait") },
    )
    .await
    .unwrap();
    assert_eq!(report.reason, Reason::Budget);
    assert_eq!(report.response_bytes, 262_144);
    for failure in [
        Failure::Timeout,
        Failure::Connection,
        Failure::Protocol,
        Failure::Decode,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        let outcome = Outcome {
            disposition: work_policy::failure(failure, None),
            failure: Some(failure),
            body: None,
            response_bytes: 0,
        };
        let mut source = Fake::new(vec![outcome, success(RAW)]);
        let mut waits = Vec::new();
        let result = run(
            &root,
            &identity(),
            &mut source,
            || Ok(DAY),
            || 0,
            |ms| {
                waits.push(ms);
                async {}
            },
        )
        .await
        .unwrap();
        if matches!(failure, Failure::Timeout | Failure::Connection) {
            assert_eq!(result.reason, Reason::Complete);
            assert_eq!(waits, vec![250]);
        } else {
            assert_eq!(result.reason, Reason::Failed);
            assert!(waits.is_empty());
            assert!(!temp.path().join("daily").exists());
        }
    }
}
fn success(raw: &[u8]) -> Outcome {
    Outcome {
        disposition: work_policy::http(200, &[]),
        failure: None,
        body: Some(raw.to_vec()),
        response_bytes: raw.len() as u64,
    }
}
fn status(code: u16, after: &[&str]) -> Outcome {
    Outcome {
        disposition: work_policy::http(code, after),
        failure: None,
        body: None,
        response_bytes: 0,
    }
}
struct Fake {
    outcomes: VecDeque<Outcome>,
    calls: Arc<AtomicUsize>,
}
impl Fake {
    fn new(outcomes: Vec<Outcome>) -> Self {
        Self {
            outcomes: outcomes.into(),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}
#[async_trait]
impl Source for Fake {
    async fn fetch(&mut self, day: Window, now: i64) -> Result<Outcome> {
        assert_eq!(day, Window::new(0, DAY).unwrap());
        assert!(now >= DAY);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.outcomes.pop_front().expect("unexpected fetch"))
    }
}

#[tokio::test]
async fn success_observation_existing_and_optional_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let id = identity();
    assert!(kline_evidence::recover_optional(&root, &id)
        .unwrap()
        .is_none());
    assert!(!temp.path().join("daily").exists());
    let mut source = Fake::new(vec![success(RAW)]);
    let calls = source.calls.clone();
    let mut clocks = 0;
    let result = run(
        &root,
        &id,
        &mut source,
        || {
            clocks += 1;
            if clocks == 2 {
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
            Ok(DAY + clocks)
        },
        || 0,
        |_| async { panic!("unexpected wait") },
    )
    .await
    .unwrap();
    assert_eq!(result.reason, Reason::Complete);
    assert_eq!(result.observed_at_ms, Some(DAY + 2));
    assert_eq!(result.requests, 1);
    assert_eq!(kline_evidence::recover(&root, &id).unwrap().raw(), RAW);
    let existing = run(
        &root,
        &id,
        &mut source,
        || panic!("wall on existing"),
        || panic!("elapsed on existing"),
        |_| async { panic!("wait on existing") },
    )
    .await
    .unwrap();
    assert_eq!(existing.reason, Reason::Existing);
    assert_eq!(existing.requests, 0);
    assert_eq!(existing.observed_at_ms, Some(DAY + 2));
}

#[tokio::test]
async fn retries_defer_stops_and_budget_are_local() {
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let id = identity();
    let mut source = Fake::new((0..4).map(|_| status(500, &[])).collect());
    let mut waits = Vec::new();
    let result = run(
        &root,
        &id,
        &mut source,
        || Ok(DAY),
        || 0,
        |ms| {
            waits.push(ms);
            async {}
        },
    )
    .await
    .unwrap();
    assert_eq!(result.reason, Reason::RetriesExhausted);
    assert_eq!(result.requests, 4);
    assert_eq!(waits, vec![250, 500, 1000]);
    for after in ["0", "99"] {
        let mut source = Fake::new(vec![status(429, &[after])]);
        let result = run(
            &root,
            &id,
            &mut source,
            || Ok(DAY),
            || 0,
            |_| async { panic!("defer wait") },
        )
        .await
        .unwrap();
        assert_eq!(result.reason, Reason::Deferred);
        assert_eq!(result.retry_after, Some(after.parse().unwrap()));
        assert_eq!(result.requests, 1);
    }
    for outcome in [
        status(418, &[]),
        status(451, &[]),
        status(400, &[]),
        success(b"{\"code\":-1}"),
    ] {
        let mut source = Fake::new(vec![outcome]);
        assert_eq!(
            run(
                &root,
                &id,
                &mut source,
                || Ok(DAY),
                || 0,
                |_| async { panic!("stop wait") }
            )
            .await
            .unwrap()
            .reason,
            Reason::Failed
        );
        assert!(kline_evidence::recover_optional(&root, &id)
            .unwrap()
            .is_none());
    }
    let mut source = Fake::new(vec![]);
    let result = run(&root, &id, &mut source, || Ok(DAY), || 30_000, |_| async {})
        .await
        .unwrap();
    assert_eq!(result.reason, Reason::Budget);
    assert_eq!(result.requests, 0);
    let mut source = Fake::new(vec![status(500, &[])]);
    let mut elapsed = 0;
    let result = run(
        &root,
        &id,
        &mut source,
        || Ok(DAY),
        || {
            elapsed += 1;
            if elapsed == 1 {
                0
            } else {
                29_999
            }
        },
        |_| async { panic!("crossing wait") },
    )
    .await
    .unwrap();
    assert_eq!(result.reason, Reason::Budget);
    assert_eq!(result.requests, 1);
}

#[tokio::test]
async fn clock_integrity_and_filesystem_fail_closed() {
    for clocks in [
        vec![DAY - 1],
        vec![DAY + 2, DAY + 1],
        vec![DAY, 253_402_300_800_000],
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        let mut source = Fake::new(vec![success(RAW)]);
        let mut clocks: VecDeque<_> = clocks.into();
        assert!(run(
            &root,
            &identity(),
            &mut source,
            || Ok(clocks.pop_front().unwrap()),
            || 0,
            |_| async {}
        )
        .await
        .is_err());
        assert!(!temp.path().join("daily").exists());
    }
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    fs::create_dir(temp.path().join("daily")).unwrap();
    assert!(kline_evidence::recover_optional(&root, &identity())
        .unwrap()
        .is_none());
    fs::write(
        temp.path().join("daily").join(kline_evidence::FILE),
        b"corrupt SECRET",
    )
    .unwrap();
    let mut source = Fake::new(vec![]);
    assert!(run(
        &root,
        &identity(),
        &mut source,
        || panic!("clock on corrupt"),
        || 0,
        |_| async {}
    )
    .await
    .is_err());
    assert_eq!(source.calls.load(Ordering::SeqCst), 0);
    let not_directory = File::open(temp.path().join("daily").join(kline_evidence::FILE)).unwrap();
    assert!(kline_evidence::recover_optional(&not_directory, &identity()).is_err());
}

#[tokio::test]
async fn retry_then_success_terminal_budget_and_report_matrix() {
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let mut waits = vec![];
    let mut source = Fake::new(vec![status(503, &[]), success(RAW)]);
    let mut elapsed = 0;
    let report = run(
        &root,
        &identity(),
        &mut source,
        || Ok(DAY),
        || {
            elapsed += 1;
            if elapsed >= 5 {
                30_000
            } else {
                0
            }
        },
        |ms| {
            waits.push(ms);
            async {}
        },
    )
    .await
    .unwrap();
    assert_eq!(report.reason, Reason::Complete);
    assert_eq!(report.requests, 2);
    assert_eq!(waits, vec![250]);
    assert_eq!(report.response_bytes, RAW.len() as u64);
}

#[tokio::test]
async fn real_report_exit_and_json_matrix() {
    for (reason, exit) in [
        (Reason::Complete, 0),
        (Reason::Existing, 0),
        (Reason::Deferred, 75),
        (Reason::RetriesExhausted, 75),
        (Reason::Budget, 75),
        (Reason::Failed, 1),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        if reason == Reason::Existing {
            kline_evidence::persist(&root, identity(), RAW, DAY).unwrap();
        }
        let outcomes = match reason {
            Reason::Complete => vec![success(RAW)],
            Reason::Deferred => vec![status(429, &["0"])],
            Reason::RetriesExhausted => (0..4).map(|_| status(500, &[])).collect(),
            Reason::Failed => vec![status(451, &[])],
            _ => vec![],
        };
        let mut source = Fake::new(outcomes);
        let report = run(
            &root,
            &identity(),
            &mut source,
            || Ok(DAY),
            || if reason == Reason::Budget { 30_000 } else { 0 },
            |_| async {},
        )
        .await
        .unwrap();
        assert_eq!(report.reason, reason);
        assert_eq!(report.exit_code(), exit);
        let json = report.json(&identity());
        assert!(json["failure"].is_null());
        if reason == Reason::Deferred {
            assert_eq!(json["retry_after_seconds"], 0);
        } else {
            assert!(json["retry_after_seconds"].is_null());
        }
        assert_eq!(json["config_sha256"], mvp::config_hex());
        assert_eq!(json.as_object().unwrap().len(), 12);
    }
}

#[tokio::test]
async fn elapsed_regression_at_account_delay_and_next_admission_stops_locally() {
    for (samples, expected_waits) in [
        (vec![10, 9], 0),
        (vec![10, 11, 9], 0),
        (vec![10, 11, 12, 9], 1),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        let outcome = if samples.len() == 2 {
            success(RAW)
        } else {
            status(500, &[])
        };
        let mut source = Fake::new(vec![outcome]);
        let mut samples: VecDeque<_> = samples.into();
        let mut waits = 0;
        let error = run(
            &root,
            &identity(),
            &mut source,
            || Ok(DAY),
            || samples.pop_front().unwrap(),
            |_| {
                waits += 1;
                async {}
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "elapsed clock regressed");
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(waits, expected_waits);
        assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    }
}
