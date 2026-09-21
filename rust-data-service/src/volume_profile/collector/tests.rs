use super::*;
use crate::volume_profile::{checkpoint::Identity, compressed_page, work_policy, Window};
use std::{collections::VecDeque, fs::File};

struct Fake(VecDeque<Outcome>, Vec<Request>);
#[async_trait]
impl Source for Fake {
    async fn fetch(&mut self, request: &Request) -> Outcome {
        self.1.push(request.clone());
        self.0.pop_front().expect("unexpected fetch")
    }
}
fn outcome(body: Option<Vec<u8>>, status: u16, failure: Option<Failure>) -> Outcome {
    Outcome {
        disposition: failure.map_or_else(
            || work_policy::http(status, &[]),
            |f| work_policy::failure(f, Some(status)),
        ),
        response_bytes: body.as_ref().map_or(0, |b| b.len() as u64),
        body,
        failure,
    }
}
fn body(id: u64) -> Vec<u8> {
    format!(r#"[{{"a":{id},"p":"1","q":"1","T":100,"f":{id},"l":{id},"m":true,"M":true}}]"#)
        .into_bytes()
}
fn setup(manifest_limit: u64) -> (tempfile::TempDir, Store) {
    let root = tempfile::tempdir().unwrap();
    let window = Window::new(100, 200).unwrap();
    let identity = Identity::new("job".into(), window, "grid".into(), [1; 32]).unwrap();
    let limits = compressed_page::Limits::new(4096, 1024).unwrap();
    let fd = File::open(root.path()).unwrap();
    let store = Store::open(&fd, identity, limits, manifest_limit).unwrap();
    (root, store)
}
async fn run(
    store: &Store,
    items: Vec<Outcome>,
    requests: u64,
    bytes: u64,
    elapsed: u64,
) -> (Result<Report>, Fake, Vec<u64>) {
    let mut fake = Fake(items.into(), vec![]);
    let mut budget = Budget::new(work_policy::Limits {
        requests,
        response_bytes: bytes,
        elapsed_ms: 100,
    })
    .unwrap();
    let mut waits = vec![];
    let schedule = RetrySchedule::new(2, 3, 10).unwrap();
    let wait = |ms| {
        waits.push(ms);
        std::future::ready(())
    };
    let report = round(store, &mut fake, &mut budget, schedule, wait, || elapsed).await;
    if !budget.is_fresh() {
        let count = fake.1.len();
        let no_wait = |_| async { panic!("wait") };
        let no_clock = || panic!("clock");
        let result = round(store, &mut fake, &mut budget, schedule, no_wait, no_clock).await;
        assert_eq!(
            result.unwrap_err().to_string(),
            "round requires fresh budget"
        );
        assert_eq!(fake.1.len(), count);
    }
    (report, fake, waits)
}
fn success(raw: Vec<u8>) -> Outcome {
    outcome(Some(raw), 200, None)
}

#[tokio::test]
async fn ack_unknown_checkpoint_requires_confirmation_before_round_progress() {
    use crate::volume_profile::{directory::Phase, store::Target};
    for terminal in [false, true] {
        let (_root, store) = setup(4096);
        store
            .append(0, &body(0), CheckpointProgress::Next(1))
            .unwrap();
        let (raw, progress) = if terminal {
            (b"[]".to_vec(), CheckpointProgress::EmptyPage)
        } else {
            (body(1), CheckpointProgress::Next(2))
        };
        let mut confirmations = 0;
        let result = store.append_with(1, &raw, progress, |target, phase| {
            if (target, phase) == (Target::Manifest, Phase::Renamed) {
                store.fail_next_recovery_confirmation();
                anyhow::bail!("manifest ACK unknown");
            }
            if (target, phase) == (Target::Confirmation, Phase::BeforeDirectorySync) {
                confirmations += 1;
                anyhow::bail!("confirmation failed");
            }
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(confirmations, 1);
        let (r, fake, waits) = run(&store, vec![], 9, 9999, 0).await;
        assert_eq!(
            r.unwrap_err().to_string(),
            "recovery confirmation unavailable"
        );
        assert!(fake.1.is_empty());
        assert!(waits.is_empty());
        let items = if terminal {
            vec![]
        } else {
            vec![success(body(2)), success(b"[]".to_vec())]
        };
        let (r, fake, waits) = run(&store, items, 9, 9999, 0).await;
        let report = r.unwrap();
        assert_eq!(report.reason, Reason::Complete);
        assert_eq!(report.requests, if terminal { 0 } else { 2 });
        let expected = if terminal {
            vec![]
        } else {
            vec![Request::Next { from_id: 2 }, Request::Next { from_id: 3 }]
        };
        assert_eq!(fake.1, expected);
        assert!(waits.is_empty());
        let trades = store.recover().unwrap().1.trades;
        assert_eq!(
            trades.iter().map(|t| t.id).collect::<Vec<_>>(),
            if terminal { vec![0] } else { vec![0, 1, 2] }
        );
    }
}

#[tokio::test]
async fn checkpoint_budget_resume_terminal_and_append_failure() {
    for (requests, bytes) in [(1, 9999), (9, 1)] {
        let (_root, store) = setup(4096);
        let (r, f, _) = run(&store, vec![success(body(0))], requests, bytes, 0).await;
        assert_eq!(r.unwrap().reason, Reason::Budget);
        assert_eq!(f.1.len(), 1);
        let items = vec![success(body(1)), success(b"[]".to_vec())];
        let (r, f, _) = run(&store, items, 9, 9999, 0).await;
        assert_eq!(r.unwrap().reason, Reason::Complete);
        assert_eq!(
            f.1,
            vec![Request::Next { from_id: 1 }, Request::Next { from_id: 2 }]
        );
        let (_, recovered) = store.recover().unwrap();
        assert_eq!(
            recovered.trades.iter().map(|t| t.id).collect::<Vec<_>>(),
            vec![0, 1]
        );
        let (r, f, _) = run(&store, vec![], 9, 9999, 0).await;
        assert_eq!(r.unwrap().reason, Reason::Complete);
        assert!(f.1.is_empty());
    }
    let (_root, store) = setup(4096);
    let (r, f, _) = run(&store, vec![], 1, 1, 100).await;
    assert_eq!(r.unwrap().reason, Reason::Budget);
    assert!(f.1.is_empty());
    let (r, _, _) = run(&store, vec![success(b"[]".to_vec())], 1, 1, 0).await;
    assert_eq!(r.unwrap().reason, Reason::Complete);
    assert!(store.recover().unwrap().1.pages.is_stopped());
    let (_root, broken) = setup(1);
    assert!(run(&broken, vec![success(body(0))], 9, 9999, 0)
        .await
        .0
        .is_err());
    assert!(broken.recover().unwrap().0.entries().is_empty());
}

#[tokio::test]
async fn retry_delays_reset_and_exhaustion() {
    let (_root, store) = setup(4096);
    let retry = || outcome(None, 500, None);
    let terminal = success(b"[]".to_vec());
    let items = vec![retry(), retry(), success(body(0)), retry(), terminal];
    let (r, f, waits) = run(&store, items, 9, 9999, 0).await;
    assert_eq!(r.unwrap().reason, Reason::Complete);
    assert_eq!(waits, vec![3, 6, 3]);
    assert_eq!(f.1.len(), 5);
    let (_root, store) = setup(4096);
    let (r, f, waits) = run(&store, vec![retry(), retry(), retry()], 9, 9999, 0).await;
    assert_eq!(
        r.unwrap().reason,
        Reason::RetriesExhausted(retry().disposition, None)
    );
    assert_eq!(f.1.len(), 3);
    assert_eq!(waits, vec![3, 6]);
    let (r, _, waits) = run(&store, vec![retry()], 1, 9999, 0).await;
    assert_eq!(r.unwrap().reason, Reason::Budget);
    assert!(waits.is_empty());
    let (r, f, waits) = run(&store, vec![retry()], 9, 9999, 99).await;
    assert_eq!(r.unwrap().reason, Reason::Budget);
    assert_eq!(f.1.len(), 1);
    assert!(waits.is_empty());
    assert!(store.recover().unwrap().0.entries().is_empty());
}

#[tokio::test]
async fn deferred_failed_and_incoherent_outcomes() {
    for seconds in [0, 7] {
        let (_root, store) = setup(4096);
        let mut response = outcome(None, 429, None);
        response.disposition.action = Action::RetryAfter(seconds);
        response.response_bytes = 4;
        let (r, f, waits) = run(&store, vec![response], 1, 1, 0).await;
        let report = r.unwrap();
        assert_eq!(report.reason, Reason::Deferred(seconds));
        assert_eq!((report.requests, report.response_bytes), (1, 4));
        assert_eq!(f.1.len(), 1);
        assert!(waits.is_empty());
        assert!(store.recover().unwrap().0.entries().is_empty());
    }
    for failure in [None, Some(Failure::Protocol)] {
        let (_root, store) = setup(4096);
        let response = outcome(None, 451, failure);
        let expected = Reason::Failed(response.disposition, failure);
        let (r, _, waits) = run(&store, vec![response], 1, 1, 0).await;
        assert_eq!(r.unwrap().reason, expected);
        assert!(waits.is_empty());
    }
    for mut bad in [
        success(b"[]".to_vec()),
        outcome(None, 200, None),
        outcome(Some(body(0)), 500, None),
    ] {
        if bad.body.as_deref() == Some(b"[]") {
            bad.failure = Some(Failure::Protocol);
        }
        let (_root, store) = setup(4096);
        assert!(run(&store, vec![bad], 9, 9999, 0).await.0.is_err());
        assert!(store.recover().unwrap().0.entries().is_empty());
    }
}
