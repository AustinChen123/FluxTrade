use super::*;

#[test]
fn missing_capture_root_startup_entry_settles_admitted_records() {
    let (sender, receiver) = mpsc::sync_channel(2);
    let counters = CounterHandle::default();
    let capture = CaptureHandle::from_sender(sender, counters.clone());
    capture.attempt(
        Direction::Outbound,
        Some(312),
        9,
        1,
        b"accepted-before-startup",
    );

    writer_loop_with_root(receiver, counters.clone(), None);

    assert_eq!(counters.enqueued.load(Ordering::Relaxed), 1);
    assert_eq!(counters.written.load(Ordering::Relaxed), 0);
    assert_eq!(counters.dropped.load(Ordering::Relaxed), 1);
    assert_eq!(counters.startup_failures.load(Ordering::Relaxed), 1);
    assert_eq!(counters.sink_errors.load(Ordering::Relaxed), 0);
    assert!(counters.writer_unavailable.load(Ordering::Relaxed));
    assert!(*counters.admission_closed.lock().unwrap());
}
