use super::*;


fn duplicate_capture_handle(capture: &CaptureHandle) -> CaptureHandle {
    CaptureHandle {
        sender: capture.sender.clone(),
        counters: capture.counters.clone(),
        enabled: capture.enabled,
    }
}

#[path = "process_restart_tests.rs"]
mod process_restart_tests;

#[test]
fn terminal_gate_accounts_queued_and_racing_records_before_receiver_drop() {
    let (sender, receiver) = mpsc::sync_channel(2);
    let counters = CounterHandle::default();
    let capture = CaptureHandle::from_sender(sender, counters.clone());
    capture.attempt(Direction::Outbound, Some(312), 1, 1, b"queued");
    let gate = counters.admission_closed.lock().unwrap();
    let racing = std::thread::spawn({
        let capture = capture;
        move || capture.attempt(Direction::Outbound, Some(312), 1, 1, b"racing")
    });
    racing.join().unwrap();
    drop(gate);
    close_and_discard(&receiver, &counters);
    drop(receiver);
    assert_eq!(counters.enqueued.load(Ordering::Relaxed), 1);
    assert_eq!(counters.written.load(Ordering::Relaxed), 0);
    assert_eq!(counters.dropped.load(Ordering::Relaxed), 2);
    assert_eq!(counters.sink_errors.load(Ordering::Relaxed), 0);
    assert!(*counters.admission_closed.lock().unwrap());
}

#[test]
fn missing_capture_path_settles_admitted_records_before_receiver_drop() {
    let (sender, receiver) = mpsc::sync_channel(2);
    let counters = CounterHandle::default();
    let capture = CaptureHandle::from_sender(sender, counters.clone());
    capture.attempt(
        Direction::Outbound,
        Some(312),
        7,
        1,
        b"accepted-before-startup-failure",
    );

    let receiver = ReceiverGuard::new(receiver, counters.clone());
    // Exercise the production missing-root startup cleanup while retaining a live receiver.
    fail_startup(&receiver, &counters);
    capture.attempt(Direction::Outbound, Some(312), 7, 2, b"late-before-drop");
    close_and_discard(&receiver, &counters);
    drop(receiver);

    assert_eq!(counters.enqueued.load(Ordering::Relaxed), 1);
    assert_eq!(counters.written.load(Ordering::Relaxed), 0);
    assert_eq!(counters.startup_failures.load(Ordering::Relaxed), 1);
    assert!(*counters.admission_closed.lock().unwrap());

    assert_eq!(counters.enqueued.load(Ordering::Relaxed), 1);
    assert_eq!(counters.dropped.load(Ordering::Relaxed), 2);
    assert_eq!(counters.sink_errors.load(Ordering::Relaxed), 1);
}
