use super::*;
use std::ops::Deref;

pub(super) fn close_and_discard(receiver: &mpsc::Receiver<Record>, counters: &CounterHandle) {
    let mut admission = counters
        .admission_closed
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    *admission = true;
    for _ in receiver.try_iter() {
        counters.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) struct ReceiverGuard {
    receiver: mpsc::Receiver<Record>,
    counters: CounterHandle,
}

impl ReceiverGuard {
    pub(super) fn new(receiver: mpsc::Receiver<Record>, counters: CounterHandle) -> Self {
        Self { receiver, counters }
    }
}

impl Deref for ReceiverGuard {
    type Target = mpsc::Receiver<Record>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl Drop for ReceiverGuard {
    fn drop(&mut self) {
        close_and_discard(&self.receiver, &self.counters);
    }
}
