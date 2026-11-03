use super::sink::writer_loop;
use super::*;

impl CaptureHandle {
    pub(crate) fn start() -> Self {
        let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
        let counters = CounterHandle::default();
        let worker_counters = counters.clone();
        let worker_status = counters.clone();
        let worker = thread::Builder::new()
            .name("rithmic-raw-capture".to_string())
            .spawn(move || {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    writer_loop(receiver, worker_counters)
                }))
                .is_err()
                {
                    worker_status
                        .writer_unavailable
                        .store(true, Ordering::Relaxed);
                }
            });
        let failed = worker.is_err();
        if failed {
            counters.startup_failures.fetch_add(1, Ordering::Relaxed);
            counters.writer_unavailable.store(true, Ordering::Relaxed);
        }
        let health = counters.clone();
        let health_counters = counters.clone();
        let health = thread::Builder::new()
            .name("rithmic-capture-health".to_string())
            .spawn(move || loop {
                thread::sleep(std::time::Duration::from_secs(60));
                health.report();
            });
        if health.is_err() {
            counters.startup_failures.fetch_add(1, Ordering::Relaxed);
            health_counters
                .writer_unavailable
                .store(true, Ordering::Relaxed);
        }
        Self::from_sender(sender, counters)
    }

    pub(super) fn unavailable() -> Self {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        let counters = CounterHandle::default();
        counters.writer_unavailable.store(true, Ordering::Relaxed);
        Self {
            sender,
            counters,
            enabled: false,
        }
    }

    pub(crate) fn from_sender(sender: SyncSender<Record>, counters: CounterHandle) -> Self {
        Self {
            sender,
            counters,
            enabled: true,
        }
    }

    #[cfg(test)]
    pub(crate) fn disabled(sender: SyncSender<Record>, counters: CounterHandle) -> Self {
        Self {
            sender,
            counters,
            enabled: false,
        }
    }

    pub(crate) fn attempt(
        &self,
        direction: Direction,
        template_id: Option<i32>,
        payload_len: usize,
        connection_id: u64,
        payload: &[u8],
    ) {
        if !self.enabled {
            return;
        }
        self.counters.attempted.fetch_add(1, Ordering::Relaxed);
        let Ok(mut admission) = self.counters.admission_closed.try_lock() else {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if *admission {
            self.counters.sink_errors.fetch_add(1, Ordering::Relaxed);
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis());
        let record = Record {
            direction,
            template_id,
            payload_len: payload_len.min(u32::MAX as usize),
            payload: (payload.len() <= MAX_CAPTURE_PAYLOAD).then(|| payload.to_vec()),
            connection_id,
            sequence: NEXT_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            timestamp_ms,
        };
        if payload.len() > MAX_CAPTURE_PAYLOAD {
            self.counters.oversize.fetch_add(1, Ordering::Relaxed);
        }
        match self.sender.try_send(record) {
            Ok(()) => {
                self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) => {
                self.counters.queue_full.fetch_add(1, Ordering::Relaxed);
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                *admission = false;
                self.counters.sink_errors.fetch_add(1, Ordering::Relaxed);
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn note_oversize(&self) {
        if self.enabled {
            self.counters.attempted.fetch_add(1, Ordering::Relaxed);
            self.counters.oversize.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn note_auth_exclusion(&self) {
        if self.enabled {
            self.counters
                .excluded_auth_ambiguous
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn shutdown(&self) {
        self.counters.stopped.store(true, Ordering::Relaxed);
        self.counters.report();
    }

    #[cfg(any(feature = "rithmic", test))]
    #[allow(dead_code)] // Used by Rithmic transport and unit-test assertions.
    pub(crate) fn counts(&self) -> (u64, u64, u64) {
        (
            self.counters.attempted.load(Ordering::Relaxed),
            self.counters.enqueued.load(Ordering::Relaxed),
            self.counters.queue_full.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub(crate) fn health_counts(&self) -> (u64, u64, u64, u64, bool) {
        self.counters.health_counts()
    }
}
