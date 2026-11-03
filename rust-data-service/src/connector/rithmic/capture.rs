//! Bounded, allowlisted Rithmic application evidence capture.
//! Producers only copy into a bounded nonblocking queue; parsing, key loading,
//! redaction, serialization and file work belong to the single process worker.

use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
#[path = "capture/causal_tests.rs"]
mod causal_tests;
#[path = "capture/field_map.rs"]
mod field_map;
#[path = "capture/lifecycle.rs"]
mod lifecycle;
#[path = "capture/owner.rs"]
mod owner;
#[path = "capture/receiver.rs"]
mod receiver;
#[path = "capture/sink.rs"]
mod sink;
#[cfg(test)]
#[path = "capture/tests.rs"]
mod tests;
#[path = "capture/writer.rs"]
mod writer;

#[cfg(test)]
pub(crate) use lifecycle::initialize_process_capture_with;
#[allow(unused_imports)]
pub(crate) use lifecycle::{
    before_action, before_outbound_write, initialize_process_capture, next_connection_id,
    process_capture, shutdown_process_capture,
};
#[cfg(test)]
use sink::format_record;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use sink::{test_owner_writer, TestOwnerWriter, TestWriterState};

const PATH_ENV: &str = "FLUXTRADE_RITHMIC_CAPTURE_PATH";
const KEY_FILE_ENV: &str = "FLUXTRADE_RITHMIC_CAPTURE_KEY_FILE";
const KEY_VERSION_ENV: &str = "FLUXTRADE_RITHMIC_CAPTURE_KEY_VERSION";
const QUEUE_CAPACITY: usize = 256;
const MAX_RECORD_BYTES: usize = 32 * 1024;
const MAX_CAPTURE_PAYLOAD: usize = 256 * 1024;

pub(crate) fn eligible_template(direction: Direction, template_id: Option<i32>) -> bool {
    let Some(template_id) = template_id else {
        return false;
    };
    match direction {
        Direction::Inbound => matches!(
            template_id,
            313 | 315 | 317 | 321 | 331 | 351 | 352 | 403 | 450 | 451 | 3505
        ),
        Direction::Outbound => {
            matches!(template_id, 308 | 312 | 314 | 316 | 320 | 330 | 3504 | 402)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Record {
    direction: Direction,
    template_id: Option<i32>,
    payload_len: usize,
    payload: Option<Vec<u8>>,
    connection_id: u64,
    sequence: u64,
    timestamp_ms: u128,
}

#[derive(Clone)]
pub(crate) struct CaptureHandle {
    sender: SyncSender<Record>,
    counters: CounterHandle,
    enabled: bool,
}

pub(super) struct CaptureKey {
    bytes: Vec<u8>,
    version: String,
}

#[derive(Clone, Default)]
pub(crate) struct CounterHandle {
    attempted: std::sync::Arc<AtomicU64>,
    enqueued: std::sync::Arc<AtomicU64>,
    queue_full: std::sync::Arc<AtomicU64>,
    dropped: std::sync::Arc<AtomicU64>,
    sink_errors: std::sync::Arc<AtomicU64>,
    written: std::sync::Arc<AtomicU64>,
    oversize: std::sync::Arc<AtomicU64>,
    key_unavailable: std::sync::Arc<AtomicU64>,
    excluded_auth_ambiguous: std::sync::Arc<AtomicU64>,
    startup_failures: std::sync::Arc<AtomicU64>,
    owner_conflicts: std::sync::Arc<AtomicU64>,
    owner_namespace_limit: std::sync::Arc<AtomicU64>,
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    writer_unavailable: std::sync::Arc<std::sync::atomic::AtomicBool>,
    admission_closed: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl CounterHandle {
    fn close_admission(&self) {
        *self
            .admission_closed
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
    }

    fn report(&self) {
        tracing::info!(
            attempted = self.attempted.load(Ordering::Relaxed),
            enqueued = self.enqueued.load(Ordering::Relaxed),
            written = self.written.load(Ordering::Relaxed),
            queue_full = self.queue_full.load(Ordering::Relaxed),
            dropped = self.dropped.load(Ordering::Relaxed),
            sink_errors = self.sink_errors.load(Ordering::Relaxed),
            oversize = self.oversize.load(Ordering::Relaxed),
            key_unavailable = self.key_unavailable.load(Ordering::Relaxed),
            excluded_auth_ambiguous = self.excluded_auth_ambiguous.load(Ordering::Relaxed),
            startup_failures = self.startup_failures.load(Ordering::Relaxed),
            owner_conflicts = self.owner_conflicts.load(Ordering::Relaxed),
            owner_namespace_limit = self.owner_namespace_limit.load(Ordering::Relaxed),
            stopped = self.stopped.load(Ordering::Relaxed),
            writer_unavailable = self.writer_unavailable.load(Ordering::Relaxed),
            "Rithmic capture counters"
        );
    }

    #[cfg(test)]
    pub(crate) fn health_counts(&self) -> (u64, u64, u64, u64, bool) {
        (
            self.enqueued.load(Ordering::Relaxed),
            self.written.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
            self.sink_errors.load(Ordering::Relaxed),
            self.writer_unavailable.load(Ordering::Relaxed),
        )
    }
}

static NEXT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
