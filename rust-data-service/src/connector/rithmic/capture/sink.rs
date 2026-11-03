use super::receiver::{close_and_discard, ReceiverGuard};
use super::writer::OwnerWriter;
use super::*;

#[cfg(test)]
#[path = "sink_tests.rs"]
mod terminal_tests;

pub(super) fn writer_loop(receiver: mpsc::Receiver<Record>, counters: CounterHandle) {
    let root = std::env::var_os(PATH_ENV).map(PathBuf::from);
    writer_loop_with_root(receiver, counters, root);
}

fn writer_loop_with_root(
    receiver: mpsc::Receiver<Record>,
    counters: CounterHandle,
    root: Option<PathBuf>,
) {
    // Protect accepted records before any startup branch can return or unwind.
    let receiver = ReceiverGuard::new(receiver, counters.clone());
    let Some(root) = root else {
        fail_startup(&receiver, &counters);
        tracing::warn!("Rithmic raw capture unavailable: capture path is not configured");
        return;
    };
    let mut writer = match OwnerWriter::open(&root) {
        Ok(writer) => writer,
        Err(writer::OwnerOpenError::Conflict) => {
            counters.owner_conflicts.fetch_add(1, Ordering::Relaxed);
            counters.writer_unavailable.store(true, Ordering::Relaxed);
            close_and_discard(&receiver, &counters);
            tracing::warn!("Rithmic raw capture unavailable: destination is owned");
            return;
        }
        Err(writer::OwnerOpenError::NamespaceLimit) => {
            counters
                .owner_namespace_limit
                .fetch_add(1, Ordering::Relaxed);
            counters.writer_unavailable.store(true, Ordering::Relaxed);
            close_and_discard(&receiver, &counters);
            tracing::warn!("Rithmic raw capture unavailable: owner namespace limit reached");
            return;
        }
        Err(writer::OwnerOpenError::Unavailable) => {
            counters.startup_failures.fetch_add(1, Ordering::Relaxed);
            counters.writer_unavailable.store(true, Ordering::Relaxed);
            close_and_discard(&receiver, &counters);
            tracing::warn!("Rithmic raw capture unavailable: destination setup failed");
            return;
        }
    };
    let key = CaptureKey::load();
    if key.is_none() {
        counters.key_unavailable.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("Rithmic raw capture content disabled: capture key unavailable");
    }
    consume_records(receiver, &mut writer, &counters, key.as_ref());
}

fn fail_startup(receiver: &ReceiverGuard, counters: &CounterHandle) {
    counters.startup_failures.fetch_add(1, Ordering::Relaxed);
    counters.writer_unavailable.store(true, Ordering::Relaxed);
    close_and_discard(receiver, counters);
}

fn consume_records(
    receiver: ReceiverGuard,
    writer: &mut OwnerWriter,
    counters: &CounterHandle,
    key: Option<&CaptureKey>,
) {
    let mut last_report = std::time::Instant::now();
    loop {
        let record = match receiver.recv_timeout(std::time::Duration::from_secs(60)) {
            Ok(record) => record,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                counters.report();
                if counters.stopped.load(Ordering::Relaxed) {
                    close_and_discard(&receiver, counters);
                    return;
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                counters.writer_unavailable.store(true, Ordering::Relaxed);
                counters.stopped.store(true, Ordering::Relaxed);
                close_and_discard(&receiver, counters);
                counters.report();
                return;
            }
        };
        let formatted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (bytes, status) = format_record(record, key);
            let write_failed =
                bytes.len() > MAX_RECORD_BYTES || writer.write_record(&bytes).is_err();
            (status, write_failed)
        }));
        let (status, write_failed) = match formatted {
            Ok(result) => result,
            Err(_) => {
                counters.sink_errors.fetch_add(1, Ordering::Relaxed);
                counters.dropped.fetch_add(1, Ordering::Relaxed);
                counters.writer_unavailable.store(true, Ordering::Relaxed);
                close_and_discard(&receiver, counters);
                tracing::warn!("Rithmic raw capture writer panicked; capture is unavailable");
                return;
            }
        };
        if status == "auth_excluded" {
            counters
                .excluded_auth_ambiguous
                .fetch_add(1, Ordering::Relaxed);
        }
        if write_failed {
            counters.sink_errors.fetch_add(1, Ordering::Relaxed);
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            counters.writer_unavailable.store(true, Ordering::Relaxed);
            close_and_discard(&receiver, counters);
            tracing::warn!("Rithmic raw capture writer failed; capture is unavailable");
            return;
        }
        counters.written.fetch_add(1, Ordering::Relaxed);
        if last_report.elapsed() >= std::time::Duration::from_secs(60) {
            counters.report();
            last_report = std::time::Instant::now();
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum TestWriterState {
    Healthy,
    Blocked,
    Dead,
    WriteFailing,
}

#[cfg(test)]
pub(crate) struct TestOwnerWriter {
    pub(crate) capture: CaptureHandle,
    release: Option<mpsc::SyncSender<()>>,
    worker: Option<thread::JoinHandle<()>>,
    pub(crate) jsonl_path: PathBuf,
}

#[cfg(test)]
impl TestOwnerWriter {
    pub(crate) fn release_without_join(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        self.worker.take();
    }

    pub(crate) fn release_and_join(mut self) -> (PathBuf, (u64, u64, u64, u64, bool)) {
        let counters = self.capture.counters.clone();
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        drop(self.capture);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        (self.jsonl_path, counters.health_counts())
    }
}

#[cfg(test)]
pub(crate) fn test_owner_writer(
    directory: &std::path::Path,
    state: TestWriterState,
) -> TestOwnerWriter {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::mpsc::Receiver;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    let counters = CounterHandle::default();
    let worker_counters = counters.clone();
    let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
    let (release_sender, release_receiver) = mpsc::sync_channel(0);
    let root = directory.to_path_buf();
    let worker = thread::spawn(move || {
        let mut writer = OwnerWriter::open(&root)
            .unwrap_or_else(|error| panic!("owner open failed at {}: {error:?}", root.display()));
        if matches!(state, TestWriterState::WriteFailing) {
            writer.fail_next_write_for_test();
        }
        let path = writer.path().to_path_buf();
        if matches!(state, TestWriterState::Dead) {
            drop(receiver);
            worker_counters
                .writer_unavailable
                .store(true, Ordering::Relaxed);
            ready_sender.send(path).unwrap();
            return;
        }
        ready_sender.send(path).unwrap();
        if matches!(state, TestWriterState::Blocked) {
            release_receiver.recv().unwrap();
        }
        let receiver: Receiver<Record> = receiver;
        let key = CaptureKey {
            bytes: vec![0x42; 32],
            version: "test".to_string(),
        };
        consume_records(
            ReceiverGuard::new(receiver, worker_counters.clone()),
            &mut writer,
            &worker_counters,
            Some(&key),
        );
    });
    let jsonl_path = ready_receiver.recv().unwrap();
    let release = matches!(state, TestWriterState::Blocked).then_some(release_sender);
    TestOwnerWriter {
        capture: CaptureHandle::from_sender(sender, counters),
        release,
        worker: Some(worker),
        jsonl_path,
    }
}

pub(super) fn format_record(record: Record, key: Option<&CaptureKey>) -> (Vec<u8>, &'static str) {
    let template = record
        .template_id
        .map_or_else(|| "null".to_string(), |value| value.to_string());
    let content = if let (Some(key), Some(payload), Some(template_id)) =
        (key, record.payload.as_deref(), record.template_id)
    {
        field_map::capture_fields(template_id, payload, key)
    } else {
        field_map::FieldCapture::metadata_only()
    };
    let status = content.decode_status;
    (format!(
        "{{\"schema_version\":1,\"timestamp_ms\":{},\"process_id\":{},\"connection_id\":{},\"sequence\":{},\"direction\":\"{}\",\"template_id\":{},\"payload_len\":{},\"capture_status\":\"{}\",\"key_version\":{},\"content\":{{\"fields\":{},\"omitted_field_count\":{},\"decode_status\":\"{}\"}}}}\n",
        record.timestamp_ms,
        std::process::id(),
        record.connection_id,
        record.sequence,
        record.direction.as_str(),
        template,
        record.payload_len,
        if key.is_some() && status == "ok" { "allowlisted" } else { "metadata_only" },
        key.map_or_else(|| "null".to_string(), |k| format!("\"{}\"", k.version)),
        serde_json::to_string(&content.fields).unwrap_or_else(|_| "[]".to_string()),
        content.omitted_field_count,
        content.decode_status,
    )
    .into_bytes(), status)
}

impl CaptureKey {
    fn load() -> Option<Self> {
        let path = std::env::var_os(KEY_FILE_ENV).map(PathBuf::from);
        let version = std::env::var(KEY_VERSION_ENV).ok();
        Self::load_from(path.as_deref(), version.as_deref())
    }

    pub(super) fn load_from(path: Option<&std::path::Path>, version: Option<&str>) -> Option<Self> {
        use std::os::unix::fs::MetadataExt;
        let path = path?;
        let link_metadata = std::fs::symlink_metadata(path).ok()?;
        if link_metadata.file_type().is_symlink()
            || !link_metadata.is_file()
            || link_metadata.mode() & 0o077 != 0
            || link_metadata.len() > 4096
        {
            return None;
        }
        let bytes = std::fs::read(path).ok()?;
        if bytes.len() < 32 || bytes.len() > 4096 {
            return None;
        }
        let version = version.unwrap_or("v1");
        if version.is_empty()
            || version.len() > 24
            || !version
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return None;
        }
        Some(Self {
            bytes,
            version: version.to_string(),
        })
    }
}
