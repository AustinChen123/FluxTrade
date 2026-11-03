use super::*;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static PROCESS_CAPTURE: OnceLock<CaptureHandle> = OnceLock::new();
static UNINITIALIZED_CAPTURE: OnceLock<CaptureHandle> = OnceLock::new();

/// Start the fixed process owner at extension/bootstrap initialization, before
/// any connection or reconnect executor can first access it.
pub(crate) fn initialize_process_capture() {
    initialize_process_capture_with(CaptureHandle::start);
}

pub(crate) fn initialize_process_capture_with(start: impl FnOnce() -> CaptureHandle) {
    let _ = PROCESS_CAPTURE.set(start());
}

pub(crate) fn process_capture() -> &'static CaptureHandle {
    PROCESS_CAPTURE
        .get()
        .unwrap_or_else(|| UNINITIALIZED_CAPTURE.get_or_init(CaptureHandle::unavailable))
}

pub(crate) fn shutdown_process_capture() {
    if let Some(capture) = PROCESS_CAPTURE.get() {
        capture.shutdown();
    }
}

pub(crate) fn next_connection_id() -> u64 {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn before_action<T>(
    capture: &CaptureHandle,
    direction: Direction,
    template_id: Option<i32>,
    payload_len: usize,
    connection_id: u64,
    payload: &[u8],
    action: impl FnOnce() -> T,
) -> T {
    if eligible_template(direction, template_id) {
        capture.attempt(direction, template_id, payload_len, connection_id, payload);
    }
    action()
}

pub(crate) async fn before_outbound_write<T, F, Fut>(
    capture: &CaptureHandle,
    template_id: Option<i32>,
    payload: Vec<u8>,
    connection_id: u64,
    write: F,
) -> T
where
    F: FnOnce(Vec<u8>) -> Fut,
    Fut: Future<Output = T>,
{
    capture.attempt(
        Direction::Outbound,
        template_id,
        payload.len(),
        connection_id,
        &payload,
    );
    write(payload).await
}
