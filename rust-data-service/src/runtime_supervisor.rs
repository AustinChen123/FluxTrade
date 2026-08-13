use std::future::Future;
use tokio::task::JoinSet;
use tracing::{error, info, warn, Level};

pub(crate) type SupervisedTask = (TaskId, anyhow::Result<()>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskId {
    Watchdog,
    Publisher,
    EventLoop,
    Connector(String),
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Watchdog => write!(f, "watchdog"),
            Self::Publisher => write!(f, "publisher"),
            Self::EventLoop => write!(f, "event-loop"),
            Self::Connector(name) => write!(f, "connector:{name}"),
        }
    }
}

pub(crate) fn initialize_process_diagnostics() {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(Level::DEBUG)
        .init();
    install_sanitized_panic_hook();
}

fn install_sanitized_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let (source_file, source_line, source_column) =
            info.location().map_or(("unknown", 0, 0), |location| {
                (
                    std::path::Path::new(location.file())
                        .file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .unwrap_or("unknown"),
                    location.line(),
                    location.column(),
                )
            });
        warn!(
            component = %"runtime", task = %"unknown", operation = %"panic",
            stage = %"panic_hook", template_id = %"unknown", payload_len = %"unknown",
            stable_error_code = %"panic_observed", disposition = %"continue_unwind",
            state_effect = %"unwinding", safe_cause = %"panic payload suppressed",
            source_file = %source_file, source_line, source_column,
            "FluxTrade panic observed"
        );
    }));
}

pub(crate) async fn supervise(join_set: JoinSet<SupervisedTask>) -> anyhow::Result<()> {
    supervise_until(join_set, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}

async fn supervise_until(
    mut join_set: JoinSet<SupervisedTask>,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<()> {
    info!(
        "Supervisor active. Monitoring {} tasks. Press Ctrl+C to shutdown.",
        join_set.len()
    );
    tokio::select! {
        _ = shutdown => {
            info!("Received shutdown signal, stopping all tasks...");
            join_set.shutdown().await;
            Ok(())
        }
        result = join_set.join_next() => {
            match result {
                None => {
                    info!("All supervised tasks have exited");
                    Ok(())
                }
                Some(Ok((task_id, task_result))) => {
                    let error = supervised_task_exit_error(&task_id, task_result);
                    join_set.shutdown().await;
                    Err(error)
                }
                Some(Err(join_error)) => {
                    let error = supervised_join_error(join_error);
                    join_set.shutdown().await;
                    Err(error)
                }
            }
        }
    }
}

fn supervised_task_exit_error(task_id: &TaskId, task_result: anyhow::Result<()>) -> anyhow::Error {
    match task_result {
        Ok(()) => {
            SupervisedFailure::new(task_id, SupervisedFailureKind::UnexpectedExit, None).into()
        }
        Err(error) => {
            SupervisedFailure::new(task_id, SupervisedFailureKind::TaskError, Some(error)).into()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SupervisedFailureKind {
    TaskError,
    UnexpectedExit,
    CancelledJoin,
    PanickedJoin,
    OtherJoin,
}

impl SupervisedFailureKind {
    fn diagnostic(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::TaskError => (
                "task_exit",
                "supervised_task_failed",
                "supervised task failed",
            ),
            Self::UnexpectedExit => (
                "task_exit",
                "supervised_task_exited",
                "supervised task exited unexpectedly",
            ),
            Self::CancelledJoin => (
                "task_join",
                "supervised_task_cancelled",
                "supervised task was cancelled",
            ),
            Self::PanickedJoin => (
                "task_join",
                "supervised_task_panicked",
                "supervised task panicked",
            ),
            Self::OtherJoin => (
                "task_join",
                "supervised_task_join_failed",
                "supervised task join failed",
            ),
        }
    }
}

#[derive(Debug)]
struct SupervisedFailure {
    task: String,
    kind: SupervisedFailureKind,
    source: Option<anyhow::Error>,
}

impl SupervisedFailure {
    fn new(task: &TaskId, kind: SupervisedFailureKind, source: Option<anyhow::Error>) -> Self {
        Self {
            task: task.to_string(),
            kind,
            source,
        }
    }
}

impl std::fmt::Display for SupervisedFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.kind.diagnostic().2)
    }
}

impl std::error::Error for SupervisedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
}

fn supervised_join_error(error: tokio::task::JoinError) -> anyhow::Error {
    let kind = classify_join_failure(error.is_cancelled(), error.is_panic());
    SupervisedFailure {
        task: "unknown".to_string(),
        kind,
        source: None,
    }
    .into()
}

fn classify_join_failure(cancelled: bool, panicked: bool) -> SupervisedFailureKind {
    if cancelled {
        SupervisedFailureKind::CancelledJoin
    } else if panicked {
        SupervisedFailureKind::PanickedJoin
    } else {
        SupervisedFailureKind::OtherJoin
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TerminalDiagnostic {
    component: &'static str,
    task: String,
    operation: &'static str,
    stage: &'static str,
    template_id: String,
    payload_len: String,
    stable_error_code: &'static str,
    disposition: &'static str,
    state_effect: &'static str,
    safe_cause: &'static str,
}

fn terminal_diagnostic(error: &anyhow::Error) -> TerminalDiagnostic {
    let supervisor = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<SupervisedFailure>());
    let task = supervisor.map_or_else(|| "unknown".to_string(), |failure| failure.task.clone());
    if let Some(failure) = crate::connector::terminal::classify(error) {
        return TerminalDiagnostic {
            component: failure.component,
            task,
            operation: failure.operation,
            stage: failure.stage,
            template_id: failure.template_id,
            payload_len: failure.payload_len,
            stable_error_code: failure.stable_error_code,
            disposition: failure.disposition,
            state_effect: failure.state_effect,
            safe_cause: failure.safe_cause,
        };
    }
    let (stage, stable_error_code, safe_cause) = supervisor
        .map(|failure| failure.kind.diagnostic())
        .unwrap_or((
            "unknown",
            "terminal_failure",
            "terminal failure details unavailable",
        ));
    TerminalDiagnostic {
        component: "unknown",
        task,
        operation: "unknown",
        stage,
        template_id: "unknown".to_string(),
        payload_len: "unknown".to_string(),
        stable_error_code,
        disposition: "fatal_service_exit",
        state_effect: "process_exit",
        safe_cause,
    }
}

pub(crate) fn report_terminal_failure(error: &anyhow::Error) {
    let diagnostic = terminal_diagnostic(error);
    error!(
        component = %diagnostic.component, task = %diagnostic.task,
        operation = %diagnostic.operation, stage = %diagnostic.stage,
        template_id = %diagnostic.template_id, payload_len = %diagnostic.payload_len,
        stable_error_code = %diagnostic.stable_error_code, disposition = %diagnostic.disposition,
        state_effect = %diagnostic.state_effect, safe_cause = %diagnostic.safe_cause,
        "FluxTrade terminal failure"
    );
}

#[cfg(test)]
#[derive(Clone)]
struct CaptureLayer {
    events: std::sync::Arc<std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>>,
}

#[cfg(test)]
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != Level::ERROR {
            return;
        }
        let mut fields = std::collections::BTreeMap::new();
        event.record(&mut FieldVisitor(&mut fields));
        self.events.lock().unwrap().push(fields);
    }
}

#[cfg(test)]
struct FieldVisitor<'a>(&'a mut std::collections::BTreeMap<String, String>);

#[cfg(test)]
impl tracing::field::Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            format!("{value:?}").trim_matches('"').to_string(),
        );
    }
}

#[cfg(test)]
fn capture_error_events(
    operation: impl FnOnce(),
) -> Vec<std::collections::BTreeMap<String, String>> {
    use tracing_subscriber::prelude::*;

    let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer {
        events: std::sync::Arc::clone(&events),
    });
    tracing::subscriber::with_default(subscriber, operation);
    let captured = events.lock().unwrap().clone();
    captured
}

#[cfg(test)]
fn capture_terminal_event(error: &anyhow::Error) -> std::collections::BTreeMap<String, String> {
    let mut captured = capture_error_events(|| report_terminal_failure(error));
    assert_eq!(captured.len(), 1);
    captured.pop().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::pending,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
    };
    use tokio::sync::{mpsc, oneshot};

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    async fn spawn_pending_sibling(join_set: &mut JoinSet<SupervisedTask>) -> Arc<AtomicBool> {
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let (started_tx, started_rx) = oneshot::channel();
        join_set.spawn(async move {
            let _drop_signal = DropSignal(task_stopped);
            started_tx.send(()).unwrap();
            pending::<SupervisedTask>().await
        });
        started_rx.await.unwrap();
        stopped
    }

    fn assert_sibling_stopped_before_return(stopped: &AtomicBool) {
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn clean_task_exit_is_fatal_and_cancels_pending_siblings() {
        let mut join_set = JoinSet::new();
        let stopped = spawn_pending_sibling(&mut join_set).await;
        join_set.spawn(async { (TaskId::EventLoop, Ok(())) });

        let error = supervise_until(join_set, pending()).await.unwrap_err();

        assert_eq!(terminal_diagnostic(&error).task, "event-loop");
        assert_eq!(
            terminal_diagnostic(&error).stable_error_code,
            "supervised_task_exited"
        );
        assert_sibling_stopped_before_return(&stopped);
    }

    #[tokio::test]
    async fn task_error_preserves_primary_source_and_cancels_pending_siblings() {
        let mut join_set = JoinSet::new();
        let stopped = spawn_pending_sibling(&mut join_set).await;
        join_set.spawn(async {
            (
                TaskId::Connector("synthetic".to_string()),
                Err(anyhow::anyhow!("provider sentinel").context("connector context")),
            )
        });

        let error = supervise_until(join_set, pending()).await.unwrap_err();

        assert_eq!(terminal_diagnostic(&error).task, "connector:synthetic");
        assert_eq!(
            error.chain().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "supervised task failed",
                "connector context",
                "provider sentinel"
            ]
        );
        assert_sibling_stopped_before_return(&stopped);
    }

    #[tokio::test]
    async fn panicked_join_is_sanitized_and_cancels_pending_siblings() {
        let mut join_set = JoinSet::new();
        let stopped = spawn_pending_sibling(&mut join_set).await;
        join_set.spawn(async { panic!("panic payload sentinel") });

        let error = supervise_until(join_set, pending()).await.unwrap_err();

        assert_eq!(
            terminal_diagnostic(&error).stable_error_code,
            "supervised_task_panicked"
        );
        assert_eq!(error.to_string(), "supervised task panicked");
        assert_sibling_stopped_before_return(&stopped);
    }

    #[tokio::test]
    async fn external_shutdown_cancels_tasks_before_clean_return() {
        let mut join_set = JoinSet::new();
        let stopped = spawn_pending_sibling(&mut join_set).await;

        assert!(supervise_until(join_set, async {}).await.is_ok());
        assert_sibling_stopped_before_return(&stopped);
    }

    #[tokio::test]
    async fn empty_task_set_completes_cleanly() {
        assert!(supervise_until(JoinSet::new(), pending()).await.is_ok());
    }

    use crate::normalized_optional_value;
    use futures_util::FutureExt;
    use std::process::Command;

    static ENV_LOCK: Mutex<()> = Mutex::new(());
    const PANIC_CHILD_ENV: &str = "FLUXTRADE_PANIC_HOOK_SUBPROCESS";
    const PANIC_CHILD_VALUE: &str = "fluxtrade-panic-hook-child-v1";
    const PANIC_SENTINEL: &str = "panic-provider-payload-sentinel";

    struct EnvVarGuard {
        name: &'static str,
        prior: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn remove(name: &'static str) -> Self {
            let prior = std::env::var_os(name);
            std::env::remove_var(name);
            Self { name, prior }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.prior.take() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

    fn assert_generic_terminal_fields(fields: &std::collections::BTreeMap<String, String>) {
        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "unknown");
        assert_eq!(fields["task"], "unknown");
        assert_eq!(fields["operation"], "unknown");
        assert_eq!(fields["stage"], "unknown");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "terminal_failure");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "terminal failure details unavailable");
    }

    #[test]
    fn terminal_reporter_emits_ten_independent_fields_and_fixed_message() {
        assert_generic_terminal_fields(&capture_terminal_event(&anyhow::anyhow!("secret")));
    }

    #[test]
    fn panic_hook_subprocess_child() {
        if std::env::var(PANIC_CHILD_ENV).as_deref() != Ok(PANIC_CHILD_VALUE) {
            return;
        }

        initialize_process_diagnostics();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("child runtime should build")
            .block_on(async {
                let panicked = tokio::spawn(async { panic!("{PANIC_SENTINEL}") });
                let error = supervised_join_error(panicked.await.unwrap_err());
                report_terminal_failure(&error);
            });
    }

    #[test]
    fn panic_hook_subprocess_suppresses_payload_and_reports_once() {
        let output = Command::new(std::env::current_exe().expect("test executable should exist"))
            .args([
                "runtime_supervisor::tests::panic_hook_subprocess_child",
                "--exact",
                "--nocapture",
            ])
            .env(PANIC_CHILD_ENV, PANIC_CHILD_VALUE)
            .output()
            .expect("panic-hook child should run");

        for stream in [&output.stdout, &output.stderr] {
            assert!(
                !stream
                    .windows(PANIC_SENTINEL.len())
                    .any(|bytes| bytes == PANIC_SENTINEL.as_bytes()),
                "panic payload leaked in raw subprocess output"
            );
        }
        assert!(output.status.success(), "child failed: {output:?}");
        let stdout = String::from_utf8(output.stdout).expect("stdout should be UTF-8");
        let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
        let combined = format!("{stdout}\n{stderr}");
        assert_eq!(combined.matches("FluxTrade panic observed").count(), 1);
        assert_eq!(combined.matches(" WARN ").count(), 1);
        assert_eq!(combined.matches("FluxTrade terminal failure").count(), 1);
        assert!(!combined.contains("Error:"), "Result termination leaked");

        let warning = combined
            .lines()
            .find(|line| line.contains("FluxTrade panic observed"))
            .expect("panic warning should be present");
        assert!(warning.contains(" WARN "), "wrong severity: {warning}");
        for field in [
            "component=runtime",
            "task=unknown",
            "operation=panic",
            "stage=panic_hook",
            "template_id=unknown",
            "payload_len=unknown",
            "stable_error_code=panic_observed",
            "disposition=continue_unwind",
            "state_effect=unwinding",
            "safe_cause=panic payload suppressed",
            "source_file=runtime_supervisor.rs",
        ] {
            assert!(warning.contains(field), "missing {field}: {warning}");
        }
        let numeric_field = |name: &str| {
            warning
                .split_whitespace()
                .find_map(|field| field.strip_prefix(name))
                .unwrap_or_else(|| panic!("missing {name}: {warning}"))
                .parse::<u32>()
                .unwrap_or_else(|_| panic!("non-numeric {name}: {warning}"))
        };
        assert!(numeric_field("source_line=") > 0);
        assert!(numeric_field("source_column=") > 0);

        let terminal = combined
            .lines()
            .find(|line| line.contains("FluxTrade terminal failure"))
            .expect("terminal event should be present");
        assert!(terminal.contains(" ERROR "), "wrong severity: {terminal}");
    }

    #[test]
    fn terminal_diagnostic_classification_matrix_has_exact_structs() {
        let generic = |task: &str, stage, stable_error_code, safe_cause| TerminalDiagnostic {
            component: "unknown",
            task: task.to_string(),
            operation: "unknown",
            stage,
            template_id: "unknown".to_string(),
            payload_len: "unknown".to_string(),
            stable_error_code,
            disposition: "fatal_service_exit",
            state_effect: "process_exit",
            safe_cause,
        };
        let direct_supervisor = |kind| {
            anyhow::Error::new(SupervisedFailure {
                task: "unknown".to_string(),
                kind,
                source: None,
            })
        };
        let mut cases = vec![
            (
                "generic",
                anyhow::anyhow!("generic-secret"),
                generic(
                    "unknown",
                    "unknown",
                    "terminal_failure",
                    "terminal failure details unavailable",
                ),
            ),
            (
                "TaskError",
                supervised_task_exit_error(
                    &TaskId::Connector("binance".to_string()),
                    Err(anyhow::anyhow!("task-secret")),
                ),
                generic(
                    "connector:binance",
                    "task_exit",
                    "supervised_task_failed",
                    "supervised task failed",
                ),
            ),
            (
                "UnexpectedExit",
                supervised_task_exit_error(&TaskId::EventLoop, Ok(())),
                generic(
                    "event-loop",
                    "task_exit",
                    "supervised_task_exited",
                    "supervised task exited unexpectedly",
                ),
            ),
        ];
        for (name, failure, code, cause) in [
            (
                "BinanceTaskError",
                crate::connector::binance::BinanceTaskFailure::task_error(
                    "trades",
                    anyhow::anyhow!("provider-secret"),
                ),
                "binance_stream_task_failed",
                "Binance stream task failed",
            ),
            (
                "BinanceUnexpectedExit",
                crate::connector::binance::BinanceTaskFailure::unexpected_exit("trades"),
                "binance_stream_task_exited",
                "Binance stream task exited unexpectedly",
            ),
            (
                "BinancePanicked",
                crate::connector::binance::BinanceTaskFailure::panicked("trades"),
                "binance_stream_task_panicked",
                "Binance stream task panicked",
            ),
            (
                "BinanceCancelled",
                crate::connector::binance::BinanceTaskFailure::cancelled("trades"),
                "binance_stream_task_cancelled",
                "Binance stream task was cancelled",
            ),
        ] {
            cases.push((
                name,
                supervised_task_exit_error(
                    &TaskId::Connector("binance".to_string()),
                    Err(failure.into()),
                ),
                TerminalDiagnostic {
                    component: "binance",
                    task: "connector:binance".to_string(),
                    operation: "stream_task",
                    stage: "trades",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: code,
                    disposition: "fatal_service_exit",
                    state_effect: "process_exit",
                    safe_cause: cause,
                },
            ));
        }
        for (name, failure, code, cause) in [
            (
                "BybitTaskError",
                crate::connector::bybit::BybitTaskFailure::task_error(
                    "trades",
                    anyhow::anyhow!("provider-secret"),
                ),
                "bybit_stream_task_failed",
                "Bybit stream task failed",
            ),
            (
                "BybitUnexpectedExit",
                crate::connector::bybit::BybitTaskFailure::unexpected_exit("trades"),
                "bybit_stream_task_exited",
                "Bybit stream task exited unexpectedly",
            ),
            (
                "BybitPanicked",
                crate::connector::bybit::BybitTaskFailure::panicked("trades"),
                "bybit_stream_task_panicked",
                "Bybit stream task panicked",
            ),
            (
                "BybitCancelled",
                crate::connector::bybit::BybitTaskFailure::cancelled("trades"),
                "bybit_stream_task_cancelled",
                "Bybit stream task was cancelled",
            ),
        ] {
            cases.push((
                name,
                supervised_task_exit_error(
                    &TaskId::Connector("bybit".to_string()),
                    Err(failure.into()),
                ),
                TerminalDiagnostic {
                    component: "bybit",
                    task: "connector:bybit".to_string(),
                    operation: "stream_task",
                    stage: "trades",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: code,
                    disposition: "fatal_service_exit",
                    state_effect: "process_exit",
                    safe_cause: cause,
                },
            ));
        }
        for (name, failure, code, cause) in [
            (
                "BackpackTaskError",
                crate::connector::backpack::BackpackTaskFailure::task_error(
                    "trades",
                    anyhow::anyhow!("provider-secret"),
                ),
                "backpack_stream_task_failed",
                "Backpack stream task failed",
            ),
            (
                "BackpackUnexpectedExit",
                crate::connector::backpack::BackpackTaskFailure::unexpected_exit("trades"),
                "backpack_stream_task_exited",
                "Backpack stream task exited unexpectedly",
            ),
            (
                "BackpackPanicked",
                crate::connector::backpack::BackpackTaskFailure::panicked("trades"),
                "backpack_stream_task_panicked",
                "Backpack stream task panicked",
            ),
            (
                "BackpackCancelled",
                crate::connector::backpack::BackpackTaskFailure::cancelled("trades"),
                "backpack_stream_task_cancelled",
                "Backpack stream task was cancelled",
            ),
        ] {
            cases.push((
                name,
                supervised_task_exit_error(
                    &TaskId::Connector("backpack".to_string()),
                    Err(failure.into()),
                ),
                TerminalDiagnostic {
                    component: "backpack",
                    task: "connector:backpack".to_string(),
                    operation: "stream_task",
                    stage: "trades",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: code,
                    disposition: "fatal_service_exit",
                    state_effect: "process_exit",
                    safe_cause: cause,
                },
            ));
        }
        for (name, kind, code, cause) in [
            (
                "CancelledJoin",
                SupervisedFailureKind::CancelledJoin,
                "supervised_task_cancelled",
                "supervised task was cancelled",
            ),
            (
                "PanickedJoin",
                SupervisedFailureKind::PanickedJoin,
                "supervised_task_panicked",
                "supervised task panicked",
            ),
            (
                "OtherJoin",
                SupervisedFailureKind::OtherJoin,
                "supervised_task_join_failed",
                "supervised task join failed",
            ),
        ] {
            cases.push((
                name,
                direct_supervisor(kind),
                generic("unknown", "task_join", code, cause),
            ));
        }

        #[cfg(feature = "rithmic")]
        {
            let mut failure = crate::connector::rithmic::PayloadFailure::new(
                crate::connector::rithmic::PayloadFailureKind::MarketDecode,
            );
            failure.attach_transport(Some(151), 777);
            let source = anyhow::Error::new(failure)
                .context("payload boundary")
                .context("connector boundary");
            cases.push((
                "RithmicPayload",
                supervised_task_exit_error(&TaskId::Connector("rithmic".to_string()), Err(source)),
                TerminalDiagnostic {
                    component: "rithmic",
                    task: "connector:rithmic".to_string(),
                    operation: "handle_payload",
                    stage: "market_decode",
                    template_id: "151".to_string(),
                    payload_len: "777".to_string(),
                    stable_error_code: "malformed_market_payload",
                    disposition: "fatal_service_exit",
                    state_effect: "none",
                    safe_cause: "market payload validation failed",
                },
            ));
            cases.push((
                "HandshakeReject",
                crate::connector::rithmic::handshake_rejection_with_contexts(),
                TerminalDiagnostic {
                    component: "rithmic",
                    task: "unknown".to_string(),
                    operation: "handshake",
                    stage: "handshake",
                    template_id: "unknown".to_string(),
                    payload_len: "unknown".to_string(),
                    stable_error_code: "rithmic_handshake_rejected",
                    disposition: "fatal_service_exit",
                    state_effect: "session_failed",
                    safe_cause: "Rithmic handshake rejected",
                },
            ));
        }

        for (name, error, expected) in cases {
            assert_eq!(terminal_diagnostic(&error), expected, "{name}");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn binance_wrapper_propagates_then_reports_exactly_one_error_without_network_poll() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _key = EnvVarGuard::remove("BINANCE_API_KEY");
        let (trade_tx, _) = mpsc::channel(1);
        let (candle_tx, _) = mpsc::channel(1);
        let (user_tx, _) = mpsc::channel(1);
        let mut events = capture_error_events(|| {
            let future =
                crate::connector::binance::run(Vec::new(), trade_tx, candle_tx, user_tx, true);
            let source = future.now_or_never().unwrap().unwrap_err();
            let error =
                supervised_task_exit_error(&TaskId::Connector("binance".to_string()), Err(source));
            report_terminal_failure(&error);
        });

        assert_eq!(events.len(), 1);
        let fields = events.pop().unwrap();
        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "unknown");
        assert_eq!(fields["task"], "connector:binance");
        assert_eq!(fields["operation"], "unknown");
        assert_eq!(fields["stage"], "task_exit");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "supervised_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "supervised task failed");
    }

    #[test]
    fn binance_internal_task_failure_reports_exact_safe_terminal_fields() {
        let source = anyhow::anyhow!("provider failure sentinel");
        let failure = crate::connector::binance::BinanceTaskFailure::task_error("trades", source);
        let error = supervised_task_exit_error(
            &TaskId::Connector("binance".to_string()),
            Err(failure.into()),
        );
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "binance");
        assert_eq!(fields["task"], "connector:binance");
        assert_eq!(fields["operation"], "stream_task");
        assert_eq!(fields["stage"], "trades");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "binance_stream_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "Binance stream task failed");
        assert!(fields
            .values()
            .all(|value| !value.contains("provider failure sentinel")));
    }

    #[test]
    fn backpack_internal_task_failure_reports_exact_safe_terminal_fields() {
        let source = anyhow::anyhow!("provider failure sentinel");
        let failure =
            crate::connector::backpack::BackpackTaskFailure::task_error("candles", source);
        let error = supervised_task_exit_error(
            &TaskId::Connector("backpack".to_string()),
            Err(failure.into()),
        );
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "backpack");
        assert_eq!(fields["task"], "connector:backpack");
        assert_eq!(fields["operation"], "stream_task");
        assert_eq!(fields["stage"], "candles");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "backpack_stream_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "Backpack stream task failed");
        assert!(fields
            .values()
            .all(|value| !value.contains("provider failure sentinel")));
    }

    #[test]
    fn bybit_internal_task_failure_reports_exact_safe_terminal_fields() {
        let source = anyhow::anyhow!("provider failure sentinel");
        let failure = crate::connector::bybit::BybitTaskFailure::task_error("candles", source);
        let error = supervised_task_exit_error(
            &TaskId::Connector("bybit".to_string()),
            Err(failure.into()),
        );
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "bybit");
        assert_eq!(fields["task"], "connector:bybit");
        assert_eq!(fields["operation"], "stream_task");
        assert_eq!(fields["stage"], "candles");
        assert_eq!(fields["template_id"], "unknown");
        assert_eq!(fields["payload_len"], "unknown");
        assert_eq!(fields["stable_error_code"], "bybit_stream_task_failed");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "process_exit");
        assert_eq!(fields["safe_cause"], "Bybit stream task failed");
        assert!(fields
            .values()
            .all(|value| !value.contains("provider failure sentinel")));
    }

    #[tokio::test]
    async fn actual_join_errors_select_safe_terminal_classifications() {
        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        let cancelled = supervised_join_error(cancelled.await.unwrap_err());
        let cancelled_fields = capture_terminal_event(&cancelled);
        assert_eq!(
            cancelled_fields["stable_error_code"],
            "supervised_task_cancelled"
        );

        let panicked = tokio::spawn(async { panic!("panic-payload-sentinel") });
        let panicked = supervised_join_error(panicked.await.unwrap_err());
        let panicked_fields = capture_terminal_event(&panicked);
        assert_eq!(
            panicked_fields["stable_error_code"],
            "supervised_task_panicked"
        );
        assert!(panicked_fields
            .values()
            .all(|value| !value.contains("panic-payload-sentinel")));
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn typed_payload_metadata_reaches_supervised_terminal_event_through_contexts() {
        let mut failure = crate::connector::rithmic::PayloadFailure::new(
            crate::connector::rithmic::PayloadFailureKind::MarketDecode,
        );
        failure.attach_transport(Some(151), 777);
        let source = anyhow::Error::new(failure)
            .context("payload boundary")
            .context("connector boundary");
        let error =
            supervised_task_exit_error(&TaskId::Connector("rithmic".to_string()), Err(source));
        let fields = capture_terminal_event(&error);

        assert_eq!(fields.len(), 11);
        assert_eq!(fields["message"], "FluxTrade terminal failure");
        assert_eq!(fields["component"], "rithmic");
        assert_eq!(fields["task"], "connector:rithmic");
        assert_eq!(fields["operation"], "handle_payload");
        assert_eq!(fields["stage"], "market_decode");
        assert_eq!(fields["template_id"], "151");
        assert_eq!(fields["payload_len"], "777");
        assert_eq!(fields["stable_error_code"], "malformed_market_payload");
        assert_eq!(fields["disposition"], "fatal_service_exit");
        assert_eq!(fields["state_effect"], "none");
        assert_eq!(fields["safe_cause"], "market payload validation failed");
    }

    #[test]
    fn test_task_id_display() {
        assert_eq!(TaskId::Watchdog.to_string(), "watchdog");
        assert_eq!(TaskId::Publisher.to_string(), "publisher");
        assert_eq!(TaskId::EventLoop.to_string(), "event-loop");
        assert_eq!(
            TaskId::Connector("binance".to_string()).to_string(),
            "connector:binance"
        );
    }

    #[test]
    fn emergency_mitigation_is_connector_owned() {
        let main_source = include_str!("main.rs");
        let watchdog_source = include_str!("watchdog.rs");
        let owner_source = include_str!("connector/emergency.rs");
        let main_product = product_source(main_source);
        let watchdog_product = product_source(watchdog_source);

        assert!(!main_product.contains("BackpackConnector"));
        assert!(!main_product.contains("fn resolve_emergency_mitigation"));
        assert!(!watchdog_product.contains("crate::connector::backpack"));
        assert!(!watchdog_product.contains("crate::connector::rithmic"));
        assert!(owner_source.contains("pub(crate) fn resolve"));
        assert!(owner_source.contains("pub(crate) async fn run"));
    }

    #[test]
    fn connector_terminal_diagnostics_are_connector_owned() {
        let main_source = include_str!("main.rs");
        let owner_source = include_str!("connector/terminal.rs");
        let main_product = product_source(main_source);

        for provider_detail in [
            "BinanceTaskFailure",
            "BackpackTaskFailure",
            "BybitTaskFailure",
            "PayloadFailure",
            "is_handshake_rejection",
        ] {
            assert!(!main_product.contains(provider_detail));
            assert!(owner_source.contains(provider_detail));
        }
        assert!(owner_source.contains("pub(crate) fn classify"));
    }

    #[test]
    fn connector_live_runtime_composition_is_connector_owned() {
        let main_source = include_str!("main.rs");
        let owner_source = include_str!("connector/live_runtime.rs");
        let main_product = product_source(main_source);

        for (main_detail, owner_detail) in [
            ("connector::binance::run", "super::binance::run"),
            ("connector::bybit::run", "super::bybit::run"),
            ("connector::backpack::run", "super::backpack::run"),
            (
                "preflight_user_stream_credentials",
                "preflight_user_stream_credentials",
            ),
            ("resolve_market_data_symbols", "resolve_market_data_symbols"),
            ("resolve_live_options", "resolve_live_options"),
        ] {
            assert!(!main_product.contains(main_detail));
            assert!(owner_source.contains(owner_detail));
        }
        assert!(owner_source.contains("pub(crate) struct LiveRuntime"));
        assert!(owner_source.contains("pub(crate) fn prepare"));
        assert!(owner_source.contains("pub(crate) fn spawn"));
    }

    #[test]
    fn generic_runtime_supervision_has_one_owner_without_absorbing_task_composition() {
        let main_product = product_source(include_str!("main.rs"));
        let owner_product = product_source(include_str!("runtime_supervisor.rs"));

        assert_eq!(
            main_product.matches("supervise(join_set).await?").count(),
            1
        );
        assert_eq!(
            main_product
                .matches("initialize_process_diagnostics();")
                .count(),
            1
        );
        assert_eq!(
            main_product
                .matches("report_terminal_failure(&error);")
                .count(),
            1
        );
        for implementation in [
            "fn terminal_diagnostic(",
            "fn supervised_task_exit_error(",
            "fn supervised_join_error(",
            "async fn supervise_until(",
        ] {
            assert!(!main_product.contains(implementation), "{implementation}");
            assert!(owner_product.contains(implementation), "{implementation}");
        }
        assert!(!owner_product.contains(".spawn("));
        assert!(!owner_product.contains("LiveRuntime::prepare"));
        assert!(!owner_product.contains("RedisPublisher::new"));
    }

    #[test]
    fn every_supervised_task_exit_is_fatal() {
        for task_id in [
            TaskId::Watchdog,
            TaskId::Publisher,
            TaskId::EventLoop,
            TaskId::Connector("binance".to_string()),
            TaskId::Connector("bybit".to_string()),
            TaskId::Connector("backpack".to_string()),
            TaskId::Connector("rithmic".to_string()),
        ] {
            let clean = supervised_task_exit_error(&task_id, Ok(()));
            assert_eq!(
                terminal_diagnostic(&clean).stable_error_code,
                "supervised_task_exited"
            );
            let failed = supervised_task_exit_error(&task_id, Err(anyhow::anyhow!("secret")));
            assert_eq!(
                terminal_diagnostic(&failed).stable_error_code,
                "supervised_task_failed"
            );
        }
        for (cancelled, panicked, expected) in [
            (true, false, "supervised_task_cancelled"),
            (false, true, "supervised_task_panicked"),
            (false, false, "supervised_task_join_failed"),
        ] {
            assert_eq!(
                classify_join_failure(cancelled, panicked).diagnostic().1,
                expected
            );
        }
    }

    #[test]
    fn supervised_task_failure_preserves_the_complete_error_chain() {
        let source = anyhow::anyhow!("unsupported Rithmic market-data template 151")
            .context("Rithmic payload handler failed");
        let error =
            supervised_task_exit_error(&TaskId::Connector("rithmic".to_string()), Err(source));

        assert_eq!(
            error.chain().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "supervised task failed",
                "Rithmic payload handler failed",
                "unsupported Rithmic market-data template 151",
            ]
        );
        assert_eq!(terminal_diagnostic(&error).task, "connector:rithmic");
    }

    #[test]
    fn optional_environment_values_are_trimmed() {
        assert_eq!(normalized_optional_value(None), None);
        assert_eq!(normalized_optional_value(Some(String::new())), None);
        assert_eq!(normalized_optional_value(Some("  ".to_string())), None);
        assert_eq!(
            normalized_optional_value(Some(" key ".to_string())),
            Some("key".to_string())
        );
    }

    #[test]
    fn generic_main_contains_no_venue_credential_schema_or_pair_validator() {
        let production = product_source(include_str!("main.rs"));
        for forbidden in [
            "BINANCE_API_KEY",
            "EXCHANGE_API_KEY",
            "EXCHANGE_SECRET",
            "optional_credentials_present",
        ] {
            assert!(!production.contains(forbidden), "{forbidden}");
        }
    }

    fn product_source(source: &str) -> &str {
        source
            .rsplit_once("\n#[cfg(test)]\nmod tests {")
            .map_or(source, |(product, _)| product)
    }
}
