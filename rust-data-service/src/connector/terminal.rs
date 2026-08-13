#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ConnectorTerminalDiagnostic {
    pub(crate) component: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) stage: &'static str,
    pub(crate) template_id: String,
    pub(crate) payload_len: String,
    pub(crate) stable_error_code: &'static str,
    pub(crate) disposition: &'static str,
    pub(crate) state_effect: &'static str,
    pub(crate) safe_cause: &'static str,
}

pub(crate) fn classify(error: &anyhow::Error) -> Option<ConnectorTerminalDiagnostic> {
    if let Some(failure) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<super::binance::BinanceTaskFailure>())
    {
        return Some(stream_task_diagnostic(
            "binance",
            failure.task(),
            failure.stable_error_code(),
            failure.safe_cause(),
        ));
    }
    if let Some(failure) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<super::backpack::BackpackTaskFailure>())
    {
        return Some(stream_task_diagnostic(
            "backpack",
            failure.task(),
            failure.stable_error_code(),
            failure.safe_cause(),
        ));
    }
    if let Some(failure) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<super::bybit::BybitTaskFailure>())
    {
        return Some(stream_task_diagnostic(
            "bybit",
            failure.task(),
            failure.stable_error_code(),
            failure.safe_cause(),
        ));
    }
    #[cfg(feature = "rithmic")]
    if let Some(failure) = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<super::rithmic::PayloadFailure>())
    {
        return Some(ConnectorTerminalDiagnostic {
            component: "rithmic",
            operation: failure.operation(),
            stage: failure.stage(),
            template_id: failure
                .template_id()
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
            payload_len: failure
                .payload_len()
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
            stable_error_code: failure.stable_error_code(),
            disposition: failure.disposition(),
            state_effect: failure.state_effect(),
            safe_cause: failure.safe_cause(),
        });
    }
    #[cfg(feature = "rithmic")]
    if error.chain().any(super::rithmic::is_handshake_rejection) {
        return Some(ConnectorTerminalDiagnostic {
            component: "rithmic",
            operation: "handshake",
            stage: "handshake",
            template_id: "unknown".to_string(),
            payload_len: "unknown".to_string(),
            stable_error_code: "rithmic_handshake_rejected",
            disposition: "fatal_service_exit",
            state_effect: "session_failed",
            safe_cause: "Rithmic handshake rejected",
        });
    }
    None
}

fn stream_task_diagnostic(
    component: &'static str,
    stage: &'static str,
    stable_error_code: &'static str,
    safe_cause: &'static str,
) -> ConnectorTerminalDiagnostic {
    ConnectorTerminalDiagnostic {
        component,
        operation: "stream_task",
        stage,
        template_id: "unknown".to_string(),
        payload_len: "unknown".to_string(),
        stable_error_code,
        disposition: "fatal_service_exit",
        state_effect: "process_exit",
        safe_cause,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_connector_error_is_not_claimed() {
        assert_eq!(classify(&anyhow::anyhow!("generic failure")), None);
    }

    #[test]
    fn provider_precedence_prefers_outer_binance_failure() {
        let source = anyhow::Error::new(super::super::backpack::BackpackTaskFailure::task_error(
            "candles",
            anyhow::anyhow!("inner failure"),
        ));
        let error = anyhow::Error::new(super::super::binance::BinanceTaskFailure::task_error(
            "trades", source,
        ));

        assert_eq!(classify(&error).unwrap().component, "binance");
    }
}
