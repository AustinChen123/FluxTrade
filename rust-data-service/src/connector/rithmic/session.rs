use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};

use super::{codec, protocol};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct FatalSessionError(String);

pub(crate) fn is_fatal_session_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<FatalSessionError>().is_some()
}

const SYSTEM_INFO_REQUEST: i32 = 16;
const SYSTEM_INFO_RESPONSE: i32 = 17;
const LOGIN_REQUEST: i32 = 10;
const LOGIN_RESPONSE: i32 = 11;
const LOGOUT_REQUEST: i32 = 12;
const LOGOUT_RESPONSE: i32 = 13;
const HEARTBEAT_REQUEST: i32 = 18;
const HEARTBEAT_RESPONSE: i32 = 19;
const REJECT: i32 = 75;
const FORCED_LOGOUT: i32 = 77;
const TEMPLATE_VERSION: &str = "3.9";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plant {
    Ticker,
    Order,
    History,
    Pnl,
}

impl Plant {
    fn protocol_value(self) -> i32 {
        match self {
            Self::Ticker => protocol::request_login::SysInfraType::TickerPlant as i32,
            Self::Order => protocol::request_login::SysInfraType::OrderPlant as i32,
            Self::History => protocol::request_login::SysInfraType::HistoryPlant as i32,
            Self::Pnl => protocol::request_login::SysInfraType::PnlPlant as i32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionState {
    Disconnected,
    AwaitingSystemInfo,
    AwaitingReconnect,
    ReadyToLogin,
    AwaitingLogin,
    Active,
    Closing,
    Closed,
    Failed,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ResponseDisposition {
    Processing,
    Succeeded,
    Failed(Vec<String>),
}

#[derive(Clone)]
pub(crate) struct LoginParameters {
    user: String,
    password: String,
    system_name: String,
    app_name: String,
    app_version: String,
    plant: Plant,
}

impl LoginParameters {
    pub(crate) fn new(
        user: String,
        password: String,
        system_name: String,
        app_name: String,
        app_version: String,
        plant: Plant,
    ) -> Result<Self> {
        for (name, value) in [
            ("user", &user),
            ("password", &password),
            ("system_name", &system_name),
            ("app_name", &app_name),
            ("app_version", &app_version),
        ] {
            ensure!(!value.trim().is_empty(), "Rithmic {name} must not be empty");
        }

        Ok(Self {
            user,
            password,
            system_name,
            app_name,
            app_version,
            plant,
        })
    }
}

pub(crate) struct RithmicSession {
    login: LoginParameters,
    state: SessionState,
    heartbeat_interval: Option<Duration>,
}

impl RithmicSession {
    pub(crate) fn new(login: LoginParameters) -> Self {
        Self {
            login,
            state: SessionState::Disconnected,
            heartbeat_interval: None,
        }
    }

    pub(crate) fn state(&self) -> SessionState {
        self.state
    }

    pub(crate) fn begin_system_info(&mut self) -> Result<Vec<u8>> {
        self.require_state(SessionState::Disconnected)?;
        let frame = codec::encode(&protocol::RequestRithmicSystemInfo {
            template_id: SYSTEM_INFO_REQUEST,
            user_msg: vec![],
        })?;
        self.state = SessionState::AwaitingSystemInfo;
        Ok(frame)
    }

    pub(crate) fn accept_system_info(&mut self, frame: &[u8]) -> Result<()> {
        self.require_state(SessionState::AwaitingSystemInfo)?;
        let result = (|| {
            expect_template(frame, SYSTEM_INFO_RESPONSE)?;
            let response: protocol::ResponseRithmicSystemInfo = codec::decode(frame)?;
            ensure_handshake_success(&response.rp_code, "system-info")?;
            if !response.system_name.contains(&self.login.system_name) {
                return Err(FatalSessionError(
                    "configured Rithmic system is unavailable".to_string(),
                )
                .into());
            }
            Ok(())
        })();
        self.finish_response(result, SessionState::AwaitingReconnect)
    }

    pub(crate) fn mark_reconnected(&mut self) -> Result<()> {
        self.require_state(SessionState::AwaitingReconnect)?;
        self.state = SessionState::ReadyToLogin;
        Ok(())
    }

    pub(crate) fn begin_login(&mut self) -> Result<Vec<u8>> {
        self.require_state(SessionState::ReadyToLogin)?;
        let frame = codec::encode(&protocol::RequestLogin {
            template_id: LOGIN_REQUEST,
            template_version: Some(TEMPLATE_VERSION.to_string()),
            user_msg: vec![],
            user: Some(self.login.user.clone()),
            password: Some(self.login.password.clone()),
            app_name: Some(self.login.app_name.clone()),
            app_version: Some(self.login.app_version.clone()),
            system_name: Some(self.login.system_name.clone()),
            infra_type: Some(self.login.plant.protocol_value()),
            mac_addr: vec![],
            os_version: None,
            os_platform: None,
            aggregated_quotes: None,
        })?;
        self.state = SessionState::AwaitingLogin;
        Ok(frame)
    }

    pub(crate) fn accept_login(&mut self, frame: &[u8]) -> Result<Vec<u8>> {
        self.require_state(SessionState::AwaitingLogin)?;
        let result = (|| {
            expect_template(frame, LOGIN_RESPONSE)?;
            let response: protocol::ResponseLogin = codec::decode(frame)?;
            ensure_handshake_success(&response.rp_code, "login")?;
            let seconds = response.heartbeat_interval.ok_or_else(|| {
                FatalSessionError("Rithmic login response omitted heartbeat_interval".to_string())
            })?;
            let interval = validated_heartbeat_interval(seconds)?;
            self.heartbeat_interval = Some(interval);
            codec::encode(&heartbeat_request())
        })();

        match result {
            Ok(frame) => {
                self.state = SessionState::Active;
                Ok(frame)
            }
            Err(error) => {
                self.state = SessionState::Failed;
                Err(error)
            }
        }
    }

    pub(crate) fn heartbeat_interval(&self) -> Result<Duration> {
        self.require_state(SessionState::Active)?;
        self.heartbeat_interval
            .context("Rithmic heartbeat interval is unavailable")
    }

    pub(crate) fn heartbeat(&self) -> Result<Vec<u8>> {
        self.require_state(SessionState::Active)?;
        codec::encode(&heartbeat_request())
    }

    pub(crate) fn accept_heartbeat(&mut self, frame: &[u8]) -> Result<()> {
        self.require_state(SessionState::Active)?;
        let result = (|| {
            expect_template(frame, HEARTBEAT_RESPONSE)?;
            let response: protocol::ResponseHeartbeat = codec::decode(frame)?;
            ensure_success(&response.rp_code)
        })();
        self.finish_response(result, SessionState::Active)
    }

    pub(crate) fn accept_control(&mut self, frame: &[u8]) -> Result<bool> {
        match codec::template_id(frame)? {
            HEARTBEAT_RESPONSE => {
                self.accept_heartbeat(frame)?;
                Ok(true)
            }
            FORCED_LOGOUT => self.accept_forced_logout(frame).map(|()| true),
            _ => Ok(false),
        }
    }

    pub(crate) fn reject_terminal(&mut self, frame: &[u8]) -> Result<()> {
        match codec::template_id(frame)? {
            REJECT => self.accept_handshake_reject(frame),
            FORCED_LOGOUT => self.accept_forced_logout(frame),
            _ => Ok(()),
        }
    }

    pub(crate) fn begin_logout(&mut self) -> Result<Vec<u8>> {
        self.require_state(SessionState::Active)?;
        let frame = codec::encode(&protocol::RequestLogout {
            template_id: LOGOUT_REQUEST,
            user_msg: vec![],
        })?;
        self.state = SessionState::Closing;
        Ok(frame)
    }

    pub(crate) fn accept_logout(&mut self, frame: &[u8]) -> Result<()> {
        self.require_state(SessionState::Closing)?;
        let result = (|| {
            expect_template(frame, LOGOUT_RESPONSE)?;
            let response: protocol::ResponseLogout = codec::decode(frame)?;
            ensure_success(&response.rp_code)
        })();
        self.finish_response(result, SessionState::Closed)
    }

    fn accept_handshake_reject(&mut self, frame: &[u8]) -> Result<()> {
        expect_template(frame, REJECT)?;
        let _: protocol::Reject = codec::decode(frame)?;
        self.state = SessionState::Failed;
        Err(FatalSessionError("stable_error_code=rithmic_handshake_rejected".to_string()).into())
    }

    fn accept_forced_logout(&mut self, frame: &[u8]) -> Result<()> {
        expect_template(frame, FORCED_LOGOUT)?;
        let _: protocol::ForcedLogout = codec::decode(frame)?;
        self.state = SessionState::Failed;
        Err(FatalSessionError("Rithmic forced the session to log out".to_string()).into())
    }

    fn require_state(&self, expected: SessionState) -> Result<()> {
        ensure!(
            self.state == expected,
            "invalid Rithmic session transition: expected {expected:?}, found {:?}",
            self.state
        );
        Ok(())
    }

    fn finish_response(&mut self, result: Result<()>, success: SessionState) -> Result<()> {
        match result {
            Ok(()) => {
                self.state = success;
                Ok(())
            }
            Err(error) => {
                self.state = SessionState::Failed;
                Err(error)
            }
        }
    }
}

fn heartbeat_request() -> protocol::RequestHeartbeat {
    protocol::RequestHeartbeat {
        template_id: HEARTBEAT_REQUEST,
        ..Default::default()
    }
}

fn validated_heartbeat_interval(seconds: f64) -> Result<Duration> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(FatalSessionError(
            "Rithmic heartbeat_interval must be finite and positive".to_string(),
        )
        .into());
    }
    let interval = Duration::try_from_secs_f64(seconds)
        .map_err(|_| FatalSessionError("Rithmic heartbeat_interval is out of range".to_string()))?;
    if interval.is_zero() {
        return Err(FatalSessionError(
            "Rithmic heartbeat_interval is below timer resolution".to_string(),
        )
        .into());
    }
    Instant::now().checked_add(interval).ok_or_else(|| {
        anyhow::Error::new(FatalSessionError(
            "Rithmic heartbeat_interval exceeds timer range".to_string(),
        ))
    })?;
    Ok(interval)
}

fn expect_template(frame: &[u8], expected: i32) -> Result<()> {
    let actual = codec::template_id(frame)?;
    ensure!(
        actual == expected,
        "unexpected Rithmic template: expected {expected}, received {actual}"
    );
    Ok(())
}

pub(super) fn ensure_success(rp_codes: &[String]) -> Result<()> {
    let code = rp_codes
        .first()
        .context("Rithmic response omitted rp_code")?;
    ensure!(code == "0", "Rithmic response code {code}");
    Ok(())
}

pub(super) fn classify_response_codes(
    rq_handler_rp_codes: &[String],
    rp_codes: &[String],
) -> Result<ResponseDisposition> {
    ensure!(
        rq_handler_rp_codes.is_empty() || rp_codes.is_empty(),
        "Rithmic response has conflicting status codes"
    );
    if let Some(code) = rq_handler_rp_codes.first() {
        return if code == "0" {
            Ok(ResponseDisposition::Processing)
        } else {
            Ok(ResponseDisposition::Failed(rq_handler_rp_codes.to_vec()))
        };
    }
    if let Some(code) = rp_codes.first() {
        return if code == "0" {
            Ok(ResponseDisposition::Succeeded)
        } else {
            Ok(ResponseDisposition::Failed(rp_codes.to_vec()))
        };
    }
    anyhow::bail!("Rithmic response omitted status codes")
}

fn ensure_handshake_success(rp_codes: &[String], phase: &str) -> Result<()> {
    let code = rp_codes.first().ok_or_else(|| {
        FatalSessionError(format!(
            "Rithmic {phase} handshake response omitted rp_code"
        ))
    })?;
    if code != "0" {
        let safe_code = code
            .parse::<u32>()
            .map(|value| value.to_string())
            .unwrap_or_else(|_| "unrecognized".to_string());
        let message = format!("Rithmic {phase} handshake response code {safe_code}");
        return Err(FatalSessionError(message).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn login(plant: Plant) -> LoginParameters {
        LoginParameters::new(
            "test-user".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            plant,
        )
        .unwrap()
    }

    fn system_info(systems: &[&str], rp_code: &str) -> Vec<u8> {
        codec::encode(&protocol::ResponseRithmicSystemInfo {
            template_id: SYSTEM_INFO_RESPONSE,
            user_msg: vec![],
            rp_code: vec![rp_code.to_string()],
            system_name: systems.iter().map(|value| (*value).to_string()).collect(),
            has_aggregated_quotes: vec![],
        })
        .unwrap()
    }

    fn login_response(interval: Option<f64>, rp_code: &str) -> Vec<u8> {
        codec::encode(&protocol::ResponseLogin {
            template_id: LOGIN_RESPONSE,
            rp_code: vec![rp_code.to_string()],
            heartbeat_interval: interval,
            ..Default::default()
        })
        .unwrap()
    }

    fn heartbeat_response(rp_code: &str) -> Vec<u8> {
        codec::encode(&protocol::ResponseHeartbeat {
            template_id: HEARTBEAT_RESPONSE,
            rp_code: vec![rp_code.to_string()],
            ..Default::default()
        })
        .unwrap()
    }

    fn activate(plant: Plant) -> RithmicSession {
        let mut session = RithmicSession::new(login(plant));
        session.begin_system_info().unwrap();
        session
            .accept_system_info(&system_info(&["test-system"], "0"))
            .unwrap();
        session.mark_reconnected().unwrap();
        session.begin_login().unwrap();
        session
            .accept_login(&login_response(Some(30.0), "0"))
            .unwrap();
        session
    }

    fn handshake_sessions() -> [RithmicSession; 2] {
        let mut discovery = RithmicSession::new(login(Plant::Ticker));
        discovery.begin_system_info().unwrap();

        let mut login_session = RithmicSession::new(login(Plant::Ticker));
        login_session.begin_system_info().unwrap();
        login_session
            .accept_system_info(&system_info(&["test-system"], "0"))
            .unwrap();
        login_session.mark_reconnected().unwrap();
        login_session.begin_login().unwrap();

        [discovery, login_session]
    }

    #[test]
    fn login_heartbeat_logout_lifecycle_matrix() {
        for plant in [Plant::Ticker, Plant::Order, Plant::History, Plant::Pnl] {
            let mut session = activate(plant);
            assert_eq!(session.state(), SessionState::Active);
            assert_eq!(
                session.heartbeat_interval().unwrap(),
                Duration::from_secs(30)
            );
            assert_eq!(
                codec::template_id(&session.heartbeat().unwrap()).unwrap(),
                18
            );
            session.accept_heartbeat(&heartbeat_response("0")).unwrap();

            session.begin_logout().unwrap();
            let response = codec::encode(&protocol::ResponseLogout {
                template_id: LOGOUT_RESPONSE,
                user_msg: vec![],
                rp_code: vec!["0".to_string()],
            })
            .unwrap();
            session.accept_logout(&response).unwrap();
            assert_eq!(session.state(), SessionState::Closed);
        }
    }

    #[test]
    fn login_requires_system_info_and_reconnect_boundary() {
        let mut session = RithmicSession::new(login(Plant::Ticker));

        assert!(session.begin_login().is_err());
        assert_eq!(session.state(), SessionState::Disconnected);

        session.begin_system_info().unwrap();
        session
            .accept_system_info(&system_info(&["test-system"], "0"))
            .unwrap();
        assert!(session.begin_login().is_err());
        assert_eq!(session.state(), SessionState::AwaitingReconnect);

        session.mark_reconnected().unwrap();
        assert!(session.begin_login().is_ok());
        assert_eq!(session.state(), SessionState::AwaitingLogin);
    }

    #[test]
    fn login_request_maps_each_plant() {
        for (plant, expected) in [
            (
                Plant::Ticker,
                protocol::request_login::SysInfraType::TickerPlant,
            ),
            (
                Plant::Order,
                protocol::request_login::SysInfraType::OrderPlant,
            ),
            (
                Plant::History,
                protocol::request_login::SysInfraType::HistoryPlant,
            ),
            (Plant::Pnl, protocol::request_login::SysInfraType::PnlPlant),
        ] {
            let mut session = RithmicSession::new(login(plant));
            session.begin_system_info().unwrap();
            session
                .accept_system_info(&system_info(&["test-system"], "0"))
                .unwrap();
            session.mark_reconnected().unwrap();

            let frame = session.begin_login().unwrap();
            let request: protocol::RequestLogin = codec::decode(&frame).unwrap();
            assert_eq!(request.template_id, LOGIN_REQUEST);
            assert_eq!(request.template_version.as_deref(), Some(TEMPLATE_VERSION));
            assert_eq!(request.infra_type, Some(expected as i32));
        }
    }

    #[test]
    fn unavailable_system_fails_closed() {
        let mut session = RithmicSession::new(login(Plant::Ticker));
        session.begin_system_info().unwrap();

        let error = session
            .accept_system_info(&system_info(&["another-system"], "0"))
            .unwrap_err();
        assert!(is_fatal_session_error(&error));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn response_error_fails_closed() {
        let mut session = RithmicSession::new(login(Plant::Ticker));
        session.begin_system_info().unwrap();

        let error = session
            .accept_system_info(&system_info(&["test-system"], "9"))
            .unwrap_err();
        assert!(is_fatal_session_error(&error));
        assert!(error.to_string().contains("system-info"));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn login_error_preserves_phase_without_untrusted_server_detail() {
        let mut session = RithmicSession::new(login(Plant::Ticker));
        session.begin_system_info().unwrap();
        session
            .accept_system_info(&system_info(&["test-system"], "0"))
            .unwrap();
        session.mark_reconnected().unwrap();
        session.begin_login().unwrap();
        let response = codec::encode(&protocol::ResponseLogin {
            template_id: LOGIN_RESPONSE,
            rp_code: vec!["13".to_string(), "session unavailable".to_string()],
            ..Default::default()
        })
        .unwrap();

        let error = session.accept_login(&response).unwrap_err();

        assert!(is_fatal_session_error(&error));
        assert_eq!(
            error.to_string(),
            "Rithmic login handshake response code 13"
        );
        assert!(!error.to_string().contains("session unavailable"));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn heartbeat_failure_remains_retryable() {
        let mut session = activate(Plant::Ticker);

        let error = session
            .accept_heartbeat(&heartbeat_response("9"))
            .unwrap_err();

        assert!(!is_fatal_session_error(&error));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn missing_response_code_fails_closed_in_each_response_state() {
        let empty_system_info = codec::encode(&protocol::ResponseRithmicSystemInfo {
            template_id: SYSTEM_INFO_RESPONSE,
            system_name: vec!["test-system".to_string()],
            ..Default::default()
        })
        .unwrap();
        let mut system_info_session = RithmicSession::new(login(Plant::Ticker));
        system_info_session.begin_system_info().unwrap();
        assert!(system_info_session
            .accept_system_info(&empty_system_info)
            .is_err());
        assert_eq!(system_info_session.state(), SessionState::Failed);

        let empty_login = codec::encode(&protocol::ResponseLogin {
            template_id: LOGIN_RESPONSE,
            heartbeat_interval: Some(30.0),
            ..Default::default()
        })
        .unwrap();
        let mut login_session = RithmicSession::new(login(Plant::Ticker));
        login_session.begin_system_info().unwrap();
        login_session
            .accept_system_info(&system_info(&["test-system"], "0"))
            .unwrap();
        login_session.mark_reconnected().unwrap();
        login_session.begin_login().unwrap();
        assert!(login_session.accept_login(&empty_login).is_err());
        assert_eq!(login_session.state(), SessionState::Failed);

        let empty_heartbeat = codec::encode(&protocol::ResponseHeartbeat {
            template_id: HEARTBEAT_RESPONSE,
            ..Default::default()
        })
        .unwrap();
        let mut heartbeat_session = activate(Plant::Ticker);
        assert!(heartbeat_session
            .accept_heartbeat(&empty_heartbeat)
            .is_err());
        assert_eq!(heartbeat_session.state(), SessionState::Failed);

        let empty_logout = codec::encode(&protocol::ResponseLogout {
            template_id: LOGOUT_RESPONSE,
            ..Default::default()
        })
        .unwrap();
        let mut logout_session = activate(Plant::Ticker);
        logout_session.begin_logout().unwrap();
        assert!(logout_session.accept_logout(&empty_logout).is_err());
        assert_eq!(logout_session.state(), SessionState::Failed);
    }

    #[test]
    fn invalid_heartbeat_interval_matrix_fails_closed() {
        for interval in [
            None,
            Some(0.0),
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::MAX),
            Some(seconds_beyond_instant_range()),
            Some(f64::MIN_POSITIVE),
        ] {
            let mut session = RithmicSession::new(login(Plant::Ticker));
            session.begin_system_info().unwrap();
            session
                .accept_system_info(&system_info(&["test-system"], "0"))
                .unwrap();
            session.mark_reconnected().unwrap();
            session.begin_login().unwrap();

            let error = session
                .accept_login(&login_response(interval, "0"))
                .unwrap_err();
            assert!(is_fatal_session_error(&error));
            assert_eq!(session.state(), SessionState::Failed);
        }
    }

    fn seconds_beyond_instant_range() -> f64 {
        let mut seconds = u64::MAX as f64;
        loop {
            if let Ok(duration) = Duration::try_from_secs_f64(seconds) {
                if Instant::now().checked_add(duration).is_none() {
                    return seconds;
                }
            }
            seconds /= 2.0;
            assert!(seconds >= 1.0, "platform Instant has no finite upper bound");
        }
    }

    #[test]
    fn active_reject_is_payload_but_forced_logout_is_fatal() {
        let reject = codec::encode(&protocol::Reject {
            template_id: REJECT,
            user_msg: vec![],
            rp_code: vec!["permission-denied".to_string()],
        })
        .unwrap();
        let mut rejected_request = activate(Plant::Ticker);
        assert!(!rejected_request.accept_control(&reject).unwrap());
        assert_eq!(rejected_request.state(), SessionState::Active);

        let forced_logout = codec::encode(&protocol::ForcedLogout {
            template_id: FORCED_LOGOUT,
        })
        .unwrap();
        let mut terminated = activate(Plant::Ticker);
        let error = terminated.accept_control(&forced_logout).unwrap_err();
        assert!(is_fatal_session_error(&error));
        assert_eq!(terminated.state(), SessionState::Failed);
    }

    #[test]
    fn terminal_message_handshake_phase_matrix_is_fatal() {
        let terminal_frames = [
            codec::encode(&protocol::Reject {
                template_id: REJECT,
                user_msg: vec![],
                rp_code: vec!["permission-denied".to_string()],
            })
            .unwrap(),
            codec::encode(&protocol::ForcedLogout {
                template_id: FORCED_LOGOUT,
            })
            .unwrap(),
        ];

        for frame in terminal_frames {
            let mut discovery_session = RithmicSession::new(login(Plant::Ticker));
            discovery_session.begin_system_info().unwrap();
            let error = discovery_session.reject_terminal(&frame).unwrap_err();
            assert!(is_fatal_session_error(&error));
            assert_eq!(discovery_session.state(), SessionState::Failed);

            let mut login_session = RithmicSession::new(login(Plant::Ticker));
            login_session.begin_system_info().unwrap();
            login_session
                .accept_system_info(&system_info(&["test-system"], "0"))
                .unwrap();
            login_session.mark_reconnected().unwrap();
            login_session.begin_login().unwrap();
            let error = login_session.reject_terminal(&frame).unwrap_err();
            assert!(is_fatal_session_error(&error));
            assert_eq!(login_session.state(), SessionState::Failed);
        }
    }

    #[test]
    fn handshake_reject_error_chain_excludes_provider_controlled_values() {
        const SAFE_MESSAGE: &str = "stable_error_code=rithmic_handshake_rejected";
        const SENTINELS: [&str; 3] = [
            "provider-user-sentinel",
            "provider-code-sentinel",
            "provider-detail-sentinel",
        ];

        for (user_msg, rp_code) in [
            (
                vec![SENTINELS[0].to_string()],
                vec![SENTINELS[1].to_string(), SENTINELS[2].to_string()],
            ),
            (vec![String::new()], vec![String::new()]),
            (vec![], vec![]),
        ] {
            let reject = codec::encode(&protocol::Reject {
                template_id: REJECT,
                user_msg,
                rp_code,
            })
            .unwrap();

            for mut session in handshake_sessions() {
                let error = session.reject_terminal(&reject).unwrap_err();
                let chain = error.chain().map(ToString::to_string).collect::<Vec<_>>();
                let alternate = format!("{error:#}");

                assert!(is_fatal_session_error(&error));
                assert_eq!(session.state(), SessionState::Failed);
                assert_eq!(error.to_string(), SAFE_MESSAGE);
                assert_eq!(chain, [SAFE_MESSAGE]);
                assert_eq!(alternate, SAFE_MESSAGE);
                for sentinel in SENTINELS {
                    assert!(!alternate.contains(sentinel));
                    assert!(chain.iter().all(|layer| !layer.contains(sentinel)));
                }
            }
        }
    }

    #[test]
    fn response_code_phase_matrix_is_explicit() {
        for (handler, terminal, expected) in [
            (vec!["0"], vec![], Some(ResponseDisposition::Processing)),
            (vec![], vec!["0"], Some(ResponseDisposition::Succeeded)),
            (
                vec!["9", "handler failed"],
                vec![],
                Some(ResponseDisposition::Failed(vec![
                    "9".to_string(),
                    "handler failed".to_string(),
                ])),
            ),
            (
                vec![],
                vec!["7", "no data"],
                Some(ResponseDisposition::Failed(vec![
                    "7".to_string(),
                    "no data".to_string(),
                ])),
            ),
            (vec!["0"], vec!["0"], None),
            (vec![], vec![], None),
        ] {
            let handler = handler.into_iter().map(str::to_string).collect::<Vec<_>>();
            let terminal = terminal.into_iter().map(str::to_string).collect::<Vec<_>>();
            let actual = classify_response_codes(&handler, &terminal);
            match expected {
                Some(expected) => assert_eq!(actual.unwrap(), expected),
                None => assert!(actual.is_err()),
            }
        }
    }

    #[test]
    fn empty_login_fields_are_rejected() {
        assert!(LoginParameters::new(
            "".to_string(),
            "test-password".to_string(),
            "test-system".to_string(),
            "FluxTrade".to_string(),
            "0.1.0".to_string(),
            Plant::Ticker,
        )
        .is_err());
    }
}
