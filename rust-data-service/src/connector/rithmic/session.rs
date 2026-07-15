use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};

use super::{codec, protocol};

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
    History,
}

impl Plant {
    fn protocol_value(self) -> i32 {
        match self {
            Self::Ticker => protocol::request_login::SysInfraType::TickerPlant as i32,
            Self::History => protocol::request_login::SysInfraType::HistoryPlant as i32,
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
            ensure_success(&response.rp_code)?;
            ensure!(
                response.system_name.contains(&self.login.system_name),
                "configured Rithmic system is unavailable"
            );
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
            ensure_success(&response.rp_code)?;
            let seconds = response
                .heartbeat_interval
                .context("Rithmic login response omitted heartbeat_interval")?;
            ensure!(
                seconds.is_finite() && seconds > 0.0,
                "Rithmic heartbeat_interval must be finite and positive"
            );
            let interval = Duration::try_from_secs_f64(seconds)
                .context("Rithmic heartbeat_interval is out of range")?;
            ensure!(
                !interval.is_zero(),
                "Rithmic heartbeat_interval is below timer resolution"
            );
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
            REJECT | FORCED_LOGOUT => self.accept_terminal(frame).map(|()| true),
            _ => Ok(false),
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

    pub(crate) fn accept_terminal(&mut self, frame: &[u8]) -> Result<()> {
        let template_id = codec::template_id(frame)?;
        match template_id {
            REJECT => {
                let response: protocol::Reject = codec::decode(frame)?;
                self.state = SessionState::Failed;
                bail!(
                    "Rithmic rejected the session: {}",
                    response.rp_code.join(",")
                );
            }
            FORCED_LOGOUT => {
                let _: protocol::ForcedLogout = codec::decode(frame)?;
                self.state = SessionState::Failed;
                bail!("Rithmic forced the session to log out");
            }
            _ => bail!("Rithmic message {template_id} is not terminal"),
        }
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

fn expect_template(frame: &[u8], expected: i32) -> Result<()> {
    let actual = codec::template_id(frame)?;
    ensure!(
        actual == expected,
        "unexpected Rithmic template: expected {expected}, received {actual}"
    );
    Ok(())
}

fn ensure_success(rp_codes: &[String]) -> Result<()> {
    let code = rp_codes
        .first()
        .context("Rithmic response omitted rp_code")?;
    ensure!(code == "0", "Rithmic response code {code}");
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

    #[test]
    fn login_heartbeat_logout_lifecycle_matrix() {
        for plant in [Plant::Ticker, Plant::History] {
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
    fn login_request_maps_each_v2_plant() {
        for (plant, expected) in [
            (
                Plant::Ticker,
                protocol::request_login::SysInfraType::TickerPlant,
            ),
            (
                Plant::History,
                protocol::request_login::SysInfraType::HistoryPlant,
            ),
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

        assert!(session
            .accept_system_info(&system_info(&["another-system"], "0"))
            .is_err());
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn response_error_fails_closed() {
        let mut session = RithmicSession::new(login(Plant::Ticker));
        session.begin_system_info().unwrap();

        assert!(session
            .accept_system_info(&system_info(&["test-system"], "9"))
            .is_err());
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
            Some(f64::MIN_POSITIVE),
        ] {
            let mut session = RithmicSession::new(login(Plant::Ticker));
            session.begin_system_info().unwrap();
            session
                .accept_system_info(&system_info(&["test-system"], "0"))
                .unwrap();
            session.mark_reconnected().unwrap();
            session.begin_login().unwrap();

            assert!(session
                .accept_login(&login_response(interval, "0"))
                .is_err());
            assert_eq!(session.state(), SessionState::Failed);
        }
    }

    #[test]
    fn reject_and_forced_logout_fail_closed() {
        let cases = [
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

        for frame in cases {
            let mut session = activate(Plant::Ticker);
            assert!(session.accept_terminal(&frame).is_err());
            assert_eq!(session.state(), SessionState::Failed);
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
