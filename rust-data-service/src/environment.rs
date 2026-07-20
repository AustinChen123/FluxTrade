use anyhow::{ensure, Context, Result};

pub const ENVIRONMENT_VARIABLE: &str = "FLUXTRADE_ENVIRONMENT";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeEnvironment(String);

impl RuntimeEnvironment {
    pub fn from_env() -> Result<Self> {
        Self::new(
            std::env::var(ENVIRONMENT_VARIABLE)
                .with_context(|| format!("{} must be set explicitly", ENVIRONMENT_VARIABLE))?,
        )
    }

    pub fn new(identity: impl Into<String>) -> Result<Self> {
        let identity = identity.into();
        let bytes = identity.as_bytes();
        ensure!(
            !bytes.is_empty()
                && bytes[0].is_ascii_alphanumeric()
                && bytes[bytes.len() - 1].is_ascii_alphanumeric()
                && bytes.iter().all(|byte| byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || *byte == b'-'),
            "{} must contain lowercase ASCII letters, digits, or internal hyphens",
            ENVIRONMENT_VARIABLE
        );
        Ok(Self(identity))
    }

    pub fn key(&self, suffix: &str) -> String {
        format!("fluxtrade:{}:{}", self.0, suffix)
    }

    pub fn allows_external_kill(&self) -> bool {
        self.0 == "live"
    }

    pub fn identity(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_isolated_by_environment() {
        let live = RuntimeEnvironment::new("live").unwrap();
        let test = RuntimeEnvironment::new("test").unwrap();

        assert_eq!(live.key("system:state"), "fluxtrade:live:system:state");
        assert_eq!(test.key("system:state"), "fluxtrade:test:system:state");
        assert_ne!(live.key("heartbeat:python"), test.key("heartbeat:python"));
        assert!(live.allows_external_kill());
        assert!(!test.allows_external_kill());
    }

    #[test]
    fn ambiguous_environment_identity_is_rejected() {
        for identity in ["", " test", "TEST", "test_1", "-test", "test-"] {
            assert!(RuntimeEnvironment::new(identity).is_err());
        }
    }
}
