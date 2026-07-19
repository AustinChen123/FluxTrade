use super::session::{LoginParameters, Plant};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::{collections::HashMap, fs, path::Path};

const CREDENTIALS_PATH_ENV: &str = "FLUXTRADE_CREDENTIALS_PATH";

pub(crate) struct RuntimeConfig {
    pub url: String,
    pub login: LoginParameters,
}

#[derive(Deserialize)]
struct CredentialsFile {
    rithmic: HashMap<String, Profile>,
}

#[derive(Deserialize)]
struct Profile {
    user: String,
    password: String,
    system_name: String,
    url: String,
    app_name: Option<String>,
    app_version: Option<String>,
}

pub(crate) fn load(profile: &str, plant: Plant) -> Result<RuntimeConfig> {
    let path = std::env::var(CREDENTIALS_PATH_ENV)
        .with_context(|| format!("{CREDENTIALS_PATH_ENV} is required for Rithmic"))?;
    let contents = fs::read_to_string(Path::new(&path)).with_context(|| {
        format!("failed to read Rithmic credentials from {CREDENTIALS_PATH_ENV}")
    })?;
    parse(&contents, profile, plant)
}

fn parse(contents: &str, profile: &str, plant: Plant) -> Result<RuntimeConfig> {
    ensure!(
        !profile.trim().is_empty(),
        "Rithmic profile must not be empty"
    );
    let mut credentials: CredentialsFile = toml::from_str(contents)
        .map_err(|_| anyhow::anyhow!("invalid Rithmic credentials TOML"))?;
    let profile = credentials
        .rithmic
        .remove(profile)
        .context("configured Rithmic profile was not found")?;
    let url = secure_url(&profile.url)?;
    let login = LoginParameters::new(
        profile.user,
        profile.password,
        profile.system_name,
        profile
            .app_name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "FluxTrade".to_string()),
        profile
            .app_version
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
        plant,
    )?;

    Ok(RuntimeConfig { url, login })
}

fn secure_url(value: &str) -> Result<String> {
    let value = value.trim();
    ensure!(!value.is_empty(), "Rithmic URL must not be empty");
    ensure!(!value.starts_with("ws://"), "Rithmic URL must use TLS");
    if value.starts_with("wss://") {
        Ok(value.to_string())
    } else {
        ensure!(!value.contains("://"), "unsupported Rithmic URL scheme");
        Ok(format!("wss://{value}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[rithmic.test]
user = "user"
password = "secret"
system_name = "test-system"
url = "example.invalid:443"
"#;

    #[test]
    fn profile_maps_to_tls_runtime_config_without_exposing_secrets() {
        let config = parse(VALID, "test", Plant::Ticker).unwrap();
        assert_eq!(config.url, "wss://example.invalid:443");
    }

    #[test]
    fn blank_optional_app_identity_uses_defaults() {
        let contents = format!("{VALID}\napp_name = ' '\napp_version = ''");
        assert!(parse(&contents, "test", Plant::Ticker).is_ok());
    }

    #[test]
    fn profile_validation_matrix_fails_closed() {
        for (contents, profile) in [
            (VALID, "missing"),
            (VALID, ""),
            ("[rithmic.test]\nuser = 'user'", "test"),
            (
                "[rithmic.test]\nuser='user'\npassword='secret'\nsystem_name='system'\nurl='ws://example.invalid'",
                "test",
            ),
            (
                "[rithmic.test]\nuser='user'\npassword='secret'\nsystem_name='system'\nurl='https://example.invalid'",
                "test",
            ),
        ] {
            assert!(parse(contents, profile, Plant::History).is_err());
        }
    }

    #[test]
    fn parse_errors_do_not_echo_credentials() {
        let secret = "do-not-log-this";
        let contents = format!(
            "[rithmic.test]\nuser='user'\npassword='{secret}' trailing\nsystem_name='system'\nurl='example.invalid'"
        );
        let error = match parse(&contents, "test", Plant::Ticker) {
            Ok(_) => panic!("malformed credentials must fail"),
            Err(error) => error,
        };

        assert!(!format!("{error:#}").contains(secret));
    }
}
