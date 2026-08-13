use crate::connector::backpack::BackpackConnector;
use crate::environment::RuntimeEnvironment;
use anyhow::Context;
#[cfg(feature = "rithmic")]
use tracing::info;

pub(crate) enum EmergencyMitigation {
    LockdownOnly,
    Backpack(BackpackConnector),
    #[cfg(feature = "rithmic")]
    Rithmic {
        profile: String,
        account_id: String,
    },
}

impl EmergencyMitigation {
    pub(crate) async fn run(&self) -> anyhow::Result<()> {
        match self {
            Self::LockdownOnly => Ok(()),
            Self::Backpack(connector) => connector.cancel_all_orders().await,
            #[cfg(feature = "rithmic")]
            Self::Rithmic {
                profile,
                account_id,
            } => {
                let result =
                    crate::connector::rithmic::emergency::mitigate(profile, account_id).await?;
                info!(
                    cancelled_orders = result.cancelled_orders,
                    exited_positions = result.exited_positions,
                    "Rithmic emergency mitigation verified flat"
                );
                Ok(())
            }
        }
    }
}

pub(crate) fn resolve(
    environment: &RuntimeEnvironment,
    execution_venue: Option<&str>,
    rithmic_identity: Option<(&str, &str)>,
) -> anyhow::Result<EmergencyMitigation> {
    #[cfg(not(feature = "rithmic"))]
    let _ = rithmic_identity;
    if !environment.allows_external_kill() {
        return Ok(EmergencyMitigation::LockdownOnly);
    }
    let venue = execution_venue
        .context("EXCHANGE_ID must be set explicitly in live")?
        .trim()
        .to_ascii_lowercase();
    match venue.as_str() {
        "backpack" => Ok(EmergencyMitigation::Backpack(BackpackConnector::new())),
        #[cfg(feature = "rithmic")]
        "rithmic" => {
            let (profile, account_id) =
                rithmic_identity.context("Rithmic watchdog requires live Rithmic configuration")?;
            Ok(EmergencyMitigation::Rithmic {
                profile: profile.to_string(),
                account_id: account_id.to_string(),
            })
        }
        _ => anyhow::bail!("unsupported emergency execution venue: {venue}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_live_never_builds_external_mitigation() {
        let mitigation = resolve(
            &RuntimeEnvironment::new("test").unwrap(),
            Some("backpack"),
            None,
        )
        .unwrap();

        assert!(matches!(mitigation, EmergencyMitigation::LockdownOnly));
    }

    #[test]
    fn live_requires_supported_explicit_execution_venue() {
        let environment = RuntimeEnvironment::new("live").unwrap();

        assert!(resolve(&environment, None, None)
            .err()
            .unwrap()
            .to_string()
            .contains("EXCHANGE_ID"));
        assert!(resolve(&environment, Some("binance"), None)
            .err()
            .unwrap()
            .to_string()
            .contains("unsupported emergency execution venue"));
    }

    #[test]
    fn live_backpack_uses_the_existing_connector_owner() {
        let mitigation = resolve(
            &RuntimeEnvironment::new("live").unwrap(),
            Some(" BACKPACK "),
            None,
        )
        .unwrap();

        assert!(matches!(mitigation, EmergencyMitigation::Backpack(_)));
    }

    #[cfg(feature = "rithmic")]
    #[test]
    fn live_rithmic_preserves_exact_profile_and_account() {
        let mitigation = resolve(
            &RuntimeEnvironment::new("live").unwrap(),
            Some("RITHMIC"),
            Some(("profile-a", "TEST_ACCOUNT_001")),
        )
        .unwrap();

        match mitigation {
            EmergencyMitigation::Rithmic {
                profile,
                account_id,
            } => {
                assert_eq!(profile, "profile-a");
                assert_eq!(account_id, "TEST_ACCOUNT_001");
            }
            _ => panic!("expected Rithmic emergency mitigation"),
        }
    }
}
