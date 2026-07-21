use anyhow::{ensure, Result};
use std::{
    collections::HashSet,
    sync::{LazyLock, Mutex},
};

static ACTIVE_PROFILES: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) struct ProfileLease {
    profile: String,
}

impl ProfileLease {
    pub(crate) fn acquire(profile: &str) -> Result<Self> {
        let profile = profile.trim();
        ensure!(!profile.is_empty(), "Rithmic profile must not be empty");
        let mut active = ACTIVE_PROFILES
            .lock()
            .map_err(|_| anyhow::anyhow!("Rithmic profile registry is unavailable"))?;
        ensure!(
            active.insert(profile.to_string()),
            "Rithmic profile already has an active session"
        );
        Ok(Self {
            profile: profile.to_string(),
        })
    }
}

impl Drop for ProfileLease {
    fn drop(&mut self) {
        if let Ok(mut active) = ACTIVE_PROFILES.lock() {
            active.remove(&self.profile);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_lease_is_exclusive_and_released_on_drop() {
        let first = ProfileLease::acquire("profile-lock-test").unwrap();
        assert!(ProfileLease::acquire("profile-lock-test").is_err());
        drop(first);
        assert!(ProfileLease::acquire("profile-lock-test").is_ok());
    }
}
