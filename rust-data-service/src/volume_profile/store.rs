//! Single trusted-root/job worker. No cleanup, transport, or power-loss guarantee.
use anyhow::{ensure, Result};
use rustix::fd::AsFd;

use super::checkpoint::{self, CheckpointProgress, Identity, Manifest, PageEntry, Recovered};
use super::compressed_page::{self, Limits};
use super::directory::{Directory, Phase};

const MANIFEST: &str = "manifest.json";
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Page,
    Manifest,
    Confirmation,
}

pub struct Store {
    directory: Directory,
    identity: Identity,
    limits: Limits,
    manifest_limit: u64,
    #[cfg(test)]
    fail_recovery_confirmation: std::cell::Cell<bool>,
}

impl Store {
    pub fn open(
        root: &impl AsFd,
        identity: Identity,
        limits: Limits,
        manifest_limit: u64,
    ) -> Result<Self> {
        ensure!(
            manifest_limit > 0 && manifest_limit < u64::MAX,
            "invalid manifest limit"
        );
        let directory = Directory::open(root, identity.job_id())?;
        Ok(Self {
            directory,
            identity,
            limits,
            manifest_limit,
            #[cfg(test)]
            fail_recovery_confirmation: std::cell::Cell::new(false),
        })
    }

    /// Visible metadata is not a durability acknowledgement. Confirm the held
    /// job directory before allowing a caller to consume any recovered progress.
    pub fn recover(&self) -> Result<(Manifest, Recovered)> {
        let recovered = self.load_unconfirmed()?;
        self.confirm(&mut |_, phase| {
            #[cfg(test)]
            if phase == Phase::BeforeDirectorySync {
                ensure!(
                    !self.fail_recovery_confirmation.replace(false),
                    "recovery confirmation unavailable"
                );
            }
            let _ = phase;
            Ok(())
        })?;
        Ok(recovered)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_recovery_confirmation(&self) {
        self.fail_recovery_confirmation.set(true);
    }

    // Only the ACK-unknown comparison may inspect this result without confirming.
    fn load_unconfirmed(&self) -> Result<(Manifest, Recovered)> {
        let manifest = match self.directory.read(MANIFEST, self.manifest_limit) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error)
                if error.downcast_ref::<rustix::io::Errno>() == Some(&rustix::io::Errno::NOENT) =>
            {
                Manifest::new(self.identity.clone(), vec![])
            }
            Err(error) => return Err(error),
        };
        let rebuilt = checkpoint::recover(&manifest, &self.identity, |name| {
            compressed_page::decode(
                &self.directory.read(name, self.limits.archive_limit())?,
                self.limits,
            )
        })?;
        Ok((manifest, rebuilt))
    }

    pub fn append(&self, sequence: u64, raw: &[u8], progress: CheckpointProgress) -> Result<()> {
        self.append_with(sequence, raw, progress, |_, _| Ok(()))
    }

    pub fn append_with(
        &self,
        sequence: u64,
        raw: &[u8],
        progress: CheckpointProgress,
        mut hook: impl FnMut(Target, Phase) -> Result<()>,
    ) -> Result<()> {
        let (current, mut rebuilt) = self.recover()?;
        let entry = PageEntry::new(sequence, raw, progress.clone());
        let count = current.entries().len() as u64;
        ensure!(
            sequence <= count,
            "page sequence skips authoritative cursor"
        );
        if sequence < count {
            ensure!(
                current.entries()[sequence as usize] == entry,
                "referenced page conflict"
            );
            return self.confirm(&mut hook);
        }
        let archive = compressed_page::encode(raw, self.limits)?;
        let (_, actual) = rebuilt.pages.accept(raw)?;
        ensure!(
            CheckpointProgress::try_from(actual)? == progress,
            "page progress mismatch"
        );
        let name = entry.name();
        let mut entries = current.entries().to_vec();
        entries.push(entry);
        let intended = Manifest::new(self.identity.clone(), entries);
        let bytes = serde_json::to_vec(&intended)?;
        ensure!(
            bytes.len() as u64 <= self.manifest_limit,
            "manifest exceeds limit"
        );
        self.directory
            .replace_with(&name, &archive, |phase| hook(Target::Page, phase))?;
        if let Err(error) = self
            .directory
            .replace_with(MANIFEST, &bytes, |phase| hook(Target::Manifest, phase))
        {
            let (observed, _) = self.load_unconfirmed()?;
            ensure!(observed == intended, "manifest commit unconfirmed: {error}");
            self.confirm(&mut hook)?;
        }
        Ok(())
    }

    fn confirm(&self, hook: &mut impl FnMut(Target, Phase) -> Result<()>) -> Result<()> {
        hook(Target::Confirmation, Phase::BeforeDirectorySync)?;
        self.directory.sync()?;
        hook(Target::Confirmation, Phase::DirectorySynced)
    }
}

#[cfg(test)]
mod tests;
