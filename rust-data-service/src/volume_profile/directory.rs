//! Single-worker atomic files anchored to directory descriptors (macOS/Linux).
//! Root/job directories must not be writable by untrusted writers. No path
//! precheck proves safety. fsync errors propagate; macOS fsync alone does not
//! promise hardware power-loss durability. After any replace error, reread.
use std::fs::File;
use std::io::{Read, Write};

use anyhow::{ensure, Result};
use ring::rand::{SecureRandom, SystemRandom};
use rustix::fd::{AsFd, OwnedFd};
use rustix::fs::{
    fstat, fsync, mkdirat, openat, renameat, statat, unlinkat, AtFlags, FileType, Mode, OFlags,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Created,
    Written,
    FileSynced,
    Renamed,
    BeforeDirectorySync,
    DirectorySynced,
}

pub struct Directory {
    fd: OwnedFd,
}

fn leaf(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}

impl Directory {
    pub(crate) fn exists(&self, name: &str) -> Result<bool> {
        ensure!(leaf(name), "unsafe leaf name");
        match statat(&self.fd, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => Ok(true),
            Err(rustix::io::Errno::NOENT) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) fn unlink_if_present(&self, name: &str) -> Result<()> {
        ensure!(leaf(name), "unsafe leaf name");
        match unlinkat(&self.fd, name, AtFlags::empty()) {
            Ok(()) | Err(rustix::io::Errno::NOENT) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) fn sync(&self) -> Result<()> {
        Ok(fsync(&self.fd)?)
    }
    pub fn open(root: &impl AsFd, job_id: &str) -> Result<Self> {
        Self::open_mode(root, job_id, true)
    }

    pub fn open_existing(root: &impl AsFd, job_id: &str) -> Result<Self> {
        Self::open_mode(root, job_id, false)
    }

    fn open_mode(root: &impl AsFd, job_id: &str, create: bool) -> Result<Self> {
        ensure!(
            !job_id.is_empty()
                && job_id.len() <= 64
                && job_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c)),
            "unsafe job ID"
        );
        if create {
            match mkdirat(root, job_id, Mode::RWXU) {
                Ok(()) => (),
                Err(rustix::io::Errno::EXIST) => (),
                Err(error) => return Err(error.into()),
            }
        }
        let fd = openat(
            root,
            job_id,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        if create {
            fsync(root)?;
        }
        Ok(Self { fd })
    }

    pub fn replace(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.replace_with(name, bytes, |_| Ok(()))
    }

    pub fn replace_with(
        &self,
        name: &str,
        bytes: &[u8],
        hook: impl FnMut(Phase) -> Result<()>,
    ) -> Result<()> {
        let mut nonce = [0_u8; 16];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| anyhow::anyhow!("temp nonce failure"))?;
        let temp = format!(".tmp-{:032x}", u128::from_le_bytes(nonce));
        self.replace_named(name, bytes, &temp, hook)
    }

    fn replace_named(
        &self,
        name: &str,
        bytes: &[u8],
        temp: &str,
        mut hook: impl FnMut(Phase) -> Result<()>,
    ) -> Result<()> {
        ensure!(leaf(name) && leaf(temp) && name != temp, "unsafe leaf name");
        let fd = openat(
            &self.fd,
            temp,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?;
        let mut file = File::from(fd);
        let result = (|| {
            hook(Phase::Created)?;
            file.write_all(bytes)?;
            hook(Phase::Written)?;
            fsync(&file)?;
            hook(Phase::FileSynced)?;
            renameat(&self.fd, temp, &self.fd, name)?;
            hook(Phase::Renamed)?;
            hook(Phase::BeforeDirectorySync)?;
            fsync(&self.fd)?;
            hook(Phase::DirectorySynced)
        })();
        if result.is_err() {
            let _ = unlinkat(&self.fd, temp, AtFlags::empty());
        }
        result
    }

    pub fn read(&self, name: &str, limit: u64) -> Result<Vec<u8>> {
        self.read_with(name, limit, || Ok(()))
    }

    fn read_with(
        &self,
        name: &str,
        limit: u64,
        before_read: impl FnOnce() -> Result<()>,
    ) -> Result<Vec<u8>> {
        ensure!(leaf(name), "unsafe leaf name");
        let cap = limit
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("read limit overflow"))?;
        let fd = openat(
            &self.fd,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        )?;
        let stat = fstat(&fd)?;
        ensure!(
            FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile,
            "not a regular file"
        );
        ensure!(
            u64::try_from(stat.st_size)? <= limit,
            "file exceeds read limit"
        );
        before_read()?;
        let mut bytes = Vec::new();
        File::from(fd).take(cap).read_to_end(&mut bytes)?;
        ensure!(bytes.len() as u64 <= limit, "file grew beyond read limit");
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
