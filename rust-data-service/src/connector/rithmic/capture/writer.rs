use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_RETAINED_FILES: usize = 2;
const MAX_OWNER_NAMESPACES: usize = 64;

pub(super) struct OwnerWriter {
    path: PathBuf,
    backup: PathBuf,
    lock: File,
    file: File,
    size: u64,
    #[cfg(test)]
    fail_next_write: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OwnerOpenError {
    Unavailable,
    Conflict,
    NamespaceLimit,
}

impl From<()> for OwnerOpenError {
    fn from(_: ()) -> Self {
        Self::Unavailable
    }
}

impl OwnerWriter {
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn open(directory: &Path) -> Result<Self, OwnerOpenError> {
        use std::os::unix::fs::MetadataExt;
        if !directory.is_absolute() {
            return Err(OwnerOpenError::Unavailable);
        }
        let root_device = fs::metadata("/").map_err(|_| ())?.dev();
        let mut existing = directory;
        while !existing.exists() {
            existing = existing.parent().ok_or(())?;
        }
        if fs::metadata(existing).map_err(|_| ())?.dev() == root_device {
            return Err(OwnerOpenError::Unavailable);
        }
        let mut directory_builder = fs::DirBuilder::new();
        directory_builder.recursive(true).mode(0o700);
        directory_builder
            .create(directory)
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .map_err(|_| ())?;
        if directory.canonicalize().map_err(|_| ())? != directory {
            return Err(OwnerOpenError::Unavailable);
        }
        let directory_metadata = fs::metadata(directory).map_err(|_| ())?;
        if directory_metadata.dev() == root_device
            || directory_metadata.permissions().mode() & 0o077 != 0
        {
            return Err(OwnerOpenError::Unavailable);
        }
        let lock_path = directory.join(".rithmic-capture-owner.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(lock_path)
            .map_err(|_| ())?;
        if fs::metadata(directory.join(".rithmic-capture-owner.lock"))
            .map_err(|_| ())?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err(OwnerOpenError::Unavailable);
        }
        use rustix::fs::{flock, FlockOperation};
        flock(&lock, FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| OwnerOpenError::Conflict)?;

        let owners = fs::read_dir(directory)
            .map_err(|_| ())?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("owner-"))
            .count();
        if owners >= MAX_OWNER_NAMESPACES {
            return Err(OwnerOpenError::NamespaceLimit);
        }

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ())?
            .as_nanos();
        let namespace = directory.join(format!("owner-{}-{nonce}", std::process::id()));
        let mut namespace_builder = fs::DirBuilder::new();
        namespace_builder
            .mode(0o700)
            .create(&namespace)
            .map_err(|_| ())?;
        let path = namespace.join("capture.jsonl");
        let backup = path.with_extension("jsonl.1");
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .mode(0o600)
            .open(&path)
            .map_err(|_| ())?;
        Ok(Self {
            path,
            backup,
            lock,
            file,
            size: 0,
            #[cfg(test)]
            fail_next_write: false,
        })
    }

    pub(super) fn write_record(&mut self, bytes: &[u8]) -> Result<(), ()> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_write) {
            return Err(());
        }
        if self.size.saturating_add(bytes.len() as u64) > MAX_FILE_BYTES {
            self.rotate()?;
        }
        self.file.write_all(bytes).map_err(|_| ())?;
        self.size = self.size.saturating_add(bytes.len() as u64);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn fail_next_write_for_test(&mut self) {
        self.fail_next_write = true;
    }

    fn rotate(&mut self) -> Result<(), ()> {
        self.file.flush().map_err(|_| ())?;
        drop(std::mem::replace(
            &mut self.file,
            OpenOptions::new()
                .write(true)
                .open(&self.path)
                .map_err(|_| ())?,
        ));
        if MAX_RETAINED_FILES >= 2 {
            fs::remove_file(&self.backup).ok();
        }
        fs::rename(&self.path, &self.backup).map_err(|_| ())?;
        self.file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .mode(0o600)
            .open(&self.path)
            .map_err(|_| ())?;
        self.size = 0;
        Ok(())
    }
}

impl Drop for OwnerWriter {
    fn drop(&mut self) {
        let _ = self.file.flush();
        let _ = &self.lock;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simultaneous_owner_is_reported_as_a_distinct_conflict() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let first = OwnerWriter::open(root.path()).unwrap();
        assert!(matches!(
            OwnerWriter::open(root.path()),
            Err(OwnerOpenError::Conflict)
        ));
        drop(first);
    }

    #[test]
    fn writer_uses_a_new_private_namespace_and_leaves_prior_tails_separate() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let old_owner = root.path().join("owner-old");
        fs::create_dir(&old_owner).unwrap();
        fs::write(old_owner.join("capture.jsonl"), b"partial-tail").unwrap();

        let mut writer = OwnerWriter::open(root.path()).unwrap();
        writer.write_record(b"{\"schema_version\":1}\n").unwrap();
        let current_path = writer.path.clone();
        assert_ne!(current_path.parent().unwrap(), old_owner);
        drop(writer);

        assert_eq!(
            fs::read(&current_path).unwrap(),
            b"{\"schema_version\":1}\n"
        );
        assert_eq!(
            fs::read(old_owner.join("capture.jsonl")).unwrap(),
            b"partial-tail"
        );
        let namespace_mode = fs::metadata(current_path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let file_mode = fs::metadata(&current_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(namespace_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[test]
    fn writer_fails_closed_at_owner_namespace_quota_without_deleting_prior_data() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        for owner in 0..MAX_OWNER_NAMESPACES {
            fs::create_dir(root.path().join(format!("owner-existing-{owner}"))).unwrap();
        }

        assert!(OwnerWriter::open(root.path()).is_err());
        assert_eq!(
            fs::read_dir(root.path()).unwrap().count(),
            MAX_OWNER_NAMESPACES + 1
        );
    }

    #[test]
    fn rotation_keeps_only_the_current_and_one_complete_prior_segment() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let mut writer = OwnerWriter::open(root.path()).unwrap();
        let mut record = vec![b'x'; MAX_FILE_BYTES as usize - 1];
        record.push(b'\n');
        writer.write_record(&record).unwrap();
        writer.write_record(b"next\n").unwrap();
        writer.write_record(&record).unwrap();
        writer.write_record(b"latest\n").unwrap();
        assert_eq!(fs::metadata(&writer.path).unwrap().len(), 7);
        assert_eq!(fs::metadata(&writer.backup).unwrap().len(), MAX_FILE_BYTES);
        let files = fs::read_dir(writer.path.parent().unwrap()).unwrap().count();
        assert_eq!(files, MAX_RETAINED_FILES);
    }
}
