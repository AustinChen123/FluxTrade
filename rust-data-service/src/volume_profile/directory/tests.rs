use super::*;
use std::os::unix::fs::{symlink, PermissionsExt};

fn setup() -> (tempfile::TempDir, Directory) {
    let root = tempfile::tempdir().unwrap();
    let dir = Directory::open(&File::open(root.path()).unwrap(), "job").unwrap();
    (root, dir)
}

#[test]
fn every_failure_boundary_has_old_or_new_final_and_no_partial_file() {
    for phase in [
        Phase::Created,
        Phase::Written,
        Phase::FileSynced,
        Phase::Renamed,
        Phase::BeforeDirectorySync,
        Phase::DirectorySynced,
    ] {
        let (root, dir) = setup();
        dir.replace("final", b"old").unwrap();
        let result = dir.replace_with("final", b"new", |at| {
            if at == phase {
                anyhow::bail!("injected {at:?}");
            }
            Ok(())
        });
        assert!(result.is_err());
        let expected = if matches!(phase, Phase::Created | Phase::Written | Phase::FileSynced) {
            b"old"
        } else {
            b"new"
        };
        assert_eq!(dir.read("final", 3).unwrap(), expected);
        assert_eq!(
            std::fs::read_dir(root.path().join("job")).unwrap().count(),
            1
        );
    }
}

#[test]
fn collision_does_not_truncate_or_unlink_existing_temp() {
    let (root, dir) = setup();
    dir.replace("occupied", b"keep").unwrap();
    assert!(dir
        .replace_named("final", b"new", "occupied", |_| Ok(()))
        .is_err());
    assert_eq!(dir.read("occupied", 4).unwrap(), b"keep");
    assert!(!root.path().join("job/final").exists());
    assert!(dir
        .replace_named("occupied", b"new", "occupied", |_| Ok(()))
        .is_err());
}

#[test]
fn symlinks_and_special_files_never_escape_or_block() {
    let (root, dir) = setup();
    let outside = root.path().join("outside");
    std::fs::write(&outside, b"safe").unwrap();
    symlink(&outside, root.path().join("job/link")).unwrap();
    assert!(dir.read("link", 10).is_err());
    assert!(dir
        .replace_named("final", b"new", "link", |_| Ok(()))
        .is_err());
    dir.replace("link", b"replacement").unwrap();
    assert_eq!(std::fs::read(&outside).unwrap(), b"safe");
    symlink(root.path(), root.path().join("linked-job")).unwrap();
    assert!(Directory::open(&File::open(root.path()).unwrap(), "linked-job").is_err());
    mkdirat(&dir.fd, "subdir", Mode::RWXU).unwrap();
    assert!(dir.read("subdir", 10).is_err());
    let _socket = std::os::unix::net::UnixListener::bind(root.path().join("job/socket")).unwrap();
    assert!(dir.read("socket", 10).is_err());
    #[cfg(target_os = "linux")]
    {
        rustix::fs::mkfifoat(&dir.fd, "fifo", Mode::RUSR | Mode::WUSR).unwrap();
        assert!(dir.read("fifo", 10).is_err());
    }
    assert!(dir
        .replace("subdir", b"cannot rename over directory")
        .is_err());
}

#[test]
fn unsafe_components_bounds_permissions_and_fd_anchor() {
    let (root, dir) = setup();
    let root_file = File::open(root.path()).unwrap();
    for name in [
        "",
        ".",
        "..",
        "../escape",
        "a/b",
        "a\\b",
        "a b",
        "/absolute",
    ] {
        assert!(Directory::open(&root_file, name).is_err());
        assert!(dir.replace(name, b"no").is_err());
        assert!(dir.read(name, 10).is_err());
    }
    dir.replace("data", b"1234").unwrap();
    assert!(dir.read("data", 3).is_err());
    assert_eq!(dir.read("data", 4).unwrap(), b"1234");
    assert_eq!(dir.read("data", 5).unwrap(), b"1234");
    assert!(dir
        .read_with("data", 4, || Ok(std::fs::write(
            root.path().join("job/data"),
            b"12345"
        )?))
        .is_err());
    dir.replace("data", b"1234").unwrap();
    assert!(dir.read("data", u64::MAX).is_err());
    assert_eq!(
        std::fs::metadata(root.path().join("job/data"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let moved = root.path().join("moved");
    std::fs::rename(root.path().join("job"), &moved).unwrap();
    dir.replace("anchored", b"yes").unwrap();
    assert_eq!(std::fs::read(moved.join("anchored")).unwrap(), b"yes");
    let container = tempfile::tempdir().unwrap();
    let old = container.path().join("root");
    std::fs::create_dir(&old).unwrap();
    let root_fd = File::open(&old).unwrap();
    std::fs::rename(&old, container.path().join("renamed")).unwrap();
    Directory::open(&root_fd, "job")
        .unwrap()
        .replace("data", b"ok")
        .unwrap();
    assert_eq!(
        std::fs::read(container.path().join("renamed/job/data")).unwrap(),
        b"ok"
    );
}
