use super::*;
use std::fs::{self, File};
const RAW: &[u8] = b" [[0,\"0\",\"0\",\"0\",\"0\",\"0.00\",86399999,\"0\",0,\"0\",\"0\",\"0\"]] \n";

fn identity() -> Identity {
    Identity::new("daily".into(), Window::new(0, DAY).unwrap()).unwrap()
}

#[test]
fn invalid_provider_wire_cannot_create_immutable_state() {
    use serde_json::{json, Value};
    let valid: Value = serde_json::from_slice(RAW).unwrap();
    let mut invalid = vec![
        json!({"code":-1000,"msg":"error"}),
        json!([]),
        json!([valid[0], valid[0]]),
        json!([[]]),
    ];
    for (index, value) in [
        (0, json!(1)),
        (6, json!(DAY)),
        (0, json!("0")),
        (8, json!(-1)),
        (8, json!(true)),
        (8, json!("0")),
        (8, json!(1.5)),
    ] {
        let mut wire = valid.clone();
        wire[0][index] = value;
        invalid.push(wire);
    }
    for index in [1, 2, 3, 4, 5, 7, 9, 10, 11] {
        for value in [
            json!(0),
            json!("-1"),
            json!("NaN"),
            json!("1e2"),
            json!("79228162514264337593543950336"),
        ] {
            let mut wire = valid.clone();
            wire[0][index] = value;
            invalid.push(wire);
        }
    }
    for wire in invalid {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        assert!(persist(&root, identity(), &serde_json::to_vec(&wire).unwrap(), DAY).is_err());
        assert!(!temp.path().join("daily").exists());
    }
}

#[test]
fn exact_bytes_first_observation_and_conflict_preservation() {
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    assert!(recover(&root, &identity()).is_err());
    assert!(!temp.path().join("daily").exists());
    let raw = RAW;
    let original = persist(&root, identity(), raw, DAY).unwrap();
    assert_eq!(original.raw(), raw);
    assert_eq!(
        persist(&root, identity(), raw, DAY + 100).unwrap(),
        original
    );
    assert_eq!(recover(&root, &identity()).unwrap(), original);
    let different = [RAW, b" "].concat();
    assert!(persist(&root, identity(), &different, DAY).is_err());
    assert_eq!(recover(&root, &identity()).unwrap(), original);
    for observed in [DAY - 1, 253_402_300_800_000] {
        assert!(persist(&root, identity(), raw, observed).is_err());
    }
    for raw in [vec![b' '; RAW_LIMIT + 1], vec![0xff], b"invalid".to_vec()] {
        assert!(persist(&root, identity(), &raw, DAY).is_err());
    }
}

#[test]
fn corrupted_existing_artifact_is_never_overwritten() {
    for field in [
        "unknown", "schema", "identity", "hash", "length", "raw", "oversize",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        persist(&root, identity(), RAW, DAY).unwrap();
        let path = temp.path().join("daily").join(FILE);
        let mut wire: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        match field {
            "unknown" => wire["extra"] = true.into(),
            "schema" => wire["identity"]["schema_version"] = 2.into(),
            "identity" => wire["identity"]["config_sha256"] = "b".repeat(64).into(),
            "hash" => wire["response_sha256"] = "b".repeat(64).into(),
            "length" => wire["response_bytes"] = 1.into(),
            "raw" => wire["raw"] = "not json".into(),
            _ => (),
        }
        let damaged = if field == "oversize" {
            vec![b' '; ARTIFACT_LIMIT as usize + 1]
        } else {
            serde_json::to_vec(&wire).unwrap()
        };
        fs::write(&path, &damaged).unwrap();
        assert!(recover(&root, &identity()).is_err(), "{field}");
        assert!(persist(&root, identity(), RAW, DAY).is_err(), "{field}");
        assert_eq!(fs::read(path).unwrap(), damaged);
    }
}

#[test]
fn nofollow_and_regular_file_boundaries() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let target = tempfile::tempdir().unwrap();
    symlink(target.path(), temp.path().join("daily")).unwrap();
    assert!(persist(&root, identity(), RAW, DAY).is_err());
    assert!(recover(&root, &identity()).is_err());
    fs::remove_file(temp.path().join("daily")).unwrap();
    fs::create_dir(temp.path().join("daily")).unwrap();
    let path = temp.path().join("daily").join(FILE);
    symlink(target.path(), &path).unwrap();
    assert!(persist(&root, identity(), RAW, DAY).is_err());
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap(); // Portable non-regular file, no FIFO sandbox dependency.
    assert!(recover(&root, &identity()).is_err());
    assert!(persist(&root, identity(), RAW, DAY).is_err());
}

#[test]
fn atomic_failure_matrix_and_confirmation_retry() {
    for phase in [
        Phase::Created,
        Phase::Written,
        Phase::FileSynced,
        Phase::Renamed,
        Phase::BeforeDirectorySync,
        Phase::DirectorySynced,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = File::open(temp.path()).unwrap();
        let directory = Directory::open(&root, "daily").unwrap();
        let intended = Evidence::new(identity(), RAW, DAY).unwrap();
        let mut fired = false;
        let result = persist_with(&directory, intended.clone(), |at| {
            if at == phase && !fired {
                fired = true;
                anyhow::bail!("injected");
            }
            Ok(())
        });
        let visible = matches!(
            phase,
            Phase::Renamed | Phase::BeforeDirectorySync | Phase::DirectorySynced
        );
        assert_eq!(result.is_ok(), visible);
        assert_eq!(recover(&root, &identity()).is_ok(), visible);
        assert_eq!(persist(&root, identity(), RAW, DAY).unwrap(), intended);
    }
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let directory = Directory::open(&root, "daily").unwrap();
    let intended = Evidence::new(identity(), RAW, DAY).unwrap();
    assert!(persist_with(&directory, intended.clone(), |phase| {
        ensure!(
            phase != Phase::BeforeDirectorySync,
            "confirmation unavailable"
        );
        Ok(())
    })
    .is_err());
    assert_eq!(recover(&root, &identity()).unwrap(), intended);
}
