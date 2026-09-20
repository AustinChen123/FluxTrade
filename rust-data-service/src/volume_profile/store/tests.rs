use super::*;
use crate::volume_profile::Window;
use serde_json::json;
use std::fs::File;

fn setup() -> (tempfile::TempDir, Store) {
    let root = tempfile::tempdir().unwrap();
    let identity = Identity::new(
        "job".into(),
        Window::new(100, 200).unwrap(),
        "grid".into(),
        [1; 32],
    )
    .unwrap();
    let store = Store::open(
        &File::open(root.path()).unwrap(),
        identity,
        Limits::new(4096, 1024).unwrap(),
        4096,
    )
    .unwrap();
    (root, store)
}
fn body(ids: &[u64]) -> Vec<u8> {
    serde_json::to_vec(
        &ids.iter()
            .map(|i| json!({"a":i,"p":"10","q":"1","T":100,"f":i,"l":i,"m":true,"M":true}))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}
fn ids(store: &Store) -> Vec<u64> {
    store
        .recover()
        .unwrap()
        .1
        .trades
        .iter()
        .map(|t| t.id)
        .collect()
}

#[test]
fn recovery_orphans_idempotency_and_conflicts() {
    let (_root, s) = setup();
    let first = body(&[0, 1]);
    let entry = PageEntry::new(0, &first, CheckpointProgress::Next(2));
    let orphan = compressed_page::encode(&body(&[9]), s.limits).unwrap();
    s.directory.replace(&entry.name(), &orphan).unwrap();
    assert!(ids(&s).is_empty());
    assert!(s.append(0, &first, CheckpointProgress::Next(3)).is_err());
    s.append(0, &first, CheckpointProgress::Next(2)).unwrap();
    let before = s.directory.read(MANIFEST, 4096).unwrap();
    s.append(0, &first, CheckpointProgress::Next(2)).unwrap();
    assert_eq!(s.directory.read(MANIFEST, 4096).unwrap(), before);
    assert!(s
        .append(0, &body(&[0]), CheckpointProgress::Next(2))
        .is_err());
    assert!(s.append(0, &first, CheckpointProgress::Next(3)).is_err());
    assert!(s
        .append(2, &body(&[2]), CheckpointProgress::Next(3))
        .is_err());
    s.append(1, &body(&[1, 2]), CheckpointProgress::Next(3))
        .unwrap();
    s.append(2, b"[]", CheckpointProgress::EmptyPage).unwrap();
    assert_eq!(ids(&s), vec![0, 1, 2]);
    let mut live = crate::volume_profile::binance_spot::Pages::new(Window::new(100, 200).unwrap());
    for raw in [&first, &body(&[1, 2]), &b"[]".to_vec()] {
        live.accept(raw).unwrap();
    }
    assert_eq!(s.recover().unwrap().1.pages, live);
}

#[test]
fn page_manifest_fault_matrix_converges_without_duplicate_ids() {
    for target in [Target::Page, Target::Manifest] {
        for phase in [
            Phase::Created,
            Phase::Written,
            Phase::FileSynced,
            Phase::Renamed,
            Phase::BeforeDirectorySync,
            Phase::DirectorySynced,
        ] {
            let (_root, s) = setup();
            let raw = body(&[0]);
            let result = s.append_with(0, &raw, CheckpointProgress::Next(1), |t, p| {
                if (t, p) == (target, phase) {
                    anyhow::bail!("injected");
                }
                Ok(())
            });
            let published = target == Target::Manifest
                && matches!(
                    phase,
                    Phase::Renamed | Phase::BeforeDirectorySync | Phase::DirectorySynced
                );
            assert_eq!(result.is_ok(), published, "{target:?}/{phase:?}");
            assert_eq!(ids(&s), if published { vec![0] } else { vec![] });
            s.append(0, &raw, CheckpointProgress::Next(1)).unwrap();
            assert_eq!(ids(&s), vec![0]);
        }
    }
    let (_root, s) = setup();
    let result = s.append_with(0, &body(&[0]), CheckpointProgress::Next(1), |t, p| {
        if t == Target::Confirmation || (t == Target::Manifest && p == Phase::Renamed) {
            anyhow::bail!("sync unavailable");
        }
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(ids(&s), vec![0]);
    s.append(0, &body(&[0]), CheckpointProgress::Next(1))
        .unwrap();
    assert_eq!(ids(&s), vec![0]);
}

#[test]
fn corrupt_oversize_and_mismatched_authority_fail_closed() {
    let (_root, mut bounded) = setup();
    bounded.manifest_limit = 1;
    assert!(bounded
        .append(0, &body(&[0]), CheckpointProgress::Next(1))
        .is_err());
    assert!(ids(&bounded).is_empty());
    let (_root, s) = setup();
    for bytes in [b"broken".to_vec(), vec![b'x'; 4097]] {
        s.directory.replace(MANIFEST, &bytes).unwrap();
        assert!(s.recover().is_err());
        assert!(s
            .append(0, &body(&[0]), CheckpointProgress::Next(1))
            .is_err());
    }
    let bad = Identity::new(
        "other".into(),
        Window::new(100, 200).unwrap(),
        "grid".into(),
        [1; 32],
    )
    .unwrap();
    s.directory
        .replace(
            MANIFEST,
            &serde_json::to_vec(&Manifest::new(bad, vec![])).unwrap(),
        )
        .unwrap();
    assert!(s.recover().is_err());
    let (_root, s) = setup();
    s.append(0, &body(&[0]), CheckpointProgress::Next(1))
        .unwrap();
    let page = PageEntry::new(0, &body(&[0]), CheckpointProgress::Next(1)).name();
    for bytes in [b"broken".to_vec(), vec![b'x'; 4097]] {
        s.directory.replace(&page, &bytes).unwrap();
        assert!(s.recover().is_err());
        assert!(s
            .append(0, &body(&[0]), CheckpointProgress::Next(1))
            .is_err());
    }
}
