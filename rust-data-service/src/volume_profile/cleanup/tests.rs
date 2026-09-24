use super::super::checkpoint::CheckpointProgress;
use super::*;
use std::fs::{self, File};
const DAY: i64 = 86_400_000;
const RAW: &[u8] = b"[[0,\"0\",\"0\",\"0\",\"0\",\"0\",86399999,\"0\",0,\"0\",\"0\",\"0\"]]";
fn day() -> Window {
    Window::new(0, DAY).unwrap()
}
fn id() -> kline_evidence::Identity {
    kline_evidence::Identity::new("daily".into(), day()).unwrap()
}
fn seed() -> (tempfile::TempDir, File, String, String) {
    let temp = tempfile::tempdir().unwrap();
    let root = File::open(temp.path()).unwrap();
    let mut inputs = vec![];
    kline_evidence::persist(&root, id(), RAW, DAY).unwrap();
    for hour in 0..24 {
        let identity = hourly(&id(), hour).unwrap();
        let job = identity.job_id().to_string();
        let store = Store::open(
            &root,
            identity,
            compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW).unwrap(),
            mvp::MANIFEST,
        )
        .unwrap();
        store
            .append(0, b"[]", CheckpointProgress::EmptyPage)
            .unwrap();
        inputs.push(store.recover().unwrap());
        fs::write(temp.path().join(job).join("extra.bin"), b"retain").unwrap();
    }
    let mut bytes = daily::assemble("daily", day(), mvp::config_hash(), &inputs, RAW, DAY)
        .unwrap()
        .to_bytes()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let content = value["content_sha256"].as_str().unwrap().to_string();
    bytes.push(b'\n');
    (temp, root, content, sha(&bytes))
}
fn raw_present(temp: &tempfile::TempDir) {
    assert!(temp.path().join("daily").join(KLINE).exists());
    for hour in 0..24 {
        assert!(temp
            .path()
            .join(hourly(&id(), hour).unwrap().job_id())
            .join("page-00000000000000000000.bin")
            .exists());
    }
}
fn complete(temp: &tempfile::TempDir) {
    assert!(!temp.path().join("daily").join(KLINE).exists());
    assert!(temp.path().join("daily").join(MARKER).exists());
    for hour in 0..24 {
        let path = temp.path().join(hourly(&id(), hour).unwrap().job_id());
        assert!(!path.join("page-00000000000000000000.bin").exists());
        for file in [MANIFEST, TOMBSTONE, "extra.bin"] {
            assert!(path.join(file).exists());
        }
    }
}
#[test]
fn exact_cleanup_retains_authority_and_blocks_recreation() {
    let (temp, root, content, handoff) = seed();
    let identity = hourly(&id(), 0).unwrap();
    let open = Store::open(
        &root,
        identity.clone(),
        compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW).unwrap(),
        mvp::MANIFEST,
    )
    .unwrap();
    let report = run(&root, "daily", day(), &content, Some(&handoff)).unwrap();
    complete(&temp);
    assert_eq!(report.referenced_pages, 24);
    assert_eq!(report.handoff_sha256, handoff);
    assert!(kline_evidence::persist(&root, id(), RAW, DAY).is_err());
    assert!(open
        .append(0, b"[]", CheckpointProgress::EmptyPage)
        .is_err());
    assert!(Store::open(
        &root,
        identity,
        compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW).unwrap(),
        mvp::MANIFEST
    )
    .is_err());
    assert_eq!(
        serde_json::to_vec(&run(&root, "daily", day(), &content, None).unwrap()).unwrap(),
        serde_json::to_vec(&report).unwrap()
    );
}

#[test]
fn held_store_cannot_idempotently_append_before_any_raw_unlink() {
    let (temp, root, content, handoff) = seed();
    let identity = hourly(&id(), 0).unwrap();
    let path = temp.path().join(identity.job_id());
    let store = Store::open(
        &root,
        identity,
        compressed_page::Limits::new(mvp::ARCHIVE, mvp::RAW).unwrap(),
        mvp::MANIFEST,
    )
    .unwrap();
    let manifest = fs::read(path.join(MANIFEST)).unwrap();
    let page = fs::read(path.join("page-00000000000000000000.bin")).unwrap();
    assert!(run_with(
        &root,
        "daily",
        day(),
        &content,
        Some(&handoff),
        |target, step| {
            anyhow::ensure!(
                target != Target::Page(0, 0) || step != Step::BeforeUnlink,
                "pause before unlink"
            );
            Ok(())
        }
    )
    .is_err());
    raw_present(&temp);
    assert_fetch_retired(&root);
    for hour in 0..24 {
        assert!(temp
            .path()
            .join(hourly(&id(), hour).unwrap().job_id())
            .join(TOMBSTONE)
            .exists());
    }
    assert_eq!(
        store
            .append(0, b"[]", CheckpointProgress::EmptyPage)
            .unwrap_err()
            .to_string(),
        "hour retired"
    );
    assert_eq!(fs::read(path.join(MANIFEST)).unwrap(), manifest);
    assert_eq!(
        fs::read(path.join("page-00000000000000000000.bin")).unwrap(),
        page
    );
    run(&root, "daily", day(), &content, None).unwrap();
    complete(&temp);
    assert_fetch_retired(&root);
}

fn assert_fetch_retired(root: &File) {
    struct Forbidden;
    #[async_trait::async_trait]
    impl super::super::kline_fetch::Source for Forbidden {
        async fn fetch(&mut self, _: Window, _: i64) -> Result<super::super::transport::Outcome> {
            panic!("retired fetch");
        }
    }
    let error = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(super::super::kline_fetch::run(
            root,
            &id(),
            &mut Forbidden,
            || panic!("retired clock"),
            || panic!("retired elapsed"),
            |_| async { panic!("retired wait") },
        ))
        .unwrap_err();
    assert_eq!(error.to_string(), "daily source retired");
    assert!(kline_evidence::recover(root, &id()).is_err());
    assert!(kline_evidence::recover_optional(root, &id()).is_err());
}
#[test]
fn initial_wrong_hash_or_source_never_marks_or_deletes() {
    for mode in [
        "content",
        "handoff",
        "missing-hash",
        "source",
        "missing-source",
        "day",
        "job",
        "config",
        "identity",
    ] {
        let (temp, root, content, handoff) = seed();
        if matches!(mode, "config" | "identity") {
            let path = temp
                .path()
                .join(hourly(&id(), 0).unwrap().job_id())
                .join(MANIFEST);
            let mut wire: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            if mode == "config" {
                wire["identity"]["config_sha256"] = serde_json::to_value([0_u8; 32]).unwrap();
            } else {
                wire["identity"]["job_id"] = "other".into();
            }
            fs::write(&path, serde_json::to_vec(&wire).unwrap()).unwrap();
        }
        if mode == "source" {
            fs::write(temp.path().join("daily").join(KLINE), b"bad").unwrap();
        }
        if mode == "missing-source" {
            fs::rename(
                temp.path().join("daily").join(KLINE),
                temp.path().join("saved"),
            )
            .unwrap();
        }
        let wrong = "a".repeat(64);
        let result = run(
            &root,
            if mode == "job" { "../daily" } else { "daily" },
            if mode == "day" {
                Window::new(DAY, 2 * DAY).unwrap()
            } else {
                day()
            },
            if mode == "content" { &wrong } else { &content },
            if mode == "missing-hash" {
                None
            } else if mode == "handoff" {
                Some(&wrong)
            } else {
                Some(&handoff)
            },
        );
        assert!(result.is_err());
        assert!(!temp.path().join("daily").join(MARKER).exists());
        if mode != "missing-source" {
            raw_present(&temp);
        }
    }
}
#[test]
fn marker_atomic_and_confirmation_boundary_precedes_any_unlink() {
    for phase in [
        Phase::Created,
        Phase::Written,
        Phase::FileSynced,
        Phase::Renamed,
        Phase::BeforeDirectorySync,
        Phase::DirectorySynced,
    ] {
        let (temp, root, content, handoff) = seed();
        let mut fired = false;
        let result = run_with(
            &root,
            "daily",
            day(),
            &content,
            Some(&handoff),
            |target, step| {
                if target == Target::Marker && step == Step::Write(phase) && !fired {
                    fired = true;
                    anyhow::bail!("injected");
                }
                if target == Target::Marker && step == Step::BeforeSync {
                    anyhow::bail!("no confirmation");
                }
                Ok(())
            },
        );
        assert!(result.is_err());
        raw_present(&temp);
        run(&root, "daily", day(), &content, Some(&handoff)).unwrap();
        complete(&temp);
    }
}
#[test]
fn tombstone_failure_never_deletes_until_all_are_durable() {
    for phase in [
        Phase::Created,
        Phase::Written,
        Phase::FileSynced,
        Phase::Renamed,
        Phase::BeforeDirectorySync,
        Phase::DirectorySynced,
    ] {
        let (temp, root, content, handoff) = seed();
        assert!(run_with(
            &root,
            "daily",
            day(),
            &content,
            Some(&handoff),
            |target, step| {
                if target == Target::Tombstone(12)
                    && (step == Step::Write(phase) || step == Step::BeforeSync)
                {
                    anyhow::bail!("injected");
                }
                Ok(())
            }
        )
        .is_err());
        raw_present(&temp);
        run(&root, "daily", day(), &content, None).unwrap();
        complete(&temp);
    }
}
#[test]
fn every_raw_parent_failure_resumes_with_missing_targets_confirmed() {
    for target in (0..24)
        .map(|hour| Target::Page(hour, 0))
        .chain([Target::Kline])
    {
        for failure in [
            Step::BeforeUnlink,
            Step::Unlinked,
            Step::BeforeSync,
            Step::Synced,
        ] {
            let (temp, root, content, handoff) = seed();
            assert!(run_with(
                &root,
                "daily",
                day(),
                &content,
                Some(&handoff),
                |at, step| {
                    anyhow::ensure!(at != target || step != failure, "injected");
                    Ok(())
                }
            )
            .is_err());
            let mut confirmed = 0;
            run_with(&root, "daily", day(), &content, None, |at, step| {
                if matches!(at, Target::Page(..) | Target::Kline) && step == Step::Synced {
                    confirmed += 1;
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(confirmed, 25);
            complete(&temp);
        }
    }
}
#[test]
fn malformed_marker_or_retained_manifest_fails_before_other_deletion() {
    for mode in [
        "unknown",
        "duplicate",
        "order",
        "count",
        "leaf",
        "manifest",
        "missing-manifest",
        "config",
    ] {
        let (temp, root, content, handoff) = seed();
        assert!(run_with(
            &root,
            "daily",
            day(),
            &content,
            Some(&handoff),
            |target, step| {
                anyhow::ensure!(
                    target != Target::Tombstone(0) || step != Step::Write(Phase::Created),
                    "pause"
                );
                Ok(())
            }
        )
        .is_err());
        let marker = temp.path().join("daily").join(MARKER);
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&marker).unwrap()).unwrap();
        match mode {
            "unknown" => value["extra"] = true.into(),
            "leaf" => value["hours"][0]["name"] = "../../external".into(),
            "order" => value["hours"].as_array_mut().unwrap().swap(0, 1),
            "count" => value["hours"][0]["page_count"] = 2.into(),
            "config" => value["identity"]["config_sha256"] = "b".repeat(64).into(),
            _ => (),
        }
        let encoded = if mode == "duplicate" {
            let text = serde_json::to_string(&value).unwrap();
            format!("{{\"schema\":1,{}", &text[1..]).into_bytes()
        } else {
            serde_json::to_vec(&value).unwrap()
        };
        fs::write(&marker, encoded).unwrap();
        let manifest = temp
            .path()
            .join(hourly(&id(), 12).unwrap().job_id())
            .join(MANIFEST);
        if mode == "manifest" {
            fs::write(&manifest, b"bad").unwrap();
        }
        if mode == "missing-manifest" {
            fs::rename(&manifest, temp.path().join("saved-manifest")).unwrap();
        }
        assert!(run(&root, "daily", day(), &content, None).is_err());
        raw_present(&temp);
    }
}
#[test]
fn post_marker_symlink_leaf_is_unlinked_without_following() {
    let (temp, root, content, handoff) = seed();
    assert!(run_with(
        &root,
        "daily",
        day(),
        &content,
        Some(&handoff),
        |at, step| {
            anyhow::ensure!(
                at != Target::Page(0, 0) || step != Step::BeforeUnlink,
                "pause"
            );
            Ok(())
        }
    )
    .is_err());
    let outside = tempfile::NamedTempFile::new().unwrap();
    fs::write(outside.path(), b"outside").unwrap();
    let leaf = temp
        .path()
        .join(hourly(&id(), 0).unwrap().job_id())
        .join("page-00000000000000000000.bin");
    fs::rename(&leaf, temp.path().join("saved-page")).unwrap();
    std::os::unix::fs::symlink(outside.path(), &leaf).unwrap();
    run(&root, "daily", day(), &content, None).unwrap();
    assert_eq!(fs::read(outside.path()).unwrap(), b"outside");
    complete(&temp);
}

#[test]
fn symlink_hour_directory_never_authorizes_deletion() {
    for resumed in [false, true] {
        let (temp, root, content, handoff) = seed();
        if resumed {
            assert!(run_with(
                &root,
                "daily",
                day(),
                &content,
                Some(&handoff),
                |target, step| {
                    anyhow::ensure!(
                        target != Target::Tombstone(0) || step != Step::Write(Phase::Created),
                        "pause"
                    );
                    Ok(())
                }
            )
            .is_err());
        }
        let path = temp.path().join(hourly(&id(), 12).unwrap().job_id());
        let saved = temp.path().join("saved-hour");
        fs::rename(&path, &saved).unwrap();
        std::os::unix::fs::symlink(&saved, &path).unwrap();
        assert!(run(&root, "daily", day(), &content, Some(&handoff)).is_err());
        raw_present(&temp);
        assert_eq!(temp.path().join("daily").join(MARKER).exists(), resumed);
    }
}

#[test]
fn ack_unknown_requires_confirmation_and_all_twenty_four_tombstones() {
    for target in [Target::Marker, Target::Tombstone(12)] {
        for phase in [
            Phase::Renamed,
            Phase::BeforeDirectorySync,
            Phase::DirectorySynced,
        ] {
            let (temp, root, content, handoff) = seed();
            let mut fired = false;
            let mut confirmed = std::collections::BTreeSet::new();
            run_with(
                &root,
                "daily",
                day(),
                &content,
                Some(&handoff),
                |at, step| {
                    if at == target && step == Step::Write(phase) && !fired {
                        fired = true;
                        anyhow::bail!("ACK unknown");
                    }
                    if let Target::Tombstone(hour) = at {
                        if matches!(step, Step::Write(Phase::DirectorySynced) | Step::Synced) {
                            confirmed.insert(hour);
                        }
                    }
                    if step == Step::BeforeUnlink {
                        assert_eq!(confirmed.len(), 24);
                    }
                    Ok(())
                },
            )
            .unwrap();
            assert!(fired);
            complete(&temp);
        }
    }
}

#[test]
fn initial_source_symlink_or_special_leaf_cannot_create_marker() {
    for kline in [false, true] {
        for symlink in [false, true] {
            let (temp, root, content, handoff) = seed();
            let leaf = if kline {
                temp.path().join("daily").join(KLINE)
            } else {
                temp.path()
                    .join(hourly(&id(), 0).unwrap().job_id())
                    .join("page-00000000000000000000.bin")
            };
            let outside = tempfile::tempdir().unwrap();
            let saved = outside.path().join("original");
            fs::rename(&leaf, &saved).unwrap();
            let bytes = fs::read(&saved).unwrap();
            if symlink {
                std::os::unix::fs::symlink(&saved, &leaf).unwrap();
            } else {
                fs::create_dir(&leaf).unwrap();
            }
            assert!(run(&root, "daily", day(), &content, Some(&handoff)).is_err());
            assert!(!temp.path().join("daily").join(MARKER).exists());
            assert_eq!(fs::read(&saved).unwrap(), bytes);
        }
    }
}
