use super::*;

fn blocked(s: &mut Session) {
    assert!(s.poisoned);
    let before = s.owner.clone();
    for reply in [
        s.apply_group("{"),
        s.capture_snapshot("{"),
        s.build_delivery("{"),
    ] {
        assert_eq!(reply, Err(BoundaryError::Invariant));
    }
    assert_eq!(s.owner, before);
}
#[test]
fn escaping_panic_and_inspection_failures_have_one_fixed_boundary() {
    let mut s = session();
    let before = s.owner.clone();
    assert_eq!(
        s.guarded(true, |_| panic!("opaque panic text")),
        Err(BoundaryError::Invariant)
    );
    assert_eq!(s.owner, before);
    blocked(&mut s);
    assert!(s.inspect_state().is_ok());
    assert!(s.poisoned);
    assert_eq!(
        s.guarded(false, |_| panic!("read panic")),
        Err(BoundaryError::Invariant)
    );
    assert_eq!(
        s.guarded(false, |_| Err(BoundaryError::Input("INVALID_SCHEMA"))),
        Err(BoundaryError::Invariant)
    );
    s.owner.state_version = u64::MAX;
    let corrupt = s.owner.clone();
    assert_eq!(s.inspect_state(), Err(BoundaryError::Invariant));
    assert_eq!(s.owner, corrupt);
    let mut fresh = session();
    fresh.owner.state_version = u64::MAX;
    assert_eq!(fresh.inspect_state(), Err(BoundaryError::Invariant));
    blocked(&mut fresh);
}
#[test]
fn cache_missing_mismatch_and_encoding_failures_poison_without_rollback() {
    for missing in [true, false] {
        let mut s = session();
        let request = group("G", 500).to_string();
        s.apply_group(&request).unwrap();
        let key = ("S_order_v1".into(), "G".into());
        if missing {
            s.completed.remove(&key);
        } else {
            s.completed.get_mut(&key).unwrap().0 = [0; 32];
        }
        let before = s.owner.clone();
        assert_eq!(s.apply_group(&request), Err(BoundaryError::Invariant));
        assert_eq!(s.owner, before);
        blocked(&mut s);
    }
    let mut s = session();
    let g = wire::group::decode_group(&group("G", 500).to_string(), &s.owner.key).unwrap();
    let outcome = s.owner.apply_group_observed(&g, |_| Ok(()));
    s.owner.state_version = u64::MAX;
    let before = s.owner.clone();
    assert_eq!(
        s.guarded(true, |s| s.group_result(&g, 0, outcome)),
        Err(BoundaryError::Invariant)
    );
    assert!(s.completed.is_empty());
    assert_eq!(s.owner, before);
    blocked(&mut s);
}
#[test]
fn caught_owner_panic_keeps_real_committed_prefix_and_poison_is_not_a_gate() {
    let (owner, completion) = wire::result::tests::run_with_panic(true, true);
    assert_eq!(completion.failure, Some("LIQUIDATION_PANIC"));
    assert!(!completion.committed.is_empty());
    let mut s = session();
    s.owner = owner;
    let before = s.owner.clone();
    let g = completion.group.clone();
    assert_eq!(
        s.guarded(true, |s| s.group_result(
            &g,
            0,
            Ok(group::Applied::Fresh(completion))
        )),
        Err(BoundaryError::Invariant)
    );
    assert_eq!(s.owner, before);
    assert!(s.completed.is_empty());
    blocked(&mut s);
    assert!(s.inspect_state().is_ok());
    let mut preflight = session();
    assert_eq!(
        preflight.guarded(true, |s| s.group_result(&g, 0, Err("EXECUTION_PANIC"))),
        Err(BoundaryError::Invariant)
    );
    blocked(&mut preflight);
}
#[test]
fn ordinary_boundary_errors_and_typed_fault_leave_session_usable() {
    let mut s = session();
    for reason in [
        "INVALID_JSON",
        "INVALID_SCHEMA",
        "ACCOUNT_KEY_MISMATCH",
        "UNKNOWN_RECEIPT_REFERENCE",
        "SNAPSHOT_ID_CONFLICT",
        "DELIVERY_ID_CONFLICT",
    ] {
        assert_eq!(
            s.guarded(true, |_| Err(boundary(reason))),
            Err(boundary(reason))
        );
        assert!(!s.poisoned);
    }
    let mut g = group("bad", 500);
    g["declared_member_count"] = json!(2);
    assert_eq!(
        value(s.apply_group(&g.to_string()))["classification"],
        "FAULT"
    );
    assert!(!s.poisoned);
    assert!(s.inspect_state().is_ok());
    let before = s.owner.clone();
    assert_eq!(
        s.capture_snapshot("[]"),
        Err(BoundaryError::Input("INVALID_SCHEMA"))
    );
    assert_eq!(s.owner, before);
    assert!(!s.poisoned);
}
