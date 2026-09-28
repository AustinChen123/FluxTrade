use super::super::tests::{d, fixture};
use super::*;
use execution::commit::tests::{at, input};
mod projections;
mod vectors;

pub(in super::super) fn delivery_fixture(
    owner: &ScenarioAccount,
    capture: bool,
) -> (Store, Option<Fact>) {
    let mut store = Store::new(owner.key.clone());
    let mut r = request(owner, Kind::Trading);
    r.continuation_id = Some("Q".into());
    let fact = capture.then(|| store.capture(owner, &r).unwrap());
    (store, fact)
}

fn request(owner: &ScenarioAccount, kind: Kind) -> Request {
    Request {
        schema_version: "snapshot_request_v1".into(),
        account_key: owner.key.clone(),
        snapshot_id: "S".into(),
        kind,
        mode: Mode::OwnerCurrent,
        fixture_key: None,
        captured_at: 500,
        continuation_id: None,
    }
}
// Protocol sections 4/6: capture reads existing financial values without writing owner.
pub(in super::super) fn assert_golden(
    owner: &ScenarioAccount,
    equity: &str,
    available: &str,
    contracts: &str,
    open: bool,
) {
    let before = owner.clone();
    let mut store = Store::new(owner.key.clone());
    let mut r = request(owner, Kind::Trading);
    r.captured_at = 900;
    assert_eq!(
        store.capture(owner, &r).unwrap().payload,
        Payload::Trading(d(equity), d(available))
    );
    assert_eq!(owner, &before);
    projections::golden(owner, contracts, open);
}

#[test]
fn owner_current_projections_are_exact_detached_and_do_not_advance_clock() {
    let (seed, config, marks) = fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let before = owner.clone();
    for kind in [Kind::Trading, Kind::Positions, Kind::OpenOrders] {
        let mut store = Store::new(owner.key.clone());
        let r = request(&owner, kind);
        let fact = store.capture(&owner, &r).unwrap();
        assert_eq!(
            (fact.captured_account_version, fact.snapshot_as_of),
            (Some(0), 500)
        );
        match &fact.payload {
            Payload::Trading(e, a) => {
                let expected = owner.reservation().unwrap();
                assert_eq!((*e, *a), (expected.equity, expected.available_margin));
            }
            Payload::Positions(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    (rows[0].contracts, rows[0].mark, rows[0].notional),
                    (d("3"), d("50001"), d("1500.03"))
                );
            }
            Payload::OpenOrders(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].facts, seed.orders[0]);
                assert_eq!(rows[0].created_at, 500);
            }
            _ => panic!("owner payload"),
        }
        let mut detached = fact.clone();
        detached.payload = Payload::Earn(d("999"));
        assert_eq!(store.capture(&owner, &r).unwrap(), fact);
        assert_ne!(detached, fact);
        let mut future = r.clone();
        future.snapshot_id = "future".into();
        future.captured_at = 900;
        store.capture(&owner, &future).unwrap();
        let mut earlier = r;
        earlier.snapshot_id = "earlier".into();
        store.capture(&owner, &earlier).unwrap(); // No hidden capture/scheduler clock.
    }
    assert_eq!(owner, before);
}

#[test]
fn all_ten_fixtures_are_closed_and_independent_of_financial_owner() {
    let (seed, _, _) = fixture();
    let owner = ScenarioAccount::synthetic_min_cash(seed.key).unwrap();
    let before = owner.clone();
    let cases = [
        ("POLL_Q1_S1", Payload::Trading(d("100"), d("100")), 100),
        ("POLL_Q2_S2", Payload::Trading(d("110"), d("110")), 110),
        ("POLL_EARN_ZERO", Payload::Earn(d("0")), 90),
        ("POLL_POSITIONS_EMPTY", Payload::Positions(vec![]), 100),
        ("POLL_OPEN_ORDERS_EMPTY", Payload::OpenOrders(vec![]), 100),
        ("POLL_EARN_FAILURE", Payload::Failure(Kind::Earn), 90),
        ("POLL_TRADING_FAILURE", Payload::Failure(Kind::Trading), 100),
        (
            "POLL_POSITIONS_FAILURE",
            Payload::Failure(Kind::Positions),
            100,
        ),
        (
            "POLL_OPEN_ORDERS_FAILURE",
            Payload::Failure(Kind::OpenOrders),
            100,
        ),
        ("MARKET_GOLDEN_V1", Payload::MarketGolden, 500),
    ];
    for (key, expected, as_of) in cases {
        let mut store = Store::new(owner.key.clone());
        let mut r = request(&owner, expected.kind());
        r.mode = Mode::FrozenPollFixture;
        r.fixture_key = Some(key.into());
        r.captured_at = 1;
        let fact = store.capture(&owner, &r).unwrap();
        assert_eq!(
            (
                fact.payload,
                fact.snapshot_as_of,
                fact.captured_account_version
            ),
            (expected, as_of, None)
        );
        let mut wrong = r.clone();
        wrong.snapshot_id = "wrong".into();
        wrong.kind = if r.kind == Kind::Trading {
            Kind::Earn
        } else {
            Kind::Trading
        };
        let saved = store.clone();
        assert_eq!(store.capture(&owner, &wrong), Err("INVALID_SCHEMA"));
        assert_eq!(store, saved);
    }
    assert_eq!(owner, before);
}

#[test]
fn identity_precedes_time_and_all_failures_leave_both_stores_unchanged() {
    let (seed, _, _) = fixture();
    let mut owner = ScenarioAccount::synthetic_min_cash(seed.key).unwrap();
    let mut store = Store::new(owner.key.clone());
    let r = request(&owner, Kind::Trading);
    let original = store.capture(&owner, &r).unwrap();
    let fill = at(input(&owner, "MIN-O1", "X", "20", "50000"), 501);
    owner.execute(&fill).unwrap();
    let intent = admission::fixture_intent(&owner, "rejected", Side::Long, d("1"), d("50000"));
    let (rejection, _) = admission::fixture_admit(&mut owner, "rejected", 502, &intent).unwrap();
    assert_eq!(rejection, "rejected");
    let before = owner.clone();
    let saved = store.clone();
    assert_eq!(store.capture(&owner, &r), Ok(original));
    let mut stale = r.clone();
    stale.snapshot_id = "after-accepted-before-rejected".into();
    stale.captured_at = 501;
    assert_eq!(store.capture(&owner, &stale), Err("INVALID_SCHEMA"));
    for field in 0..10 {
        let mut bad = r.clone();
        match field {
            0 => bad.captured_at = 499,
            1 => bad.continuation_id = Some("Q".into()),
            2 => bad.kind = Kind::Earn,
            3 => {
                bad.mode = Mode::FrozenPollFixture;
                bad.fixture_key = Some("POLL_Q1_S1".into());
            }
            4 => bad.schema_version = "wrong".into(),
            5 => bad.account_key.account = "other".into(),
            6 => bad.snapshot_id = "new".into(),
            7 => {
                bad.snapshot_id = "new".into();
                bad.captured_at = 502;
                bad.kind = Kind::Market;
            }
            8 => bad.continuation_id = Some(" ".into()),
            _ => {
                bad.snapshot_id = "new".into();
                bad.mode = Mode::FrozenPollFixture;
                bad.fixture_key = Some("unknown".into());
            }
        }
        let reason = match field {
            0..=3 => "SNAPSHOT_ID_CONFLICT",
            5 => "ACCOUNT_KEY_MISMATCH",
            _ => "INVALID_SCHEMA",
        };
        assert_eq!(store.capture(&owner, &bad), Err(reason));
        assert_eq!(store, saved);
        assert_eq!(owner, before);
    }
    let mut overflow = owner.clone();
    overflow.state_version = u64::MAX;
    let mut fresh = r;
    fresh.snapshot_id = "overflow".into();
    fresh.captured_at = 502;
    assert_eq!(store.capture(&overflow, &fresh), Err("NATIVE_INVARIANT"));
    assert_eq!(store, saved);
}

#[test]
fn request_digest_matches_independent_bytes_and_field_order() {
    use ring::digest::{digest, SHA256};
    let (seed, config, marks) = fixture();
    let owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let r = request(&owner, Kind::Trading);
    let mut bytes = Vec::new();
    let text = |b: &mut Vec<u8>, s: &str| {
        b.extend_from_slice(&(s.len() as i64).to_be_bytes());
        b.extend_from_slice(s.as_bytes());
    };
    for s in [
        "SCENARIO_SNAPSHOT_REQUEST_V1",
        "snapshot_request_v1",
        "okx-scenario",
        "test",
        "A",
    ] {
        text(&mut bytes, s);
    }
    bytes.push(0);
    for s in ["S", "TRADING", "OWNER_CURRENT"] {
        text(&mut bytes, s);
    }
    bytes.push(0);
    bytes.extend_from_slice(&500_i64.to_be_bytes());
    bytes.push(0);
    assert_eq!(r.digest().as_slice(), digest(&SHA256, &bytes).as_ref());
    let mut reordered = r.clone();
    std::mem::swap(
        &mut reordered.account_key.venue,
        &mut reordered.account_key.environment,
    );
    assert_ne!(r.digest(), reordered.digest());
}
