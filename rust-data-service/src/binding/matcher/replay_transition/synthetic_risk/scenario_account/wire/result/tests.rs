use super::super::super::group::{Group, Input, Member};
use super::super::super::tests::{d, fixture};
use super::*;

pub(in super::super::super) fn run(fault: bool) -> (ScenarioAccount, Completion) {
    let (mut seed, config, mut marks) = fixture();
    seed.cash = d("3000");
    seed.orders.clear();
    seed.positions[0].contracts = d("1001");
    seed.positions[0].lots.truncate(1);
    seed.positions[0].lots[0].contracts = d("1001");
    marks[0].price = d("50000");
    marks[0].valid_to = 501;
    marks.push(Mark {
        product: Product::Btc,
        price: d("49900"),
        valid_from: 501,
        valid_to: 3000,
    });
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let input = context::tests::activation(&owner, 501, context::tests::mark_rows(&marks, 501));
    let group = Group {
        group_id: "actual".into(),
        account_key: owner.key.clone(),
        ordering_contract_id: "S_order_v1".into(),
        group_effective_at: 501,
        declared_member_count: 2,
        members: vec![
            Member {
                stamp: input.stamp.clone(),
                input: Input::Context(input),
            },
            Member {
                stamp: source::stamp("later", 501, 60),
                input: Input::Intent(admission::fixture_intent(
                    &owner,
                    "later",
                    Side::Short,
                    d("0.01"),
                    d("49900"),
                )),
            },
        ],
    };
    let completion = owner
        .apply_group_checked(&group, |stage| {
            if fault && stage == risk_transition::Stage::LiquidationBeforeSwap(2) {
                Err("INJECTED")
            } else {
                Ok(())
            }
        })
        .unwrap();
    (owner, completion)
}
fn capture(
    owner: &ScenarioAccount,
    before: u64,
    outcome: Result<&Completion, Fault>,
) -> GroupResult {
    let saved = owner.clone();
    let result = GroupResult::from_outcome(owner, "requested", before, outcome).unwrap();
    assert_eq!(result.canonical().unwrap(), result.canonical().unwrap());
    assert_eq!(*owner, saved);
    assert_eq!(result.owner_digest, owner.owner_evidence_digest().unwrap());
    result
}
#[test]
fn actual_completions_prefix_and_preflight_keep_authoritative_evidence() {
    for fault in [false, true] {
        let (mut owner, completion) = run(fault);
        let result = capture(&owner, 0, Ok(&completion));
        assert_eq!(result.group_id, "actual");
        assert_eq!(result.group_digest, Some(completion.digest));
        assert_eq!(result.references, completion.committed);
        assert!(matches!(result.references[0], Reference::Source(_)));
        assert!(matches!(result.references[1], Reference::Liquidation(_)));
        assert_eq!(result.rejections, completion.rejections);
        assert_eq!(result.failure, completion.failure);
        assert_eq!(
            result.classification,
            if fault { "FAULT" } else { "REJECTED" }
        );
        assert_eq!(result.after, owner.state_version as i64);
        assert_eq!(result.gate, owner.gate);
        assert_eq!(result.lifecycle, owner.transition.lifecycle);
        let saved = owner.clone();
        let mut detached = result.clone();
        detached.references.clear();
        detached.owner_digest[0] ^= 1;
        assert_ne!(detached.canonical(), result.canonical());
        assert_eq!(owner, saved);
        if !fault {
            assert_eq!(result.rejections, vec![("later".into(), "RUN_TERMINAL")]);
            let mut fresh = completion.group.clone();
            fresh.group_id = "fresh".into();
            let error = owner.apply_group(&fresh).unwrap_err();
            assert_eq!(error, "RUN_TERMINAL");
            let terminal = capture(&owner, owner.state_version, Err(error));
            assert_eq!(
                (terminal.classification, terminal.group_digest),
                ("REJECTED", None)
            );
            assert!(terminal.references.is_empty() && terminal.rejections.is_empty());
        }
    }
    let (seed, config, marks) = fixture();
    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
    let mut group = run(false).1.group;
    group.group_effective_at = 500;
    group.members = vec![Member {
        stamp: source::stamp("accepted", 500, 60),
        input: Input::Intent(admission::fixture_intent(
            &owner,
            "accepted",
            Side::Short,
            d("0.01"),
            d("50000"),
        )),
    }];
    group.declared_member_count = 1;
    let completion = owner.apply_group(&group).unwrap();
    let committed = capture(&owner, 0, Ok(&completion));
    assert_eq!(committed.classification, "COMMITTED");
    assert_eq!(
        committed.references,
        vec![Reference::Source("accepted".into())]
    );
    assert!(committed.failure.is_none() && committed.rejections.is_empty());
    group.group_id = "invalid".into();
    group.members.clear();
    let error = owner.apply_group(&group).unwrap_err();
    assert_eq!(error, "INVALID_SCENARIO_GROUP");
    assert_eq!(capture(&owner, 0, Err(error)).classification, "FAULT");
    assert_eq!(
        GroupResult::from_outcome(&owner, "G", u64::MAX, Err(error)),
        Err("NATIVE_INVARIANT")
    );
    owner.state_version = u64::MAX;
    assert_eq!(
        GroupResult::from_outcome(&owner, "G", 0, Err(error)),
        Err("NATIVE_INVARIANT")
    );
}
