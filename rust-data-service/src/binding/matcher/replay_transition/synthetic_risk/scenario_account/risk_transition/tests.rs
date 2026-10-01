use super::super::context::tests::{activation, mark_rows, stamp};
use super::super::tests::{d, fixture};
use super::*;

#[test]
fn shortfall_remainder_role_matrix_uses_full_quantity_and_pending_state() {
    for side in [Side::Long, Side::Short] {
        for order_side in [Side::Long, Side::Short] {
            for quantity in ["0.5", "1", "2"] {
                for reduce_only in [false, true] {
                    let (mut seed, config, mut marks) = fixture();
                    seed.cash = d("10");
                    seed.positions[0].contracts = d("1");
                    seed.positions[0].side = side;
                    seed.positions[0].lots.truncate(1);
                    marks[0].price = d("50000");
                    let order = &mut seed.orders[0];
                    order.side = order_side;
                    order.original = d(quantity);
                    order.remaining = d(quantity);
                    order.filled = Decimal::ZERO;
                    order.price = d("50000");
                    order.status = "OPEN".into();
                    order.reduce_only = reduce_only;
                    let reducing = side != order_side && d(quantity) <= d("1");
                    if reduce_only && !reducing {
                        assert_eq!(
                            ScenarioAccount::from_seed(&seed, &config, &marks),
                            Err("REDUCE_ONLY_NOT_REDUCING")
                        );
                        continue;
                    }
                    let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
                    let decision = owner.classify_risk(&owner.reservation().unwrap()).unwrap();
                    assert_eq!(decision.targets.is_empty(), reducing);
                    owner
                        .request_cancel(&cancel::RequestInput {
                            stamp: stamp("request", 500, 40),
                            targets: vec![("O1".into(), cancel::Reason::ExplicitScenario)],
                        })
                        .unwrap();
                    let pending = owner.classify_risk(&owner.reservation().unwrap()).unwrap();
                    assert!(pending.targets.is_empty());
                    assert_eq!(pending.pending.len(), 1);
                    owner
                        .effect_cancel(&cancel::EffectInput {
                            stamp: stamp("effect", 500, 50),
                            effects: vec![(
                                "request".into(),
                                "O1".into(),
                                cancel::Reason::ExplicitScenario,
                            )],
                        })
                        .unwrap();
                    let terminal = owner.classify_risk(&owner.reservation().unwrap()).unwrap();
                    assert!(terminal.targets.is_empty());
                    assert!(terminal.pending.is_empty());
                }
            }
        }
    }
}

#[test]
fn escalation_preserves_episode_and_all_pending_never_emits_empty_batch() {
    for already_pending in [false, true] {
        let (mut seed, config, mut marks) = fixture();
        seed.cash = d("3");
        seed.positions[0].contracts = d("1");
        seed.positions[0].lots.truncate(1);
        marks[0].price = d("50000");
        marks[0].valid_to = 600;
        let reducing = &mut seed.orders[0];
        reducing.original = d("1");
        reducing.filled = Decimal::ZERO;
        reducing.remaining = d("1");
        reducing.price = d("50000");
        reducing.status = "OPEN".into();
        let mut increasing = reducing.clone();
        increasing.intent_id = "I2".into();
        increasing.order_id = "O2".into();
        increasing.client_id = "C2".into();
        increasing.side = Side::Long;
        increasing.reduce_only = false;
        seed.orders.push(increasing);
        marks.extend(
            [
                (600, 700, "49900"),
                (700, 800, "49800"),
                (800, 3000, "50000"),
            ]
            .into_iter()
            .map(|(valid_from, valid_to, price)| Mark {
                product: Product::Btc,
                valid_from,
                valid_to,
                price: d(price),
            }),
        );
        let mut owner = ScenarioAccount::from_seed(&seed, &config, &marks).unwrap();
        if already_pending {
            owner
                .request_cancel(&cancel::RequestInput {
                    stamp: stamp("explicit", 500, 40),
                    targets: vec![("O2".into(), cancel::Reason::ExplicitScenario)],
                })
                .unwrap();
        }
        let first = activation(&owner, 600, mark_rows(&marks, 600));
        owner.activate_context(&first).unwrap();
        assert_eq!(owner.state_version, 2);
        assert_eq!(
            owner.transition.batches.len(),
            usize::from(!already_pending)
        );
        let episode = owner.transition.episode.clone().unwrap();
        assert_eq!(episode.initial_reason, cancel::Reason::RiskShortfall);
        let mut second = activation(&owner, 700, mark_rows(&marks, 700));
        second.stamp.event_id = "escalate".into();
        owner.activate_context(&second).unwrap();
        let escalated = owner.transition.episode.clone().unwrap();
        assert_eq!(escalated.id, episode.id);
        assert_eq!(escalated.current_reason, cancel::Reason::MmrBreach);
        assert_eq!(escalated.escalation_event.as_deref(), Some("escalate"));
        let cancel::State::Requested(request) = &owner.orders["O1"].cancel else {
            panic!("reducing target required")
        };
        assert_eq!(request.detecting_event_id, "escalate");
        let version = owner.state_version;
        let batches = owner.transition.batches.len();
        let mut recovery = activation(&owner, 800, mark_rows(&marks, 800));
        recovery.stamp.event_id = "recover".into();
        owner.activate_context(&recovery).unwrap();
        assert_eq!(owner.state_version, version + 1);
        assert_eq!(owner.transition.batches.len(), batches);
        assert_eq!(
            owner.transition.episode.as_ref().unwrap().current_reason,
            cancel::Reason::MmrBreach
        );
        assert_eq!(
            owner.transition.lifecycle,
            Lifecycle::AwaitingCancelEffective
        );
    }
}
