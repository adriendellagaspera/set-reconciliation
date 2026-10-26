use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};
use rbsr::{
    initial_ranges, initial_ranges_async, protocol_round_async, protocol_round_with_policy,
    protocol_round_with_policy_async, Comparison, Decision, EnumerateBelowThreshold, ImmediateView,
    RefinementPolicy, SplitStride, SqrtFanOut,
};
use rsos::{Aggregate, Fingerprint};

#[path = "async_round/support.rs"]
mod support;
use support::*;

fn parity(
    root: Arc<Map>,
    active: Vec<rbsr::RangeAggregate<i32>>,
    policy: &dyn RefinementPolicy,
    seed: u64,
) {
    let sync = View::new(root.clone(), false);
    let asynchronous = View::new(root, true);
    let (mut a, mut ae) = outputs();
    let (mut b, mut be) = outputs();
    let mut ar = StdRng::seed_from_u64(seed);
    let mut br = ar.clone();
    let ao = protocol_round_with_policy(&sync, policy, active.clone(), &mut a, &mut ae, &mut ar);
    let bo = run(protocol_round_with_policy_async(
        &asynchronous,
        policy,
        active,
        &mut b,
        &mut be,
        &mut br,
    ))
    .unwrap();
    assert_eq!((a, ae, ao), (b, be, bo));
    assert_eq!(sync.trace(), asynchronous.trace());
    assert_eq!(ar.next_u64(), br.next_u64());
}

#[test]
fn outputs_queries_outcomes_and_rng_match_across_real_suspensions() {
    parity(root(), active(), &SqrtFanOut, 91);
}

#[test]
fn seeded_generated_sets_and_ranges_match_under_multiple_policies() {
    let mut generator = StdRng::seed_from_u64(91);
    for case in 0..128 {
        let map: Map = (0..generator.gen_range(0..200))
            .filter_map(|key| {
                if generator.gen_bool(0.8) {
                    Some((key, generator.gen_range(0..100)))
                } else {
                    None
                }
            })
            .collect();
        let root = Arc::new(map);
        let ranges = vec![
            rbsr::RangeAggregate::new(
                None,
                None,
                Aggregate::new(generator.gen_range(0..210), Fingerprint([7, 0, 0, 0])),
            ),
            rbsr::RangeAggregate::new(
                Some(generator.gen_range(-10..210)),
                Some(generator.gen_range(-10..210)),
                Aggregate::ZERO,
            ),
        ];
        parity(root.clone(), ranges.clone(), &SqrtFanOut, case);
        parity(root, ranges, &EnumerateBelowThreshold::default(), case);
    }
}

#[test]
fn initial_async_is_identical_and_propagates_failure() {
    let root = root();
    let view = View::new(root.clone(), true);
    assert_eq!(
        initial_ranges(&*root),
        run(initial_ranges_async(&view)).unwrap()
    );
    let mut view = View::new(root, true);
    view.fail = Some(0);
    assert_eq!(run(initial_ranges_async(&view)), Err(Error::Io));
}

#[test]
fn immediate_adapter_completes_in_one_poll_and_matches_direct_calls() {
    let root = root();
    let view = ImmediateView::new(&*root);
    let (mut a, mut ae) = outputs();
    let mut rng = StdRng::seed_from_u64(91);
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    let outcome = {
        let future = protocol_round_async(&view, active(), &mut a, &mut ae, &mut rng);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(outcome)) => outcome,
            _ => panic!("RAM adapter must complete immediately"),
        }
    };
    let (mut b, mut be) = outputs();
    let mut other_rng = StdRng::seed_from_u64(91);
    let other = rbsr::protocol_round(&*root, active(), &mut b, &mut be, &mut other_rng);
    assert_eq!((a, ae, outcome), (b, be, other));
    assert_eq!(rng.next_u64(), other_rng.next_u64());
}

#[test]
fn published_new_roots_do_not_change_an_in_flight_snapshot() {
    let published = Mutex::new(root());
    let pinned = published.lock().unwrap().clone();
    let view = View::new(pinned.clone(), true);
    let (mut a, mut ae) = outputs();
    let mut rng = StdRng::seed_from_u64(91);
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    let outcome = {
        let future = protocol_round_with_policy_async(
            &view,
            &SqrtFanOut,
            active(),
            &mut a,
            &mut ae,
            &mut rng,
        );
        let mut future = std::pin::pin!(future);
        let mut updates = 0;
        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(result) => {
                    assert!(updates > 10);
                    break result.unwrap();
                }
                Poll::Pending => {
                    let mut next = (**published.lock().unwrap()).clone();
                    next.insert(1000 + updates, updates);
                    *published.lock().unwrap() = Arc::new(next);
                    updates += 1;
                }
            }
        }
    };
    let (mut b, mut be) = outputs();
    let mut other_rng = StdRng::seed_from_u64(91);
    let other = protocol_round_with_policy(
        &*pinned,
        &SqrtFanOut,
        active(),
        &mut b,
        &mut be,
        &mut other_rng,
    );
    assert_eq!((a, ae, outcome), (b, be, other));
    assert_eq!(rng.next_u64(), other_rng.next_u64());
    assert!(published.lock().unwrap().len() > pinned.len());
}

#[test]
fn expiry_during_suspension_preserves_outputs_and_rng() {
    let view = View::new(root(), true);
    let (mut a, mut ae) = outputs();
    let original = (a.clone(), ae.clone());
    let mut rng = StdRng::seed_from_u64(91);
    let mut original_rng = rng.clone();
    {
        let future = protocol_round_async(&view, active(), &mut a, &mut ae, &mut rng);
        let mut future = std::pin::pin!(future);
        let waker = waker();
        let mut cx = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        view.expired.store(true, Ordering::SeqCst);
        assert_eq!(run(future), Err(Error::Expired));
    }
    assert_eq!((a, ae), original);
    assert_eq!(rng.next_u64(), original_rng.next_u64());
}

#[test]
fn hostile_rank_is_clamped_on_both_paths() {
    let sync = View {
        hostile: true,
        ..View::new(root(), false)
    };
    let asynchronous = View {
        hostile: true,
        ..View::new(root(), true)
    };
    let (mut a, mut ae) = outputs();
    let (mut b, mut be) = outputs();
    let mut ar = StdRng::seed_from_u64(91);
    let mut br = ar.clone();
    let ao = protocol_round_with_policy(&sync, &SqrtFanOut, active(), &mut a, &mut ae, &mut ar);
    let bo = run(protocol_round_with_policy_async(
        &asynchronous,
        &SqrtFanOut,
        active(),
        &mut b,
        &mut be,
        &mut br,
    ))
    .unwrap();
    assert_eq!((a, ae, ao), (b, be, bo));
    assert_eq!(sync.trace(), asynchronous.trace());
    assert_eq!(ar.next_u64(), br.next_u64());
}

#[test]
fn future_is_send_without_a_runtime_dependency() {
    fn assert_send(_: impl Future + Send) {}
    let view = View::new(root(), true);
    let (mut a, mut ae) = outputs();
    let mut rng = StdRng::seed_from_u64(91);
    assert_send(protocol_round_async(
        &view,
        active(),
        &mut a,
        &mut ae,
        &mut rng,
    ));
}

#[test]
fn malformed_and_nonprogressing_policy_decisions_match() {
    struct Stalled;
    impl RefinementPolicy for Stalled {
        fn decide(&self, comparison: Comparison) -> Decision {
            Decision::Split(SplitStride::per_child(comparison.span()))
        }
    }
    parity(root(), active(), &Stalled, 91);
}

#[path = "async_round/failures.rs"]
mod failures;
