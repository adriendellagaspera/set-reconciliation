// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cell::Cell;
use std::ops::RangeBounds;

use crate::policy::SqrtFanOut;

use super::*;

struct CountingView {
    inner: FingerprintTreeMap<i32, i32>,
    aggregate_calls: Cell<usize>,
}

impl RsosView<i32> for CountingView {
    fn size(&self) -> usize {
        self.inner.len()
    }

    fn aggregate<R: RangeBounds<i32>>(&self, range: R) -> Aggregate {
        self.aggregate_calls.set(self.aggregate_calls.get() + 1);
        self.inner.aggregate(range)
    }

    fn rank(&self, key: &i32) -> usize {
        self.inner.rank(key)
    }

    fn select(&self, rank: usize) -> &i32 {
        self.inner.select(rank)
    }
}

// ----- The fan-out rule: this crate's communication cost, pinned -----

/// The default fan-out is a constant `b` whatever the range's size.
#[test]
fn default_split_fan_out_is_constant_at_sixteen() {
    for m in [100usize, 400, 2_500, 250_000] {
        let store = tree(&(0..m as i32).collect::<Vec<_>>());
        let (child_ranges, enumeration_ranges) = round(&store, splitting_segment(m));
        assert!(enumeration_ranges.is_empty());
        assert!(
            child_ranges.len() <= FanOut::NEGENTROPY.get(),
            "m={m}: SPLIT emitted {} children, expected at most b={} \
             (a size-dependent fan-out would grow with m)",
            child_ranges.len(),
            FanOut::NEGENTROPY.get()
        );
        assert!(child_ranges.len() > 1, "m={m}: the split must refine");
    }
}

/// `SqrtFanOut` is public API, so its cut positions are a contract.
#[test]
fn sqrt_fan_out_is_still_the_square_root_of_the_range_size() {
    for m in [100usize, 400, 2_500] {
        let store = tree(&(0..m as i32).collect::<Vec<_>>());
        let mut child_ranges = Vec::new();
        let mut enumeration_ranges = Vec::new();
        protocol_round_with_policy(
            &store,
            &SqrtFanOut,
            vec![splitting_segment(m)],
            &mut child_ranges,
            &mut enumeration_ranges,
            &mut rng(),
        );
        assert!(enumeration_ranges.is_empty());
        let root = (m as f64).sqrt() as usize;
        assert!(
            child_ranges.len() >= root / 2 && child_ranges.len() <= root * 2,
            "m={m}: SPLIT emitted {} children, expected ~√m = {root}",
            child_ranges.len()
        );
    }
}

#[test]
fn one_child_split_reuses_the_parent_aggregate() {
    let store = CountingView {
        inner: tree(&[10]),
        aggregate_calls: Cell::new(0),
    };
    let segment = RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        // local span is one, remote span is two: the default policy asks the peer with more
        // elements to refine and emits one child equal to the parent.
        aggregate: Aggregate::new(2, Fingerprint([7, 0, 0, 0])),
    };
    let mut child_ranges = Vec::new();
    let mut enumeration_ranges = Vec::new();

    let outcome = protocol_round(
        &store,
        vec![segment],
        &mut child_ranges,
        &mut enumeration_ranges,
        &mut rng(),
    );

    assert_eq!(outcome.split(), 1);
    assert_eq!(child_ranges.len(), 1);
    assert_eq!(outcome.children(), child_ranges.len());
    assert!(enumeration_ranges.is_empty());
    assert_eq!(
        store.aggregate_calls.get(),
        1,
        "an uncut child is the parent; its aggregate must be reused rather than recomputed"
    );
}

#[test]
fn every_split_child_advertises_its_own_range_summary() {
    for size in [17, 100, 397] {
        let store = tree(&(0..size).collect::<Vec<_>>());
        let (children, enumerations) = round(&store, splitting_segment(size as usize));
        assert!(enumerations.is_empty());
        assert!(children.len() > 1);
        for child in children {
            assert_eq!(
                child.aggregate,
                store.aggregate(child.range.clone()),
                "a refined child must advertise its own summary, including the final child"
            );
        }
    }
}
