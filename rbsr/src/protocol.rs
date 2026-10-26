// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The protocol driver: [`initial_ranges`], [`protocol_round`], and the [`RangeAggregate`] wire
//! type they exchange — generic over [`RsosView`], never over a concrete store.
//! Split across siblings by concern: `bounds` owns `StartBound`/`EndBound`'s conversion to
//! [`std::ops::Bound`] and `KeyRange`'s construction and `RangeBounds` implementation; `rank` owns rank admission and clamping arithmetic; `driver` owns the shared
//! range plan and decision accounting; `range_aggregate` owns [`RangeAggregate`]'s own
//! construction and field readers; `outcome` owns [`RoundOutcome`]'s accessors and its
//! [`AddAssign`](std::ops::AddAssign). This file keeps the public type definitions (their module
//! location is their `cargo public-api`-visible path) plus the immediate query executor.

use std::ops::Bound;

use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};

use rsos::Aggregate;

use crate::policy::{Decision, FanOut, FixedFanOut, RefinementPolicy};
use crate::rsos_view::RsosView;
use crate::AsyncRsosView;

mod bounds;
mod driver;
mod outcome;
mod range_aggregate;
mod rank;

use driver::{Driver, Split};
use rank::BoundedRange;

/// The default refinement policy used by [`protocol_round`].
const DEFAULT_POLICY: FixedFanOut = FixedFanOut::new(FanOut::NEGENTROPY);

/// The start bound of a [`RangeAggregate`] range: `Included` or `Unbounded`, never `Excluded`.
/// Narrower than `std::ops::Bound<K>` so a peer sending the third shape fails to deserialize
/// rather than reaching a runtime check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum StartBound<K> {
    Unbounded,
    Included(K),
}

/// The end bound of a [`RangeAggregate`] range: `Excluded` or `Unbounded`, never `Included`. See
/// [`StartBound`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum EndBound<K> {
    Unbounded,
    Excluded(K),
}

/// A [`RangeAggregate`]'s range. A local tuple struct, not a bare tuple, so it can implement the
/// foreign `RangeBounds` and feed [`RsosView::aggregate`] directly.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct KeyRange<K>(StartBound<K>, EndBound<K>);

/// A key range paired with its [`Aggregate`], as exchanged by the protocol.
///
/// Wire encoding follows field declaration order. Changing the order or representation of these
/// fields is a wire-format change.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RangeAggregate<K> {
    range: KeyRange<K>,
    aggregate: Aggregate,
}

/// A range whose contents this peer must send explicitly: the **IDLIST** outcome, to be fed to
/// [`rsos::Rsos::enumerate`] by the caller — the driver itself never enumerates.
/// A bare pair of [`Bound`]s, not the narrowed wire types: this is a local output, never sent.
pub type EnumerationRange<K> = (Bound<K>, Bound<K>);

/// The initial family of **active ranges**: one [`RangeAggregate`] `{(−∞, +∞), A(whole store)}`.
/// The outer range is fixed to the whole universe here; [`protocol_round`] never assumes it, so
/// partial reconciliation needs only a different starting family.
pub fn initial_ranges<K, B: RsosView<K>>(local: &B) -> Vec<RangeAggregate<K>> {
    vec![RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate: local.aggregate(..),
    }]
}

/// Counts the decisions made by one [`protocol_round`].
///
/// [`AddAssign`](std::ops::AddAssign) accumulates several rounds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoundOutcome {
    skipped: usize,
    enumerated: usize,
    split: usize,
    children: usize,
    dropped_malformed: usize,
}

/// One **protocol round** under this crate's default refinement policy: classify every active range this peer was
/// asked to answer as SKIP, IDLIST or SPLIT.
/// A range's fate is read exhaustively off the outputs: `child_ranges` (SPLIT),
/// `enumeration_ranges` (IDLIST), **both** (an IDLIST against a non-empty peer range, which also
/// bounces the parent back), or neither (SKIP, or dropped as malformed and counted in
/// [`RoundOutcome::dropped_malformed`]).
/// The rule is a [`RefinementPolicy`], swappable through [`protocol_round_with_policy`] without a
/// protocol break. Split children are pairwise disjoint and cover the parent range.
pub fn protocol_round<K, B: RsosView<K>>(
    local: &B,
    active_ranges: Vec<RangeAggregate<K>>,
    child_ranges: &mut Vec<RangeAggregate<K>>,
    enumeration_ranges: &mut Vec<EnumerationRange<K>>,
    rng: &mut StdRng,
) -> RoundOutcome
where
    K: Clone,
{
    protocol_round_with_policy(
        local,
        &DEFAULT_POLICY,
        active_ranges,
        child_ranges,
        enumeration_ranges,
        rng,
    )
}

/// [`protocol_round`] with a caller-supplied refinement policy.
///
/// The policy chooses the outcome and split width. Bounds validation, rank arithmetic, partitioning,
/// and the progress guard remain enforced by the protocol driver. A non-progressing split is
/// converted to enumeration.
///
/// # RNG seam
/// `rng` is injected, never drawn from ambient/thread-local entropy: the driver's own tests stay
/// deterministic under a seeded `StdRng`, and a caller with no session-scoped RNG to reuse
/// (`Replica`/`ReadReplicaMap` share one across rounds and peers, `src/replica/dispatch.rs`) can
/// seed one fresh per call. Consumed only when a [`crate::Decision::Split`] actually reaches the fan-out
/// below — a [`crate::Decision::Skip`]/[`crate::Decision::Enumerate`] round draws nothing.
pub fn protocol_round_with_policy<K, B, P>(
    local: &B,
    policy: &P,
    active_ranges: Vec<RangeAggregate<K>>,
    child_ranges: &mut Vec<RangeAggregate<K>>,
    enumeration_ranges: &mut Vec<EnumerationRange<K>>,
    rng: &mut StdRng,
) -> RoundOutcome
where
    K: Clone,
    B: RsosView<K>,
    P: RefinementPolicy + ?Sized,
{
    let mut driver = Driver::default();
    for segment in active_ranges {
        let RangeAggregate {
            range: KeyRange(start, end),
            aggregate: remote,
        } = segment;
        let aggregate = local.aggregate(KeyRange::new(start.clone(), end.clone()));
        let size = local.size();
        let raw_start = match &start {
            StartBound::Unbounded => 0,
            StartBound::Included(key) => local.rank(key),
        };
        let raw_end = match &end {
            EndBound::Unbounded => size,
            EndBound::Excluded(key) => local.rank(key),
        };
        let Some(bounded) = driver.admit(start, end, size, raw_start, raw_end) else {
            continue;
        };
        let BoundedRange {
            start,
            end,
            start_index,
            end_index,
        } = bounded;
        match driver.classify(aggregate, remote, policy) {
            Decision::Skip => {}
            Decision::Enumerate => {
                driver.enumerate(start, end, remote, child_ranges, enumeration_ranges)
            }
            Decision::Split(stride) => {
                let mut split = Split::new(start_index, end_index, stride, rng);
                let mut current = start;
                for _ in 0..split.cuts() {
                    let index = split.next_cut();
                    let key = local.select(index).clone();
                    let (range, next) = Split::child(current, key);
                    let child_aggregate = local.aggregate(range.clone());
                    driver.emit(range, child_aggregate, child_ranges);
                    current = next;
                }
                let range = KeyRange::new(current, end);
                let child_aggregate = if split.uncut() {
                    aggregate
                } else {
                    local.aggregate(range.clone())
                };
                driver.emit(range, child_aggregate, child_ranges);
            }
        }
    }
    driver.outcome()
}

/// Fallible asynchronous equivalent of [`initial_ranges`].
///
/// All queries in a subsequent round must observe the same logical snapshot for that round.
pub async fn initial_ranges_async<K, B: AsyncRsosView<K>>(
    local: &B,
) -> Result<Vec<RangeAggregate<K>>, B::Error> {
    let aggregate = local
        .aggregate((Bound::Unbounded, Bound::Unbounded))
        .await?;
    Ok(vec![RangeAggregate {
        range: KeyRange::new(StartBound::Unbounded, EndBound::Unbounded),
        aggregate,
    }])
}

/// Fallible asynchronous equivalent of [`protocol_round`], using the same refinement plan.
///
/// Errors and cancellation preserve caller output vectors and RNG. See
/// [`protocol_round_with_policy_async`] for the complete transaction boundary.
pub async fn protocol_round_async<K: Clone, B: AsyncRsosView<K>>(
    local: &B,
    active_ranges: Vec<RangeAggregate<K>>,
    child_ranges: &mut Vec<RangeAggregate<K>>,
    enumeration_ranges: &mut Vec<EnumerationRange<K>>,
    rng: &mut StdRng,
) -> Result<RoundOutcome, B::Error> {
    protocol_round_with_policy_async(
        local,
        &DEFAULT_POLICY,
        active_ranges,
        child_ranges,
        enumeration_ranges,
        rng,
    )
    .await
}

/// Fallible asynchronous round under a caller-supplied policy.
///
/// The same private range planner drives the synchronous and asynchronous executors. This
/// executor stages outputs and a cloned RNG, appending outputs and committing the RNG only
/// after every query succeeds. Query errors, snapshot expiry and dropping the future leave
/// caller output vectors and RNG unchanged. Active input ranges are consumed: callers must
/// retain them separately if retry is needed. Policy side effects and backend side effects
/// are outside this rollback boundary. A view must obey [`AsyncRsosView`]'s snapshot contract.
///
/// The returned future is `Send` when keys are `Send + Sync` and the view and policy are `Sync`.
/// No runtime, heap-allocated future or physical paging model is imposed.
pub async fn protocol_round_with_policy_async<K, B, P>(
    local: &B,
    policy: &P,
    active_ranges: Vec<RangeAggregate<K>>,
    child_ranges: &mut Vec<RangeAggregate<K>>,
    enumeration_ranges: &mut Vec<EnumerationRange<K>>,
    rng: &mut StdRng,
) -> Result<RoundOutcome, B::Error>
where
    K: Clone,
    B: AsyncRsosView<K>,
    P: RefinementPolicy + ?Sized,
{
    let mut driver = Driver::default();
    let mut children = Vec::new();
    let mut enumerations = Vec::new();
    let mut staged_rng = rng.clone();
    for segment in active_ranges {
        let RangeAggregate {
            range: KeyRange(start, end),
            aggregate: remote,
        } = segment;
        let aggregate = local
            .aggregate((start.clone().into(), end.clone().into()))
            .await?;
        let size = local.size().await?;
        let raw_start = match &start {
            StartBound::Unbounded => 0,
            StartBound::Included(key) => local.rank(key).await?,
        };
        let raw_end = match &end {
            EndBound::Unbounded => size,
            EndBound::Excluded(key) => local.rank(key).await?,
        };
        let Some(bounded) = driver.admit(start, end, size, raw_start, raw_end) else {
            continue;
        };
        let BoundedRange {
            start,
            end,
            start_index,
            end_index,
        } = bounded;
        match driver.classify(aggregate, remote, policy) {
            Decision::Skip => {}
            Decision::Enumerate => {
                driver.enumerate(start, end, remote, &mut children, &mut enumerations)
            }
            Decision::Split(stride) => {
                let mut split = Split::new(start_index, end_index, stride, &mut staged_rng);
                let mut current = start;
                for _ in 0..split.cuts() {
                    let index = split.next_cut();
                    let key = local.select(index).await?;
                    let (range, next) = Split::child(current, key);
                    let child_aggregate = local
                        .aggregate((range.0.clone().into(), range.1.clone().into()))
                        .await?;
                    driver.emit(range, child_aggregate, &mut children);
                    current = next;
                }
                let range = KeyRange::new(current, end);
                let child_aggregate = if split.uncut() {
                    aggregate
                } else {
                    local
                        .aggregate((range.0.clone().into(), range.1.clone().into()))
                        .await?
                };
                driver.emit(range, child_aggregate, &mut children);
            }
        }
    }
    child_ranges.extend(children);
    enumeration_ranges.extend(enumerations);
    *rng = staged_rng;
    Ok(driver.outcome())
}

#[cfg(test)]
mod tests;
