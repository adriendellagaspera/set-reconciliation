//! Shared per-range refinement plan. Query execution remains with the sync/async caller.

use rand::{rngs::StdRng, Rng};
use rsos::Aggregate;
use tracing::debug;

use crate::{Comparison, Decision, RefinementPolicy};

use super::rank::{AdmittedRank, BoundedRange, InvertedRange};
use super::{EndBound, EnumerationRange, KeyRange, RangeAggregate, RoundOutcome, StartBound};

#[derive(Default)]
pub(super) struct Driver {
    outcome: RoundOutcome,
}

impl Driver {
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare<K: Clone, P: RefinementPolicy + ?Sized>(
        &mut self,
        start: StartBound<K>,
        end: EndBound<K>,
        local: Aggregate,
        remote: Aggregate,
        size: usize,
        raw_start: usize,
        raw_end: usize,
        policy: &P,
        children: &mut Vec<RangeAggregate<K>>,
        enumerations: &mut Vec<EnumerationRange<K>>,
        rng: &mut StdRng,
    ) -> Option<Split<K>> {
        let bounded = match BoundedRange::from_ranks(start, end, size, raw_start, raw_end) {
            Ok(bounded) => bounded,
            Err(InvertedRange { raw_start, raw_end }) => {
                debug!("dropping malformed segment: start rank {raw_start} > end rank {raw_end}");
                self.outcome.dropped_malformed += 1;
                return None;
            }
        };
        let comparison = Comparison::new(local, remote, self.outcome.children);
        let span = comparison.span();
        let decision = match policy.decide(comparison) {
            Decision::Split(stride) if span > 1 && stride.get() >= span => {
                debug!(
                    "non-progressing SPLIT: stride {} >= span {span}; forcing IDLIST",
                    stride.get()
                );
                Decision::Enumerate
            }
            other => other,
        };
        let BoundedRange {
            start,
            end,
            start_index,
            end_index,
        } = bounded;
        match decision {
            Decision::Skip => {
                self.outcome.skipped += 1;
                None
            }
            Decision::Enumerate => {
                self.outcome.enumerated += 1;
                if std::num::NonZeroUsize::new(remote.size()).is_some() {
                    self.emit(
                        KeyRange::new(start.clone(), end.clone()),
                        Aggregate::ZERO,
                        children,
                    );
                }
                enumerations.push((start.into(), end.into()));
                None
            }
            Decision::Split(stride) => {
                self.outcome.split += 1;
                let stride = stride.get();
                let actual_span = end_index.get() - start_index.get();
                let remainder = actual_span % stride;
                let child_count = block_count(actual_span, stride).max(1);
                let short_block = (remainder != 0).then(|| rng.gen_range(0..child_count));
                Some(Split {
                    start,
                    end,
                    end_index,
                    cur_index: start_index,
                    stride,
                    remainder,
                    child_count,
                    short_block,
                    block: 0,
                    parent: local,
                })
            }
        }
    }

    #[inline(always)]
    pub(super) fn emit<K>(
        &mut self,
        range: KeyRange<K>,
        aggregate: Aggregate,
        children: &mut Vec<RangeAggregate<K>>,
    ) {
        children.push(RangeAggregate { range, aggregate });
        self.outcome.children += 1;
    }

    #[inline(always)]
    pub(super) fn outcome(self) -> RoundOutcome {
        self.outcome
    }
}

pub(super) struct Split<K> {
    start: StartBound<K>,
    end: EndBound<K>,
    end_index: AdmittedRank,
    cur_index: AdmittedRank,
    stride: usize,
    remainder: usize,
    child_count: usize,
    short_block: Option<usize>,
    block: usize,
    parent: Aggregate,
}

impl<K: Clone> Split<K> {
    #[inline(always)]
    pub(super) fn next_cut(&mut self) -> Option<usize> {
        if self.block >= self.child_count.saturating_sub(1) {
            return None;
        }
        let stride = if self.short_block == Some(self.block) {
            self.remainder
        } else {
            self.stride
        };
        self.cur_index = self
            .cur_index
            .cut_before(self.end_index, stride)
            .expect("non-final split child must cut before the parent end");
        self.block += 1;
        Some(self.cur_index.get())
    }

    #[inline(always)]
    pub(super) fn cut(&mut self, key: K) -> KeyRange<K> {
        let start = std::mem::replace(&mut self.start, StartBound::Included(key.clone()));
        KeyRange::new(start, EndBound::Excluded(key))
    }

    #[inline(always)]
    pub(super) fn finish(self) -> (KeyRange<K>, Option<Aggregate>) {
        let aggregate = (self.block == 0).then_some(self.parent);
        (KeyRange::new(self.start, self.end), aggregate)
    }
}

pub(super) fn block_count(actual_span: usize, stride: usize) -> usize {
    actual_span.div_ceil(stride)
}
