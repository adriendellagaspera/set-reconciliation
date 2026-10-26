//! Shared admission, classification, cut arithmetic and output accounting.
//! Query executors retain their bounds and aggregates locally.

use rand::{rngs::StdRng, Rng};
use rsos::Aggregate;
use tracing::debug;

use crate::{Comparison, Decision, RefinementPolicy, SplitStride};

use super::rank::{AdmittedRank, BoundedRange, InvertedRange};
use super::{EndBound, EnumerationRange, KeyRange, RangeAggregate, RoundOutcome, StartBound};

#[derive(Default)]
pub(super) struct Driver {
    outcome: RoundOutcome,
}

impl Driver {
    #[inline(always)]
    pub(super) fn admit<K>(
        &mut self,
        start: StartBound<K>,
        end: EndBound<K>,
        size: usize,
        raw_start: usize,
        raw_end: usize,
    ) -> Option<BoundedRange<K>> {
        match BoundedRange::from_ranks(start, end, size, raw_start, raw_end) {
            Ok(bounded) => Some(bounded),
            Err(InvertedRange { raw_start, raw_end }) => {
                debug!("dropping malformed segment: start rank {raw_start} > end rank {raw_end}");
                self.outcome.dropped_malformed += 1;
                None
            }
        }
    }

    #[inline(always)]
    pub(super) fn classify<P: RefinementPolicy + ?Sized>(
        &mut self,
        local: Aggregate,
        remote: Aggregate,
        policy: &P,
    ) -> Decision {
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
        match decision {
            Decision::Skip => self.outcome.skipped += 1,
            Decision::Enumerate => self.outcome.enumerated += 1,
            Decision::Split(_) => self.outcome.split += 1,
        }
        decision
    }

    #[inline(always)]
    pub(super) fn enumerate<K: Clone>(
        &mut self,
        start: StartBound<K>,
        end: EndBound<K>,
        remote: Aggregate,
        children: &mut Vec<RangeAggregate<K>>,
        enumerations: &mut Vec<EnumerationRange<K>>,
    ) {
        if std::num::NonZeroUsize::new(remote.size()).is_some() {
            self.emit(
                KeyRange::new(start.clone(), end.clone()),
                Aggregate::ZERO,
                children,
            );
        }
        enumerations.push((start.into(), end.into()));
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

/// Only scalar cut state. Bounds and summaries remain in the query executor's own locals.
pub(super) struct Split {
    cur_index: AdmittedRank,
    end_index: AdmittedRank,
    stride: usize,
    remainder: usize,
    child_count: usize,
    short_block: Option<usize>,
    block: usize,
}

impl Split {
    #[inline(always)]
    pub(super) fn new(
        start: AdmittedRank,
        end: AdmittedRank,
        stride: SplitStride,
        rng: &mut StdRng,
    ) -> Self {
        let stride = stride.get();
        let actual_span = end.get() - start.get();
        let remainder = actual_span % stride;
        let child_count = block_count(actual_span, stride).max(1);
        let short_block = (remainder != 0).then(|| rng.gen_range(0..child_count));
        Self {
            cur_index: start,
            end_index: end,
            stride,
            remainder,
            child_count,
            short_block,
            block: 0,
        }
    }

    // Bound query work independently of cursor updates, as the protocol's fixed fan-out does.
    #[inline(always)]
    pub(super) fn cuts(&self) -> usize {
        self.child_count.saturating_sub(1)
    }

    // Called exactly `cuts()` times by the query executor's bounded loop.
    #[inline(always)]
    pub(super) fn next_cut(&mut self) -> usize {
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
        self.cur_index.get()
    }

    #[inline(always)]
    pub(super) fn uncut(&self) -> bool {
        self.block == 0
    }

    /// One interior boundary, with disjoint half-open children by construction.
    #[inline(always)]
    pub(super) fn child<K: Clone>(start: StartBound<K>, key: K) -> (KeyRange<K>, StartBound<K>) {
        (
            KeyRange::new(start, EndBound::Excluded(key.clone())),
            StartBound::Included(key),
        )
    }
}

pub(super) fn block_count(actual_span: usize, stride: usize) -> usize {
    actual_span.div_ceil(stride)
}
