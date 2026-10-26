// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Resolving a wire [`StartBound`]/[`EndBound`] pair against a concrete store: [`BoundedRange`]'s
//! admission and clamping arithmetic, its rank vocabulary ([`AdmittedRank`]/[`StoreSize`]), and
//! the one way admission can fail ([`InvertedRange`]).

use tracing::debug;

use super::{EndBound, StartBound};

/// A [`super::RangeAggregate`]'s range checked against a concrete local set. Bound shapes are
/// already guaranteed by [`StartBound`]/[`EndBound`], so the one remaining malformation — an
/// inverted range — needs a set to be detected, hence a fallible constructor.
pub(super) struct BoundedRange<K> {
    pub(super) start: StartBound<K>,
    pub(super) end: EndBound<K>,
    pub(super) start_index: AdmittedRank,
    pub(super) end_index: AdmittedRank,
}

/// A rank a backend returned that has been **admitted** into that backend's own store.
/// Its existence is the proof that [`crate::RsosView`]'s **rank-within-store** law (`rank(z) <= size`)
/// was applied: the only way to obtain one is [`StoreSize::admit`], which performs the clamp, so a
/// raw answer cannot reach [`crate::RsosView::select`] by accident. A backend that breaks the law therefore
/// cannot be walked off the end of its own store by a key the remote peer chose.
/// **State typing**, the shape this workspace already uses wherever untrusted input crosses into
/// trusted arithmetic: `lww_register::clock`'s `AdmittedTime` (a peer's time, admitted through
/// `clamped_to_drift`) and `gossip`'s `Payload<Authenticated>`/`Payload<Verified>` (a datagram,
/// verified before it can be handled). Named rather than linked — `rbsr` depends on `rsos` alone,
/// so neither crate is in scope here.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct AdmittedRank(usize);

/// The size of the store a rank was answered against, read once.
/// It carries two properties this driver relies on. It is the **authority that admits** a rank —
/// `size.admit(raw)`, never `admit(raw, size)` — so the bound and the value it bounds are different
/// types and cannot be swapped at a call site, which two bare `usize` parameters silently allow
/// . And reading it once rather than per use keeps a round's arithmetic
/// self-consistent even against a backend that breaks **one-snapshot-per-round**.
#[derive(Clone, Copy, Debug)]
struct StoreSize(usize);

impl StoreSize {
    /// Admit a rank this store answered, clamping it to this size.
    fn admit(self, raw: usize) -> AdmittedRank {
        AdmittedRank(raw.min(self.0))
    }
}

impl AdmittedRank {
    pub(super) fn get(self) -> usize {
        self.0
    }

    /// The next cut, `stride` further on, or `None` once that would reach `end`.
    /// The result is `< end <= size`, hence a valid [`crate::RsosView::select`] argument by construction
    /// — the reason the fan-out walks through this rather than through bare `usize` arithmetic.
    /// Saturating, so this never panics regardless of the `overflow-checks` profile setting.
    pub(super) fn cut_before(self, end: AdmittedRank, stride: usize) -> Option<AdmittedRank> {
        let next = AdmittedRank(self.0.saturating_add(stride));
        (next < end).then_some(next)
    }
}

/// The one way [`BoundedRange::from_ranks`] can fail: the segment's start ranks *after* its end in the
/// store it was checked against.
/// Carries both ranks as the backend returned them — unbounded — so the drop can name the numbers
/// that made the segment malformed instead of only its category.
pub(super) struct InvertedRange {
    pub(super) raw_start: usize,
    pub(super) raw_end: usize,
}

fn clamped_rank_offender(
    raw_start: usize,
    raw_end: usize,
    start_index: AdmittedRank,
    end_index: AdmittedRank,
) -> Option<usize> {
    if start_index.get() != raw_start {
        Some(raw_start)
    } else if end_index.get() != raw_end {
        Some(raw_end)
    } else {
        None
    }
}

impl<K> BoundedRange<K> {
    /// Absolute positions from [`crate::RsosView::rank`], which the fan-out steps through with
    /// [`crate::RsosView::select`] — an aggregate gives the count, not the positions. Admitted here, see
    /// [`AdmittedRank`], where a backend's answer stops being trusted.
    pub(super) fn from_ranks(
        start: StartBound<K>,
        end: EndBound<K>,
        size: usize,
        raw_start: usize,
        raw_end: usize,
    ) -> Result<Self, InvertedRange> {
        let size = StoreSize(size);
        // Judged on the raw answers, before bounding: an inverted range is a malformed *wire*
        // segment and must stay distinguishable from a backend that merely over-reports rank.
        if raw_end < raw_start {
            return Err(InvertedRange { raw_start, raw_end });
        }
        let start_index = size.admit(raw_start);
        let end_index = size.admit(raw_end);
        // The clamp bit exactly when admitting changed the value — no flag to carry alongside.
        if let Some(offender) = clamped_rank_offender(raw_start, raw_end, start_index, end_index) {
            let size = size.0;
            debug!(
                "RsosView backend broke rank-within-store: returned rank {offender} for a store of \
                 size {size}; bounding it so `select` stays inside that store"
            );
        }
        Ok(BoundedRange {
            start,
            end,
            start_index,
            end_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{clamped_rank_offender, StoreSize};

    #[test]
    fn clamped_rank_offender_identifies_the_changed_admission() {
        let size = StoreSize(3);

        assert_eq!(
            clamped_rank_offender(4, 5, size.admit(4), size.admit(5)),
            Some(4),
            "when both ranks are clamped, report the start first"
        );
        assert_eq!(
            clamped_rank_offender(2, 5, size.admit(2), size.admit(5)),
            Some(5),
            "an unchanged start leaves an over-reported end as the offender"
        );
        assert_eq!(
            clamped_rank_offender(2, 3, size.admit(2), size.admit(3)),
            None,
            "ranks already within the store need no diagnostic"
        );
    }

    #[test]
    fn cut_before_is_strictly_before_the_end_rank() {
        let size = StoreSize(5);
        let start = size.admit(2);
        let end = size.admit(5);

        assert_eq!(
            start.cut_before(end, 3),
            None,
            "a cut that lands exactly on the end is not an interior boundary"
        );
        assert_eq!(
            start.cut_before(end, 2).map(|rank| rank.get()),
            Some(4),
            "a cut strictly inside the range remains admissible"
        );
    }
}
