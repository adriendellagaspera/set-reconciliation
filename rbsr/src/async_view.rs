//! Fallible queries against a coherent logical snapshot, independent of its physical layout.

use std::convert::Infallible;
use std::future::{ready, Future};
use std::ops::Bound;

use rsos::Aggregate;

use crate::RsosView;

/// The four RBSR queries for a fallible, potentially asynchronous store.
///
/// A view must pin one coherent logical snapshot for the entire round, including across
/// suspension points. The rank, count and fingerprint laws of [`RsosView`] apply. If that
/// snapshot expires, return an error: never silently continue against a newer root. Even
/// inverted bounds must be safe to query (an empty aggregate), as on [`RsosView`].
///
/// `select` returns an owned boundary key; values and physical pages never enter this contract.
/// Enumeration and mutation remain the caller's responsibility. No executor, boxing or storage
/// dependency is required. Implementations may use native async methods or ready futures.
pub trait AsyncRsosView<K> {
    /// Store-specific query or snapshot-expiry error. RBSR propagates it without conversion.
    type Error;

    /// Number of elements in the pinned snapshot.
    fn size(&self) -> impl Future<Output = Result<usize, Self::Error>> + Send;

    /// Count and fingerprint of the requested logical range in the pinned snapshot.
    fn aggregate(
        &self,
        range: (Bound<K>, Bound<K>),
    ) -> impl Future<Output = Result<Aggregate, Self::Error>> + Send;

    /// Number of keys strictly below `key` in the pinned snapshot.
    fn rank(&self, key: &K) -> impl Future<Output = Result<usize, Self::Error>> + Send;

    /// Owned key at zero-based in-order position `rank` (strictly below `size()`).
    fn select(&self, rank: usize) -> impl Future<Output = Result<K, Self::Error>> + Send;
}

/// Infallible ready-future adapter over a borrowed synchronous view.
///
/// Only selected boundary keys are cloned; stored values remain untouched. The caller must
/// keep the borrowed view coherent, just as for [`RsosView`]. The synchronous protocol API
/// calls the original view directly and does not use this adapter.
pub struct ImmediateView<'a, B: ?Sized> {
    inner: &'a B,
}

impl<'a, B: ?Sized> ImmediateView<'a, B> {
    /// Borrow a synchronous snapshot.
    pub fn new(inner: &'a B) -> Self {
        Self { inner }
    }
}

impl<K: Clone + Send, B: RsosView<K> + ?Sized> AsyncRsosView<K> for ImmediateView<'_, B> {
    type Error = Infallible;

    fn size(&self) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        ready(Ok(self.inner.size()))
    }

    fn aggregate(
        &self,
        range: (Bound<K>, Bound<K>),
    ) -> impl Future<Output = Result<Aggregate, Self::Error>> + Send {
        ready(Ok(self.inner.aggregate(range)))
    }

    fn rank(&self, key: &K) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        ready(Ok(self.inner.rank(key)))
    }

    fn select(&self, rank: usize) -> impl Future<Output = Result<K, Self::Error>> + Send {
        ready(Ok(self.inner.select(rank).clone()))
    }
}
