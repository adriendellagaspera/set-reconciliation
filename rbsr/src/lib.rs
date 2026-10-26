// Copyright 2026 Developers of the reconcile-rs project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Transport-independent Range-Based Set Reconciliation.
//!
//! [`initial_ranges`] starts a reconciliation. [`protocol_round`] advances one side by comparing
//! [`RangeAggregate`] values and producing ranges to enumerate or refine. Callers alternate rounds
//! until no active ranges remain.
//!
//! [`RsosView`] is the read-only store contract and is implemented for every [`rsos::Rsos`].
//! [`RefinementPolicy`] controls local refinement only; it is never negotiated on the wire.
//!
//! # Origin
//!
//! RBSR is not a protocol introduced by this crate. The implementation follows Aljoscha Meyer,
//! *Range-Based Set Reconciliation* (IEEE SRDS 2023, DOI: 10.1109/SRDS60354.2023.00016;
//! arXiv:2212.13567).
//!
//! The RSOS backend interface follows the abstraction formalized by Elvio G. Amparore,
//! *Range-Based Set Reconciliation via Range-Summarizable Order-Statistics Stores* (2026,
//! arXiv:2603.19820). This crate is an independent Rust implementation of those ideas.
//!
//! Equality uses both range cardinality and fingerprint.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod async_view;
mod policy;
#[cfg(rbsr_internal_testing)]
mod probe_harness;
mod protocol;
mod rsos_view;

pub use async_view::{AsyncRsosView, ImmediateView};

pub use policy::{
    Comparison, Decision, EnumerateBelowThreshold, FanOut, FixedFanOut, RefinementPolicy,
    SplitStride, SqrtFanOut,
};
#[cfg(rbsr_internal_testing)]
pub use policy::{ConstantStrideSplit, SpanHashedStrideSplit, STRIDE_SPREAD};
// Repository-only probe harness.
#[cfg(rbsr_internal_testing)]
pub use probe_harness::{
    balanced_swap, drive, drive_pair, Drive, NarrowStore, Termination, DRIVE_STORE_SIZE,
};
pub use protocol::{
    initial_ranges, initial_ranges_async, protocol_round, protocol_round_async,
    protocol_round_with_policy, protocol_round_with_policy_async, EnumerationRange, RangeAggregate,
    RoundOutcome,
};
pub use rsos_view::RsosView;

// Keep the `rsos` version used by public signatures directly reachable.
pub use rsos;
