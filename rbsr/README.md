# rbsr

Transport-independent Range-Based Set Reconciliation over an RSOS backend.

## Origin

**RBSR is not a protocol introduced by this crate.** This implementation follows:

> Aljoscha Meyer, *Range-Based Set Reconciliation*, 42nd IEEE International Symposium on
> Reliable Distributed Systems (SRDS), 2023, pp. 59–69,
> DOI: [10.1109/SRDS60354.2023.00016](https://doi.org/10.1109/SRDS60354.2023.00016),
> [arXiv:2212.13567](https://arxiv.org/abs/2212.13567).

The `RsosView` backend used by this crate follows the Range-Summarizable Order-Statistics Store
abstraction formalized by Elvio G. Amparore in *Range-Based Set Reconciliation via
Range-Summarizable Order-Statistics Stores* (2026,
[arXiv:2603.19820](https://arxiv.org/abs/2603.19820)).

This crate is an independent Rust implementation of those ideas; it does not claim authorship of
the underlying RBSR protocol or RSOS abstraction. See the repository
[references](../REFERENCES.md) for the full citations.

See the crate rustdoc for the protocol driver, range messages, and refinement policies.

## Synchronous and asynchronous queries

`initial_ranges` and `protocol_round[_with_policy]` call the borrowed, infallible `RsosView`
directly. The RAM path does not poll a future or require an executor.

`initial_ranges_async` and `protocol_round[_with_policy]_async` consume `AsyncRsosView` queries:
fallible `size`, `aggregate`, `rank` and owned-key `select`, using native Send futures. The store
must pin one coherent logical snapshot across a round's suspension points, or return an error
if it expires. Physical pages and stored values remain outside this interface.

Both executors share bound admission, policy decisions, progress guards, scalar cut planning,
half-open child construction and outcome accounting. Each query executor retains its bounds
and aggregates locally and iterates a precomputed number of cuts. Async errors and cancellation leave caller
output vectors and RNG unchanged; active input ranges are consumed, and policy/backend side
effects are outside that rollback contract. `ImmediateView::new(&snapshot)` adapts a synchronous
view with ready, infallible futures, cloning only selected boundary keys.

This is a query execution seam, not a storage transaction, a production disk backend or a
replication runtime. No async runtime, backend dependency or wire-format change is introduced.

Licensed under MIT OR Apache-2.0.
