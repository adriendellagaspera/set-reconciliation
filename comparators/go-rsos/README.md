# External comparator: phillipjad/go-rsos

This directory integrates the **original Go implementation** of a partitioned persistent range-summarizable order-statistics store, not a Rust port and not a replicated runtime. Upstream: https://github.com/phillipjad/go-rsos, MIT © 2026 Phillip Miavelstuck.

Pinned upstream revision: d4c1aee23b344dc9a61ada8ae3f5f849f5a841b2 (develop, 2026-08-18). The Go module is pinned to the matching pseudo-version. Go 1.23+ required. A publicly verified module checksum is resolved by Go's checksum database at download. The standalone comparator and its subprocess bridge never enter the published Rust workspace.

## Reproduce

From this directory:

    go mod download
    go test ./...
    echo '{"records":4096,"changed_keys":13,"shape":"updated-scattered","layout":"hashed","build":"bulk","split_factor":16,"resolve_threshold":128}' | go run .

For the matched Rust control (from repository root):

    (cd comparators/go-rsos && go build -o /tmp/go-rsos-comparator .)
    GO_RSOS_BIN=/tmp/go-rsos-comparator cargo run --manifest-path experiments/Cargo.toml -p set-reconciliation-experiments --bin compare_go_rsos --release <<<'{"records":4096,"changed_keys":13,"shape":"updated-scattered","layout":"hashed","build":"bulk","split_factor":16,"resolve_threshold":128}'

Output is JSON with explicit attribution and timings; the Rust command rejects incorrect signed differences before producing its combined report.

## Contract

- Workload: two replicas of 128-bit keys with 64-bit values, generated deterministically by logical index. The physical layout is either ordered (all keys in the first shard) or hashed (SplitMix64, spread across 256 shards). Equal logical entries always map to equal keys.
- Shapes: deletion-scattered, deletion-clustered, updated-scattered (equal counts but unequal values), right-only-scattered.
- Build: bulk-load from sorted keys or incremental insertion in 256-element batches. Timings include indexing *both* peers.
- Query: upstream Reconcile runs against the two forests, and returns exact signed differences. Both indices are frozen during reconciliation.
- Measurement: build/reconcile elapsed nanoseconds and logical Store calls / byte totals for BOTH peers. The instrument wraps the in-memory KV adapter. Get/Put/WriteBatch counts are not network round trips; WriteBatch counts as one call regardless of operation count. DeleteRange does not charge an unknown number of removed bytes. No end-to-end payload transfer, crash safety, concurrency validation, or WAN time is measured.
- Fingerprints and wire formats are **not interoperable**: upstream hashes SHA-256(ref || 0x00 || value_bytes), while rsos uses BLAKE3 over canonical key/value encodings. Only exact decoded (logical ID, version) differences are compared across languages. Go's Split and reconciliation decisions are not the Rust protocol transcript; do not compare their range counts or predict wire bytes from Store I/O counters.

A Go MemStore run is a **functional and index-cost comparator**, not proof that any particular remote KV adapter is safe, atomic, or efficient. In particular, go-rsos's cross-shard queries and optimistic versioning do not guarantee snapshot isolation under concurrent writes. The next experiment should use a latency-injected KV and a deliberately torn-batch/concurrent-writer fault model, recording true operation dependencies and recovery outcomes. See set-reconciliation issues #27, #36, #50 and reconcile-rs #313/#317.
