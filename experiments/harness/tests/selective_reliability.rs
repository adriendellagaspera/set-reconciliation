// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! #69: matched, identity-addressed fault replay for the real small-frame RBSR control and a
//! benchmark-only selective-retransmission transport.

#![forbid(unsafe_code)]

#[path = "selective_reliability/bitmap.rs"]
mod bitmap;
#[path = "selective_reliability/harness.rs"]
mod harness;
#[path = "selective_reliability/report.rs"]
mod report;
#[path = "selective_reliability/wire.rs"]
mod wire;

use bitmap::{run_bitmap_sample, run_minimal_bitmap_sample, MAX_TRACKED_BITMAP_FLIGHTS};
use harness::{fixture, run_sample};
use report::{print_bitmap_triplet, print_minimal_triplet, print_pair};
use wire::{
    decode_bitmap_ack, decode_envelope, decode_minimal_envelope, encode_bitmap_ack,
    encode_envelope, encode_minimal_envelope, stable_id, BitmapAck, BitmapAckEntry, FaultKey,
    FaultProfile, Mode, WireKey, DATA, MAX_FRAMES_PER_FLIGHT, MAX_PENDING_FRAMES,
    TRACKED_RECEIVER_FLIGHTS,
};

#[tokio::test]
#[ignore = "100k-entry matched replay sweep; run explicitly in one-off CI"]
async fn matched_control_vs_selective_retry() {
    let fixture = fixture();
    let profiles = [
        ("clean", FaultProfile::clean(0x1000), 4u64),
        ("loss_0_1", FaultProfile::data_loss(0x2000, 10), 8),
        ("loss_1", FaultProfile::data_loss(0x3000, 100), 8),
        ("loss_5", FaultProfile::data_loss(0x4000, 500), 8),
        ("mixed", FaultProfile::mixed(0x5000), 8),
    ];
    for budget in [1_200, 1_472] {
        for (name, base, trials) in profiles {
            let mut exercised = [0usize; 4];
            for trial in 0..trials {
                let profile = base.with_scenario(base.scenario + trial);
                let control = run_sample(&fixture, Mode::Control, budget, profile).await;
                let candidate = run_sample(&fixture, Mode::Selective, budget, profile).await;
                print_pair(budget, name, profile.scenario, &control, &candidate);
                assert!(control.converged, "control failed: {name} {trial}");
                assert!(candidate.converged, "candidate failed: {name} {trial}");
                assert!(control.metrics.first_data_bytes > 0);
                assert!(candidate.metrics.first_data_bytes > 0);
                assert!(candidate.metrics.max_pending_frames <= MAX_PENDING_FRAMES);
                assert!(
                    candidate.metrics.max_seen_ids
                        <= TRACKED_RECEIVER_FLIGHTS * MAX_FRAMES_PER_FLIGHT
                );
                assert!(candidate.metrics.stale_rejected <= candidate.metrics.stale_injected);
                if name == "mixed" {
                    exercised[0] += candidate.metrics.control_fault_drops;
                    exercised[1] += candidate.metrics.fault_duplicates;
                    exercised[2] += candidate.metrics.fault_reorders;
                    exercised[3] += candidate.metrics.stale_rejected;
                }
            }
            if name == "mixed" {
                assert!(
                    exercised.iter().all(|count| *count > 0),
                    "mixed scenario must exercise ACK loss, duplication, reorder and stale epochs"
                );
            }
        }
    }
}

#[tokio::test]
#[ignore = "100k-entry bitmap ACK sweep; run explicitly in one-off CI"]
async fn bitmap_ack_vs_per_frame_ack() {
    let fixture = fixture();
    let profiles = [
        ("clean", FaultProfile::clean(0x6000), 4u64),
        ("loss_0_1", FaultProfile::data_loss(0x7000, 10), 8),
        ("loss_1", FaultProfile::data_loss(0x8000, 100), 8),
        ("loss_5", FaultProfile::data_loss(0x9000, 500), 8),
        ("mixed", FaultProfile::mixed(0xa000), 8),
    ];
    for budget in [1_200, 1_472] {
        for (name, base, trials) in profiles {
            let mut exercised = [0usize; 4];
            for trial in 0..trials {
                let profile = base.with_scenario(base.scenario + trial);
                let control = run_sample(&fixture, Mode::Control, budget, profile).await;
                let per_frame = run_sample(&fixture, Mode::Selective, budget, profile).await;
                let bitmap = run_bitmap_sample(&fixture, budget, profile).await;
                print_bitmap_triplet(
                    budget,
                    name,
                    profile.scenario,
                    &control,
                    &per_frame,
                    &bitmap,
                );
                assert!(control.converged, "control failed: {name} {trial}");
                assert!(per_frame.converged, "per-frame ACK failed: {name} {trial}");
                assert!(bitmap.converged, "bitmap ACK failed: {name} {trial}");
                assert!(bitmap.metrics.max_pending_frames <= MAX_PENDING_FRAMES);
                assert!(
                    bitmap.metrics.max_seen_ids
                        <= MAX_TRACKED_BITMAP_FLIGHTS * MAX_FRAMES_PER_FLIGHT
                );
                assert!(bitmap.metrics.max_receiver_flights <= MAX_TRACKED_BITMAP_FLIGHTS);
                assert!(bitmap.metrics.stale_rejected <= bitmap.metrics.stale_injected);
                if name == "mixed" {
                    exercised[0] += bitmap.metrics.control_fault_drops;
                    exercised[1] += bitmap.metrics.fault_duplicates;
                    exercised[2] += bitmap.metrics.fault_reorders;
                    exercised[3] += bitmap.metrics.stale_rejected;
                }
            }
            if name == "mixed" {
                assert!(
                    exercised.iter().all(|count| *count > 0),
                    "bitmap mixed scenario must exercise ACK loss, duplication, reorder and stale"
                );
            }
        }
    }
}

#[tokio::test]
#[ignore = "100k-entry minimal reliability wire sweep; run explicitly in one-off CI"]
async fn minimal_wire_vs_bitmap() {
    let fixture = fixture();
    let profiles = [
        ("clean", FaultProfile::clean(0xb000), 4u64),
        ("loss_0_1", FaultProfile::data_loss(0xc000, 10), 8),
        ("loss_1", FaultProfile::data_loss(0xd000, 100), 8),
        ("loss_5", FaultProfile::data_loss(0xe000, 500), 8),
        ("mixed", FaultProfile::mixed(0xf000), 8),
    ];
    for budget in [1_200, 1_472] {
        for (name, base, trials) in profiles {
            let mut exercised = [0usize; 4];
            for trial in 0..trials {
                let profile = base.with_scenario(base.scenario + trial);
                let control = run_sample(&fixture, Mode::Control, budget, profile).await;
                let bitmap = run_bitmap_sample(&fixture, budget, profile).await;
                let minimal = run_minimal_bitmap_sample(&fixture, budget, profile).await;
                print_minimal_triplet(budget, name, profile.scenario, &control, &bitmap, &minimal);
                assert!(control.converged, "control failed: {name} {trial}");
                assert!(bitmap.converged, "bitmap failed: {name} {trial}");
                assert!(minimal.converged, "minimal wire failed: {name} {trial}");
                assert!(minimal.metrics.max_pending_frames <= MAX_PENDING_FRAMES);
                assert!(
                    minimal.metrics.max_seen_ids
                        <= MAX_TRACKED_BITMAP_FLIGHTS * MAX_FRAMES_PER_FLIGHT
                );
                assert!(minimal.metrics.max_receiver_flights <= MAX_TRACKED_BITMAP_FLIGHTS);
                assert!(minimal.metrics.stale_rejected <= minimal.metrics.stale_injected);
                if name == "mixed" {
                    exercised[0] += minimal.metrics.control_fault_drops;
                    exercised[1] += minimal.metrics.fault_duplicates;
                    exercised[2] += minimal.metrics.fault_reorders;
                    exercised[3] += minimal.metrics.stale_rejected;
                }
            }
            if name == "mixed" {
                assert!(
                    exercised.iter().all(|count| *count > 0),
                    "minimal mixed scenario must exercise ACK loss, duplication, reorder and stale"
                );
            }
        }
    }
}

#[test]
fn envelope_and_fault_identity_are_stable() {
    let key = WireKey {
        epoch: 7,
        flight: 11,
        slot: 3,
        semantic: stable_id(b"payload"),
    };
    let encoded = encode_envelope(DATA, key, b"payload");
    let (kind, decoded, payload) = decode_envelope(&encoded).unwrap();
    assert_eq!(kind, DATA);
    assert_eq!(decoded, key);
    assert_eq!(payload, b"payload");

    let minimal = encode_minimal_envelope(DATA, key, b"payload");
    let (minimal_kind, minimal_key, minimal_payload) = decode_minimal_envelope(&minimal).unwrap();
    assert_eq!(minimal_kind, DATA);
    assert_eq!(minimal_key.epoch, key.epoch);
    assert_eq!(minimal_key.flight, key.flight);
    assert_eq!(minimal_key.slot, key.slot);
    assert_eq!(minimal_key.semantic, stable_id(b"payload"));
    assert_eq!(minimal_payload, b"payload");

    let bitmap_ack = BitmapAck {
        epoch: 7,
        entries: vec![
            BitmapAckEntry {
                flight: 42,
                bits: 0x8000_0000_0000_0001,
            },
            BitmapAckEntry {
                flight: 43,
                bits: 0x3,
            },
        ],
    };
    assert_eq!(
        decode_bitmap_ack(&encode_bitmap_ack(&bitmap_ack).unwrap()).unwrap(),
        bitmap_ack
    );

    let profile = FaultProfile::mixed(0x55aa);
    let fault = FaultKey {
        direction: 1,
        kind: DATA,
        semantic: key.semantic,
        attempt: 2,
    };
    let first = profile.decision(fault);
    let second = profile.decision(fault);
    assert_eq!(first.drop, second.drop);
    assert_eq!(first.duplicate, second.duplicate);
    assert_eq!(first.reorder, second.reorder);
    assert_eq!(first.stale, second.stale);
}
