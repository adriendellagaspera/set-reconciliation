//! Match a pinned external Go RSOS index/protocol comparator against our Rust
//! RSOS/RBSR on the same immutable logical workload. Not a production interop test.

use std::env;
use std::error::Error;
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::time::Instant;

use rand::{rngs::StdRng, SeedableRng};
use rbsr::{initial_ranges, protocol_round_with_policy, FanOut, FixedFanOut};
use rsos::FingerprintTreeMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    records: u64,
    changed_keys: u64,
    shape: String,
    layout: String,
    build: String,
    split_factor: usize,
    resolve_threshold: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct Element {
    key: u64,
    version: u8,
}

const UPSTREAM: &str = "phillipjad/go-rsos@d4c1aee23b344dc9a61ada8ae3f5f849f5a841b2";

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn key(index: u64, layout: &str) -> [u8; 16] {
    let mut key = [0; 16];
    if layout == "hashed" {
        key[..8].copy_from_slice(&splitmix64(index ^ 0x6a09_e667_f3bc_c909).to_be_bytes());
        key[8..].copy_from_slice(&splitmix64(index ^ 0xbb67_ae85_84ca_a73b).to_be_bytes());
    } else {
        key[8..].copy_from_slice(&index.to_be_bytes());
    }
    key
}

fn changed(index: u64, case: &Case) -> bool {
    if case.changed_keys == 0 {
        return false;
    }
    if case.shape == "deletion-clustered" {
        let start = (case.records - case.changed_keys) / 2;
        return index >= start && index < start + case.changed_keys;
    }
    let step = case.records / (case.changed_keys + 1);
    step > 0 && index > 0 && index % step == 0 && index / step <= case.changed_keys
}

fn value(index: u64, version: u8) -> u64 {
    let value = index.wrapping_mul(2_654_435_761);
    if version == 1 {
        !value
    } else {
        value
    }
}

fn make_stores(
    case: &Case,
) -> (
    FingerprintTreeMap<[u8; 16], u64>,
    FingerprintTreeMap<[u8; 16], u64>,
    Vec<Element>,
    Vec<Element>,
) {
    let mut left = FingerprintTreeMap::new();
    let mut right = FingerprintTreeMap::new();
    let mut expected_left = Vec::new();
    let mut expected_right = Vec::new();

    for index in 0..case.records {
        let different = changed(index, case);
        let physical = key(index, &case.layout);
        let left_present = !(different && case.shape == "right-only-scattered");
        let right_present = !(different
            && matches!(
                case.shape.as_str(),
                "deletion-scattered" | "deletion-clustered"
            ));
        let right_version = u8::from(different && case.shape == "updated-scattered");

        if left_present {
            left.insert(physical, value(index, 0));
        }
        if right_present {
            right.insert(physical, value(index, right_version));
        }
        if different {
            if left_present {
                expected_left.push(Element {
                    key: index,
                    version: 0,
                });
            }
            if right_present {
                expected_right.push(Element {
                    key: index,
                    version: right_version,
                });
            }
        }
    }
    (left, right, expected_left, expected_right)
}

fn run_rust_protocol(
    left: &FingerprintTreeMap<[u8; 16], u64>,
    right: &FingerprintTreeMap<[u8; 16], u64>,
    fan_out: usize,
) -> (u64, u64) {
    let mut rng = StdRng::seed_from_u64(42);
    let policy = FixedFanOut::new(FanOut::new(fan_out));
    let mut active = initial_ranges(left);
    let mut right_turn = true;
    let mut messages = 0;
    let mut enumerated = 0;
    while !active.is_empty() {
        assert!(messages < 128, "RBSR failed to terminate");
        let responder = if right_turn { right } else { left };
        let mut children = Vec::new();
        let mut enumeration = Vec::new();
        protocol_round_with_policy(
            responder,
            &policy,
            active,
            &mut children,
            &mut enumeration,
            &mut rng,
        );
        for range in enumeration {
            enumerated += responder.range(range).count() as u64;
        }
        active = children;
        right_turn = !right_turn;
        messages += 1;
    }
    (messages, enumerated)
}

fn run_go(case: &Case) -> Result<Value, Box<dyn Error>> {
    let binary = env::var("GO_RSOS_BIN").map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "set GO_RSOS_BIN to the built Go comparator",
        )
    })?;
    let mut child = Command::new(binary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    serde_json::to_writer(child.stdin.take().expect("piped stdin"), case)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "go-rsos comparator failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }
    let result: Value = serde_json::from_slice(&output.stdout)?;
    if result["schema_version"] != 1
        || result["upstream"] != UPSTREAM
        || result["case"] != serde_json::to_value(case)?
    {
        return Err(io::Error::other("Go response schema, pin or case mismatch").into());
    }
    Ok(result)
}

fn compare(case: Case) -> Result<Value, Box<dyn Error>> {
    let go = run_go(&case)?;
    let build_start = Instant::now();
    let (left, right, want_left, want_right) = make_stores(&case);
    let rust_build_ns = build_start.elapsed().as_nanos();

    let got_left: Vec<Element> = serde_json::from_value(go["left_only"].clone())?;
    let got_right: Vec<Element> = serde_json::from_value(go["right_only"].clone())?;
    if got_left != want_left || got_right != want_right {
        return Err(io::Error::other(format!(
            "incorrect signed difference: Go {:?}/{:?}, oracle {:?}/{:?}",
            got_left, got_right, want_left, want_right
        ))
        .into());
    }

    let started = Instant::now();
    let (messages, enumerated_elements) = run_rust_protocol(&left, &right, case.split_factor);
    let rust_reconcile_ns = started.elapsed().as_nanos();
    Ok(json!({
        "schema_version": 1,
        "logical_task": "difference-discovered",
        "verification": "exact-signed-difference",
        "case": case,
        "go_rsos": go,
        "rust_rbsr": {
            "index": "rsos::FingerprintTreeMap",
            "protocol": "rbsr::FixedFanOut",
            "build_ns": rust_build_ns,
            "reconcile_ns": rust_reconcile_ns,
            "one_way_messages": messages,
            "enumerated_elements": enumerated_elements,
        },
        "caveats": [
            "different fingerprint lifts, not wire interoperable",
            "Rust and Go use different enumeration cutoffs and split boundary rules",
            "Go MemStore is not a networked datastore; Store get counts are not RTTs",
            "Rust measurements include no physical KV I/O",
            "build timings include both peer indexes; process startup is excluded",
            "Go bulk loading includes sorting; Rust always inserts incrementally"
        ]
    }))
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    let case: Case = serde_json::from_slice(&input)?;
    let report = compare(case)?;
    serde_json::to_writer_pretty(io::stdout().lock(), &report)?;
    writeln!(io::stdout().lock())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_follow_the_same_bytes_and_are_distinct() {
        assert_eq!(
            key(1, "ordered"),
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_ne!(key(1, "hashed"), key(2, "hashed"));
    }

    #[test]
    fn go_bridge_matches_independent_rust_oracle() {
        if env::var("GO_RSOS_BIN").is_err() {
            // The normal Rust experimental workspace does not require Go installed.
            return;
        }
        for shape in [
            "deletion-scattered",
            "deletion-clustered",
            "updated-scattered",
            "right-only-scattered",
        ] {
            for layout in ["ordered", "hashed"] {
                for build in ["bulk", "incremental"] {
                    let report = compare(Case {
                        records: 128,
                        changed_keys: 7,
                        shape: shape.to_owned(),
                        layout: layout.to_owned(),
                        build: build.to_owned(),
                        split_factor: 16,
                        resolve_threshold: 16,
                    })
                    .expect("both implementations must discover the exact difference");
                    assert_eq!(report["verification"], "exact-signed-difference");
                }
            }
        }
    }
}
