// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;

use parking_lot::Mutex;
use rbsr::RangeAggregate;
use reconcile::{Entry, State, Timestamp};
use serde::{Deserialize, Serialize};

pub(super) const DATA: u8 = 0;
pub(super) const ACK: u8 = 1;
pub(super) const BITMAP_ACK: u8 = 2;
pub(super) const HEADER_LEN: usize = 23;
pub(super) const MINIMAL_HEADER_LEN: usize = 15;
pub(super) const BITMAP_ACK_BASE_LEN: usize = 10;
pub(super) const BITMAP_ACK_ENTRY_LEN: usize = 20;
pub(super) const MAX_FRAMES_PER_FLIGHT: usize = 128;
pub(super) const MAX_PENDING_FRAMES: usize = 256;
pub(super) const TRACKED_RECEIVER_FLIGHTS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Mode {
    Control,
    Selective,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct WireKey {
    pub(super) epoch: u64,
    pub(super) flight: u32,
    pub(super) slot: u16,
    pub(super) semantic: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum DataWire {
    Full,
    Minimal,
}

impl DataWire {
    pub(super) fn header_len(self) -> usize {
        match self {
            Self::Full => HEADER_LEN,
            Self::Minimal => MINIMAL_HEADER_LEN,
        }
    }

    pub(super) fn encode(self, key: WireKey, payload: &[u8]) -> Vec<u8> {
        match self {
            Self::Full => encode_envelope(DATA, key, payload),
            Self::Minimal => encode_minimal_envelope(DATA, key, payload),
        }
    }

    pub(super) fn decode(self, bytes: &[u8]) -> io::Result<(u8, WireKey, &[u8])> {
        match self {
            Self::Full => decode_envelope(bytes),
            Self::Minimal => decode_minimal_envelope(bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct FaultKey {
    pub(super) direction: u8,
    pub(super) kind: u8,
    pub(super) semantic: u64,
    pub(super) attempt: u16,
}

#[derive(Clone, Copy)]
pub(super) struct FaultProfile {
    pub(super) scenario: u64,
    pub(super) data_loss_bp: u16,
    pub(super) ack_loss_bp: u16,
    pub(super) duplicate_bp: u16,
    pub(super) reorder_bp: u16,
    pub(super) stale_bp: u16,
}

impl FaultProfile {
    pub(super) const fn clean(scenario: u64) -> Self {
        Self {
            scenario,
            data_loss_bp: 0,
            ack_loss_bp: 0,
            duplicate_bp: 0,
            reorder_bp: 0,
            stale_bp: 0,
        }
    }

    pub(super) const fn data_loss(scenario: u64, data_loss_bp: u16) -> Self {
        Self {
            scenario,
            data_loss_bp,
            ack_loss_bp: 0,
            duplicate_bp: 0,
            reorder_bp: 0,
            stale_bp: 0,
        }
    }

    pub(super) const fn mixed(scenario: u64) -> Self {
        Self {
            scenario,
            data_loss_bp: 100,
            ack_loss_bp: 500,
            duplicate_bp: 200,
            reorder_bp: 200,
            stale_bp: 100,
        }
    }

    pub(super) fn with_scenario(self, scenario: u64) -> Self {
        Self { scenario, ..self }
    }

    pub(super) fn decision(self, key: FaultKey) -> FaultDecision {
        let loss = if key.kind == DATA {
            self.data_loss_bp
        } else {
            self.ack_loss_bp
        };
        FaultDecision {
            drop: self.score(key, 0x6c6f_7373) < loss,
            duplicate: self.score(key, 0x6475_7065) < self.duplicate_bp,
            reorder: self.score(key, 0x7265_6f72) < self.reorder_bp,
            stale: key.kind == DATA && self.score(key, 0x7374_616c) < self.stale_bp,
        }
    }

    fn score(self, key: FaultKey, salt: u64) -> u16 {
        let word = self.scenario
            ^ key.semantic.rotate_left(17)
            ^ u64::from(key.attempt).rotate_left(41)
            ^ (u64::from(key.direction) << 56)
            ^ (u64::from(key.kind) << 48)
            ^ salt;
        (mix64(word) % 10_000) as u16
    }
}

#[derive(Clone, Copy)]
pub(super) struct FaultDecision {
    pub(super) drop: bool,
    pub(super) duplicate: bool,
    pub(super) reorder: bool,
    pub(super) stale: bool,
}

#[derive(Clone, Default)]
pub(super) struct Metrics {
    pub(super) useful_bytes: usize,
    pub(super) first_data_bytes: usize,
    pub(super) retry_bytes: usize,
    pub(super) control_bytes: usize,
    pub(super) first_data_frames: usize,
    pub(super) retry_frames: usize,
    pub(super) control_frames: usize,
    pub(super) fault_drops: usize,
    pub(super) data_fault_drops: usize,
    pub(super) control_fault_drops: usize,
    pub(super) fault_duplicates: usize,
    pub(super) fault_reorders: usize,
    pub(super) stale_injected: usize,
    pub(super) stale_rejected: usize,
    pub(super) retry_exhausted: usize,
    pub(super) max_pending_bytes: usize,
    pub(super) max_pending_frames: usize,
    pub(super) max_seen_ids: usize,
    pub(super) max_receiver_flights: usize,
}

impl Metrics {
    pub(super) fn add(&mut self, other: &Self) {
        self.useful_bytes += other.useful_bytes;
        self.first_data_bytes += other.first_data_bytes;
        self.retry_bytes += other.retry_bytes;
        self.control_bytes += other.control_bytes;
        self.first_data_frames += other.first_data_frames;
        self.retry_frames += other.retry_frames;
        self.control_frames += other.control_frames;
        self.fault_drops += other.fault_drops;
        self.data_fault_drops += other.data_fault_drops;
        self.control_fault_drops += other.control_fault_drops;
        self.fault_duplicates += other.fault_duplicates;
        self.fault_reorders += other.fault_reorders;
        self.stale_injected += other.stale_injected;
        self.stale_rejected += other.stale_rejected;
        self.retry_exhausted += other.retry_exhausted;
        self.max_pending_bytes = self.max_pending_bytes.max(other.max_pending_bytes);
        self.max_pending_frames = self.max_pending_frames.max(other.max_pending_frames);
        self.max_seen_ids = self.max_seen_ids.max(other.max_seen_ids);
        self.max_receiver_flights = self.max_receiver_flights.max(other.max_receiver_flights);
    }

    pub(super) fn total_frames(&self) -> usize {
        self.first_data_frames + self.retry_frames + self.control_frames
    }
}

#[derive(Default)]
pub(super) struct Trace {
    pub(super) drops: HashSet<FaultKey>,
}

#[derive(Default)]
pub(super) struct SeenState {
    flights: VecDeque<(u32, HashSet<u16>)>,
}

impl SeenState {
    pub(super) fn record(&mut self, flight: u32, slot: u16) -> (bool, usize) {
        if let Some((_, ids)) = self
            .flights
            .iter_mut()
            .find(|(candidate, _)| *candidate == flight)
        {
            let duplicate = !ids.insert(slot);
            return (duplicate, self.total());
        }
        if self.flights.len() == TRACKED_RECEIVER_FLIGHTS {
            self.flights.pop_front();
        }
        let mut ids = HashSet::new();
        ids.insert(slot);
        self.flights.push_back((flight, ids));
        (false, self.total())
    }

    fn total(&self) -> usize {
        self.flights.iter().map(|(_, ids)| ids.len()).sum()
    }
}

#[derive(Deserialize, Serialize)]
#[allow(dead_code)]
enum WireMessage {
    EntryFingerprint(RangeAggregate<u64>),
    EntryUpdate((u64, Entry<Timestamp, u64>)),
    TombstoneAck((u64, u64)),
    StateFingerprint(RangeAggregate<u64>),
    StateUpdate((u64, State<u64>)),
    ConvergenceAck,
    Reserved6(Vec<u8>),
}

pub(super) fn pack_messages(buf: &[u8], budget: usize) -> io::Result<Vec<Vec<u8>>> {
    let Some((&version, payload)) = buf.split_first() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty wire frame",
        ));
    };
    if budget <= 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "payload budget cannot hold a message",
        ));
    }
    let messages: Vec<WireMessage> = gossip::bincode::decode_stream(payload, 65_507)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut reencoded = Vec::new();
    let mut frame = vec![version];
    let mut frames = Vec::new();
    for message in messages {
        let start = reencoded.len();
        gossip::bincode::encode(&message, &mut reencoded)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let item = &reencoded[start..];
        if item.len() + 1 > budget {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "atomic RBSR message exceeds payload budget",
            ));
        }
        if frame.len() + item.len() > budget {
            frames.push(frame);
            frame = vec![version];
        }
        frame.extend_from_slice(item);
    }
    if reencoded != payload {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "wire mirror changed bytes",
        ));
    }
    if frame.len() > 1 {
        frames.push(frame);
    }
    Ok(frames)
}

pub(super) fn encode_envelope(kind: u8, key: WireKey, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(kind);
    out.extend_from_slice(&key.epoch.to_le_bytes());
    out.extend_from_slice(&key.flight.to_le_bytes());
    out.extend_from_slice(&key.slot.to_le_bytes());
    out.extend_from_slice(&key.semantic.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

pub(super) fn decode_envelope(bytes: &[u8]) -> io::Result<(u8, WireKey, &[u8])> {
    if bytes.len() < HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "research envelope is truncated",
        ));
    }
    let epoch = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
    let flight = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
    let slot = u16::from_le_bytes(bytes[13..15].try_into().unwrap());
    let semantic = u64::from_le_bytes(bytes[15..23].try_into().unwrap());
    Ok((
        bytes[0],
        WireKey {
            epoch,
            flight,
            slot,
            semantic,
        },
        &bytes[HEADER_LEN..],
    ))
}

pub(super) fn encode_minimal_envelope(kind: u8, key: WireKey, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(MINIMAL_HEADER_LEN + payload.len());
    out.push(kind);
    out.extend_from_slice(&key.epoch.to_le_bytes());
    out.extend_from_slice(&key.flight.to_le_bytes());
    out.extend_from_slice(&key.slot.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

pub(super) fn decode_minimal_envelope(bytes: &[u8]) -> io::Result<(u8, WireKey, &[u8])> {
    if bytes.len() < MINIMAL_HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "minimal reliability envelope is truncated",
        ));
    }
    let epoch = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
    let flight = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
    let slot = u16::from_le_bytes(bytes[13..15].try_into().unwrap());
    let payload = &bytes[MINIMAL_HEADER_LEN..];
    Ok((
        bytes[0],
        WireKey {
            epoch,
            flight,
            slot,
            semantic: stable_id(payload),
        },
        payload,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BitmapAckEntry {
    pub(super) flight: u32,
    pub(super) bits: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BitmapAck {
    pub(super) epoch: u64,
    pub(super) entries: Vec<BitmapAckEntry>,
}

pub(super) fn encode_bitmap_ack(ack: &BitmapAck) -> io::Result<Vec<u8>> {
    let count = u8::try_from(ack.entries.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many bitmap ACK entries"))?;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty bitmap ACK",
        ));
    }
    let mut out =
        Vec::with_capacity(BITMAP_ACK_BASE_LEN + ack.entries.len() * BITMAP_ACK_ENTRY_LEN);
    out.push(BITMAP_ACK);
    out.extend_from_slice(&ack.epoch.to_le_bytes());
    out.push(count);
    for entry in &ack.entries {
        out.extend_from_slice(&entry.flight.to_le_bytes());
        out.extend_from_slice(&entry.bits.to_le_bytes());
    }
    Ok(out)
}

pub(super) fn decode_bitmap_ack(bytes: &[u8]) -> io::Result<BitmapAck> {
    if bytes.len() < BITMAP_ACK_BASE_LEN || bytes.first() != Some(&BITMAP_ACK) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid bitmap ACK",
        ));
    }
    let count = usize::from(bytes[9]);
    let expected = BITMAP_ACK_BASE_LEN + count * BITMAP_ACK_ENTRY_LEN;
    if count == 0 || bytes.len() != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid bitmap ACK length",
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let start = BITMAP_ACK_BASE_LEN + index * BITMAP_ACK_ENTRY_LEN;
        entries.push(BitmapAckEntry {
            flight: u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap()),
            bits: u128::from_le_bytes(bytes[start + 4..start + 20].try_into().unwrap()),
        });
    }
    Ok(BitmapAck {
        epoch: u64::from_le_bytes(bytes[1..9].try_into().unwrap()),
        entries,
    })
}

pub(super) fn stable_id(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(super) type AttemptMap = Mutex<HashMap<(u8, u64), u16>>;
