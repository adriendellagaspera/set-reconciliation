// Copyright 2026 Developers of the reconcile-rs project.
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gossip::netem::{Link, Netem, NetemTransport, Probability, Rtt, Seed};
use parking_lot::Mutex;
use reconcile::persistence::{InMemoryPersistence, PersistedState, Persistence};
use reconcile::replicated_map::Config;
use reconcile::{InMemoryNetwork, InMemoryTransport, ReplicatedMap, Transport};
use tokio_util::sync::CancellationToken;

use super::harness::{Fixture, Sample};
use super::wire::{
    decode_bitmap_ack, encode_bitmap_ack, pack_messages, stable_id, AttemptMap, BitmapAck,
    BitmapAckEntry, DataWire, FaultKey, FaultProfile, Metrics, Trace, WireKey, ACK, BITMAP_ACK,
    BITMAP_ACK_BASE_LEN, BITMAP_ACK_ENTRY_LEN, DATA, MAX_FRAMES_PER_FLIGHT, MAX_PENDING_FRAMES,
};

const MAX_RETRIES: usize = 3;
const RETRY_AFTER: Duration = Duration::from_millis(80);
const ACK_DELAY: Duration = Duration::from_millis(2);
const REORDER_EXTRA: Duration = Duration::from_millis(25);
const DUPLICATE_EXTRA: Duration = Duration::from_millis(2);
const MAX_ACK_FLIGHTS_PER_FRAME: usize = 16;
pub(super) const MAX_TRACKED_BITMAP_FLIGHTS: usize = 1_024;

#[derive(Clone)]
struct PendingFrame {
    key: WireKey,
    bytes: Arc<Vec<u8>>,
    dst: SocketAddr,
}

#[derive(Default)]
struct AckLedger {
    flights: HashMap<u32, u128>,
    dirty: BTreeSet<u32>,
    scheduled: bool,
}

impl AckLedger {
    fn record(&mut self, flight: u32, slot: u16) -> io::Result<(bool, usize, usize, bool)> {
        if usize::from(slot) >= MAX_FRAMES_PER_FLIGHT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bitmap ACK slot exceeds flight bound",
            ));
        }
        if !self.flights.contains_key(&flight) {
            if self.flights.len() == MAX_TRACKED_BITMAP_FLIGHTS {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "bitmap ACK flight ledger exhausted",
                ));
            }
            self.flights.insert(flight, 0);
        }
        let bits = self.flights.get_mut(&flight).unwrap();
        let mask = 1u128 << slot;
        let duplicate = *bits & mask != 0;
        *bits |= mask;
        self.dirty.insert(flight);

        let seen_ids = self
            .flights
            .values()
            .map(|bits| bits.count_ones() as usize)
            .sum();
        let tracked_flights = self.flights.len();
        let schedule = if self.scheduled {
            false
        } else {
            self.scheduled = true;
            true
        };
        Ok((duplicate, seen_ids, tracked_flights, schedule))
    }

    fn take_batches(&mut self, epoch: u64, batch_entries: usize) -> Vec<BitmapAck> {
        self.scheduled = false;
        let dirty = std::mem::take(&mut self.dirty);
        let entries: Vec<_> = dirty
            .into_iter()
            .filter_map(|flight| {
                self.flights
                    .get(&flight)
                    .copied()
                    .map(|bits| BitmapAckEntry { flight, bits })
            })
            .collect();
        entries
            .chunks(batch_entries)
            .map(|chunk| BitmapAck {
                epoch,
                entries: chunk.to_vec(),
            })
            .collect()
    }
}

struct BitmapShared {
    raw: Arc<InMemoryTransport>,
    inner: Arc<NetemTransport<InMemoryTransport>>,
    profile: FaultProfile,
    direction: u8,
    attempts: AttemptMap,
    pending: Mutex<HashMap<WireKey, PendingFrame>>,
    ack_ledger: Mutex<AckLedger>,
    ack_batch_entries: usize,
    frame_budget: usize,
    data_header_len: usize,
    metrics: Arc<Mutex<Metrics>>,
    trace: Arc<Mutex<Trace>>,
}

impl BitmapShared {
    fn next_fault_key(&self, kind: u8, semantic: u64) -> FaultKey {
        let mut attempts = self.attempts.lock();
        let attempt = attempts.entry((kind, semantic)).or_default();
        let key = FaultKey {
            direction: self.direction,
            kind,
            semantic,
            attempt: *attempt,
        };
        *attempt = attempt.saturating_add(1);
        key
    }

    async fn emit_data(self: &Arc<Self>, frame: PendingFrame, retry: bool) -> io::Result<()> {
        {
            let mut metrics = self.metrics.lock();
            if retry {
                metrics.retry_frames += 1;
                metrics.retry_bytes += frame.bytes.len();
            } else {
                metrics.useful_bytes += frame.bytes.len() - self.data_header_len;
                metrics.first_data_frames += 1;
                metrics.first_data_bytes += frame.bytes.len();
            }
        }
        self.emit_faulted(DATA, frame.key.semantic, frame.bytes, frame.dst)
            .await
    }

    async fn emit_ack(self: &Arc<Self>, bytes: Arc<Vec<u8>>, dst: SocketAddr) -> io::Result<()> {
        {
            let mut metrics = self.metrics.lock();
            metrics.control_frames += 1;
            metrics.control_bytes += bytes.len();
        }
        let semantic = stable_id(bytes.as_slice());
        self.emit_faulted(ACK, semantic, bytes, dst).await
    }

    async fn emit_faulted(
        self: &Arc<Self>,
        kind: u8,
        semantic: u64,
        bytes: Arc<Vec<u8>>,
        dst: SocketAddr,
    ) -> io::Result<()> {
        let fault_key = self.next_fault_key(kind, semantic);
        let decision = self.profile.decision(fault_key);
        if decision.drop {
            let mut metrics = self.metrics.lock();
            metrics.fault_drops += 1;
            if kind == DATA {
                metrics.data_fault_drops += 1;
            } else {
                metrics.control_fault_drops += 1;
            }
            drop(metrics);
            self.trace.lock().drops.insert(fault_key);
            return Ok(());
        }

        if decision.stale {
            self.metrics.lock().stale_injected += 1;
            let mut stale = bytes.as_ref().clone();
            let stale_epoch =
                u64::from_le_bytes(stale[1..9].try_into().unwrap()) ^ 0xa5a5_a5a5_a5a5_a5a5;
            stale[1..9].copy_from_slice(&stale_epoch.to_le_bytes());
            self.raw.send_to(&stale, &dst).await?;
        }

        if decision.reorder {
            self.metrics.lock().fault_reorders += 1;
            let inner = Arc::clone(&self.inner);
            let delayed = Arc::clone(&bytes);
            tokio::spawn(async move {
                tokio::time::sleep(REORDER_EXTRA).await;
                let _ = inner.send_to(delayed.as_slice(), &dst).await;
            });
        } else {
            self.inner.send_to(bytes.as_slice(), &dst).await?;
        }

        if decision.duplicate {
            self.metrics.lock().fault_duplicates += 1;
            let inner = Arc::clone(&self.inner);
            let duplicate = Arc::clone(&bytes);
            tokio::spawn(async move {
                tokio::time::sleep(DUPLICATE_EXTRA).await;
                let _ = inner.send_to(duplicate.as_slice(), &dst).await;
            });
        }
        Ok(())
    }

    fn insert_pending(&self, frames: &[PendingFrame]) -> io::Result<()> {
        let mut pending = self.pending.lock();
        if pending.len() + frames.len() > MAX_PENDING_FRAMES {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "bitmap retry state bound exceeded",
            ));
        }
        for frame in frames {
            pending.insert(frame.key, frame.clone());
        }
        let pending_bytes = pending.values().map(|frame| frame.bytes.len()).sum();
        let mut metrics = self.metrics.lock();
        metrics.max_pending_frames = metrics.max_pending_frames.max(pending.len());
        metrics.max_pending_bytes = metrics.max_pending_bytes.max(pending_bytes);
        Ok(())
    }

    fn apply_ack(&self, ack: BitmapAck) {
        let mut pending = self.pending.lock();
        for entry in ack.entries {
            pending.retain(|key, _| {
                key.flight != entry.flight || entry.bits & (1u128 << usize::from(key.slot)) == 0
            });
        }
    }

    fn spawn_ack(self: &Arc<Self>, epoch: u64, dst: SocketAddr) {
        let shared = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(ACK_DELAY).await;
            let batches = shared
                .ack_ledger
                .lock()
                .take_batches(epoch, shared.ack_batch_entries);
            for ack in batches {
                let Ok(bytes) = encode_bitmap_ack(&ack) else {
                    continue;
                };
                debug_assert!(bytes.len() <= shared.frame_budget);
                let _ = shared.emit_ack(Arc::new(bytes), dst).await;
            }
        });
    }

    fn spawn_retries(self: &Arc<Self>, flight: u32) {
        let shared = Arc::clone(self);
        tokio::spawn(async move {
            for _ in 0..MAX_RETRIES {
                tokio::time::sleep(RETRY_AFTER).await;
                let frames: Vec<_> = shared
                    .pending
                    .lock()
                    .values()
                    .filter(|frame| frame.key.flight == flight)
                    .cloned()
                    .collect();
                if frames.is_empty() {
                    return;
                }
                for frame in frames {
                    let _ = shared.emit_data(frame, true).await;
                }
            }
            tokio::time::sleep(RETRY_AFTER).await;
            let exhausted = {
                let mut pending = shared.pending.lock();
                let before = pending.len();
                pending.retain(|key, _| key.flight != flight);
                before - pending.len()
            };
            shared.metrics.lock().retry_exhausted += exhausted;
        });
    }
}

struct BitmapTransport {
    shared: Arc<BitmapShared>,
    budget: usize,
    data_wire: DataWire,
    epoch: u64,
    peer_epoch: u64,
    next_flight: AtomicU32,
}

#[async_trait::async_trait]
impl Transport for BitmapTransport {
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let mut raw = vec![0; 65_507];
        loop {
            let (n, src) = self.shared.inner.recv_from(&mut raw).await?;
            match raw.first().copied() {
                Some(BITMAP_ACK) => {
                    let ack = decode_bitmap_ack(&raw[..n])?;
                    if ack.epoch == self.epoch {
                        self.shared.apply_ack(ack);
                    }
                }
                Some(DATA) => {
                    let (_, key, payload) = self.data_wire.decode(&raw[..n])?;
                    if key.epoch != self.peer_epoch {
                        self.shared.metrics.lock().stale_rejected += 1;
                        continue;
                    }
                    let (duplicate, seen, flights, schedule_ack) =
                        self.shared.ack_ledger.lock().record(key.flight, key.slot)?;
                    {
                        let mut metrics = self.shared.metrics.lock();
                        metrics.max_seen_ids = metrics.max_seen_ids.max(seen);
                        metrics.max_receiver_flights = metrics.max_receiver_flights.max(flights);
                    }
                    if schedule_ack {
                        self.shared.spawn_ack(self.peer_epoch, src);
                    }
                    if duplicate {
                        continue;
                    }
                    if payload.len() > buf.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "decoded bitmap frame exceeds receive buffer",
                        ));
                    }
                    buf[..payload.len()].copy_from_slice(payload);
                    return Ok((payload.len(), src));
                }
                Some(ACK) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "per-frame ACK reached bitmap transport",
                    ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown bitmap research frame",
                    ));
                }
            }
        }
    }

    async fn send_to(&self, buf: &[u8], dst: &SocketAddr) -> io::Result<usize> {
        let payload_budget = self
            .budget
            .checked_sub(self.data_wire.header_len())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "frame budget below identity header size",
                )
            })?;
        let frames = pack_messages(buf, payload_budget)?;
        if frames.len() > MAX_FRAMES_PER_FLIGHT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "logical datagram exceeds bounded flight size",
            ));
        }
        let flight = self.next_flight.fetch_add(1, Ordering::Relaxed);
        let encoded: Vec<_> = frames
            .into_iter()
            .enumerate()
            .map(|(slot, payload)| {
                let key = WireKey {
                    epoch: self.epoch,
                    flight,
                    slot: slot as u16,
                    semantic: stable_id(&payload),
                };
                PendingFrame {
                    key,
                    bytes: Arc::new(self.data_wire.encode(key, &payload)),
                    dst: *dst,
                }
            })
            .collect();
        debug_assert!(encoded.iter().all(|frame| frame.bytes.len() <= self.budget));

        self.shared.insert_pending(&encoded)?;
        for frame in &encoded {
            self.shared.emit_data(frame.clone(), false).await?;
        }
        self.shared.spawn_retries(flight);
        Ok(buf.len())
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared.inner.local_addr()
    }
}

struct ReplicaSpec<'a> {
    ip: IpAddr,
    epoch: u64,
    peer_epoch: u64,
    direction: u8,
    entries: &'a [(u64, reconcile::Entry<reconcile::Timestamp, u64>)],
}

type ReplicaProbe = (
    ReplicatedMap<u64, u64>,
    Arc<Mutex<Metrics>>,
    Arc<Mutex<Trace>>,
);

fn replica(
    network: &InMemoryNetwork,
    budget: usize,
    profile: FaultProfile,
    data_wire: DataWire,
    spec: ReplicaSpec<'_>,
) -> ReplicaProbe {
    let port = 24_100;
    let link = Link::at(Rtt::from_millis(10.0)).with_loss(Probability::percent(0.0));
    let raw = Arc::new(network.bind(SocketAddr::new(spec.ip, port)));
    let inner = Arc::new(NetemTransport::new(
        Arc::clone(&raw),
        Netem::uniform(
            link,
            Seed::new(0x6269_746d_6170_0000 + u64::from(spec.direction)),
        ),
    ));
    let metrics = Arc::new(Mutex::new(Metrics::default()));
    let trace = Arc::new(Mutex::new(Trace::default()));
    let ack_batch_entries = ((budget.saturating_sub(BITMAP_ACK_BASE_LEN)) / BITMAP_ACK_ENTRY_LEN)
        .clamp(1, MAX_ACK_FLIGHTS_PER_FRAME);
    let shared = Arc::new(BitmapShared {
        raw,
        inner,
        profile,
        direction: spec.direction,
        attempts: Mutex::new(HashMap::new()),
        pending: Mutex::new(HashMap::new()),
        ack_ledger: Mutex::new(AckLedger::default()),
        ack_batch_entries,
        frame_budget: budget,
        data_header_len: data_wire.header_len(),
        metrics: Arc::clone(&metrics),
        trace: Arc::clone(&trace),
    });
    let transport = BitmapTransport {
        shared,
        budget,
        data_wire,
        epoch: spec.epoch,
        peer_epoch: spec.peer_epoch,
        next_flight: AtomicU32::new(0),
    };
    let persistence = Arc::new(InMemoryPersistence::<u64, u64>::new());
    persistence
        .save(&PersistedState::from(spec.entries.to_vec()))
        .unwrap();
    let config = Config::default()
        .with_port(port)
        .with_listen_addr(spec.ip)
        .with_net("127.0.0.0/8".parse().unwrap())
        .unwrap()
        .with_insecure_no_key();
    (
        ReplicatedMap::new_with_transport(config, Arc::new(transport))
            .unwrap()
            .with_persistence(persistence)
            .unwrap(),
        metrics,
        trace,
    )
}

pub(super) async fn run_bitmap_sample(
    fixture: &Fixture,
    budget: usize,
    profile: FaultProfile,
) -> Sample {
    run_bitmap_sample_with_wire(fixture, budget, profile, DataWire::Full).await
}

pub(super) async fn run_minimal_bitmap_sample(
    fixture: &Fixture,
    budget: usize,
    profile: FaultProfile,
) -> Sample {
    run_bitmap_sample_with_wire(fixture, budget, profile, DataWire::Minimal).await
}

async fn run_bitmap_sample_with_wire(
    fixture: &Fixture,
    budget: usize,
    profile: FaultProfile,
    data_wire: DataWire,
) -> Sample {
    let network = InMemoryNetwork::new();
    let left_ip: IpAddr = "127.9.10.1".parse().unwrap();
    let right_ip: IpAddr = "127.9.10.2".parse().unwrap();
    let left_epoch = 0x4c45_4654_0000_0001;
    let right_epoch = 0x5249_4748_0000_0001;
    let (left, left_metrics, left_trace) = replica(
        &network,
        budget,
        profile,
        data_wire,
        ReplicaSpec {
            ip: left_ip,
            epoch: left_epoch,
            peer_epoch: right_epoch,
            direction: 0,
            entries: &fixture.full,
        },
    );
    let (right, right_metrics, right_trace) = replica(
        &network,
        budget,
        profile,
        data_wire,
        ReplicaSpec {
            ip: right_ip,
            epoch: right_epoch,
            peer_epoch: left_epoch,
            direction: 1,
            entries: &fixture.shared,
        },
    );
    let target = left.fingerprint(..);
    assert_ne!(right.fingerprint(..), target);
    right.seed_peer(left_ip);

    let start = Instant::now();
    let tasks = [
        tokio::spawn(left.clone().run(CancellationToken::new())),
        tokio::spawn(right.clone().run(CancellationToken::new())),
    ];
    let converged = tokio::time::timeout(Duration::from_secs(12), async {
        while right.fingerprint(..) != target {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    let elapsed = start.elapsed();
    for task in tasks {
        task.abort();
    }
    if converged {
        for key in 0..100_000u64 {
            assert_eq!(
                right.get_cloned(&key),
                Some(key.wrapping_mul(2_654_435_761)),
                "bitmap wire={data_wire:?} scenario={:#x} key={key}",
                profile.scenario
            );
        }
    }

    let mut metrics = left_metrics.lock().clone();
    metrics.add(&right_metrics.lock());
    let mut drops = left_trace.lock().drops.clone();
    drops.extend(right_trace.lock().drops.iter().copied());
    Sample {
        elapsed,
        converged,
        metrics,
        drops,
    }
}
