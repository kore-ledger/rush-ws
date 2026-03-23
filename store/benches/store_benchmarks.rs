//! # Store Benchmarks
//!
//! Criterion benchmarks for persistent actors and the underlying journal store.
//!
//! ## Structure
//!
//! - **Low-level** — exercises `Journal` directly (no actor overhead):
//!   - `journal_put_throughput` — sequential event writes, N = 100 / 1 000 / 10 000
//!   - `journal_put_payload_size` — write throughput as a function of payload size
//!   - `journal_range_query` — read a contiguous range of N pre-written events
//!
//! - **High-level** — exercises `PersistentActor` end-to-end through the actor system:
//!   - `persistent_actor_persist_latency` — round-trip latency for a single persisted event
//!   - `persistent_actor_persist_throughput` — batch event throughput via fire-and-forget
//!   - `persistent_actor_snapshot_latency` — round-trip latency for taking a snapshot
//!   - `persistent_actor_state_recovery` — cold-start recovery time from N pre-written events

use actor::{Actor, ActorContext, ActorPath, ActorRef, Config, Error as ActorError, Event, Message, Response, System};
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use store::{DbManager, FjallDbManager, Journal, PersistentActor};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

fn bench_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("store-bench")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Persistent actor definitions
// ---------------------------------------------------------------------------

/// A simple counter actor that is also a persistent actor.
/// Used as the subject for all high-level benchmarks.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CounterActor {
    /// Unique instance id used to isolate store partitions.
    actor_id: String,
    count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CounterEvent {
    delta: u64,
}

impl Event for CounterEvent {}

enum CounterMessage {
    /// Persist one increment event and wait for the sequence number.
    Increment(u64),
    /// Fire-and-forget: persist one increment event.
    IncrementTell(u64),
    /// Return the current counter value.
    GetCount,
    /// Trigger a snapshot and wait for completion.
    TakeSnapshot,
}

impl Message for CounterMessage {}

enum CounterResponse {
    /// Sequence number of the persisted event (not inspected in benches).
    #[allow(dead_code)]
    Sequence(u64),
    Count(u64),
    Done,
}

impl Response for CounterResponse {}

#[async_trait]
impl Actor for CounterActor {
    type Message = CounterMessage;
    type Event = CounterEvent;
    type Response = CounterResponse;

    async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.init_state(ctx).await
    }

    async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.flush(ctx).await
    }

    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        msg: Self::Message,
    ) -> Result<Self::Response, ActorError> {
        match msg {
            CounterMessage::Increment(n) | CounterMessage::IncrementTell(n) => {
                let sn = self.persist(&CounterEvent { delta: n }, ctx).await?;
                Ok(CounterResponse::Sequence(sn))
            }
            CounterMessage::GetCount => Ok(CounterResponse::Count(self.count)),
            CounterMessage::TakeSnapshot => {
                self.snapshot(ctx).await?;
                Ok(CounterResponse::Done)
            }
        }
    }
}

#[async_trait]
impl PersistentActor for CounterActor {
    fn id(&self) -> String {
        self.actor_id.clone()
    }

    async fn apply_event(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.count += event.delta;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

/// Creates a fresh actor system with a `FjallDbManager` helper and a single `CounterActor`.
async fn setup_persistent_system(
    tmp: &TempDir,
    actor_id: &str,
) -> (System, ActorRef<CounterActor>, CancellationToken) {
    let token = CancellationToken::new();
    let config = Config::default();
    let mut system = System::new(config, token.clone());
    let manager = FjallDbManager::new(tmp.path().to_str().unwrap())
        .expect("Failed to open FjallDbManager");
    system.add_helper("storage", manager).await;
    let actor = CounterActor { actor_id: actor_id.to_owned(), count: 0 };
    let actor_ref = system
        .create_actor(actor, "counter")
        .await
        .expect("Failed to create CounterActor");
    (system, actor_ref, token)
}

/// Persists `n` events via `tell` and then flushes with a single `ask` to ensure all
/// events have been processed before timing ends.
async fn preload_events(actor_ref: &ActorRef<CounterActor>, n: u64) {
    for _ in 0..n {
        actor_ref
            .tell(CounterMessage::IncrementTell(1))
            .await
            .expect("tell failed");
    }
    // Barrier: wait until the actor has processed all preceding messages.
    let _ = actor_ref.ask(CounterMessage::GetCount).await.expect("ask failed");
}

// ---------------------------------------------------------------------------
// Low-level journal benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: Sequential event write throughput of the raw `Journal`.
///
/// Measures how fast bytes can be appended to the journal store, varying the
/// number of events per iteration (100, 1 000, 10 000).
fn bench_journal_put_throughput(c: &mut Criterion) {
    let rt = bench_runtime();
    let payload = vec![0xABu8; 64];

    let mut group = c.benchmark_group("journal_put_throughput");
    for &n in &[100u64, 1_000, 10_000] {
        group.throughput(criterion::Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &size| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    let manager =
                        FjallDbManager::new(tmp.path().to_str().unwrap()).unwrap();
                    let store = manager.create_store("BenchJournal", "bench").unwrap();
                    let mut journal = Journal::new(store);

                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..size {
                            journal.put(&payload).unwrap();
                        }
                    }
                    let elapsed = start.elapsed();
                    drop(tmp);
                    elapsed
                })
            });
        });
    }
    group.finish();
}

/// Benchmark: Write throughput of the raw `Journal` as a function of payload size.
///
/// Keeps event count constant at 1 per Criterion iteration and varies the
/// payload (64 B, 512 B, 4 KiB, 16 KiB) to isolate serialisation cost.
fn bench_journal_put_payload_size(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("journal_put_payload_size");
    for &payload_bytes in &[64usize, 512, 4_096, 16_384] {
        group.throughput(criterion::Throughput::Bytes(payload_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_bytes),
            &payload_bytes,
            |b, &size| {
                let payload = vec![0xABu8; size];
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let tmp = tempfile::tempdir().unwrap();
                        let manager =
                            FjallDbManager::new(tmp.path().to_str().unwrap()).unwrap();
                        let store =
                            manager.create_store("BenchJournal", "bench").unwrap();
                        let mut journal = Journal::new(store);

                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            journal.put(&payload).unwrap();
                        }
                        let elapsed = start.elapsed();
                        drop(tmp);
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Range read throughput of the raw `Journal`.
///
/// Pre-populates the journal with N events and then times repeated full-range
/// scans to measure read throughput (100, 1 000, 5 000 events).
fn bench_journal_range_query(c: &mut Criterion) {
    let rt = bench_runtime();
    let payload = vec![0xABu8; 64];

    let mut group = c.benchmark_group("journal_range_query");
    for &n in &[100u64, 1_000, 5_000] {
        group.throughput(criterion::Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &size| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    let manager =
                        FjallDbManager::new(tmp.path().to_str().unwrap()).unwrap();
                    let store = manager.create_store("BenchJournal", "bench").unwrap();
                    let mut journal = Journal::new(store);

                    // Pre-populate outside timed section.
                    for _ in 0..size {
                        journal.put(&payload).unwrap();
                    }

                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let events = journal.range(1, None);
                        // Consume the iterator so the work is not elided by the optimiser.
                        std::hint::black_box(events.len());
                    }
                    let elapsed = start.elapsed();
                    drop(tmp);
                    elapsed
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// High-level PersistentActor benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: Round-trip latency for persisting a single event.
///
/// Each Criterion iteration sends one `ask(Increment)` and waits for the
/// sequence-number response, capturing the full actor round-trip including
/// serialisation and journal write.
fn bench_persistent_actor_persist_latency(c: &mut Criterion) {
    let rt = bench_runtime();

    c.bench_function("persistent_actor_persist_latency", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let tmp = tempfile::tempdir().unwrap();
                let (_system, actor_ref, token) =
                    setup_persistent_system(&tmp, "bench-latency").await;

                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let resp = actor_ref
                        .ask(CounterMessage::Increment(1))
                        .await
                        .expect("ask failed");
                    std::hint::black_box(resp);
                }
                let elapsed = start.elapsed();
                token.cancel();
                drop(tmp);
                elapsed
            })
        });
    });
}

/// Benchmark: Event persist throughput via fire-and-forget (`tell`).
///
/// Measures how many persisted events per second the actor pipeline can sustain
/// for batch sizes of 100, 1 000 and 10 000.
fn bench_persistent_actor_persist_throughput(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("persistent_actor_persist_throughput");
    for &batch in &[100u64, 1_000, 10_000] {
        group.throughput(criterion::Throughput::Elements(batch));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &size| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    let (_system, actor_ref, token) =
                        setup_persistent_system(&tmp, "bench-throughput").await;

                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..size {
                            actor_ref
                                .tell(CounterMessage::IncrementTell(1))
                                .await
                                .expect("tell failed");
                        }
                        // Barrier: ensure all tell messages are processed before next iter.
                        let _ = actor_ref
                            .ask(CounterMessage::GetCount)
                            .await
                            .expect("barrier ask failed");
                    }
                    let elapsed = start.elapsed();
                    token.cancel();
                    drop(tmp);
                    elapsed
                })
            });
        });
    }
    group.finish();
}

/// Benchmark: Round-trip latency for taking a snapshot.
///
/// Pre-populates N events before timing and then measures how long each
/// snapshot ask takes (serialises the full actor state + writes to the
/// snapshotter store).
fn bench_persistent_actor_snapshot_latency(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("persistent_actor_snapshot_latency");
    for &n_events in &[10u64, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("after_events", n_events),
            &n_events,
            |b, &n| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let tmp = tempfile::tempdir().unwrap();
                        let (_system, actor_ref, token) =
                            setup_persistent_system(&tmp, "bench-snapshot").await;

                        // Pre-populate outside timed section.
                        preload_events(&actor_ref, n).await;

                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let resp = actor_ref
                                .ask(CounterMessage::TakeSnapshot)
                                .await
                                .expect("snapshot ask failed");
                            std::hint::black_box(resp);
                        }
                        let elapsed = start.elapsed();
                        token.cancel();
                        drop(tmp);
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Cold-start state recovery time from N pre-written events.
///
/// Measures only the `create_actor` call on a fresh actor instance whose
/// `pre_start` calls `init_state`, which needs to read back journal events
/// (and optionally a snapshot). The preparation phase (writing events, taking
/// a snapshot, stopping the actor) is excluded from the timed section.
fn bench_persistent_actor_state_recovery(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("persistent_actor_state_recovery");
    // Larger sample time because each iteration creates and destroys an actor.
    group.sample_size(20);

    for &n_events in &[10u64, 100, 1_000] {
        group.throughput(criterion::Throughput::Elements(n_events));
        group.bench_with_input(
            BenchmarkId::from_parameter(n_events),
            &n_events,
            |b, &n| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = std::time::Duration::ZERO;

                        for _ in 0..iters {
                            // ── Preparation (not timed) ───────────────────────────────
                            let tmp = tempfile::tempdir().unwrap();
                            let token = CancellationToken::new();
                            let config = Config::default();
                            let mut system = System::new(config, token.clone());
                            let manager =
                                FjallDbManager::new(tmp.path().to_str().unwrap()).unwrap();
                            system.add_helper("storage", manager).await;

                            let actor = CounterActor {
                                actor_id: "bench-recovery".to_owned(),
                                count: 0,
                            };
                            let actor_ref = system
                                .create_actor(actor, "counter")
                                .await
                                .expect("create failed");

                            // Persist N events and take a snapshot.
                            preload_events(&actor_ref, n).await;
                            let _ = actor_ref
                                .ask(CounterMessage::TakeSnapshot)
                                .await
                                .expect("snapshot failed");

                            // Stop the actor so pre_stop flushes to disk.
                            system.stop_actor("counter").await.expect("stop failed");
                            // Give the actor time to finish its shutdown sequence.
                            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

                            // ── Timed section: cold-start recovery ───────────────────
                            let fresh = CounterActor {
                                actor_id: "bench-recovery".to_owned(),
                                count: 0,
                            };
                            let t0 = std::time::Instant::now();
                            let recovered_ref = system
                                .create_actor(fresh, "counter")
                                .await
                                .expect("recovery create failed");
                            total += t0.elapsed();

                            // Verify state was restored (not counted in timing).
                            let resp = recovered_ref
                                .ask(CounterMessage::GetCount)
                                .await
                                .expect("verify ask failed");
                            if let CounterResponse::Count(c) = resp {
                                assert_eq!(c, n, "State recovery returned wrong count");
                            }

                            token.cancel();
                            drop(tmp);
                        }

                        total
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Persist N events followed by a snapshot in a single burst.
///
/// Simulates a common write pattern where an actor persists a batch of events
/// and then checkpoints its state. Batch sizes: 10, 100, 500.
fn bench_persist_and_snapshot_cycle(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("persist_and_snapshot_cycle");
    group.sample_size(30);

    for &batch in &[10u64, 100, 500] {
        group.throughput(criterion::Throughput::Elements(batch));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &size| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    let (_system, actor_ref, token) =
                        setup_persistent_system(&tmp, "bench-cycle").await;

                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..size {
                            actor_ref
                                .tell(CounterMessage::IncrementTell(1))
                                .await
                                .expect("tell failed");
                        }
                        // Ensure all events are persisted before snapshotting.
                        let _ = actor_ref
                            .ask(CounterMessage::TakeSnapshot)
                            .await
                            .expect("snapshot failed");
                    }
                    let elapsed = start.elapsed();
                    token.cancel();
                    drop(tmp);
                    elapsed
                })
            });
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups & main
// ---------------------------------------------------------------------------

criterion_group!(
    low_level,
    bench_journal_put_throughput,
    bench_journal_put_payload_size,
    bench_journal_range_query,
);

criterion_group!(
    high_level,
    bench_persistent_actor_persist_latency,
    bench_persistent_actor_persist_throughput,
    bench_persistent_actor_snapshot_latency,
    bench_persistent_actor_state_recovery,
    bench_persist_and_snapshot_cycle,
);

criterion_main!(low_level, high_level);
