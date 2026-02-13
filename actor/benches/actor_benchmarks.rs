//! # Actor System Benchmarks
//!
//! Load tests for the actor system using Criterion.
//! Measures throughput and latency for actor creation, messaging (tell/ask),
//! event subscription, concurrent access, and system teardown.

use actor::{
    Actor, ActorContext, ActorPath, ActorRef, Config, Error, Event, Message, Response, System,
};
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

/// Creates a single-threaded Tokio runtime suitable for benchmarking.
fn bench_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("actor-bench")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap()
}

// ---------------------------------------------------------------------------
// Test actor definitions
// ---------------------------------------------------------------------------

/// A minimal no-op actor used to benchmark raw creation overhead.
struct NoopActor;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct NoopEvent;
impl Event for NoopEvent {}

struct NoopMessage;
impl Message for NoopMessage {}

struct NoopResponse;
impl Response for NoopResponse {}

#[async_trait]
impl Actor for NoopActor {
    type Message = NoopMessage;
    type Event = NoopEvent;
    type Response = NoopResponse;

    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        _msg: Self::Message,
    ) -> Result<Self::Response, Error> {
        Ok(NoopResponse)
    }
}

/// A counter actor that increments an internal counter on every message.
/// Used to benchmark stateful message processing.
struct CounterActor {
    count: u64,
}

impl CounterActor {
    fn new() -> Self {
        Self { count: 0 }
    }
}

#[derive(Debug, Clone)]
enum CounterMessage {
    Increment,
    GetCount,
}
impl Message for CounterMessage {}

#[derive(Debug, Clone)]
enum CounterResponse {
    Ack,
    Count(u64),
}
impl Response for CounterResponse {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CounterEvent(u64);
impl Event for CounterEvent {}

#[async_trait]
impl Actor for CounterActor {
    type Message = CounterMessage;
    type Event = CounterEvent;
    type Response = CounterResponse;

    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        msg: Self::Message,
    ) -> Result<Self::Response, Error> {
        match msg {
            CounterMessage::Increment => {
                self.count += 1;
                Ok(CounterResponse::Ack)
            }
            CounterMessage::GetCount => Ok(CounterResponse::Count(self.count)),
        }
    }
}

/// An echo actor that returns the received payload. Used to benchmark ask latency
/// with variable payload sizes.
struct EchoActor;

/// Wrapper to avoid orphan rule (cannot impl foreign trait on foreign type).
#[derive(Debug, Clone)]
struct Payload(Vec<u8>);
impl Message for Payload {}
impl Response for Payload {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EchoEvent;
impl Event for EchoEvent {}

#[async_trait]
impl Actor for EchoActor {
    type Message = Payload;
    type Event = EchoEvent;
    type Response = Payload;

    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        msg: Self::Message,
    ) -> Result<Self::Response, Error> {
        Ok(msg)
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Creates a system and a single counter actor, returning the actor reference.
async fn setup_counter_system() -> (System, ActorRef<CounterActor>, CancellationToken) {
    let token = CancellationToken::new();
    let config = Config::default();
    let mut system = System::new(config, token.clone());
    let actor_ref = system
        .create_actor(CounterActor::new(), "counter")
        .await
        .expect("Failed to create counter actor");
    (system, actor_ref, token)
}

/// Creates a system and a single echo actor, returning the actor reference.
async fn setup_echo_system() -> (System, ActorRef<EchoActor>, CancellationToken) {
    let token = CancellationToken::new();
    let config = Config::default();
    let mut system = System::new(config, token.clone());
    let actor_ref = system
        .create_actor(EchoActor, "echo")
        .await
        .expect("Failed to create echo actor");
    (system, actor_ref, token)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmark: Actor creation throughput.
/// Measures how fast actors can be spawned in the system.
fn bench_actor_creation(c: &mut Criterion) {
    let rt = bench_runtime();

    c.bench_function("actor_creation", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let token = CancellationToken::new();
                let config = Config::default();
                let mut system = System::new(config, token.clone());
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let name = format!("actor_{i}");
                    system
                        .create_actor(NoopActor, &name)
                        .await
                        .expect("Failed to create actor");
                }
                let elapsed = start.elapsed();
                token.cancel();
                elapsed
            })
        });
    });
}

/// Benchmark: Tell (fire-and-forget) message throughput.
/// Measures throughput of sending messages without waiting for a response.
fn bench_tell_throughput(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("tell_throughput");
    for &batch_size in &[100u64, 1_000, 10_000] {
        group.throughput(criterion::Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let (_system, actor_ref, token) = setup_counter_system().await;
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            for _ in 0..size {
                                actor_ref
                                    .tell(CounterMessage::Increment)
                                    .await
                                    .expect("tell failed");
                            }
                        }
                        // Ensure all messages are processed
                        let _ = actor_ref.ask(CounterMessage::GetCount).await;
                        let elapsed = start.elapsed();
                        token.cancel();
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Ask (request-response) latency.
/// Measures round-trip latency per message.
fn bench_ask_latency(c: &mut Criterion) {
    let rt = bench_runtime();

    c.bench_function("ask_latency", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let (_system, actor_ref, token) = setup_counter_system().await;
                let start = std::time::Instant::now();
                for _ in 0..iters {
                    let resp = actor_ref
                        .ask(CounterMessage::Increment)
                        .await
                        .expect("ask failed");
                    assert!(matches!(resp, CounterResponse::Ack));
                }
                let elapsed = start.elapsed();
                token.cancel();
                elapsed
            })
        });
    });
}

/// Benchmark: Ask throughput with variable payload sizes.
/// Tests how payload size affects message processing performance.
fn bench_ask_payload_sizes(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("ask_payload_size");
    for &payload_bytes in &[64usize, 1_024, 16_384, 65_536] {
        group.throughput(criterion::Throughput::Bytes(payload_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_bytes),
            &payload_bytes,
            |b, &size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let (_system, actor_ref, token) = setup_echo_system().await;
                        let payload = Payload(vec![0xABu8; size]);
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let resp = actor_ref
                                .ask(payload.clone())
                                .await
                                .expect("ask failed");
                            assert_eq!(resp.0.len(), size);
                        }
                        let elapsed = start.elapsed();
                        token.cancel();
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Multiple actors with sequential tell (fan-out).
/// Measures fan-out: sending one message to each of N actors.
fn bench_fanout_tell(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("fanout_tell");
    for &num_actors in &[10usize, 50, 100] {
        group.throughput(criterion::Throughput::Elements(num_actors as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_actors),
            &num_actors,
            |b, &n| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let token = CancellationToken::new();
                        let config = Config::default();
                        let mut system = System::new(config, token.clone());

                        // Create N actors
                        let mut refs = Vec::with_capacity(n);
                        for i in 0..n {
                            let name = format!("counter_{i}");
                            let actor_ref = system
                                .create_actor(CounterActor::new(), &name)
                                .await
                                .expect("Failed to create actor");
                            refs.push(actor_ref);
                        }

                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            for actor_ref in &refs {
                                actor_ref
                                    .tell(CounterMessage::Increment)
                                    .await
                                    .expect("tell failed");
                            }
                        }
                        // Flush: ask each actor to ensure messages are processed
                        for actor_ref in &refs {
                            let _ = actor_ref.ask(CounterMessage::GetCount).await;
                        }
                        let elapsed = start.elapsed();
                        token.cancel();
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Concurrent ask from multiple tasks to a single actor.
/// Measures contention: N tasks sending ask messages concurrently to the same actor.
fn bench_concurrent_ask(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("concurrent_ask");
    for &num_tasks in &[10usize, 50, 100] {
        group.throughput(criterion::Throughput::Elements(num_tasks as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_tasks),
            &num_tasks,
            |b, &n| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let (_system, actor_ref, token) = setup_counter_system().await;

                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let mut handles = Vec::with_capacity(n);
                            for _ in 0..n {
                                let actor_ref = actor_ref.clone();
                                handles.push(tokio::spawn(async move {
                                    actor_ref
                                        .ask(CounterMessage::Increment)
                                        .await
                                        .expect("ask failed");
                                }));
                            }
                            for handle in handles {
                                handle.await.expect("task panicked");
                            }
                        }
                        let elapsed = start.elapsed();
                        token.cancel();
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: Event subscription throughput.
/// Measures the overhead of emitting events to subscribers.
fn bench_event_subscription(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("event_subscription");
    for &num_subscribers in &[1usize, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("subscribers", num_subscribers),
            &num_subscribers,
            |b, &n| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let (_system, actor_ref, token) = setup_counter_system().await;

                        // Create N subscribers
                        let mut subscribers = Vec::with_capacity(n);
                        for _ in 0..n {
                            subscribers.push(actor_ref.subscribe());
                        }

                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            actor_ref
                                .tell(CounterMessage::Increment)
                                .await
                                .expect("tell failed");
                        }
                        // Flush
                        let _ = actor_ref.ask(CounterMessage::GetCount).await;
                        let elapsed = start.elapsed();
                        token.cancel();
                        elapsed
                    })
                });
            },
        );
    }
    group.finish();
}

/// Benchmark: System teardown time.
/// Measures how long it takes to stop all actors in the system.
fn bench_system_teardown(c: &mut Criterion) {
    let rt = bench_runtime();

    let mut group = c.benchmark_group("system_teardown");
    for &num_actors in &[10usize, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_actors),
            &num_actors,
            |b, &n| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let mut total = std::time::Duration::ZERO;
                        for _ in 0..iters {
                            let token = CancellationToken::new();
                            let config = Config::default();
                            let mut system = System::new(config, token.clone());
                            for i in 0..n {
                                let name = format!("actor_{i}");
                                system
                                    .create_actor(NoopActor, &name)
                                    .await
                                    .expect("Failed to create actor");
                            }
                            let start = std::time::Instant::now();
                            system.stop_children().await.expect("stop failed");
                            total += start.elapsed();
                            token.cancel();
                        }
                        total
                    })
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups & main
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_actor_creation,
    bench_tell_throughput,
    bench_ask_latency,
    bench_ask_payload_sizes,
    bench_fanout_tell,
    bench_concurrent_ask,
    bench_event_subscription,
    bench_system_teardown,
);
criterion_main!(benches);
