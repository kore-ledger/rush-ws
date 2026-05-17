# Rush

Rush is a Rust actor framework with optional persistent state.
Built on tokio, it provides an actor runtime and a pluggable
persistence layer with disk-backed (Fjall) and in-memory store
backends.

## Features

- Actor runtime with `Actor` trait, `ActorRef`, `System`, lifecycle hooks, and supervision
- Persistent actors via journal + snapshot pattern (`PersistentActor` trait)
- Pluggable store backends: Fjall (default, on-disk LSM-tree) or in-memory (`BTreeMap`)
- Actor hierarchy with configurable depth limit (max 100)
- Event subscription via `tokio::sync::broadcast` channel
- Helper system for dependency injection (`add_helper` / `get_helper`)
- Supervision strategies: `Stop`, `Retry` with no interval, fixed interval, or custom backoff
- Retry support for actor message delivery (`retry_ask`)

## Crates

| Crate | Description |
|-------|-------------|
| `actor` | Core actor runtime: `Actor` trait, `ActorRef`, `System`, lifecycle, supervision |
| `store` | Persistence layer: `PersistentActor`, `Journal`/`Snapshotter` actors, `Store`/`DbManager` traits |
| `rush` | Root crate re-exporting both as a unified public API |

## Quick Start

```rust
use rush::{Actor, ActorContext, ActorPath, Config, Error, Event, Message, Response, System};
use tokio_util::sync::CancellationToken;

// Define message, response, and event types.
#[derive(Clone)]
struct Ping;
impl Message for Ping {}
impl Response for String {}
impl Event for String {}

// Define an actor.
struct EchoActor;

#[async_trait::async_trait]
impl Actor for EchoActor {
    type Message = Ping;
    type Response = String;
    type Event = String;

    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        _msg: Self::Message,
    ) -> Result<Self::Response, Error> {
        ctx.emit_event("pong".to_owned());
        Ok("pong".to_owned())
    }
}

// Run the system.
let token = CancellationToken::new();
let mut system = System::new(Config::default(), token.clone());
let actor_ref = system.create_actor(EchoActor, "echo").await.unwrap();

// Send a message and await the response.
let response = actor_ref.ask(Ping).await.unwrap();
assert_eq!(response, "pong");

// Subscribe to events.
let mut rx = actor_ref.subscribe();
// ... call .ask() or .tell(), then receive events via rx.recv().await

// Shutdown.
token.cancel();
```

## Persistent Actors

Actors that need durable state implement `PersistentActor`, which
extends `Actor` with journaling and snapshotting.

```rust
use rush::{Actor, ActorContext, ActorPath, Config, Error as ActorError, Event, Message, Response,
           PersistentActor, StoreManager, System};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

// Define message types.
#[derive(Clone)]
enum CounterMsg { Increment, GetValue }
impl Message for CounterMsg {}

enum CounterResp { Value(u64), Ok }
impl Response for CounterResp {}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Incremented;
impl Event for Incremented {}

// Define the persistent actor.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Counter { value: u64 }

#[async_trait::async_trait]
impl Actor for Counter {
    type Message = CounterMsg;
    type Response = CounterResp;
    type Event = Incremented;

    async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.init_state(ctx).await // replay journal + last snapshot
    }

    async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.flush(ctx).await // snapshot + flush stores
    }

    async fn handle(
        &mut self, ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        msg: Self::Message,
    ) -> Result<Self::Response, ActorError> {
        match msg {
            CounterMsg::Increment => {
                let sn = self.persist(&Incremented, ctx).await?;
                Ok(CounterResp::Ok)
            }
            CounterMsg::GetValue => Ok(CounterResp::Value(self.value)),
        }
    }
}

#[async_trait::async_trait]
impl PersistentActor for Counter {
    async fn apply_event(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += 1;
        Ok(())
    }
    fn id(&self) -> String { "counter-1".to_string() }
}

// Run.
let token = CancellationToken::new();
let mut system = System::new(Config::default(), token.clone());
let manager = StoreManager::default();
system.add_helper("storage", manager.clone()).await; // required before creating actors

let actor = Counter { value: 0 };
let actor_ref = system.create_actor(actor, "counter").await.unwrap();
actor_ref.tell(CounterMsg::Increment).await.unwrap();
let resp = actor_ref.ask(CounterMsg::GetValue).await.unwrap();
// resp == CounterResp::Value(1)

token.cancel();
```

The `StoreManager` must be registered as the `"storage"` helper
**before** any `PersistentActor` starts. The `init_state` method
replays state from the journal on startup, and `flush` persists
a snapshot and flushes underlying stores on shutdown.

## Feature Flags

| Feature | Default | Backend | Persistence |
|---------|---------|---------|-------------|
| `fjall` | Yes | Fjall LSM-tree | On-disk |
| `memory` | No | `BTreeMap<String, Vec<u8>>` | None (testing) |

The two store backends are mutually exclusive — enabling both
produces a `compile_error!`. Select the backend on the root crate:

```toml
[dependencies]
rush = { features = ["memory"] }  # in-memory variant
```

## License

Apache 2.0
