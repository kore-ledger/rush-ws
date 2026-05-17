# AGENTS.md

## Build & Test Commands

```bash
cargo build              # build all crates
cargo test               # run all tests
cargo test -p actor      # test actor crate
cargo test -p store      # test store crate
cargo bench              # run all benchmarks (criterion)
```

Features: `default = ["fjall"]` (disk-backed store); use `--features memory` for in-memory variant. **Mutually exclusive** — `compile_error!` if both enabled.

## Key Facts

- **Rust**: 2024 edition, requires 1.91.0+
- **Workspace members**: `actor`, `store`
- **Testing**: Nearly all tests use `#[serial_test::serial]` — `cargo test` handles this correctly (serial_test uses a mutex)
- All tests are inline (`#[cfg(test)] mod tests`), no `tests/` directory
- Store crate uses `test_store_trait!` macro to generate store backend test suites
- `tracing_test::traced_test` + `logs_contain!("...")` for log assertion in supervision tests
- Fjall tests use `tempfile::TempDir`; `FjallDbManager::default()` writes to `./fjall_db` CWD

## Architecture

- `actor/` — Actor runtime (Actor trait, ActorRef, System, supervision, lifecycle). 6 source modules.
- `store/` — Persistence layer (PersistentActor, Journal/Snapshotter actors, Store/DbManager traits). Store depends on `actor`.
- Root `rush` crate re-exports both as public API.

Non-obvious wiring:
- `System` root path is always `/user` (hardcoded)
- `MAX_ACTOR_DEPTH = 100` hard limit
- Helper system via `add_helper`/`get_helper` for DI. `StoreManager` **must** be registered as helper `"storage"` before any `PersistentActor` starts.
- `PersistentActor::init_state` is called from `pre_start`; `flush` from `pre_stop`

## Known Issues

- `ActorRef::retry_ask()` at `actor/src/actor.rs:637`: `attempts` is `let` (immutable), never incremented — loop runs infinitely until backoff depletes.

## Important Context

- See `.github/copilot-instructions.md` for general Rust style conventions
- `rustfmt.toml`: `max_width = 100`
