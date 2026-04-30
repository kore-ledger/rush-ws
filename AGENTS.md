# AGENTS.md

## Build & Test Commands

```bash
cargo build              # build all crates
cargo test              # run all tests
cargo test -p actor     # test single crate
cargo bench            # run benchmarks (criterion)
```

## Key Facts

- **Rust**: 2024 edition, requires 1.91.0+
- **Workspace members**: `actor`, `store`
- **Default features**: `fjall` (disk-backed store); use `--features memory` for in-memory variant
- **Testing**: Uses `serial_test` - some tests must run serially, not in parallel

## Architecture

- `actor/` - Actor runtime (Actor, ActorRef, System, supervision)
- `store/` - Persistence layer (PersistentActor, Store, StoreManager)
- Root `rush` crate re-exports both as public API

## Important Context

- See `.github/copilot-instructions.md` for general Rust style conventions
- `rustfmt.toml`: max_width = 100
