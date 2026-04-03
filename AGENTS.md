# Repository Guidelines

## Project Structure & Module Organization
`sfo-pool` is a single-crate Rust library. The crate root is [`src/lib.rs`](F:\work\sfo-pool\src\lib.rs), which re-exports the two main modules:

- [`src/worker_pool.rs`](F:\work\sfo-pool\src\worker_pool.rs): generic async worker pool primitives.
- [`src/classified_worker_pool.rs`](F:\work\sfo-pool\src\classified_worker_pool.rs): worker pools with classification-aware allocation.

Build artifacts are written to `target/` and should not be committed. Tests currently live alongside implementation in the same module files.

## Build, Test, and Development Commands
- `cargo check`: fast compile check for the library.
- `cargo test`: runs the crate’s unit tests, including async pool behavior.
- `cargo fmt`: applies standard Rust formatting.
- `cargo fmt -- --check`: CI-style formatting check before opening a PR.

Run commands from the repository root:

```bash
cargo check
cargo test
```

## Coding Style & Naming Conventions
Use Rust 2021 idioms and let `rustfmt` drive formatting. Follow the existing conventions:

- 4-space indentation, no manual alignment.
- `snake_case` for functions, methods, modules, and test names.
- `PascalCase` for structs, enums, traits, and type aliases.
- Keep public APIs small and re-export them through `src/lib.rs` when they are part of the crate surface.

Prefer concise error paths with `PoolResult<T>` and `PoolError` instead of ad hoc result types.

## Testing Guidelines
Add tests next to the code they exercise using `#[test]` or `#[tokio::test]`. Cover queueing, wake-up, invalid worker replacement, and classification-specific behavior when touching pool logic. Prefer focused names such as `test_clear_all_worker` over generic names.

Use `cargo test <name>` to run a specific test during iteration.

## Commit & Pull Request Guidelines
Recent history uses short, imperative commit subjects such as `fix bug`, `rename`, and `add clear all worker api`. Keep commits small and message lines brief, lowercase, and action-oriented.

PRs should include a short summary, the behavioral change, and the validation performed (`cargo test`, `cargo fmt -- --check`). If the change affects the public API, include a minimal usage example in the PR description.
