# sfo-pool

[简体中文](README_cn.md)

`sfo-pool` is a Tokio-based asynchronous worker pool that provides both general-purpose and keyed worker pools.

It manages workers with RAII: `get_worker()` returns a guard, and the worker is automatically returned to the pool when that guard goes out of scope. No explicit `release` call is required.

## Features

- Asynchronous worker creation with configurable pool capacity
- Asynchronous queueing when no worker is available, with automatic wake-up when a worker is returned
- Invalid-worker detection and eviction through `Worker::is_work()`
- Idle timeouts and explicit idle-worker cleanup
- Graceful pool clearing that waits for in-flight workers
- Keyed worker reuse, creation, replacement, and limits
- Automatic capacity-reservation rollback when creation fails or a creation task is cancelled

## Installation

Add the following dependencies to `Cargo.toml`:

```toml
[dependencies]
sfo-pool = "0.3"
async-trait = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## General-purpose worker pool

Implement `Worker` and `WorkerFactory`, then acquire workers from a `WorkerPool`:

```rust
use async_trait::async_trait;
use sfo_pool::{PoolResult, Worker, WorkerFactory, WorkerPool};

struct Connection {
    id: u64,
    healthy: bool,
}

impl Worker for Connection {
    fn is_work(&self) -> bool {
        self.healthy
    }
}

struct ConnectionFactory;

#[async_trait]
impl WorkerFactory<Connection> for ConnectionFactory {
    async fn create(&self) -> PoolResult<Connection> {
        // A real connection can be established asynchronously here.
        Ok(Connection {
            id: 1,
            healthy: true,
        })
    }
}

#[tokio::main]
async fn main() -> PoolResult<()> {
    let pool = WorkerPool::new(8, ConnectionFactory);

    {
        let connection = pool.get_worker().await?;
        println!("using connection {}", connection.id);
    } // Dropping the guard automatically returns the connection to the pool.

    pool.clear_all_worker().await;
    Ok(())
}
```

Use `new_with_config` to configure an idle timeout:

```rust
use sfo_pool::{WorkerPool, WorkerPoolConfig};
use std::time::Duration;

# use async_trait::async_trait;
# use sfo_pool::{PoolResult, Worker, WorkerFactory};
# struct Connection;
# impl Worker for Connection { fn is_work(&self) -> bool { true } }
# struct ConnectionFactory;
# #[async_trait]
# impl WorkerFactory<Connection> for ConnectionFactory {
#     async fn create(&self) -> PoolResult<Connection> { Ok(Connection) }
# }
let pool = WorkerPool::new_with_config(
    ConnectionFactory,
    WorkerPoolConfig::default()
        .with_max_count(Some(8))
        .with_max_idle_count(Some(4))
        .with_idle_timeout(Some(Duration::from_secs(60))),
);

// Idle workers are cleaned up lazily before the next acquisition, or explicitly.
let removed_count = pool.cleanup_idle_worker();
# let _ = removed_count;
```

`max_count` limits the total of active, idle, and currently creating workers. Set it to `None` for an unlimited total; `Some(0)` is invalid. `max_idle_count` independently limits only the idle LRU cache. `None` preserves the previous behavior, `Some(0)` disables idle caching, and a full idle cache never prevents creating another active worker when total capacity permits. Configuration fields are private; construct values with `Default` and the `with_*` methods.

## Keyed worker pool

The keyed pool is useful when workers must be allocated by tenant, region, database, or another condition:

```rust
use async_trait::async_trait;
use sfo_pool::{
    KeyedWorker, KeyedWorkerFactory, KeyedWorkerPool,
    KeyedWorkerPoolConfig, PoolResult,
};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
enum Region {
    East,
    West,
}

struct RegionalConnection {
    region: Region,
    healthy: bool,
}

impl KeyedWorker<Region> for RegionalConnection {
    fn is_work(&self) -> bool {
        self.healthy
    }

    fn supports(&self, region: Region) -> bool {
        self.region == region
    }

    fn primary_key(&self) -> Region {
        self.region.clone()
    }
}

struct RegionalConnectionFactory;

#[async_trait]
impl KeyedWorkerFactory<Region, RegionalConnection> for RegionalConnectionFactory {
    async fn create(&self, region: Region) -> PoolResult<RegionalConnection> {
        Ok(RegionalConnection {
            region,
            healthy: true,
        })
    }
}

#[tokio::main]
async fn main() -> PoolResult<()> {
    let pool = KeyedWorkerPool::new(
        RegionalConnectionFactory,
        KeyedWorkerPoolConfig::default()
            .with_max_count_per_key(Some(4))
            .with_max_idle_count_per_key(Some(2)),
    );

    let connection = pool.get_worker(Region::East).await?;
    assert_eq!(connection.region, Region::East);

    Ok(())
}
```

In a keyed pool, a worker created by the factory for `key` must satisfy both conditions:

- `worker.primary_key() == key`
- `worker.supports(worker.primary_key()) == true`

Otherwise, acquisition returns `PoolErrorCode::InvalidConfig`. A worker's primary key is cached when it is created. If that primary key changes while the worker is checked out, the worker is evicted when returned.

### Keyed capacity semantics

`max_count_per_key` is an independent hard limit for each key. Once the limit is reached, new requests for that key wait for an existing worker. `None` leaves each key unlimited, while `Some(0)` causes requests to return an invalid-configuration error. There is no pool-wide worker-count limit, so activity for one key does not consume another key's capacity.

`max_idle_count_per_key` independently limits the idle cache for each primary key and counts neither checked-out nor pending workers. On return, a worker is handed directly to a compatible waiter when possible; otherwise it becomes the MRU worker in its primary-key bucket. When a bucket exceeds its limit, its oldest idle worker is evicted. `None` leaves each bucket unlimited; zero disables idle caching. There is no pool-wide idle-cache limit.

Explicit keyed acquisition only cleans and reuses the requested key's primary bucket. Workers from other primary-key buckets are not considered, even if `worker.supports(requested_key)` would return `true`.

## Classified worker creation concurrency

`ClassifiedWorkerPoolConfig` limits concurrent calls to `ClassifiedWorkerFactory::create` to 20 by default. The limit is shared by generic `get_worker()` calls and classification-specific `get_classified_worker()` calls, preventing bursts of speculative generic creation from exhausting resources. Use `with_max_concurrent_creation_count()` to change it; zero is invalid and causes acquisition to return `PoolErrorCode::InvalidConfig`.

## Clearing and errors

`clear_all_worker().await` performs the following operations:

1. Immediately removes all idle workers.
2. Completes currently queued requests with `PoolErrorCode::Cleared`.
3. Temporarily rejects new requests with `PoolErrorCode::Clearing`.
4. Waits for checked-out and currently creating workers to leave the pool.

The pool can be used again after clearing completes.

The public error types are:

- `PoolResult<T>`: the common result type for pool operations
- `PoolError`: an error containing a `PoolErrorCode`
- `PoolErrorCode::Failed`: a general failure
- `PoolErrorCode::Clearing`: the pool is currently being cleared
- `PoolErrorCode::Cleared`: the request was terminated by a clear operation
- `PoolErrorCode::InvalidConfig`: the configuration or a factory result is invalid

## Implementation requirements

`Worker::is_work()`, `KeyedWorker::is_work()`, `KeyedWorker::supports()`, and `KeyedWorker::primary_key()` may be called while the pool's internal state lock is held. These methods must be fast and non-blocking, and they must not re-enter APIs on the same pool.

Idle timeouts do not start a background cleanup task. Explicit keyed or classified acquisition cleans only the requested bucket. A classification-free acquisition cleans each bucket it actually traverses while looking for a worker. Applications can call `cleanup_idle_worker()` to scan every idle bucket explicitly.

## Development

```bash
cargo fmt -- --check
cargo check
cargo test
```

See [`doc/design.md`](doc/design.md) for a more detailed discussion of scheduling and concurrency behavior.

## License

This project is licensed under the terms described in the repository's [`LICENSE`](LICENSE) file.
