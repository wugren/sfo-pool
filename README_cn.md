# sfo-pool

[English](README.md)

`sfo-pool` 是一个基于 Tokio 的异步 worker 池，提供普通 worker 池和按键分配的 worker 池。

它使用 RAII 管理 worker：`get_worker()` 返回一个 guard，guard 离开作用域时会自动归还 worker，无需手动调用 `release`。

## 特性

- 支持异步创建 worker，并限制池内 worker 数量
- 无可用 worker 时异步排队，worker 归还后自动唤醒等待者
- 通过 `Worker::is_work()` 检测并淘汰失效 worker
- 支持空闲超时和手动清理空闲 worker
- 支持等待在途 worker 归还的完整清池操作
- 支持按键复用、创建、替换和限制 worker
- 创建失败或创建任务被取消时自动回滚容量预留

## 安装

在 `Cargo.toml` 中添加：

```toml
[dependencies]
sfo-pool = "0.3"
async-trait = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## 普通 worker 池

实现 `Worker` 和 `WorkerFactory`，然后通过 `WorkerPool` 获取 worker：

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
        // 这里可以异步建立真实连接。
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
    } // guard 被丢弃，connection 自动归还池中

    pool.clear_all_worker().await;
    Ok(())
}
```

需要配置空闲超时时，可使用 `new_with_config`：

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
    WorkerPoolConfig {
        max_count: Some(8),
        idle_timeout: Some(Duration::from_secs(60)),
    },
);

// 空闲 worker 会在下次获取前被懒惰清理，也可以主动触发清理。
let removed_count = pool.cleanup_idle_worker();
# let _ = removed_count;
```

`max_count: None` 表示不限制 worker 数量。`max_count: Some(0)` 是无效配置，获取 worker 时会返回 `PoolErrorCode::InvalidConfig`。

## 键 worker 池

键池适用于按租户、区域、数据库或其他条件分配 worker 的场景：

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
        KeyedWorkerPoolConfig {
            max_count: Some(16),
            max_count_per_key: Some(4),
            idle_timeout: None,
        },
    );

    let connection = pool.get_worker(Region::East).await?;
    assert_eq!(connection.region, Region::East);

    Ok(())
}
```

键池中，factory 为请求键 `key` 创建的 worker 必须满足：

- `worker.primary_key() == key`
- `worker.supports(worker.primary_key()) == true`

否则获取操作会返回 `PoolErrorCode::InvalidConfig`。worker 的主键在创建时会被缓存；如果借出期间主键发生变化，该 worker 在归还时会被淘汰。

### 键池容量语义

`KeyedWorkerPoolConfig::max_count` 是目标上限，而不是严格上限。当池已满、没有可替换的空闲 worker，并且请求的键当前没有已创建或正在创建的 worker 时，池可以临时超过该值创建 worker。多余 worker 会在归还且没有等待者需要时被移除。

`max_count_per_key` 是独立的单个键硬限制；达到限制后，同键的新请求会等待已有 worker。两个上限均可设为 `None`，表示不限制。设为 `Some(0)` 时，相关请求会返回无效配置错误。

## 清理与错误

`clear_all_worker().await` 会：

1. 立即移除所有空闲 worker；
2. 让当前排队请求返回 `PoolErrorCode::Cleared`；
3. 暂时拒绝新请求，并返回 `PoolErrorCode::Clearing`；
4. 等待已借出和正在创建的 worker 退出池后完成。

清理完成后，池可以继续使用。

公开错误类型包括：

- `PoolResult<T>`：池操作的统一结果类型
- `PoolError`：包含 `PoolErrorCode` 的错误类型
- `PoolErrorCode::Failed`：通用失败
- `PoolErrorCode::Clearing`：池正在清理
- `PoolErrorCode::Cleared`：请求因清池而终止
- `PoolErrorCode::InvalidConfig`：配置或 factory 返回值无效

## 实现约束

`Worker::is_work()`、`KeyedWorker::is_work()`、`KeyedWorker::supports()` 和 `KeyedWorker::primary_key()` 可能在池的内部状态锁持有期间调用。它们必须快速、非阻塞，且不能重入同一个池的 API。

空闲超时不会启动后台清理任务。过期 worker 会在后续 `get_worker()` 调用前被清理，应用也可以定期调用 `cleanup_idle_worker()`。

## 开发

```bash
cargo fmt -- --check
cargo check
cargo test
```

更详细的调度与并发设计参见 [`doc/design.md`](doc/design.md)。

## License

本项目使用仓库中 [`LICENSE`](LICENSE) 文件所述许可证。
