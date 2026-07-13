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
    WorkerPoolConfig::default()
        .with_max_count(Some(8))
        .with_max_idle_count(Some(4))
        .with_idle_timeout(Some(Duration::from_secs(60))),
);

// 空闲 worker 会在下次获取前被懒惰清理，也可以主动触发清理。
let removed_count = pool.cleanup_idle_worker();
# let _ = removed_count;
```

`max_count` 限制活动、空闲和正在创建的 worker 总数；`None` 表示总数不受限，`Some(0)` 是无效配置。`max_idle_count` 只独立限制空闲 LRU 缓存；`None` 保持旧行为，`Some(0)` 禁用空闲缓存。即使空闲缓存已满，只要总容量允许，仍可创建新的活动 worker。配置字段为私有字段，请通过 `Default` 和 `with_*` 方法构造配置。

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
        KeyedWorkerPoolConfig::default()
            .with_max_count_per_key(Some(4))
            .with_max_idle_count_per_key(Some(2)),
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

`max_count_per_key` 是每个键相互独立的硬限制；达到限制后，同键的新请求会等待已有 worker。`None` 表示每个键均不限制，`Some(0)` 会让请求返回无效配置错误。池不再提供全局 worker 数量限制，因此一个键的活动不会占用另一个键的容量。

`max_idle_count_per_key` 独立限制每个主键的空闲缓存，不统计已借出或正在创建的 worker。归还时会优先直接交付给兼容 waiter，否则成为对应主键桶中的 MRU；单桶超限时淘汰该桶最旧的空闲 worker。`None` 表示每个桶均不限制，零值禁用空闲缓存。池不再提供全局空闲缓存上限。

明确 key 的获取只清理和复用该 key 对应的主键桶。即使其他主键桶中的 worker 对 `worker.supports(requested_key)` 返回 `true`，也不会跨桶复用。

## 分类 worker 创建并发

`ClassifiedWorkerPoolConfig` 默认最多允许 20 个 `ClassifiedWorkerFactory::create` 调用并发执行。通用 `get_worker()` 和指定分类的 `get_classified_worker()` 共享该限制，避免通用获取的推测创建在并发突发时耗尽资源。可通过 `with_max_concurrent_creation_count()` 调整；零值无效，获取操作会返回 `PoolErrorCode::InvalidConfig`。

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

空闲超时不会启动后台清理任务。明确 key 或 classification 的获取只清理请求对应的桶；不指定 classification 的获取只清理查找过程中实际遍历到的桶。应用可以调用 `cleanup_idle_worker()` 显式扫描所有空闲桶。

## 开发

```bash
cargo fmt -- --check
cargo check
cargo test
```

更详细的调度与并发设计参见 [`doc/design.md`](doc/design.md)。

## License

本项目使用仓库中 [`LICENSE`](LICENSE) 文件所述许可证。
