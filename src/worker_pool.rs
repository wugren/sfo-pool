use notify_future::Notify;
pub use sfo_result::err as pool_err;
pub use sfo_result::into_err as into_pool_err;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub enum PoolErrorCode {
    #[default]
    Failed,
    Clearing,
    Cleared,
    InvalidConfig,
}
pub type PoolError = sfo_result::Error<PoolErrorCode>;
pub type PoolResult<T> = sfo_result::Result<T, PoolErrorCode>;

pub(crate) fn pool_error(code: PoolErrorCode, message: &str) -> PoolError {
    PoolError::new(code, message.to_string())
}

pub(crate) fn pool_clearing_error() -> PoolError {
    pool_error(PoolErrorCode::Clearing, "pool is clearing")
}

pub(crate) fn pool_cleared_error() -> PoolError {
    pool_error(PoolErrorCode::Cleared, "pool cleared")
}

pub(crate) fn pool_invalid_config_error(message: &str) -> PoolError {
    pool_error(PoolErrorCode::InvalidConfig, message)
}

#[derive(Debug, Clone, Default)]
pub struct WorkerPoolConfig {
    pub idle_timeout: Option<Duration>,
}

#[async_trait::async_trait]
pub trait Worker: Send + 'static {
    fn is_work(&self) -> bool;
}

pub struct WorkerGuard<W: Worker, F: WorkerFactory<W>> {
    pool_ref: WorkerPoolRef<W, F>,
    worker: Option<W>,
}

impl<W: Worker, F: WorkerFactory<W>> WorkerGuard<W, F> {
    fn new(worker: W, pool_ref: WorkerPoolRef<W, F>) -> Self {
        WorkerGuard {
            pool_ref,
            worker: Some(worker),
        }
    }
}

impl<W: Worker, F: WorkerFactory<W>> Deref for WorkerGuard<W, F> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        self.worker.as_ref().unwrap()
    }
}

impl<W: Worker, F: WorkerFactory<W>> DerefMut for WorkerGuard<W, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.worker.as_mut().unwrap()
    }
}

impl<W: Worker, F: WorkerFactory<W>> Drop for WorkerGuard<W, F> {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool_ref.release(worker);
        }
    }
}

#[async_trait::async_trait]
pub trait WorkerFactory<W: Worker>: Send + Sync + 'static {
    async fn create(&self) -> PoolResult<W>;
}

struct IdleWorker<W: Worker> {
    worker: W,
    idle_since: Instant,
}

enum WorkerWaitResult<W: Worker, F: WorkerFactory<W>> {
    Worker(WorkerGuard<W, F>),
    Retry,
    Error(PoolError),
}

struct WorkerPoolState<W: Worker, F: WorkerFactory<W>> {
    current_count: u16,
    worker_list: VecDeque<IdleWorker<W>>,
    waiting_list: VecDeque<Notify<WorkerWaitResult<W, F>>>,
    clearing: bool,
    clear_waiting_list: Vec<Notify<()>>,
}

impl<W: Worker, F: WorkerFactory<W>> WorkerPoolState<W, F> {
    fn take_clear_waiters_if_done(&mut self) -> Vec<Notify<()>> {
        if self.clearing && self.current_count == 0 {
            self.clearing = false;
            self.clear_waiting_list.drain(..).collect()
        } else {
            Vec::new()
        }
    }

    fn pop_next_waiter(&mut self) -> Option<Notify<WorkerWaitResult<W, F>>> {
        while let Some(waiter) = self.waiting_list.pop_front() {
            if !waiter.is_canceled() {
                return Some(waiter);
            }
        }
        None
    }

    fn drain_waiters(&mut self) -> Vec<Notify<WorkerWaitResult<W, F>>> {
        self.waiting_list.drain(..).collect()
    }
}
pub struct WorkerPool<W: Worker, F: WorkerFactory<W>> {
    factory: Arc<F>,
    max_count: u16,
    config: WorkerPoolConfig,
    state: Mutex<WorkerPoolState<W, F>>,
}
pub type WorkerPoolRef<W, F> = Arc<WorkerPool<W, F>>;

impl<W: Worker, F: WorkerFactory<W>> WorkerPool<W, F> {
    pub fn new(max_count: u16, factory: F) -> WorkerPoolRef<W, F> {
        Self::new_with_config(max_count, factory, WorkerPoolConfig::default())
    }

    pub fn new_with_config(
        max_count: u16,
        factory: F,
        config: WorkerPoolConfig,
    ) -> WorkerPoolRef<W, F> {
        Arc::new(WorkerPool {
            factory: Arc::new(factory),
            max_count,
            config,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                worker_list: VecDeque::with_capacity(max_count as usize),
                waiting_list: VecDeque::new(),
                clearing: false,
                clear_waiting_list: Vec::new(),
            }),
        })
    }

    fn remove_expired_idle_workers(
        state: &mut WorkerPoolState<W, F>,
        idle_timeout: Option<Duration>,
    ) -> u16 {
        let Some(idle_timeout) = idle_timeout else {
            return 0;
        };
        let mut removed_count = 0;
        let now = Instant::now();
        while state
            .worker_list
            .front()
            .map(|idle_worker| now.duration_since(idle_worker.idle_since) >= idle_timeout)
            .unwrap_or(false)
        {
            state.worker_list.pop_front();
            state.current_count -= 1;
            removed_count += 1;
        }
        removed_count
    }

    pub fn cleanup_idle_worker(&self) -> u16 {
        let (removed_count, clear_waiters) = {
            let mut state = self.state.lock().unwrap();
            let removed_count =
                Self::remove_expired_idle_workers(&mut state, self.config.idle_timeout);
            let clear_waiters = state.take_clear_waiters_if_done();
            (removed_count, clear_waiters)
        };
        for waiter in clear_waiters {
            waiter.notify(());
        }
        removed_count
    }

    pub async fn get_worker(self: &WorkerPoolRef<W, F>) -> PoolResult<WorkerGuard<W, F>> {
        loop {
            if self.max_count == 0 {
                return Err(pool_invalid_config_error("pool max_count is zero"));
            }

            let wait = {
                let mut state = self.state.lock().unwrap();
                if state.clearing {
                    return Err(pool_clearing_error());
                }

                Self::remove_expired_idle_workers(&mut state, self.config.idle_timeout);

                while let Some(idle_worker) = state.worker_list.pop_back() {
                    let worker = idle_worker.worker;
                    if !worker.is_work() {
                        state.current_count -= 1;
                        continue;
                    }
                    return Ok(WorkerGuard::new(worker, self.clone()));
                }

                if state.current_count < self.max_count {
                    state.current_count += 1;
                    None
                } else {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push_back(notify);
                    Some(waiter)
                }
            };

            if let Some(wait) = wait {
                match wait.await {
                    WorkerWaitResult::Worker(worker) => return Ok(worker),
                    WorkerWaitResult::Retry => continue,
                    WorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let worker = match self.factory.create().await {
                Ok(worker) => worker,
                Err(err) => {
                    let (retry_waiters, clear_waiters) = {
                        let mut state = self.state.lock().unwrap();
                        state.current_count -= 1;
                        let retry_waiters = state.drain_waiters();
                        let clear_waiters = state.take_clear_waiters_if_done();
                        (retry_waiters, clear_waiters)
                    };
                    Self::notify_retry_waiters(retry_waiters);
                    for waiter in clear_waiters {
                        waiter.notify(());
                    }
                    return Err(err);
                }
            };
            let (clearing, clear_waiters) = {
                let mut state = self.state.lock().unwrap();
                if state.clearing {
                    state.current_count -= 1;
                    (true, state.take_clear_waiters_if_done())
                } else {
                    (false, Vec::new())
                }
            };
            for waiter in clear_waiters {
                waiter.notify(());
            }
            if clearing {
                return Err(pool_cleared_error());
            }
            return Ok(WorkerGuard::new(worker, self.clone()));
        }
    }

    pub async fn clear_all_worker(&self) {
        let (waiter, waiting_list, clear_waiters) = {
            let mut state = self.state.lock().unwrap();
            if !state.clearing {
                state.clearing = true;
                let cur_worker_count = state.worker_list.len();
                state.worker_list.clear();
                state.current_count -= cur_worker_count as u16;
            }

            let waiting_list = state.waiting_list.drain(..).collect::<Vec<_>>();
            if state.current_count == 0 {
                let clear_waiters = state.take_clear_waiters_if_done();
                (None, waiting_list, clear_waiters)
            } else {
                let (notify, waiter) = Notify::new();
                state.clear_waiting_list.push(notify);
                (Some(waiter), waiting_list, Vec::new())
            }
        };
        for waiting in waiting_list {
            waiting.notify(WorkerWaitResult::Error(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        if let Some(waiter) = waiter {
            waiter.await;
        }
    }

    fn notify_retry_waiters(waiters: Vec<Notify<WorkerWaitResult<W, F>>>) {
        for waiter in waiters {
            waiter.notify(WorkerWaitResult::Retry);
        }
    }

    fn release(self: &WorkerPoolRef<W, F>, work: W) {
        enum ReleaseAction<W: Worker, F: WorkerFactory<W>> {
            None,
            Notify(Notify<WorkerWaitResult<W, F>>, WorkerGuard<W, F>),
            Retry(Vec<Notify<WorkerWaitResult<W, F>>>),
        }

        let mut clear_waiters = Vec::new();
        let action = {
            let mut state = self.state.lock().unwrap();
            if state.clearing {
                state.current_count -= 1;
                clear_waiters = state.take_clear_waiters_if_done();
                ReleaseAction::None
            } else if work.is_work() {
                let future = state.pop_next_waiter();
                if let Some(future) = future {
                    ReleaseAction::Notify(future, WorkerGuard::new(work, self.clone()))
                } else {
                    state.worker_list.push_back(IdleWorker {
                        worker: work,
                        idle_since: Instant::now(),
                    });
                    ReleaseAction::None
                }
            } else {
                state.current_count -= 1;
                let waiters = state.drain_waiters();
                if !waiters.is_empty() {
                    ReleaseAction::Retry(waiters)
                } else {
                    clear_waiters = state.take_clear_waiters_if_done();
                    ReleaseAction::None
                }
            }
        };

        for waiter in clear_waiters {
            waiter.notify(());
        }

        match action {
            ReleaseAction::None => {}
            ReleaseAction::Notify(future, worker) => {
                future.notify(WorkerWaitResult::Worker(worker));
            }
            ReleaseAction::Retry(waiters) => {
                Self::notify_retry_waiters(waiters);
            }
        }
    }
}

#[tokio::test]
async fn test_pool() {
    struct TestWorker {
        work: bool,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            Ok(TestWorker { work: true })
        }
    }

    let pool = WorkerPool::new(2, TestWorkerFactory);

    let worker1 = pool.get_worker().await.unwrap();
    let worker2 = pool.get_worker().await.unwrap();

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());

    drop(worker1);
    let worker3 = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(worker2);
    drop(worker3);

    let worker1 = pool.get_worker().await.unwrap();
    let worker2 = pool.get_worker().await.unwrap();

    let pool_ref = pool.clone();
    let waiter1 = tokio::spawn(async move { pool_ref.get_worker().await });
    let pool_ref = pool.clone();
    let waiter2 = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiter1.is_finished());
    assert!(!waiter2.is_finished());

    let pool_ref = pool.clone();
    let clear_task = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert!(waiter1.await.unwrap().is_err());
    assert!(waiter2.await.unwrap().is_err());

    drop(worker1);
    drop(worker2);

    tokio::time::timeout(std::time::Duration::from_secs(1), clear_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_clear_all_worker_waits_for_inflight_create() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            self.create_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(TestWorker)
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = WorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let pool_ref = pool.clone();
    let worker_task = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    pool.clear_all_worker().await;

    let worker = worker_task.await.unwrap();
    assert!(worker.is_err());
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_concurrent_clear_all_worker() {
    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            Ok(TestWorker)
        }
    }

    let pool = WorkerPool::new(1, TestWorkerFactory);
    let worker = pool.get_worker().await.unwrap();

    let pool_ref = pool.clone();
    let clear_task1 = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });

    let pool_ref = pool.clone();
    let clear_task2 = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(worker);

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        clear_task1.await.unwrap();
        clear_task2.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_zero_max_count_returns_error() {
    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            Ok(TestWorker)
        }
    }

    let pool = WorkerPool::new(0, TestWorkerFactory);
    let worker = pool.get_worker().await;
    assert!(worker.is_err());
    assert_eq!(worker.err().unwrap().code(), PoolErrorCode::InvalidConfig);
}

#[tokio::test]
async fn test_create_failure_fails_waiting_workers() {
    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Err(pool_invalid_config_error("create failed"))
        }
    }

    let pool = WorkerPool::new(1, TestWorkerFactory);

    let pool_ref = pool.clone();
    let worker1 = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let pool_ref = pool.clone();
    let worker2 = tokio::spawn(async move { pool_ref.get_worker().await });

    let (worker1, worker2) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        (worker1.await.unwrap(), worker2.await.unwrap())
    })
    .await
    .unwrap();

    assert_eq!(worker1.err().unwrap().code(), PoolErrorCode::InvalidConfig);
    assert_eq!(worker2.err().unwrap().code(), PoolErrorCode::InvalidConfig);
}

#[tokio::test]
async fn test_invalid_worker_drop_outside_runtime_wakes_waiter() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestWorker {
        id: usize,
        work: bool,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, work: true })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = WorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let mut worker = pool.get_worker().await.unwrap();
    assert_eq!(worker.id, 0);
    worker.work = false;

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());

    std::thread::spawn(move || drop(worker)).join().unwrap();

    let worker = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_retry_notification_skips_canceled_waiter() {
    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            Ok(TestWorker)
        }
    }

    let (canceled_notify, canceled_waiter) = Notify::new();
    drop(canceled_waiter);
    let (notify, waiter) = Notify::new();

    WorkerPool::<TestWorker, TestWorkerFactory>::notify_retry_waiters(vec![
        canceled_notify,
        notify,
    ]);

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap();
    assert!(matches!(result, WorkerWaitResult::Retry));
}

#[tokio::test]
async fn test_clearing_and_cleared_error_codes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        should_block: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            while self.should_block.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(TestWorker)
        }
    }

    let should_block = Arc::new(AtomicBool::new(true));
    let pool = WorkerPool::new(
        1,
        TestWorkerFactory {
            should_block: should_block.clone(),
        },
    );

    let pool_ref = pool.clone();
    let inflight = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::task::yield_now().await;

    let pool_ref = pool.clone();
    let clear_task = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });
    tokio::task::yield_now().await;

    let err = pool.get_worker().await.err().unwrap();
    assert_eq!(err.code(), PoolErrorCode::Clearing);

    should_block.store(false, Ordering::SeqCst);
    clear_task.await.unwrap();

    let err = inflight.await.unwrap().err().unwrap();
    assert_eq!(err.code(), PoolErrorCode::Cleared);
}

#[tokio::test]
async fn test_idle_worker_timeout_releases_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestWorker {
        id: usize,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = WorkerPool::new_with_config(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        WorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..WorkerPoolConfig::default()
        },
    );

    {
        let worker = pool.get_worker().await.unwrap();
        assert_eq!(worker.id, 0);
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let worker = pool.get_worker().await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_idle_worker_reused_before_timeout() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestWorker {
        id: usize,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = WorkerPool::new_with_config(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        WorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_secs(1)),
            ..WorkerPoolConfig::default()
        },
    );

    {
        let worker = pool.get_worker().await.unwrap();
        assert_eq!(worker.id, 0);
    }

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let worker = pool.get_worker().await.unwrap();
    assert_eq!(worker.id, 0);
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_cleanup_idle_worker_can_be_triggered_externally() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestWorker {
        id: usize,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = WorkerPool::new_with_config(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        WorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..WorkerPoolConfig::default()
        },
    );

    {
        let worker = pool.get_worker().await.unwrap();
        assert_eq!(worker.id, 0);
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    assert_eq!(pool.cleanup_idle_worker(), 1);

    let worker = pool.get_worker().await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_get_worker_uses_most_recent_idle_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct TestWorker {
        id: usize,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = WorkerPool::new(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let worker1 = pool.get_worker().await.unwrap();
    let worker2 = pool.get_worker().await.unwrap();
    assert_eq!(worker1.id, 0);
    assert_eq!(worker2.id, 1);

    drop(worker1);
    drop(worker2);

    let worker = pool.get_worker().await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}
