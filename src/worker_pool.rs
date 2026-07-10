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

struct WorkerReservation<W: Worker, F: WorkerFactory<W>> {
    pool_ref: WorkerPoolRef<W, F>,
    active: bool,
}

impl<W: Worker, F: WorkerFactory<W>> WorkerReservation<W, F> {
    fn new(pool_ref: WorkerPoolRef<W, F>) -> Self {
        Self {
            pool_ref,
            active: true,
        }
    }

    fn complete(mut self) -> bool {
        let (clearing, clear_waiters) = {
            let mut state = self.pool_ref.state.lock().unwrap();
            if state.clearing {
                state.current_count -= 1;
                (true, state.take_clear_waiters_if_done())
            } else {
                (false, Vec::new())
            }
        };
        self.active = false;
        for waiter in clear_waiters {
            waiter.notify(());
        }
        clearing
    }
}

impl<W: Worker, F: WorkerFactory<W>> Drop for WorkerReservation<W, F> {
    fn drop(&mut self) {
        if self.active {
            self.pool_ref.rollback_reservation();
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

    fn take_expired_idle_workers(
        state: &mut WorkerPoolState<W, F>,
        idle_timeout: Option<Duration>,
    ) -> Vec<W> {
        let Some(idle_timeout) = idle_timeout else {
            return Vec::new();
        };
        let mut removed_workers = Vec::new();
        let now = Instant::now();
        while state
            .worker_list
            .front()
            .map(|idle_worker| now.duration_since(idle_worker.idle_since) >= idle_timeout)
            .unwrap_or(false)
        {
            let idle_worker = state.worker_list.pop_front().unwrap();
            state.current_count -= 1;
            removed_workers.push(idle_worker.worker);
        }
        removed_workers
    }

    pub fn cleanup_idle_worker(&self) -> u16 {
        let (removed_workers, clear_waiters) = {
            let mut state = self.state.lock().unwrap();
            let removed_workers =
                Self::take_expired_idle_workers(&mut state, self.config.idle_timeout);
            let clear_waiters = state.take_clear_waiters_if_done();
            (removed_workers, clear_waiters)
        };
        for waiter in clear_waiters {
            waiter.notify(());
        }
        let removed_count = removed_workers.len() as u16;
        drop(removed_workers);
        removed_count
    }

    pub async fn get_worker(self: &WorkerPoolRef<W, F>) -> PoolResult<WorkerGuard<W, F>> {
        loop {
            if self.max_count == 0 {
                return Err(pool_invalid_config_error("pool max_count is zero"));
            }

            let (worker, wait, should_create, removed_workers) = {
                let mut state = self.state.lock().unwrap();
                if state.clearing {
                    return Err(pool_clearing_error());
                }

                let mut removed_workers =
                    Self::take_expired_idle_workers(&mut state, self.config.idle_timeout);

                let worker = loop {
                    let Some(idle_worker) = state.worker_list.pop_back() else {
                        break None;
                    };
                    let worker = idle_worker.worker;
                    if !worker.is_work() {
                        state.current_count -= 1;
                        removed_workers.push(worker);
                        continue;
                    }
                    break Some(worker);
                };

                if worker.is_some() {
                    (worker, None, false, removed_workers)
                } else if state.current_count < self.max_count {
                    state.current_count += 1;
                    (None, None, true, removed_workers)
                } else {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push_back(notify);
                    (None, Some(waiter), false, removed_workers)
                }
            };

            let reservation = should_create.then(|| WorkerReservation::new(self.clone()));
            drop(removed_workers);

            if let Some(worker) = worker {
                return Ok(WorkerGuard::new(worker, self.clone()));
            }

            if let Some(wait) = wait {
                match wait.await {
                    WorkerWaitResult::Worker(worker) => return Ok(worker),
                    WorkerWaitResult::Retry => continue,
                    WorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let reservation = reservation.unwrap();
            let worker = match self.factory.create().await {
                Ok(worker) => worker,
                Err(err) => return Err(err),
            };
            if reservation.complete() {
                return Err(pool_cleared_error());
            }
            return Ok(WorkerGuard::new(worker, self.clone()));
        }
    }

    pub async fn clear_all_worker(&self) {
        let (waiter, waiting_list, clear_waiters, idle_workers) = {
            let mut state = self.state.lock().unwrap();
            let idle_workers = if !state.clearing {
                state.clearing = true;
                let cur_worker_count = state.worker_list.len();
                let idle_workers = state
                    .worker_list
                    .drain(..)
                    .map(|idle_worker| idle_worker.worker)
                    .collect::<Vec<_>>();
                state.current_count -= cur_worker_count as u16;
                idle_workers
            } else {
                Vec::new()
            };

            let waiting_list = state.waiting_list.drain(..).collect::<Vec<_>>();
            if state.current_count == 0 {
                let clear_waiters = state.take_clear_waiters_if_done();
                (None, waiting_list, clear_waiters, idle_workers)
            } else {
                let (notify, waiter) = Notify::new();
                state.clear_waiting_list.push(notify);
                (Some(waiter), waiting_list, Vec::new(), idle_workers)
            }
        };
        for waiting in waiting_list {
            waiting.notify(WorkerWaitResult::Error(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        drop(idle_workers);
        if let Some(waiter) = waiter {
            waiter.await;
        }
    }

    fn notify_retry_waiters(waiters: Vec<Notify<WorkerWaitResult<W, F>>>) {
        for waiter in waiters {
            waiter.notify(WorkerWaitResult::Retry);
        }
    }

    fn rollback_reservation(&self) {
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

#[tokio::test]
async fn test_canceled_create_rolls_back_reservation() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct TestWorker;

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    struct TestWorkerFactory {
        create_started: Arc<AtomicBool>,
        allow_create: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            self.create_started.store(true, Ordering::SeqCst);
            while !self.allow_create.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(TestWorker)
        }
    }

    let create_started = Arc::new(AtomicBool::new(false));
    let allow_create = Arc::new(AtomicBool::new(false));
    let pool = WorkerPool::new(
        1,
        TestWorkerFactory {
            create_started: create_started.clone(),
            allow_create: allow_create.clone(),
        },
    );

    let pool_ref = pool.clone();
    let create_task = tokio::spawn(async move { pool_ref.get_worker().await });
    while !create_started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    create_task.abort();
    assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

    allow_create.store(true, Ordering::SeqCst);
    let worker = tokio::time::timeout(std::time::Duration::from_secs(1), pool.get_worker())
        .await
        .unwrap()
        .unwrap();
    drop(worker);

    tokio::time::timeout(std::time::Duration::from_secs(1), pool.clear_all_worker())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_cleanup_drops_idle_worker_outside_state_lock() {
    use std::sync::mpsc;

    type DropCallback = Box<dyn FnOnce() + Send>;

    struct TestWorker {
        on_drop: Option<DropCallback>,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            true
        }
    }

    impl Drop for TestWorker {
        fn drop(&mut self) {
            if let Some(on_drop) = self.on_drop.take() {
                on_drop();
            }
        }
    }

    struct TestWorkerFactory {
        on_drop: Arc<Mutex<Option<DropCallback>>>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                on_drop: self.on_drop.lock().unwrap().take(),
            })
        }
    }

    let on_drop = Arc::new(Mutex::new(None));
    let pool = WorkerPool::new_with_config(
        1,
        TestWorkerFactory {
            on_drop: on_drop.clone(),
        },
        WorkerPoolConfig {
            idle_timeout: Some(Duration::ZERO),
        },
    );
    let (tx, rx) = mpsc::channel();
    let pool_ref = pool.clone();
    *on_drop.lock().unwrap() = Some(Box::new(move || {
        pool_ref.cleanup_idle_worker();
        tx.send(()).unwrap();
    }));

    let worker = pool.get_worker().await.unwrap();
    drop(worker);

    let pool_ref = pool.clone();
    let cleanup_thread = std::thread::spawn(move || pool_ref.cleanup_idle_worker());
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(cleanup_thread.join().unwrap(), 1);
}

#[cfg(test)]
mod affected_drop_path_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    type DropCallback = Box<dyn FnOnce() + Send>;

    struct TestWorker {
        working: Arc<AtomicBool>,
        on_drop: Option<DropCallback>,
    }

    #[async_trait::async_trait]
    impl Worker for TestWorker {
        fn is_work(&self) -> bool {
            self.working.load(Ordering::SeqCst)
        }
    }

    impl Drop for TestWorker {
        fn drop(&mut self) {
            if let Some(on_drop) = self.on_drop.take() {
                on_drop();
            }
        }
    }

    struct WorkerSpec {
        working: Arc<AtomicBool>,
        on_drop: Option<DropCallback>,
    }

    struct TestWorkerFactory {
        specs: Arc<Mutex<VecDeque<WorkerSpec>>>,
    }

    #[async_trait::async_trait]
    impl WorkerFactory<TestWorker> for TestWorkerFactory {
        async fn create(&self) -> PoolResult<TestWorker> {
            let spec = self.specs.lock().unwrap().pop_front().unwrap();
            Ok(TestWorker {
                working: spec.working,
                on_drop: spec.on_drop,
            })
        }
    }

    fn new_pool() -> (
        WorkerPoolRef<TestWorker, TestWorkerFactory>,
        Arc<Mutex<VecDeque<WorkerSpec>>>,
    ) {
        let specs = Arc::new(Mutex::new(VecDeque::new()));
        let pool = WorkerPool::new(
            1,
            TestWorkerFactory {
                specs: specs.clone(),
            },
        );
        (pool, specs)
    }

    fn lock_check_spec(
        pool: &WorkerPoolRef<TestWorker, TestWorkerFactory>,
        working: Arc<AtomicBool>,
    ) -> (WorkerSpec, mpsc::Receiver<bool>) {
        let (tx, rx) = mpsc::channel();
        let pool_ref = Arc::downgrade(pool);
        let on_drop = Box::new(move || {
            let pool_ref = pool_ref.upgrade().unwrap();
            tx.send(pool_ref.state.try_lock().is_ok()).unwrap();
        });
        (
            WorkerSpec {
                working,
                on_drop: Some(on_drop),
            },
            rx,
        )
    }

    fn plain_spec() -> WorkerSpec {
        WorkerSpec {
            working: Arc::new(AtomicBool::new(true)),
            on_drop: None,
        }
    }

    #[tokio::test]
    async fn test_invalid_idle_worker_is_dropped_outside_state_lock() {
        let (pool, specs) = new_pool();
        let working = Arc::new(AtomicBool::new(true));
        let (spec, drop_result) = lock_check_spec(&pool, working.clone());
        specs.lock().unwrap().push_back(spec);

        let worker = pool.get_worker().await.unwrap();
        drop(worker);
        working.store(false, Ordering::SeqCst);
        specs.lock().unwrap().push_back(plain_spec());

        let replacement = pool.get_worker().await.unwrap();
        assert!(drop_result.recv_timeout(Duration::from_secs(1)).unwrap());
        drop(replacement);
    }

    #[tokio::test]
    async fn test_clear_drops_idle_worker_outside_state_lock() {
        let (pool, specs) = new_pool();
        let (spec, drop_result) = lock_check_spec(&pool, Arc::new(AtomicBool::new(true)));
        specs.lock().unwrap().push_back(spec);

        let worker = pool.get_worker().await.unwrap();
        drop(worker);
        pool.clear_all_worker().await;

        assert!(drop_result.recv_timeout(Duration::from_secs(1)).unwrap());
    }
}
