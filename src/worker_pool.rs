use notify_future::Notify;
pub use sfo_result::err as pool_err;
pub use sfo_result::into_err as into_pool_err;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

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

struct WorkerPoolState<W: Worker, F: WorkerFactory<W>> {
    current_count: u16,
    worker_list: VecDeque<W>,
    waiting_list: VecDeque<Notify<PoolResult<WorkerGuard<W, F>>>>,
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
}
pub struct WorkerPool<W: Worker, F: WorkerFactory<W>> {
    factory: Arc<F>,
    max_count: u16,
    state: Mutex<WorkerPoolState<W, F>>,
}
pub type WorkerPoolRef<W, F> = Arc<WorkerPool<W, F>>;

impl<W: Worker, F: WorkerFactory<W>> WorkerPool<W, F> {
    pub fn new(max_count: u16, factory: F) -> WorkerPoolRef<W, F> {
        Arc::new(WorkerPool {
            factory: Arc::new(factory),
            max_count,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                worker_list: VecDeque::with_capacity(max_count as usize),
                waiting_list: VecDeque::new(),
                clearing: false,
                clear_waiting_list: Vec::new(),
            }),
        })
    }

    pub async fn get_worker(self: &WorkerPoolRef<W, F>) -> PoolResult<WorkerGuard<W, F>> {
        if self.max_count == 0 {
            return Err(pool_invalid_config_error("pool max_count is zero"));
        }

        let wait = {
            let mut state = self.state.lock().unwrap();
            if state.clearing {
                return Err(pool_clearing_error());
            }

            while state.worker_list.len() > 0 {
                let worker = state.worker_list.pop_front().unwrap();
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
            wait.await
        } else {
            let worker = match self.factory.create().await {
                Ok(worker) => worker,
                Err(err) => {
                    let mut state = self.state.lock().unwrap();
                    state.current_count -= 1;
                    let clear_waiters = state.take_clear_waiters_if_done();
                    drop(state);
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
            Ok(WorkerGuard::new(worker, self.clone()))
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
            waiting.notify(Err(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        if let Some(waiter) = waiter {
            waiter.await;
        }
    }

    fn release(self: &WorkerPoolRef<W, F>, work: W) {
        enum ReleaseAction<W: Worker, F: WorkerFactory<W>> {
            None,
            Notify(Notify<PoolResult<WorkerGuard<W, F>>>, WorkerGuard<W, F>),
            Replace(Notify<PoolResult<WorkerGuard<W, F>>>),
        }

        let mut clear_waiters = Vec::new();
        let action = {
            let mut state = self.state.lock().unwrap();
            if state.clearing {
                state.current_count -= 1;
                clear_waiters = state.take_clear_waiters_if_done();
                ReleaseAction::None
            } else if work.is_work() {
                let future = state.waiting_list.pop_front();
                if let Some(future) = future {
                    ReleaseAction::Notify(future, WorkerGuard::new(work, self.clone()))
                } else {
                    state.worker_list.push_back(work);
                    ReleaseAction::None
                }
            } else {
                let future = state.waiting_list.pop_front();
                if let Some(future) = future {
                    ReleaseAction::Replace(future)
                } else {
                    state.current_count -= 1;
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
                future.notify(Ok(worker));
            }
            ReleaseAction::Replace(future) => {
                let factory = self.factory.clone();
                let this = self.clone();
                tokio::spawn(async move {
                    let result = match factory.create().await {
                        Ok(worker) => {
                            let (clearing, clear_waiters) = {
                                let mut state = this.state.lock().unwrap();
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
                                Err(pool_cleared_error())
                            } else {
                                Ok(WorkerGuard::new(worker, this))
                            }
                        }
                        Err(err) => {
                            let mut state = this.state.lock().unwrap();
                            state.current_count -= 1;
                            let clear_waiters = state.take_clear_waiters_if_done();
                            drop(state);
                            for waiter in clear_waiters {
                                waiter.notify(());
                            }
                            Err(err)
                        }
                    };
                    future.notify(result);
                });
            }
        }
    }
}

#[test]
fn test_pool() {
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
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool_ref = pool.clone();
    rt.spawn(async move {
        let _worker = pool_ref.get_worker().await;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });
    let pool_ref = pool.clone();
    rt.spawn(async move {
        let _worker = pool_ref.get_worker().await;
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    });

    let pool_ref = pool.clone();
    rt.spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let _worker3 = pool_ref.get_worker().await;
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("duration {}", duration.as_millis());
        assert!(duration.as_millis() > 2000);
    });

    std::thread::sleep(std::time::Duration::from_secs(10));

    let pool_ref = pool.clone();
    rt.spawn(async move {
        let _worker = pool_ref.get_worker().await;
        let _worker1 = pool_ref.get_worker().await;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });

    let pool_ref = pool.clone();
    rt.spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let worker = pool_ref.get_worker().await;
        assert!(worker.is_err());
    });

    let pool_ref = pool.clone();
    rt.spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let worker = pool_ref.get_worker().await;
        assert!(worker.is_err());
    });

    let pool_ref = pool.clone();
    rt.spawn(async move {
        let start = std::time::Instant::now();
        pool_ref.clear_all_worker().await;
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("duration1 {}", duration.as_millis());
        assert!(duration.as_millis() > 4000);
    });

    std::thread::sleep(std::time::Duration::from_secs(10));
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
