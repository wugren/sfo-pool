use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use notify_future::NotifyFuture;
use tokio::runtime::Runtime;

#[async_trait::async_trait]
pub trait Worker {
    fn is_work(&self) -> bool;
}

pub struct WorkerGuard<W: Worker, F: WorkerFactory<W>> {
    pool_ref: WorkerPoolRef<W, F>,
    worker: Option<W>
}

impl<W: Worker, F: WorkerFactory<W>> WorkerGuard<W, F> {
    fn new(worker: W, pool_ref: WorkerPoolRef<W, F>) -> Self {
        WorkerGuard {
            pool_ref,
            worker: Some(worker)
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
pub trait WorkerFactory<W: Worker> {
    async fn create(&self) -> W;
}

struct WorkerPoolState<W: Worker, F: WorkerFactory<W>> {
    current_count: u16,
    worker_list: VecDeque<W>,
    waiting_list: VecDeque<NotifyFuture<WorkerGuard<W, F>>>,
}
pub struct WorkerPool<W: Worker, F: WorkerFactory<W>> {
    factory: F,
    max_count: u16,
    state: Mutex<WorkerPoolState<W, F>>,
}
pub type WorkerPoolRef<W, F> = Arc<WorkerPool<W, F>>;

impl<W: Worker, F: WorkerFactory<W>> WorkerPool<W, F> {
    pub fn new(max_count: u16, factory: F) -> WorkerPoolRef<W, F> {
        Arc::new(WorkerPool {
            factory,
            max_count,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                worker_list: VecDeque::with_capacity(max_count as usize),
                waiting_list: VecDeque::new(),
            }),
        })
    }

    pub async fn get_worker(self: &WorkerPoolRef<W, F>) -> WorkerGuard<W, F> {
        let wait = {
            let mut state = self.state.lock().unwrap();

            while state.worker_list.len() > 0 {
                let worker = state.worker_list.pop_front().unwrap();
                if !worker.is_work() {
                    state.current_count -= 1;
                    continue;
                }
                return WorkerGuard::new(worker, self.clone());
            }

            if state.current_count < self.max_count {
                state.current_count += 1;
                None
            } else {
                let future = NotifyFuture::new();
                state.waiting_list.push_back(future.clone());
                Some(future)
            }
        };

        if let Some(wait) = wait {
            wait.await
        } else {
            let worker = self.factory.create().await;
            WorkerGuard::new(worker, self.clone())
        }
    }

    fn release(self: &WorkerPoolRef<W, F>, work: W) {
        if work.is_work() {
            let mut state = self.state.lock().unwrap();
            let future = state.waiting_list.pop_front();
            if let Some(future) = future {
                future.set_complete(WorkerGuard::new(work, self.clone()));
            } else {
                state.worker_list.push_back(work);
            }
        } else {
            let mut state = self.state.lock().unwrap();
            let future = state.waiting_list.pop_front();
            if let Some(future) = future {
                let rt = Runtime::new().unwrap();
                let work = rt.block_on(self.factory.create());
                future.set_complete(WorkerGuard::new(work, self.clone()));
            } else {
                state.current_count -= 1;
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
        async fn create(&self) -> TestWorker {
            TestWorker { work: true }
        }
    }

    let pool = WorkerPool::new(2, TestWorkerFactory);
    let rt = Runtime::new().unwrap();
    let pool_ref = pool.clone();
    rt.spawn(async move {
        let _worker = pool_ref.get_worker().await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let pool_ref = pool.clone();
    rt.spawn(async move {
        let _worker = pool_ref.get_worker().await;
        tokio::time::sleep(Duration::from_secs(10)).await;
    });

    let pool_ref = pool.clone();
    rt.spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let _worker3 = pool_ref.get_worker().await;
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("duration {}", duration.as_millis());
        assert!(duration.as_millis() > 2000);
    });

    sleep(Duration::from_secs(10));
}
