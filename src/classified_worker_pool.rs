use crate::{
    pool_cleared_error, pool_clearing_error, pool_invalid_config_error, PoolError, PoolResult,
};
use notify_future::Notify;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub trait WorkerClassification: Send + 'static + Clone + Hash + Eq + PartialEq {}

impl<T: Send + 'static + Clone + Hash + Eq + PartialEq> WorkerClassification for T {}

#[derive(Debug, Clone, Default)]
pub struct ClassifiedWorkerPoolConfig {
    pub idle_timeout: Option<Duration>,
}

#[async_trait::async_trait]
pub trait ClassifiedWorker<C: WorkerClassification>: Send + 'static {
    fn is_work(&self) -> bool;
    /// Returns whether this worker can currently serve the requested classification.
    /// The pool still tracks capacity by the worker's primary `classification()`.
    fn is_valid(&self, c: C) -> bool;
    /// Returns the worker's primary classification used for accounting and replacement.
    fn classification(&self) -> C;
}

pub struct ClassifiedWorkerGuard<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    pool_ref: ClassifiedWorkerPoolRef<C, W, F>,
    worker: Option<W>,
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>>
    ClassifiedWorkerGuard<C, W, F>
{
    fn new(worker: W, pool_ref: ClassifiedWorkerPoolRef<C, W, F>) -> Self {
        ClassifiedWorkerGuard {
            pool_ref,
            worker: Some(worker),
        }
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> Deref
    for ClassifiedWorkerGuard<C, W, F>
{
    type Target = W;

    fn deref(&self) -> &Self::Target {
        self.worker.as_ref().unwrap()
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> DerefMut
    for ClassifiedWorkerGuard<C, W, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.worker.as_mut().unwrap()
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> Drop
    for ClassifiedWorkerGuard<C, W, F>
{
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool_ref.release(worker);
        }
    }
}

#[async_trait::async_trait]
pub trait ClassifiedWorkerFactory<C: WorkerClassification, W: ClassifiedWorker<C>>:
    Send + Sync + 'static
{
    async fn create(&self, c: Option<C>) -> PoolResult<W>;
}

struct WaitingItem<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    future: Notify<ClassifiedWorkerWaitResult<C, W, F>>,
    condition: Option<C>,
}

struct IdleWorker<W> {
    worker: W,
    idle_since: Instant,
}

enum ClassifiedWorkerWaitResult<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    Worker(ClassifiedWorkerGuard<C, W, F>),
    Retry,
    Error(PoolError),
}

struct WorkerPoolState<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    current_count: u16,
    classified_count_map: HashMap<C, u16>,
    pending_classified_count_map: HashMap<C, u16>,
    worker_list: VecDeque<IdleWorker<W>>,
    waiting_list: Vec<WaitingItem<C, W, F>>,
    clearing: bool,
    clear_waiting_list: Vec<Notify<()>>,
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>>
    WorkerPoolState<C, W, F>
{
    fn inc_classified_count(&mut self, c: C) {
        let count = self.classified_count_map.entry(c).or_insert(0);
        *count += 1;
    }

    fn dec_classified_count(&mut self, c: C) {
        let mut should_remove = false;
        if let Some(count) = self.classified_count_map.get_mut(&c) {
            debug_assert!(*count > 0);
            *count -= 1;
            should_remove = *count == 0;
        }
        if should_remove {
            self.classified_count_map.remove(&c);
        }
    }

    fn inc_pending_classified_count(&mut self, c: C) {
        let count = self.pending_classified_count_map.entry(c).or_insert(0);
        *count += 1;
    }

    fn dec_pending_classified_count(&mut self, c: C) {
        let mut should_remove = false;
        if let Some(count) = self.pending_classified_count_map.get_mut(&c) {
            debug_assert!(*count > 0);
            *count -= 1;
            should_remove = *count == 0;
        }
        if should_remove {
            self.pending_classified_count_map.remove(&c);
        }
    }

    fn reserved_classified_count(&self, c: &C) -> u16 {
        self.classified_count_map.get(c).copied().unwrap_or(0)
            + self
                .pending_classified_count_map
                .get(c)
                .copied()
                .unwrap_or(0)
    }

    fn take_clear_waiters_if_done(&mut self) -> Vec<Notify<()>> {
        if self.clearing && self.current_count == 0 {
            self.clearing = false;
            self.clear_waiting_list.drain(..).collect()
        } else {
            Vec::new()
        }
    }

    fn find_matching_waiter_index_for_worker(&self, worker: &W) -> Option<usize> {
        self.waiting_list.iter().position(|waiting| {
            if waiting.future.is_canceled() {
                return false;
            }
            waiting
                .condition
                .as_ref()
                .map(|condition| worker.is_valid(condition.clone()))
                .unwrap_or(true)
        })
    }

    fn find_any_classified_waiter_index(&self) -> Option<usize> {
        self.waiting_list
            .iter()
            .position(|waiting| waiting.condition.is_some() && !waiting.future.is_canceled())
    }

    fn drain_waiters(&mut self) -> Vec<Notify<ClassifiedWorkerWaitResult<C, W, F>>> {
        self.waiting_list
            .drain(..)
            .map(|waiting| waiting.future)
            .collect()
    }
}

pub struct ClassifiedWorkerPool<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    factory: Arc<F>,
    max_count: u16,
    config: ClassifiedWorkerPoolConfig,
    state: Mutex<WorkerPoolState<C, W, F>>,
}
pub type ClassifiedWorkerPoolRef<C, W, F> = Arc<ClassifiedWorkerPool<C, W, F>>;

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>>
    ClassifiedWorkerPool<C, W, F>
{
    fn validate_created_worker(requested_classification: Option<&C>, worker: &W) -> PoolResult<()> {
        let worker_classification = worker.classification();
        if !worker.is_valid(worker_classification.clone()) {
            return Err(pool_invalid_config_error(
                "worker primary classification is not valid for itself",
            ));
        }
        if let Some(classification) = requested_classification {
            if worker_classification != classification.clone() {
                return Err(pool_invalid_config_error(
                    "factory returned worker with mismatched classification",
                ));
            }
        }
        Ok(())
    }

    pub fn new(max_count: u16, factory: F) -> ClassifiedWorkerPoolRef<C, W, F> {
        Self::new_with_config(max_count, factory, ClassifiedWorkerPoolConfig::default())
    }

    pub fn new_with_config(
        max_count: u16,
        factory: F,
        config: ClassifiedWorkerPoolConfig,
    ) -> ClassifiedWorkerPoolRef<C, W, F> {
        Arc::new(ClassifiedWorkerPool {
            factory: Arc::new(factory),
            max_count,
            config,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                classified_count_map: HashMap::new(),
                pending_classified_count_map: HashMap::new(),
                worker_list: VecDeque::with_capacity(max_count as usize),
                waiting_list: Vec::new(),
                clearing: false,
                clear_waiting_list: Vec::new(),
            }),
        })
    }

    fn remove_expired_idle_workers(
        state: &mut WorkerPoolState<C, W, F>,
        idle_timeout: Option<std::time::Duration>,
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
            let idle_worker = state.worker_list.pop_front().unwrap();
            state.current_count -= 1;
            state.dec_classified_count(idle_worker.worker.classification());
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

    pub async fn get_worker(
        self: &ClassifiedWorkerPoolRef<C, W, F>,
    ) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
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
                        state.dec_classified_count(worker.classification());
                        continue;
                    }
                    return Ok(ClassifiedWorkerGuard::new(worker, self.clone()));
                }

                if state.current_count < self.max_count {
                    state.current_count += 1;
                    None
                } else {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push(WaitingItem {
                        future: notify,
                        condition: None,
                    });
                    Some(waiter)
                }
            };

            if let Some(wait) = wait {
                match wait.await {
                    ClassifiedWorkerWaitResult::Worker(worker) => return Ok(worker),
                    ClassifiedWorkerWaitResult::Retry => continue,
                    ClassifiedWorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let worker = match self.factory.create(None).await {
                Ok(worker) => {
                    if let Err(err) = Self::validate_created_worker(None, &worker) {
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
                    worker
                }
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
                    state.inc_classified_count(worker.classification());
                    (false, Vec::new())
                }
            };
            for waiter in clear_waiters {
                waiter.notify(());
            }
            if clearing {
                return Err(pool_cleared_error());
            }
            return Ok(ClassifiedWorkerGuard::new(worker, self.clone()));
        }
    }

    pub async fn get_classified_worker(
        self: &ClassifiedWorkerPoolRef<C, W, F>,
        classification: C,
    ) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        loop {
            if self.max_count == 0 {
                return Err(pool_invalid_config_error("pool max_count is zero"));
            }

            let wait =
                {
                    let mut state = self.state.lock().unwrap();
                    if state.clearing {
                        return Err(pool_clearing_error());
                    }

                    Self::remove_expired_idle_workers(&mut state, self.config.idle_timeout);

                    let old_count = state.worker_list.len() as u16;
                    let unwork_classification = state
                        .worker_list
                        .iter()
                        .filter(|idle_worker| !idle_worker.worker.is_work())
                        .map(|idle_worker| idle_worker.worker.classification())
                        .collect::<Vec<C>>();
                    for classification in unwork_classification.iter() {
                        state.dec_classified_count(classification.clone());
                    }
                    state
                        .worker_list
                        .retain(|idle_worker| idle_worker.worker.is_work());
                    state.current_count -= old_count - state.worker_list.len() as u16;
                    if let Some(index) = state.worker_list.iter().rposition(|idle_worker| {
                        idle_worker.worker.is_valid(classification.clone())
                    }) {
                        let idle_worker = state.worker_list.remove(index).unwrap();
                        return Ok(ClassifiedWorkerGuard::new(idle_worker.worker, self.clone()));
                    }

                    if state.current_count < self.max_count {
                        state.current_count += 1;
                        state.inc_pending_classified_count(classification.clone());
                        None
                    } else if let Some(idle_worker) = state.worker_list.pop_front() {
                        state.dec_classified_count(idle_worker.worker.classification());
                        state.inc_pending_classified_count(classification.clone());
                        None
                    } else if state.reserved_classified_count(&classification) == 0 {
                        if state.current_count == u16::MAX {
                            return Err(pool_invalid_config_error(
                                "pool current_count reached u16 max",
                            ));
                        }
                        state.current_count += 1;
                        state.inc_pending_classified_count(classification.clone());
                        None
                    } else {
                        let (notify, waiter) = Notify::new();
                        state.waiting_list.push(WaitingItem {
                            future: notify,
                            condition: Some(classification.clone()),
                        });
                        Some(waiter)
                    }
                };

            if let Some(wait) = wait {
                match wait.await {
                    ClassifiedWorkerWaitResult::Worker(worker) => return Ok(worker),
                    ClassifiedWorkerWaitResult::Retry => continue,
                    ClassifiedWorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let worker = match self.factory.create(Some(classification.clone())).await {
                Ok(worker) => {
                    if let Err(err) = Self::validate_created_worker(Some(&classification), &worker)
                    {
                        let (retry_waiters, clear_waiters) = {
                            let mut state = self.state.lock().unwrap();
                            state.current_count -= 1;
                            state.dec_pending_classified_count(classification.clone());
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
                    worker
                }
                Err(err) => {
                    let (retry_waiters, clear_waiters) = {
                        let mut state = self.state.lock().unwrap();
                        state.current_count -= 1;
                        state.dec_pending_classified_count(classification.clone());
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
                state.dec_pending_classified_count(classification.clone());
                if state.clearing {
                    state.current_count -= 1;
                    (true, state.take_clear_waiters_if_done())
                } else {
                    state.inc_classified_count(worker.classification());
                    (false, Vec::new())
                }
            };
            for waiter in clear_waiters {
                waiter.notify(());
            }
            if clearing {
                return Err(pool_cleared_error());
            }
            return Ok(ClassifiedWorkerGuard::new(worker, self.clone()));
        }
    }

    pub async fn clear_all_worker(&self) {
        let (waiter, waiting_list, clear_waiters) = {
            let mut state = self.state.lock().unwrap();
            if !state.clearing {
                state.clearing = true;
                let idle_classifications = state
                    .worker_list
                    .iter()
                    .map(|idle_worker| idle_worker.worker.classification())
                    .collect::<Vec<_>>();
                let cur_worker_count = idle_classifications.len();
                state.worker_list.clear();
                state.current_count -= cur_worker_count as u16;
                for classification in idle_classifications {
                    state.dec_classified_count(classification);
                }
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
            waiting
                .future
                .notify(ClassifiedWorkerWaitResult::Error(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        if let Some(waiter) = waiter {
            waiter.await;
        }
    }

    fn notify_retry_waiters(waiters: Vec<Notify<ClassifiedWorkerWaitResult<C, W, F>>>) {
        for waiter in waiters {
            waiter.notify(ClassifiedWorkerWaitResult::Retry);
        }
    }

    fn release(self: &ClassifiedWorkerPoolRef<C, W, F>, work: W) {
        enum ReleaseAction<
            C: WorkerClassification,
            W: ClassifiedWorker<C>,
            F: ClassifiedWorkerFactory<C, W>,
        > {
            None,
            Notify(
                Notify<ClassifiedWorkerWaitResult<C, W, F>>,
                ClassifiedWorkerGuard<C, W, F>,
            ),
            Retry(Vec<Notify<ClassifiedWorkerWaitResult<C, W, F>>>),
        }

        let mut clear_waiters = Vec::new();
        let action = {
            let mut state = self.state.lock().unwrap();
            if state.clearing {
                state.current_count -= 1;
                let classification = work.classification();
                state.dec_classified_count(classification);
                clear_waiters = state.take_clear_waiters_if_done();
                ReleaseAction::None
            } else if work.is_work() {
                if let Some(index) = state.find_matching_waiter_index_for_worker(&work) {
                    let waiting_item = state.waiting_list.remove(index);
                    ReleaseAction::Notify(
                        waiting_item.future,
                        ClassifiedWorkerGuard::new(work, self.clone()),
                    )
                } else if let Some(index) = state.find_any_classified_waiter_index() {
                    let classification = work.classification();
                    state.current_count -= 1;
                    state.dec_classified_count(classification);
                    let mut waiters = state.drain_waiters();
                    if index < waiters.len() {
                        waiters.swap(0, index);
                    }
                    ReleaseAction::Retry(waiters)
                } else if state.current_count > self.max_count {
                    state.current_count -= 1;
                    let classification = work.classification();
                    state.dec_classified_count(classification);
                    clear_waiters = state.take_clear_waiters_if_done();
                    ReleaseAction::None
                } else {
                    state.worker_list.push_back(IdleWorker {
                        worker: work,
                        idle_since: Instant::now(),
                    });
                    ReleaseAction::None
                }
            } else {
                let classification = work.classification();
                state.dec_classified_count(classification);
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
            ReleaseAction::Notify(waiting, worker) => {
                waiting.notify(ClassifiedWorkerWaitResult::Worker(worker));
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
        classification: TestWorkerClassification,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }
    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            if let Some(classification) = classification {
                Ok(TestWorker {
                    work: true,
                    classification,
                })
            } else {
                Ok(TestWorker {
                    work: true,
                    classification: TestWorkerClassification::A,
                })
            }
        }
    }

    let pool = ClassifiedWorkerPool::new(3, TestWorkerFactory);

    let worker_a1 = pool.get_worker().await.unwrap();
    let worker_a2 = pool.get_worker().await.unwrap();
    let worker_b = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();

    let pool_ref = pool.clone();
    let classified_waiter = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!classified_waiter.is_finished());

    drop(worker_b);
    let worker_b = tokio::time::timeout(std::time::Duration::from_secs(1), classified_waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(worker_a1);
    drop(worker_a2);
    drop(worker_b);

    let worker3 = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();
    let worker1 = pool.get_worker().await.unwrap();
    let worker2 = pool.get_worker().await.unwrap();

    let pool_ref = pool.clone();
    let generic_waiter = tokio::spawn(async move { pool_ref.get_worker().await });
    let pool_ref = pool.clone();
    let classified_waiter = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!generic_waiter.is_finished());
    assert!(!classified_waiter.is_finished());

    let pool_ref = pool.clone();
    let clear_task = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert!(generic_waiter.await.unwrap().is_err());
    assert!(classified_waiter.await.unwrap().is_err());

    drop(worker1);
    drop(worker2);
    drop(worker3);

    tokio::time::timeout(std::time::Duration::from_secs(1), clear_task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_clear_all_worker_waits_for_inflight_create() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            self.create_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(TestWorker {
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let pool_ref = pool.clone();
    let worker_task = tokio::spawn(async move { pool_ref.get_worker().await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    pool.clear_all_worker().await;

    let worker = worker_task.await.unwrap();
    assert!(worker.is_err());
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_concurrent_clear_all_worker() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let pool = ClassifiedWorkerPool::new(1, TestWorkerFactory);
    let worker = pool.get_worker().await.unwrap();

    let pool_ref = pool.clone();
    let clear_task1 = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });

    let pool_ref = pool.clone();
    let clear_task2 = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let pool = ClassifiedWorkerPool::new(0, TestWorkerFactory);
    let worker = pool.get_worker().await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn test_classified_pool_waits_when_classification_already_has_worker() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let pool = ClassifiedWorkerPool::new(1, TestWorkerFactory);
    let _worker = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();

    let pool_ref = pool.clone();
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    })
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_missing_classification_can_exceed_max_count_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let worker_a = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    let worker_b = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();

    assert_eq!(worker_a.id, 0);
    assert_eq!(worker_b.id, 1);
    assert_eq!(worker_b.classification(), TestWorkerClassification::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_classified_create_failure_fails_same_classification_waiters() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        B,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            _classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Err(crate::pool_invalid_config_error("create failed"))
        }
    }

    let pool = ClassifiedWorkerPool::new(1, TestWorkerFactory);

    let pool_ref = pool.clone();
    let worker1 = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let pool_ref = pool.clone();
    let worker2 = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    });

    let (worker1, worker2) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        (worker1.await.unwrap(), worker2.await.unwrap())
    })
    .await
    .unwrap();

    assert_eq!(
        worker1.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
    assert_eq!(
        worker2.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn test_classified_create_failure_wakes_generic_waiter_to_create() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if id == 0 && classification == Some(TestWorkerClassification::A) {
                Err(crate::pool_invalid_config_error("create failed"))
            } else {
                Ok(TestWorker {
                    id,
                    classification: classification.unwrap_or(TestWorkerClassification::A),
                })
            }
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let pool_ref = pool.clone();
    let classified = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::A)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let pool_ref = pool.clone();
    let generic = tokio::spawn(async move { pool_ref.get_worker().await });

    let (classified, generic) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        (classified.await.unwrap(), generic.await.unwrap())
    })
    .await
    .unwrap();

    assert_eq!(
        classified.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
    let generic = generic.unwrap();
    assert_eq!(generic.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_classified_retry_notification_skips_canceled_waiter() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let (canceled_notify, canceled_waiter) = Notify::new();
    drop(canceled_waiter);
    let (notify, waiter) = Notify::new();

    ClassifiedWorkerPool::<TestWorkerClassification, TestWorker, TestWorkerFactory>::notify_retry_waiters(
        vec![canceled_notify, notify],
    );

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap();
    assert!(matches!(result, ClassifiedWorkerWaitResult::Retry));
}

#[tokio::test]
async fn test_classified_request_replaces_non_matching_idle_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    {
        let worker = pool
            .get_classified_worker(TestWorkerClassification::A)
            .await
            .unwrap();
        assert_eq!(worker.id, 0);
    }

    let worker = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(worker.classification(), TestWorkerClassification::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_classified_waiter_replaces_returned_non_matching_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );
    let worker_a = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    let _worker_b = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());

    drop(worker_a);
    let worker = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(worker.id, 2);
    assert_eq!(worker.classification(), TestWorkerClassification::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_classified_waiter_replaces_unwork_non_matching_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        work: bool,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                work: true,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );
    let mut worker_a = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    let _worker_b = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());

    worker_a.work = false;
    drop(worker_a);

    let worker = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(worker.id, 2);
    assert_eq!(worker.classification(), TestWorkerClassification::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_factory_must_return_matching_classification() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let count = self.create_count.fetch_add(1, Ordering::SeqCst);
            let classification = if count == 0 {
                TestWorkerClassification::A
            } else {
                classification.unwrap_or(TestWorkerClassification::A)
            };
            Ok(TestWorker { classification })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );
    let worker = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );

    let worker = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await;
    assert!(worker.is_ok());
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_classified_waiter_keeps_queue_priority_over_later_generic_waiter() {
    use std::sync::mpsc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        B,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                classification: classification.unwrap_or(TestWorkerClassification::B),
            })
        }
    }

    let pool = ClassifiedWorkerPool::new(1, TestWorkerFactory);
    let worker = pool
        .get_classified_worker(TestWorkerClassification::B)
        .await
        .unwrap();

    let (tx, rx) = mpsc::channel();

    let pool_ref = pool.clone();
    let tx_classified = tx.clone();
    let classified_task = tokio::spawn(async move {
        let _worker = pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
            .unwrap();
        tx_classified.send("classified").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let pool_ref = pool.clone();
    let generic_task = tokio::spawn(async move {
        let _worker = pool_ref.get_worker().await.unwrap();
        tx.send("generic").unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(worker);

    let first = rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    assert_eq!(first, "classified");

    classified_task.await.unwrap();
    generic_task.await.unwrap();
}

#[tokio::test]
async fn test_generic_factory_worker_must_be_valid_for_its_primary_classification() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            c == TestWorkerClassification::B
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            _classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                classification: TestWorkerClassification::A,
            })
        }
    }

    let pool = ClassifiedWorkerPool::new(1, TestWorkerFactory);
    let worker = pool.get_worker().await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn test_classified_idle_worker_timeout_releases_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new_with_config(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        ClassifiedWorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..ClassifiedWorkerPoolConfig::default()
        },
    );

    {
        let worker = pool
            .get_classified_worker(TestWorkerClassification::B)
            .await
            .unwrap();
        assert_eq!(worker.id, 0);
        assert_eq!(worker.classification(), TestWorkerClassification::B);
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let worker = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(worker.classification(), TestWorkerClassification::A);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_get_classified_worker_uses_most_recent_matching_idle_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let worker1 = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    let worker2 = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    assert_eq!(worker1.id, 0);
    assert_eq!(worker2.id, 1);

    drop(worker1);
    drop(worker2);

    let worker = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_classified_cleanup_idle_worker_can_be_triggered_externally() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, c: TestWorkerClassification) -> bool {
            self.classification == c
        }

        fn classification(&self) -> TestWorkerClassification {
            self.classification.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = ClassifiedWorkerPool::new_with_config(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        ClassifiedWorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..ClassifiedWorkerPoolConfig::default()
        },
    );

    {
        let worker = pool
            .get_classified_worker(TestWorkerClassification::B)
            .await
            .unwrap();
        assert_eq!(worker.id, 0);
        assert_eq!(worker.classification(), TestWorkerClassification::B);
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    assert_eq!(pool.cleanup_idle_worker(), 1);

    let worker = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(worker.classification(), TestWorkerClassification::A);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}
