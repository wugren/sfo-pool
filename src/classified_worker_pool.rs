use crate::{pool_cleared_error, pool_clearing_error, pool_invalid_config_error, PoolResult};
use notify_future::Notify;
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

pub trait WorkerClassification: Send + 'static + Clone + Hash + Eq + PartialEq {}

impl<T: Send + 'static + Clone + Hash + Eq + PartialEq> WorkerClassification for T {}

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
    future: Notify<PoolResult<ClassifiedWorkerGuard<C, W, F>>>,
    condition: Option<C>,
}
struct WorkerPoolState<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    current_count: u16,
    classified_count_map: HashMap<C, u16>,
    worker_list: Vec<W>,
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
            waiting
                .condition
                .as_ref()
                .map(|condition| worker.is_valid(condition.clone()))
                .unwrap_or(true)
        })
    }
}

pub struct ClassifiedWorkerPool<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    factory: Arc<F>,
    max_count: u16,
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
        Arc::new(ClassifiedWorkerPool {
            factory: Arc::new(factory),
            max_count,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                classified_count_map: HashMap::new(),
                worker_list: Vec::with_capacity(max_count as usize),
                waiting_list: Vec::new(),
                clearing: false,
                clear_waiting_list: Vec::new(),
            }),
        })
    }

    pub async fn get_worker(
        self: &ClassifiedWorkerPoolRef<C, W, F>,
    ) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        if self.max_count == 0 {
            return Err(pool_invalid_config_error("pool max_count is zero"));
        }

        let wait = {
            let mut state = self.state.lock().unwrap();
            if state.clearing {
                return Err(pool_clearing_error());
            }

            while state.worker_list.len() > 0 {
                let worker = state.worker_list.pop().unwrap();
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
            wait.await
        } else {
            let worker = match self.factory.create(None).await {
                Ok(worker) => {
                    if let Err(err) = Self::validate_created_worker(None, &worker) {
                        let mut state = self.state.lock().unwrap();
                        state.current_count -= 1;
                        let clear_waiters = state.take_clear_waiters_if_done();
                        drop(state);
                        for waiter in clear_waiters {
                            waiter.notify(());
                        }
                        return Err(err);
                    }
                    worker
                }
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
            Ok(ClassifiedWorkerGuard::new(worker, self.clone()))
        }
    }

    pub async fn get_classified_worker(
        self: &ClassifiedWorkerPoolRef<C, W, F>,
        classification: C,
    ) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        if self.max_count == 0 {
            return Err(pool_invalid_config_error("pool max_count is zero"));
        }

        let wait = {
            let mut state = self.state.lock().unwrap();
            if state.clearing {
                return Err(pool_clearing_error());
            }

            let old_count = state.worker_list.len() as u16;
            let unwork_classification = state
                .worker_list
                .iter()
                .filter(|worker| !worker.is_work())
                .map(|worker| worker.classification())
                .collect::<Vec<C>>();
            for classification in unwork_classification.iter() {
                state.dec_classified_count(classification.clone());
            }
            state.worker_list.retain(|worker| worker.is_work());
            state.current_count -= old_count - state.worker_list.len() as u16;
            for (index, worker) in state.worker_list.iter().enumerate() {
                if worker.is_valid(classification.clone()) {
                    let worker = state.worker_list.remove(index);
                    return Ok(ClassifiedWorkerGuard::new(worker, self.clone()));
                }
            }

            if state.current_count < self.max_count {
                state.current_count += 1;
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
            wait.await
        } else {
            let worker = match self.factory.create(Some(classification.clone())).await {
                Ok(worker) => {
                    if let Err(err) = Self::validate_created_worker(Some(&classification), &worker)
                    {
                        let mut state = self.state.lock().unwrap();
                        state.current_count -= 1;
                        let clear_waiters = state.take_clear_waiters_if_done();
                        drop(state);
                        for waiter in clear_waiters {
                            waiter.notify(());
                        }
                        return Err(err);
                    }
                    worker
                }
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
            Ok(ClassifiedWorkerGuard::new(worker, self.clone()))
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
                    .map(|worker| worker.classification())
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
            waiting.future.notify(Err(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        if let Some(waiter) = waiter {
            waiter.await;
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
                Notify<PoolResult<ClassifiedWorkerGuard<C, W, F>>>,
                ClassifiedWorkerGuard<C, W, F>,
            ),
            Replace(
                Notify<PoolResult<ClassifiedWorkerGuard<C, W, F>>>,
                Option<C>,
            ),
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
                } else {
                    state.worker_list.push(work);
                    ReleaseAction::None
                }
            } else {
                let classification = work.classification();
                state.dec_classified_count(classification.clone());
                if let Some(index) = state.find_matching_waiter_index_for_worker(&work) {
                    let waiting_item = state.waiting_list.remove(index);
                    let request_classification =
                        waiting_item.condition.clone().or(Some(classification));
                    ReleaseAction::Replace(waiting_item.future, request_classification)
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
            ReleaseAction::Notify(waiting, worker) => {
                waiting.notify(Ok(worker));
            }
            ReleaseAction::Replace(waiting, request_classification) => {
                let factory = self.factory.clone();
                let this = self.clone();
                tokio::spawn(async move {
                    let result = match factory.create(request_classification.clone()).await {
                        Ok(worker) => {
                            if let Err(err) = Self::validate_created_worker(
                                request_classification.as_ref(),
                                &worker,
                            ) {
                                let mut state = this.state.lock().unwrap();
                                state.current_count -= 1;
                                let clear_waiters = state.take_clear_waiters_if_done();
                                drop(state);
                                for waiter in clear_waiters {
                                    waiter.notify(());
                                }
                                waiting.notify(Err(err));
                                return;
                            }
                            let mut state = this.state.lock().unwrap();
                            if state.clearing {
                                state.current_count -= 1;
                                let clear_waiters = state.take_clear_waiters_if_done();
                                drop(state);
                                for waiter in clear_waiters {
                                    waiter.notify(());
                                }
                                Err(pool_cleared_error())
                            } else {
                                state.inc_classified_count(worker.classification());
                                drop(state);
                                Ok(ClassifiedWorkerGuard::new(worker, this))
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
                    waiting.notify(result);
                });
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
    let pool_ref = pool.clone();
    tokio::spawn(async move {
        let _worker = pool_ref.get_worker().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });
    let pool_ref = pool.clone();
    tokio::spawn(async move {
        let _worker = pool_ref.get_worker().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        let _worker = pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let _worker3 = pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await
            .unwrap();
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("classified duration {}", duration.as_millis());
        assert!(duration.as_millis() > 2000);
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let _worker3 = pool_ref.get_worker().await.unwrap();
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("classified duration2 {}", duration.as_millis());
        assert!(duration.as_millis() > 2000);
    });

    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        let _worker = pool_ref.get_worker().await;
        let _worker1 = pool_ref.get_worker().await;
        let _worker2 = pool_ref.get_worker().await;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let worker = pool_ref.get_worker().await;
        assert!(worker.is_err());
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let worker = pool_ref
            .get_classified_worker(TestWorkerClassification::B)
            .await;
        assert!(worker.is_err());
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        pool_ref.clear_all_worker().await;
        let end = std::time::Instant::now();
        let duration = end.duration_since(start);
        println!("classified duration3 {}", duration.as_millis());
        assert!(duration.as_millis() > 4000);
    });

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
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
async fn test_classified_pool_respects_max_count() {
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
    let _worker = pool.get_worker().await.unwrap();

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
