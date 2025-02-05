use std::collections::{HashMap};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use notify_future::NotifyFuture;
use crate::PoolResult;

pub trait WorkerClassification: Send + Sync + 'static + Clone + Hash + Eq + PartialEq {

}

impl<T: Send + Sync + 'static + Clone + Hash + Eq + PartialEq> WorkerClassification for T {

}

#[async_trait::async_trait]
pub trait ClassifiedWorker<C: WorkerClassification>: Send + Sync + 'static {
    fn is_work(&self) -> bool;
    fn is_valid(&self, c: C) -> bool;
    fn classification(&self) -> C;
}

pub struct ClassifiedWorkerGuard<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> {
    pool_ref: ClassifiedWorkerPoolRef<C, W, F>,
    worker: Option<W>
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> ClassifiedWorkerGuard<C, W, F> {
    fn new(worker: W, pool_ref: ClassifiedWorkerPoolRef<C, W, F>) -> Self {
        ClassifiedWorkerGuard {
            pool_ref,
            worker: Some(worker)
        }
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> Deref for ClassifiedWorkerGuard<C, W, F> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        self.worker.as_ref().unwrap()
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> DerefMut for ClassifiedWorkerGuard<C, W, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.worker.as_mut().unwrap()
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> Drop for ClassifiedWorkerGuard<C, W, F> {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool_ref.release(worker);
        }
    }
}

#[async_trait::async_trait]
pub trait ClassifiedWorkerFactory<C: WorkerClassification, W: ClassifiedWorker<C>>: Send + Sync + 'static {
    async fn create(&self, c: Option<C>) -> PoolResult<W>;
}

struct WaitingItem<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> {
    future: NotifyFuture<PoolResult<ClassifiedWorkerGuard<C, W, F>>>,
    condition: Option<C>,
}
struct WorkerPoolState<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> {
    current_count: u16,
    classified_count_map: HashMap<C, u16>,
    worker_list: Vec<W>,
    waiting_list: Vec<WaitingItem<C, W, F>>,
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> WorkerPoolState<C, W, F> {
    fn inc_classified_count(&mut self, c: C) {
        let count = self.classified_count_map.entry(c).or_insert(0);
        *count += 1;
    }

    fn dec_classified_count(&mut self, c: C) {
        let count = self.classified_count_map.entry(c).or_insert(0);
        *count -= 1;
    }

    fn get_classified_count(&self, c: C) -> u16 {
        *self.classified_count_map.get(&c).unwrap_or(&0)
    }
}

pub struct ClassifiedWorkerPool<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> {
    factory: Arc<F>,
    max_count: u16,
    state: Mutex<WorkerPoolState<C, W, F>>,
}
pub type ClassifiedWorkerPoolRef<C, W, F> = Arc<ClassifiedWorkerPool<C, W, F>>;

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> ClassifiedWorkerPool<C, W, F> {
    pub fn new(max_count: u16, factory: F) -> ClassifiedWorkerPoolRef<C, W, F> {
        Arc::new(ClassifiedWorkerPool {
            factory: Arc::new(factory),
            max_count,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                classified_count_map: HashMap::new(),
                worker_list: Vec::with_capacity(max_count as usize),
                waiting_list: Vec::new(),
            }),
        })
    }

    pub async fn get_worker(self: &ClassifiedWorkerPoolRef<C, W, F>) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        let wait = {
            let mut state = self.state.lock().unwrap();

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
                let future = NotifyFuture::new();
                state.waiting_list.push(WaitingItem {
                    future: future.clone(),
                    condition: None,
                });
                Some(future)
            }
        };

        if let Some(wait) = wait {
            wait.await
        } else {
            let worker = match self.factory.create(None).await {
                Ok(worker) => worker,
                Err(err) => {
                    let mut state = self.state.lock().unwrap();
                    state.current_count -= 1;
                    return Err(err)
                },
            };
            let mut state = self.state.lock().unwrap();
            state.inc_classified_count(worker.classification());
            Ok(ClassifiedWorkerGuard::new(worker, self.clone()))
        }
    }

    pub async fn get_classified_worker(self: &ClassifiedWorkerPoolRef<C, W, F>, classification: C) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        let wait = {
            let mut state = self.state.lock().unwrap();

            let old_count = state.worker_list.len() as u16;
            let unwork_classification = state.worker_list.iter().filter(|worker| !worker.is_work()).map(|worker| worker.classification()).collect::<Vec<C>>();
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

            if state.current_count < self.max_count || state.get_classified_count(classification.clone()) == 0 {
                state.current_count += 1;
                None
            } else {
                let future = NotifyFuture::new();
                state.waiting_list.push(WaitingItem {
                    future: future.clone(),
                    condition: Some(classification.clone()),
                });
                Some(future)
            }
        };

        if let Some(wait) = wait {
            wait.await
        } else {
            let worker = match self.factory.create(Some(classification)).await {
                Ok(worker) => worker,
                Err(err) => {
                    let mut state = self.state.lock().unwrap();
                    state.current_count -= 1;
                    return Err(err)
                },
            };
            let mut state = self.state.lock().unwrap();
            state.inc_classified_count(worker.classification());
            Ok(ClassifiedWorkerGuard::new(worker, self.clone()))
        }
    }

    fn release(self: &ClassifiedWorkerPoolRef<C, W, F>, work: W) {
        if work.is_work() {
            let mut state = self.state.lock().unwrap();
            for (index, waiting) in state.waiting_list.iter().enumerate() {
                if waiting.condition.is_none() {
                    let waiting_item = state.waiting_list.remove(index);
                    waiting_item.future.set_complete(Ok(ClassifiedWorkerGuard::new(work, self.clone())));
                    return;
                } else {
                    if work.is_valid(waiting.condition.as_ref().unwrap().clone()) {
                        let waiting_item = state.waiting_list.remove(index);
                        waiting_item.future.set_complete(Ok(ClassifiedWorkerGuard::new(work, self.clone())));
                        return;
                    }
                }
            }
            state.worker_list.push(work);
        } else {
            let mut state = self.state.lock().unwrap();
            let classification = work.classification();
            for (index, waiting) in state.waiting_list.iter().enumerate() {
                if waiting.condition.is_none() {
                    let waiting_item = state.waiting_list.remove(index);
                    let factory = self.factory.clone();
                    let this = self.clone();
                    let classification = classification.clone();
                    tokio::spawn(async move {
                        match factory.create(Some(classification.clone())).await {
                            Ok(worker) => {
                                waiting_item.future.set_complete(Ok(ClassifiedWorkerGuard::new(worker, this)));
                            }
                            Err(err) => {
                                let mut state = this.state.lock().unwrap();
                                state.current_count -= 1;
                                state.dec_classified_count(classification);
                                waiting_item.future.set_complete(Err(err));
                            }
                        }
                    });
                    return;
                } else {
                    if classification == waiting.condition.as_ref().unwrap().clone() {
                        let waiting_item = state.waiting_list.remove(index);
                        let factory = self.factory.clone();
                        let this = self.clone();
                        let classification = classification.clone();
                        tokio::spawn(async move {
                            match factory.create(Some(classification.clone())).await {
                                Ok(worker) => {
                                    waiting_item.future.set_complete(Ok(ClassifiedWorkerGuard::new(worker, this)));
                                }
                                Err(err) => {
                                    let mut state = this.state.lock().unwrap();
                                    state.current_count -= 1;
                                    state.dec_classified_count(classification);
                                    waiting_item.future.set_complete(Err(err));
                                }
                            }
                        });
                        return;
                    }
                }
            }
            state.current_count -= 1;
            state.dec_classified_count(classification);
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
        async fn create(&self, classification: Option<TestWorkerClassification>) -> PoolResult<TestWorker> {
            if let Some(classification) = classification {
                Ok(TestWorker { work: true, classification })
            } else {
                Ok(TestWorker { work: true, classification: TestWorkerClassification::A })
            }
        }
    }

    let pool = ClassifiedWorkerPool::new(2, TestWorkerFactory);
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
        let _worker = pool_ref.get_classified_worker(TestWorkerClassification::B).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    });

    let pool_ref = pool.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let _worker3 = pool_ref.get_classified_worker(TestWorkerClassification::B).await.unwrap();
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
}
