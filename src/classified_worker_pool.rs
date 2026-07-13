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
/// Configuration for [`ClassifiedWorkerPool`].
///
/// All limits default to `None`. Use the `with_*` methods to configure the pool.
pub struct ClassifiedWorkerPoolConfig {
    /// Target maximum number of workers managed by the pool.
    ///
    /// `None` leaves the worker count unlimited. A finite target may be
    /// temporarily exceeded to create a worker for a missing classification.
    max_count: Option<u16>,
    /// Maximum number of idle workers retained across the whole pool.
    ///
    /// `None` adds no separate idle limit; `Some(0)` disables idle caching.
    max_idle_count: Option<u16>,
    idle_timeout: Option<Duration>,
    /// Maximum number of workers whose primary classification is the same.
    ///
    /// `None` leaves classification counts unlimited. This limit is independent
    /// of the pool-wide `max_count` target.
    max_count_per_classification: Option<u16>,
    /// Maximum number of idle workers retained for each primary classification.
    ///
    /// This limit is independent for every classification. `None` adds no
    /// per-classification idle limit; `Some(0)` disables idle caching.
    max_idle_count_per_classification: Option<u16>,
}

impl ClassifiedWorkerPoolConfig {
    /// Sets the pool-wide target worker count.
    ///
    /// `None` leaves the count unlimited. `Some(0)` is invalid.
    pub fn with_max_count(mut self, max_count: Option<u16>) -> Self {
        self.max_count = max_count;
        self
    }

    /// Sets the pool-wide idle-worker cache limit.
    ///
    /// `None` adds no idle limit. `Some(0)` disables idle caching.
    pub fn with_max_idle_count(mut self, max_idle_count: Option<u16>) -> Self {
        self.max_idle_count = max_idle_count;
        self
    }

    /// Sets the maximum duration for which an idle worker is retained.
    ///
    /// `None` disables timeout-based cleanup.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the worker-count limit for each primary classification.
    ///
    /// `None` leaves per-classification counts unlimited. `Some(0)` is invalid.
    pub fn with_max_count_per_classification(mut self, max_count: Option<u16>) -> Self {
        self.max_count_per_classification = max_count;
        self
    }

    /// Sets the idle-worker cache limit for each primary classification.
    ///
    /// `None` adds no per-classification idle limit. `Some(0)` disables idle caching.
    pub fn with_max_idle_count_per_classification(mut self, max_idle_count: Option<u16>) -> Self {
        self.max_idle_count_per_classification = max_idle_count;
        self
    }
}

#[async_trait::async_trait]
/// A classification-aware worker managed by [`ClassifiedWorkerPool`].
///
/// Methods on this trait may be called while the pool's internal state lock is held.
/// Implementations must be non-blocking and must not re-enter APIs on the same pool.
pub trait ClassifiedWorker<C: WorkerClassification>: Send + 'static {
    fn is_work(&self) -> bool;
    /// Returns whether this worker can currently serve the requested classification.
    /// The pool still tracks capacity by the worker's primary `classification()`.
    /// A worker that is no longer valid for its cached primary classification is discarded.
    fn is_valid(&self, c: C) -> bool;
    /// Returns the worker's primary classification used for accounting and replacement.
    /// The pool validates and caches this value when the worker is created.
    /// If it differs from the cached value when the worker is returned, the worker is discarded.
    fn classification(&self) -> C;
}

pub struct ClassifiedWorkerGuard<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    pool_ref: ClassifiedWorkerPoolRef<C, W, F>,
    worker: Option<W>,
    primary_classification: C,
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>>
    ClassifiedWorkerGuard<C, W, F>
{
    fn new(
        worker: W,
        pool_ref: ClassifiedWorkerPoolRef<C, W, F>,
        primary_classification: C,
    ) -> Self {
        ClassifiedWorkerGuard {
            pool_ref,
            worker: Some(worker),
            primary_classification,
        }
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> DerefMut
    for ClassifiedWorkerGuard<C, W, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.worker.as_mut().unwrap()
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

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> Drop
    for ClassifiedWorkerGuard<C, W, F>
{
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool_ref
                .release(worker, self.primary_classification.clone());
        }
    }
}

struct ClassifiedWorkerReservation<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
> {
    pool_ref: ClassifiedWorkerPoolRef<C, W, F>,
    requested_classification: Option<C>,
    active: bool,
}

enum ReservationCompletion {
    Complete,
    Clearing,
    ClassificationLimitReached,
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>>
    ClassifiedWorkerReservation<C, W, F>
{
    fn new(pool_ref: ClassifiedWorkerPoolRef<C, W, F>, classification: Option<C>) -> Self {
        Self {
            pool_ref,
            requested_classification: classification,
            active: true,
        }
    }

    fn complete(mut self, worker_classification: C) -> ReservationCompletion {
        let (completion, retry_waiters, clear_waiters) = {
            let mut state = self.pool_ref.state.lock().unwrap();
            if let Some(classification) = self.requested_classification.as_ref() {
                state.dec_pending_classified_count(classification.clone());
            }
            if state.clearing {
                state.current_count -= 1;
                (
                    ReservationCompletion::Clearing,
                    Vec::new(),
                    state.take_clear_waiters_if_done(),
                )
            } else if self
                .pool_ref
                .classification_limit_reached(&state, &worker_classification)
            {
                state.current_count -= 1;
                (
                    ReservationCompletion::ClassificationLimitReached,
                    state.drain_waiters(),
                    Vec::new(),
                )
            } else {
                state.inc_classified_count(worker_classification);
                (ReservationCompletion::Complete, Vec::new(), Vec::new())
            }
        };
        self.active = false;
        ClassifiedWorkerPool::<C, W, F>::notify_retry_waiters(retry_waiters);
        for waiter in clear_waiters {
            waiter.notify(());
        }
        completion
    }
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>> Drop
    for ClassifiedWorkerReservation<C, W, F>
{
    fn drop(&mut self) {
        if self.active {
            self.pool_ref
                .rollback_reservation(self.requested_classification.as_ref());
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

struct IdleWorker<C, W> {
    worker: W,
    primary_classification: C,
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
    current_count: usize,
    classified_count_map: HashMap<C, usize>,
    pending_classified_count_map: HashMap<C, usize>,
    worker_list: VecDeque<IdleWorker<C, W>>,
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

    fn reserved_classified_count(&self, c: &C) -> usize {
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

    fn remove_canceled_waiters(&mut self) {
        self.waiting_list
            .retain(|waiting| !waiting.future.is_canceled());
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
    config: ClassifiedWorkerPoolConfig,
    state: Mutex<WorkerPoolState<C, W, F>>,
}
pub type ClassifiedWorkerPoolRef<C, W, F> = Arc<ClassifiedWorkerPool<C, W, F>>;

#[cfg(test)]
#[test]
fn test_classified_worker_pool_config_default_idle_limits() {
    let config = ClassifiedWorkerPoolConfig::default();
    assert_eq!(config.max_idle_count, None);
    assert_eq!(config.max_idle_count_per_classification, None);
}

#[cfg(test)]
#[test]
fn test_classified_worker_pool_config_builder() {
    let timeout = Duration::from_secs(1);
    let config = ClassifiedWorkerPoolConfig::default()
        .with_max_count(Some(4))
        .with_max_idle_count(Some(3))
        .with_idle_timeout(Some(timeout))
        .with_max_count_per_classification(Some(2))
        .with_max_idle_count_per_classification(Some(1));
    assert_eq!(config.max_count, Some(4));
    assert_eq!(config.max_idle_count, Some(3));
    assert_eq!(config.idle_timeout, Some(timeout));
    assert_eq!(config.max_count_per_classification, Some(2));
    assert_eq!(config.max_idle_count_per_classification, Some(1));
}

impl<C: WorkerClassification, W: ClassifiedWorker<C>, F: ClassifiedWorkerFactory<C, W>>
    ClassifiedWorkerPool<C, W, F>
{
    fn classification_limit_reached(&self, state: &WorkerPoolState<C, W, F>, c: &C) -> bool {
        self.config
            .max_count_per_classification
            .map(|max_count| state.reserved_classified_count(c) >= usize::from(max_count))
            .unwrap_or(false)
    }

    fn find_replaceable_classified_waiter_index(
        &self,
        state: &WorkerPoolState<C, W, F>,
    ) -> Option<usize> {
        state.waiting_list.iter().position(|waiting| {
            waiting
                .condition
                .as_ref()
                .map(|classification| {
                    !waiting.future.is_canceled()
                        && !self.classification_limit_reached(state, classification)
                })
                .unwrap_or(false)
        })
    }

    fn validate_created_worker(requested_classification: Option<&C>, worker: &W) -> PoolResult<C> {
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
        Ok(worker_classification)
    }

    /// Creates a classification-aware worker pool with explicit configuration.
    ///
    /// A finite `max_count` is a target rather than a strict upper bound. When the
    /// pool is full, no idle worker can be replaced, and a requested classification
    /// has no created or pending worker, the pool may temporarily exceed the target.
    /// Excess workers are removed when they are returned and are not needed by a
    /// waiter. `None` leaves the pool-wide worker count unlimited.
    pub fn new(factory: F, config: ClassifiedWorkerPoolConfig) -> ClassifiedWorkerPoolRef<C, W, F> {
        let idle_capacity = config.max_count.unwrap_or(0) as usize;
        Arc::new(ClassifiedWorkerPool {
            factory: Arc::new(factory),
            config,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                classified_count_map: HashMap::new(),
                pending_classified_count_map: HashMap::new(),
                worker_list: VecDeque::with_capacity(idle_capacity),
                waiting_list: Vec::new(),
                clearing: false,
                clear_waiting_list: Vec::new(),
            }),
        })
    }

    fn take_expired_idle_workers(
        state: &mut WorkerPoolState<C, W, F>,
        idle_timeout: Option<std::time::Duration>,
    ) -> Vec<IdleWorker<C, W>> {
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
            state.dec_classified_count(idle_worker.primary_classification.clone());
            removed_workers.push(idle_worker);
        }
        removed_workers
    }

    pub fn cleanup_idle_worker(&self) -> usize {
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
        let removed_count = removed_workers.len();
        drop(removed_workers);
        removed_count
    }

    pub async fn get_worker(
        self: &ClassifiedWorkerPoolRef<C, W, F>,
    ) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        let mut blocked_classification = None;
        loop {
            if self.config.max_count == Some(0) {
                return Err(pool_invalid_config_error("pool max_count is zero"));
            }
            if self.config.max_count_per_classification == Some(0) {
                return Err(pool_invalid_config_error(
                    "pool max_count_per_classification is zero",
                ));
            }

            let (worker, wait, should_create, removed_workers) = {
                let mut state = self.state.lock().unwrap();
                if state.clearing {
                    return Err(pool_clearing_error());
                }
                state.remove_canceled_waiters();

                let mut removed_workers =
                    Self::take_expired_idle_workers(&mut state, self.config.idle_timeout);

                let worker = loop {
                    let Some(idle_worker) = state.worker_list.pop_back() else {
                        break None;
                    };
                    if !idle_worker.worker.is_work()
                        || idle_worker.worker.classification() != idle_worker.primary_classification
                        || !idle_worker
                            .worker
                            .is_valid(idle_worker.primary_classification.clone())
                    {
                        state.current_count -= 1;
                        state.dec_classified_count(idle_worker.primary_classification.clone());
                        removed_workers.push(idle_worker);
                        continue;
                    }
                    break Some((idle_worker.worker, idle_worker.primary_classification));
                };

                if worker.is_some() {
                    (worker, None, false, removed_workers)
                } else if blocked_classification
                    .as_ref()
                    .map(|classification| self.classification_limit_reached(&state, classification))
                    .unwrap_or(false)
                {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push(WaitingItem {
                        future: notify,
                        condition: None,
                    });
                    (None, Some(waiter), false, removed_workers)
                } else if self
                    .config
                    .max_count
                    .map(|max_count| state.current_count < usize::from(max_count))
                    .unwrap_or(true)
                {
                    state.current_count += 1;
                    (None, None, true, removed_workers)
                } else {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push(WaitingItem {
                        future: notify,
                        condition: None,
                    });
                    (None, Some(waiter), false, removed_workers)
                }
            };

            let reservation =
                should_create.then(|| ClassifiedWorkerReservation::new(self.clone(), None));
            drop(removed_workers);

            if let Some((worker, primary_classification)) = worker {
                return Ok(ClassifiedWorkerGuard::new(
                    worker,
                    self.clone(),
                    primary_classification,
                ));
            }

            if let Some(wait) = wait {
                match wait.await {
                    ClassifiedWorkerWaitResult::Worker(worker) => return Ok(worker),
                    ClassifiedWorkerWaitResult::Retry => continue,
                    ClassifiedWorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let reservation = reservation.unwrap();
            let (worker, primary_classification) = match self.factory.create(None).await {
                Ok(worker) => {
                    let primary_classification = Self::validate_created_worker(None, &worker)?;
                    (worker, primary_classification)
                }
                Err(err) => return Err(err),
            };
            match reservation.complete(primary_classification.clone()) {
                ReservationCompletion::Complete => {}
                ReservationCompletion::Clearing => return Err(pool_cleared_error()),
                ReservationCompletion::ClassificationLimitReached => {
                    blocked_classification = Some(primary_classification);
                    drop(worker);
                    continue;
                }
            }
            return Ok(ClassifiedWorkerGuard::new(
                worker,
                self.clone(),
                primary_classification,
            ));
        }
    }

    pub async fn get_classified_worker(
        self: &ClassifiedWorkerPoolRef<C, W, F>,
        classification: C,
    ) -> PoolResult<ClassifiedWorkerGuard<C, W, F>> {
        loop {
            if self.config.max_count == Some(0) {
                return Err(pool_invalid_config_error("pool max_count is zero"));
            }
            if self.config.max_count_per_classification == Some(0) {
                return Err(pool_invalid_config_error(
                    "pool max_count_per_classification is zero",
                ));
            }

            let (worker, wait, should_create, removed_workers) = {
                let mut state = self.state.lock().unwrap();
                if state.clearing {
                    return Err(pool_clearing_error());
                }
                state.remove_canceled_waiters();

                let mut removed_workers =
                    Self::take_expired_idle_workers(&mut state, self.config.idle_timeout);

                let mut valid_workers = VecDeque::with_capacity(state.worker_list.len());
                while let Some(idle_worker) = state.worker_list.pop_front() {
                    if idle_worker.worker.is_work()
                        && idle_worker.worker.classification() == idle_worker.primary_classification
                        && idle_worker
                            .worker
                            .is_valid(idle_worker.primary_classification.clone())
                    {
                        valid_workers.push_back(idle_worker);
                    } else {
                        state.current_count -= 1;
                        state.dec_classified_count(idle_worker.primary_classification.clone());
                        removed_workers.push(idle_worker);
                    }
                }
                state.worker_list = valid_workers;

                let worker = state
                    .worker_list
                    .iter()
                    .rposition(|idle_worker| idle_worker.worker.is_valid(classification.clone()))
                    .map(|index| {
                        let idle_worker = state.worker_list.remove(index).unwrap();
                        (idle_worker.worker, idle_worker.primary_classification)
                    });

                if worker.is_some() {
                    (worker, None, false, removed_workers)
                } else if self.classification_limit_reached(&state, &classification) {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push(WaitingItem {
                        future: notify,
                        condition: Some(classification.clone()),
                    });
                    (None, Some(waiter), false, removed_workers)
                } else if self
                    .config
                    .max_count
                    .map(|max_count| state.current_count < usize::from(max_count))
                    .unwrap_or(true)
                {
                    state.current_count += 1;
                    state.inc_pending_classified_count(classification.clone());
                    (None, None, true, removed_workers)
                } else if let Some(idle_worker) = state.worker_list.pop_front() {
                    state.dec_classified_count(idle_worker.primary_classification.clone());
                    state.inc_pending_classified_count(classification.clone());
                    removed_workers.push(idle_worker);
                    (None, None, true, removed_workers)
                } else if state.reserved_classified_count(&classification) == 0 {
                    state.current_count += 1;
                    state.inc_pending_classified_count(classification.clone());
                    (None, None, true, removed_workers)
                } else {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push(WaitingItem {
                        future: notify,
                        condition: Some(classification.clone()),
                    });
                    (None, Some(waiter), false, removed_workers)
                }
            };

            let reservation = should_create.then(|| {
                ClassifiedWorkerReservation::new(self.clone(), Some(classification.clone()))
            });
            drop(removed_workers);

            if let Some((worker, primary_classification)) = worker {
                return Ok(ClassifiedWorkerGuard::new(
                    worker,
                    self.clone(),
                    primary_classification,
                ));
            }

            if let Some(wait) = wait {
                match wait.await {
                    ClassifiedWorkerWaitResult::Worker(worker) => return Ok(worker),
                    ClassifiedWorkerWaitResult::Retry => continue,
                    ClassifiedWorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let reservation = reservation.unwrap();
            let (worker, primary_classification) =
                match self.factory.create(Some(classification.clone())).await {
                    Ok(worker) => {
                        let primary_classification =
                            Self::validate_created_worker(Some(&classification), &worker)?;
                        (worker, primary_classification)
                    }
                    Err(err) => return Err(err),
                };
            match reservation.complete(primary_classification.clone()) {
                ReservationCompletion::Complete => {}
                ReservationCompletion::Clearing => return Err(pool_cleared_error()),
                ReservationCompletion::ClassificationLimitReached => {
                    drop(worker);
                    continue;
                }
            }
            return Ok(ClassifiedWorkerGuard::new(
                worker,
                self.clone(),
                primary_classification,
            ));
        }
    }

    pub async fn clear_all_worker(&self) {
        let (waiter, waiting_list, clear_waiters, idle_workers) = {
            let mut state = self.state.lock().unwrap();
            let idle_workers = if !state.clearing {
                state.clearing = true;
                let idle_workers = state.worker_list.drain(..).collect::<Vec<_>>();
                let cur_worker_count = idle_workers.len();
                state.current_count -= cur_worker_count;
                for idle_worker in &idle_workers {
                    state.dec_classified_count(idle_worker.primary_classification.clone());
                }
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
            waiting
                .future
                .notify(ClassifiedWorkerWaitResult::Error(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        drop(idle_workers);
        if let Some(waiter) = waiter {
            waiter.await;
        }
    }

    fn notify_retry_waiters(waiters: Vec<Notify<ClassifiedWorkerWaitResult<C, W, F>>>) {
        for waiter in waiters {
            waiter.notify(ClassifiedWorkerWaitResult::Retry);
        }
    }

    fn rollback_reservation(&self, classification: Option<&C>) {
        let (retry_waiters, clear_waiters) = {
            let mut state = self.state.lock().unwrap();
            state.current_count -= 1;
            if let Some(classification) = classification {
                state.dec_pending_classified_count(classification.clone());
            }
            let retry_waiters = state.drain_waiters();
            let clear_waiters = state.take_clear_waiters_if_done();
            (retry_waiters, clear_waiters)
        };
        Self::notify_retry_waiters(retry_waiters);
        for waiter in clear_waiters {
            waiter.notify(());
        }
    }

    fn release(self: &ClassifiedWorkerPoolRef<C, W, F>, work: W, primary_classification: C) {
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

        let primary_classification_valid = work.classification() == primary_classification
            && work.is_valid(primary_classification.clone());
        let mut clear_waiters = Vec::new();
        let mut removed_workers = Vec::new();
        let action = {
            let mut state = self.state.lock().unwrap();
            state.remove_canceled_waiters();
            if state.clearing {
                state.current_count -= 1;
                state.dec_classified_count(primary_classification);
                clear_waiters = state.take_clear_waiters_if_done();
                ReleaseAction::None
            } else if !primary_classification_valid {
                state.current_count -= 1;
                state.dec_classified_count(primary_classification);
                let waiters = state.drain_waiters();
                if !waiters.is_empty() {
                    ReleaseAction::Retry(waiters)
                } else {
                    ReleaseAction::None
                }
            } else if work.is_work() {
                if let Some(index) = state.find_matching_waiter_index_for_worker(&work) {
                    let waiting_item = state.waiting_list.remove(index);
                    ReleaseAction::Notify(
                        waiting_item.future,
                        ClassifiedWorkerGuard::new(work, self.clone(), primary_classification),
                    )
                } else if let Some(index) = self.find_replaceable_classified_waiter_index(&state) {
                    state.current_count -= 1;
                    state.dec_classified_count(primary_classification);
                    let mut waiters = state.drain_waiters();
                    if index < waiters.len() {
                        waiters.swap(0, index);
                    }
                    ReleaseAction::Retry(waiters)
                } else if self
                    .config
                    .max_count
                    .map(|max_count| state.current_count > usize::from(max_count))
                    .unwrap_or(false)
                {
                    state.current_count -= 1;
                    state.dec_classified_count(primary_classification);
                    clear_waiters = state.take_clear_waiters_if_done();
                    ReleaseAction::None
                } else {
                    state.worker_list.push_back(IdleWorker {
                        worker: work,
                        primary_classification: primary_classification.clone(),
                        idle_since: Instant::now(),
                    });
                    if let Some(max_idle_count_per_classification) =
                        self.config.max_idle_count_per_classification
                    {
                        while state
                            .worker_list
                            .iter()
                            .filter(|idle_worker| {
                                idle_worker.primary_classification == primary_classification
                            })
                            .count()
                            > usize::from(max_idle_count_per_classification)
                        {
                            let index = state
                                .worker_list
                                .iter()
                                .position(|idle_worker| {
                                    idle_worker.primary_classification == primary_classification
                                })
                                .unwrap();
                            let idle_worker = state.worker_list.remove(index).unwrap();
                            state.current_count -= 1;
                            state.dec_classified_count(idle_worker.primary_classification.clone());
                            removed_workers.push(idle_worker);
                        }
                    }
                    if let Some(max_idle_count) = self.config.max_idle_count {
                        while state.worker_list.len() > usize::from(max_idle_count) {
                            let idle_worker = state.worker_list.pop_front().unwrap();
                            state.current_count -= 1;
                            state.dec_classified_count(idle_worker.primary_classification.clone());
                            removed_workers.push(idle_worker);
                        }
                    }
                    ReleaseAction::None
                }
            } else {
                state.dec_classified_count(primary_classification);
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
        drop(removed_workers);

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

#[cfg(test)]
mod idle_limit_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug, Eq, Hash, PartialEq)]
    enum Classification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        classification: Classification,
    }

    impl ClassifiedWorker<Classification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, classification: Classification) -> bool {
            self.classification == classification
        }

        fn classification(&self) -> Classification {
            self.classification.clone()
        }
    }

    struct TestFactory(AtomicUsize);

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<Classification, TestWorker> for TestFactory {
        async fn create(&self, classification: Option<Classification>) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                id: self.0.fetch_add(1, Ordering::SeqCst),
                classification: classification.unwrap_or(Classification::A),
            })
        }
    }

    #[tokio::test]
    async fn classification_and_global_idle_limits_preserve_mru_workers() {
        let pool = ClassifiedWorkerPool::new(
            TestFactory(AtomicUsize::new(0)),
            ClassifiedWorkerPoolConfig {
                max_count: Some(3),
                max_idle_count: Some(2),
                idle_timeout: None,
                max_count_per_classification: None,
                max_idle_count_per_classification: Some(1),
            },
        );
        let a0 = pool.get_classified_worker(Classification::A).await.unwrap();
        let a1 = pool.get_classified_worker(Classification::A).await.unwrap();
        let b2 = pool.get_classified_worker(Classification::B).await.unwrap();
        drop(a0);
        drop(b2);
        drop(a1);

        let a = pool.get_classified_worker(Classification::A).await.unwrap();
        let b = pool.get_classified_worker(Classification::B).await.unwrap();
        assert_eq!(a.id, 1);
        assert_eq!(b.id, 2);
    }

    #[tokio::test]
    async fn zero_per_classification_idle_limit_disables_idle_cache() {
        let pool = ClassifiedWorkerPool::new(
            TestFactory(AtomicUsize::new(0)),
            ClassifiedWorkerPoolConfig {
                max_count: Some(2),
                max_idle_count_per_classification: Some(0),
                ..Default::default()
            },
        );
        let a = pool.get_classified_worker(Classification::A).await.unwrap();
        assert_eq!(a.id, 0);
        drop(a);
        let a = pool.get_classified_worker(Classification::A).await.unwrap();
        assert_eq!(a.id, 1);
    }
}

#[cfg(test)]
fn new_classified_worker_pool<
    C: WorkerClassification,
    W: ClassifiedWorker<C>,
    F: ClassifiedWorkerFactory<C, W>,
>(
    max_count: u16,
    factory: F,
    mut config: ClassifiedWorkerPoolConfig,
) -> ClassifiedWorkerPoolRef<C, W, F> {
    config.max_count = Some(max_count);
    ClassifiedWorkerPool::new(factory, config)
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(3),
            ..Default::default()
        },
    );

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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(1),
            ..Default::default()
        },
    );
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(0),
            ..Default::default()
        },
    );
    let worker = pool.get_worker().await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn test_default_config_has_no_max_count() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker;

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, _classification: TestWorkerClassification) -> bool {
            true
        }

        fn classification(&self) -> TestWorkerClassification {
            TestWorkerClassification::A
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            _classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            Ok(TestWorker)
        }
    }

    let pool = ClassifiedWorkerPool::new(TestWorkerFactory, Default::default());
    let worker1 = pool.get_worker().await.unwrap();
    let worker2 = pool.get_worker().await.unwrap();

    assert_eq!(pool.state.lock().unwrap().current_count, 2);

    drop(worker1);
    drop(worker2);
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(1),
            ..Default::default()
        },
    );
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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(1),
            ..Default::default()
        },
    );

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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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
    let pool = new_classified_worker_pool(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        work: AtomicBool,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            self.work.load(Ordering::SeqCst)
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
                work: AtomicBool::new(true),
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_classified_worker_pool(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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

    worker_a.work.store(false, Ordering::SeqCst);
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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(1),
            ..Default::default()
        },
    );
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

    let pool = ClassifiedWorkerPool::new(
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: Some(1),
            ..Default::default()
        },
    );
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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        ClassifiedWorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..Default::default()
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
    let pool = new_classified_worker_pool(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        Default::default(),
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
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        ClassifiedWorkerPoolConfig {
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..Default::default()
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

#[tokio::test]
async fn test_canceled_classified_create_rolls_back_reservation() {
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
    }

    struct TestWorker;

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, _classification: TestWorkerClassification) -> bool {
            true
        }

        fn classification(&self) -> TestWorkerClassification {
            TestWorkerClassification::A
        }
    }

    struct TestWorkerFactory {
        create_started: Arc<AtomicBool>,
        allow_create: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<TestWorkerClassification, TestWorker> for TestWorkerFactory {
        async fn create(
            &self,
            _classification: Option<TestWorkerClassification>,
        ) -> PoolResult<TestWorker> {
            self.create_started.store(true, Ordering::SeqCst);
            while !self.allow_create.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(TestWorker)
        }
    }

    let create_started = Arc::new(AtomicBool::new(false));
    let allow_create = Arc::new(AtomicBool::new(false));
    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory {
            create_started: create_started.clone(),
            allow_create: allow_create.clone(),
        },
        Default::default(),
    );

    let pool_ref = pool.clone();
    let create_task = tokio::spawn(async move {
        pool_ref
            .get_classified_worker(TestWorkerClassification::A)
            .await
    });
    while !create_started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    create_task.abort();
    assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

    allow_create.store(true, Ordering::SeqCst);
    let worker = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pool.get_classified_worker(TestWorkerClassification::A),
    )
    .await
    .unwrap()
    .unwrap();
    drop(worker);

    tokio::time::timeout(std::time::Duration::from_secs(1), pool.clear_all_worker())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_mutating_worker_classification_removes_returned_worker() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerClassification {
        A,
        B,
    }

    struct TestWorker {
        work: bool,
        classification: TestWorkerClassification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<TestWorkerClassification> for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }

        fn is_valid(&self, classification: TestWorkerClassification) -> bool {
            self.classification == classification
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
                work: true,
                classification: classification.unwrap_or(TestWorkerClassification::A),
            })
        }
    }

    let pool = new_classified_worker_pool(
        1,
        TestWorkerFactory,
        ClassifiedWorkerPoolConfig {
            max_count: None,
            idle_timeout: None,
            max_count_per_classification: Some(1),
            ..Default::default()
        },
    );
    let mut worker = pool
        .get_classified_worker(TestWorkerClassification::A)
        .await
        .unwrap();
    worker.classification = TestWorkerClassification::B;
    drop(worker);

    {
        let state = pool.state.lock().unwrap();
        assert_eq!(state.current_count, 0);
        assert!(state.classified_count_map.is_empty());
    }

    let worker = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pool.get_classified_worker(TestWorkerClassification::A),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(worker.classification(), TestWorkerClassification::A);
}

#[cfg(test)]
mod affected_path_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum Classification {
        A,
        B,
        C,
    }

    struct BlockingWorker {
        classification: Classification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<Classification> for BlockingWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn is_valid(&self, classification: Classification) -> bool {
            self.classification == classification
        }

        fn classification(&self) -> Classification {
            self.classification.clone()
        }
    }

    struct BlockingFactory {
        create_started: Arc<AtomicBool>,
        allow_create: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<Classification, BlockingWorker> for BlockingFactory {
        async fn create(
            &self,
            classification: Option<Classification>,
        ) -> PoolResult<BlockingWorker> {
            self.create_started.store(true, Ordering::SeqCst);
            while !self.allow_create.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(BlockingWorker {
                classification: classification.unwrap_or(Classification::A),
            })
        }
    }

    async fn wait_for_create(create_started: &AtomicBool) {
        while !create_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    }

    fn new_blocking_pool(
        max_count: u16,
    ) -> (
        ClassifiedWorkerPoolRef<Classification, BlockingWorker, BlockingFactory>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let create_started = Arc::new(AtomicBool::new(false));
        let allow_create = Arc::new(AtomicBool::new(false));
        let pool = new_classified_worker_pool(
            max_count,
            BlockingFactory {
                create_started: create_started.clone(),
                allow_create: allow_create.clone(),
            },
            Default::default(),
        );
        (pool, create_started, allow_create)
    }

    #[tokio::test]
    async fn test_canceled_generic_create_rolls_back_classified_pool_reservation() {
        let (pool, create_started, allow_create) = new_blocking_pool(1);
        let pool_ref = pool.clone();
        let create_task = tokio::spawn(async move { pool_ref.get_worker().await });
        wait_for_create(&create_started).await;
        create_task.abort();
        assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

        allow_create.store(true, Ordering::SeqCst);
        let worker = tokio::time::timeout(Duration::from_secs(1), pool.get_worker())
            .await
            .unwrap()
            .unwrap();
        drop(worker);
        tokio::time::timeout(Duration::from_secs(1), pool.clear_all_worker())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_canceled_replacement_create_rolls_back_classified_pool_reservation() {
        let (pool, create_started, allow_create) = new_blocking_pool(1);
        allow_create.store(true, Ordering::SeqCst);
        let worker = pool.get_classified_worker(Classification::A).await.unwrap();
        drop(worker);

        create_started.store(false, Ordering::SeqCst);
        allow_create.store(false, Ordering::SeqCst);
        let pool_ref = pool.clone();
        let create_task =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::B).await });
        wait_for_create(&create_started).await;
        create_task.abort();
        assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

        allow_create.store(true, Ordering::SeqCst);
        let worker = tokio::time::timeout(
            Duration::from_secs(1),
            pool.get_classified_worker(Classification::B),
        )
        .await
        .unwrap()
        .unwrap();
        drop(worker);
        tokio::time::timeout(Duration::from_secs(1), pool.clear_all_worker())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_canceled_overcommit_create_rolls_back_classified_pool_reservation() {
        let (pool, create_started, allow_create) = new_blocking_pool(1);
        allow_create.store(true, Ordering::SeqCst);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();

        create_started.store(false, Ordering::SeqCst);
        allow_create.store(false, Ordering::SeqCst);
        let pool_ref = pool.clone();
        let create_task =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::B).await });
        wait_for_create(&create_started).await;
        create_task.abort();
        assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 1);
            assert_eq!(state.reserved_classified_count(&Classification::B), 0);
        }

        allow_create.store(true, Ordering::SeqCst);
        let worker_b = tokio::time::timeout(
            Duration::from_secs(1),
            pool.get_classified_worker(Classification::B),
        )
        .await
        .unwrap()
        .unwrap();
        drop(worker_b);
        drop(worker_a);
        tokio::time::timeout(Duration::from_secs(1), pool.clear_all_worker())
            .await
            .unwrap();
    }

    type DropCallback = Box<dyn FnOnce() + Send>;

    struct DropProbeWorker {
        working: Arc<AtomicBool>,
        classification: Classification,
        on_drop: Option<DropCallback>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<Classification> for DropProbeWorker {
        fn is_work(&self) -> bool {
            self.working.load(Ordering::SeqCst)
        }

        fn is_valid(&self, classification: Classification) -> bool {
            self.classification == classification
        }

        fn classification(&self) -> Classification {
            self.classification.clone()
        }
    }

    impl Drop for DropProbeWorker {
        fn drop(&mut self) {
            if let Some(on_drop) = self.on_drop.take() {
                on_drop();
            }
        }
    }

    struct DropProbeSpec {
        working: Arc<AtomicBool>,
        on_drop: Option<DropCallback>,
    }

    struct DropProbeFactory {
        specs: Arc<Mutex<VecDeque<DropProbeSpec>>>,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<Classification, DropProbeWorker> for DropProbeFactory {
        async fn create(
            &self,
            classification: Option<Classification>,
        ) -> PoolResult<DropProbeWorker> {
            let spec = self.specs.lock().unwrap().pop_front().unwrap();
            Ok(DropProbeWorker {
                working: spec.working,
                classification: classification.unwrap_or(Classification::A),
                on_drop: spec.on_drop,
            })
        }
    }

    type DropProbePool = ClassifiedWorkerPoolRef<Classification, DropProbeWorker, DropProbeFactory>;

    fn new_drop_probe_pool(
        idle_timeout: Option<Duration>,
        max_idle_count: Option<u16>,
    ) -> (DropProbePool, Arc<Mutex<VecDeque<DropProbeSpec>>>) {
        let specs = Arc::new(Mutex::new(VecDeque::new()));
        let pool = new_classified_worker_pool(
            1,
            DropProbeFactory {
                specs: specs.clone(),
            },
            ClassifiedWorkerPoolConfig {
                max_idle_count,
                idle_timeout,
                ..Default::default()
            },
        );
        (pool, specs)
    }

    fn drop_lock_probe(
        pool: &DropProbePool,
        working: Arc<AtomicBool>,
    ) -> (DropProbeSpec, mpsc::Receiver<bool>) {
        let (tx, rx) = mpsc::channel();
        let pool_ref = Arc::downgrade(pool);
        let on_drop = Box::new(move || {
            let pool_ref = pool_ref.upgrade().unwrap();
            tx.send(pool_ref.state.try_lock().is_ok()).unwrap();
        });
        (
            DropProbeSpec {
                working,
                on_drop: Some(on_drop),
            },
            rx,
        )
    }

    fn plain_drop_probe_spec() -> DropProbeSpec {
        DropProbeSpec {
            working: Arc::new(AtomicBool::new(true)),
            on_drop: None,
        }
    }

    #[derive(Copy, Clone)]
    enum IdleDropPath {
        Cleanup,
        IdleLimit,
        GenericInvalidScan,
        ClassifiedInvalidScan,
        ClassifiedReplacement,
        Clear,
    }

    async fn assert_idle_drop_path_runs_outside_lock(path: IdleDropPath) {
        let idle_timeout = matches!(path, IdleDropPath::Cleanup).then_some(Duration::ZERO);
        let max_idle_count = matches!(path, IdleDropPath::IdleLimit).then_some(0);
        let (pool, specs) = new_drop_probe_pool(idle_timeout, max_idle_count);
        let working = Arc::new(AtomicBool::new(true));
        let (spec, drop_result) = drop_lock_probe(&pool, working.clone());
        specs.lock().unwrap().push_back(spec);

        let worker = pool.get_classified_worker(Classification::A).await.unwrap();
        drop(worker);

        match path {
            IdleDropPath::Cleanup => {
                assert_eq!(pool.cleanup_idle_worker(), 1);
            }
            IdleDropPath::IdleLimit => {
                // The first return above evicts immediately from the zero-cap pool.
            }
            IdleDropPath::GenericInvalidScan => {
                working.store(false, Ordering::SeqCst);
                specs.lock().unwrap().push_back(plain_drop_probe_spec());
                let worker = pool.get_worker().await.unwrap();
                drop(worker);
            }
            IdleDropPath::ClassifiedInvalidScan => {
                working.store(false, Ordering::SeqCst);
                specs.lock().unwrap().push_back(plain_drop_probe_spec());
                let worker = pool.get_classified_worker(Classification::A).await.unwrap();
                drop(worker);
            }
            IdleDropPath::ClassifiedReplacement => {
                specs.lock().unwrap().push_back(plain_drop_probe_spec());
                let worker = pool.get_classified_worker(Classification::B).await.unwrap();
                drop(worker);
            }
            IdleDropPath::Clear => pool.clear_all_worker().await,
        }

        assert!(drop_result.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    #[tokio::test]
    async fn test_all_classified_idle_drop_paths_run_outside_state_lock() {
        for path in [
            IdleDropPath::Cleanup,
            IdleDropPath::IdleLimit,
            IdleDropPath::GenericInvalidScan,
            IdleDropPath::ClassifiedInvalidScan,
            IdleDropPath::ClassifiedReplacement,
            IdleDropPath::Clear,
        ] {
            assert_idle_drop_path_runs_outside_lock(path).await;
        }
    }

    struct MutableWorker {
        working: Arc<AtomicBool>,
        valid: Arc<AtomicBool>,
        classification: Classification,
    }

    #[async_trait::async_trait]
    impl ClassifiedWorker<Classification> for MutableWorker {
        fn is_work(&self) -> bool {
            self.working.load(Ordering::SeqCst)
        }

        fn is_valid(&self, classification: Classification) -> bool {
            self.valid.load(Ordering::SeqCst) && self.classification == classification
        }

        fn classification(&self) -> Classification {
            self.classification.clone()
        }
    }

    struct MutableWorkerFactory;

    #[async_trait::async_trait]
    impl ClassifiedWorkerFactory<Classification, MutableWorker> for MutableWorkerFactory {
        async fn create(
            &self,
            classification: Option<Classification>,
        ) -> PoolResult<MutableWorker> {
            Ok(MutableWorker {
                working: Arc::new(AtomicBool::new(true)),
                valid: Arc::new(AtomicBool::new(true)),
                classification: classification.unwrap_or(Classification::A),
            })
        }
    }

    type MutablePool = ClassifiedWorkerPoolRef<Classification, MutableWorker, MutableWorkerFactory>;

    fn new_mutable_pool(max_count: u16, idle_timeout: Option<Duration>) -> MutablePool {
        new_classified_worker_pool(
            max_count,
            MutableWorkerFactory,
            ClassifiedWorkerPoolConfig {
                idle_timeout,
                ..Default::default()
            },
        )
    }

    fn new_limited_mutable_pool(max_count: u16, max_count_per_classification: u16) -> MutablePool {
        new_classified_worker_pool(
            max_count,
            MutableWorkerFactory,
            ClassifiedWorkerPoolConfig {
                max_count: None,
                idle_timeout: None,
                max_count_per_classification: Some(max_count_per_classification),
                ..Default::default()
            },
        )
    }

    fn assert_accounting_empty(pool: &MutablePool) {
        let state = pool.state.lock().unwrap();
        assert_eq!(state.current_count, 0);
        assert!(state.classified_count_map.is_empty());
        assert!(state.pending_classified_count_map.is_empty());
    }

    fn assert_only_classification(
        pool: &MutablePool,
        classification: Classification,
        count: usize,
    ) {
        let state = pool.state.lock().unwrap();
        assert_eq!(state.current_count, count);
        assert_eq!(state.classified_count_map.len(), 1);
        assert_eq!(
            state.classified_count_map.get(&classification).copied(),
            Some(count)
        );
    }

    #[derive(Copy, Clone)]
    enum IdleAccountingPath {
        Cleanup,
        Clear,
        GenericInvalidScan,
        ClassifiedInvalidScan,
        ClassifiedReplacement,
    }

    async fn assert_idle_accounting_path(path: IdleAccountingPath) {
        let idle_timeout = matches!(path, IdleAccountingPath::Cleanup).then_some(Duration::ZERO);
        let pool = new_mutable_pool(1, idle_timeout);
        let worker = pool.get_classified_worker(Classification::A).await.unwrap();
        let working = worker.working.clone();
        drop(worker);

        match path {
            IdleAccountingPath::Cleanup => {
                assert_eq!(pool.cleanup_idle_worker(), 1);
                assert_accounting_empty(&pool);
            }
            IdleAccountingPath::Clear => {
                pool.clear_all_worker().await;
                assert_accounting_empty(&pool);
            }
            IdleAccountingPath::GenericInvalidScan => {
                working.store(false, Ordering::SeqCst);
                let worker = pool.get_worker().await.unwrap();
                assert_only_classification(&pool, Classification::A, 1);
                worker.working.store(false, Ordering::SeqCst);
                drop(worker);
                assert_accounting_empty(&pool);
            }
            IdleAccountingPath::ClassifiedInvalidScan => {
                working.store(false, Ordering::SeqCst);
                let worker = pool.get_classified_worker(Classification::A).await.unwrap();
                assert_only_classification(&pool, Classification::A, 1);
                worker.working.store(false, Ordering::SeqCst);
                drop(worker);
                assert_accounting_empty(&pool);
            }
            IdleAccountingPath::ClassifiedReplacement => {
                let worker = pool.get_classified_worker(Classification::B).await.unwrap();
                assert_only_classification(&pool, Classification::B, 1);
                worker.working.store(false, Ordering::SeqCst);
                drop(worker);
                assert_accounting_empty(&pool);
            }
        }
    }

    #[tokio::test]
    async fn test_all_idle_accounting_paths() {
        for path in [
            IdleAccountingPath::Cleanup,
            IdleAccountingPath::Clear,
            IdleAccountingPath::GenericInvalidScan,
            IdleAccountingPath::ClassifiedInvalidScan,
            IdleAccountingPath::ClassifiedReplacement,
        ] {
            assert_idle_accounting_path(path).await;
        }
    }

    async fn wait_for_waiter(pool: &MutablePool) {
        loop {
            if !pool.state.lock().unwrap().waiting_list.is_empty() {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn test_canceled_waiter_is_removed_when_worker_returns() {
        let pool = new_limited_mutable_pool(1, 1);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();

        let pool_ref = pool.clone();
        let waiter =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::A).await });
        wait_for_waiter(&pool).await;
        waiter.abort();
        assert!(matches!(waiter.await, Err(err) if err.is_cancelled()));

        drop(worker_a);

        let state = pool.state.lock().unwrap();
        assert!(state.waiting_list.is_empty());
        assert_eq!(state.worker_list.len(), 1);
    }

    #[tokio::test]
    async fn test_worker_invalid_for_primary_classification_wakes_waiter() {
        let pool = new_limited_mutable_pool(1, 1);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();
        let valid = worker_a.valid.clone();

        let pool_ref = pool.clone();
        let waiting_a =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::A).await });
        wait_for_waiter(&pool).await;

        valid.store(false, Ordering::SeqCst);
        drop(worker_a);

        let replacement_a = tokio::time::timeout(Duration::from_secs(1), waiting_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.classification(), Classification::A);
    }

    #[tokio::test]
    async fn test_idle_worker_invalid_for_primary_classification_is_replaced() {
        let pool = new_limited_mutable_pool(1, 1);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();
        let valid = worker_a.valid.clone();
        drop(worker_a);

        valid.store(false, Ordering::SeqCst);

        let replacement_a = tokio::time::timeout(
            Duration::from_secs(1),
            pool.get_classified_worker(Classification::A),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(replacement_a.classification(), Classification::A);
    }

    #[tokio::test]
    async fn test_capped_waiter_does_not_replace_other_classification_worker() {
        let pool = new_limited_mutable_pool(2, 1);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();
        let worker_b = pool.get_classified_worker(Classification::B).await.unwrap();

        let pool_ref = pool.clone();
        let waiting_a =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::A).await });
        wait_for_waiter(&pool).await;

        drop(worker_b);
        tokio::task::yield_now().await;

        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 2);
            assert_eq!(state.worker_list.len(), 1);
            assert_eq!(
                state.worker_list.front().unwrap().primary_classification,
                Classification::B
            );
        }
        assert!(!waiting_a.is_finished());

        drop(worker_a);
        let replacement_a = tokio::time::timeout(Duration::from_secs(1), waiting_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.classification(), Classification::A);
    }

    #[tokio::test]
    async fn test_changed_classification_worker_is_removed_and_generic_waiter_retries() {
        let pool = new_mutable_pool(1, None);
        let mut worker = pool.get_classified_worker(Classification::A).await.unwrap();
        worker.classification = Classification::B;

        let pool_ref = pool.clone();
        let waiter = tokio::spawn(async move { pool_ref.get_worker().await });
        wait_for_waiter(&pool).await;
        drop(worker);

        let worker = waiter.await.unwrap().unwrap();
        assert_eq!(worker.classification(), Classification::A);
        worker.working.store(false, Ordering::SeqCst);
        drop(worker);
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_cached_classification_is_used_while_clearing() {
        let pool = new_mutable_pool(1, None);
        let mut worker = pool.get_classified_worker(Classification::A).await.unwrap();
        worker.classification = Classification::B;

        let pool_ref = pool.clone();
        let clear_task = tokio::spawn(async move { pool_ref.clear_all_worker().await });
        loop {
            if pool.state.lock().unwrap().clearing {
                break;
            }
            tokio::task::yield_now().await;
        }
        drop(worker);
        clear_task.await.unwrap();
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_changed_classification_worker_wakes_classified_waiter() {
        let pool = new_mutable_pool(2, None);
        let mut worker_a = pool.get_classified_worker(Classification::A).await.unwrap();
        let worker_b = pool.get_classified_worker(Classification::B).await.unwrap();
        worker_a.classification = Classification::C;

        let pool_ref = pool.clone();
        let waiter =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::B).await });
        wait_for_waiter(&pool).await;
        drop(worker_a);

        let replacement_b = waiter.await.unwrap().unwrap();
        assert_only_classification(&pool, Classification::B, 2);
        replacement_b.working.store(false, Ordering::SeqCst);
        worker_b.working.store(false, Ordering::SeqCst);
        drop(replacement_b);
        drop(worker_b);
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_changed_classification_overcommit_worker_is_removed() {
        let pool = new_mutable_pool(1, None);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();
        let mut worker_b = pool.get_classified_worker(Classification::B).await.unwrap();
        worker_b.classification = Classification::C;
        drop(worker_b);

        assert_only_classification(&pool, Classification::A, 1);
        worker_a.working.store(false, Ordering::SeqCst);
        drop(worker_a);
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_max_count_per_classification_blocks_only_that_classification() {
        let pool = new_limited_mutable_pool(4, 2);
        let worker_a1 = pool.get_classified_worker(Classification::A).await.unwrap();
        let worker_a2 = pool.get_classified_worker(Classification::A).await.unwrap();

        let pool_ref = pool.clone();
        let waiting_a =
            tokio::spawn(async move { pool_ref.get_classified_worker(Classification::A).await });
        wait_for_waiter(&pool).await;
        assert!(!waiting_a.is_finished());

        let worker_b = pool.get_classified_worker(Classification::B).await.unwrap();
        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 3);
            assert_eq!(state.reserved_classified_count(&Classification::A), 2);
            assert_eq!(state.reserved_classified_count(&Classification::B), 1);
        }

        drop(worker_a1);
        let replacement_a = tokio::time::timeout(Duration::from_secs(1), waiting_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.classification(), Classification::A);

        drop(worker_a2);
        drop(replacement_a);
        drop(worker_b);
    }

    #[tokio::test]
    async fn test_generic_get_does_not_exceed_classification_limit() {
        let pool = new_limited_mutable_pool(3, 1);
        let worker_a = pool.get_classified_worker(Classification::A).await.unwrap();

        let pool_ref = pool.clone();
        let generic = tokio::spawn(async move { pool_ref.get_worker().await });
        wait_for_waiter(&pool).await;
        assert!(!generic.is_finished());
        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 1);
            assert_eq!(state.reserved_classified_count(&Classification::A), 1);
        }

        let worker_b = pool.get_classified_worker(Classification::B).await.unwrap();
        drop(worker_b);
        let generic_worker = tokio::time::timeout(Duration::from_secs(1), generic)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(generic_worker.classification(), Classification::B);

        drop(worker_a);
        drop(generic_worker);
    }

    #[tokio::test]
    async fn test_zero_max_count_per_classification_returns_error() {
        let pool = new_limited_mutable_pool(1, 0);

        let generic_error = pool.get_worker().await.err().unwrap();
        assert_eq!(generic_error.code(), crate::PoolErrorCode::InvalidConfig);

        let classified_error = pool
            .get_classified_worker(Classification::A)
            .await
            .err()
            .unwrap();
        assert_eq!(classified_error.code(), crate::PoolErrorCode::InvalidConfig);
    }
}
