use crate::{
    pool_cleared_error, pool_clearing_error, pool_invalid_config_error, PoolError, PoolResult,
};
use notify_future::Notify;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub trait WorkerKey: Send + 'static + Clone + Hash + Eq + PartialEq {}

impl<T: Send + 'static + Clone + Hash + Eq + PartialEq> WorkerKey for T {}

#[derive(Debug, Clone, Default)]
/// Configuration for [`KeyedWorkerPool`].
///
/// All options default to `None`. Use the `with_*` methods to configure the pool.
pub struct KeyedWorkerPoolConfig {
    idle_timeout: Option<Duration>,
    /// Maximum number of workers whose primary key is the same.
    ///
    /// `None` leaves key counts unlimited.
    max_count_per_key: Option<u16>,
    /// Maximum number of idle workers retained for each primary key.
    ///
    /// This limit is independent for every key. `None` adds no per-key idle
    /// limit; `Some(0)` disables idle caching for every key.
    max_idle_count_per_key: Option<u16>,
}

impl KeyedWorkerPoolConfig {
    /// Sets the maximum duration for which an idle worker is retained.
    ///
    /// `None` disables timeout-based cleanup.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the worker-count limit for each primary key.
    ///
    /// `None` leaves per-key counts unlimited. `Some(0)` is invalid.
    pub fn with_max_count_per_key(mut self, max_count: Option<u16>) -> Self {
        self.max_count_per_key = max_count;
        self
    }

    /// Sets the idle-worker cache limit for each primary key.
    ///
    /// `None` adds no per-key idle limit. `Some(0)` disables idle caching.
    pub fn with_max_idle_count_per_key(mut self, max_idle_count: Option<u16>) -> Self {
        self.max_idle_count_per_key = max_idle_count;
        self
    }
}

#[async_trait::async_trait]
/// A keyed worker managed by [`KeyedWorkerPool`].
///
/// Methods on this trait may be called while the pool's internal state lock is held.
/// Implementations must be non-blocking and must not re-enter APIs on the same pool.
pub trait KeyedWorker<K: WorkerKey>: Send + 'static {
    fn is_work(&self) -> bool;
    /// Returns whether this worker can currently serve its requested primary key.
    /// Explicit keyed acquisition only reuses workers from that key's primary bucket.
    /// A worker that is no longer valid for its cached primary key is discarded.
    fn supports(&self, key: K) -> bool;
    /// Returns the worker's primary key used for accounting and replacement.
    /// The pool validates and caches this value when the worker is created.
    /// If it differs from the cached value when the worker is returned, the worker is discarded.
    fn primary_key(&self) -> K;
}

pub struct KeyedWorkerGuard<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
    pool_ref: KeyedWorkerPoolRef<K, W, F>,
    worker: Option<W>,
    primary_key: K,
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> KeyedWorkerGuard<K, W, F> {
    fn new(worker: W, pool_ref: KeyedWorkerPoolRef<K, W, F>, primary_key: K) -> Self {
        KeyedWorkerGuard {
            pool_ref,
            worker: Some(worker),
            primary_key,
        }
    }
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> DerefMut
    for KeyedWorkerGuard<K, W, F>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.worker.as_mut().unwrap()
    }
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> Deref
    for KeyedWorkerGuard<K, W, F>
{
    type Target = W;

    fn deref(&self) -> &Self::Target {
        self.worker.as_ref().unwrap()
    }
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> Drop
    for KeyedWorkerGuard<K, W, F>
{
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool_ref.release(worker, self.primary_key.clone());
        }
    }
}

struct KeyedWorkerReservation<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
    pool_ref: KeyedWorkerPoolRef<K, W, F>,
    requested_key: K,
    active: bool,
}

enum ReservationCompletion {
    Complete,
    Clearing,
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> KeyedWorkerReservation<K, W, F> {
    fn new(pool_ref: KeyedWorkerPoolRef<K, W, F>, key: K) -> Self {
        Self {
            pool_ref,
            requested_key: key,
            active: true,
        }
    }

    fn complete(mut self, worker_key: K) -> ReservationCompletion {
        let (completion, clear_waiters) = {
            let mut state = self.pool_ref.state.lock().unwrap();
            state.dec_pending_count_for_key(self.requested_key.clone());
            if state.clearing {
                state.current_count -= 1;
                (
                    ReservationCompletion::Clearing,
                    state.take_clear_waiters_if_done(),
                )
            } else {
                state.inc_worker_count_for_key(worker_key);
                (ReservationCompletion::Complete, Vec::new())
            }
        };
        self.active = false;
        for waiter in clear_waiters {
            waiter.notify(());
        }
        completion
    }
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> Drop
    for KeyedWorkerReservation<K, W, F>
{
    fn drop(&mut self) {
        if self.active {
            self.pool_ref.rollback_reservation(&self.requested_key);
        }
    }
}

#[async_trait::async_trait]
pub trait KeyedWorkerFactory<K: WorkerKey, W: KeyedWorker<K>>: Send + Sync + 'static {
    /// Creates a usable worker for `key`.
    ///
    /// Returning `Ok` asserts that the worker is ready for use. Its primary
    /// key must be `key`.
    async fn create(&self, key: K) -> PoolResult<W>;
}

struct WaitingItem<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
    future: Notify<KeyedWorkerWaitResult<K, W, F>>,
    key: K,
}

struct IdleWorker<K, W> {
    worker: W,
    primary_key: K,
    idle_since: Instant,
}

enum KeyedWorkerWaitResult<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
    Worker(KeyedWorkerGuard<K, W, F>),
    Retry,
    Error(PoolError),
}

struct WorkerPoolState<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
    current_count: usize,
    worker_count_by_key: HashMap<K, usize>,
    pending_count_by_key: HashMap<K, usize>,
    // Idle workers are grouped by primary key; each bucket is ordered from LRU to MRU.
    worker_list: HashMap<K, VecDeque<IdleWorker<K, W>>>,
    waiting_list: Vec<WaitingItem<K, W, F>>,
    clearing: bool,
    clear_waiting_list: Vec<Notify<()>>,
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> WorkerPoolState<K, W, F> {
    fn push_idle_worker(&mut self, worker: W, primary_key: K) {
        let idle_worker = IdleWorker {
            worker,
            primary_key: primary_key.clone(),
            idle_since: Instant::now(),
        };
        self.worker_list
            .entry(primary_key)
            .or_default()
            .push_back(idle_worker);
    }

    fn remove_idle_worker(&mut self, primary_key: &K, index: usize) -> IdleWorker<K, W> {
        let (idle_worker, remove_bucket) = {
            let workers = self.worker_list.get_mut(primary_key).unwrap();
            let idle_worker = workers.remove(index).unwrap();
            (idle_worker, workers.is_empty())
        };
        if remove_bucket {
            self.worker_list.remove(primary_key);
        }
        idle_worker
    }

    fn take_idle_worker_for_key(&mut self, key: &K) -> Option<IdleWorker<K, W>> {
        let index = self
            .worker_list
            .get(key)
            .map(VecDeque::len)
            .and_then(|len| len.checked_sub(1))?;
        Some(self.remove_idle_worker(key, index))
    }

    fn drain_idle_workers(&mut self) -> Vec<IdleWorker<K, W>> {
        std::mem::take(&mut self.worker_list)
            .into_values()
            .flatten()
            .collect()
    }

    fn inc_worker_count_for_key(&mut self, key: K) {
        let count = self.worker_count_by_key.entry(key).or_insert(0);
        *count += 1;
    }

    fn dec_worker_count_for_key(&mut self, key: K) {
        let mut should_remove = false;
        if let Some(count) = self.worker_count_by_key.get_mut(&key) {
            debug_assert!(*count > 0);
            *count -= 1;
            should_remove = *count == 0;
        }
        if should_remove {
            self.worker_count_by_key.remove(&key);
        }
    }

    fn inc_pending_count_for_key(&mut self, key: K) {
        let count = self.pending_count_by_key.entry(key).or_insert(0);
        *count += 1;
    }

    fn dec_pending_count_for_key(&mut self, key: K) {
        let mut should_remove = false;
        if let Some(count) = self.pending_count_by_key.get_mut(&key) {
            debug_assert!(*count > 0);
            *count -= 1;
            should_remove = *count == 0;
        }
        if should_remove {
            self.pending_count_by_key.remove(&key);
        }
    }

    fn reserved_count_for_key(&self, key: &K) -> usize {
        self.worker_count_by_key.get(key).copied().unwrap_or(0)
            + self.pending_count_by_key.get(key).copied().unwrap_or(0)
    }

    fn take_clear_waiters_if_done(&mut self) -> Vec<Notify<()>> {
        if self.clearing && self.current_count == 0 {
            self.clearing = false;
            self.clear_waiting_list.drain(..).collect()
        } else {
            Vec::new()
        }
    }

    fn find_matching_waiter_index_for_worker(&self, worker: &W, primary_key: &K) -> Option<usize> {
        self.waiting_list.iter().position(|waiting| {
            if waiting.future.is_canceled() {
                return false;
            }
            waiting.key == *primary_key && worker.supports(waiting.key.clone())
        })
    }

    fn remove_canceled_waiters(&mut self) {
        self.waiting_list
            .retain(|waiting| !waiting.future.is_canceled());
    }

    fn drain_waiters(&mut self) -> Vec<Notify<KeyedWorkerWaitResult<K, W, F>>> {
        self.waiting_list
            .drain(..)
            .map(|waiting| waiting.future)
            .collect()
    }
}

pub struct KeyedWorkerPool<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
    factory: Arc<F>,
    config: KeyedWorkerPoolConfig,
    state: Mutex<WorkerPoolState<K, W, F>>,
}
pub type KeyedWorkerPoolRef<K, W, F> = Arc<KeyedWorkerPool<K, W, F>>;

#[cfg(test)]
#[test]
fn test_keyed_worker_pool_config_default_idle_limits() {
    let config = KeyedWorkerPoolConfig::default();
    assert_eq!(config.max_idle_count_per_key, None);
}

#[cfg(test)]
#[test]
fn test_keyed_worker_pool_config_builder() {
    let timeout = Duration::from_secs(1);
    let config = KeyedWorkerPoolConfig::default()
        .with_idle_timeout(Some(timeout))
        .with_max_count_per_key(Some(2))
        .with_max_idle_count_per_key(Some(1));
    assert_eq!(config.idle_timeout, Some(timeout));
    assert_eq!(config.max_count_per_key, Some(2));
    assert_eq!(config.max_idle_count_per_key, Some(1));
}

impl<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> KeyedWorkerPool<K, W, F> {
    fn key_limit_reached(&self, state: &WorkerPoolState<K, W, F>, key: &K) -> bool {
        self.config
            .max_count_per_key
            .map(|max_count| state.reserved_count_for_key(key) >= usize::from(max_count))
            .unwrap_or(false)
    }

    fn validate_created_worker(requested_key: &K, worker: &W) -> PoolResult<K> {
        let worker_key = worker.primary_key();
        if !worker.supports(worker_key.clone()) {
            return Err(pool_invalid_config_error(
                "worker primary key is not valid for itself",
            ));
        }
        if worker_key != requested_key.clone() {
            return Err(pool_invalid_config_error(
                "factory returned worker with mismatched key",
            ));
        }
        Ok(worker_key)
    }

    /// Creates a keyed worker pool with explicit per-key configuration.
    pub fn new(factory: F, config: KeyedWorkerPoolConfig) -> KeyedWorkerPoolRef<K, W, F> {
        Arc::new(KeyedWorkerPool {
            factory: Arc::new(factory),
            config,
            state: Mutex::new(WorkerPoolState {
                current_count: 0,
                worker_count_by_key: HashMap::new(),
                pending_count_by_key: HashMap::new(),
                worker_list: HashMap::new(),
                waiting_list: Vec::new(),
                clearing: false,
                clear_waiting_list: Vec::new(),
            }),
        })
    }

    fn take_expired_idle_workers(
        state: &mut WorkerPoolState<K, W, F>,
        idle_timeout: Option<std::time::Duration>,
    ) -> Vec<IdleWorker<K, W>> {
        let Some(idle_timeout) = idle_timeout else {
            return Vec::new();
        };
        let mut removed_workers = Vec::new();
        let now = Instant::now();
        for workers in state.worker_list.values_mut() {
            while workers
                .front()
                .map(|idle_worker| now.duration_since(idle_worker.idle_since) >= idle_timeout)
                .unwrap_or(false)
            {
                removed_workers.push(workers.pop_front().unwrap());
            }
        }
        state.worker_list.retain(|_, workers| !workers.is_empty());
        for idle_worker in &removed_workers {
            state.current_count -= 1;
            state.dec_worker_count_for_key(idle_worker.primary_key.clone());
        }
        removed_workers
    }

    fn take_expired_idle_workers_for_key(
        state: &mut WorkerPoolState<K, W, F>,
        key: &K,
        idle_timeout: Option<Duration>,
    ) -> Vec<IdleWorker<K, W>> {
        let Some(idle_timeout) = idle_timeout else {
            return Vec::new();
        };
        let mut removed_workers = Vec::new();
        let now = Instant::now();
        let remove_bucket = if let Some(workers) = state.worker_list.get_mut(key) {
            while workers
                .front()
                .map(|idle_worker| now.duration_since(idle_worker.idle_since) >= idle_timeout)
                .unwrap_or(false)
            {
                removed_workers.push(workers.pop_front().unwrap());
            }
            workers.is_empty()
        } else {
            false
        };
        if remove_bucket {
            state.worker_list.remove(key);
        }
        for idle_worker in &removed_workers {
            state.current_count -= 1;
            state.dec_worker_count_for_key(idle_worker.primary_key.clone());
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
        self: &KeyedWorkerPoolRef<K, W, F>,
        key: K,
    ) -> PoolResult<KeyedWorkerGuard<K, W, F>> {
        loop {
            if self.config.max_count_per_key == Some(0) {
                return Err(pool_invalid_config_error("pool max_count_per_key is zero"));
            }

            let (worker, wait, should_create, removed_workers) = {
                let mut state = self.state.lock().unwrap();
                if state.clearing {
                    return Err(pool_clearing_error());
                }
                state.remove_canceled_waiters();

                let mut removed_workers = Self::take_expired_idle_workers_for_key(
                    &mut state,
                    &key,
                    self.config.idle_timeout,
                );

                let worker = loop {
                    let Some(idle_worker) = state.take_idle_worker_for_key(&key) else {
                        break None;
                    };
                    let expired = self
                        .config
                        .idle_timeout
                        .map(|idle_timeout| idle_worker.idle_since.elapsed() >= idle_timeout)
                        .unwrap_or(false);
                    if expired
                        || !idle_worker.worker.is_work()
                        || idle_worker.worker.primary_key() != idle_worker.primary_key
                        || !idle_worker.worker.supports(idle_worker.primary_key.clone())
                    {
                        state.current_count -= 1;
                        state.dec_worker_count_for_key(idle_worker.primary_key.clone());
                        removed_workers.push(idle_worker);
                        continue;
                    }
                    break Some((idle_worker.worker, idle_worker.primary_key));
                };

                if worker.is_some() {
                    (worker, None, false, removed_workers)
                } else if self.key_limit_reached(&state, &key) {
                    let (notify, waiter) = Notify::new();
                    state.waiting_list.push(WaitingItem {
                        future: notify,
                        key: key.clone(),
                    });
                    (None, Some(waiter), false, removed_workers)
                } else {
                    state.current_count += 1;
                    state.inc_pending_count_for_key(key.clone());
                    (None, None, true, removed_workers)
                }
            };

            let reservation =
                should_create.then(|| KeyedWorkerReservation::new(self.clone(), key.clone()));
            drop(removed_workers);

            if let Some((worker, primary_key)) = worker {
                return Ok(KeyedWorkerGuard::new(worker, self.clone(), primary_key));
            }

            if let Some(wait) = wait {
                match wait.await {
                    KeyedWorkerWaitResult::Worker(worker) => return Ok(worker),
                    KeyedWorkerWaitResult::Retry => continue,
                    KeyedWorkerWaitResult::Error(err) => return Err(err),
                }
            }

            let reservation = reservation.unwrap();
            let (worker, primary_key) = match self.factory.create(key.clone()).await {
                Ok(worker) => {
                    let primary_key = Self::validate_created_worker(&key, &worker)?;
                    (worker, primary_key)
                }
                Err(err) => return Err(err),
            };
            match reservation.complete(primary_key.clone()) {
                ReservationCompletion::Complete => {}
                ReservationCompletion::Clearing => return Err(pool_cleared_error()),
            }
            return Ok(KeyedWorkerGuard::new(worker, self.clone(), primary_key));
        }
    }

    pub async fn clear_all_worker(&self) {
        let (waiter, waiting_list, clear_waiters, idle_workers) = {
            let mut state = self.state.lock().unwrap();
            let idle_workers = if !state.clearing {
                state.clearing = true;
                let idle_workers = state.drain_idle_workers();
                let cur_worker_count = idle_workers.len();
                state.current_count -= cur_worker_count;
                for idle_worker in &idle_workers {
                    state.dec_worker_count_for_key(idle_worker.primary_key.clone());
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
                .notify(KeyedWorkerWaitResult::Error(pool_cleared_error()));
        }
        for waiter in clear_waiters {
            waiter.notify(());
        }
        drop(idle_workers);
        if let Some(waiter) = waiter {
            waiter.await;
        }
    }

    fn notify_retry_waiters(waiters: Vec<Notify<KeyedWorkerWaitResult<K, W, F>>>) {
        for waiter in waiters {
            waiter.notify(KeyedWorkerWaitResult::Retry);
        }
    }

    fn rollback_reservation(&self, key: &K) {
        let (retry_waiters, clear_waiters) = {
            let mut state = self.state.lock().unwrap();
            state.current_count -= 1;
            state.dec_pending_count_for_key(key.clone());
            let retry_waiters = state.drain_waiters();
            let clear_waiters = state.take_clear_waiters_if_done();
            (retry_waiters, clear_waiters)
        };
        Self::notify_retry_waiters(retry_waiters);
        for waiter in clear_waiters {
            waiter.notify(());
        }
    }

    fn release(self: &KeyedWorkerPoolRef<K, W, F>, work: W, primary_key: K) {
        enum ReleaseAction<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>> {
            None,
            Notify(
                Notify<KeyedWorkerWaitResult<K, W, F>>,
                KeyedWorkerGuard<K, W, F>,
            ),
            Retry(Vec<Notify<KeyedWorkerWaitResult<K, W, F>>>),
        }

        let primary_key_valid =
            work.primary_key() == primary_key && work.supports(primary_key.clone());
        let mut clear_waiters = Vec::new();
        let mut removed_workers = Vec::new();
        let action = {
            let mut state = self.state.lock().unwrap();
            state.remove_canceled_waiters();
            if state.clearing {
                state.current_count -= 1;
                state.dec_worker_count_for_key(primary_key);
                clear_waiters = state.take_clear_waiters_if_done();
                ReleaseAction::None
            } else if !primary_key_valid {
                state.current_count -= 1;
                state.dec_worker_count_for_key(primary_key);
                let waiters = state.drain_waiters();
                if !waiters.is_empty() {
                    ReleaseAction::Retry(waiters)
                } else {
                    ReleaseAction::None
                }
            } else if work.is_work() {
                if let Some(index) =
                    state.find_matching_waiter_index_for_worker(&work, &primary_key)
                {
                    let waiting_item = state.waiting_list.remove(index);
                    ReleaseAction::Notify(
                        waiting_item.future,
                        KeyedWorkerGuard::new(work, self.clone(), primary_key),
                    )
                } else {
                    state.push_idle_worker(work, primary_key.clone());
                    if let Some(max_idle_count_per_key) = self.config.max_idle_count_per_key {
                        while state
                            .worker_list
                            .get(&primary_key)
                            .map(VecDeque::len)
                            .unwrap_or(0)
                            > usize::from(max_idle_count_per_key)
                        {
                            let idle_worker = state.remove_idle_worker(&primary_key, 0);
                            state.current_count -= 1;
                            state.dec_worker_count_for_key(idle_worker.primary_key.clone());
                            removed_workers.push(idle_worker);
                        }
                    }
                    ReleaseAction::None
                }
            } else {
                state.dec_worker_count_for_key(primary_key);
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
                waiting.notify(KeyedWorkerWaitResult::Worker(worker));
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
    enum Key {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        key: Key,
    }

    impl KeyedWorker<Key> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: Key) -> bool {
            self.key == key || (self.key == Key::A && key == Key::B)
        }

        fn primary_key(&self) -> Key {
            self.key.clone()
        }
    }

    struct TestFactory(AtomicUsize);

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<Key, TestWorker> for TestFactory {
        async fn create(&self, key: Key) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                id: self.0.fetch_add(1, Ordering::SeqCst),
                key,
            })
        }
    }

    fn new_pool(per_key_idle: u16) -> KeyedWorkerPoolRef<Key, TestWorker, TestFactory> {
        KeyedWorkerPool::new(
            TestFactory(AtomicUsize::new(0)),
            KeyedWorkerPoolConfig {
                max_count_per_key: None,
                max_idle_count_per_key: Some(per_key_idle),
                idle_timeout: None,
            },
        )
    }

    #[tokio::test]
    async fn per_key_idle_limits_are_isolated_and_evict_key_lru() {
        let pool = new_pool(1);
        let a0 = pool.get_worker(Key::A).await.unwrap();
        let a1 = pool.get_worker(Key::A).await.unwrap();
        let b2 = pool.get_worker(Key::B).await.unwrap();
        drop(a0);
        drop(b2);
        drop(a1);

        let a = pool.get_worker(Key::A).await.unwrap();
        let b = pool.get_worker(Key::B).await.unwrap();
        assert_eq!(a.id, 1);
        assert_eq!(b.id, 2);
    }

    #[tokio::test]
    async fn acquisition_cleans_expired_worker_behind_bucket_mru() {
        let pool = KeyedWorkerPool::new(
            TestFactory(AtomicUsize::new(0)),
            KeyedWorkerPoolConfig::default().with_idle_timeout(Some(Duration::from_millis(20))),
        );
        let a0 = pool.get_worker(Key::A).await.unwrap();
        let a1 = pool.get_worker(Key::A).await.unwrap();

        drop(a0);
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(a1);

        let a = pool.get_worker(Key::A).await.unwrap();
        assert_eq!(a.id, 1);
        assert_eq!(pool.cleanup_idle_worker(), 0);
    }

    #[tokio::test]
    async fn keyed_acquisition_only_cleans_and_reuses_requested_bucket() {
        let pool = KeyedWorkerPool::new(
            TestFactory(AtomicUsize::new(0)),
            KeyedWorkerPoolConfig::default().with_idle_timeout(Some(Duration::from_millis(20))),
        );
        let a = pool.get_worker(Key::A).await.unwrap();
        drop(a);
        tokio::time::sleep(Duration::from_millis(30)).await;

        let b = pool.get_worker(Key::B).await.unwrap();
        assert_eq!(b.id, 1);
        assert_eq!(b.primary_key(), Key::B);
        assert_eq!(pool.cleanup_idle_worker(), 1);
    }

    #[tokio::test]
    async fn returned_worker_only_wakes_waiter_for_its_primary_key() {
        let pool = KeyedWorkerPool::new(
            TestFactory(AtomicUsize::new(0)),
            KeyedWorkerPoolConfig::default().with_max_count_per_key(Some(1)),
        );
        let a = pool.get_worker(Key::A).await.unwrap();
        let b = pool.get_worker(Key::B).await.unwrap();
        let pool_ref = pool.clone();
        let waiting_b = tokio::spawn(async move { pool_ref.get_worker(Key::B).await });
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(a);
        tokio::task::yield_now().await;
        assert!(!waiting_b.is_finished());

        drop(b);
        let b = tokio::time::timeout(Duration::from_secs(1), waiting_b)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(b.primary_key(), Key::B);
    }

    #[tokio::test]
    async fn zero_per_key_idle_limit_disables_idle_cache() {
        let pool = new_pool(0);
        let a = pool.get_worker(Key::A).await.unwrap();
        assert_eq!(a.id, 0);
        drop(a);
        let a = pool.get_worker(Key::A).await.unwrap();
        assert_eq!(a.id, 1);
    }
}

#[cfg(test)]
fn new_keyed_worker_pool<K: WorkerKey, W: KeyedWorker<K>, F: KeyedWorkerFactory<K, W>>(
    max_count: u16,
    factory: F,
) -> KeyedWorkerPoolRef<K, W, F> {
    KeyedWorkerPool::new(
        factory,
        KeyedWorkerPoolConfig {
            max_count_per_key: Some(max_count),
            ..Default::default()
        },
    )
}

#[tokio::test]
async fn test_pool() {
    struct TestWorker {
        work: bool,
        key: TestWorkerKey,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }
    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { work: true, key })
        }
    }

    let pool = new_keyed_worker_pool(2, TestWorkerFactory);

    let worker_a1 = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let worker_a2 = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let worker_b1 = pool.get_worker(TestWorkerKey::B).await.unwrap();
    let worker_b2 = pool.get_worker(TestWorkerKey::B).await.unwrap();

    let pool_ref = pool.clone();
    let keyed_waiter = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::B).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!keyed_waiter.is_finished());

    drop(worker_b1);
    let worker_b = tokio::time::timeout(std::time::Duration::from_secs(1), keyed_waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(worker_a1);
    drop(worker_a2);
    drop(worker_b2);
    drop(worker_b);

    let worker_b1 = pool.get_worker(TestWorkerKey::B).await.unwrap();
    let worker_b2 = pool.get_worker(TestWorkerKey::B).await.unwrap();
    let worker1 = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let worker2 = pool.get_worker(TestWorkerKey::A).await.unwrap();

    let pool_ref = pool.clone();
    let keyed_a_waiter = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::A).await });
    let pool_ref = pool.clone();
    let keyed_waiter = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::B).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!keyed_a_waiter.is_finished());
    assert!(!keyed_waiter.is_finished());

    let pool_ref = pool.clone();
    let clear_task = tokio::spawn(async move {
        pool_ref.clear_all_worker().await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    assert!(keyed_a_waiter.await.unwrap().is_err());
    assert!(keyed_waiter.await.unwrap().is_err());

    drop(worker1);
    drop(worker2);
    drop(worker_b1);
    drop(worker_b2);

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
    enum TestWorkerKey {
        A,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            self.create_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(TestWorker { key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let pool_ref = pool.clone();
    let worker_task = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::A).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    pool.clear_all_worker().await;

    let worker = worker_task.await.unwrap();
    assert!(worker.is_err());
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_concurrent_clear_all_worker() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { key })
        }
    }

    let pool = new_keyed_worker_pool(1, TestWorkerFactory);
    let worker = pool.get_worker(TestWorkerKey::A).await.unwrap();

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
async fn test_zero_max_count_per_key_returns_error() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { key })
        }
    }

    let pool = new_keyed_worker_pool(0, TestWorkerFactory);
    let worker = pool.get_worker(TestWorkerKey::A).await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn test_keyed_pool_default_config_has_no_per_key_count_limit() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    struct TestWorkerKey;

    struct TestWorker;

    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, _key: TestWorkerKey) -> bool {
            true
        }

        fn primary_key(&self) -> TestWorkerKey {
            TestWorkerKey
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, _key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker)
        }
    }

    let pool = KeyedWorkerPool::new(TestWorkerFactory, Default::default());
    let worker1 = pool.get_worker(TestWorkerKey).await.unwrap();
    let worker2 = pool.get_worker(TestWorkerKey).await.unwrap();
    drop((worker1, worker2));
}

#[tokio::test]
async fn test_keyed_pool_waits_when_key_already_has_worker() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        B,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { key })
        }
    }

    let pool = new_keyed_worker_pool(1, TestWorkerFactory);
    let _worker = pool.get_worker(TestWorkerKey::B).await.unwrap();

    let pool_ref = pool.clone();
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), async move {
        pool_ref.get_worker(TestWorkerKey::B).await
    })
    .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_different_key_is_not_blocked_by_per_key_limit() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let worker_a = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let worker_b = pool.get_worker(TestWorkerKey::B).await.unwrap();

    assert_eq!(worker_a.id, 0);
    assert_eq!(worker_b.id, 1);
    assert_eq!(worker_b.primary_key(), TestWorkerKey::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_keyed_create_failure_fails_same_key_waiters() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        B,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, _key: TestWorkerKey) -> PoolResult<TestWorker> {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Err(crate::pool_invalid_config_error("create failed"))
        }
    }

    let pool = new_keyed_worker_pool(1, TestWorkerFactory);

    let pool_ref = pool.clone();
    let worker1 = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::B).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let pool_ref = pool.clone();
    let worker2 = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::B).await });

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
async fn test_keyed_create_failure_wakes_waiter_to_create() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if id == 0 && key == TestWorkerKey::A {
                Err(crate::pool_invalid_config_error("create failed"))
            } else {
                Ok(TestWorker { id, key })
            }
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let pool_ref = pool.clone();
    let keyed = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::A).await });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::A).await });

    let (keyed, waiter) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        (keyed.await.unwrap(), waiter.await.unwrap())
    })
    .await
    .unwrap();

    assert_eq!(
        keyed.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
    let waiter = waiter.unwrap();
    assert_eq!(waiter.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_keyed_retry_notification_skips_canceled_waiter() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { key })
        }
    }

    let (canceled_notify, canceled_waiter) = Notify::new();
    let _key = TestWorkerKey::A;
    drop(canceled_waiter);
    let (notify, waiter) = Notify::new();

    KeyedWorkerPool::<TestWorkerKey, TestWorker, TestWorkerFactory>::notify_retry_waiters(vec![
        canceled_notify,
        notify,
    ]);

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap();
    assert!(matches!(result, KeyedWorkerWaitResult::Retry));
}

#[tokio::test]
async fn test_keyed_request_replaces_non_matching_idle_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    {
        let worker = pool.get_worker(TestWorkerKey::A).await.unwrap();
        assert_eq!(worker.id, 0);
    }

    let worker = pool.get_worker(TestWorkerKey::B).await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(worker.primary_key(), TestWorkerKey::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_other_key_does_not_wait_for_returned_non_matching_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );
    let worker_a = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let _worker_b = pool.get_worker(TestWorkerKey::B).await.unwrap();

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::B).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(waiter.is_finished());

    let worker = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(worker.id, 2);
    assert_eq!(worker.primary_key(), TestWorkerKey::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 3);
    drop(worker_a);
}

#[tokio::test]
async fn test_other_key_does_not_wait_for_unwork_non_matching_worker() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        work: AtomicBool,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            self.work.load(Ordering::SeqCst)
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker {
                id,
                work: AtomicBool::new(true),
                key,
            })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );
    let worker_a = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let _worker_b = pool.get_worker(TestWorkerKey::B).await.unwrap();

    let pool_ref = pool.clone();
    let waiter = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::B).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(waiter.is_finished());

    worker_a.work.store(false, Ordering::SeqCst);
    let worker = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(worker.id, 2);
    assert_eq!(worker.primary_key(), TestWorkerKey::B);
    assert_eq!(create_count.load(Ordering::SeqCst), 3);
    drop(worker_a);
}

#[tokio::test]
async fn test_factory_must_return_matching_key() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let count = self.create_count.fetch_add(1, Ordering::SeqCst);
            let key = if count == 0 { TestWorkerKey::A } else { key };
            Ok(TestWorker { key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        1,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );
    let worker = pool.get_worker(TestWorkerKey::B).await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );

    let worker = pool.get_worker(TestWorkerKey::B).await;
    assert!(worker.is_ok());
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keyed_waiter_keeps_queue_priority_over_later_waiter() {
    use std::sync::mpsc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        B,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { key })
        }
    }

    let pool = new_keyed_worker_pool(1, TestWorkerFactory);
    let worker = pool.get_worker(TestWorkerKey::B).await.unwrap();

    let (tx, rx) = mpsc::channel();

    let pool_ref = pool.clone();
    let tx_keyed = tx.clone();
    let keyed_task = tokio::spawn(async move {
        let _worker = pool_ref.get_worker(TestWorkerKey::B).await.unwrap();
        tx_keyed.send("keyed").unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let pool_ref = pool.clone();
    let later_task = tokio::spawn(async move {
        let _worker = pool_ref.get_worker(TestWorkerKey::B).await.unwrap();
        tx.send("later").unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(worker);

    let first = rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    assert_eq!(first, "keyed");

    keyed_task.await.unwrap();
    later_task.await.unwrap();
}

#[tokio::test]
async fn test_factory_worker_must_be_valid_for_its_primary_key() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            key == TestWorkerKey::B
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, _key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker {
                key: TestWorkerKey::A,
            })
        }
    }

    let pool = new_keyed_worker_pool(1, TestWorkerFactory);
    let worker = pool.get_worker(TestWorkerKey::A).await;
    assert!(worker.is_err());
    assert_eq!(
        worker.err().unwrap().code(),
        crate::PoolErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn test_keyed_idle_worker_timeout_releases_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = KeyedWorkerPool::new(
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        KeyedWorkerPoolConfig {
            max_count_per_key: Some(1),
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..Default::default()
        },
    );

    {
        let worker = pool.get_worker(TestWorkerKey::B).await.unwrap();
        assert_eq!(worker.id, 0);
        assert_eq!(worker.primary_key(), TestWorkerKey::B);
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    let worker = pool.get_worker(TestWorkerKey::A).await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(worker.primary_key(), TestWorkerKey::A);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_get_keyed_worker_uses_most_recent_matching_idle_worker() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = new_keyed_worker_pool(
        2,
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
    );

    let worker1 = pool.get_worker(TestWorkerKey::A).await.unwrap();
    let worker2 = pool.get_worker(TestWorkerKey::A).await.unwrap();
    assert_eq!(worker1.id, 0);
    assert_eq!(worker2.id, 1);

    drop(worker1);
    drop(worker2);

    let worker = pool.get_worker(TestWorkerKey::A).await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_keyed_cleanup_idle_worker_can_be_triggered_externally() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        id: usize,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory {
        create_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            let id = self.create_count.fetch_add(1, Ordering::SeqCst);
            Ok(TestWorker { id, key })
        }
    }

    let create_count = Arc::new(AtomicUsize::new(0));
    let pool = KeyedWorkerPool::new(
        TestWorkerFactory {
            create_count: create_count.clone(),
        },
        KeyedWorkerPoolConfig {
            max_count_per_key: Some(1),
            idle_timeout: Some(std::time::Duration::from_millis(30)),
            ..Default::default()
        },
    );

    {
        let worker = pool.get_worker(TestWorkerKey::B).await.unwrap();
        assert_eq!(worker.id, 0);
        assert_eq!(worker.primary_key(), TestWorkerKey::B);
    }

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    assert_eq!(pool.cleanup_idle_worker(), 1);

    let worker = pool.get_worker(TestWorkerKey::A).await.unwrap();
    assert_eq!(worker.id, 1);
    assert_eq!(worker.primary_key(), TestWorkerKey::A);
    assert_eq!(create_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_canceled_keyed_create_rolls_back_reservation() {
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
    }

    struct TestWorker;

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, _key: TestWorkerKey) -> bool {
            true
        }

        fn primary_key(&self) -> TestWorkerKey {
            TestWorkerKey::A
        }
    }

    struct TestWorkerFactory {
        create_started: Arc<AtomicBool>,
        allow_create: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, _key: TestWorkerKey) -> PoolResult<TestWorker> {
            self.create_started.store(true, Ordering::SeqCst);
            while !self.allow_create.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(TestWorker)
        }
    }

    let create_started = Arc::new(AtomicBool::new(false));
    let allow_create = Arc::new(AtomicBool::new(false));
    let pool = new_keyed_worker_pool(
        1,
        TestWorkerFactory {
            create_started: create_started.clone(),
            allow_create: allow_create.clone(),
        },
    );

    let pool_ref = pool.clone();
    let create_task = tokio::spawn(async move { pool_ref.get_worker(TestWorkerKey::A).await });
    while !create_started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    create_task.abort();
    assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

    allow_create.store(true, Ordering::SeqCst);
    let worker = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pool.get_worker(TestWorkerKey::A),
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
async fn test_mutating_worker_key_removes_returned_worker() {
    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum TestWorkerKey {
        A,
        B,
    }

    struct TestWorker {
        work: bool,
        key: TestWorkerKey,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<TestWorkerKey> for TestWorker {
        fn is_work(&self) -> bool {
            self.work
        }

        fn supports(&self, key: TestWorkerKey) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> TestWorkerKey {
            self.key.clone()
        }
    }

    struct TestWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<TestWorkerKey, TestWorker> for TestWorkerFactory {
        async fn create(&self, key: TestWorkerKey) -> PoolResult<TestWorker> {
            Ok(TestWorker { work: true, key })
        }
    }

    let pool = KeyedWorkerPool::new(
        TestWorkerFactory,
        KeyedWorkerPoolConfig {
            idle_timeout: None,
            max_count_per_key: Some(1),
            ..Default::default()
        },
    );
    let mut worker = pool.get_worker(TestWorkerKey::A).await.unwrap();
    worker.key = TestWorkerKey::B;
    drop(worker);

    {
        let state = pool.state.lock().unwrap();
        assert_eq!(state.current_count, 0);
        assert!(state.worker_count_by_key.is_empty());
    }

    let worker = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        pool.get_worker(TestWorkerKey::A),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(worker.primary_key(), TestWorkerKey::A);
}

#[cfg(test)]
mod affected_path_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    #[derive(Clone, Debug, Eq, PartialEq, Hash)]
    enum Key {
        A,
        B,
        C,
    }

    struct BlockingWorker {
        key: Key,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<Key> for BlockingWorker {
        fn is_work(&self) -> bool {
            true
        }

        fn supports(&self, key: Key) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> Key {
            self.key.clone()
        }
    }

    struct BlockingFactory {
        create_started: Arc<AtomicBool>,
        allow_create: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<Key, BlockingWorker> for BlockingFactory {
        async fn create(&self, key: Key) -> PoolResult<BlockingWorker> {
            self.create_started.store(true, Ordering::SeqCst);
            while !self.allow_create.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            Ok(BlockingWorker { key })
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
        KeyedWorkerPoolRef<Key, BlockingWorker, BlockingFactory>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let create_started = Arc::new(AtomicBool::new(false));
        let allow_create = Arc::new(AtomicBool::new(false));
        let pool = new_keyed_worker_pool(
            max_count,
            BlockingFactory {
                create_started: create_started.clone(),
                allow_create: allow_create.clone(),
            },
        );
        (pool, create_started, allow_create)
    }

    #[tokio::test]
    async fn test_canceled_create_rolls_back_keyed_pool_reservation() {
        let (pool, create_started, allow_create) = new_blocking_pool(1);
        let pool_ref = pool.clone();
        let create_task = tokio::spawn(async move { pool_ref.get_worker(Key::A).await });
        wait_for_create(&create_started).await;
        create_task.abort();
        assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

        allow_create.store(true, Ordering::SeqCst);
        let worker = tokio::time::timeout(Duration::from_secs(1), pool.get_worker(Key::A))
            .await
            .unwrap()
            .unwrap();
        drop(worker);
        tokio::time::timeout(Duration::from_secs(1), pool.clear_all_worker())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_canceled_replacement_create_rolls_back_keyed_pool_reservation() {
        let (pool, create_started, allow_create) = new_blocking_pool(1);
        allow_create.store(true, Ordering::SeqCst);
        let worker = pool.get_worker(Key::A).await.unwrap();
        drop(worker);

        create_started.store(false, Ordering::SeqCst);
        allow_create.store(false, Ordering::SeqCst);
        let pool_ref = pool.clone();
        let create_task = tokio::spawn(async move { pool_ref.get_worker(Key::B).await });
        wait_for_create(&create_started).await;
        create_task.abort();
        assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

        allow_create.store(true, Ordering::SeqCst);
        let worker = tokio::time::timeout(Duration::from_secs(1), pool.get_worker(Key::B))
            .await
            .unwrap()
            .unwrap();
        drop(worker);
        tokio::time::timeout(Duration::from_secs(1), pool.clear_all_worker())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_canceled_overcommit_create_rolls_back_keyed_pool_reservation() {
        let (pool, create_started, allow_create) = new_blocking_pool(1);
        allow_create.store(true, Ordering::SeqCst);
        let worker_a = pool.get_worker(Key::A).await.unwrap();

        create_started.store(false, Ordering::SeqCst);
        allow_create.store(false, Ordering::SeqCst);
        let pool_ref = pool.clone();
        let create_task = tokio::spawn(async move { pool_ref.get_worker(Key::B).await });
        wait_for_create(&create_started).await;
        create_task.abort();
        assert!(matches!(create_task.await, Err(err) if err.is_cancelled()));

        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 1);
            assert_eq!(state.reserved_count_for_key(&Key::B), 0);
        }

        allow_create.store(true, Ordering::SeqCst);
        let worker_b = tokio::time::timeout(Duration::from_secs(1), pool.get_worker(Key::B))
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
        key: Key,
        on_drop: Option<DropCallback>,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<Key> for DropProbeWorker {
        fn is_work(&self) -> bool {
            self.working.load(Ordering::SeqCst)
        }

        fn supports(&self, key: Key) -> bool {
            self.key == key
        }

        fn primary_key(&self) -> Key {
            self.key.clone()
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
    impl KeyedWorkerFactory<Key, DropProbeWorker> for DropProbeFactory {
        async fn create(&self, key: Key) -> PoolResult<DropProbeWorker> {
            let spec = self.specs.lock().unwrap().pop_front().unwrap();
            Ok(DropProbeWorker {
                working: spec.working,
                key,
                on_drop: spec.on_drop,
            })
        }
    }

    type DropProbePool = KeyedWorkerPoolRef<Key, DropProbeWorker, DropProbeFactory>;

    fn new_drop_probe_pool(
        idle_timeout: Option<Duration>,
        max_idle_count: Option<u16>,
    ) -> (DropProbePool, Arc<Mutex<VecDeque<DropProbeSpec>>>) {
        let specs = Arc::new(Mutex::new(VecDeque::new()));
        let pool = KeyedWorkerPool::new(
            DropProbeFactory {
                specs: specs.clone(),
            },
            KeyedWorkerPoolConfig {
                max_count_per_key: Some(1),
                max_idle_count_per_key: max_idle_count,
                idle_timeout,
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
        KeyedInvalidScan,
        Clear,
    }

    async fn assert_idle_drop_path_runs_outside_lock(path: IdleDropPath) {
        let idle_timeout = matches!(path, IdleDropPath::Cleanup).then_some(Duration::ZERO);
        let max_idle_count = matches!(path, IdleDropPath::IdleLimit).then_some(0);
        let (pool, specs) = new_drop_probe_pool(idle_timeout, max_idle_count);
        let working = Arc::new(AtomicBool::new(true));
        let (spec, drop_result) = drop_lock_probe(&pool, working.clone());
        specs.lock().unwrap().push_back(spec);

        let worker = pool.get_worker(Key::A).await.unwrap();
        drop(worker);

        match path {
            IdleDropPath::Cleanup => {
                assert_eq!(pool.cleanup_idle_worker(), 1);
            }
            IdleDropPath::IdleLimit => {
                // The first return above evicts immediately from the zero-cap pool.
            }
            IdleDropPath::KeyedInvalidScan => {
                working.store(false, Ordering::SeqCst);
                specs.lock().unwrap().push_back(plain_drop_probe_spec());
                let worker = pool.get_worker(Key::A).await.unwrap();
                drop(worker);
            }
            IdleDropPath::Clear => pool.clear_all_worker().await,
        }

        assert!(drop_result.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    #[tokio::test]
    async fn test_all_keyed_idle_drop_paths_run_outside_state_lock() {
        for path in [
            IdleDropPath::Cleanup,
            IdleDropPath::IdleLimit,
            IdleDropPath::KeyedInvalidScan,
            IdleDropPath::Clear,
        ] {
            assert_idle_drop_path_runs_outside_lock(path).await;
        }
    }

    struct MutableWorker {
        working: Arc<AtomicBool>,
        valid: Arc<AtomicBool>,
        key: Key,
    }

    #[async_trait::async_trait]
    impl KeyedWorker<Key> for MutableWorker {
        fn is_work(&self) -> bool {
            self.working.load(Ordering::SeqCst)
        }

        fn supports(&self, key: Key) -> bool {
            self.valid.load(Ordering::SeqCst) && self.key == key
        }

        fn primary_key(&self) -> Key {
            self.key.clone()
        }
    }

    struct MutableWorkerFactory;

    #[async_trait::async_trait]
    impl KeyedWorkerFactory<Key, MutableWorker> for MutableWorkerFactory {
        async fn create(&self, key: Key) -> PoolResult<MutableWorker> {
            Ok(MutableWorker {
                working: Arc::new(AtomicBool::new(true)),
                valid: Arc::new(AtomicBool::new(true)),
                key,
            })
        }
    }

    type MutablePool = KeyedWorkerPoolRef<Key, MutableWorker, MutableWorkerFactory>;

    fn new_mutable_pool(max_count: u16, idle_timeout: Option<Duration>) -> MutablePool {
        KeyedWorkerPool::new(
            MutableWorkerFactory,
            KeyedWorkerPoolConfig {
                max_count_per_key: Some(max_count),
                idle_timeout,
                ..Default::default()
            },
        )
    }

    fn new_limited_mutable_pool(_max_count: u16, max_count_per_key: u16) -> MutablePool {
        KeyedWorkerPool::new(
            MutableWorkerFactory,
            KeyedWorkerPoolConfig {
                idle_timeout: None,
                max_count_per_key: Some(max_count_per_key),
                ..Default::default()
            },
        )
    }

    fn assert_accounting_empty(pool: &MutablePool) {
        let state = pool.state.lock().unwrap();
        assert_eq!(state.current_count, 0);
        assert!(state.worker_count_by_key.is_empty());
        assert!(state.pending_count_by_key.is_empty());
    }

    fn assert_only_key(pool: &MutablePool, key: Key, count: usize) {
        let state = pool.state.lock().unwrap();
        assert_eq!(state.current_count, count);
        assert_eq!(state.worker_count_by_key.len(), 1);
        assert_eq!(state.worker_count_by_key.get(&key).copied(), Some(count));
    }

    #[derive(Copy, Clone)]
    enum IdleAccountingPath {
        Cleanup,
        Clear,
        KeyedInvalidScan,
    }

    async fn assert_idle_accounting_path(path: IdleAccountingPath) {
        let idle_timeout = matches!(path, IdleAccountingPath::Cleanup).then_some(Duration::ZERO);
        let pool = new_mutable_pool(1, idle_timeout);
        let worker = pool.get_worker(Key::A).await.unwrap();
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
            IdleAccountingPath::KeyedInvalidScan => {
                working.store(false, Ordering::SeqCst);
                let worker = pool.get_worker(Key::A).await.unwrap();
                assert_only_key(&pool, Key::A, 1);
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
            IdleAccountingPath::KeyedInvalidScan,
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
        let worker_a = pool.get_worker(Key::A).await.unwrap();

        let pool_ref = pool.clone();
        let waiter = tokio::spawn(async move { pool_ref.get_worker(Key::A).await });
        wait_for_waiter(&pool).await;
        waiter.abort();
        assert!(matches!(waiter.await, Err(err) if err.is_cancelled()));

        drop(worker_a);

        let state = pool.state.lock().unwrap();
        assert!(state.waiting_list.is_empty());
        assert_eq!(
            state.worker_list.values().map(VecDeque::len).sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn test_worker_invalid_for_primary_key_wakes_waiter() {
        let pool = new_limited_mutable_pool(1, 1);
        let worker_a = pool.get_worker(Key::A).await.unwrap();
        let valid = worker_a.valid.clone();

        let pool_ref = pool.clone();
        let waiting_a = tokio::spawn(async move { pool_ref.get_worker(Key::A).await });
        wait_for_waiter(&pool).await;

        valid.store(false, Ordering::SeqCst);
        drop(worker_a);

        let replacement_a = tokio::time::timeout(Duration::from_secs(1), waiting_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.primary_key(), Key::A);
    }

    #[tokio::test]
    async fn test_idle_worker_invalid_for_primary_key_is_replaced() {
        let pool = new_limited_mutable_pool(1, 1);
        let worker_a = pool.get_worker(Key::A).await.unwrap();
        let valid = worker_a.valid.clone();
        drop(worker_a);

        valid.store(false, Ordering::SeqCst);

        let replacement_a = tokio::time::timeout(Duration::from_secs(1), pool.get_worker(Key::A))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.primary_key(), Key::A);
    }

    #[tokio::test]
    async fn test_capped_waiter_does_not_replace_other_key_worker() {
        let pool = new_limited_mutable_pool(2, 1);
        let worker_a = pool.get_worker(Key::A).await.unwrap();
        let worker_b = pool.get_worker(Key::B).await.unwrap();

        let pool_ref = pool.clone();
        let waiting_a = tokio::spawn(async move { pool_ref.get_worker(Key::A).await });
        wait_for_waiter(&pool).await;

        drop(worker_b);
        tokio::task::yield_now().await;

        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 2);
            assert_eq!(
                state.worker_list.values().map(VecDeque::len).sum::<usize>(),
                1
            );
            assert_eq!(
                state
                    .worker_list
                    .get(&Key::B)
                    .and_then(VecDeque::front)
                    .unwrap()
                    .primary_key,
                Key::B
            );
        }
        assert!(!waiting_a.is_finished());

        drop(worker_a);
        let replacement_a = tokio::time::timeout(Duration::from_secs(1), waiting_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.primary_key(), Key::A);
    }

    #[tokio::test]
    async fn test_changed_key_worker_is_removed_and_waiter_retries() {
        let pool = new_mutable_pool(1, None);
        let mut worker = pool.get_worker(Key::A).await.unwrap();
        worker.key = Key::B;

        let pool_ref = pool.clone();
        let waiter = tokio::spawn(async move { pool_ref.get_worker(Key::A).await });
        wait_for_waiter(&pool).await;
        drop(worker);

        let worker = waiter.await.unwrap().unwrap();
        assert_eq!(worker.primary_key(), Key::A);
        worker.working.store(false, Ordering::SeqCst);
        drop(worker);
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_cached_key_is_used_while_clearing() {
        let pool = new_mutable_pool(1, None);
        let mut worker = pool.get_worker(Key::A).await.unwrap();
        worker.key = Key::B;

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
    async fn test_changed_key_worker_does_not_wake_other_key_waiter() {
        let pool = new_mutable_pool(1, None);
        let mut worker_a = pool.get_worker(Key::A).await.unwrap();
        let worker_b = pool.get_worker(Key::B).await.unwrap();
        worker_a.key = Key::C;

        let pool_ref = pool.clone();
        let waiter = tokio::spawn(async move { pool_ref.get_worker(Key::B).await });
        wait_for_waiter(&pool).await;
        drop(worker_a);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(worker_b);
        let replacement_b = waiter.await.unwrap().unwrap();
        assert_only_key(&pool, Key::B, 1);
        replacement_b.working.store(false, Ordering::SeqCst);
        drop(replacement_b);
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_changed_key_worker_is_removed_independently() {
        let pool = new_mutable_pool(1, None);
        let worker_a = pool.get_worker(Key::A).await.unwrap();
        let mut worker_b = pool.get_worker(Key::B).await.unwrap();
        worker_b.key = Key::C;
        drop(worker_b);

        assert_only_key(&pool, Key::A, 1);
        worker_a.working.store(false, Ordering::SeqCst);
        drop(worker_a);
        assert_accounting_empty(&pool);
    }

    #[tokio::test]
    async fn test_max_count_per_key_blocks_only_that_key() {
        let pool = new_limited_mutable_pool(4, 2);
        let worker_a1 = pool.get_worker(Key::A).await.unwrap();
        let worker_a2 = pool.get_worker(Key::A).await.unwrap();

        let pool_ref = pool.clone();
        let waiting_a = tokio::spawn(async move { pool_ref.get_worker(Key::A).await });
        wait_for_waiter(&pool).await;
        assert!(!waiting_a.is_finished());

        let worker_b = pool.get_worker(Key::B).await.unwrap();
        {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.current_count, 3);
            assert_eq!(state.reserved_count_for_key(&Key::A), 2);
            assert_eq!(state.reserved_count_for_key(&Key::B), 1);
        }

        drop(worker_a1);
        let replacement_a = tokio::time::timeout(Duration::from_secs(1), waiting_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(replacement_a.primary_key(), Key::A);

        drop(worker_a2);
        drop(replacement_a);
        drop(worker_b);
    }

    #[tokio::test]
    async fn test_zero_max_count_per_key_returns_error() {
        let pool = new_limited_mutable_pool(1, 0);

        let keyed_error = pool.get_worker(Key::A).await.err().unwrap();
        assert_eq!(keyed_error.code(), crate::PoolErrorCode::InvalidConfig);
    }
}
