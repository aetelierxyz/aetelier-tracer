use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

pub const FAILURE_TTL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadError {
    Loading,
    Failed(String),
}

type Outcome<T> = Result<Arc<T>, String>;

struct Gate<T> {
    done: Mutex<Option<Outcome<T>>>,
    ready: Condvar,
}

struct Inner<T> {
    entries: Vec<(String, Arc<T>)>,
    loading: HashMap<String, Arc<Gate<T>>>,
    failed: HashMap<String, (Instant, String)>,
}

pub struct TapeCache<T> {
    capacity: usize,
    failure_ttl: Duration,
    inner: Mutex<Inner<T>>,
}

fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

enum Lookup<T> {
    Hit(Arc<T>),
    Failed(String),
    Wait(Arc<Gate<T>>),
    Start(Arc<Gate<T>>),
}

impl<T: Send + Sync + 'static> TapeCache<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        Self::with_failure_ttl(capacity, FAILURE_TTL)
    }

    pub fn with_failure_ttl(capacity: usize, failure_ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.max(1),
            failure_ttl,
            inner: Mutex::new(Inner {
                entries: Vec::new(),
                loading: HashMap::new(),
                failed: HashMap::new(),
            }),
        })
    }

    fn touch(inner: &mut Inner<T>, key: &str) -> Option<Arc<T>> {
        let position = inner.entries.iter().position(|(k, _)| k == key)?;
        let entry = inner.entries.remove(position);
        let value = entry.1.clone();
        inner.entries.push(entry);
        Some(value)
    }

    pub fn get(&self, key: &str) -> Option<Arc<T>> {
        Self::touch(&mut locked(&self.inner), key)
    }

    pub fn len(&self) -> usize {
        locked(&self.inner).entries.len()
    }

    pub fn remove(&self, key: &str) {
        let mut inner = locked(&self.inner);
        inner.entries.retain(|(k, _)| k != key);
        inner.failed.remove(key);
    }

    #[cfg(test)]
    pub fn keys(&self) -> Vec<String> {
        let inner = locked(&self.inner);
        inner.entries.iter().map(|(k, _)| k.clone()).collect()
    }

    fn lookup(&self, key: &str) -> Lookup<T> {
        let mut inner = locked(&self.inner);
        if let Some(value) = Self::touch(&mut inner, key) {
            return Lookup::Hit(value);
        }
        if let Some((at, reason)) = inner.failed.get(key) {
            if at.elapsed() < self.failure_ttl {
                return Lookup::Failed(reason.clone());
            }
            inner.failed.remove(key);
        }
        if let Some(gate) = inner.loading.get(key) {
            return Lookup::Wait(gate.clone());
        }
        let gate = Arc::new(Gate {
            done: Mutex::new(None),
            ready: Condvar::new(),
        });
        inner.loading.insert(key.to_owned(), gate.clone());
        Lookup::Start(gate)
    }

    pub fn get_or_load(
        self: &Arc<Self>,
        key: &str,
        budget: Duration,
        load: impl FnOnce() -> Result<T, String> + Send + 'static,
    ) -> Result<Arc<T>, LoadError> {
        let gate = match self.lookup(key) {
            Lookup::Hit(value) => return Ok(value),
            Lookup::Failed(reason) => return Err(LoadError::Failed(reason)),
            Lookup::Wait(gate) => gate,
            Lookup::Start(gate) => {
                let cache = self.clone();
                let key = key.to_owned();
                let gate_for_thread = gate.clone();
                std::thread::spawn(move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(load))
                        .unwrap_or_else(|_| Err("tape loader panicked".to_owned()))
                        .map(Arc::new);
                    cache.finish(&key, &gate_for_thread, outcome);
                });
                gate
            }
        };
        let guard = locked(&gate.done);
        let (guard, _) = gate
            .ready
            .wait_timeout_while(guard, budget, |done| done.is_none())
            .unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            Some(Ok(value)) => Ok(value.clone()),
            Some(Err(reason)) => Err(LoadError::Failed(reason.clone())),
            None => Err(LoadError::Loading),
        }
    }

    fn finish(&self, key: &str, gate: &Gate<T>, outcome: Outcome<T>) {
        {
            let mut inner = locked(&self.inner);
            inner.loading.remove(key);
            match &outcome {
                Ok(value) => {
                    inner.failed.remove(key);
                    inner.entries.retain(|(k, _)| k != key);
                    inner.entries.push((key.to_owned(), value.clone()));
                    while inner.entries.len() > self.capacity {
                        inner.entries.remove(0);
                    }
                }
                Err(reason) => {
                    inner
                        .failed
                        .insert(key.to_owned(), (Instant::now(), reason.clone()));
                }
            }
        }
        *locked(&gate.done) = Some(outcome);
        gate.ready.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn loads(
        counter: &Arc<AtomicUsize>,
        value: u32,
        delay: Duration,
    ) -> impl FnOnce() -> Result<u32, String> + Send + 'static {
        let counter = counter.clone();
        move || {
            counter.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(delay);
            Ok(value)
        }
    }

    #[test]
    fn a_fast_load_is_served_within_the_budget_and_then_cached() {
        let cache = TapeCache::<u32>::new(2);
        let calls = Arc::new(AtomicUsize::new(0));

        let first = cache.get_or_load(
            "a",
            Duration::from_secs(5),
            loads(&calls, 7, Duration::ZERO),
        );
        let second = cache.get_or_load(
            "a",
            Duration::from_secs(5),
            loads(&calls, 8, Duration::ZERO),
        );

        assert_eq!(first.map(|v| *v), Ok(7));
        assert_eq!(second.map(|v| *v), Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(cache.keys(), ["a"]);
    }

    #[test]
    fn a_slow_load_reports_loading_until_it_lands() {
        let cache = TapeCache::<u32>::new(2);
        let calls = Arc::new(AtomicUsize::new(0));

        let early = cache.get_or_load(
            "slow",
            Duration::from_millis(20),
            loads(&calls, 1, Duration::from_millis(300)),
        );
        assert_eq!(early, Err(LoadError::Loading));
        let waited = cache.get_or_load(
            "slow",
            Duration::from_secs(5),
            loads(&calls, 2, Duration::ZERO),
        );

        assert_eq!(waited.map(|v| *v), Ok(1));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn concurrent_callers_share_one_load() {
        let cache = TapeCache::<u32>::new(2);
        let calls = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let calls = calls.clone();
                std::thread::spawn(move || {
                    cache.get_or_load(
                        "shared",
                        Duration::from_secs(5),
                        loads(&calls, 42, Duration::from_millis(100)),
                    )
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.join().unwrap().map(|v| *v), Ok(42));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn capacity_evicts_the_least_recently_used_entry() {
        let cache = TapeCache::<u32>::new(2);
        let calls = Arc::new(AtomicUsize::new(0));
        for (key, value) in [("a", 1), ("b", 2)] {
            cache
                .get_or_load(
                    key,
                    Duration::from_secs(5),
                    loads(&calls, value, Duration::ZERO),
                )
                .unwrap();
        }
        assert!(cache.get("a").is_some());
        cache
            .get_or_load(
                "c",
                Duration::from_secs(5),
                loads(&calls, 3, Duration::ZERO),
            )
            .unwrap();

        assert_eq!(cache.keys(), ["a", "c"]);
        assert!(cache.get("b").is_none());
    }

    #[test]
    fn a_failed_load_is_cached_briefly_then_retried() {
        let cache = TapeCache::<u32>::with_failure_ttl(2, Duration::from_millis(100));

        let failed = cache.get_or_load("x", Duration::from_secs(5), || Err("boom".to_owned()));
        let cached = cache.get_or_load("x", Duration::from_secs(5), || Ok(8));
        std::thread::sleep(Duration::from_millis(150));
        let retried = cache.get_or_load("x", Duration::from_secs(5), || Ok(9));

        assert_eq!(failed, Err(LoadError::Failed("boom".to_owned())));
        assert_eq!(cached, Err(LoadError::Failed("boom".to_owned())));
        assert_eq!(retried.map(|v| *v), Ok(9));
    }

    #[test]
    fn remove_clears_a_cached_failure() {
        let cache = TapeCache::<u32>::new(2);
        let _ = cache.get_or_load("x", Duration::from_secs(5), || Err("boom".to_owned()));
        cache.remove("x");
        let retried = cache.get_or_load("x", Duration::from_secs(5), || Ok(9));
        assert_eq!(retried.map(|v| *v), Ok(9));
    }

    #[test]
    fn a_panicking_loader_fails_the_waiters_instead_of_hanging() {
        let cache = TapeCache::<u32>::with_failure_ttl(2, Duration::ZERO);
        let started = Instant::now();
        let failed = cache.get_or_load("p", Duration::from_secs(5), || panic!("decode bug"));
        assert_eq!(
            failed,
            Err(LoadError::Failed("tape loader panicked".to_owned()))
        );
        assert!(started.elapsed() < Duration::from_secs(4));
        let retried = cache.get_or_load("p", Duration::from_secs(5), || Ok(3));
        assert_eq!(retried.map(|v| *v), Ok(3));
    }

    #[test]
    fn remove_drops_the_entry() {
        let cache = TapeCache::<u32>::new(2);
        cache
            .get_or_load("a", Duration::from_secs(5), || Ok(1))
            .unwrap();

        cache.remove("a");

        assert!(cache.get("a").is_none());
        assert!(cache.keys().is_empty());
    }
}
