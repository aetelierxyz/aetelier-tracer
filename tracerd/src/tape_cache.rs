use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

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
}

pub struct TapeCache<T> {
    capacity: usize,
    inner: Mutex<Inner<T>>,
}

impl<T: Send + Sync + 'static> TapeCache<T> {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity.max(1),
            inner: Mutex::new(Inner {
                entries: Vec::new(),
                loading: HashMap::new(),
            }),
        })
    }

    pub fn get(&self, key: &str) -> Option<Arc<T>> {
        let mut inner = self.inner.lock().unwrap();
        let position = inner.entries.iter().position(|(k, _)| k == key)?;
        let entry = inner.entries.remove(position);
        let value = entry.1.clone();
        inner.entries.push(entry);
        Some(value)
    }

    pub fn remove(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.retain(|(k, _)| k != key);
    }

    #[cfg(test)]
    pub fn keys(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        inner.entries.iter().map(|(k, _)| k.clone()).collect()
    }

    pub fn get_or_load(
        self: &Arc<Self>,
        key: &str,
        budget: Duration,
        load: impl FnOnce() -> Result<T, String> + Send + 'static,
    ) -> Result<Arc<T>, LoadError> {
        if let Some(value) = self.get(key) {
            return Ok(value);
        }
        let gate = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(gate) = inner.loading.get(key) {
                gate.clone()
            } else {
                let gate = Arc::new(Gate {
                    done: Mutex::new(None),
                    ready: Condvar::new(),
                });
                inner.loading.insert(key.to_owned(), gate.clone());
                let cache = self.clone();
                let key = key.to_owned();
                let gate_for_thread = gate.clone();
                std::thread::spawn(move || {
                    let outcome = load().map(Arc::new);
                    cache.finish(&key, &gate_for_thread, outcome);
                });
                gate
            }
        };
        let guard = gate.done.lock().unwrap();
        let (guard, _) = gate
            .ready
            .wait_timeout_while(guard, budget, |done| done.is_none())
            .unwrap();
        match guard.as_ref() {
            Some(Ok(value)) => Ok(value.clone()),
            Some(Err(reason)) => Err(LoadError::Failed(reason.clone())),
            None => Err(LoadError::Loading),
        }
    }

    fn finish(&self, key: &str, gate: &Gate<T>, outcome: Outcome<T>) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.loading.remove(key);
            if let Ok(value) = &outcome {
                inner.entries.retain(|(k, _)| k != key);
                inner.entries.push((key.to_owned(), value.clone()));
                while inner.entries.len() > self.capacity {
                    inner.entries.remove(0);
                }
            }
        }
        *gate.done.lock().unwrap() = Some(outcome);
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
    fn a_failed_load_is_reported_and_not_cached() {
        let cache = TapeCache::<u32>::new(2);

        let failed = cache.get_or_load("x", Duration::from_secs(5), || Err("boom".to_owned()));
        let retried = cache.get_or_load("x", Duration::from_secs(5), || Ok(9));

        assert_eq!(failed, Err(LoadError::Failed("boom".to_owned())));
        assert_eq!(retried.map(|v| *v), Ok(9));
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
