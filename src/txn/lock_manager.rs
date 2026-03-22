//! Row-level lock manager with deadlock detection and timeout.
//!
//! Provides shared (read) and exclusive (write) locks on (table, row_id) pairs.
//! Deadlock detection uses a wait-for graph cycle check.
//! Locks are automatically released when a transaction commits or aborts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::common::TxnId;

/// Lock mode: shared (many readers) or exclusive (single writer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// A lock target: (table_name, row_key) where row_key is a string
/// representation of the primary key or RID.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct LockTarget {
    pub table: String,
    pub key: String,
}

/// A single lock entry.
struct LockEntry {
    /// Transactions holding a shared lock.
    shared_holders: HashSet<TxnId>,
    /// Transaction holding the exclusive lock (if any).
    exclusive_holder: Option<TxnId>,
}

/// Row-level lock manager with deadlock detection.
pub struct LockManager {
    inner: Mutex<LockManagerInner>,
    cond: Condvar,
}

struct LockManagerInner {
    locks: HashMap<LockTarget, LockEntry>,
    /// Wait-for graph: txn_id -> set of txn_ids it's waiting on.
    wait_for: HashMap<TxnId, HashSet<TxnId>>,
    /// Default lock timeout.
    lock_timeout: Duration,
}

/// Error type for lock operations.
#[derive(Debug)]
pub enum LockError {
    Deadlock(TxnId),
    Timeout(TxnId),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Deadlock(txn) => write!(f, "deadlock detected for transaction {:?}", txn),
            LockError::Timeout(txn) => write!(f, "lock timeout for transaction {:?}", txn),
        }
    }
}

impl LockManager {
    /// Create a new lock manager with the given timeout for lock waits.
    pub fn new(lock_timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(LockManagerInner {
                locks: HashMap::new(),
                wait_for: HashMap::new(),
                lock_timeout,
            }),
            cond: Condvar::new(),
        }
    }

    /// Acquire a lock on a target. Blocks until the lock is granted,
    /// or returns an error on deadlock/timeout.
    pub fn acquire(
        &self,
        txn_id: TxnId,
        target: &LockTarget,
        mode: LockMode,
    ) -> Result<(), LockError> {
        let deadline = {
            let inner = self.inner.lock().unwrap();
            Instant::now() + inner.lock_timeout
        };

        let mut inner = self.inner.lock().unwrap();

        loop {
            let entry = inner.locks.entry(target.clone()).or_insert_with(|| LockEntry {
                shared_holders: HashSet::new(),
                exclusive_holder: None,
            });

            match mode {
                LockMode::Shared => {
                    if entry.exclusive_holder.is_none() || entry.exclusive_holder == Some(txn_id) {
                        // No exclusive lock held (or we hold it) — grant shared
                        entry.shared_holders.insert(txn_id);
                        inner.wait_for.remove(&txn_id);
                        return Ok(());
                    }
                    // Must wait — record in wait-for graph
                    if let Some(holder) = entry.exclusive_holder {
                        inner.wait_for.entry(txn_id).or_default().insert(holder);
                    }
                }
                LockMode::Exclusive => {
                    let can_grant =
                        (entry.exclusive_holder.is_none() || entry.exclusive_holder == Some(txn_id))
                        && (entry.shared_holders.is_empty()
                            || (entry.shared_holders.len() == 1 && entry.shared_holders.contains(&txn_id)));

                    if can_grant {
                        // Grant exclusive lock (upgrade from shared if needed)
                        entry.shared_holders.remove(&txn_id);
                        entry.exclusive_holder = Some(txn_id);
                        inner.wait_for.remove(&txn_id);
                        return Ok(());
                    }
                    // Must wait — record in wait-for graph
                    let mut blocking = HashSet::new();
                    if let Some(holder) = entry.exclusive_holder {
                        if holder != txn_id { blocking.insert(holder); }
                    }
                    for holder in &entry.shared_holders {
                        if *holder != txn_id { blocking.insert(*holder); }
                    }
                    inner.wait_for.insert(txn_id, blocking);
                }
            }

            // Check for deadlock before waiting
            if Self::has_cycle(&inner.wait_for, txn_id) {
                inner.wait_for.remove(&txn_id);
                return Err(LockError::Deadlock(txn_id));
            }

            // Wait with timeout
            let now = Instant::now();
            if now >= deadline {
                inner.wait_for.remove(&txn_id);
                return Err(LockError::Timeout(txn_id));
            }
            let remaining = deadline - now;
            let (new_inner, timeout_result) = self.cond.wait_timeout(inner, remaining).unwrap();
            inner = new_inner;
            if timeout_result.timed_out() {
                inner.wait_for.remove(&txn_id);
                return Err(LockError::Timeout(txn_id));
            }
        }
    }

    /// Release all locks held by a transaction (called on commit/abort).
    pub fn release_all(&self, txn_id: TxnId) {
        let mut inner = self.inner.lock().unwrap();
        let targets: Vec<LockTarget> = inner.locks.keys().cloned().collect();
        let mut released = false;
        for target in targets {
            if let Some(entry) = inner.locks.get_mut(&target) {
                if entry.exclusive_holder == Some(txn_id) {
                    entry.exclusive_holder = None;
                    released = true;
                }
                if entry.shared_holders.remove(&txn_id) {
                    released = true;
                }
                // Clean up empty entries
                if entry.exclusive_holder.is_none() && entry.shared_holders.is_empty() {
                    inner.locks.remove(&target);
                }
            }
        }
        inner.wait_for.remove(&txn_id);
        // Also remove this txn from other transactions' wait sets
        for waiters in inner.wait_for.values_mut() {
            waiters.remove(&txn_id);
        }
        if released {
            drop(inner);
            self.cond.notify_all();
        }
    }

    /// Detect a cycle in the wait-for graph starting from `start`.
    /// Returns true if `start` transitively waits for itself (deadlock).
    fn has_cycle(wait_for: &HashMap<TxnId, HashSet<TxnId>>, start: TxnId) -> bool {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(start);
        while let Some(txn) = queue.pop_front() {
            if !visited.insert(txn) {
                continue;
            }
            if let Some(deps) = wait_for.get(&txn) {
                for &dep in deps {
                    if dep == start {
                        return true; // cycle found
                    }
                    queue.push_back(dep);
                }
            }
        }
        false
    }

    /// Set the lock timeout duration.
    pub fn set_timeout(&self, timeout: Duration) {
        let mut inner = self.inner.lock().unwrap();
        inner.lock_timeout = timeout;
    }

    /// Get current lock timeout.
    pub fn get_timeout(&self) -> Duration {
        let inner = self.inner.lock().unwrap();
        inner.lock_timeout
    }
}

/// Isolation levels supported by ForgeDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Each statement sees only committed data as of statement start.
    ReadCommitted,
    /// Transaction sees a consistent snapshot from transaction start.
    RepeatableRead,
    /// Full serializability — detects and prevents phantoms.
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::RepeatableRead // ForgeDB default (matches snapshot isolation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_shared_locks_compatible() {
        let lm = LockManager::new(Duration::from_secs(5));
        let target = LockTarget { table: "t".into(), key: "1".into() };
        lm.acquire(TxnId(1), &target, LockMode::Shared).unwrap();
        lm.acquire(TxnId(2), &target, LockMode::Shared).unwrap();
        // Both hold shared lock — no conflict
        lm.release_all(TxnId(1));
        lm.release_all(TxnId(2));
    }

    #[test]
    fn test_exclusive_blocks_shared() {
        let lm = Arc::new(LockManager::new(Duration::from_millis(100)));
        let target = LockTarget { table: "t".into(), key: "1".into() };
        lm.acquire(TxnId(1), &target, LockMode::Exclusive).unwrap();

        let lm2 = Arc::clone(&lm);
        let h = thread::spawn(move || {
            let target = LockTarget { table: "t".into(), key: "1".into() };
            lm2.acquire(TxnId(2), &target, LockMode::Shared)
        });

        // TxnId(2) should timeout waiting for the exclusive lock
        thread::sleep(Duration::from_millis(50));
        lm.release_all(TxnId(1));
        let result = h.join().unwrap();
        assert!(result.is_ok()); // should succeed after release
    }

    #[test]
    fn test_deadlock_detection() {
        let lm = Arc::new(LockManager::new(Duration::from_secs(5)));

        let t1 = LockTarget { table: "t".into(), key: "1".into() };
        let t2 = LockTarget { table: "t".into(), key: "2".into() };

        // TxnId(1) holds lock on t1
        lm.acquire(TxnId(1), &t1, LockMode::Exclusive).unwrap();
        // TxnId(2) holds lock on t2
        lm.acquire(TxnId(2), &t2, LockMode::Exclusive).unwrap();

        // TxnId(1) tries to lock t2 (blocked by TxnId(2))
        let lm1 = Arc::clone(&lm);
        let h1 = thread::spawn(move || {
            let t2 = LockTarget { table: "t".into(), key: "2".into() };
            lm1.acquire(TxnId(1), &t2, LockMode::Exclusive)
        });

        thread::sleep(Duration::from_millis(50));

        // TxnId(2) tries to lock t1 (blocked by TxnId(1)) → deadlock
        let t1_copy = LockTarget { table: "t".into(), key: "1".into() };
        let result = lm.acquire(TxnId(2), &t1_copy, LockMode::Exclusive);
        assert!(matches!(result, Err(LockError::Deadlock(_))));

        // Release TxnId(2) so TxnId(1) can proceed
        lm.release_all(TxnId(2));
        let r1 = h1.join().unwrap();
        assert!(r1.is_ok());
        lm.release_all(TxnId(1));
    }

    #[test]
    fn test_timeout() {
        let lm = LockManager::new(Duration::from_millis(50));
        let target = LockTarget { table: "t".into(), key: "1".into() };
        lm.acquire(TxnId(1), &target, LockMode::Exclusive).unwrap();

        // TxnId(2) should timeout
        let result = lm.acquire(TxnId(2), &target, LockMode::Exclusive);
        assert!(matches!(result, Err(LockError::Timeout(_))));
        lm.release_all(TxnId(1));
    }
}
