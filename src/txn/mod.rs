pub mod mvcc;
pub mod undo;
pub mod wal;
pub mod transaction;
pub mod lock_manager;

pub use mvcc::Snapshot;
pub use undo::{UndoLog, UndoEntry};
pub use wal::{Wal, WalRecord};
pub use transaction::TransactionManager;
pub use lock_manager::{LockManager, LockMode, LockTarget, LockError, IsolationLevel};

use crate::common::TxnId;

/// Transaction context threaded through executors during DML.
/// Carries the identity, snapshot, and undo log for the active transaction.
pub struct TxnContext {
    pub txn_id: TxnId,
    pub snapshot: Snapshot,
    pub undo_log: UndoLog,
}
