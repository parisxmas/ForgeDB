use std::collections::HashSet;

use crate::common::*;
use crate::error::{ForgeError, Result};
use crate::storage::BufferPoolManager;
use super::wal::{Wal, WalRecord};

/// Manages transactions: begin, commit, abort, WAL logging, and crash recovery.
pub struct TransactionManager {
    next_txn_id: u64,
    active_txns: HashSet<TxnId>,
    wal: Wal,
}

impl TransactionManager {
    /// Create a new `TransactionManager` with a WAL at the given path.
    pub fn new(wal_path: &str) -> Result<Self> {
        let wal = Wal::new(wal_path)?;
        Ok(Self {
            next_txn_id: 1,
            active_txns: HashSet::new(),
            wal,
        })
    }

    /// Begin a new transaction. Returns the assigned `TxnId`.
    pub fn begin(&mut self) -> Result<TxnId> {
        let txn_id = TxnId(self.next_txn_id);
        self.next_txn_id += 1;
        self.active_txns.insert(txn_id);
        self.wal.append(&WalRecord::Begin(txn_id))?;
        Ok(txn_id)
    }

    /// Commit a transaction: write a Commit record and remove from active set.
    pub fn commit(&mut self, txn_id: TxnId) -> Result<()> {
        if !self.active_txns.contains(&txn_id) {
            return Err(ForgeError::Transaction(format!(
                "transaction {:?} is not active",
                txn_id
            )));
        }
        self.wal.append(&WalRecord::Commit(txn_id))?;
        self.active_txns.remove(&txn_id);
        Ok(())
    }

    /// Abort a transaction: write an Abort record and remove from active set.
    pub fn abort(&mut self, txn_id: TxnId) -> Result<()> {
        if !self.active_txns.contains(&txn_id) {
            return Err(ForgeError::Transaction(format!(
                "transaction {:?} is not active",
                txn_id
            )));
        }
        self.wal.append(&WalRecord::Abort(txn_id))?;
        self.active_txns.remove(&txn_id);
        Ok(())
    }

    /// Log a page write (before/after images) for the given transaction.
    pub fn log_page_write(
        &mut self,
        txn_id: TxnId,
        page_id: PageId,
        before: &[u8; PAGE_SIZE],
        after: &[u8; PAGE_SIZE],
    ) -> Result<()> {
        let mut before_image = Box::new([0u8; PAGE_SIZE]);
        before_image.copy_from_slice(before);

        let mut after_image = Box::new([0u8; PAGE_SIZE]);
        after_image.copy_from_slice(after);

        self.wal.append(&WalRecord::PageWrite {
            txn_id,
            page_id,
            before_image,
            after_image,
        })?;
        Ok(())
    }

    /// Redo-only recovery: read all WAL records, determine which transactions
    /// committed, then replay their PageWrite after-images via the buffer pool.
    pub fn recover(&mut self, bpm: &mut BufferPoolManager) -> Result<()> {
        let records = self.wal.read_all_records()?;

        // First pass: determine which transactions committed.
        let mut committed: HashSet<TxnId> = HashSet::new();
        for record in &records {
            if let WalRecord::Commit(txn_id) = record {
                committed.insert(*txn_id);
            }
        }

        // Second pass: redo page writes for committed transactions.
        for record in &records {
            if let WalRecord::PageWrite {
                txn_id,
                page_id,
                after_image,
                ..
            } = record
            {
                if committed.contains(txn_id) {
                    // Fetch the page into the buffer pool, apply the after-image,
                    // then unpin as dirty so it will be flushed.
                    bpm.fetch_page(*page_id)?;
                    let page = bpm.get_page_mut(*page_id);
                    page.data.copy_from_slice(after_image.as_ref());
                    bpm.unpin_page(*page_id, true)?;
                }
            }
        }

        Ok(())
    }

    /// Checkpoint: flush all dirty pages, write a Checkpoint record, truncate WAL.
    pub fn checkpoint(&mut self, bpm: &mut BufferPoolManager) -> Result<()> {
        bpm.flush_all()?;
        self.wal.append(&WalRecord::Checkpoint)?;
        self.wal.truncate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;

    /// Helper: create a `BufferPoolManager` backed by a temp directory.
    fn make_bpm(
        dir: &tempfile::TempDir,
        pool_size: usize,
    ) -> BufferPoolManager {
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(&path).unwrap();
        BufferPoolManager::new(pool_size, dm)
    }

    #[test]
    fn test_begin_commit_writes_records() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut tm = TransactionManager::new(wal_str).unwrap();

        let txn1 = tm.begin().unwrap();
        assert_eq!(txn1, TxnId(1));

        let txn2 = tm.begin().unwrap();
        assert_eq!(txn2, TxnId(2));

        tm.commit(txn1).unwrap();
        tm.commit(txn2).unwrap();

        // Re-read the WAL to verify records.
        let records = tm.wal.read_all_records().unwrap();
        assert_eq!(records.len(), 4);
        assert!(matches!(&records[0], WalRecord::Begin(TxnId(1))));
        assert!(matches!(&records[1], WalRecord::Begin(TxnId(2))));
        assert!(matches!(&records[2], WalRecord::Commit(TxnId(1))));
        assert!(matches!(&records[3], WalRecord::Commit(TxnId(2))));
    }

    #[test]
    fn test_page_write_record_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut tm = TransactionManager::new(wal_str).unwrap();
        let txn = tm.begin().unwrap();

        let before = [0u8; PAGE_SIZE];
        let mut after = [0u8; PAGE_SIZE];
        after[0] = 0xDE;
        after[100] = 0xAD;
        after[PAGE_SIZE - 1] = 0xFF;

        tm.log_page_write(txn, PageId(3), &before, &after).unwrap();
        tm.commit(txn).unwrap();

        let records = tm.wal.read_all_records().unwrap();
        // Begin, PageWrite, Commit
        assert_eq!(records.len(), 3);

        match &records[1] {
            WalRecord::PageWrite {
                txn_id,
                page_id,
                before_image,
                after_image,
            } => {
                assert_eq!(*txn_id, txn);
                assert_eq!(*page_id, PageId(3));
                assert_eq!(before_image[0], 0);
                assert_eq!(after_image[0], 0xDE);
                assert_eq!(after_image[100], 0xAD);
                assert_eq!(after_image[PAGE_SIZE - 1], 0xFF);
            }
            _ => panic!("expected PageWrite record"),
        }
    }

    #[test]
    fn test_recovery_replays_committed_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        // Phase 1: write data through a transaction manager + buffer pool,
        // then simulate a "crash" by dropping them without checkpoint.
        {
            let mut bpm = make_bpm(&dir, 4);
            let mut tm = TransactionManager::new(wal_str).unwrap();

            // Allocate a page and remember its id.
            let pid = bpm.new_page().unwrap();
            assert_eq!(pid, PageId(0));

            let txn = tm.begin().unwrap();

            let before = [0u8; PAGE_SIZE];
            let mut after = [0u8; PAGE_SIZE];
            after[0] = 0x42;
            after[1] = 0x43;

            tm.log_page_write(txn, pid, &before, &after).unwrap();

            // Apply the write to the in-memory page.
            bpm.get_page_mut(pid).data.copy_from_slice(&after);
            bpm.unpin_page(pid, true).unwrap();

            tm.commit(txn).unwrap();

            // Simulate crash: do NOT flush or checkpoint.
            // The dirty page data is lost, but the WAL survives.
        }

        // Phase 2: recover using the WAL.
        {
            let mut bpm = make_bpm(&dir, 4);
            let mut tm = TransactionManager::new(wal_str).unwrap();

            tm.recover(&mut bpm).unwrap();

            // The page should now have the committed after-image.
            bpm.fetch_page(PageId(0)).unwrap();
            let page = bpm.get_page(PageId(0));
            assert_eq!(page.data[0], 0x42);
            assert_eq!(page.data[1], 0x43);
            bpm.unpin_page(PageId(0), false).unwrap();
        }
    }

    #[test]
    fn test_recovery_skips_aborted_writes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        // Phase 1: one committed txn, one aborted txn that writes to the same page.
        {
            let mut bpm = make_bpm(&dir, 4);
            let mut tm = TransactionManager::new(wal_str).unwrap();

            let pid = bpm.new_page().unwrap();
            bpm.unpin_page(pid, false).unwrap();

            // Committed transaction: writes 0xAA to page 0.
            let txn1 = tm.begin().unwrap();
            let before1 = [0u8; PAGE_SIZE];
            let mut after1 = [0u8; PAGE_SIZE];
            after1[0] = 0xAA;
            tm.log_page_write(txn1, pid, &before1, &after1).unwrap();
            tm.commit(txn1).unwrap();

            // Aborted transaction: writes 0xFF to page 0.
            let txn2 = tm.begin().unwrap();
            let mut after2 = [0u8; PAGE_SIZE];
            after2[0] = 0xFF;
            tm.log_page_write(txn2, pid, &after1, &after2).unwrap();
            tm.abort(txn2).unwrap();

            // Simulate crash.
        }

        // Phase 2: recover. Only committed writes should be replayed.
        {
            let mut bpm = make_bpm(&dir, 4);
            let mut tm = TransactionManager::new(wal_str).unwrap();

            tm.recover(&mut bpm).unwrap();

            bpm.fetch_page(PageId(0)).unwrap();
            let page = bpm.get_page(PageId(0));
            // Should see the committed value, NOT the aborted one.
            assert_eq!(page.data[0], 0xAA);
            bpm.unpin_page(PageId(0), false).unwrap();
        }
    }

    #[test]
    fn test_checkpoint_truncates_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut bpm = make_bpm(&dir, 4);
        let mut tm = TransactionManager::new(wal_str).unwrap();

        let txn = tm.begin().unwrap();
        tm.commit(txn).unwrap();

        // Before checkpoint, WAL has records.
        let records = tm.wal.read_all_records().unwrap();
        assert_eq!(records.len(), 2);

        tm.checkpoint(&mut bpm).unwrap();

        // After checkpoint, WAL should be empty.
        let records = tm.wal.read_all_records().unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_commit_non_active_txn_fails() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut tm = TransactionManager::new(wal_str).unwrap();
        let result = tm.commit(TxnId(999));
        assert!(result.is_err());
    }

    #[test]
    fn test_abort_non_active_txn_fails() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("txn.wal");
        let wal_str = wal_path.to_str().unwrap();

        let mut tm = TransactionManager::new(wal_str).unwrap();
        let result = tm.abort(TxnId(999));
        assert!(result.is_err());
    }
}
