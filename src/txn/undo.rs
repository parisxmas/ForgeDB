//! Undo log for transaction rollback.
//!
//! Each write operation (INSERT, DELETE, UPDATE) records an undo entry.
//! On ROLLBACK, entries are applied in reverse order to restore the
//! previous state. This is in-memory only — if the process crashes,
//! MVCC visibility handles correctness (uncommitted xmin = invisible).

use crate::common::RID;

/// A single undo operation.
#[derive(Debug, Clone)]
pub enum UndoEntry {
    /// Undo an INSERT: physically delete the inserted row.
    InsertUndo {
        table_name: String,
        rid: RID,
    },
    /// Undo a DELETE: re-insert the old tuple data.
    DeleteUndo {
        table_name: String,
        rid: RID,
        old_xmax: u64,
        old_data: Vec<u8>,
    },
    /// Undo an UPDATE: delete the new version and re-insert the old data.
    UpdateUndo {
        table_name: String,
        old_rid: RID,
        old_xmax: u64,
        new_rid: RID,
        old_data: Vec<u8>,
    },
}

/// The undo log for a single transaction.
#[derive(Debug, Default)]
pub struct UndoLog {
    entries: Vec<UndoEntry>,
}

impl UndoLog {
    /// Create a new empty undo log.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Record an undo entry.
    pub fn push(&mut self, entry: UndoEntry) {
        self.entries.push(entry);
    }

    /// Get the entries in reverse order for rollback.
    pub fn entries_reversed(&self) -> impl Iterator<Item = &UndoEntry> {
        self.entries.iter().rev()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Consume the undo log and return its entries.
    pub fn into_entries(self) -> Vec<UndoEntry> {
        self.entries
    }

    /// Create an undo log from existing entries.
    pub fn from_entries(entries: Vec<UndoEntry>) -> Self {
        Self { entries }
    }
}

/// Apply undo entries in reverse order to physically reverse DML.
/// Used by ROLLBACK and auto-transaction error recovery.
pub fn apply_undo(
    log: &UndoLog,
    bpm: &mut crate::storage::local_bpm::LocalBpm,
    catalog: &crate::catalog::Catalog,
) -> crate::error::Result<()> {
    use crate::storage::heap_file::HeapFile;

    for entry in log.entries_reversed() {
        match entry {
            UndoEntry::InsertUndo { table_name, rid } => {
                // Remove the inserted row
                if let Some(info) = catalog.get_table(table_name) {
                    let heap = HeapFile::new(info.table_id, info.first_page_id);
                    let _ = heap.delete_tuple(bpm, *rid);
                }
            }
            UndoEntry::DeleteUndo { table_name, old_data, .. } => {
                // Re-insert the deleted row's original data
                if let Some(info) = catalog.get_table(table_name) {
                    let heap = HeapFile::new(info.table_id, info.first_page_id);
                    let _ = heap.insert_tuple(bpm, old_data);
                }
            }
            UndoEntry::UpdateUndo { table_name, new_rid, old_data, .. } => {
                // Delete the new version and re-insert the old data
                if let Some(info) = catalog.get_table(table_name) {
                    let heap = HeapFile::new(info.table_id, info.first_page_id);
                    let _ = heap.delete_tuple(bpm, *new_rid);
                    let _ = heap.insert_tuple(bpm, old_data);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PageId;

    #[test]
    fn test_undo_log_push_and_reverse() {
        let mut log = UndoLog::new();
        let rid1 = RID { page_id: PageId(1), slot_id: 0 };
        let rid2 = RID { page_id: PageId(2), slot_id: 1 };
        let rid3 = RID { page_id: PageId(3), slot_id: 2 };

        log.push(UndoEntry::InsertUndo {
            table_name: "t".into(),
            rid: rid1,
        });
        log.push(UndoEntry::DeleteUndo {
            table_name: "t".into(),
            rid: rid2,
            old_xmax: 0,
            old_data: vec![],
        });
        log.push(UndoEntry::UpdateUndo {
            table_name: "t".into(),
            old_rid: rid2,
            old_xmax: 0,
            new_rid: rid3,
            old_data: vec![],
        });

        assert_eq!(log.len(), 3);

        // Reverse order
        let reversed: Vec<_> = log.entries_reversed().collect();
        assert!(matches!(reversed[0], UndoEntry::UpdateUndo { .. }));
        assert!(matches!(reversed[1], UndoEntry::DeleteUndo { .. }));
        assert!(matches!(reversed[2], UndoEntry::InsertUndo { .. }));
    }

    #[test]
    fn test_empty_undo_log() {
        let log = UndoLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.entries_reversed().count(), 0);
    }
}
