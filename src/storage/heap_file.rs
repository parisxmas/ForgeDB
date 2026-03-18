use crate::common::*;
use crate::error::{ForgeError, Result};
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_page;

/// A heap file is a collection of slotted pages linked together, belonging to
/// a single table.
pub struct HeapFile {
    pub table_id: TableId,
    pub first_page_id: PageId,
}

impl HeapFile {
    /// Create a handle to a heap file.
    pub fn new(table_id: TableId, first_page_id: PageId) -> Self {
        Self {
            table_id,
            first_page_id,
        }
    }

    /// Insert a tuple into the heap file. Walks the linked list of pages
    /// looking for one with enough free space, and allocates a new page if
    /// none is found. Returns the RID of the inserted tuple.
    pub fn insert_tuple(&self, bpm: &mut LocalBpm, data: &[u8]) -> Result<RID> {
        let mut current_pid = self.first_page_id;

        loop {
            bpm.fetch_page(current_pid)?;
            {
                let page = bpm.get_page_mut(current_pid);
                if let Some(slot_id) = heap_page::insert_tuple(&mut page.data, data) {
                    let rid = RID {
                        page_id: current_pid,
                        slot_id,
                    };
                    bpm.unpin_page(current_pid, true)?;
                    return Ok(rid);
                }
            }

            // Check if there is a next page.
            let next_pid = {
                let page = bpm.get_page(current_pid);
                heap_page::get_next_page_id(&page.data)
            };
            bpm.unpin_page(current_pid, false)?;

            if next_pid == INVALID_PAGE_ID {
                break;
            }
            current_pid = PageId(next_pid);
        }

        // No existing page had space; allocate a new page and link it.
        let new_pid = bpm.new_page()?;
        {
            let new_page = bpm.get_page_mut(new_pid);
            heap_page::init(&mut new_page.data);
        }

        // Link the new page from the last page.
        bpm.unpin_page(new_pid, true)?;
        bpm.fetch_page(current_pid)?;
        {
            let last_page = bpm.get_page_mut(current_pid);
            heap_page::set_next_page_id(&mut last_page.data, new_pid.0);
        }
        bpm.unpin_page(current_pid, true)?;

        // Insert the tuple into the new page.
        bpm.fetch_page(new_pid)?;
        let slot_id = {
            let new_page = bpm.get_page_mut(new_pid);
            heap_page::insert_tuple(&mut new_page.data, data)
                .ok_or_else(|| ForgeError::Page("tuple too large for empty page".to_string()))?
        };
        bpm.unpin_page(new_pid, true)?;

        Ok(RID {
            page_id: new_pid,
            slot_id,
        })
    }

    /// Read a tuple by its RID.
    pub fn get_tuple(&self, bpm: &mut LocalBpm, rid: RID) -> Result<Vec<u8>> {
        bpm.fetch_page(rid.page_id)?;
        let data = {
            let page = bpm.get_page(rid.page_id);
            heap_page::get_tuple(&page.data, rid.slot_id)
        };
        bpm.unpin_page(rid.page_id, false)?;
        data.ok_or_else(|| ForgeError::Tuple(format!("tuple not found at {:?}", rid)))
    }

    /// Delete a tuple by its RID.
    pub fn delete_tuple(&self, bpm: &mut LocalBpm, rid: RID) -> Result<()> {
        bpm.fetch_page(rid.page_id)?;
        let ok = {
            let page = bpm.get_page_mut(rid.page_id);
            heap_page::delete_tuple(&mut page.data, rid.slot_id)
        };
        bpm.unpin_page(rid.page_id, true)?;
        if ok {
            Ok(())
        } else {
            Err(ForgeError::Tuple(format!(
                "failed to delete tuple at {:?}",
                rid
            )))
        }
    }

    /// Update a tuple. If the new data fits in the existing slot, the update
    /// is performed in-place and the original RID is returned. Otherwise, the
    /// old tuple is deleted and a new one is inserted, returning the new RID.
    pub fn update_tuple(
        &self,
        bpm: &mut LocalBpm,
        rid: RID,
        data: &[u8],
    ) -> Result<RID> {
        bpm.fetch_page(rid.page_id)?;
        let in_place = {
            let page = bpm.get_page_mut(rid.page_id);
            heap_page::update_tuple(&mut page.data, rid.slot_id, data)
        };

        if in_place {
            bpm.unpin_page(rid.page_id, true)?;
            return Ok(rid);
        }

        // Delete the old tuple and insert a new one.
        {
            let page = bpm.get_page_mut(rid.page_id);
            heap_page::delete_tuple(&mut page.data, rid.slot_id);
        }
        bpm.unpin_page(rid.page_id, true)?;

        self.insert_tuple(bpm, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::concurrent_bpm::ConcurrentBufferPool;
    use crate::storage::local_bpm::LocalBpm;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::heap_page;

    fn setup() -> (HeapFile, ConcurrentBufferPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(path.to_str().unwrap()).unwrap();
        let cbpm = ConcurrentBufferPool::new(10, dm);

        let first_pid;
        {
            let mut bpm = LocalBpm::new(&cbpm);
            // Allocate the first page and initialize it as a slotted page.
            first_pid = bpm.new_page().unwrap();
            {
                let page = bpm.get_page_mut(first_pid);
                heap_page::init(&mut page.data);
            }
            bpm.unpin_page(first_pid, true).unwrap();
        }

        let hf = HeapFile::new(TableId(0), first_pid);
        (hf, cbpm, dir)
    }

    #[test]
    fn test_insert_and_get() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);
        let rid = hf.insert_tuple(&mut bpm, b"hello").unwrap();
        let data = hf.get_tuple(&mut bpm, rid).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn test_delete() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);
        let rid = hf.insert_tuple(&mut bpm, b"to be deleted").unwrap();
        hf.delete_tuple(&mut bpm, rid).unwrap();
        let result = hf.get_tuple(&mut bpm, rid);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_in_place() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);
        let rid = hf.insert_tuple(&mut bpm, b"hello world").unwrap();
        let new_rid = hf.update_tuple(&mut bpm, rid, b"hi").unwrap();
        // In-place update keeps the same RID.
        assert_eq!(new_rid, rid);
        let data = hf.get_tuple(&mut bpm, new_rid).unwrap();
        assert_eq!(data, b"hi");
    }

    #[test]
    fn test_update_larger() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);
        let rid = hf.insert_tuple(&mut bpm, b"hi").unwrap();
        let new_rid = hf
            .update_tuple(&mut bpm, rid, b"this is much longer data")
            .unwrap();
        // The RID may change when the update does not fit in-place.
        let data = hf.get_tuple(&mut bpm, new_rid).unwrap();
        assert_eq!(data, b"this is much longer data");
    }

    #[test]
    fn test_multiple_pages() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);
        // Insert enough tuples to force a second page allocation.
        let big = vec![0xABu8; 2000];
        let mut rids = Vec::new();
        for _ in 0..20 {
            let rid = hf.insert_tuple(&mut bpm, &big).unwrap();
            rids.push(rid);
        }

        // Verify all tuples can be read back.
        for rid in &rids {
            let data = hf.get_tuple(&mut bpm, *rid).unwrap();
            assert_eq!(data, big);
        }

        // Confirm we used more than one page.
        let distinct_pages: std::collections::HashSet<_> =
            rids.iter().map(|r| r.page_id).collect();
        assert!(distinct_pages.len() > 1);
    }
}
