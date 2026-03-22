use crate::common::*;
use crate::error::Result;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_page;

/// An iterator that walks through all non-deleted tuples in a heap file's
/// linked list of slotted pages.
pub struct TableIterator {
    current_page_id: PageId,
    current_slot: u16,
}

impl TableIterator {
    /// Create a new iterator starting from the first page of a heap file.
    pub fn new(first_page_id: PageId) -> Self {
        Self {
            current_page_id: first_page_id,
            current_slot: 0,
        }
    }

    /// Collect all page IDs in the linked list starting from the first page.
    /// This traverses the page chain reading only headers, not tuple data.
    pub fn collect_page_ids(
        first_page_id: PageId,
        bpm: &mut LocalBpm,
    ) -> Result<Vec<PageId>> {
        let mut page_ids = Vec::new();
        let mut current = first_page_id;

        loop {
            if current.0 == INVALID_PAGE_ID {
                break;
            }

            bpm.fetch_page(current)?;
            page_ids.push(current);

            let next_pid = {
                let page = bpm.get_page(current);
                heap_page::get_next_page_id(&page.data)
            };
            bpm.unpin_page(current, false)?;

            current = PageId(next_pid);
        }

        Ok(page_ids)
    }

    /// Scan tuples only from a specific set of page IDs.
    /// This is used by parallel scan to process a chunk of pages.
    pub fn scan_pages(
        page_ids: &[PageId],
        bpm: &mut LocalBpm,
    ) -> Result<Vec<(RID, Vec<u8>)>> {
        let mut results = Vec::new();

        for &page_id in page_ids {
            bpm.fetch_page(page_id)?;

            let num_slots = {
                let page = bpm.get_page(page_id);
                heap_page::get_num_slots(&page.data)
            };

            for slot_id in 0..num_slots {
                let tuple_data = {
                    let page = bpm.get_page(page_id);
                    heap_page::get_tuple(&page.data, slot_id)
                };

                if let Some(data) = tuple_data {
                    let rid = RID {
                        page_id,
                        slot_id,
                    };
                    results.push((rid, data));
                }
            }

            bpm.unpin_page(page_id, false)?;
        }

        Ok(results)
    }

    /// Advance the iterator, returning the next `(RID, tuple_data)` pair, or
    /// `None` when all tuples have been visited.
    pub fn next(
        &mut self,
        bpm: &mut LocalBpm,
    ) -> Result<Option<(RID, Vec<u8>)>> {
        loop {
            // Check if we have run out of pages.
            if self.current_page_id.0 == INVALID_PAGE_ID {
                return Ok(None);
            }

            bpm.fetch_page(self.current_page_id)?;

            let num_slots = {
                let page = bpm.get_page(self.current_page_id);
                heap_page::get_num_slots(&page.data)
            };

            // Scan slots on the current page.
            while self.current_slot < num_slots {
                let slot_id = self.current_slot;
                self.current_slot += 1;

                let tuple_data = {
                    let page = bpm.get_page(self.current_page_id);
                    heap_page::get_tuple(&page.data, slot_id)
                };

                if let Some(data) = tuple_data {
                    let rid = RID {
                        page_id: self.current_page_id,
                        slot_id,
                    };
                    bpm.unpin_page(self.current_page_id, false)?;
                    return Ok(Some((rid, data)));
                }
            }

            // Move to the next page in the linked list.
            let next_pid = {
                let page = bpm.get_page(self.current_page_id);
                heap_page::get_next_page_id(&page.data)
            };
            bpm.unpin_page(self.current_page_id, false)?;

            self.current_page_id = PageId(next_pid);
            self.current_slot = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::concurrent_bpm::ConcurrentBufferPool;
    use crate::storage::local_bpm::LocalBpm;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::heap_file::HeapFile;
    use crate::storage::heap_page;

    fn setup() -> (HeapFile, ConcurrentBufferPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(&path).unwrap();
        let cbpm = ConcurrentBufferPool::new(10, dm);

        let first_pid;
        {
            let mut bpm = LocalBpm::new(&cbpm);
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
    fn test_empty_table() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);
        let mut iter = TableIterator::new(hf.first_page_id);
        let result = iter.next(&mut bpm).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_iterate_all() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);

        let mut expected = Vec::new();
        for i in 0u32..5 {
            let data = format!("tuple_{}", i);
            let rid = hf.insert_tuple(&mut bpm, data.as_bytes()).unwrap();
            expected.push((rid, data.into_bytes()));
        }

        let mut iter = TableIterator::new(hf.first_page_id);
        let mut collected = Vec::new();
        while let Some((rid, data)) = iter.next(&mut bpm).unwrap() {
            collected.push((rid, data));
        }

        assert_eq!(collected.len(), expected.len());
        for (exp, got) in expected.iter().zip(collected.iter()) {
            assert_eq!(exp.0, got.0);
            assert_eq!(exp.1, got.1);
        }
    }

    #[test]
    fn test_iterate_across_pages() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);

        // Insert enough large tuples to span multiple pages.
        let big = vec![0xCDu8; 500];
        let mut rids = Vec::new();
        for _ in 0..20 {
            let rid = hf.insert_tuple(&mut bpm, &big).unwrap();
            rids.push(rid);
        }

        let mut iter = TableIterator::new(hf.first_page_id);
        let mut count = 0;
        while let Some((_rid, data)) = iter.next(&mut bpm).unwrap() {
            assert_eq!(data, big);
            count += 1;
        }
        assert_eq!(count, 20);
    }

    #[test]
    fn test_iterate_skips_deleted() {
        let (hf, cbpm, _dir) = setup();
        let mut bpm = LocalBpm::new(&cbpm);

        let r0 = hf.insert_tuple(&mut bpm, b"aaa").unwrap();
        let _r1 = hf.insert_tuple(&mut bpm, b"bbb").unwrap();
        let r2 = hf.insert_tuple(&mut bpm, b"ccc").unwrap();

        // Delete the first and third tuples.
        hf.delete_tuple(&mut bpm, r0).unwrap();
        hf.delete_tuple(&mut bpm, r2).unwrap();

        let mut iter = TableIterator::new(hf.first_page_id);
        let mut collected = Vec::new();
        while let Some((_rid, data)) = iter.next(&mut bpm).unwrap() {
            collected.push(data);
        }

        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0], b"bbb");
    }
}
