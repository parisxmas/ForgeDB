use crate::common::*;
use crate::error::Result;
use crate::storage::buffer_pool::BufferPoolManager;
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

    /// Advance the iterator, returning the next `(RID, tuple_data)` pair, or
    /// `None` when all tuples have been visited.
    pub fn next(
        &mut self,
        bpm: &mut BufferPoolManager,
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
    use crate::storage::buffer_pool::BufferPoolManager;
    use crate::storage::disk_manager::DiskManager;
    use crate::storage::heap_file::HeapFile;
    use crate::storage::heap_page;

    fn setup() -> (HeapFile, BufferPoolManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(&path).unwrap();
        let mut bpm = BufferPoolManager::new(10, dm);

        let first_pid = bpm.new_page().unwrap();
        {
            let page = bpm.get_page_mut(first_pid);
            heap_page::init(&mut page.data);
        }
        bpm.unpin_page(first_pid, true).unwrap();

        let hf = HeapFile::new(TableId(0), first_pid);
        (hf, bpm, dir)
    }

    #[test]
    fn test_empty_table() {
        let (hf, mut bpm, _dir) = setup();
        let mut iter = TableIterator::new(hf.first_page_id);
        let result = iter.next(&mut bpm).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_iterate_all() {
        let (hf, mut bpm, _dir) = setup();

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
        let (hf, mut bpm, _dir) = setup();

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
        let (hf, mut bpm, _dir) = setup();

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
