use std::collections::{HashMap, VecDeque};

use crate::common::{PageId, INVALID_PAGE_ID};
use crate::error::{ForgeError, Result};
use crate::storage::disk_manager::DiskManager;
use crate::storage::page::Page;

/// A buffer pool that caches pages in memory and manages eviction via LRU.
pub struct BufferPoolManager {
    pool: Vec<Page>,
    page_table: HashMap<PageId, usize>,
    free_list: Vec<usize>,
    lru: VecDeque<usize>,
    disk_manager: DiskManager,
}

impl BufferPoolManager {
    /// Create a new buffer pool with the given number of page frames.
    pub fn new(pool_size: usize, disk_manager: DiskManager) -> Self {
        let mut pool = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            pool.push(Page::new(PageId(INVALID_PAGE_ID)));
            free_list.push(i);
        }

        Self {
            pool,
            page_table: HashMap::new(),
            free_list,
            lru: VecDeque::new(),
            disk_manager,
        }
    }

    /// Fetch a page into the buffer pool, returning its PageId on success.
    /// The page is pinned (pin_count incremented). Caller must unpin when done.
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<PageId> {
        // If already in the pool, pin it and return.
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.pool[frame_idx].pin_count += 1;
            // Remove from LRU since it is now pinned.
            self.lru.retain(|&idx| idx != frame_idx);
            return Ok(page_id);
        }

        // Need a frame: try free list, then evict.
        let frame_idx = self.find_free_frame()?;

        // Read the page from disk.
        let page = &mut self.pool[frame_idx];
        page.reset(page_id);
        self.disk_manager.read_page(page_id, &mut page.data)?;
        page.pin_count = 1;

        self.page_table.insert(page_id, frame_idx);
        Ok(page_id)
    }

    /// Allocate a new page on disk and bring it into the buffer pool.
    /// Returns the PageId of the new page. The page is pinned.
    pub fn new_page(&mut self) -> Result<PageId> {
        let frame_idx = self.find_free_frame()?;
        let page_id = self.disk_manager.allocate_page()?;

        let page = &mut self.pool[frame_idx];
        page.reset(page_id);
        page.pin_count = 1;

        self.page_table.insert(page_id, frame_idx);
        Ok(page_id)
    }

    /// Unpin a page, optionally marking it dirty. When pin_count reaches 0,
    /// the frame becomes eligible for LRU eviction.
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let frame_idx = match self.page_table.get(&page_id) {
            Some(&idx) => idx,
            None => {
                return Err(ForgeError::BufferPool(format!(
                    "page {:?} not in buffer pool",
                    page_id
                )));
            }
        };

        let page = &mut self.pool[frame_idx];
        if page.pin_count == 0 {
            return Err(ForgeError::BufferPool(format!(
                "page {:?} already has pin_count 0",
                page_id
            )));
        }

        if is_dirty {
            page.is_dirty = true;
        }

        page.pin_count -= 1;
        if page.pin_count == 0 {
            // Add to the back of LRU (most recently used).
            self.lru.push_back(frame_idx);
        }
        Ok(())
    }

    /// Flush a specific page to disk.
    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        let frame_idx = match self.page_table.get(&page_id) {
            Some(&idx) => idx,
            None => {
                return Err(ForgeError::BufferPool(format!(
                    "page {:?} not in buffer pool",
                    page_id
                )));
            }
        };

        let page = &self.pool[frame_idx];
        self.disk_manager.write_page(page.id, &page.data)?;
        self.pool[frame_idx].is_dirty = false;
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> Result<()> {
        let page_ids: Vec<PageId> = self.page_table.keys().copied().collect();
        for page_id in page_ids {
            let frame_idx = self.page_table[&page_id];
            if self.pool[frame_idx].is_dirty {
                self.flush_page(page_id)?;
            }
        }
        Ok(())
    }

    /// Get an immutable reference to a page that is currently in the pool.
    pub fn get_page(&self, page_id: PageId) -> &Page {
        let frame_idx = self.page_table[&page_id];
        &self.pool[frame_idx]
    }

    /// Get a mutable reference to a page that is currently in the pool.
    pub fn get_page_mut(&mut self, page_id: PageId) -> &mut Page {
        let frame_idx = self.page_table[&page_id];
        &mut self.pool[frame_idx]
    }

    // ---- private helpers ----

    /// Find a free frame index, either from the free list or by evicting an
    /// unpinned page from the LRU queue.
    fn find_free_frame(&mut self) -> Result<usize> {
        if let Some(frame_idx) = self.free_list.pop() {
            return Ok(frame_idx);
        }

        // Evict least-recently-used unpinned frame.
        while let Some(frame_idx) = self.lru.pop_front() {
            let page = &self.pool[frame_idx];
            if page.pin_count == 0 {
                // Flush if dirty.
                if page.is_dirty {
                    let old_page_id = page.id;
                    self.disk_manager
                        .write_page(old_page_id, &page.data)?;
                    self.pool[frame_idx].is_dirty = false;
                }
                // Remove old mapping.
                let old_page_id = self.pool[frame_idx].id;
                self.page_table.remove(&old_page_id);
                return Ok(frame_idx);
            }
            // If somehow pinned, skip (shouldn't happen for properly maintained LRU).
        }

        Err(ForgeError::BufferPool(
            "no free frames available; all pages are pinned".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk_manager::DiskManager;

    fn make_bpm(pool_size: usize) -> (BufferPoolManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(&path).unwrap();
        let bpm = BufferPoolManager::new(pool_size, dm);
        (bpm, dir)
    }

    #[test]
    fn test_new_page_and_fetch() {
        let (mut bpm, _dir) = make_bpm(4);

        let pid = bpm.new_page().unwrap();
        assert_eq!(pid, PageId(0));

        // Write some data.
        bpm.get_page_mut(pid).data[0] = 42;
        bpm.unpin_page(pid, true).unwrap();

        // Fetch it back.
        bpm.fetch_page(pid).unwrap();
        assert_eq!(bpm.get_page(pid).data[0], 42);
        bpm.unpin_page(pid, false).unwrap();
    }

    #[test]
    fn test_eviction() {
        let (mut bpm, _dir) = make_bpm(2);

        // Allocate 2 pages filling the pool.
        let p0 = bpm.new_page().unwrap();
        let p1 = bpm.new_page().unwrap();

        bpm.get_page_mut(p0).data[0] = 0xAA;
        bpm.get_page_mut(p1).data[0] = 0xBB;

        bpm.unpin_page(p0, true).unwrap();
        bpm.unpin_page(p1, true).unwrap();

        // Allocate a third page; should evict p0 (LRU).
        let p2 = bpm.new_page().unwrap();
        bpm.unpin_page(p2, false).unwrap();

        // Fetch p0 back from disk; should evict p1.
        bpm.fetch_page(p0).unwrap();
        assert_eq!(bpm.get_page(p0).data[0], 0xAA);
        bpm.unpin_page(p0, false).unwrap();
    }

    #[test]
    fn test_unpin_error_on_missing_page() {
        let (mut bpm, _dir) = make_bpm(2);
        let result = bpm.unpin_page(PageId(999), false);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_free_frames_error() {
        let (mut bpm, _dir) = make_bpm(1);

        // Allocate one page (pinned).
        let _p0 = bpm.new_page().unwrap();

        // Try to allocate another while the first is still pinned.
        let result = bpm.new_page();
        assert!(result.is_err());
    }

    #[test]
    fn test_flush_page() {
        let (mut bpm, _dir) = make_bpm(2);

        let pid = bpm.new_page().unwrap();
        bpm.get_page_mut(pid).data[10] = 0xCC;
        bpm.flush_page(pid).unwrap();
        assert!(!bpm.get_page(pid).is_dirty);
        bpm.unpin_page(pid, false).unwrap();
    }

    #[test]
    fn test_flush_all() {
        let (mut bpm, _dir) = make_bpm(4);

        let p0 = bpm.new_page().unwrap();
        let p1 = bpm.new_page().unwrap();

        bpm.get_page_mut(p0).data[0] = 1;
        bpm.get_page_mut(p1).data[0] = 2;

        bpm.unpin_page(p0, true).unwrap();
        bpm.unpin_page(p1, true).unwrap();

        bpm.flush_all().unwrap();

        assert!(!bpm.get_page(p0).is_dirty);
        assert!(!bpm.get_page(p1).is_dirty);
    }
}
