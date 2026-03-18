//! Thread-local adapter that wraps ConcurrentBufferPool to provide the
//! same API as BufferPoolManager. Each thread gets its own LocalBpm
//! but they share the underlying ConcurrentBufferPool with page-level locks.

use crate::common::{PageId, PAGE_SIZE};
use crate::error::Result;
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::page::Page;

/// A thread-local handle to the shared ConcurrentBufferPool.
/// Provides the same API as BufferPoolManager so existing executors work unchanged.
pub struct LocalBpm<'a> {
    cbpm: &'a ConcurrentBufferPool,
    /// Local cache of page data for get_page/get_page_mut compatibility.
    /// The old BPM returned references to frames; we copy data into this cache.
    cached_page: Page,
    cached_page_id: Option<PageId>,
}

impl<'a> LocalBpm<'a> {
    pub fn new(cbpm: &'a ConcurrentBufferPool) -> Self {
        Self {
            cbpm,
            cached_page: Page::new(PageId(0)),
            cached_page_id: None,
        }
    }

    /// Fetch a page into the concurrent pool. Pins it.
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<PageId> {
        self.flush_cache();
        self.cbpm.fetch_page(page_id)?;
        // Load into local cache
        let guard = self.cbpm.read_page(page_id)?;
        self.cached_page.id = page_id;
        self.cached_page.data = *guard.data();
        self.cached_page.is_dirty = false;
        self.cached_page.pin_count = 1;
        self.cached_page_id = Some(page_id);
        Ok(page_id)
    }

    /// Allocate a new page.
    pub fn new_page(&mut self) -> Result<PageId> {
        self.flush_cache();
        let (page_id, _) = self.cbpm.new_page()?;
        self.cached_page.id = page_id;
        self.cached_page.data = [0u8; PAGE_SIZE];
        self.cached_page.is_dirty = false;
        self.cached_page.pin_count = 1;
        self.cached_page_id = Some(page_id);
        Ok(page_id)
    }

    /// Get immutable ref to cached page data.
    pub fn get_page(&self, page_id: PageId) -> &Page {
        debug_assert!(self.cached_page_id == Some(page_id));
        &self.cached_page
    }

    /// Get mutable ref to cached page data.
    pub fn get_page_mut(&mut self, page_id: PageId) -> &mut Page {
        debug_assert!(self.cached_page_id == Some(page_id));
        &mut self.cached_page
    }

    /// Unpin a page. Writes back dirty data to the concurrent pool.
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        if is_dirty || self.cached_page.is_dirty {
            if self.cached_page_id == Some(page_id) {
                // Write dirty data back to the concurrent pool
                self.cbpm.write_page_data(page_id, &self.cached_page.data)?;
            }
        }
        self.cached_page_id = None;
        self.cbpm.unpin_page(page_id, is_dirty || self.cached_page.is_dirty)
    }

    /// Flush all dirty pages.
    pub fn flush_all(&mut self) -> Result<()> {
        self.flush_cache();
        self.cbpm.flush_all()
    }

    /// Flush the local cache if it has a dirty page.
    fn flush_cache(&mut self) {
        if let Some(pid) = self.cached_page_id.take() {
            if self.cached_page.is_dirty {
                let _ = self.cbpm.write_page_data(pid, &self.cached_page.data);
            }
        }
    }
}

impl<'a> Drop for LocalBpm<'a> {
    fn drop(&mut self) {
        self.flush_cache();
    }
}
