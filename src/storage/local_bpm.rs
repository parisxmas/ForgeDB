//! Thread-local adapter that wraps ConcurrentBufferPool to provide the
//! same API as BufferPoolManager. Each thread gets its own LocalBpm
//! but they share the underlying ConcurrentBufferPool with page-level locks.
//!
//! Uses a small local page cache (default 8 pages) to avoid repeatedly
//! copying page data from the concurrent pool on every access. This is
//! critical for B-tree traversals and joins that touch multiple pages.

use std::collections::HashMap;

use crate::common::{PageId, TxnId, PAGE_SIZE};
use crate::error::Result;
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::page::Page;

/// Maximum pages cached locally per thread.
/// Larger cache reduces re-fetches and 16KB page copies for read-heavy workloads.
const LOCAL_CACHE_SIZE: usize = 128;

/// A thread-local handle to the shared ConcurrentBufferPool.
/// Caches up to LOCAL_CACHE_SIZE pages to avoid lock contention and data copies.
pub struct LocalBpm<'a> {
    cbpm: &'a ConcurrentBufferPool,
    /// Cached pages indexed by PageId.
    cache: HashMap<PageId, Page>,
    /// LRU order: most recently used at the back.
    lru_order: Vec<PageId>,
    /// Which page is currently "active" (for get_page/get_page_mut compat).
    active_page_id: Option<PageId>,
    /// Before-images captured on first fetch for WAL logging.
    before_images: HashMap<PageId, Box<[u8; PAGE_SIZE]>>,
    /// When true, skip WAL before-image capture on fetch (read-only path).
    readonly: bool,
}

impl<'a> LocalBpm<'a> {
    pub fn new(cbpm: &'a ConcurrentBufferPool) -> Self {
        Self {
            cbpm,
            cache: HashMap::with_capacity(LOCAL_CACHE_SIZE),
            lru_order: Vec::with_capacity(LOCAL_CACHE_SIZE),
            active_page_id: None,
            before_images: HashMap::new(),
            readonly: false,
        }
    }

    /// Create a read-only LocalBpm that skips WAL before-image capture.
    /// This eliminates one 16KB copy per page fetch on the read path.
    pub fn new_readonly(cbpm: &'a ConcurrentBufferPool) -> Self {
        Self {
            cbpm,
            cache: HashMap::with_capacity(LOCAL_CACHE_SIZE),
            lru_order: Vec::with_capacity(LOCAL_CACHE_SIZE),
            active_page_id: None,
            before_images: HashMap::new(),
            readonly: true,
        }
    }

    /// Get a reference to the underlying ConcurrentBufferPool.
    /// Used for spawning parallel workers that need their own LocalBpm.
    pub fn get_cbpm(&self) -> &'a ConcurrentBufferPool {
        self.cbpm
    }

    /// Fetch a page into the concurrent pool. Pins it and caches locally.
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<PageId> {
        // If already in local cache, just promote in LRU and return
        if self.cache.contains_key(&page_id) {
            self.touch_lru(page_id);
            self.active_page_id = Some(page_id);
            return Ok(page_id);
        }

        // Evict oldest if cache is full
        if self.cache.len() >= LOCAL_CACHE_SIZE {
            self.evict_one();
        }

        // Fetch from concurrent pool
        self.cbpm.fetch_page(page_id)?;
        let guard = self.cbpm.read_page(page_id)?;
        let mut page = Page::new(page_id);
        page.data = *guard.data();
        page.is_dirty = false;
        page.pin_count = 1;

        // Capture before-image for WAL (only on first fetch, skip for read-only)
        if !self.readonly && !self.before_images.contains_key(&page_id) {
            let mut before = Box::new([0u8; PAGE_SIZE]);
            before.copy_from_slice(guard.data());
            self.before_images.insert(page_id, before);
        }
        drop(guard);

        self.cache.insert(page_id, page);
        self.lru_order.push(page_id);
        self.active_page_id = Some(page_id);
        Ok(page_id)
    }

    /// Allocate a new page.
    pub fn new_page(&mut self) -> Result<PageId> {
        if self.cache.len() >= LOCAL_CACHE_SIZE {
            self.evict_one();
        }

        let (page_id, _) = self.cbpm.new_page()?;
        let mut page = Page::new(page_id);
        page.data = [0u8; PAGE_SIZE];
        page.is_dirty = false;
        page.pin_count = 1;

        self.cache.insert(page_id, page);
        self.lru_order.push(page_id);
        self.active_page_id = Some(page_id);
        Ok(page_id)
    }

    /// Get immutable ref to cached page data.
    pub fn get_page(&self, page_id: PageId) -> &Page {
        debug_assert!(self.cache.contains_key(&page_id),
            "get_page({:?}) called but page not in local cache", page_id);
        &self.cache[&page_id]
    }

    /// Get mutable ref to cached page data.
    pub fn get_page_mut(&mut self, page_id: PageId) -> &mut Page {
        debug_assert!(self.cache.contains_key(&page_id),
            "get_page_mut({:?}) called but page not in local cache", page_id);
        self.cache.get_mut(&page_id)
            .expect("get_page_mut called for page not in local cache")
    }

    /// Unpin a page. Writes back dirty data to the concurrent pool.
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        if let Some(page) = self.cache.get_mut(&page_id) {
            if is_dirty {
                page.is_dirty = true;
            }
            if page.is_dirty {
                // Write dirty data back to the concurrent pool
                self.cbpm.write_page_data(page_id, &page.data)?;
                page.is_dirty = false;
            }
        }
        // Remove from local cache and unpin in concurrent pool
        self.cache.remove(&page_id);
        self.lru_order.retain(|&p| p != page_id);
        if self.active_page_id == Some(page_id) {
            self.active_page_id = None;
        }
        self.cbpm.unpin_page(page_id, is_dirty)
    }

    /// Flush all dirty pages.
    pub fn flush_all(&mut self) -> Result<()> {
        self.flush_cache();
        self.cbpm.flush_all()
    }

    /// Flush all dirty pages in the local cache back to the concurrent pool.
    fn flush_cache(&mut self) {
        let page_ids: Vec<PageId> = self.cache.keys().copied().collect();
        for pid in page_ids {
            if let Some(page) = self.cache.get(&pid) {
                if page.is_dirty {
                    let _ = self.cbpm.write_page_data(pid, &page.data);
                }
            }
        }
        self.cache.clear();
        self.lru_order.clear();
        self.active_page_id = None;
    }

    /// Evict the least recently used page from local cache.
    fn evict_one(&mut self) {
        if let Some(evict_pid) = self.lru_order.first().copied() {
            if let Some(page) = self.cache.get(&evict_pid) {
                if page.is_dirty {
                    let _ = self.cbpm.write_page_data(evict_pid, &page.data);
                }
            }
            self.cache.remove(&evict_pid);
            self.lru_order.remove(0);
        }
    }

    /// Promote a page to most-recently-used in LRU.
    fn touch_lru(&mut self, page_id: PageId) {
        self.lru_order.retain(|&p| p != page_id);
        self.lru_order.push(page_id);
    }

    /// Drain WAL entries: for each page that was fetched and subsequently dirtied,
    /// produce (txn_id, page_id, before_image, after_image) pairs.
    /// Clears the before-image map.
    pub fn drain_wal_entries(&mut self, txn_id: TxnId) -> Vec<(TxnId, PageId, Box<[u8; PAGE_SIZE]>, Box<[u8; PAGE_SIZE]>)> {
        let mut entries = Vec::new();
        let before_images = std::mem::take(&mut self.before_images);
        for (page_id, before) in before_images {
            if let Some(page) = self.cache.get(&page_id) {
                if page.is_dirty {
                    let mut after = Box::new([0u8; PAGE_SIZE]);
                    after.copy_from_slice(&page.data);
                    entries.push((txn_id, page_id, before, after));
                }
            }
        }
        entries
    }
}

impl<'a> Drop for LocalBpm<'a> {
    fn drop(&mut self) {
        self.flush_cache();
    }
}
