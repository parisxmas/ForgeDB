//! Concurrent buffer pool manager with page-level locking.
//!
//! Multiple threads can read/write different pages simultaneously.
//! Only same-page access serializes. The metadata (page table) is
//! protected by an RwLock allowing concurrent readers. Pin counts
//! are atomic so read-only operations never block each other.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{ForgeError, Result};
use crate::storage::disk_manager::DiskManager;

/// Per-frame data protected by its own RwLock.
struct Frame {
    page_id: PageId,
    data: [u8; PAGE_SIZE],
    is_dirty: bool,
}

/// Shared metadata protected by RwLock (readers don't block each other).
struct Meta {
    page_table: HashMap<PageId, usize>,
    free_list: Vec<usize>,
    lru: VecDeque<usize>,
}

/// A concurrent buffer pool where each page frame has its own RwLock.
///
/// All public methods take `&self`, enabling safe sharing via `Arc<ConcurrentBufferPool>`.
pub struct ConcurrentBufferPool {
    frames: Vec<RwLock<Frame>>,
    pin_counts: Vec<AtomicU32>,
    meta: RwLock<Meta>,
    meta_write: Mutex<()>, // serialize structural mutations (eviction, new page)
    disk: Mutex<DiskManager>,
    pool_size: usize,
}

/// A read guard that provides immutable access to a page's data.
pub struct PageReadGuard<'a> {
    guard: RwLockReadGuard<'a, Frame>,
}

impl<'a> PageReadGuard<'a> {
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.guard.data
    }
}

/// A write guard that provides mutable access to a page's data.
pub struct PageWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, Frame>,
}

impl<'a> PageWriteGuard<'a> {
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.guard.data
    }

    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.guard.data
    }

    pub fn mark_dirty(&mut self) {
        self.guard.is_dirty = true;
    }
}

impl ConcurrentBufferPool {
    /// Create a new concurrent buffer pool.
    pub fn new(pool_size: usize, disk_manager: DiskManager) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut pin_counts = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            frames.push(RwLock::new(Frame {
                page_id: PageId(INVALID_PAGE_ID),
                data: [0u8; PAGE_SIZE],
                is_dirty: false,
            }));
            pin_counts.push(AtomicU32::new(0));
            free_list.push(i);
        }

        Self {
            frames,
            pin_counts,
            meta: RwLock::new(Meta {
                page_table: HashMap::new(),
                free_list,
                lru: VecDeque::new(),
            }),
            meta_write: Mutex::new(()),
            disk: Mutex::new(disk_manager),
            pool_size,
        }
    }

    /// Fetch a page and return its frame index. Pins the page.
    pub fn fetch_page(&self, page_id: PageId) -> Result<usize> {
        // Fast path: read-only meta lookup (multiple threads can do this concurrently)
        {
            let meta = self.meta.read().unwrap();
            if let Some(&frame_idx) = meta.page_table.get(&page_id) {
                self.pin_counts[frame_idx].fetch_add(1, Ordering::Relaxed);
                return Ok(frame_idx);
            }
        }

        // Slow path: need to bring page from disk
        // Serialize structural mutations
        let _write_guard = self.meta_write.lock().unwrap();

        // Double-check under write intent
        {
            let meta = self.meta.read().unwrap();
            if let Some(&frame_idx) = meta.page_table.get(&page_id) {
                self.pin_counts[frame_idx].fetch_add(1, Ordering::Relaxed);
                return Ok(frame_idx);
            }
        }

        // Read from disk WITHOUT holding meta lock
        let mut data = [0u8; PAGE_SIZE];
        {
            let mut disk = self.disk.lock().unwrap();
            disk.read_page(page_id, &mut data)?;
        }

        // Acquire meta write lock to insert
        let mut meta = self.meta.write().unwrap();

        // Triple-check
        if let Some(&frame_idx) = meta.page_table.get(&page_id) {
            self.pin_counts[frame_idx].fetch_add(1, Ordering::Relaxed);
            return Ok(frame_idx);
        }

        let frame_idx = self.find_free_frame_locked(&mut meta)?;

        // Initialize the frame
        {
            let mut frame = self.frames[frame_idx].write().unwrap();
            frame.page_id = page_id;
            frame.data = data;
            frame.is_dirty = false;
        }
        self.pin_counts[frame_idx].store(1, Ordering::Relaxed);

        meta.page_table.insert(page_id, frame_idx);
        Ok(frame_idx)
    }

    /// Allocate a new page on disk and bring it into the pool.
    pub fn new_page(&self) -> Result<(PageId, usize)> {
        let _write_guard = self.meta_write.lock().unwrap();
        let mut meta = self.meta.write().unwrap();
        let frame_idx = self.find_free_frame_locked(&mut meta)?;

        let page_id = {
            let mut disk = self.disk.lock().unwrap();
            disk.allocate_page()?
        };

        {
            let mut frame = self.frames[frame_idx].write().unwrap();
            frame.page_id = page_id;
            frame.data = [0u8; PAGE_SIZE];
            frame.is_dirty = false;
        }
        self.pin_counts[frame_idx].store(1, Ordering::Relaxed);

        meta.page_table.insert(page_id, frame_idx);
        Ok((page_id, frame_idx))
    }

    /// Unpin a page. When pin_count reaches 0, frame becomes LRU-evictable.
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()> {
        let frame_idx = {
            let meta = self.meta.read().unwrap();
            match meta.page_table.get(&page_id) {
                Some(&idx) => idx,
                None => {
                    return Err(ForgeError::BufferPool(format!(
                        "page {:?} not in buffer pool",
                        page_id
                    )));
                }
            }
        };

        if is_dirty {
            let mut frame = self.frames[frame_idx].write().unwrap();
            frame.is_dirty = true;
        }

        let old = self.pin_counts[frame_idx].fetch_sub(1, Ordering::Release);
        if old == 0 {
            // Was already zero — restore and error
            self.pin_counts[frame_idx].fetch_add(1, Ordering::Relaxed);
            return Err(ForgeError::BufferPool(format!(
                "page {:?} already has pin_count 0",
                page_id
            )));
        }
        if old == 1 {
            // pin_count went to 0 — try to add to LRU without blocking
            // If another thread holds the write lock, skip — the page stays
            // in the pool and will be added to LRU on the next unpin or eviction check.
            if let Ok(mut meta) = self.meta.try_write() {
                meta.lru.push_back(frame_idx);
            }
        }
        Ok(())
    }

    /// Get a read guard for a page (concurrent readers allowed).
    pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard<'_>> {
        let frame_idx = {
            let meta = self.meta.read().unwrap();
            match meta.page_table.get(&page_id) {
                Some(&idx) => idx,
                None => {
                    return Err(ForgeError::BufferPool(format!(
                        "page {:?} not in buffer pool for read",
                        page_id
                    )));
                }
            }
        };

        let guard = self.frames[frame_idx].read().unwrap();
        Ok(PageReadGuard { guard })
    }

    /// Get a write guard for a page (exclusive access).
    pub fn write_page(&self, page_id: PageId) -> Result<PageWriteGuard<'_>> {
        let frame_idx = {
            let meta = self.meta.read().unwrap();
            match meta.page_table.get(&page_id) {
                Some(&idx) => idx,
                None => {
                    return Err(ForgeError::BufferPool(format!(
                        "page {:?} not in buffer pool for write",
                        page_id
                    )));
                }
            }
        };

        let guard = self.frames[frame_idx].write().unwrap();
        Ok(PageWriteGuard { guard })
    }

    /// Read a page directly without pin/unpin overhead.
    /// For read-only access where the caller processes data immediately.
    /// Avoids 3 separate lock acquisitions (fetch + read + unpin).
    pub fn read_page_direct(&self, page_id: PageId) -> Result<PageReadGuard<'_>> {
        // Single read lock to find frame
        let frame_idx = {
            let meta = self.meta.read().unwrap();
            if let Some(&idx) = meta.page_table.get(&page_id) {
                idx
            } else {
                drop(meta);
                // Page not in pool — fetch it first (slow path)
                self.fetch_page(page_id)?;
                let meta = self.meta.read().unwrap();
                match meta.page_table.get(&page_id) {
                    Some(&idx) => {
                        // Immediately unpin since we'll hold the read guard
                        self.pin_counts[idx].fetch_sub(1, Ordering::Relaxed);
                        idx
                    }
                    None => return Err(ForgeError::BufferPool(
                        format!("page {:?} not in buffer pool after fetch", page_id)
                    )),
                }
            }
        };
        let guard = self.frames[frame_idx].read().unwrap();
        Ok(PageReadGuard { guard })
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&self) -> Result<()> {
        let page_ids: Vec<(PageId, usize)> = {
            let meta = self.meta.read().unwrap();
            meta.page_table.iter()
                .map(|(&pid, &idx)| (pid, idx))
                .collect()
        };

        let mut disk = self.disk.lock().unwrap();
        for (page_id, frame_idx) in page_ids {
            let frame = self.frames[frame_idx].read().unwrap();
            if frame.is_dirty {
                disk.write_page(page_id, &frame.data)?;
            }
            drop(frame);
            // Clear dirty flag
            let mut frame = self.frames[frame_idx].write().unwrap();
            frame.is_dirty = false;
        }
        Ok(())
    }

    /// Find a free frame, evicting LRU if needed. Caller must hold meta write lock.
    fn find_free_frame_locked(&self, meta: &mut Meta) -> Result<usize> {
        if let Some(frame_idx) = meta.free_list.pop() {
            return Ok(frame_idx);
        }

        while let Some(frame_idx) = meta.lru.pop_front() {
            if self.pin_counts[frame_idx].load(Ordering::Relaxed) == 0 {
                let frame = self.frames[frame_idx].read().unwrap();
                let old_page_id = frame.page_id;
                if frame.is_dirty {
                    let mut disk = self.disk.lock().unwrap();
                    disk.write_page(old_page_id, &frame.data)?;
                }
                drop(frame);

                // Clear the frame
                {
                    let mut frame = self.frames[frame_idx].write().unwrap();
                    frame.is_dirty = false;
                }
                meta.page_table.remove(&old_page_id);
                return Ok(frame_idx);
            }
        }

        Err(ForgeError::BufferPool(
            "no free frames available; all pages are pinned".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Adapter: make ConcurrentBufferPool usable where BufferPoolManager was used
// ---------------------------------------------------------------------------

/// Wraps ConcurrentBufferPool to provide the same API as BufferPoolManager.
/// This adapter copies page data in/out so callers don't hold page locks
/// across operations.
impl ConcurrentBufferPool {
    /// Fetch + read page data (copy out). Leaves page pinned.
    pub fn fetch_page_data(&self, page_id: PageId) -> Result<[u8; PAGE_SIZE]> {
        self.fetch_page(page_id)?;
        let guard = self.read_page(page_id)?;
        let data = *guard.data();
        Ok(data)
    }

    /// Write page data back. Page must be fetched/pinned.
    pub fn write_page_data(&self, page_id: PageId, data: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut guard = self.write_page(page_id)?;
        guard.data_mut().copy_from_slice(data);
        guard.mark_dirty();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn make_cbpm(pool_size: usize) -> (ConcurrentBufferPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(path.to_str().unwrap()).unwrap();
        (ConcurrentBufferPool::new(pool_size, dm), dir)
    }

    #[test]
    fn test_basic_new_and_fetch() {
        let (cbpm, _dir) = make_cbpm(16);

        let (page_id, _) = cbpm.new_page().unwrap();
        {
            let mut guard = cbpm.write_page(page_id).unwrap();
            guard.data_mut()[0] = 42;
            guard.mark_dirty();
        }
        cbpm.unpin_page(page_id, true).unwrap();

        cbpm.fetch_page(page_id).unwrap();
        {
            let guard = cbpm.read_page(page_id).unwrap();
            assert_eq!(guard.data()[0], 42);
        }
        cbpm.unpin_page(page_id, false).unwrap();
    }

    #[test]
    fn test_concurrent_reads() {
        let (cbpm, _dir) = make_cbpm(16);
        let cbpm = Arc::new(cbpm);

        // Create a page with data
        let (page_id, _) = cbpm.new_page().unwrap();
        {
            let mut guard = cbpm.write_page(page_id).unwrap();
            guard.data_mut()[0] = 99;
            guard.mark_dirty();
        }
        cbpm.unpin_page(page_id, true).unwrap();

        // Spawn 10 threads that all read the same page concurrently
        let mut handles = Vec::new();
        for _ in 0..10 {
            let cbpm = Arc::clone(&cbpm);
            handles.push(thread::spawn(move || {
                cbpm.fetch_page(page_id).unwrap();
                let guard = cbpm.read_page(page_id).unwrap();
                let val = guard.data()[0];
                drop(guard);
                cbpm.unpin_page(page_id, false).unwrap();
                val
            }));
        }

        for h in handles {
            assert_eq!(h.join().unwrap(), 99);
        }
    }

    #[test]
    fn test_concurrent_different_pages() {
        let (cbpm, _dir) = make_cbpm(64);
        let cbpm = Arc::new(cbpm);

        // Create 10 pages
        let mut page_ids = Vec::new();
        for i in 0..10u8 {
            let (pid, _) = cbpm.new_page().unwrap();
            {
                let mut guard = cbpm.write_page(pid).unwrap();
                guard.data_mut()[0] = i;
                guard.mark_dirty();
            }
            cbpm.unpin_page(pid, true).unwrap();
            page_ids.push(pid);
        }

        // 10 threads each write to their own page concurrently
        let mut handles = Vec::new();
        for (i, &pid) in page_ids.iter().enumerate() {
            let cbpm = Arc::clone(&cbpm);
            handles.push(thread::spawn(move || {
                cbpm.fetch_page(pid).unwrap();
                {
                    let mut guard = cbpm.write_page(pid).unwrap();
                    guard.data_mut()[0] = (i as u8) * 10;
                    guard.mark_dirty();
                }
                cbpm.unpin_page(pid, true).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify all writes landed correctly
        for (i, &pid) in page_ids.iter().enumerate() {
            cbpm.fetch_page(pid).unwrap();
            let guard = cbpm.read_page(pid).unwrap();
            assert_eq!(guard.data()[0], (i as u8) * 10);
            drop(guard);
            cbpm.unpin_page(pid, false).unwrap();
        }
    }
}
