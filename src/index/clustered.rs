//! Clustered B+ tree index: stores full row data in leaf nodes.
//!
//! This eliminates the heap-file hop for primary key lookups.
//! Leaf nodes store: [key_bytes][row_data] instead of [key_bytes][RID].
//! The linked-leaf chain enables efficient range scans and full-table scans.
//!
//! Page layout is similar to btree_page but leaf entries carry row payloads.

use std::collections::HashMap;

use crate::common::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{ForgeError, Result};
use crate::storage::local_bpm::LocalBpm;
use crate::tuple::types::Value;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const HEADER_SIZE: usize = 12; // type(1) + num_keys(2) + parent(4) + reserved(1) + next_leaf(4)
const INTERNAL_HEADER_SIZE: usize = 12; // type(1) + num_keys(2) + parent(4) + reserved(1) + first_child(4)
const PAGE_TYPE_INTERNAL: u8 = 0;
const PAGE_TYPE_LEAF: u8 = 1;

// ---------------------------------------------------------------------------
// LE helpers
// ---------------------------------------------------------------------------

#[inline]
fn r_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}
#[inline]
fn w_u16(buf: &mut [u8], off: usize, v: u16) {
    let b = v.to_le_bytes();
    buf[off] = b[0];
    buf[off + 1] = b[1];
}
#[inline]
fn r_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}
#[inline]
fn w_u32(buf: &mut [u8], off: usize, v: u32) {
    let b = v.to_le_bytes();
    buf[off] = b[0];
    buf[off + 1] = b[1];
    buf[off + 2] = b[2];
    buf[off + 3] = b[3];
}

// ---------------------------------------------------------------------------
// Leaf page operations
// ---------------------------------------------------------------------------
// Entry format: [key_len: u16][key_bytes][data_len: u16][row_data]

fn leaf_init(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    page[0] = PAGE_TYPE_LEAF;
    w_u16(page, 1, 0); // num_keys
    w_u32(page, 8, INVALID_PAGE_ID); // next_leaf
}

fn leaf_num_keys(page: &[u8; PAGE_SIZE]) -> u16 {
    r_u16(page, 1)
}

fn leaf_next_leaf(page: &[u8; PAGE_SIZE]) -> u32 {
    r_u32(page, 8)
}

fn leaf_set_next_leaf(page: &mut [u8; PAGE_SIZE], next: u32) {
    w_u32(page, 8, next);
}

/// Get offset of entry at `index` by scanning from HEADER_SIZE.
fn leaf_entry_offset(page: &[u8; PAGE_SIZE], index: u16) -> usize {
    let mut off = HEADER_SIZE;
    for _ in 0..index {
        let key_len = r_u16(page, off) as usize;
        off += 2 + key_len;
        let data_len = r_u16(page, off) as usize;
        off += 2 + data_len;
    }
    off
}

/// Used bytes from HEADER_SIZE through all entries.
fn leaf_used(page: &[u8; PAGE_SIZE]) -> usize {
    leaf_entry_offset(page, leaf_num_keys(page))
}

fn leaf_has_room(page: &[u8; PAGE_SIZE], key_len: usize, data_len: usize) -> bool {
    let need = 2 + key_len + 2 + data_len;
    leaf_used(page) + need <= PAGE_SIZE
}

/// Get (key, data) at index.
fn leaf_get_entry(page: &[u8; PAGE_SIZE], index: u16) -> (Vec<u8>, Vec<u8>) {
    let off = leaf_entry_offset(page, index);
    let key_len = r_u16(page, off) as usize;
    let key = page[off + 2..off + 2 + key_len].to_vec();
    let data_off = off + 2 + key_len;
    let data_len = r_u16(page, data_off) as usize;
    let data = page[data_off + 2..data_off + 2 + data_len].to_vec();
    (key, data)
}

/// Collect all (key, data) entries.
fn leaf_get_all(page: &[u8; PAGE_SIZE]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let n = leaf_num_keys(page);
    (0..n).map(|i| leaf_get_entry(page, i)).collect()
}

/// Insert (key, data) in sorted order. Returns false if no room.
fn leaf_insert(page: &mut [u8; PAGE_SIZE], key: &[u8], data: &[u8]) -> bool {
    if !leaf_has_room(page, key.len(), data.len()) {
        return false;
    }
    let n = leaf_num_keys(page) as usize;
    // Find insertion position
    let mut entries = leaf_get_all(page);
    let pos = entries.partition_point(|(k, _)| k.as_slice() < key);
    entries.insert(pos, (key.to_vec(), data.to_vec()));
    // Rewrite all entries
    leaf_rewrite(page, &entries);
    true
}

/// Search for exact key. Returns data if found.
fn leaf_search(page: &[u8; PAGE_SIZE], key: &[u8]) -> Option<Vec<u8>> {
    let n = leaf_num_keys(page);
    for i in 0..n {
        let (k, d) = leaf_get_entry(page, i);
        if k == key {
            return Some(d);
        }
        if k.as_slice() > key {
            break;
        }
    }
    None
}

/// Delete entry by key. Returns true if found.
fn leaf_delete(page: &mut [u8; PAGE_SIZE], key: &[u8]) -> bool {
    let entries = leaf_get_all(page);
    let new_entries: Vec<_> = entries.into_iter().filter(|(k, _)| k.as_slice() != key).collect();
    let found = new_entries.len() < leaf_num_keys(page) as usize;
    if found {
        let next = leaf_next_leaf(page);
        leaf_rewrite(page, &new_entries);
        leaf_set_next_leaf(page, next);
    }
    found
}

/// Rewrite all entries into the page.
fn leaf_rewrite(page: &mut [u8; PAGE_SIZE], entries: &[(Vec<u8>, Vec<u8>)]) {
    let next = leaf_next_leaf(page);
    page[HEADER_SIZE..].fill(0);
    w_u16(page, 1, entries.len() as u16);
    w_u32(page, 8, next);
    let mut off = HEADER_SIZE;
    for (key, data) in entries {
        w_u16(page, off, key.len() as u16);
        off += 2;
        page[off..off + key.len()].copy_from_slice(key);
        off += key.len();
        w_u16(page, off, data.len() as u16);
        off += 2;
        page[off..off + data.len()].copy_from_slice(data);
        off += data.len();
    }
}

// ---------------------------------------------------------------------------
// Internal page operations (same as btree_page)
// ---------------------------------------------------------------------------

fn internal_init(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    page[0] = PAGE_TYPE_INTERNAL;
}

fn internal_num_keys(page: &[u8; PAGE_SIZE]) -> u16 {
    r_u16(page, 1)
}

fn internal_first_child(page: &[u8; PAGE_SIZE]) -> u32 {
    r_u32(page, 8)
}

fn internal_set_first_child(page: &mut [u8; PAGE_SIZE], child: u32) {
    w_u32(page, 8, child);
}

fn internal_entry_offset(page: &[u8; PAGE_SIZE], index: u16) -> usize {
    let mut off = INTERNAL_HEADER_SIZE;
    for _ in 0..index {
        let key_len = r_u16(page, off) as usize;
        off += 2 + key_len + 4; // key_len + key + child_page_id
    }
    off
}

fn internal_used(page: &[u8; PAGE_SIZE]) -> usize {
    internal_entry_offset(page, internal_num_keys(page))
}

fn internal_has_room(page: &[u8; PAGE_SIZE], key_len: usize) -> bool {
    internal_used(page) + 2 + key_len + 4 <= PAGE_SIZE
}

fn internal_get_all(page: &[u8; PAGE_SIZE]) -> Vec<(Vec<u8>, u32)> {
    let n = internal_num_keys(page);
    let mut result = Vec::with_capacity(n as usize);
    let mut off = INTERNAL_HEADER_SIZE;
    for _ in 0..n {
        let key_len = r_u16(page, off) as usize;
        let key = page[off + 2..off + 2 + key_len].to_vec();
        let child = r_u32(page, off + 2 + key_len);
        result.push((key, child));
        off += 2 + key_len + 4;
    }
    result
}

fn internal_insert(page: &mut [u8; PAGE_SIZE], key: &[u8], child: u32) -> bool {
    if !internal_has_room(page, key.len()) {
        return false;
    }
    let mut entries = internal_get_all(page);
    let pos = entries.partition_point(|(k, _)| k.as_slice() < key);
    entries.insert(pos, (key.to_vec(), child));
    // Rewrite
    let first = internal_first_child(page);
    page[INTERNAL_HEADER_SIZE..].fill(0);
    w_u16(page, 1, entries.len() as u16);
    internal_set_first_child(page, first);
    let mut off = INTERNAL_HEADER_SIZE;
    for (k, c) in &entries {
        w_u16(page, off, k.len() as u16);
        off += 2;
        page[off..off + k.len()].copy_from_slice(k);
        off += k.len();
        w_u32(page, off, *c);
        off += 4;
    }
    true
}

fn internal_search_child(page: &[u8; PAGE_SIZE], key: &[u8]) -> u32 {
    let entries = internal_get_all(page);
    for (k, c) in entries.iter().rev() {
        if key >= k.as_slice() {
            return *c;
        }
    }
    internal_first_child(page)
}

// ---------------------------------------------------------------------------
// ClusteredIndex
// ---------------------------------------------------------------------------

/// A clustered B+ tree index that stores full row data in leaf nodes.
/// Eliminates the heap-file indirection for primary key lookups.
#[derive(Clone)]
pub struct ClusteredIndex {
    pub root_page_id: PageId,
    pub key_column_index: usize,
}

impl ClusteredIndex {
    /// Create a new empty clustered index.
    pub fn create(bpm: &mut LocalBpm, key_column_index: usize) -> Result<Self> {
        let page_id = bpm.new_page()?;
        {
            let page = bpm.get_page_mut(page_id);
            leaf_init(&mut page.data);
        }
        bpm.unpin_page(page_id, true)?;
        Ok(Self {
            root_page_id: page_id,
            key_column_index,
        })
    }

    /// Search by key, returns serialized row data if found.
    pub fn search(&self, bpm: &mut LocalBpm, key: &Value) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.to_sort_key_bytes();
        let leaf_id = self.find_leaf(bpm, &key_bytes)?;

        bpm.fetch_page(leaf_id)?;
        let page = bpm.get_page(leaf_id);
        let result = leaf_search(&page.data, &key_bytes);
        bpm.unpin_page(leaf_id, false)?;
        Ok(result)
    }

    /// Insert a row with its key.
    pub fn insert(
        &mut self,
        bpm: &mut LocalBpm,
        key: &Value,
        row_data: &[u8],
    ) -> Result<()> {
        let key_bytes = key.to_sort_key_bytes();
        let leaf_id = self.find_leaf(bpm, &key_bytes)?;

        bpm.fetch_page(leaf_id)?;
        let has_room = {
            let page = bpm.get_page(leaf_id);
            leaf_has_room(&page.data, key_bytes.len(), row_data.len())
        };

        if has_room {
            let page = bpm.get_page_mut(leaf_id);
            leaf_insert(&mut page.data, &key_bytes, row_data);
            bpm.unpin_page(leaf_id, true)?;
            return Ok(());
        }

        // Split
        let (all_entries, old_next) = {
            let page = bpm.get_page(leaf_id);
            let mut entries = leaf_get_all(&page.data);
            let next = leaf_next_leaf(&page.data);
            let pos = entries.partition_point(|(k, _)| k.as_slice() < key_bytes.as_slice());
            entries.insert(pos, (key_bytes.clone(), row_data.to_vec()));
            (entries, next)
        };

        let mid = all_entries.len() / 2;

        // Unpin leaf before allocating (LocalBpm single-page cache).
        bpm.unpin_page(leaf_id, false)?;

        let new_leaf_id = bpm.new_page()?;
        bpm.unpin_page(new_leaf_id, false)?;

        // Old leaf gets lower half
        bpm.fetch_page(leaf_id)?;
        {
            let page = bpm.get_page_mut(leaf_id);
            leaf_init(&mut page.data);
            leaf_set_next_leaf(&mut page.data, new_leaf_id.0);
            leaf_rewrite(&mut page.data, &all_entries[..mid]);
            leaf_set_next_leaf(&mut page.data, new_leaf_id.0);
        }
        bpm.unpin_page(leaf_id, true)?;

        // New leaf gets upper half
        bpm.fetch_page(new_leaf_id)?;
        {
            let page = bpm.get_page_mut(new_leaf_id);
            leaf_init(&mut page.data);
            leaf_set_next_leaf(&mut page.data, old_next);
            leaf_rewrite(&mut page.data, &all_entries[mid..]);
            leaf_set_next_leaf(&mut page.data, old_next);
        }
        bpm.unpin_page(new_leaf_id, true)?;

        let separator = all_entries[mid].0.clone();
        self.insert_into_parent(bpm, leaf_id, &separator, new_leaf_id)?;

        Ok(())
    }

    /// Delete by key. Returns the old row data if found.
    pub fn delete(&mut self, bpm: &mut LocalBpm, key: &Value) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.to_sort_key_bytes();
        let leaf_id = self.find_leaf(bpm, &key_bytes)?;

        bpm.fetch_page(leaf_id)?;
        // Get old data before deleting
        let old_data = {
            let page = bpm.get_page(leaf_id);
            leaf_search(&page.data, &key_bytes)
        };
        if old_data.is_some() {
            let page = bpm.get_page_mut(leaf_id);
            leaf_delete(&mut page.data, &key_bytes);
        }
        bpm.unpin_page(leaf_id, old_data.is_some())?;
        Ok(old_data)
    }

    /// Scan all rows in key order. Returns (key_bytes, row_data) pairs.
    pub fn scan_all(&self, bpm: &mut LocalBpm) -> Result<Vec<Vec<u8>>> {
        let first_leaf = self.find_leftmost_leaf(bpm)?;
        let mut rows = Vec::new();
        let mut current = first_leaf;

        loop {
            bpm.fetch_page(current)?;
            let (entries, next) = {
                let page = bpm.get_page(current);
                (leaf_get_all(&page.data), leaf_next_leaf(&page.data))
            };
            bpm.unpin_page(current, false)?;

            for (_, data) in entries {
                rows.push(data);
            }

            if next == INVALID_PAGE_ID {
                break;
            }
            current = PageId(next);
        }

        Ok(rows)
    }

    /// Scan rows matching a predicate on serialized key bytes.
    pub fn scan_range(
        &self,
        bpm: &mut LocalBpm,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<Vec<u8>>> {
        let first_leaf = match start_key {
            Some(k) => self.find_leaf(bpm, k)?,
            None => self.find_leftmost_leaf(bpm)?,
        };

        let mut rows = Vec::new();
        let mut current = first_leaf;

        'outer: loop {
            bpm.fetch_page(current)?;
            let (entries, next) = {
                let page = bpm.get_page(current);
                (leaf_get_all(&page.data), leaf_next_leaf(&page.data))
            };
            bpm.unpin_page(current, false)?;

            for (key, data) in &entries {
                if let Some(sk) = start_key {
                    if key.as_slice() < sk {
                        continue;
                    }
                }
                if let Some(ek) = end_key {
                    if key.as_slice() > ek {
                        break 'outer;
                    }
                }
                rows.push(data.clone());
            }

            if next == INVALID_PAGE_ID {
                break;
            }
            current = PageId(next);
        }

        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn find_leaf(&self, bpm: &mut LocalBpm, key: &[u8]) -> Result<PageId> {
        let mut current = self.root_page_id;
        loop {
            bpm.fetch_page(current)?;
            let page_type = bpm.get_page(current).data[0];
            if page_type == PAGE_TYPE_LEAF {
                bpm.unpin_page(current, false)?;
                return Ok(current);
            }
            let child_id = {
                let page = bpm.get_page(current);
                internal_search_child(&page.data, key)
            };
            bpm.unpin_page(current, false)?;
            current = PageId(child_id);
        }
    }

    fn find_leftmost_leaf(&self, bpm: &mut LocalBpm) -> Result<PageId> {
        let mut current = self.root_page_id;
        loop {
            bpm.fetch_page(current)?;
            let page_type = bpm.get_page(current).data[0];
            if page_type == PAGE_TYPE_LEAF {
                bpm.unpin_page(current, false)?;
                return Ok(current);
            }
            let child_id = {
                let page = bpm.get_page(current);
                internal_first_child(&page.data)
            };
            bpm.unpin_page(current, false)?;
            current = PageId(child_id);
        }
    }

    fn find_parent(
        &self,
        bpm: &mut LocalBpm,
        current: PageId,
        target: PageId,
    ) -> Result<PageId> {
        bpm.fetch_page(current)?;
        let page_type = bpm.get_page(current).data[0];
        if page_type == PAGE_TYPE_LEAF {
            bpm.unpin_page(current, false)?;
            return Err(ForgeError::Index("target not found in tree".into()));
        }

        let (first_child, entries) = {
            let page = bpm.get_page(current);
            (internal_first_child(&page.data), internal_get_all(&page.data))
        };
        bpm.unpin_page(current, false)?;

        let mut children = vec![first_child];
        for (_, c) in &entries {
            children.push(*c);
        }

        for &child in &children {
            if PageId(child) == target {
                return Ok(current);
            }
        }

        for &child in &children {
            if let Ok(parent) = self.find_parent(bpm, PageId(child), target) {
                return Ok(parent);
            }
        }

        Err(ForgeError::Index("parent not found".into()))
    }

    fn insert_into_parent(
        &mut self,
        bpm: &mut LocalBpm,
        left_id: PageId,
        key: &[u8],
        right_id: PageId,
    ) -> Result<()> {
        if left_id == self.root_page_id {
            let new_root = bpm.new_page()?;
            {
                let page = bpm.get_page_mut(new_root);
                internal_init(&mut page.data);
                internal_set_first_child(&mut page.data, left_id.0);
                internal_insert(&mut page.data, key, right_id.0);
            }
            bpm.unpin_page(new_root, true)?;
            self.root_page_id = new_root;
            return Ok(());
        }

        let parent_id = self.find_parent(bpm, self.root_page_id, left_id)?;
        bpm.fetch_page(parent_id)?;

        let has_room = {
            let page = bpm.get_page(parent_id);
            internal_has_room(&page.data, key.len())
        };

        if has_room {
            let page = bpm.get_page_mut(parent_id);
            internal_insert(&mut page.data, key, right_id.0);
            bpm.unpin_page(parent_id, true)?;
            return Ok(());
        }

        // Split internal
        let (first_child, entries) = {
            let page = bpm.get_page(parent_id);
            let first = internal_first_child(&page.data);
            let mut entries = internal_get_all(&page.data);
            let pos = entries.partition_point(|(k, _)| k.as_slice() < key);
            entries.insert(pos, (key.to_vec(), right_id.0));
            (first, entries)
        };

        let mid = entries.len() / 2;
        let push_up = entries[mid].0.clone();

        // Unpin parent before allocating (LocalBpm single-page cache).
        bpm.unpin_page(parent_id, false)?;

        let new_internal = bpm.new_page()?;
        bpm.unpin_page(new_internal, false)?;

        bpm.fetch_page(parent_id)?;
        {
            let page = bpm.get_page_mut(parent_id);
            internal_init(&mut page.data);
            internal_set_first_child(&mut page.data, first_child);
            for (k, c) in &entries[..mid] {
                internal_insert(&mut page.data, k, *c);
            }
        }
        bpm.unpin_page(parent_id, true)?;

        bpm.fetch_page(new_internal)?;
        {
            let page = bpm.get_page_mut(new_internal);
            internal_init(&mut page.data);
            internal_set_first_child(&mut page.data, entries[mid].1);
            for (k, c) in &entries[mid + 1..] {
                internal_insert(&mut page.data, k, *c);
            }
        }
        bpm.unpin_page(new_internal, true)?;

        self.insert_into_parent(bpm, parent_id, &push_up, new_internal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::concurrent_bpm::ConcurrentBufferPool;
    use crate::storage::local_bpm::LocalBpm;
    use crate::storage::DiskManager;
    use tempfile::TempDir;

    fn make_bpm() -> (ConcurrentBufferPool, TempDir) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::new(path.to_str().unwrap()).unwrap();
        let cbpm = ConcurrentBufferPool::new(256, dm);
        (cbpm, dir)
    }

    #[test]
    fn test_clustered_insert_and_search() {
        let (cbpm, _dir) = make_bpm();
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = ClusteredIndex::create(&mut bpm, 0).unwrap();

        let key = Value::Integer(42);
        let row = b"hello world";
        idx.insert(&mut bpm, &key, row).unwrap();

        let result = idx.search(&mut bpm, &key).unwrap();
        assert_eq!(result, Some(row.to_vec()));

        let missing = idx.search(&mut bpm, &Value::Integer(99)).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_clustered_scan_all() {
        let (cbpm, _dir) = make_bpm();
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = ClusteredIndex::create(&mut bpm, 0).unwrap();

        for i in 0..100 {
            let key = Value::Integer(i);
            let data = format!("row_{}", i);
            idx.insert(&mut bpm, &key, data.as_bytes()).unwrap();
        }

        let rows = idx.scan_all(&mut bpm).unwrap();
        assert_eq!(rows.len(), 100);
        // Should be in sorted order
        assert_eq!(&rows[0], b"row_0");
        assert_eq!(&rows[99], b"row_99");
    }

    #[test]
    fn test_clustered_delete() {
        let (cbpm, _dir) = make_bpm();
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = ClusteredIndex::create(&mut bpm, 0).unwrap();

        for i in 0..10 {
            idx.insert(&mut bpm, &Value::Integer(i), format!("r{}", i).as_bytes()).unwrap();
        }

        let old = idx.delete(&mut bpm, &Value::Integer(5)).unwrap();
        assert_eq!(old, Some(b"r5".to_vec()));

        let result = idx.search(&mut bpm, &Value::Integer(5)).unwrap();
        assert!(result.is_none());

        let rows = idx.scan_all(&mut bpm).unwrap();
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn test_clustered_splits() {
        let (cbpm, _dir) = make_bpm();
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = ClusteredIndex::create(&mut bpm, 0).unwrap();

        // Insert enough to trigger multiple splits (each row ~50 bytes, page=4096)
        for i in 0..500 {
            let key = Value::Integer(i);
            let data = format!("data_{:04}", i);
            idx.insert(&mut bpm, &key, data.as_bytes()).unwrap();
        }

        // Verify all are retrievable
        for i in 0..500 {
            let result = idx.search(&mut bpm, &Value::Integer(i)).unwrap();
            assert!(result.is_some(), "missing key {}", i);
        }

        // Scan should return all in order
        let rows = idx.scan_all(&mut bpm).unwrap();
        assert_eq!(rows.len(), 500);
    }
}
