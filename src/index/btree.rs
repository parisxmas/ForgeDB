//! B-tree index implementation for ForgeDB.
//!
//! The tree starts as a single leaf (the root). When a leaf overflows it is
//! split and a separator key is pushed into the parent internal node. If the
//! root itself splits, a new root is created, increasing the tree height by
//! one.
//!
//! Deletion uses lazy removal: the entry is simply deleted from the leaf
//! without any rebalancing.

use crate::common::{PageId, RID, TableId, INVALID_PAGE_ID};
use crate::error::{ForgeError, Result};
use crate::index::btree_page;
use crate::storage::local_bpm::LocalBpm;
use crate::tuple::types::{DataType, Value};

/// A B-tree index over a single column of a table.
#[derive(Clone)]
pub struct BTreeIndex {
    pub root_page_id: PageId,
    pub key_type: DataType,
    pub table_id: TableId,
    pub key_column_index: usize,
}

impl BTreeIndex {
    /// Wrap an existing root page as a B-tree index.
    pub fn new(
        root_page_id: PageId,
        key_type: DataType,
        table_id: TableId,
        key_column_index: usize,
    ) -> Self {
        Self {
            root_page_id,
            key_type,
            table_id,
            key_column_index,
        }
    }

    /// Allocate a new root page (initialised as an empty leaf) and return the
    /// index handle.
    pub fn create(
        bpm: &mut LocalBpm,
        key_type: DataType,
        table_id: TableId,
        key_column_index: usize,
    ) -> Result<Self> {
        let page_id = bpm.new_page()?;
        {
            let page = bpm.get_page_mut(page_id);
            btree_page::leaf_init(&mut page.data);
        }
        bpm.unpin_page(page_id, true)?;

        Ok(Self {
            root_page_id: page_id,
            key_type,
            table_id,
            key_column_index,
        })
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    /// Look up an exact key and return its RID (if present).
    pub fn search(&self, bpm: &mut LocalBpm, key: &Value) -> Result<Option<RID>> {
        let key_bytes = key.to_sort_key_bytes();
        let leaf_id = self.find_leaf(bpm, &key_bytes)?;

        bpm.fetch_page(leaf_id)?;
        let page = bpm.get_page(leaf_id);
        let result = btree_page::leaf_search(&page.data, &key_bytes);
        bpm.unpin_page(leaf_id, false)?;
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    /// Insert a (key, RID) pair into the index. Splits pages as needed.
    pub fn insert(
        &mut self,
        bpm: &mut LocalBpm,
        key: &Value,
        rid: RID,
    ) -> Result<()> {
        let key_bytes = key.to_sort_key_bytes();
        let leaf_id = self.find_leaf(bpm, &key_bytes)?;

        bpm.fetch_page(leaf_id)?;

        // Try inserting directly.
        let has_room = {
            let page = bpm.get_page(leaf_id);
            btree_page::leaf_has_room(&page.data, key_bytes.len())
        };

        if has_room {
            let page = bpm.get_page_mut(leaf_id);
            btree_page::leaf_insert(&mut page.data, &key_bytes, rid);
            bpm.unpin_page(leaf_id, true)?;
            return Ok(());
        }

        // Leaf is full -- need to split.
        // Collect all existing entries + the new one, sort them, then
        // distribute across the old leaf and a new leaf.
        let (all_entries, old_next_leaf) = {
            let page = bpm.get_page(leaf_id);
            let mut entries = btree_page::leaf_get_all_entries(&page.data);
            let old_next = btree_page::leaf_get_next_leaf(&page.data);
            // Insert the new entry in sorted position.
            let pos = entries.partition_point(|(k, _)| k.as_slice() < key_bytes.as_slice());
            entries.insert(pos, (key_bytes.clone(), rid));
            (entries, old_next)
        };

        let mid = all_entries.len() / 2;

        // Unpin leaf before allocating (LocalBpm single-page cache).
        bpm.unpin_page(leaf_id, false)?;

        // Allocate new leaf.
        let new_leaf_id = bpm.new_page()?;
        bpm.unpin_page(new_leaf_id, false)?;

        // Re-init old leaf and populate with the lower half.
        bpm.fetch_page(leaf_id)?;
        {
            let page = bpm.get_page_mut(leaf_id);
            btree_page::leaf_init(&mut page.data);
            btree_page::leaf_set_next_leaf(&mut page.data, new_leaf_id.0);
            for (k, r) in &all_entries[..mid] {
                btree_page::leaf_insert(&mut page.data, k, *r);
            }
        }
        bpm.unpin_page(leaf_id, true)?;

        // Populate new leaf with upper half.
        bpm.fetch_page(new_leaf_id)?;
        {
            let page = bpm.get_page_mut(new_leaf_id);
            btree_page::leaf_init(&mut page.data);
            btree_page::leaf_set_next_leaf(&mut page.data, old_next_leaf);
            for (k, r) in &all_entries[mid..] {
                btree_page::leaf_insert(&mut page.data, k, *r);
            }
        }
        bpm.unpin_page(new_leaf_id, true)?;

        // The separator key to push up is the first key of the new (right) leaf.
        let separator = all_entries[mid].0.clone();

        // Push the separator into the parent.
        self.insert_into_parent(bpm, leaf_id, &separator, new_leaf_id)?;

        Ok(())
    }

    /// Recursively insert a separator key and right-child pointer into the
    /// parent of `left_page_id`. If `left_page_id` is the root and has no
    /// parent, a new root is created.
    fn insert_into_parent(
        &mut self,
        bpm: &mut LocalBpm,
        left_page_id: PageId,
        key: &[u8],
        right_page_id: PageId,
    ) -> Result<()> {
        // If left is the current root, create a new root.
        if left_page_id == self.root_page_id {
            let new_root_id = bpm.new_page()?;
            {
                let page = bpm.get_page_mut(new_root_id);
                btree_page::internal_init(&mut page.data);
                btree_page::internal_set_first_child(&mut page.data, left_page_id.0);
                btree_page::internal_insert(&mut page.data, key, right_page_id.0);
            }
            bpm.unpin_page(new_root_id, true)?;
            self.root_page_id = new_root_id;
            return Ok(());
        }

        // Otherwise, find the parent by traversing from the root.
        let parent_id = self.find_parent(bpm, self.root_page_id, left_page_id)?;

        bpm.fetch_page(parent_id)?;
        let has_room = {
            let page = bpm.get_page(parent_id);
            btree_page::internal_has_room(&page.data, key.len())
        };

        if has_room {
            let page = bpm.get_page_mut(parent_id);
            btree_page::internal_insert(&mut page.data, key, right_page_id.0);
            bpm.unpin_page(parent_id, true)?;
            return Ok(());
        }

        // Parent is full -- split the internal node.
        let all_entries = {
            let page = bpm.get_page(parent_id);
            let first_child = btree_page::internal_get_first_child(&page.data);
            let mut entries = btree_page::internal_get_all_entries(&page.data);
            // Insert the new key+child in sorted position.
            let pos = entries.partition_point(|(k, _)| k.as_slice() < key);
            entries.insert(pos, (key.to_vec(), right_page_id.0));
            (first_child, entries)
        };

        let (first_child, entries) = all_entries;
        let mid = entries.len() / 2;

        // The middle key will be pushed further up; it is NOT duplicated in
        // either child internal node.
        let push_up_key = entries[mid].0.clone();

        // Unpin parent before allocating (LocalBpm single-page cache).
        bpm.unpin_page(parent_id, false)?;

        // Allocate new internal node.
        let new_internal_id = bpm.new_page()?;
        bpm.unpin_page(new_internal_id, false)?;

        // Re-init old internal node with entries [0..mid).
        bpm.fetch_page(parent_id)?;
        {
            let page = bpm.get_page_mut(parent_id);
            btree_page::internal_init(&mut page.data);
            btree_page::internal_set_first_child(&mut page.data, first_child);
            for (k, c) in &entries[..mid] {
                btree_page::internal_insert(&mut page.data, k, *c);
            }
        }
        bpm.unpin_page(parent_id, true)?;

        // New internal node gets entries [mid+1..].
        // Its first_child is the right_child of entries[mid].
        bpm.fetch_page(new_internal_id)?;
        {
            let page = bpm.get_page_mut(new_internal_id);
            btree_page::internal_init(&mut page.data);
            btree_page::internal_set_first_child(&mut page.data, entries[mid].1);
            for (k, c) in &entries[mid + 1..] {
                btree_page::internal_insert(&mut page.data, k, *c);
            }
        }
        bpm.unpin_page(new_internal_id, true)?;

        // Push the middle key up.
        self.insert_into_parent(bpm, parent_id, &push_up_key, new_internal_id)?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Delete (lazy)
    // -----------------------------------------------------------------------

    /// Delete the entry with the given key. Returns `true` if found.
    /// No rebalancing is performed (lazy deletion).
    pub fn delete(
        &mut self,
        bpm: &mut LocalBpm,
        key: &Value,
    ) -> Result<bool> {
        let key_bytes = key.to_sort_key_bytes();
        let leaf_id = self.find_leaf(bpm, &key_bytes)?;

        bpm.fetch_page(leaf_id)?;
        let found = {
            let page = bpm.get_page_mut(leaf_id);
            btree_page::leaf_delete(&mut page.data, &key_bytes)
        };
        bpm.unpin_page(leaf_id, found)?;
        Ok(found)
    }

    // -----------------------------------------------------------------------
    // Range scan
    // -----------------------------------------------------------------------

    /// Scan keys in `[start_key, end_key]`. `None` bounds mean unbounded.
    /// Returns entries in ascending key order.
    pub fn range_scan(
        &self,
        bpm: &mut LocalBpm,
        start_key: Option<&Value>,
        end_key: Option<&Value>,
    ) -> Result<Vec<(Vec<u8>, RID)>> {
        let start_bytes = start_key.map(|v| v.to_sort_key_bytes());
        let end_bytes = end_key.map(|v| v.to_sort_key_bytes());

        // Find the starting leaf.
        let start_leaf_id = match &start_bytes {
            Some(k) => self.find_leaf(bpm, k)?,
            None => self.find_leftmost_leaf(bpm)?,
        };

        let mut results = Vec::new();
        let mut current_leaf_id = start_leaf_id;

        loop {
            bpm.fetch_page(current_leaf_id)?;
            let (entries, next_leaf) = {
                let page = bpm.get_page(current_leaf_id);
                let entries = btree_page::leaf_get_all_entries(&page.data);
                let next = btree_page::leaf_get_next_leaf(&page.data);
                (entries, next)
            };
            bpm.unpin_page(current_leaf_id, false)?;

            let mut exceeded_end = false;
            for (k, r) in entries {
                // Skip keys below the start bound.
                if let Some(ref sb) = start_bytes {
                    if k.as_slice() < sb.as_slice() {
                        continue;
                    }
                }
                // Stop if we exceed the end bound.
                if let Some(ref eb) = end_bytes {
                    if k.as_slice() > eb.as_slice() {
                        exceeded_end = true;
                        break;
                    }
                }
                results.push((k, r));
            }

            if exceeded_end || next_leaf == INVALID_PAGE_ID {
                break;
            }

            current_leaf_id = PageId(next_leaf);
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Traverse from the root to find the leaf page that should contain the
    /// given key.
    fn find_leaf(&self, bpm: &mut LocalBpm, key: &[u8]) -> Result<PageId> {
        let mut current_id = self.root_page_id;

        loop {
            bpm.fetch_page(current_id)?;
            let page = bpm.get_page(current_id);
            if btree_page::is_leaf(&page.data) {
                bpm.unpin_page(current_id, false)?;
                return Ok(current_id);
            }
            // Internal node: find the right child.
            let child_raw = btree_page::internal_search_child(&page.data, key);
            bpm.unpin_page(current_id, false)?;
            current_id = PageId(child_raw);
        }
    }

    /// Find the leftmost leaf by always following child_0.
    fn find_leftmost_leaf(&self, bpm: &mut LocalBpm) -> Result<PageId> {
        let mut current_id = self.root_page_id;

        loop {
            bpm.fetch_page(current_id)?;
            let page = bpm.get_page(current_id);
            if btree_page::is_leaf(&page.data) {
                bpm.unpin_page(current_id, false)?;
                return Ok(current_id);
            }
            let child_raw = btree_page::internal_get_first_child(&page.data);
            bpm.unpin_page(current_id, false)?;
            current_id = PageId(child_raw);
        }
    }

    /// Find the parent of `target_page_id` by traversing from `current_id`.
    /// This is O(n) but acceptable for an educational implementation.
    fn find_parent(
        &self,
        bpm: &mut LocalBpm,
        current_id: PageId,
        target_page_id: PageId,
    ) -> Result<PageId> {
        bpm.fetch_page(current_id)?;
        let page = bpm.get_page(current_id);

        if btree_page::is_leaf(&page.data) {
            bpm.unpin_page(current_id, false)?;
            return Err(ForgeError::Index(
                "find_parent reached a leaf without finding target".into(),
            ));
        }

        let n = btree_page::internal_get_num_keys(&page.data) as usize;
        let mut children = Vec::with_capacity(n + 1);
        for i in 0..=n {
            children.push(PageId(btree_page::internal_get_child(&page.data, i as u16)));
        }
        bpm.unpin_page(current_id, false)?;

        // Check if any child is the target.
        for &child in &children {
            if child == target_page_id {
                return Ok(current_id);
            }
        }

        // Recurse into children that are internal nodes.
        for &child in &children {
            bpm.fetch_page(child)?;
            let child_page = bpm.get_page(child);
            let child_is_leaf = btree_page::is_leaf(&child_page.data);
            bpm.unpin_page(child, false)?;

            if !child_is_leaf {
                match self.find_parent(bpm, child, target_page_id) {
                    Ok(pid) => return Ok(pid),
                    Err(_) => continue,
                }
            }
        }

        Err(ForgeError::Index(
            "find_parent: target page not found in tree".into(),
        ))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::concurrent_bpm::ConcurrentBufferPool;
    use crate::storage::local_bpm::LocalBpm;
    use crate::storage::disk_manager::DiskManager;

    fn make_bpm(pool_size: usize) -> (ConcurrentBufferPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_btree.db");
        let dm = DiskManager::new(&path).unwrap();
        let cbpm = ConcurrentBufferPool::new(pool_size, dm);
        (cbpm, dir)
    }

    #[test]
    fn test_insert_and_search_single() {
        let (cbpm, _dir) = make_bpm(64);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let key = Value::Integer(42);
        let rid = RID { page_id: PageId(10), slot_id: 5 };
        idx.insert(&mut bpm, &key, rid).unwrap();

        let result = idx.search(&mut bpm, &key).unwrap();
        assert_eq!(result, Some(rid));

        // Key not present.
        let result2 = idx.search(&mut bpm, &Value::Integer(99)).unwrap();
        assert_eq!(result2, None);
    }

    #[test]
    fn test_insert_many_and_search_all() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let n = 500;
        for i in 0..n {
            let key = Value::Integer(i);
            let rid = RID { page_id: PageId(i as u32), slot_id: (i % 100) as u16 };
            idx.insert(&mut bpm, &key, rid).unwrap();
        }

        for i in 0..n {
            let key = Value::Integer(i);
            let expected_rid = RID { page_id: PageId(i as u32), slot_id: (i % 100) as u16 };
            let result = idx.search(&mut bpm, &key).unwrap();
            assert_eq!(result, Some(expected_rid), "failed to find key {}", i);
        }
    }

    #[test]
    fn test_insert_reverse_order() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let n = 300;
        for i in (0..n).rev() {
            let key = Value::Integer(i);
            let rid = RID { page_id: PageId(i as u32), slot_id: 0 };
            idx.insert(&mut bpm, &key, rid).unwrap();
        }

        for i in 0..n {
            let key = Value::Integer(i);
            let result = idx.search(&mut bpm, &key).unwrap();
            assert!(result.is_some(), "missing key {}", i);
            assert_eq!(result.unwrap().page_id, PageId(i as u32));
        }
    }

    #[test]
    fn test_leaf_split() {
        // With 16KB pages, a leaf fits ~1259 entries (5-byte keys).
        // Insert 1500 to force at least one split.
        let (cbpm, _dir) = make_bpm(128);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let n = 1500;
        for i in 0..n {
            let key = Value::Integer(i);
            let rid = RID { page_id: PageId(i as u32), slot_id: 0 };
            idx.insert(&mut bpm, &key, rid).unwrap();
        }

        // Verify the root is no longer a leaf (it must have been split).
        bpm.fetch_page(idx.root_page_id).unwrap();
        let root_page = bpm.get_page(idx.root_page_id);
        assert!(
            btree_page::is_internal(&root_page.data),
            "root should be internal after splits"
        );
        bpm.unpin_page(idx.root_page_id, false).unwrap();

        // Verify all keys are findable.
        for i in 0..n {
            let key = Value::Integer(i);
            assert!(
                idx.search(&mut bpm, &key).unwrap().is_some(),
                "key {} not found after splits",
                i
            );
        }
    }

    #[test]
    fn test_internal_node_split() {
        // With varchar keys of ~100 bytes, entries are large and internal
        // nodes fill up faster, forcing internal splits.
        let (cbpm, _dir) = make_bpm(1024);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Varchar(120), TableId(0), 0).unwrap();

        let n = 2000;
        for i in 0..n {
            let s = format!("{:0>100}", i); // 100-char zero-padded string
            let key = Value::Varchar(s);
            let rid = RID { page_id: PageId(i as u32), slot_id: 0 };
            idx.insert(&mut bpm, &key, rid).unwrap();
        }

        // Verify all are searchable.
        for i in 0..n {
            let s = format!("{:0>100}", i);
            let key = Value::Varchar(s);
            let result = idx.search(&mut bpm, &key).unwrap();
            assert!(result.is_some(), "varchar key {} not found", i);
        }
    }

    #[test]
    fn test_range_scan() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let n = 200;
        for i in 0..n {
            let key = Value::Integer(i);
            let rid = RID { page_id: PageId(i as u32), slot_id: 0 };
            idx.insert(&mut bpm, &key, rid).unwrap();
        }

        // Scan [50, 99].
        let start = Value::Integer(50);
        let end = Value::Integer(99);
        let results = idx.range_scan(&mut bpm, Some(&start), Some(&end)).unwrap();
        assert_eq!(results.len(), 50);

        // Verify ordering.
        for (i, (_, rid)) in results.iter().enumerate() {
            assert_eq!(rid.page_id, PageId((50 + i) as u32));
        }
    }

    #[test]
    fn test_range_scan_unbounded() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        for i in 0..100 {
            idx.insert(
                &mut bpm,
                &Value::Integer(i),
                RID { page_id: PageId(i as u32), slot_id: 0 },
            ).unwrap();
        }

        // Full scan.
        let results = idx.range_scan(&mut bpm, None, None).unwrap();
        assert_eq!(results.len(), 100);

        // Scan [50, +inf).
        let start = Value::Integer(50);
        let results = idx.range_scan(&mut bpm, Some(&start), None).unwrap();
        assert_eq!(results.len(), 50);

        // Scan (-inf, 49].
        let end = Value::Integer(49);
        let results = idx.range_scan(&mut bpm, None, Some(&end)).unwrap();
        assert_eq!(results.len(), 50);
    }

    #[test]
    fn test_delete() {
        let (cbpm, _dir) = make_bpm(128);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        for i in 0..100 {
            idx.insert(
                &mut bpm,
                &Value::Integer(i),
                RID { page_id: PageId(i as u32), slot_id: 0 },
            ).unwrap();
        }

        // Delete key 42.
        assert!(idx.delete(&mut bpm, &Value::Integer(42)).unwrap());
        assert_eq!(idx.search(&mut bpm, &Value::Integer(42)).unwrap(), None);

        // Key 41 and 43 still present.
        assert!(idx.search(&mut bpm, &Value::Integer(41)).unwrap().is_some());
        assert!(idx.search(&mut bpm, &Value::Integer(43)).unwrap().is_some());

        // Deleting a non-existent key returns false.
        assert!(!idx.delete(&mut bpm, &Value::Integer(42)).unwrap());
        assert!(!idx.delete(&mut bpm, &Value::Integer(9999)).unwrap());
    }

    #[test]
    fn test_delete_many() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let n = 500;
        for i in 0..n {
            idx.insert(
                &mut bpm,
                &Value::Integer(i),
                RID { page_id: PageId(i as u32), slot_id: 0 },
            ).unwrap();
        }

        // Delete all even keys.
        for i in (0..n).step_by(2) {
            assert!(idx.delete(&mut bpm, &Value::Integer(i)).unwrap());
        }

        // Even keys gone, odd keys still there.
        for i in 0..n {
            let result = idx.search(&mut bpm, &Value::Integer(i)).unwrap();
            if i % 2 == 0 {
                assert!(result.is_none(), "even key {} should be deleted", i);
            } else {
                assert!(result.is_some(), "odd key {} should still exist", i);
            }
        }
    }

    #[test]
    fn test_large_scale_10k() {
        let (cbpm, _dir) = make_bpm(2048);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        let n: i32 = 10_000;

        // Insert in a scrambled order.
        let mut keys: Vec<i32> = (0..n).collect();
        // Simple deterministic shuffle using a basic LCG-style swap.
        for i in 0..keys.len() {
            let j = (i.wrapping_mul(2654435761)) % keys.len();
            keys.swap(i, j);
        }

        for &i in &keys {
            idx.insert(
                &mut bpm,
                &Value::Integer(i),
                RID { page_id: PageId(i as u32), slot_id: (i % 256) as u16 },
            ).unwrap();
        }

        // Search every key.
        for i in 0..n {
            let result = idx.search(&mut bpm, &Value::Integer(i)).unwrap();
            assert!(result.is_some(), "key {} not found in 10k test", i);
            assert_eq!(result.unwrap().page_id, PageId(i as u32));
        }

        // Range scan a window.
        let start = Value::Integer(1000);
        let end = Value::Integer(1999);
        let results = idx.range_scan(&mut bpm, Some(&start), Some(&end)).unwrap();
        assert_eq!(results.len(), 1000, "range scan should return 1000 entries");
    }

    #[test]
    fn test_varchar_keys() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Varchar(50), TableId(0), 0).unwrap();

        let words = ["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"];
        for (i, &w) in words.iter().enumerate() {
            idx.insert(
                &mut bpm,
                &Value::Varchar(w.to_string()),
                RID { page_id: PageId(i as u32), slot_id: 0 },
            ).unwrap();
        }

        for (i, &w) in words.iter().enumerate() {
            let result = idx.search(&mut bpm, &Value::Varchar(w.to_string())).unwrap();
            assert_eq!(result, Some(RID { page_id: PageId(i as u32), slot_id: 0 }));
        }

        // Range scan ["cherry", "fig"].
        let start = Value::Varchar("cherry".to_string());
        let end = Value::Varchar("fig".to_string());
        let results = idx.range_scan(&mut bpm, Some(&start), Some(&end)).unwrap();
        // cherry, date, elderberry, fig = 4
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_negative_integers() {
        let (cbpm, _dir) = make_bpm(256);
        let mut bpm = LocalBpm::new(&cbpm);
        let mut idx = BTreeIndex::create(&mut bpm, DataType::Integer, TableId(0), 0).unwrap();

        for i in -100..100 {
            idx.insert(
                &mut bpm,
                &Value::Integer(i),
                RID { page_id: PageId((i + 200) as u32), slot_id: 0 },
            ).unwrap();
        }

        for i in -100..100 {
            let result = idx.search(&mut bpm, &Value::Integer(i)).unwrap();
            assert!(result.is_some(), "key {} not found", i);
            assert_eq!(result.unwrap().page_id, PageId((i + 200) as u32));
        }

        // Range scan [-10, 10].
        let start = Value::Integer(-10);
        let end = Value::Integer(10);
        let results = idx.range_scan(&mut bpm, Some(&start), Some(&end)).unwrap();
        assert_eq!(results.len(), 21); // -10..=10 inclusive
    }
}
