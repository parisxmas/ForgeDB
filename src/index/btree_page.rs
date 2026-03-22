//! Stateless functions that operate directly on `[u8; PAGE_SIZE]` buffers to
//! read and write B-tree internal and leaf pages.
//!
//! ## Common header (8 bytes)
//! | Offset | Size | Field            |
//! |--------|------|------------------|
//! | 0      | 1    | page_type (0=internal, 1=leaf) |
//! | 1      | 2    | num_keys (u16 LE) |
//! | 3      | 4    | parent_page_id (u32 LE, reserved) |
//! | 7      | 1    | reserved          |
//!
//! ## Leaf page (after 8-byte header)
//! | 8..12  | next_leaf_page_id (u32 LE), INVALID_PAGE_ID if none |
//! | 12..   | variable-length entries: [key_len: u16 LE][key_bytes][page_id: u32 LE][slot_id: u16 LE] |
//!
//! ## Internal page (after 8-byte header)
//! | 8..12  | first_child (u32 LE) |
//! | 12..   | variable-length entries: [key_len: u16 LE][key_bytes][child_page_id: u32 LE] |

use crate::common::{PageId, RID, INVALID_PAGE_ID, PAGE_SIZE};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LEAF_HEADER_SIZE: usize = 12; // 8 common header + 4 next_leaf
const INTERNAL_HEADER_SIZE: usize = 12; // 8 common header + 4 first_child

const PAGE_TYPE_INTERNAL: u8 = 0;
const PAGE_TYPE_LEAF: u8 = 1;

// ---------------------------------------------------------------------------
// Tiny LE helpers
// ---------------------------------------------------------------------------

#[inline]
fn read_u16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

#[inline]
fn write_u16(buf: &mut [u8], off: usize, v: u16) {
    let bytes = v.to_le_bytes();
    buf[off] = bytes[0];
    buf[off + 1] = bytes[1];
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[inline]
fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    let bytes = v.to_le_bytes();
    buf[off] = bytes[0];
    buf[off + 1] = bytes[1];
    buf[off + 2] = bytes[2];
    buf[off + 3] = bytes[3];
}

// ---------------------------------------------------------------------------
// Common header accessors
// ---------------------------------------------------------------------------

/// Returns the page type (0 = internal, 1 = leaf).
pub fn get_page_type(page: &[u8; PAGE_SIZE]) -> u8 {
    page[0]
}

pub fn is_leaf(page: &[u8; PAGE_SIZE]) -> bool {
    page[0] == PAGE_TYPE_LEAF
}

pub fn is_internal(page: &[u8; PAGE_SIZE]) -> bool {
    page[0] == PAGE_TYPE_INTERNAL
}

fn set_num_keys(page: &mut [u8; PAGE_SIZE], n: u16) {
    write_u16(page, 1, n);
}

fn get_num_keys_raw(page: &[u8; PAGE_SIZE]) -> u16 {
    read_u16(page, 1)
}

// =========================================================================
// LEAF PAGE
// =========================================================================

/// Initialise a page as an empty leaf.
pub fn leaf_init(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    page[0] = PAGE_TYPE_LEAF;
    set_num_keys(page, 0);
    write_u32(page, 3, INVALID_PAGE_ID); // parent
    write_u32(page, 8, INVALID_PAGE_ID); // next_leaf
}

pub fn leaf_get_num_keys(page: &[u8; PAGE_SIZE]) -> u16 {
    get_num_keys_raw(page)
}

pub fn leaf_get_next_leaf(page: &[u8; PAGE_SIZE]) -> u32 {
    read_u32(page, 8)
}

pub fn leaf_set_next_leaf(page: &mut [u8; PAGE_SIZE], next: u32) {
    write_u32(page, 8, next);
}

/// Walk to the byte offset where entry `index` begins. Returns `None` if
/// `index >= num_keys`.
pub fn leaf_get_entry_offset(page: &[u8; PAGE_SIZE], index: u16) -> Option<usize> {
    let n = leaf_get_num_keys(page);
    if index >= n {
        return None;
    }
    let mut off = LEAF_HEADER_SIZE;
    for _ in 0..index {
        let key_len = read_u16(page, off) as usize;
        // entry size = 2 (key_len) + key_len + 4 (page_id) + 2 (slot_id)
        off += 2 + key_len + 4 + 2;
    }
    Some(off)
}

/// Return the total used bytes from the start of the page through the last
/// entry. This is the offset where the next entry would be written.
fn leaf_used_bytes(page: &[u8; PAGE_SIZE]) -> usize {
    let n = leaf_get_num_keys(page);
    if n == 0 {
        return LEAF_HEADER_SIZE;
    }
    let mut off = LEAF_HEADER_SIZE;
    for _ in 0..n {
        let key_len = read_u16(page, off) as usize;
        off += 2 + key_len + 4 + 2;
    }
    off
}

/// Read the key at the given entry index.
pub fn leaf_get_key(page: &[u8; PAGE_SIZE], index: u16) -> Option<Vec<u8>> {
    let off = leaf_get_entry_offset(page, index)?;
    let key_len = read_u16(page, off) as usize;
    Some(page[off + 2..off + 2 + key_len].to_vec())
}

/// Read the RID at the given entry index.
pub fn leaf_get_rid(page: &[u8; PAGE_SIZE], index: u16) -> Option<RID> {
    let off = leaf_get_entry_offset(page, index)?;
    let key_len = read_u16(page, off) as usize;
    let rid_off = off + 2 + key_len;
    let page_id = read_u32(page, rid_off);
    let slot_id = read_u16(page, rid_off + 4);
    Some(RID {
        page_id: PageId(page_id),
        slot_id,
    })
}

/// Insert a (key, RID) pair into the leaf in sorted order.
/// Returns `false` if the page does not have enough space.
pub fn leaf_insert(page: &mut [u8; PAGE_SIZE], key: &[u8], rid: RID) -> bool {
    let entry_size = 2 + key.len() + 4 + 2;
    let used = leaf_used_bytes(page);
    if used + entry_size > PAGE_SIZE {
        return false;
    }

    let n = leaf_get_num_keys(page) as usize;

    // Find insertion position via linear scan (keys are sorted).
    let mut insert_pos: usize = n;
    for i in 0..n {
        let existing_key = match leaf_get_key(page, i as u16) {
            Some(k) => k,
            None => return false,
        };
        if key < existing_key.as_slice() {
            insert_pos = i;
            break;
        }
    }

    // Shift entries at [insert_pos..n) to the right by entry_size bytes.
    if insert_pos < n {
        let shift_start = match leaf_get_entry_offset(page, insert_pos as u16) {
            Some(o) => o,
            None => return false,
        };
        // Byte range to shift: shift_start..used
        page.copy_within(shift_start..used, shift_start + entry_size);
    }

    // Write the new entry. After the shift, entries before insert_pos are
    // unchanged, so we can compute the offset by walking entries 0..insert_pos.
    let off = if insert_pos == 0 {
        LEAF_HEADER_SIZE
    } else {
        // Walk entries 0..insert_pos (they are unchanged).
        let mut o = LEAF_HEADER_SIZE;
        for _ in 0..insert_pos {
            let kl = read_u16(page, o) as usize;
            o += 2 + kl + 4 + 2;
        }
        o
    };

    write_u16(page, off, key.len() as u16);
    page[off + 2..off + 2 + key.len()].copy_from_slice(key);
    write_u32(page, off + 2 + key.len(), rid.page_id.0);
    write_u16(page, off + 2 + key.len() + 4, rid.slot_id);

    set_num_keys(page, (n + 1) as u16);
    true
}

/// Helper: computes offset after insert shift - not actually needed, using
/// inline walk above instead. Kept private to silence unused warnings.
#[allow(dead_code)]
fn leaf_get_entry_offset_after_insert(
    _page: &[u8; PAGE_SIZE],
    _index: u16,
    _entry_size: usize,
) -> usize {
    // Not used; see inline computation in leaf_insert.
    0
}

/// Binary search for an exact key, returning the associated RID.
pub fn leaf_search(page: &[u8; PAGE_SIZE], key: &[u8]) -> Option<RID> {
    let n = leaf_get_num_keys(page) as usize;
    if n == 0 {
        return None;
    }

    // Binary search.
    let mut lo: usize = 0;
    let mut hi: usize = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = match leaf_get_key(page, mid as u16) {
            Some(k) => k,
            None => return None,
        };
        match key.cmp(mid_key.as_slice()) {
            std::cmp::Ordering::Equal => return leaf_get_rid(page, mid as u16),
            std::cmp::Ordering::Less => hi = mid,
            std::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    None
}

/// Find the index where `key` would be inserted (first index whose key >= key).
/// Used by btree.rs to locate the insertion point.
pub fn leaf_find_insert_pos(page: &[u8; PAGE_SIZE], key: &[u8]) -> usize {
    let n = leaf_get_num_keys(page) as usize;
    let mut lo: usize = 0;
    let mut hi: usize = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = match leaf_get_key(page, mid as u16) {
            Some(k) => k,
            None => return lo,
        };
        if key <= mid_key.as_slice() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Delete the entry with the given key. Returns `true` if found and removed.
pub fn leaf_delete(page: &mut [u8; PAGE_SIZE], key: &[u8]) -> bool {
    let n = leaf_get_num_keys(page) as usize;
    if n == 0 {
        return false;
    }

    // Find the key.
    let mut found_idx: Option<usize> = None;
    for i in 0..n {
        let k = match leaf_get_key(page, i as u16) {
            Some(k) => k,
            None => return false,
        };
        if k.as_slice() == key {
            found_idx = Some(i);
            break;
        }
    }

    let idx = match found_idx {
        Some(i) => i,
        None => return false,
    };

    let entry_off = match leaf_get_entry_offset(page, idx as u16) {
        Some(o) => o,
        None => return false,
    };
    let key_len = read_u16(page, entry_off) as usize;
    let entry_size = 2 + key_len + 4 + 2;
    let used = leaf_used_bytes(page);

    // Shift entries after this one to the left.
    let src_start = entry_off + entry_size;
    if src_start < used {
        page.copy_within(src_start..used, entry_off);
    }

    // Zero out the freed space at the end.
    let new_used = used - entry_size;
    page[new_used..used].fill(0);

    set_num_keys(page, (n - 1) as u16);
    true
}

/// Collect all entries from the leaf as (key, RID) pairs.
pub fn leaf_get_all_entries(page: &[u8; PAGE_SIZE]) -> Vec<(Vec<u8>, RID)> {
    let n = leaf_get_num_keys(page) as usize;
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let key = match leaf_get_key(page, i as u16) {
            Some(k) => k,
            None => break,
        };
        let rid = match leaf_get_rid(page, i as u16) {
            Some(r) => r,
            None => break,
        };
        entries.push((key, rid));
    }
    entries
}

/// Returns true if the leaf has room for one more entry with the given key.
pub fn leaf_has_room(page: &[u8; PAGE_SIZE], key_len: usize) -> bool {
    let entry_size = 2 + key_len + 4 + 2;
    let used = leaf_used_bytes(page);
    used + entry_size <= PAGE_SIZE
}

// =========================================================================
// INTERNAL PAGE
// =========================================================================

/// Initialise a page as an empty internal node.
pub fn internal_init(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    page[0] = PAGE_TYPE_INTERNAL;
    set_num_keys(page, 0);
    write_u32(page, 3, INVALID_PAGE_ID); // parent
    write_u32(page, 8, INVALID_PAGE_ID); // first_child
}

pub fn internal_get_num_keys(page: &[u8; PAGE_SIZE]) -> u16 {
    get_num_keys_raw(page)
}

/// Set the leftmost child pointer (child_0).
pub fn internal_set_first_child(page: &mut [u8; PAGE_SIZE], child_page_id: u32) {
    write_u32(page, 8, child_page_id);
}

/// Get the first child (child_0).
pub fn internal_get_first_child(page: &[u8; PAGE_SIZE]) -> u32 {
    read_u32(page, 8)
}

/// Get the byte offset of the i-th (key, child) pair in the internal page.
/// Entries start at INTERNAL_HEADER_SIZE.
fn internal_entry_offset(page: &[u8; PAGE_SIZE], index: u16) -> Option<usize> {
    let n = internal_get_num_keys(page);
    if index >= n {
        return None;
    }
    let mut off = INTERNAL_HEADER_SIZE;
    for _ in 0..index {
        let key_len = read_u16(page, off) as usize;
        off += 2 + key_len + 4; // key_len field + key_data + child_page_id
    }
    Some(off)
}

/// Total used bytes in the internal page.
fn internal_used_bytes(page: &[u8; PAGE_SIZE]) -> usize {
    let n = internal_get_num_keys(page);
    if n == 0 {
        return INTERNAL_HEADER_SIZE;
    }
    let mut off = INTERNAL_HEADER_SIZE;
    for _ in 0..n {
        let key_len = read_u16(page, off) as usize;
        off += 2 + key_len + 4;
    }
    off
}

/// Get the child pointer at the given index. Index 0 is the first_child.
/// For n keys there are n+1 children: child_0, child_1, ..., child_n.
/// child_0 is stored separately, child_i (for i>=1) is stored after key_{i-1}.
pub fn internal_get_child(page: &[u8; PAGE_SIZE], index: u16) -> u32 {
    if index == 0 {
        return internal_get_first_child(page);
    }
    // child at index i is stored after key at index i-1
    let entry_off = match internal_entry_offset(page, index - 1) {
        Some(o) => o,
        None => return INVALID_PAGE_ID,
    };
    let key_len = read_u16(page, entry_off) as usize;
    read_u32(page, entry_off + 2 + key_len)
}

/// Get the key at index i in the internal page.
pub fn internal_get_key(page: &[u8; PAGE_SIZE], index: u16) -> Option<Vec<u8>> {
    let off = internal_entry_offset(page, index)?;
    let key_len = read_u16(page, off) as usize;
    Some(page[off + 2..off + 2 + key_len].to_vec())
}

/// Insert a (key, right_child) pair into the internal page in sorted order.
/// Returns `false` if there is no space.
pub fn internal_insert(page: &mut [u8; PAGE_SIZE], key: &[u8], right_child: u32) -> bool {
    let entry_size = 2 + key.len() + 4;
    let used = internal_used_bytes(page);
    if used + entry_size > PAGE_SIZE {
        return false;
    }

    let n = internal_get_num_keys(page) as usize;

    // Find insertion position.
    let mut insert_pos: usize = n;
    for i in 0..n {
        let existing_key = match internal_get_key(page, i as u16) {
            Some(k) => k,
            None => return false,
        };
        if key < existing_key.as_slice() {
            insert_pos = i;
            break;
        }
    }

    // Shift entries at [insert_pos..n) to the right.
    if insert_pos < n {
        let shift_start = match internal_entry_offset(page, insert_pos as u16) {
            Some(o) => o,
            None => return false,
        };
        page.copy_within(shift_start..used, shift_start + entry_size);
    }

    // Compute offset for the new entry.
    let off = {
        let mut o = INTERNAL_HEADER_SIZE;
        for _ in 0..insert_pos {
            let kl = read_u16(page, o) as usize;
            o += 2 + kl + 4;
        }
        o
    };

    write_u16(page, off, key.len() as u16);
    page[off + 2..off + 2 + key.len()].copy_from_slice(key);
    write_u32(page, off + 2 + key.len(), right_child);

    set_num_keys(page, (n + 1) as u16);
    true
}

/// Determine which child to follow for the given search key.
/// For keys k_0, k_1, ..., k_{n-1} and children c_0, c_1, ..., c_n:
///   - if key < k_0 => c_0
///   - if k_i <= key < k_{i+1} => c_{i+1}
///   - if key >= k_{n-1} => c_n
pub fn internal_search_child(page: &[u8; PAGE_SIZE], key: &[u8]) -> u32 {
    let n = internal_get_num_keys(page) as usize;
    for i in 0..n {
        let k = match internal_get_key(page, i as u16) {
            Some(k) => k,
            None => break,
        };
        if key < k.as_slice() {
            return internal_get_child(page, i as u16);
        }
    }
    // key >= all keys, follow rightmost child
    internal_get_child(page, n as u16)
}

/// Returns true if the internal page has room for one more entry with the
/// given key length.
pub fn internal_has_room(page: &[u8; PAGE_SIZE], key_len: usize) -> bool {
    let entry_size = 2 + key_len + 4;
    let used = internal_used_bytes(page);
    used + entry_size <= PAGE_SIZE
}

/// Collect all (key, right_child) pairs from the internal page.
pub fn internal_get_all_entries(page: &[u8; PAGE_SIZE]) -> Vec<(Vec<u8>, u32)> {
    let n = internal_get_num_keys(page) as usize;
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let key = match internal_get_key(page, i as u16) {
            Some(k) => k,
            None => break,
        };
        let child = internal_get_child(page, (i + 1) as u16);
        entries.push((key, child));
    }
    entries
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_init() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);
        assert!(is_leaf(&page));
        assert!(!is_internal(&page));
        assert_eq!(leaf_get_num_keys(&page), 0);
        assert_eq!(leaf_get_next_leaf(&page), INVALID_PAGE_ID);
    }

    #[test]
    fn test_leaf_insert_and_search() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);

        let k1 = vec![0x02, 0x80, 0x00, 0x00, 0x05]; // some key
        let r1 = RID { page_id: PageId(1), slot_id: 0 };

        assert!(leaf_insert(&mut page, &k1, r1));
        assert_eq!(leaf_get_num_keys(&page), 1);
        assert_eq!(leaf_search(&page, &k1), Some(r1));
    }

    #[test]
    fn test_leaf_sorted_insert() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);

        let k3 = vec![3u8];
        let k1 = vec![1u8];
        let k2 = vec![2u8];

        leaf_insert(&mut page, &k3, RID { page_id: PageId(3), slot_id: 0 });
        leaf_insert(&mut page, &k1, RID { page_id: PageId(1), slot_id: 0 });
        leaf_insert(&mut page, &k2, RID { page_id: PageId(2), slot_id: 0 });

        assert_eq!(leaf_get_num_keys(&page), 3);

        // Verify sorted order.
        assert_eq!(leaf_get_key(&page, 0).unwrap(), vec![1u8]);
        assert_eq!(leaf_get_key(&page, 1).unwrap(), vec![2u8]);
        assert_eq!(leaf_get_key(&page, 2).unwrap(), vec![3u8]);

        // Verify RIDs.
        assert_eq!(leaf_get_rid(&page, 0).unwrap().page_id, PageId(1));
        assert_eq!(leaf_get_rid(&page, 1).unwrap().page_id, PageId(2));
        assert_eq!(leaf_get_rid(&page, 2).unwrap().page_id, PageId(3));
    }

    #[test]
    fn test_leaf_delete() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);

        let k1 = vec![1u8];
        let k2 = vec![2u8];
        let k3 = vec![3u8];

        leaf_insert(&mut page, &k1, RID { page_id: PageId(1), slot_id: 0 });
        leaf_insert(&mut page, &k2, RID { page_id: PageId(2), slot_id: 0 });
        leaf_insert(&mut page, &k3, RID { page_id: PageId(3), slot_id: 0 });

        assert!(leaf_delete(&mut page, &k2));
        assert_eq!(leaf_get_num_keys(&page), 2);
        assert_eq!(leaf_search(&page, &k2), None);
        assert_eq!(leaf_search(&page, &k1), Some(RID { page_id: PageId(1), slot_id: 0 }));
        assert_eq!(leaf_search(&page, &k3), Some(RID { page_id: PageId(3), slot_id: 0 }));
    }

    #[test]
    fn test_leaf_full() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);

        // Each entry with a 200-byte key = 2 + 200 + 4 + 2 = 208 bytes.
        // Available: 4096 - 12 = 4084 bytes => 4084 / 208 = 19 entries.
        let big_key = vec![0xAA; 200];
        let mut count = 0u32;
        loop {
            let mut k = big_key.clone();
            // Make each key unique by appending count bytes.
            k[0] = (count & 0xFF) as u8;
            k[1] = ((count >> 8) & 0xFF) as u8;
            let ok = leaf_insert(
                &mut page,
                &k,
                RID { page_id: PageId(count), slot_id: 0 },
            );
            if !ok {
                break;
            }
            count += 1;
        }
        assert!(count > 0);
        assert_eq!(leaf_get_num_keys(&page), count as u16);
    }

    #[test]
    fn test_internal_init() {
        let mut page = [0u8; PAGE_SIZE];
        internal_init(&mut page);
        assert!(is_internal(&page));
        assert!(!is_leaf(&page));
        assert_eq!(internal_get_num_keys(&page), 0);
    }

    #[test]
    fn test_internal_insert_and_search() {
        let mut page = [0u8; PAGE_SIZE];
        internal_init(&mut page);
        internal_set_first_child(&mut page, 100);

        // Insert keys [10, 20, 30] with children [100, 101, 102, 103].
        assert!(internal_insert(&mut page, &[10u8], 101));
        assert!(internal_insert(&mut page, &[30u8], 103));
        assert!(internal_insert(&mut page, &[20u8], 102));

        assert_eq!(internal_get_num_keys(&page), 3);
        assert_eq!(internal_get_child(&page, 0), 100);

        // Verify sorted keys.
        assert_eq!(internal_get_key(&page, 0).unwrap(), vec![10u8]);
        assert_eq!(internal_get_key(&page, 1).unwrap(), vec![20u8]);
        assert_eq!(internal_get_key(&page, 2).unwrap(), vec![30u8]);

        // Search.
        assert_eq!(internal_search_child(&page, &[5u8]), 100);   // < 10
        assert_eq!(internal_search_child(&page, &[10u8]), 101);  // >= 10, < 20
        assert_eq!(internal_search_child(&page, &[15u8]), 101);  // >= 10, < 20
        assert_eq!(internal_search_child(&page, &[20u8]), 102);  // >= 20, < 30
        assert_eq!(internal_search_child(&page, &[30u8]), 103);  // >= 30
        assert_eq!(internal_search_child(&page, &[99u8]), 103);  // >= 30
    }

    #[test]
    fn test_leaf_next_leaf() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);
        assert_eq!(leaf_get_next_leaf(&page), INVALID_PAGE_ID);

        leaf_set_next_leaf(&mut page, 42);
        assert_eq!(leaf_get_next_leaf(&page), 42);
    }

    #[test]
    fn test_leaf_get_all_entries() {
        let mut page = [0u8; PAGE_SIZE];
        leaf_init(&mut page);

        leaf_insert(&mut page, &[3u8], RID { page_id: PageId(30), slot_id: 3 });
        leaf_insert(&mut page, &[1u8], RID { page_id: PageId(10), slot_id: 1 });
        leaf_insert(&mut page, &[2u8], RID { page_id: PageId(20), slot_id: 2 });

        let entries = leaf_get_all_entries(&page);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, vec![1u8]);
        assert_eq!(entries[1].0, vec![2u8]);
        assert_eq!(entries[2].0, vec![3u8]);
    }

    #[test]
    fn test_internal_get_all_entries() {
        let mut page = [0u8; PAGE_SIZE];
        internal_init(&mut page);
        internal_set_first_child(&mut page, 100);

        internal_insert(&mut page, &[10u8], 101);
        internal_insert(&mut page, &[20u8], 102);

        let entries = internal_get_all_entries(&page);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (vec![10u8], 101));
        assert_eq!(entries[1], (vec![20u8], 102));
    }
}
