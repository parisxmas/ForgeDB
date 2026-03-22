//! Slotted-page layout functions operating on raw `[u8; PAGE_SIZE]` buffers.
//!
//! Page layout:
//! ```text
//! [ Header (12 bytes) ][ Slot Array (grows →) ][ Free Space ][ Tuple Data (← grows) ]
//! ```
//!
//! Header (12 bytes, all little-endian):
//!   bytes 0..2   : num_slots   (u16)
//!   bytes 2..4   : free_space_start (u16) – not stored separately; always = 12 + num_slots * 4
//!                  (but we store it explicitly for fast access)
//!   bytes 4..6   : free_space_end   (u16) – lowest byte of tuple data region
//!   bytes 6..10  : next_page_id     (u32) – linked-list pointer, INVALID_PAGE_ID if none
//!   bytes 10..12 : flags            (u16) – reserved
//!
//! Slot entry (4 bytes each):
//!   bytes 0..2 : offset (u16) into page where tuple data starts. 0 = deleted.
//!   bytes 2..4 : length (u16) of tuple data. 0 when deleted.

use crate::common::{INVALID_PAGE_ID, PAGE_SIZE};

const HEADER_SIZE: usize = 12;
const SLOT_SIZE: usize = 4;

// ── Header accessors ───────────────────────────────────────────────

/// Initialize an empty slotted page.
pub fn init(page: &mut [u8; PAGE_SIZE]) {
    page.fill(0);
    set_num_slots(page, 0);
    set_free_space_start(page, HEADER_SIZE as u16);
    set_free_space_end(page, PAGE_SIZE as u16);
    set_next_page_id(page, INVALID_PAGE_ID);
    set_flags(page, 0);
}

pub fn get_num_slots(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes([page[0], page[1]])
}

fn set_num_slots(page: &mut [u8; PAGE_SIZE], val: u16) {
    let bytes = val.to_le_bytes();
    page[0] = bytes[0];
    page[1] = bytes[1];
}

fn get_free_space_start(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes([page[2], page[3]])
}

fn set_free_space_start(page: &mut [u8; PAGE_SIZE], val: u16) {
    let bytes = val.to_le_bytes();
    page[2] = bytes[0];
    page[3] = bytes[1];
}

fn get_free_space_end(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes([page[4], page[5]])
}

fn set_free_space_end(page: &mut [u8; PAGE_SIZE], val: u16) {
    let bytes = val.to_le_bytes();
    page[4] = bytes[0];
    page[5] = bytes[1];
}

pub fn get_next_page_id(page: &[u8; PAGE_SIZE]) -> u32 {
    u32::from_le_bytes([page[6], page[7], page[8], page[9]])
}

pub fn set_next_page_id(page: &mut [u8; PAGE_SIZE], next: u32) {
    let bytes = next.to_le_bytes();
    page[6] = bytes[0];
    page[7] = bytes[1];
    page[8] = bytes[2];
    page[9] = bytes[3];
}

fn _get_flags(page: &[u8; PAGE_SIZE]) -> u16 {
    u16::from_le_bytes([page[10], page[11]])
}

fn set_flags(page: &mut [u8; PAGE_SIZE], val: u16) {
    let bytes = val.to_le_bytes();
    page[10] = bytes[0];
    page[11] = bytes[1];
}

// ── Slot accessors ─────────────────────────────────────────────────

fn slot_offset_in_page(slot_id: u16) -> usize {
    HEADER_SIZE + slot_id as usize * SLOT_SIZE
}

fn get_slot(page: &[u8; PAGE_SIZE], slot_id: u16) -> (u16, u16) {
    let base = slot_offset_in_page(slot_id);
    let offset = u16::from_le_bytes([page[base], page[base + 1]]);
    let length = u16::from_le_bytes([page[base + 2], page[base + 3]]);
    (offset, length)
}

fn set_slot(page: &mut [u8; PAGE_SIZE], slot_id: u16, offset: u16, length: u16) {
    let base = slot_offset_in_page(slot_id);
    let off_bytes = offset.to_le_bytes();
    let len_bytes = length.to_le_bytes();
    page[base] = off_bytes[0];
    page[base + 1] = off_bytes[1];
    page[base + 2] = len_bytes[0];
    page[base + 3] = len_bytes[1];
}

// ── Fast-path counters ────────────────────────────────────────────

/// Count the number of live (non-deleted) tuple slots in a page WITHOUT
/// reading any tuple data. This is used by the COUNT(*) fast path to avoid
/// deserializing or allocating Vec<u8> per tuple.
///
/// A slot is considered deleted when its offset is 0.
///
/// On ARM64, uses NEON SIMD to check multiple slot offsets at once.
/// On other architectures, uses a scalar loop written for auto-vectorization.
pub fn count_live_tuples(page: &[u8; PAGE_SIZE]) -> u16 {
    let num_slots = get_num_slots(page);
    if num_slots == 0 {
        return 0;
    }
    // Use SIMD-accelerated slot counting.
    // Slot entries are 4 bytes each (2-byte offset + 2-byte length),
    // starting at HEADER_SIZE. We check the 2-byte offset at each entry.
    crate::executor::simd::count_nonzero_slots(
        page,
        HEADER_SIZE,
        SLOT_SIZE,
        num_slots as usize,
    )
}

// ── Public API ─────────────────────────────────────────────────────

/// Amount of free space available for new tuples (including the overhead of a
/// new slot entry if a deleted slot cannot be reused).
pub fn get_free_space(page: &[u8; PAGE_SIZE]) -> u16 {
    let start = get_free_space_start(page) as i32;
    let end = get_free_space_end(page) as i32;
    let free = end - start;
    if free < 0 { 0 } else { free as u16 }
}

/// Insert a tuple into the page. Returns the slot_id on success, or `None` if
/// there is not enough space.
pub fn insert_tuple(page: &mut [u8; PAGE_SIZE], data: &[u8]) -> Option<u16> {
    let data_len = data.len() as u16;

    // Try to reuse a deleted slot first.
    let num_slots = get_num_slots(page);
    let mut reuse_slot: Option<u16> = None;
    for i in 0..num_slots {
        let (off, _len) = get_slot(page, i);
        if off == 0 {
            reuse_slot = Some(i);
            break;
        }
    }

    // Determine how much free space we need.
    let extra_slot_space = if reuse_slot.is_some() { 0u16 } else { SLOT_SIZE as u16 };
    let total_needed = data_len + extra_slot_space;

    let free = get_free_space(page);
    if total_needed > free {
        return None;
    }

    // Allocate tuple space from the end of the page.
    let free_space_end = get_free_space_end(page);
    let new_free_space_end = free_space_end - data_len;
    set_free_space_end(page, new_free_space_end);

    // Copy tuple data.
    let dest_start = new_free_space_end as usize;
    page[dest_start..dest_start + data.len()].copy_from_slice(data);

    let slot_id;
    if let Some(reused) = reuse_slot {
        slot_id = reused;
        set_slot(page, slot_id, new_free_space_end, data_len);
    } else {
        slot_id = num_slots;
        set_slot(page, slot_id, new_free_space_end, data_len);
        set_num_slots(page, num_slots + 1);
        set_free_space_start(page, HEADER_SIZE as u16 + (num_slots + 1) * SLOT_SIZE as u16);
    }

    Some(slot_id)
}

/// Retrieve the tuple data for the given slot. Returns `None` if the slot is
/// out of range or has been deleted.
pub fn get_tuple(page: &[u8; PAGE_SIZE], slot_id: u16) -> Option<Vec<u8>> {
    let num_slots = get_num_slots(page);
    if slot_id >= num_slots {
        return None;
    }
    let (offset, length) = get_slot(page, slot_id);
    if offset == 0 {
        return None;
    }
    let start = offset as usize;
    let end = start + length as usize;
    Some(page[start..end].to_vec())
}

/// Return (offset, length) of the tuple in the given slot without allocating.
/// Returns `None` if the slot is out of range or has been deleted.
/// The caller can then borrow `&page[offset..offset+length]` directly.
pub fn get_tuple_slice(page: &[u8; PAGE_SIZE], slot_id: u16) -> Option<(usize, usize)> {
    let num_slots = get_num_slots(page);
    if slot_id >= num_slots {
        return None;
    }
    let (offset, length) = get_slot(page, slot_id);
    if offset == 0 {
        return None;
    }
    Some((offset as usize, length as usize))
}

/// Delete the tuple in the given slot. Returns `true` if the slot existed and
/// was not already deleted.
pub fn delete_tuple(page: &mut [u8; PAGE_SIZE], slot_id: u16) -> bool {
    let num_slots = get_num_slots(page);
    if slot_id >= num_slots {
        return false;
    }
    let (offset, _length) = get_slot(page, slot_id);
    if offset == 0 {
        return false; // already deleted
    }
    set_slot(page, slot_id, 0, 0);
    // Note: we do not reclaim the tuple data region here. A compaction pass
    // could be added in the future.
    true
}

/// Update a tuple in-place if the new data fits within the existing allocation.
/// Returns `true` on success, `false` if the new data is larger than the old
/// slot's space (caller must delete and re-insert at a higher level).
pub fn update_tuple(page: &mut [u8; PAGE_SIZE], slot_id: u16, data: &[u8]) -> bool {
    let num_slots = get_num_slots(page);
    if slot_id >= num_slots {
        return false;
    }
    let (offset, length) = get_slot(page, slot_id);
    if offset == 0 {
        return false; // deleted
    }
    let new_len = data.len() as u16;
    if new_len > length {
        return false; // does not fit
    }
    // Write the new data at the same offset; update the length.
    let start = offset as usize;
    page[start..start + data.len()].copy_from_slice(data);
    // If the new data is smaller, we keep the old allocation but record the
    // actual length so readers get the right data.
    set_slot(page, slot_id, offset, new_len);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_page() -> Box<[u8; PAGE_SIZE]> {
        let mut page = Box::new([0u8; PAGE_SIZE]);
        init(&mut *page);
        page
    }

    #[test]
    fn test_init() {
        let page = new_page();
        assert_eq!(get_num_slots(&page), 0);
        assert_eq!(get_free_space_start(&page), HEADER_SIZE as u16);
        assert_eq!(get_free_space_end(&page), PAGE_SIZE as u16);
        assert_eq!(get_next_page_id(&page), INVALID_PAGE_ID);
        assert_eq!(_get_flags(&page), 0);
    }

    #[test]
    fn test_insert_and_get() {
        let mut page = new_page();
        let data = b"hello world";
        let slot = insert_tuple(&mut page, data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(get_num_slots(&page), 1);

        let retrieved = get_tuple(&page, slot).unwrap();
        assert_eq!(retrieved, data.to_vec());
    }

    #[test]
    fn test_multiple_inserts() {
        let mut page = new_page();
        let s0 = insert_tuple(&mut page, b"aaa").unwrap();
        let s1 = insert_tuple(&mut page, b"bbb").unwrap();
        let s2 = insert_tuple(&mut page, b"ccc").unwrap();

        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(get_num_slots(&page), 3);

        assert_eq!(get_tuple(&page, 0).unwrap(), b"aaa".to_vec());
        assert_eq!(get_tuple(&page, 1).unwrap(), b"bbb".to_vec());
        assert_eq!(get_tuple(&page, 2).unwrap(), b"ccc".to_vec());
    }

    #[test]
    fn test_delete_tuple() {
        let mut page = new_page();
        let s0 = insert_tuple(&mut page, b"data").unwrap();
        assert!(delete_tuple(&mut page, s0));
        assert!(get_tuple(&page, s0).is_none());
        // Deleting again should return false.
        assert!(!delete_tuple(&mut page, s0));
    }

    #[test]
    fn test_slot_reuse() {
        let mut page = new_page();
        let s0 = insert_tuple(&mut page, b"first").unwrap();
        let _s1 = insert_tuple(&mut page, b"second").unwrap();

        delete_tuple(&mut page, s0);

        // Next insert should reuse slot 0.
        let reused = insert_tuple(&mut page, b"third").unwrap();
        assert_eq!(reused, 0);
        assert_eq!(get_tuple(&page, 0).unwrap(), b"third".to_vec());
    }

    #[test]
    fn test_update_tuple_fits() {
        let mut page = new_page();
        let s0 = insert_tuple(&mut page, b"hello").unwrap();
        assert!(update_tuple(&mut page, s0, b"hi"));
        assert_eq!(get_tuple(&page, s0).unwrap(), b"hi".to_vec());
    }

    #[test]
    fn test_update_tuple_too_large() {
        let mut page = new_page();
        let s0 = insert_tuple(&mut page, b"hi").unwrap();
        assert!(!update_tuple(&mut page, s0, b"this is way longer"));
        // Original data should be unchanged.
        assert_eq!(get_tuple(&page, s0).unwrap(), b"hi".to_vec());
    }

    #[test]
    fn test_page_full() {
        let mut page = new_page();
        // Fill the page with large tuples.
        let big = vec![0xABu8; 1000];
        let mut count = 0;
        while insert_tuple(&mut page, &big).is_some() {
            count += 1;
        }
        // We should be able to fit multiple ~1000-byte tuples in a page.
        assert!(count >= 3);
        assert!(count <= 16); // 16KB page fits up to ~16 tuples of 1000 bytes
    }

    #[test]
    fn test_next_page_id() {
        let mut page = new_page();
        assert_eq!(get_next_page_id(&page), INVALID_PAGE_ID);
        set_next_page_id(&mut page, 42);
        assert_eq!(get_next_page_id(&page), 42);
    }

    #[test]
    fn test_get_tuple_invalid_slot() {
        let page = new_page();
        assert!(get_tuple(&page, 0).is_none());
        assert!(get_tuple(&page, 100).is_none());
    }

    #[test]
    fn test_free_space_decreases() {
        let mut page = new_page();
        let initial_free = get_free_space(&page);
        insert_tuple(&mut page, b"some data here").unwrap();
        let after_free = get_free_space(&page);
        assert!(after_free < initial_free);
    }
}
