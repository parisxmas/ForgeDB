//! Overflow page storage for tuples that exceed a single page.
//!
//! Large tuples are split into chunks stored across linked overflow pages.
//! Format: each overflow page has a 6-byte header:
//!   [next_page_id: u32 LE][chunk_len: u16 LE][chunk_data...]
//! INVALID_PAGE_ID means this is the last chunk.

use crate::common::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::Result;
use crate::storage::local_bpm::LocalBpm;

/// Maximum data per overflow page (page size minus header).
const OVERFLOW_HEADER: usize = 6; // next_page_id(4) + chunk_len(2)
const CHUNK_SIZE: usize = PAGE_SIZE - OVERFLOW_HEADER;

/// Write a large tuple across overflow pages. Returns the PageId of the first overflow page.
pub fn write_overflow(bpm: &mut LocalBpm, data: &[u8]) -> Result<PageId> {
    let chunks: Vec<&[u8]> = data.chunks(CHUNK_SIZE).collect();
    let mut page_ids: Vec<PageId> = Vec::with_capacity(chunks.len());

    // Allocate all pages first
    for _ in 0..chunks.len() {
        let pid = bpm.new_page()?;
        bpm.unpin_page(pid, false)?;
        page_ids.push(pid);
    }

    // Write chunks with forward links
    for (i, chunk) in chunks.iter().enumerate() {
        let pid = page_ids[i];
        let next_pid = if i + 1 < page_ids.len() {
            page_ids[i + 1].0
        } else {
            INVALID_PAGE_ID
        };

        bpm.fetch_page(pid)?;
        {
            let page = bpm.get_page_mut(pid);
            // Header
            let next_bytes = next_pid.to_le_bytes();
            page.data[0..4].copy_from_slice(&next_bytes);
            let len_bytes = (chunk.len() as u16).to_le_bytes();
            page.data[4..6].copy_from_slice(&len_bytes);
            // Data
            page.data[OVERFLOW_HEADER..OVERFLOW_HEADER + chunk.len()].copy_from_slice(chunk);
        }
        bpm.unpin_page(pid, true)?;
    }

    Ok(page_ids[0])
}

/// Read a large tuple from overflow pages. Returns the reassembled data.
pub fn read_overflow(bpm: &mut LocalBpm, first_page_id: PageId) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut current = first_page_id;

    loop {
        bpm.fetch_page(current)?;
        let (next_pid, chunk) = {
            let page = bpm.get_page(current);
            let next = u32::from_le_bytes([
                page.data[0], page.data[1], page.data[2], page.data[3],
            ]);
            let len = u16::from_le_bytes([page.data[4], page.data[5]]) as usize;
            let chunk = page.data[OVERFLOW_HEADER..OVERFLOW_HEADER + len].to_vec();
            (next, chunk)
        };
        bpm.unpin_page(current, false)?;

        result.extend_from_slice(&chunk);

        if next_pid == INVALID_PAGE_ID {
            break;
        }
        current = PageId(next_pid);
    }

    Ok(result)
}
