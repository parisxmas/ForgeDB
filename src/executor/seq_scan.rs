use crate::catalog::Catalog;
use crate::common::{PageId, RID, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{ForgeError, Result};
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::heap_page;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::table_iterator::TableIterator;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;
use crate::txn::TxnContext;
use crate::txn::mvcc::{MVCC_HEADER_SIZE, XMAX_NONE, decode_version_header, is_visible};

/// Execute a sequential scan of all tuples in a table.
pub fn execute_seq_scan(
    table_name: &str,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    execute_seq_scan_full(table_name, bpm, catalog, None, None)
}

/// Execute a sequential scan with an optional row limit.
pub fn execute_seq_scan_limit(
    table_name: &str,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    limit: Option<usize>,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    execute_seq_scan_full(table_name, bpm, catalog, limit, None)
}

/// Execute a sequential scan with optional row limit and MVCC visibility filtering.
pub fn execute_seq_scan_full(
    table_name: &str,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    limit: Option<usize>,
    txn_ctx: Option<&TxnContext>,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let mvcc_enabled = info.mvcc_enabled;

    // For large tables (parallel hint), use batch-page scanning with pre-allocated buffers
    let estimated_rows = info.stats.as_ref().map(|s| s.row_count as usize).unwrap_or(0);
    let use_batch_scan = estimated_rows > 1000;

    let mut iter = TableIterator::new(info.first_page_id);
    let mut rows = if use_batch_scan {
        Vec::with_capacity(estimated_rows.min(limit.unwrap_or(usize::MAX)))
    } else {
        Vec::new()
    };

    while let Some((rid, raw)) = iter.next(bpm)? {
        if mvcc_enabled {
            if raw.len() < MVCC_HEADER_SIZE {
                // Malformed tuple, skip
                continue;
            }
            let (xmin, xmax) = decode_version_header(&raw);
            let tuple_data = &raw[MVCC_HEADER_SIZE..];

            if let Some(ctx) = txn_ctx {
                if !is_visible(xmin, xmax, &ctx.snapshot) {
                    continue;
                }
            } else {
                // Auto-commit mode: skip deleted tuples
                if xmax != XMAX_NONE {
                    continue;
                }
            }

            let values = deserialize(tuple_data, &schema)?;
            rows.push((rid, values));
        } else {
            let values = deserialize(&raw, &schema)?;
            rows.push((rid, values));
        }

        if let Some(max) = limit {
            if rows.len() >= max {
                break;
            }
        }
    }

    Ok((schema, rows))
}

// =========================================================================
// COUNT(*) fast path — bypasses LocalBpm, tuple deserialization, and Vec
// allocation entirely. Reads pages directly from ConcurrentBufferPool and
// counts live slots in the slot directory.
// =========================================================================

/// Fast COUNT(*) that counts live tuples by scanning slot directories only.
/// Bypasses LocalBpm (no page copy, no WAL before-image), does NOT deserialize
/// tuples, and does NOT allocate Vec<u8> per tuple.
///
/// For MVCC-enabled tables, falls back to the standard scan because we need
/// to read tuple headers to check visibility.
pub fn count_tuples_fast(
    table_name: &str,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
    txn_ctx: Option<&TxnContext>,
) -> Result<i64> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    // MVCC tables need per-tuple visibility checks; fall back to slow path
    if info.mvcc_enabled {
        return count_tuples_mvcc(info.first_page_id, cbpm, txn_ctx);
    }

    let mut count: i64 = 0;
    let mut page_id = info.first_page_id;

    loop {
        if page_id.0 == INVALID_PAGE_ID {
            break;
        }

        // Fetch + pin in the concurrent pool
        cbpm.fetch_page(page_id)?;
        let guard = cbpm.read_page(page_id)?;
        let data: &[u8; PAGE_SIZE] = guard.data();

        // Count live slots without reading tuple data
        count += heap_page::count_live_tuples(data) as i64;

        let next = heap_page::get_next_page_id(data);
        drop(guard);
        cbpm.unpin_page(page_id, false)?;

        page_id = PageId(next);
    }

    Ok(count)
}

/// MVCC-aware COUNT(*): still bypasses LocalBpm but must read tuple headers
/// to check visibility. Avoids full deserialization — only reads the 16-byte
/// version header per tuple.
fn count_tuples_mvcc(
    first_page_id: PageId,
    cbpm: &ConcurrentBufferPool,
    txn_ctx: Option<&TxnContext>,
) -> Result<i64> {
    let mut count: i64 = 0;
    let mut page_id = first_page_id;

    loop {
        if page_id.0 == INVALID_PAGE_ID {
            break;
        }

        cbpm.fetch_page(page_id)?;
        let guard = cbpm.read_page(page_id)?;
        let data: &[u8; PAGE_SIZE] = guard.data();

        let num_slots = heap_page::get_num_slots(data);
        for slot_id in 0..num_slots {
            // Use get_tuple_slice to read MVCC header without Vec allocation
            if let Some((off, len)) = heap_page::get_tuple_slice(data, slot_id) {
                if len < MVCC_HEADER_SIZE {
                    continue;
                }
                let tuple_bytes = &data[off..off + len];
                let (xmin, xmax) = decode_version_header(tuple_bytes);
                if let Some(ctx) = txn_ctx {
                    if is_visible(xmin, xmax, &ctx.snapshot) {
                        count += 1;
                    }
                } else {
                    // Auto-commit mode: skip deleted tuples
                    if xmax == XMAX_NONE {
                        count += 1;
                    }
                }
            }
        }

        let next = heap_page::get_next_page_id(data);
        drop(guard);
        cbpm.unpin_page(page_id, false)?;

        page_id = PageId(next);
    }

    Ok(count)
}

// =========================================================================
// Direct scan — bypasses LocalBpm for read-only sequential scans.
// Reads pages directly from ConcurrentBufferPool, avoiding the 16KB page
// copy into LocalBpm's cache and the WAL before-image capture.
// =========================================================================

/// Execute a sequential scan reading directly from the ConcurrentBufferPool.
/// Avoids LocalBpm overhead (no page copy, no WAL before-image).
/// Used for read-only scans.
pub fn execute_seq_scan_direct(
    table_name: &str,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
    limit: Option<usize>,
    txn_ctx: Option<&TxnContext>,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let mvcc_enabled = info.mvcc_enabled;

    let estimated_rows = info.stats.as_ref().map(|s| s.row_count as usize).unwrap_or(0);
    let mut rows = if estimated_rows > 0 {
        Vec::with_capacity(estimated_rows.min(limit.unwrap_or(usize::MAX)))
    } else {
        Vec::new()
    };

    let mut page_id = info.first_page_id;

    loop {
        if page_id.0 == INVALID_PAGE_ID {
            break;
        }

        cbpm.fetch_page(page_id)?;
        let guard = cbpm.read_page(page_id)?;
        let data: &[u8; PAGE_SIZE] = guard.data();

        let num_slots = heap_page::get_num_slots(data);
        for slot_id in 0..num_slots {
            // Use get_tuple_slice to avoid Vec allocation per tuple — read
            // directly from the page buffer while we hold the read guard.
            if let Some((off, len)) = heap_page::get_tuple_slice(data, slot_id) {
                let rid = RID { page_id, slot_id };
                let raw = &data[off..off + len];

                if mvcc_enabled {
                    if raw.len() < MVCC_HEADER_SIZE {
                        continue;
                    }
                    let (xmin, xmax) = decode_version_header(raw);
                    let tuple_data = &raw[MVCC_HEADER_SIZE..];

                    if let Some(ctx) = txn_ctx {
                        if !is_visible(xmin, xmax, &ctx.snapshot) {
                            continue;
                        }
                    } else {
                        if xmax != XMAX_NONE {
                            continue;
                        }
                    }

                    let values = deserialize(tuple_data, &schema)?;
                    rows.push((rid, values));
                } else {
                    let values = deserialize(raw, &schema)?;
                    rows.push((rid, values));
                }

                if let Some(max) = limit {
                    if rows.len() >= max {
                        drop(guard);
                        cbpm.unpin_page(page_id, false)?;
                        return Ok((schema, rows));
                    }
                }
            }
        }

        let next = heap_page::get_next_page_id(data);
        drop(guard);
        cbpm.unpin_page(page_id, false)?;

        page_id = PageId(next);
    }

    Ok((schema, rows))
}

/// Minimum number of pages to trigger parallel scan.
const PARALLEL_PAGE_THRESHOLD: usize = 4;

/// Execute a parallel sequential scan using `std::thread::scope`.
///
/// Splits the table's pages across multiple threads. Each thread creates its
/// own `LocalBpm` from the shared `ConcurrentBufferPool` reference. The scoped
/// threads guarantee all workers join before the scope exits, satisfying
/// lifetime requirements.
pub fn execute_seq_scan_parallel(
    table_name: &str,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    limit: Option<usize>,
    txn_ctx: Option<&TxnContext>,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let mvcc_enabled = info.mvcc_enabled;

    // Collect all page IDs by following the linked list
    let page_ids = TableIterator::collect_page_ids(info.first_page_id, bpm)?;

    // If fewer pages than threshold, fall back to single-threaded scan
    if page_ids.len() < PARALLEL_PAGE_THRESHOLD {
        return execute_seq_scan_full(table_name, bpm, catalog, limit, txn_ctx);
    }

    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(page_ids.len());

    let chunk_size = (page_ids.len() + num_workers - 1) / num_workers;
    let chunks: Vec<&[PageId]> = page_ids.chunks(chunk_size).collect();

    let cbpm = bpm.get_cbpm();

    // Use scoped threads so workers can borrow `cbpm` safely
    let results: Vec<Result<Vec<(RID, Vec<u8>)>>> = std::thread::scope(|s| {
        let handles: Vec<_> = chunks.iter().map(|chunk| {
            s.spawn(move || {
                let mut worker_bpm = LocalBpm::new(cbpm);
                TableIterator::scan_pages(chunk, &mut worker_bpm)
            })
        }).collect();

        handles.into_iter().map(|h| h.join().unwrap_or_else(|_| {
            Err(ForgeError::Execution("parallel scan worker panicked".into()))
        })).collect()
    });

    // Merge results from all workers and apply MVCC/deserialization
    let mut rows = Vec::new();
    for worker_result in results {
        let raw_rows = worker_result?;
        for (rid, raw) in raw_rows {
            if mvcc_enabled {
                if raw.len() < MVCC_HEADER_SIZE {
                    continue;
                }
                let (xmin, xmax) = decode_version_header(&raw);
                let tuple_data = &raw[MVCC_HEADER_SIZE..];

                if let Some(ctx) = txn_ctx {
                    if !is_visible(xmin, xmax, &ctx.snapshot) {
                        continue;
                    }
                } else {
                    if xmax != XMAX_NONE {
                        continue;
                    }
                }

                let values = deserialize(tuple_data, &schema)?;
                rows.push((rid, values));
            } else {
                let values = deserialize(&raw, &schema)?;
                rows.push((rid, values));
            }

            if let Some(max) = limit {
                if rows.len() >= max {
                    return Ok((schema, rows));
                }
            }
        }
    }

    Ok((schema, rows))
}
