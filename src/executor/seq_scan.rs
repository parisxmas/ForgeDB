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

        // Direct read — single lock acquisition, no pin/unpin overhead
        let guard = cbpm.read_page_direct(page_id)?;
        let data: &[u8; PAGE_SIZE] = guard.data();

        // Count live slots without reading tuple data
        count += heap_page::count_live_tuples(data) as i64;

        page_id = PageId(heap_page::get_next_page_id(data));
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

        let guard = cbpm.read_page_direct(page_id)?;
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

        page_id = PageId(next);
    }

    Ok(count)
}

// =========================================================================
// Fast aggregate path — SUM/MIN/MAX/AVG on a single column.
// Bypasses LocalBpm and full tuple deserialization: reads pages directly
// from ConcurrentBufferPool, extracts only the target column bytes using
// deserialize_single_column(), and accumulates the aggregate incrementally.
// =========================================================================

/// Aggregate function type for the fast path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Min,
    Max,
    Avg,
}

/// Fast single-column aggregate that bypasses LocalBpm and full deserialization.
/// Scans pages directly from ConcurrentBufferPool and reads only the target
/// column using `deserialize_single_column()`.
///
/// For MVCC-enabled tables in auto-commit mode, skips deleted tuples (xmax != NONE).
/// For MVCC tables inside explicit transactions, falls back to None (caller uses slow path).
pub fn aggregate_column_fast(
    table_name: &str,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
    col_name: &str,
    agg_func: AggFunc,
    txn_ctx: Option<&TxnContext>,
) -> Result<Option<Value>> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = &info.schema;

    // Find the column index
    let col_idx = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
        .ok_or_else(|| ForgeError::Execution(format!("column '{}' not found in '{}'", col_name, table_name)))?;

    let mvcc_enabled = info.mvcc_enabled;

    // For MVCC tables inside explicit transactions, fall back (need snapshot visibility)
    if mvcc_enabled && txn_ctx.is_some() {
        return Ok(None); // caller will use the slow path
    }

    let mut acc_int: Option<i64> = None;   // integer accumulator for SUM
    let mut acc_float: Option<f64> = None; // float accumulator for SUM (used if any float value)
    let mut is_float_sum = false;
    let mut count: i64 = 0;
    // For MIN/MAX we track the Value directly to preserve type
    let mut min_val: Option<Value> = None;
    let mut max_val: Option<Value> = None;

    // SIMD batch buffer: collect i32 values from a page, then process in bulk.
    // Avoids per-value branching in the tight loop and enables SIMD acceleration
    // for SUM/MIN/MAX on integer columns.
    let mut i32_batch: Vec<i32> = Vec::with_capacity(512);

    let mut page_id = info.first_page_id;

    loop {
        if page_id.0 == INVALID_PAGE_ID {
            break;
        }

        let guard = cbpm.read_page_direct(page_id)?;
        let data: &[u8; PAGE_SIZE] = guard.data();

        let num_slots = heap_page::get_num_slots(data);

        // Clear batch buffer for this page
        i32_batch.clear();
        let mut use_batch = !is_float_sum; // Only batch if we're still in integer mode

        for slot_id in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(data, slot_id) {
                let raw = &data[off..off + len];

                let tuple_data = if mvcc_enabled {
                    if raw.len() < MVCC_HEADER_SIZE {
                        continue;
                    }
                    let (_xmin, xmax) = decode_version_header(raw);
                    // Auto-commit mode: skip deleted tuples
                    if xmax != XMAX_NONE {
                        continue;
                    }
                    &raw[MVCC_HEADER_SIZE..]
                } else {
                    raw
                };

                let val = crate::tuple::tuple::deserialize_single_column(tuple_data, schema, col_idx)?;
                if matches!(val, Value::Null) {
                    continue; // NULL values are ignored by aggregates
                }

                // Try to batch i32 values for SIMD processing
                if use_batch {
                    match &val {
                        Value::Integer(n) => {
                            i32_batch.push(*n);
                            continue; // Skip per-value accumulation; batch below
                        }
                        _ => {
                            // Non-integer value found: flush batch and fall back
                            use_batch = false;
                            // Flush accumulated i32 batch
                            if !i32_batch.is_empty() {
                                flush_i32_batch(
                                    &i32_batch, agg_func,
                                    &mut acc_int, &mut acc_float, &mut is_float_sum,
                                    &mut count, &mut min_val, &mut max_val,
                                );
                                i32_batch.clear();
                            }
                        }
                    }
                }

                // Per-value accumulation (scalar fallback)
                match agg_func {
                    AggFunc::Sum | AggFunc::Avg => {
                        match &val {
                            Value::Integer(n) if !is_float_sum => {
                                acc_int = Some(acc_int.unwrap_or(0) + (*n as i64));
                            }
                            Value::BigInt(n) if !is_float_sum => {
                                acc_int = Some(acc_int.unwrap_or(0) + n);
                            }
                            _ => {
                                // Switch to float accumulation
                                if !is_float_sum {
                                    is_float_sum = true;
                                    acc_float = Some(acc_int.unwrap_or(0) as f64);
                                    acc_int = None;
                                }
                                acc_float = Some(acc_float.unwrap_or(0.0) + value_to_f64(&val));
                            }
                        }
                        count += 1;
                    }
                    AggFunc::Min => {
                        min_val = Some(match min_val {
                            None => val,
                            Some(cur) => if value_lt(&val, &cur) { val } else { cur },
                        });
                        count += 1;
                    }
                    AggFunc::Max => {
                        max_val = Some(match max_val {
                            None => val,
                            Some(cur) => if value_gt(&val, &cur) { val } else { cur },
                        });
                        count += 1;
                    }
                }
            }
        }

        // Flush any remaining batched i32 values from this page
        if !i32_batch.is_empty() {
            flush_i32_batch(
                &i32_batch, agg_func,
                &mut acc_int, &mut acc_float, &mut is_float_sum,
                &mut count, &mut min_val, &mut max_val,
            );
            i32_batch.clear();
        }

        page_id = PageId(heap_page::get_next_page_id(data));
    }

    // Produce the result
    if count == 0 {
        return Ok(Some(Value::Null));
    }

    let result = match agg_func {
        AggFunc::Sum => {
            if is_float_sum {
                let s = acc_float.unwrap_or(0.0);
                Some(Value::Float(s))
            } else {
                let s = acc_int.unwrap_or(0);
                // Preserve Integer type if the sum fits in i32
                if s >= i32::MIN as i64 && s <= i32::MAX as i64 {
                    Some(Value::Integer(s as i32))
                } else {
                    Some(Value::BigInt(s))
                }
            }
        }
        AggFunc::Avg => {
            let s = if is_float_sum {
                acc_float.unwrap_or(0.0)
            } else {
                acc_int.unwrap_or(0) as f64
            };
            Some(Value::Float(s / count as f64))
        }
        AggFunc::Min => Some(min_val.unwrap_or(Value::Null)),
        AggFunc::Max => Some(max_val.unwrap_or(Value::Null)),
    };

    Ok(result)
}

/// Flush a batch of i32 values using SIMD-accelerated aggregation.
/// This processes the entire batch at once using vectorized operations
/// instead of per-value branching.
#[inline]
fn flush_i32_batch(
    batch: &[i32],
    agg_func: AggFunc,
    acc_int: &mut Option<i64>,
    _acc_float: &mut Option<f64>,
    _is_float_sum: &mut bool,
    count: &mut i64,
    min_val: &mut Option<Value>,
    max_val: &mut Option<Value>,
) {
    if batch.is_empty() {
        return;
    }

    match agg_func {
        AggFunc::Sum | AggFunc::Avg => {
            let batch_sum = crate::executor::simd::sum_i32(batch);
            *acc_int = Some(acc_int.unwrap_or(0) + batch_sum);
            *count += batch.len() as i64;
        }
        AggFunc::Min => {
            if let Some(batch_min) = crate::executor::simd::min_i32(batch) {
                let batch_val = Value::Integer(batch_min);
                *min_val = Some(match min_val.take() {
                    None => batch_val,
                    Some(cur) => if value_lt(&batch_val, &cur) { batch_val } else { cur },
                });
            }
            *count += batch.len() as i64;
        }
        AggFunc::Max => {
            if let Some(batch_max) = crate::executor::simd::max_i32(batch) {
                let batch_val = Value::Integer(batch_max);
                *max_val = Some(match max_val.take() {
                    None => batch_val,
                    Some(cur) => if value_gt(&batch_val, &cur) { batch_val } else { cur },
                });
            }
            *count += batch.len() as i64;
        }
    }
}

/// Convert a Value to f64 for aggregate accumulation.
#[inline]
fn value_to_f64(val: &Value) -> f64 {
    match val {
        Value::Integer(n) => *n as f64,
        Value::BigInt(n) => *n as f64,
        Value::Float(f) => *f,
        Value::Boolean(b) => if *b { 1.0 } else { 0.0 },
        _ => 0.0,
    }
}

/// Compare two Values: returns true if a < b.
#[inline]
fn value_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x < y,
        (Value::BigInt(x), Value::BigInt(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Integer(x), Value::BigInt(y)) => (*x as i64) < *y,
        (Value::BigInt(x), Value::Integer(y)) => *x < (*y as i64),
        (Value::Integer(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Integer(y)) => *x < (*y as f64),
        (Value::BigInt(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::BigInt(y)) => *x < (*y as f64),
        (Value::Varchar(x), Value::Varchar(y)) => x < y,
        _ => false,
    }
}

/// Compare two Values: returns true if a > b.
#[inline]
fn value_gt(a: &Value, b: &Value) -> bool {
    value_lt(b, a)
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
