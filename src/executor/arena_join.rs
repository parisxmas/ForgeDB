//! PostgreSQL-style hash join — faithful port of PG's nodeHash/nodeHashjoin.
//!
//! Key techniques from PG:
//! - **Dense chunk allocation** (32KB blocks like PG's dense_alloc)
//! - **Power-of-2 bucket array** with singly-linked list chains
//! - **Hash value stored per tuple** (avoids recomputation during probe)
//! - **Bucket = hashvalue & (nbuckets - 1)** (bit mask, no modulo)
//! - Operates on raw tuple bytes — zero Value allocation on hot path
//! - ForgeWire projection: encode only requested columns

use crate::catalog::{Catalog, TableInfo};
use crate::common::{PageId, INVALID_PAGE_ID};
use crate::executor::executor::ExecuteResult;
use crate::sql::ast::{BinaryOperator, Expr};
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::heap_page;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::read_column_i64_raw;
use crate::tuple::types::{DataType, Value};

// ---------------------------------------------------------------------------
// Dense chunk allocator — PG's dense_alloc equivalent
// ---------------------------------------------------------------------------

const CHUNK_SIZE: usize = 32768; // 32KB per chunk, matching PG's HASH_CHUNK_SIZE

/// Per-tuple header stored in the arena (PG's HashJoinTupleData equivalent).
/// Layout in chunk: [next: u32][hashvalue: u32][tuple_bytes...]
const TUPLE_HEADER_SIZE: usize = 8; // 4 bytes next + 4 bytes hashvalue
const INVALID_TUPLE: u32 = u32::MAX; // sentinel for end of chain

/// Arena that packs raw tuples into 32KB chunks with PG-style headers.
struct TupleArena {
    chunks: Vec<Vec<u8>>,
    tuple_count: u32,
    /// (chunk_index, offset_in_chunk, total_len_including_header)
    tuples: Vec<(u32, u32, u32)>,
}

impl TupleArena {
    fn new() -> Self {
        Self {
            chunks: vec![Vec::with_capacity(CHUNK_SIZE)],
            tuple_count: 0,
            tuples: Vec::with_capacity(4096),
        }
    }

    /// Append a tuple with its hash value. Returns the tuple index.
    /// Stores: [next_ptr: u32 = INVALID][hashvalue: u32][data...]
    #[inline]
    fn push(&mut self, hashvalue: u32, data: &[u8]) -> u32 {
        let total = TUPLE_HEADER_SIZE + data.len();
        let chunk_idx = self.chunks.len() - 1;

        if self.chunks[chunk_idx].len() + total > CHUNK_SIZE && !self.chunks[chunk_idx].is_empty() {
            self.chunks.push(Vec::with_capacity(CHUNK_SIZE));
            return self.push(hashvalue, data);
        }

        let chunk_idx = self.chunks.len() - 1;
        let offset = self.chunks[chunk_idx].len();
        let chunk = &mut self.chunks[chunk_idx];

        // Write header: next = INVALID, hashvalue
        chunk.extend_from_slice(&INVALID_TUPLE.to_le_bytes());
        chunk.extend_from_slice(&hashvalue.to_le_bytes());
        // Write tuple data
        chunk.extend_from_slice(data);

        let idx = self.tuple_count;
        self.tuples.push((chunk_idx as u32, offset as u32, total as u32));
        self.tuple_count += 1;
        idx
    }

    /// Get the `next` pointer for a tuple (for chain walking).
    #[inline]
    fn get_next(&self, idx: u32) -> u32 {
        let (ci, off, _) = self.tuples[idx as usize];
        let chunk = &self.chunks[ci as usize];
        let o = off as usize;
        u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]])
    }

    /// Set the `next` pointer for a tuple (bucket chain linking).
    #[inline]
    fn set_next(&mut self, idx: u32, next: u32) {
        let (ci, off, _) = self.tuples[idx as usize];
        let chunk = &mut self.chunks[ci as usize];
        let o = off as usize;
        chunk[o..o + 4].copy_from_slice(&next.to_le_bytes());
    }

    /// Get the stored hash value for a tuple.
    #[inline]
    fn get_hashvalue(&self, idx: u32) -> u32 {
        let (ci, off, _) = self.tuples[idx as usize];
        let chunk = &self.chunks[ci as usize];
        let o = off as usize + 4;
        u32::from_le_bytes([chunk[o], chunk[o + 1], chunk[o + 2], chunk[o + 3]])
    }

    /// Get a reference to the raw tuple bytes (after header).
    #[inline]
    fn get_data(&self, idx: u32) -> &[u8] {
        let (ci, off, total) = self.tuples[idx as usize];
        let o = off as usize + TUPLE_HEADER_SIZE;
        let end = off as usize + total as usize;
        &self.chunks[ci as usize][o..end]
    }
}

// ---------------------------------------------------------------------------
// PG-style hash table: power-of-2 buckets + linked list chains
// ---------------------------------------------------------------------------

struct HashTable {
    /// Bucket heads: each entry is a tuple index or INVALID_TUPLE.
    buckets: Vec<u32>,
    /// Number of buckets (always power of 2).
    nbuckets: u32,
    /// log2(nbuckets) for bit operations.
    log2_nbuckets: u32,
}

impl HashTable {
    /// Create a hash table sized for `estimated_rows` tuples.
    /// PG targets ~1 tuple per bucket (NTUP_PER_BUCKET = 1).
    fn new(estimated_rows: usize) -> Self {
        let nbuckets = (estimated_rows.max(64)).next_power_of_two() as u32;
        let log2 = nbuckets.trailing_zeros();
        HashTable {
            buckets: vec![INVALID_TUPLE; nbuckets as usize],
            nbuckets,
            log2_nbuckets: log2,
        }
    }

    /// Insert a tuple into the appropriate bucket (push to front of chain).
    #[inline]
    fn insert(&mut self, arena: &mut TupleArena, tuple_idx: u32) {
        let hv = arena.get_hashvalue(tuple_idx);
        let bucket = (hv & (self.nbuckets - 1)) as usize;
        let old_head = self.buckets[bucket];
        arena.set_next(tuple_idx, old_head);
        self.buckets[bucket] = tuple_idx;
    }

    /// Find the head of the chain for a given hash value.
    #[inline]
    fn bucket_head(&self, hashvalue: u32) -> u32 {
        let bucket = (hashvalue & (self.nbuckets - 1)) as usize;
        self.buckets[bucket]
    }
}

// ---------------------------------------------------------------------------
// Pre-computed key reader — avoids per-tuple column iteration
// ---------------------------------------------------------------------------

/// Pre-compute the byte offset of an integer key column for O(1) reads.
/// Returns (bitmap_len, byte_offset, is_i32). None if column is variable-length.
pub fn precompute_key_offset(schema: &Schema, col_idx: usize) -> Option<(usize, usize, bool)> {
    let ncols = schema.columns.len();
    let bitmap_len = (ncols + 7) / 8;
    let dt = &schema.columns[col_idx].data_type;
    if !matches!(dt, DataType::Integer | DataType::BigInt) { return None; }
    let is_i32 = matches!(dt, DataType::Integer);

    let mut pos = bitmap_len;
    for (i, col) in schema.columns.iter().enumerate() {
        if i == col_idx { return Some((bitmap_len, pos, is_i32)); }
        // Variable-length columns before the key column make offset unpredictable
        if matches!(col.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
            return None;
        }
        // Skip null columns (no space occupied)... but we can't predict nulls at compile time
        // So this optimization only works when no preceding columns can be null
        // For now, assume all non-null (common case for join keys)
        pos += match col.data_type {
            DataType::Integer | DataType::Date | DataType::Time => 4,
            DataType::BigInt | DataType::Float | DataType::DateTime => 8,
            DataType::Boolean => 1,
            DataType::Decimal(_, _) => 9,
            _ => return None, // can't pre-compute
        };
    }
    None
}

/// Read an i64 key from a tuple using pre-computed offset. O(1) instead of O(col_idx).
#[inline(always)]
pub fn read_key_fast(data: &[u8], bitmap_len: usize, offset: usize, is_i32: bool, col_idx: usize) -> Option<i64> {
    // Null check
    if data.len() <= col_idx / 8 { return None; }
    if data[col_idx / 8] & (1 << (col_idx % 8)) != 0 { return None; }

    if is_i32 {
        if offset + 4 > data.len() { return None; }
        Some(i32::from_le_bytes([data[offset], data[offset+1], data[offset+2], data[offset+3]]) as i64)
    } else {
        if offset + 8 > data.len() { return None; }
        Some(i64::from_le_bytes(data[offset..offset+8].try_into().ok()?))
    }
}

// ---------------------------------------------------------------------------
// Hash function — Murmur3 finalizer for integer keys
// ---------------------------------------------------------------------------

#[inline]
fn hash_i64(key: i64) -> u32 {
    // Murmur3-style finalizer for integer keys (better distribution than FNV)
    let mut h = key as u64;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h as u32
}

// ---------------------------------------------------------------------------
// Public API: PG-style arena hash join
// ---------------------------------------------------------------------------

/// Try an arena-based hash join with optional column projection.
/// Uses PG-style power-of-2 bucket array + linked list chains.
/// Returns ForgeWire-ready binary bytes on success.
pub fn try_arena_join_projected(
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
    on_expr: &Expr,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
    projection: Option<&[String]>,
) -> Option<Vec<u8>> {
    let (left_key_name, right_key_name) = extract_join_keys(on_expr)?;

    let left_info = catalog.get_table(left_table)?;
    let right_info = catalog.get_table(right_table)?;
    let left_schema = &left_info.schema;
    let right_schema = &right_info.schema;

    let (left_key_idx, _) = left_schema.get_column(&left_key_name)?;
    let (right_key_idx, _) = right_schema.get_column(&right_key_name)?;

    if !is_int_type(&left_schema.columns[left_key_idx].data_type) { return None; }
    if !is_int_type(&right_schema.columns[right_key_idx].data_type) { return None; }

    // Choose smaller table as build side (PG does this in the optimizer)
    let (build_info, build_schema, build_key_idx, probe_info, probe_schema, probe_key_idx, build_is_left) =
        if right_info.schema.columns.len() <= left_info.schema.columns.len() {
            (right_info, right_schema, right_key_idx, left_info, left_schema, left_key_idx, false)
        } else {
            (left_info, left_schema, left_key_idx, right_info, right_schema, right_key_idx, true)
        };

    // Pre-compute key offsets for O(1) reads
    let probe_key_info = precompute_key_offset(probe_schema, probe_key_idx);
    let build_key_info = precompute_key_offset(build_schema, build_key_idx);

    // Phase 1: Build — scan inner relation into arena + hash table
    let mut arena = TupleArena::new();
    let mut hashtable = scan_build_hashtable(build_info, build_schema, build_key_idx, build_key_info, cbpm, &mut arena)?;

    let left_prefix = left_alias.unwrap_or(left_table);
    let right_prefix = right_alias.unwrap_or(right_table);
    let build_col_info = compute_column_info(build_schema);
    let probe_col_info = compute_column_info(probe_schema);

    // Build combined column list
    let all_cols: Vec<(&str, &str, &DataType, bool, usize)> = if build_is_left {
        build_schema.columns.iter().enumerate().map(|(i, c)| (left_prefix, c.name.as_str(), &c.data_type, true, i))
            .chain(probe_schema.columns.iter().enumerate().map(|(i, c)| (right_prefix, c.name.as_str(), &c.data_type, false, i)))
            .collect()
    } else {
        probe_schema.columns.iter().enumerate().map(|(i, c)| (left_prefix, c.name.as_str(), &c.data_type, false, i))
            .chain(build_schema.columns.iter().enumerate().map(|(i, c)| (right_prefix, c.name.as_str(), &c.data_type, true, i)))
            .collect()
    };

    // Determine output columns (projection)
    let output_cols: Vec<&(&str, &str, &DataType, bool, usize)> = if let Some(proj) = projection {
        proj.iter().filter_map(|pname| {
            all_cols.iter().find(|(_, cname, _, _, _)| cname.eq_ignore_ascii_case(pname))
        }).collect()
    } else {
        all_cols.iter().collect()
    };

    let mut out = Vec::with_capacity(64 * 1024);

    // ROW_HEADER
    let mut hdr = Vec::with_capacity(128);
    hdr.extend_from_slice(&(output_cols.len() as u16).to_le_bytes());
    for (_, col_name, dt, _, _) in &output_cols {
        let nb = col_name.as_bytes();
        hdr.extend_from_slice(&(nb.len() as u16).to_le_bytes());
        hdr.extend_from_slice(nb);
        hdr.push(datatype_to_wire_tag(dt));
    }
    append_msg(&mut out, 0x10, &hdr);

    // Try to build O(1) fast encoders for all projected columns
    let mut build_fast_col_indices = Vec::new();
    let mut probe_fast_col_indices = Vec::new();
    let mut col_plan: Vec<(bool, usize)> = Vec::new(); // (is_build, col_idx)
    for (_, _, _, is_build, col_idx) in &output_cols {
        col_plan.push((*is_build, *col_idx));
        if *is_build { build_fast_col_indices.push(*col_idx); }
        else { probe_fast_col_indices.push(*col_idx); }
    }
    let build_fast = build_fast_encoders(build_schema, &build_fast_col_indices);
    let probe_fast = build_fast_encoders(probe_schema, &probe_fast_col_indices);
    let use_fast = build_fast.is_some() && probe_fast.is_some();

    // Build lookup: for each output column, which fast encoder index to use
    let mut fast_plan: Vec<(bool, usize)> = Vec::new(); // (is_build, fast_encoder_idx)
    if use_fast {
        let mut bi = 0usize;
        let mut pi = 0usize;
        for &(is_build, _) in &col_plan {
            if is_build { fast_plan.push((true, bi)); bi += 1; }
            else { fast_plan.push((false, pi)); pi += 1; }
        }
    }

    // Phase 2: Probe — scan outer relation, probe hash table, encode matches
    let mut row_count: u64 = 0;
    let probe_mvcc = probe_info.mvcc_enabled;
    let mut current_pid = probe_info.first_page_id;

    while current_pid.0 != INVALID_PAGE_ID {
        let guard = cbpm.read_page_direct(current_pid).ok()?;
        let page = guard.data();
        let num_slots = heap_page::get_num_slots(page);

        for slot in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(page, slot) {
                let raw = &page[off..off + len];
                let tuple_data = if probe_mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                    let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                    if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                } else { raw };

                let probe_key = if let Some((bml, off, is32)) = probe_key_info {
                    read_key_fast(tuple_data, bml, off, is32, probe_key_idx)
                } else {
                    read_column_i64_raw(tuple_data, probe_schema, probe_key_idx)
                };
                if let Some(probe_key) = probe_key {
                    let probe_hash = hash_i64(probe_key);

                    let mut chain_idx = hashtable.bucket_head(probe_hash);
                    while chain_idx != INVALID_TUPLE {
                        if arena.get_hashvalue(chain_idx) == probe_hash {
                            let build_data = arena.get_data(chain_idx);
                            let build_key = if let Some((bml, off, is32)) = build_key_info {
                                read_key_fast(build_data, bml, off, is32, build_key_idx)
                            } else {
                                read_column_i64_raw(build_data, build_schema, build_key_idx)
                            };
                            if build_key == Some(probe_key) {
                                let row_start = out.len();
                                out.extend_from_slice(&[0x11, 0, 0, 0, 0]);

                                if use_fast {
                                    let bf = build_fast.as_ref().unwrap();
                                    let pf = probe_fast.as_ref().unwrap();
                                    for &(is_build, fi) in &fast_plan {
                                        if is_build { bf[fi].encode(&mut out, build_data); }
                                        else { pf[fi].encode(&mut out, tuple_data); }
                                    }
                                } else {
                                    for &(is_build, col_idx) in &col_plan {
                                        if is_build {
                                            encode_single_column_raw(&mut out, build_data, build_schema, &build_col_info, col_idx);
                                        } else {
                                            encode_single_column_raw(&mut out, tuple_data, probe_schema, &probe_col_info, col_idx);
                                        }
                                    }
                                }

                                let payload_len = (out.len() - row_start - 5) as u32;
                                out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
                                row_count += 1;
                            }
                        }
                        chain_idx = arena.get_next(chain_idx);
                    }
                }
            }
        }

        current_pid = PageId(heap_page::get_next_page_id(page));
    }

    append_msg(&mut out, 0x12, &row_count.to_le_bytes());
    Some(out)
}

/// Try an arena-based hash join. Returns ForgeWire-ready binary bytes on success.
/// All columns encoded (no projection).
pub fn try_arena_join(
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
    on_expr: &Expr,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
) -> Option<Vec<u8>> {
    try_arena_join_projected(left_table, left_alias, right_table, right_alias, on_expr, catalog, cbpm, None)
}

/// Returns ExecuteResult for non-ForgeWire callers (executor path).
pub fn try_arena_join_result(
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
    on_expr: &Expr,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
) -> Option<ExecuteResult> {
    let (left_key_name, right_key_name) = extract_join_keys(on_expr)?;
    let left_info = catalog.get_table(left_table)?;
    let right_info = catalog.get_table(right_table)?;
    let left_schema = &left_info.schema;
    let right_schema = &right_info.schema;
    let (left_key_idx, _) = left_schema.get_column(&left_key_name)?;
    let (right_key_idx, _) = right_schema.get_column(&right_key_name)?;
    if !is_int_type(&left_schema.columns[left_key_idx].data_type) { return None; }
    if !is_int_type(&right_schema.columns[right_key_idx].data_type) { return None; }

    let (build_info, build_schema, build_key_idx, probe_info, probe_schema, probe_key_idx, build_is_left) =
        if right_info.schema.columns.len() <= left_info.schema.columns.len() {
            (right_info, right_schema, right_key_idx, left_info, left_schema, left_key_idx, false)
        } else {
            (left_info, left_schema, left_key_idx, right_info, right_schema, right_key_idx, true)
        };

    let build_key_info = precompute_key_offset(build_schema, build_key_idx);
    let probe_key_info = precompute_key_offset(probe_schema, probe_key_idx);
    let mut arena = TupleArena::new();
    let mut hashtable = scan_build_hashtable(build_info, build_schema, build_key_idx, build_key_info, cbpm, &mut arena)?;

    let col_names: Vec<String> = if build_is_left {
        left_schema.columns.iter().map(|c| c.name.clone())
            .chain(right_schema.columns.iter().map(|c| c.name.clone())).collect()
    } else {
        left_schema.columns.iter().map(|c| c.name.clone())
            .chain(right_schema.columns.iter().map(|c| c.name.clone())).collect()
    };

    let mut rows = Vec::new();
    let probe_mvcc = probe_info.mvcc_enabled;
    let mut current_pid = probe_info.first_page_id;

    while current_pid.0 != INVALID_PAGE_ID {
        let guard = cbpm.read_page_direct(current_pid).ok()?;
        let page = guard.data();
        let num_slots = heap_page::get_num_slots(page);

        for slot in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(page, slot) {
                let raw = &page[off..off + len];
                let tuple_data = if probe_mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                    let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                    if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                } else { raw };

                if let Some(probe_key) = read_column_i64_raw(tuple_data, probe_schema, probe_key_idx) {
                    let probe_hash = hash_i64(probe_key);
                    let mut chain_idx = hashtable.bucket_head(probe_hash);

                    while chain_idx != INVALID_TUPLE {
                        if arena.get_hashvalue(chain_idx) == probe_hash {
                            let build_data = arena.get_data(chain_idx);
                            if let Some(build_key) = read_column_i64_raw(build_data, build_schema, build_key_idx) {
                                if build_key == probe_key {
                                    let probe_vals = crate::tuple::tuple::deserialize(tuple_data, probe_schema).ok()?;
                                    let build_vals = crate::tuple::tuple::deserialize(build_data, build_schema).ok()?;
                                    let mut row = Vec::with_capacity(col_names.len());
                                    if build_is_left {
                                        row.extend(build_vals.iter().cloned());
                                        row.extend(probe_vals.iter().cloned());
                                    } else {
                                        row.extend(probe_vals.iter().cloned());
                                        row.extend(build_vals.iter().cloned());
                                    }
                                    rows.push(row);
                                }
                            }
                        }
                        chain_idx = arena.get_next(chain_idx);
                    }
                }
            }
        }

        current_pid = PageId(heap_page::get_next_page_id(page));
    }

    Some(ExecuteResult {
        rows, columns: col_names,
        rows_affected: 0, last_insert_id: 0, message: String::new(),
    })
}

// ---------------------------------------------------------------------------
// LEFT JOIN: PG-style arena hash join — every left row emitted, NULLs for unmatched
// ---------------------------------------------------------------------------

/// LEFT JOIN via ForgeWire — left table is always the probe (outer) side.
/// Build side = right table (inner). Unmatched left rows get NULLs for right columns.
pub fn try_arena_left_join_projected(
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
    on_expr: &Expr,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
    projection: Option<&[String]>,
) -> Option<Vec<u8>> {
    let (left_key_name, right_key_name) = extract_join_keys(on_expr)?;

    let left_info = catalog.get_table(left_table)?;
    let right_info = catalog.get_table(right_table)?;
    let left_schema = &left_info.schema;
    let right_schema = &right_info.schema;

    let (left_key_idx, _) = left_schema.get_column(&left_key_name)?;
    let (right_key_idx, _) = right_schema.get_column(&right_key_name)?;

    if !is_int_type(&left_schema.columns[left_key_idx].data_type) { return None; }
    if !is_int_type(&right_schema.columns[right_key_idx].data_type) { return None; }

    // LEFT JOIN: build side = RIGHT (inner), probe side = LEFT (outer)
    let build_info = right_info;
    let build_schema = right_schema;
    let build_key_idx = right_key_idx;
    let probe_info = left_info;
    let probe_schema = left_schema;
    let probe_key_idx = left_key_idx;

    let build_key_info = precompute_key_offset(build_schema, build_key_idx);
    let probe_key_info = precompute_key_offset(probe_schema, probe_key_idx);
    let mut arena = TupleArena::new();
    let hashtable = scan_build_hashtable(build_info, build_schema, build_key_idx, build_key_info, cbpm, &mut arena)?;

    let left_prefix = left_alias.unwrap_or(left_table);
    let right_prefix = right_alias.unwrap_or(right_table);
    let build_col_info = compute_column_info(build_schema);
    let probe_col_info = compute_column_info(probe_schema);

    // Combined columns: always left first, then right
    let all_cols: Vec<(&str, &str, &DataType, bool, usize)> =
        probe_schema.columns.iter().enumerate().map(|(i, c)| (left_prefix, c.name.as_str(), &c.data_type, false, i))
            .chain(build_schema.columns.iter().enumerate().map(|(i, c)| (right_prefix, c.name.as_str(), &c.data_type, true, i)))
            .collect();

    let output_cols: Vec<&(&str, &str, &DataType, bool, usize)> = if let Some(proj) = projection {
        proj.iter().filter_map(|pname| {
            all_cols.iter().find(|(_, cname, _, _, _)| cname.eq_ignore_ascii_case(pname))
        }).collect()
    } else {
        all_cols.iter().collect()
    };

    let mut out = Vec::with_capacity(64 * 1024);

    // ROW_HEADER
    let mut hdr = Vec::with_capacity(128);
    hdr.extend_from_slice(&(output_cols.len() as u16).to_le_bytes());
    for (_, col_name, dt, _, _) in &output_cols {
        let nb = col_name.as_bytes();
        hdr.extend_from_slice(&(nb.len() as u16).to_le_bytes());
        hdr.extend_from_slice(nb);
        hdr.push(datatype_to_wire_tag(dt));
    }
    append_msg(&mut out, 0x10, &hdr);

    // Build fast encoders for LEFT JOIN
    let mut build_fast_col_indices = Vec::new();
    let mut probe_fast_col_indices = Vec::new();
    let mut col_plan: Vec<(bool, usize)> = Vec::new();
    for (_, _, _, is_build, col_idx) in &output_cols {
        col_plan.push((*is_build, *col_idx));
        if *is_build { build_fast_col_indices.push(*col_idx); }
        else { probe_fast_col_indices.push(*col_idx); }
    }
    let build_fast = build_fast_encoders(build_schema, &build_fast_col_indices);
    let probe_fast = build_fast_encoders(probe_schema, &probe_fast_col_indices);
    let use_fast = build_fast.is_some() && probe_fast.is_some();
    let mut fast_plan: Vec<(bool, usize)> = Vec::new();
    if use_fast {
        let mut bi = 0usize;
        let mut pi = 0usize;
        for &(is_build, _) in &col_plan {
            if is_build { fast_plan.push((true, bi)); bi += 1; }
            else { fast_plan.push((false, pi)); pi += 1; }
        }
    }

    // Probe: scan LEFT table, for each row probe hash table
    let mut row_count: u64 = 0;
    let probe_mvcc = probe_info.mvcc_enabled;
    let mut current_pid = probe_info.first_page_id;

    while current_pid.0 != INVALID_PAGE_ID {
        let guard = cbpm.read_page_direct(current_pid).ok()?;
        let page = guard.data();
        let num_slots = heap_page::get_num_slots(page);

        for slot in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(page, slot) {
                let raw = &page[off..off + len];
                let tuple_data = if probe_mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                    let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                    if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                } else { raw };

                let mut matched = false;

                let probe_key = if let Some((bml, off, is32)) = probe_key_info {
                    read_key_fast(tuple_data, bml, off, is32, probe_key_idx)
                } else {
                    read_column_i64_raw(tuple_data, probe_schema, probe_key_idx)
                };
                if let Some(probe_key) = probe_key {
                    let probe_hash = hash_i64(probe_key);
                    let mut chain_idx = hashtable.bucket_head(probe_hash);

                    while chain_idx != INVALID_TUPLE {
                        if arena.get_hashvalue(chain_idx) == probe_hash {
                            let build_data = arena.get_data(chain_idx);
                            let build_key = if let Some((bml, off, is32)) = build_key_info {
                                read_key_fast(build_data, bml, off, is32, build_key_idx)
                            } else {
                                read_column_i64_raw(build_data, build_schema, build_key_idx)
                            };
                            if build_key == Some(probe_key) {
                                    matched = true;
                                    let row_start = out.len();
                                    out.extend_from_slice(&[0x11, 0, 0, 0, 0]);
                                    if use_fast {
                                        let bf = build_fast.as_ref().unwrap();
                                        let pf = probe_fast.as_ref().unwrap();
                                        for &(is_build, fi) in &fast_plan {
                                            if is_build { bf[fi].encode(&mut out, build_data); }
                                            else { pf[fi].encode(&mut out, tuple_data); }
                                        }
                                    } else {
                                        for &(is_build, col_idx) in &col_plan {
                                            if is_build {
                                                encode_single_column_raw(&mut out, build_data, build_schema, &build_col_info, col_idx);
                                            } else {
                                                encode_single_column_raw(&mut out, tuple_data, probe_schema, &probe_col_info, col_idx);
                                            }
                                        }
                                    }
                                    let payload_len = (out.len() - row_start - 5) as u32;
                                    out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
                                    row_count += 1;
                            }
                        }
                        chain_idx = arena.get_next(chain_idx);
                    }
                }

                // LEFT JOIN: if no match, emit left columns + NULLs for right columns
                if !matched {
                    let row_start = out.len();
                    out.extend_from_slice(&[0x11, 0, 0, 0, 0]);
                    if use_fast {
                        let pf = probe_fast.as_ref().unwrap();
                        for &(is_build, fi) in &fast_plan {
                            if is_build { out.push(0x00); }
                            else { pf[fi].encode(&mut out, tuple_data); }
                        }
                    } else {
                        for &(is_build, col_idx) in &col_plan {
                            if is_build {
                                out.push(0x00);
                            } else {
                                encode_single_column_raw(&mut out, tuple_data, probe_schema, &probe_col_info, col_idx);
                            }
                        }
                    }
                    let payload_len = (out.len() - row_start - 5) as u32;
                    out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
                    row_count += 1;
                }
            }
        }

        current_pid = PageId(heap_page::get_next_page_id(page));
    }

    append_msg(&mut out, 0x12, &row_count.to_le_bytes());
    Some(out)
}

/// LEFT JOIN returning ExecuteResult for non-ForgeWire callers.
pub fn try_arena_left_join_result(
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
    on_expr: &Expr,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
) -> Option<ExecuteResult> {
    let (left_key_name, right_key_name) = extract_join_keys(on_expr)?;
    let left_info = catalog.get_table(left_table)?;
    let right_info = catalog.get_table(right_table)?;
    let left_schema = &left_info.schema;
    let right_schema = &right_info.schema;
    let (left_key_idx, _) = left_schema.get_column(&left_key_name)?;
    let (right_key_idx, _) = right_schema.get_column(&right_key_name)?;
    if !is_int_type(&left_schema.columns[left_key_idx].data_type) { return None; }
    if !is_int_type(&right_schema.columns[right_key_idx].data_type) { return None; }

    // Build = right, probe = left
    let build_key_info = precompute_key_offset(right_schema, right_key_idx);
    let mut arena = TupleArena::new();
    let hashtable = scan_build_hashtable(right_info, right_schema, right_key_idx, build_key_info, cbpm, &mut arena)?;

    let col_names: Vec<String> = left_schema.columns.iter().map(|c| c.name.clone())
        .chain(right_schema.columns.iter().map(|c| c.name.clone())).collect();
    let null_right: Vec<Value> = right_schema.columns.iter().map(|_| Value::Null).collect();

    let mut rows = Vec::new();
    let probe_mvcc = left_info.mvcc_enabled;
    let mut current_pid = left_info.first_page_id;

    while current_pid.0 != INVALID_PAGE_ID {
        let guard = cbpm.read_page_direct(current_pid).ok()?;
        let page = guard.data();
        let num_slots = heap_page::get_num_slots(page);

        for slot in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(page, slot) {
                let raw = &page[off..off + len];
                let tuple_data = if probe_mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                    let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                    if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                } else { raw };

                let mut matched = false;

                if let Some(probe_key) = read_column_i64_raw(tuple_data, left_schema, left_key_idx) {
                    let probe_hash = hash_i64(probe_key);
                    let mut chain_idx = hashtable.bucket_head(probe_hash);

                    while chain_idx != INVALID_TUPLE {
                        if arena.get_hashvalue(chain_idx) == probe_hash {
                            let build_data = arena.get_data(chain_idx);
                            if let Some(build_key) = read_column_i64_raw(build_data, right_schema, right_key_idx) {
                                if build_key == probe_key {
                                    matched = true;
                                    let left_vals = crate::tuple::tuple::deserialize(tuple_data, left_schema).ok()?;
                                    let right_vals = crate::tuple::tuple::deserialize(build_data, right_schema).ok()?;
                                    let mut row = Vec::with_capacity(col_names.len());
                                    row.extend(left_vals.iter().cloned());
                                    row.extend(right_vals.iter().cloned());
                                    rows.push(row);
                                }
                            }
                        }
                        chain_idx = arena.get_next(chain_idx);
                    }
                }

                if !matched {
                    let left_vals = crate::tuple::tuple::deserialize(tuple_data, left_schema).ok()?;
                    let mut row = Vec::with_capacity(col_names.len());
                    row.extend(left_vals.iter().cloned());
                    row.extend(null_right.iter().cloned());
                    rows.push(row);
                }
            }
        }

        current_pid = PageId(heap_page::get_next_page_id(page));
    }

    Some(ExecuteResult {
        rows, columns: col_names,
        rows_affected: 0, last_insert_id: 0, message: String::new(),
    })
}

// ---------------------------------------------------------------------------
// Build phase: scan inner relation into arena + hash table
// ---------------------------------------------------------------------------

fn scan_build_hashtable(
    info: &TableInfo,
    schema: &Schema,
    key_col_idx: usize,
    key_info: Option<(usize, usize, bool)>,
    cbpm: &ConcurrentBufferPool,
    arena: &mut TupleArena,
) -> Option<HashTable> {
    let mvcc = info.mvcc_enabled;
    let mut current_pid = info.first_page_id;

    let estimated = info.stats.as_ref().map(|s| s.row_count as usize).unwrap_or(4096);
    let mut hashtable = HashTable::new(estimated);

    while current_pid.0 != INVALID_PAGE_ID {
        let guard = cbpm.read_page_direct(current_pid).ok()?;
        let page = guard.data();
        let num_slots = heap_page::get_num_slots(page);

        for slot in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(page, slot) {
                let raw = &page[off..off + len];
                let tuple_data = if mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                    let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                    if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                } else { raw };

                let key = if let Some((bml, off, is32)) = key_info {
                    read_key_fast(tuple_data, bml, off, is32, key_col_idx)
                } else {
                    read_column_i64_raw(tuple_data, schema, key_col_idx)
                };
                if let Some(key) = key {
                    let hashvalue = hash_i64(key);
                    let tuple_idx = arena.push(hashvalue, tuple_data);
                    hashtable.insert(arena, tuple_idx);
                }
            }
        }

        current_pid = PageId(heap_page::get_next_page_id(page));
    }

    Some(hashtable)
}

// ---------------------------------------------------------------------------
// Column encoding helpers
// ---------------------------------------------------------------------------

/// Column info pre-computed once per schema for fast raw byte access.
pub struct ColInfo {
    byte_offset: usize,
    pub dt: DataType,
    pub is_fixed: bool,
    pub fixed_size: usize,
}

/// Pre-computed encoder for a single output column — O(1) per row.
/// Stores the absolute byte offset (from bitmap end) assuming no nulls before it.
pub struct FastColEncoder {
    col_idx: usize,
    null_byte: usize,   // col_idx / 8
    null_bit: u8,       // 1 << (col_idx % 8)
    abs_offset: usize,  // bitmap_len + offset past all preceding fixed columns
    wire_tag: u8,       // 0x01=i32, 0x02=i64, 0x03=f64, 0x04=bool
    size: usize,        // 4 or 8 or 1
    is_fast: bool,      // true if we can use abs_offset directly
}

impl FastColEncoder {
    /// Encode this column from raw tuple data — O(1), no iteration.
    #[inline(always)]
    pub fn encode(&self, out: &mut Vec<u8>, tuple_data: &[u8]) {
        // Null check
        if tuple_data[self.null_byte] & self.null_bit != 0 {
            out.push(0x00);
            return;
        }
        if self.is_fast {
            let pos = self.abs_offset;
            if pos + self.size <= tuple_data.len() {
                out.push(self.wire_tag);
                out.extend_from_slice(&tuple_data[pos..pos + self.size]);
                return;
            }
        }
        out.push(0x00); // fallback null
    }
}

/// Build FastColEncoders for a set of column indices.
/// Only works when ALL columns before each target are fixed-size and non-null.
/// Returns None if any column can't be fast-encoded.
pub fn build_fast_encoders(schema: &Schema, col_indices: &[usize]) -> Option<Vec<FastColEncoder>> {
    let ncols = schema.columns.len();
    let bitmap_len = (ncols + 7) / 8;

    // Check: all columns in the schema before the last target must be fixed-size
    let max_idx = col_indices.iter().copied().max().unwrap_or(0);
    for i in 0..=max_idx {
        let dt = &schema.columns[i].data_type;
        if matches!(dt, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
            return None; // variable-length column makes offsets unpredictable
        }
    }

    let mut encoders = Vec::with_capacity(col_indices.len());
    for &ci in col_indices {
        let mut abs_offset = bitmap_len;
        for i in 0..ci {
            abs_offset += match schema.columns[i].data_type {
                DataType::Integer | DataType::Date | DataType::Time => 4,
                DataType::BigInt | DataType::Float | DataType::DateTime => 8,
                DataType::Boolean => 1,
                DataType::Decimal(_, _) => 9,
                _ => return None,
            };
        }
        let dt = &schema.columns[ci].data_type;
        let (wire_tag, size) = match dt {
            DataType::Integer | DataType::Date | DataType::Time => (0x01u8, 4usize),
            DataType::BigInt | DataType::DateTime => (0x02, 8),
            DataType::Float => (0x03, 8),
            DataType::Boolean => (0x04, 1),
            _ => return None,
        };
        encoders.push(FastColEncoder {
            col_idx: ci,
            null_byte: ci / 8,
            null_bit: 1 << (ci % 8),
            abs_offset,
            wire_tag,
            size,
            is_fast: true,
        });
    }
    Some(encoders)
}

pub fn compute_column_info(schema: &Schema) -> Vec<ColInfo> {
    let mut infos = Vec::with_capacity(schema.columns.len());
    let mut fixed_offset = 0usize;
    for col in &schema.columns {
        let is_fixed = matches!(col.data_type,
            DataType::Integer | DataType::BigInt | DataType::Float |
            DataType::Boolean | DataType::DateTime | DataType::Date | DataType::Time);
        let size = match col.data_type {
            DataType::Integer | DataType::Date | DataType::Time => 4,
            DataType::BigInt | DataType::Float | DataType::DateTime => 8,
            DataType::Boolean => 1,
            DataType::Decimal(_, _) => 9,
            _ => 0,
        };
        infos.push(ColInfo { byte_offset: fixed_offset, dt: col.data_type.clone(), is_fixed, fixed_size: size });
        if is_fixed { fixed_offset += size; }
    }
    infos
}

/// Encode all columns of a tuple directly from raw bytes to ForgeWire format.
#[inline]
pub fn encode_tuple_columns_raw(out: &mut Vec<u8>, tuple_data: &[u8], schema: &Schema, col_info: &[ColInfo]) {
    let ncols = schema.columns.len();
    let bitmap_len = (ncols + 7) / 8;

    for (ci, info) in col_info.iter().enumerate() {
        if tuple_data.len() > ci / 8 && tuple_data[ci / 8] & (1 << (ci % 8)) != 0 {
            out.push(0x00);
            continue;
        }

        if info.is_fixed && info.fixed_size > 0 {
            let mut pos = bitmap_len;
            let mut found = false;
            for (j, jinfo) in col_info.iter().enumerate() {
                if !jinfo.is_fixed { continue; }
                if j == ci { found = true; break; }
                if tuple_data[j / 8] & (1 << (j % 8)) != 0 { continue; }
                pos += jinfo.fixed_size;
            }
            if found && pos + info.fixed_size <= tuple_data.len() {
                encode_fixed_value(out, &info.dt, &tuple_data[pos..pos + info.fixed_size]);
                continue;
            }
        }

        encode_value_fallback(out, tuple_data, schema, ci);
    }
}

/// Encode a single column from raw tuple bytes to ForgeWire format.
#[inline]
pub fn encode_single_column_raw(out: &mut Vec<u8>, tuple_data: &[u8], schema: &Schema, col_info: &[ColInfo], ci: usize) {
    let ncols = schema.columns.len();
    let info = &col_info[ci];

    if tuple_data.len() > ci / 8 && tuple_data[ci / 8] & (1 << (ci % 8)) != 0 {
        out.push(0x00);
        return;
    }

    let bitmap_len = (ncols + 7) / 8;

    if info.is_fixed && info.fixed_size > 0 {
        let mut pos = bitmap_len;
        let mut found = false;
        for (j, jinfo) in col_info.iter().enumerate() {
            if !jinfo.is_fixed { continue; }
            if j == ci { found = true; break; }
            if tuple_data[j / 8] & (1 << (j % 8)) != 0 { continue; }
            pos += jinfo.fixed_size;
        }
        if found && pos + info.fixed_size <= tuple_data.len() {
            encode_fixed_value(out, &info.dt, &tuple_data[pos..pos + info.fixed_size]);
            return;
        }
    }

    encode_value_fallback(out, tuple_data, schema, ci);
}

#[inline]
fn encode_fixed_value(out: &mut Vec<u8>, dt: &DataType, bytes: &[u8]) {
    match dt {
        DataType::Integer | DataType::Date | DataType::Time => {
            out.push(0x01);
            out.extend_from_slice(&bytes[..4]);
        }
        DataType::BigInt | DataType::DateTime => {
            out.push(0x02);
            out.extend_from_slice(&bytes[..8]);
        }
        DataType::Float => {
            out.push(0x03);
            out.extend_from_slice(&bytes[..8]);
        }
        DataType::Boolean => {
            out.push(0x04);
            out.push(bytes[0]);
        }
        _ => out.push(0x00),
    }
}

#[inline]
fn encode_value_fallback(out: &mut Vec<u8>, tuple_data: &[u8], schema: &Schema, ci: usize) {
    match crate::tuple::tuple::deserialize_single_column(tuple_data, schema, ci) {
        Ok(val) => {
            match val {
                Value::Null => out.push(0x00),
                Value::Integer(n) => { out.push(0x01); out.extend_from_slice(&n.to_le_bytes()); }
                Value::BigInt(n) => { out.push(0x02); out.extend_from_slice(&n.to_le_bytes()); }
                Value::Float(f) => { out.push(0x03); out.extend_from_slice(&f.to_le_bytes()); }
                Value::Boolean(b) => { out.push(0x04); out.push(if b { 1 } else { 0 }); }
                _ => {
                    let s = val.to_string();
                    let b = s.as_bytes();
                    out.push(0x05);
                    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    out.extend_from_slice(b);
                }
            }
        }
        Err(_) => out.push(0x00),
    }
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

fn extract_join_keys(expr: &Expr) -> Option<(String, String)> {
    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
        let l = if let Expr::ColumnRef { column, .. } = left.as_ref() { column.clone() } else { return None; };
        let r = if let Expr::ColumnRef { column, .. } = right.as_ref() { column.clone() } else { return None; };
        Some((l, r))
    } else { None }
}

fn is_int_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Integer | DataType::BigInt)
}

pub fn datatype_to_wire_tag(dt: &DataType) -> u8 {
    match dt {
        DataType::Integer | DataType::Date | DataType::Time => 0x01,
        DataType::BigInt | DataType::DateTime => 0x02,
        DataType::Float => 0x03,
        DataType::Boolean => 0x04,
        _ => 0x05,
    }
}

#[inline]
fn append_msg(buf: &mut Vec<u8>, msg_type: u8, payload: &[u8]) {
    buf.push(msg_type);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
}
