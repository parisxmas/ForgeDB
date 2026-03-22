//! Fully columnar hash join — operates on raw column arrays without Value objects.
//!
//! Reads integer columns directly from heap pages into flat Vec<i32>/Vec<i64>,
//! performs the hash join on arrays, and produces results as columnar arrays
//! that can be encoded to ForgeWire without any Value allocation.

use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::common::{PageId, RID, INVALID_PAGE_ID};
use crate::error::{ForgeError, Result};
use crate::executor::executor::ExecuteResult;
use crate::sql::ast::{BinaryOperator, Expr};
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::heap_page;
use crate::tuple::schema::Schema;
use crate::tuple::types::{DataType, Value};
use crate::txn::TxnContext;

/// Columnar representation of a table scan — each column is a typed flat array.
pub enum ColumnArray {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Str(Vec<String>),
}

impl ColumnArray {
    fn len(&self) -> usize {
        match self {
            Self::Int32(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Str(v) => v.len(),
        }
    }
}

/// Result of a columnar join — ready for direct wire encoding.
pub struct ColumnarResult {
    pub col_names: Vec<String>,
    pub col_types: Vec<u8>, // ForgeWire value tags
    pub columns: Vec<ColumnArray>,
    pub row_count: usize,
}

/// Try to execute an INNER JOIN entirely in columnar mode.
/// Returns None if the tables/columns don't support columnar processing.
pub fn try_columnar_inner_join(
    left_table: &str,
    left_alias: Option<&str>,
    right_table: &str,
    right_alias: Option<&str>,
    on_expr: &Expr,
    catalog: &Catalog,
    cbpm: &ConcurrentBufferPool,
    txn_ctx: Option<&TxnContext>,
) -> Option<ColumnarResult> {
    let left_info = catalog.get_table(left_table)?;
    let right_info = catalog.get_table(right_table)?;
    let left_schema = &left_info.schema;
    let right_schema = &right_info.schema;

    // Extract equi-join key column names
    let (left_key_col, right_key_col) = extract_join_key_names(on_expr)?;

    // Find key column indices
    let (left_key_idx, _) = left_schema.get_column(&left_key_col)?;
    let (right_key_idx, _) = right_schema.get_column(&right_key_col)?;

    // Both key columns must be integer
    let left_key_type = &left_schema.columns[left_key_idx].data_type;
    let right_key_type = &right_schema.columns[right_key_idx].data_type;
    if !matches!(left_key_type, DataType::Integer | DataType::BigInt) { return None; }
    if !matches!(right_key_type, DataType::Integer | DataType::BigInt) { return None; }

    // Scan both tables into columnar arrays
    let left_cols = scan_to_columns(left_table, left_info, cbpm)?;
    let right_cols = scan_to_columns(right_table, right_info, cbpm)?;
    if left_cols.is_empty() || right_cols.is_empty() { return None; }
    let num_left_rows = left_cols[0].len();
    let num_right_rows = right_cols[0].len();

    // Extract join key arrays as i64
    let left_keys = column_to_i64(&left_cols[left_key_idx])?;
    let right_keys = column_to_i64(&right_cols[right_key_idx])?;

    // Build hash table on the smaller side
    let (build_keys, probe_keys, build_is_left) = if num_right_rows <= num_left_rows {
        (&right_keys, &left_keys, false)
    } else {
        (&left_keys, &right_keys, true)
    };

    let mut hash_map: HashMap<i64, Vec<u32>> = HashMap::with_capacity(build_keys.len());
    for (i, &key) in build_keys.iter().enumerate() {
        hash_map.entry(key).or_default().push(i as u32);
    }

    // Probe and collect matched index pairs: (left_idx, right_idx)
    let mut left_indices: Vec<u32> = Vec::with_capacity(std::cmp::min(num_left_rows, num_right_rows));
    let mut right_indices: Vec<u32> = Vec::with_capacity(left_indices.capacity());

    for (probe_idx, &key) in probe_keys.iter().enumerate() {
        if let Some(build_idxs) = hash_map.get(&key) {
            for &build_idx in build_idxs {
                if build_is_left {
                    left_indices.push(build_idx);
                    right_indices.push(probe_idx as u32);
                } else {
                    left_indices.push(probe_idx as u32);
                    right_indices.push(build_idx);
                }
            }
        }
    }

    let result_count = left_indices.len();

    // Gather output columns by index — no Value objects
    let left_prefix = left_alias.unwrap_or(left_table);
    let right_prefix = right_alias.unwrap_or(right_table);

    let mut col_names = Vec::new();
    let mut col_types = Vec::new();
    let mut columns = Vec::new();

    // Left columns
    for (ci, col) in left_schema.columns.iter().enumerate() {
        col_names.push(format!("{}.{}", left_prefix, col.name));
        let (gathered, type_tag) = gather_column(&left_cols[ci], &left_indices, result_count);
        col_types.push(type_tag);
        columns.push(gathered);
    }
    // Right columns
    for (ci, col) in right_schema.columns.iter().enumerate() {
        col_names.push(format!("{}.{}", right_prefix, col.name));
        let (gathered, type_tag) = gather_column(&right_cols[ci], &right_indices, result_count);
        col_types.push(type_tag);
        columns.push(gathered);
    }

    Some(ColumnarResult { col_names, col_types, columns, row_count: result_count })
}

/// Convert a ColumnarResult to ExecuteResult (for non-ForgeWire paths).
pub fn columnar_to_execute_result(cr: ColumnarResult) -> ExecuteResult {
    let num_cols = cr.columns.len();
    let mut col_names: Vec<String> = cr.col_names.into_iter().map(|n| {
        if let Some(dot) = n.find('.') { n[dot+1..].to_string() } else { n }
    }).collect();
    let mut rows = Vec::with_capacity(cr.row_count);
    for r in 0..cr.row_count {
        let mut row = Vec::with_capacity(num_cols);
        for c in 0..num_cols {
            row.push(column_get_value(&cr.columns[c], r));
        }
        rows.push(row);
    }
    ExecuteResult { rows, columns: col_names, rows_affected: 0, last_insert_id: 0, message: String::new() }
}

/// Encode a ColumnarResult directly to ForgeWire binary — zero Value objects.
pub fn columnar_to_forgewire(cr: &ColumnarResult) -> Vec<u8> {
    let estimated = 128 + cr.row_count * cr.columns.len() * 6;
    let mut out = Vec::with_capacity(estimated);

    // ROW_HEADER
    let mut hdr = Vec::with_capacity(64);
    hdr.extend_from_slice(&(cr.col_names.len() as u16).to_le_bytes());
    for (i, name) in cr.col_names.iter().enumerate() {
        // Strip table prefix for output
        let display_name = if let Some(dot) = name.find('.') { &name[dot+1..] } else { name.as_str() };
        let nb = display_name.as_bytes();
        hdr.extend_from_slice(&(nb.len() as u16).to_le_bytes());
        hdr.extend_from_slice(nb);
        hdr.push(cr.col_types[i]);
    }
    append_msg(&mut out, 0x10, &hdr); // MSG_ROW_HEADER

    // ROW messages — encode directly from columnar arrays
    for r in 0..cr.row_count {
        let row_start = out.len();
        out.extend_from_slice(&[0x11, 0, 0, 0, 0]); // MSG_ROW + placeholder length
        for c in 0..cr.columns.len() {
            encode_column_value(&mut out, &cr.columns[c], r);
        }
        let payload_len = (out.len() - row_start - 5) as u32;
        out[row_start + 1..row_start + 5].copy_from_slice(&payload_len.to_le_bytes());
    }

    // DONE
    append_msg(&mut out, 0x12, &(cr.row_count as u64).to_le_bytes());

    out
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn extract_join_key_names(expr: &Expr) -> Option<(String, String)> {
    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
        let left_col = extract_col_name(left)?;
        let right_col = extract_col_name(right)?;
        return Some((left_col, right_col));
    }
    None
}

fn extract_col_name(expr: &Expr) -> Option<String> {
    if let Expr::ColumnRef { column, .. } = expr {
        Some(column.clone())
    } else {
        None
    }
}

/// Scan a table into columnar arrays — one Vec per column.
fn scan_to_columns(
    table_name: &str,
    info: &crate::catalog::TableInfo,
    cbpm: &ConcurrentBufferPool,
) -> Option<Vec<ColumnArray>> {
    let schema = &info.schema;
    let ncols = schema.columns.len();
    let mvcc = info.mvcc_enabled;

    // Initialize column builders
    let mut builders: Vec<ColumnBuilder> = schema.columns.iter().map(|c| {
        match c.data_type {
            DataType::Integer => ColumnBuilder::Int32(Vec::with_capacity(1024)),
            DataType::BigInt | DataType::DateTime => ColumnBuilder::Int64(Vec::with_capacity(1024)),
            DataType::Float => ColumnBuilder::Float64(Vec::with_capacity(1024)),
            _ => ColumnBuilder::Str(Vec::with_capacity(1024)),
        }
    }).collect();

    let mut current_pid = info.first_page_id;
    while current_pid.0 != INVALID_PAGE_ID {
        cbpm.fetch_page(current_pid).ok()?;
        let guard = cbpm.read_page(current_pid).ok()?;
        let page = guard.data();
        let num_slots = heap_page::get_num_slots(page);

        for slot in 0..num_slots {
            if let Some((off, len)) = heap_page::get_tuple_slice(page, slot) {
                let raw = &page[off..off+len];
                let tuple_data = if mvcc && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                    let (_, xmax) = crate::txn::mvcc::decode_version_header(raw);
                    if xmax != crate::txn::mvcc::XMAX_NONE { continue; }
                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                } else { raw };

                // Deserialize into columnar builders
                if let Ok(vals) = crate::tuple::tuple::deserialize(tuple_data, schema) {
                    for (ci, val) in vals.into_iter().enumerate() {
                        if ci < ncols {
                            builders[ci].push(val);
                        }
                    }
                }
            }
        }

        current_pid = PageId(heap_page::get_next_page_id(guard.data()));
        drop(guard);
        cbpm.unpin_page(current_pid, false).ok();
    }

    Some(builders.into_iter().map(|b| b.finish()).collect())
}

fn column_to_i64(col: &ColumnArray) -> Option<Vec<i64>> {
    match col {
        ColumnArray::Int32(v) => Some(v.iter().map(|&x| x as i64).collect()),
        ColumnArray::Int64(v) => Some(v.clone()),
        _ => None,
    }
}

/// Gather values from a column by index array — the SIMD-friendly operation.
fn gather_column(col: &ColumnArray, indices: &[u32], count: usize) -> (ColumnArray, u8) {
    match col {
        ColumnArray::Int32(v) => {
            let mut out = Vec::with_capacity(count);
            for &idx in indices {
                out.push(v[idx as usize]);
            }
            (ColumnArray::Int32(out), 0x01) // VAL_INT32
        }
        ColumnArray::Int64(v) => {
            let mut out = Vec::with_capacity(count);
            for &idx in indices {
                out.push(v[idx as usize]);
            }
            (ColumnArray::Int64(out), 0x02) // VAL_INT64
        }
        ColumnArray::Float64(v) => {
            let mut out = Vec::with_capacity(count);
            for &idx in indices {
                out.push(v[idx as usize]);
            }
            (ColumnArray::Float64(out), 0x03) // VAL_FLOAT64
        }
        ColumnArray::Str(v) => {
            let mut out = Vec::with_capacity(count);
            for &idx in indices {
                out.push(v[idx as usize].clone());
            }
            (ColumnArray::Str(out), 0x05) // VAL_STRING
        }
    }
}

fn column_get_value(col: &ColumnArray, idx: usize) -> Value {
    match col {
        ColumnArray::Int32(v) => Value::Integer(v[idx]),
        ColumnArray::Int64(v) => Value::BigInt(v[idx]),
        ColumnArray::Float64(v) => Value::Float(v[idx]),
        ColumnArray::Str(v) => Value::Varchar(v[idx].clone()),
    }
}

#[inline]
fn encode_column_value(buf: &mut Vec<u8>, col: &ColumnArray, idx: usize) {
    match col {
        ColumnArray::Int32(v) => { buf.push(0x01); buf.extend_from_slice(&v[idx].to_le_bytes()); }
        ColumnArray::Int64(v) => { buf.push(0x02); buf.extend_from_slice(&v[idx].to_le_bytes()); }
        ColumnArray::Float64(v) => { buf.push(0x03); buf.extend_from_slice(&v[idx].to_le_bytes()); }
        ColumnArray::Str(v) => {
            let b = v[idx].as_bytes();
            buf.push(0x05);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

#[inline]
fn append_msg(buf: &mut Vec<u8>, msg_type: u8, payload: &[u8]) {
    buf.push(msg_type);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
}

enum ColumnBuilder {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Str(Vec<String>),
}

impl ColumnBuilder {
    fn push(&mut self, val: Value) {
        match (self, val) {
            (ColumnBuilder::Int32(v), Value::Integer(n)) => v.push(n),
            (ColumnBuilder::Int32(v), Value::BigInt(n)) => v.push(n as i32),
            (ColumnBuilder::Int64(v), Value::BigInt(n)) => v.push(n),
            (ColumnBuilder::Int64(v), Value::Integer(n)) => v.push(n as i64),
            (ColumnBuilder::Int64(v), Value::DateTime(n)) => v.push(n),
            (ColumnBuilder::Float64(v), Value::Float(f)) => v.push(f),
            (ColumnBuilder::Float64(v), Value::Integer(n)) => v.push(n as f64),
            (ColumnBuilder::Str(v), Value::Varchar(s)) => v.push(s),
            (ColumnBuilder::Str(v), val) => v.push(val.to_string()),
            (ColumnBuilder::Int32(v), _) => v.push(0),
            (ColumnBuilder::Int64(v), _) => v.push(0),
            (ColumnBuilder::Float64(v), _) => v.push(0.0),
        }
    }

    fn finish(self) -> ColumnArray {
        match self {
            ColumnBuilder::Int32(v) => ColumnArray::Int32(v),
            ColumnBuilder::Int64(v) => ColumnArray::Int64(v),
            ColumnBuilder::Float64(v) => ColumnArray::Float64(v),
            ColumnBuilder::Str(v) => ColumnArray::Str(v),
        }
    }
}
