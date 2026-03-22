//! DuckDB-style vectorized execution engine for ForgeDB.
//!
//! Instead of processing one tuple at a time (volcano) or all tuples at once
//! (materialization), this engine processes BATCHES of tuples (up to 1024 at a
//! time). Each operator call processes a full batch, enabling:
//! - Fewer virtual dispatch calls (1 per 1024 tuples vs 1 per tuple)
//! - Cache-friendly columnar data layout
//! - Potential SIMD operations on numeric arrays
//!
//! Supported operators:
//! - [`VecSeqScan`]: reads heap pages from ConcurrentBufferPool, builds columnar chunks
//! - [`VecFilter`]: batch predicate evaluation with selection vector compaction
//! - [`VecProjection`]: evaluates expressions on batch columns
//! - [`VecAggregate`]: accumulates COUNT/SUM/AVG/MIN/MAX across chunks (no GROUP BY)
//! - [`VecGroupBy`]: hash aggregation with chunks
//! - [`VecLimit`]: count tuples across chunks, stop when limit reached
//! - [`VecSort`]: materialize all chunks, sort, emit in chunks
//!
//! The vectorized path is tried AFTER COUNT(*) fast path but BEFORE volcano
//! iterator. Falls back to volcano/materialization for JOINs, IndexScan,
//! clustered tables.

use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::common::{PageId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{ForgeError, Result};
use crate::planner::plan::PlanNode;
use crate::sql::ast::{Expr, OrderByItem, SelectColumn};
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::heap_page;
use crate::index::ClusteredIndex;
use crate::tuple::schema::{Column, Schema};
use crate::tuple::tuple::deserialize;
use crate::tuple::types::{DataType, Value};
use crate::txn::TxnContext;
use crate::txn::mvcc::{MVCC_HEADER_SIZE, XMAX_NONE, decode_version_header, is_visible};

use super::eval::{eval_to_bool, evaluate};
use super::aggregate;
use super::executor::ExecuteResult;

// =========================================================================
// Constants
// =========================================================================

/// Number of tuples processed per batch. Chosen to fit in L1/L2 cache for
/// common column widths while amortizing per-batch overhead.
pub const VECTOR_SIZE: usize = 1024;

// =========================================================================
// DataChunk — the batch unit
// =========================================================================

/// A batch of tuples stored in columnar format.
/// Each column is a separate vector, enabling cache-friendly access patterns
/// and potential SIMD operations on numeric arrays.
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// One ColumnVector per column in the schema.
    pub columns: Vec<ColumnVector>,
    /// Number of valid rows in this chunk (may be < VECTOR_SIZE).
    pub len: usize,
}

impl DataChunk {
    /// Create a new empty DataChunk with the given number of columns.
    pub fn new(num_columns: usize) -> Self {
        Self {
            columns: (0..num_columns).map(|_| ColumnVector::new()).collect(),
            len: 0,
        }
    }

    /// Create a DataChunk pre-allocated for the given schema.
    pub fn with_schema(schema: &Schema) -> Self {
        let columns = schema.columns.iter().map(|col| {
            ColumnVector::with_type(&col.data_type)
        }).collect();
        Self { columns, len: 0 }
    }

    /// Push a row (as Vec<Value>) into this chunk, distributing values across columns.
    pub fn push_row(&mut self, values: &[Value]) {
        for (i, val) in values.iter().enumerate() {
            if i < self.columns.len() {
                self.columns[i].push(val);
            }
        }
        self.len += 1;
    }

    /// Get a row at the given index, reconstructed from columnar data.
    pub fn get_row(&self, row_idx: usize) -> Vec<Value> {
        self.columns.iter().map(|col| col.get(row_idx)).collect()
    }

    /// Check if this chunk is full (has VECTOR_SIZE rows).
    pub fn is_full(&self) -> bool {
        self.len >= VECTOR_SIZE
    }

    /// Filter this chunk in-place using a selection vector.
    /// Only rows where `selection[i]` is true are kept.
    pub fn compact(&mut self, selection: &[bool]) {
        for col in &mut self.columns {
            col.compact(selection);
        }
        self.len = selection.iter().filter(|&&s| s).count();
    }

    /// Truncate this chunk to at most `max_rows` rows.
    pub fn truncate(&mut self, max_rows: usize) {
        if self.len > max_rows {
            for col in &mut self.columns {
                col.truncate(max_rows);
            }
            self.len = max_rows;
        }
    }
}

// =========================================================================
// ColumnVector — a typed array of values for one column
// =========================================================================

/// A single column's data within a DataChunk, stored as a typed array.
/// Uses `ColumnData::Values` (variant storage) when the column type is
/// unknown at construction time, and native typed arrays when the schema
/// specifies a concrete type for cache-friendly batch processing.
#[derive(Debug, Clone)]
pub struct ColumnVector {
    pub data: ColumnData,
    pub nulls: Vec<bool>, // true = null
}

impl ColumnVector {
    /// Create a new empty column vector with variant (type-preserving) storage.
    /// Used when the output type is unknown (e.g., projection expressions).
    pub fn new() -> Self {
        Self {
            data: ColumnData::Values(Vec::new()),
            nulls: Vec::new(),
        }
    }

    /// Create a column vector pre-allocated for the given data type.
    /// Uses variant storage for types that need exact type preservation
    /// (DateTime, Date, Time, Json, Uuid, Binary), and native typed arrays
    /// for common numeric/string types for cache-friendly batch processing.
    pub fn with_type(dt: &DataType) -> Self {
        let data = match dt {
            DataType::Integer => ColumnData::Int32(Vec::new()),
            DataType::BigInt => ColumnData::Int64(Vec::new()),
            DataType::Float => ColumnData::Float64(Vec::new()),
            DataType::Boolean => ColumnData::Bool(Vec::new()),
            DataType::Decimal(_, _) => ColumnData::Decimal(Vec::new()),
            DataType::Varchar(_) => ColumnData::Str(Vec::new()),
            // DateTime, Date, Time, Json, Uuid, Binary need exact type preservation
            // so we use the Values variant to avoid lossy Int32/Int64 conversions.
            _ => ColumnData::Values(Vec::new()),
        };
        Self { data, nulls: Vec::new() }
    }

    /// Push a Value into this column vector.
    pub fn push(&mut self, val: &Value) {
        match val {
            Value::Null => {
                self.nulls.push(true);
                self.data.push_default();
            }
            Value::Integer(n) => {
                self.nulls.push(false);
                self.data.push_i32(*n);
            }
            Value::BigInt(n) => {
                self.nulls.push(false);
                self.data.push_i64(*n);
            }
            Value::Float(f) => {
                self.nulls.push(false);
                self.data.push_f64(*f);
            }
            Value::Boolean(b) => {
                self.nulls.push(false);
                self.data.push_bool(*b);
            }
            Value::DateTime(epoch) => {
                self.nulls.push(false);
                self.data.push_value(Value::DateTime(*epoch));
            }
            Value::Date(d) => {
                self.nulls.push(false);
                self.data.push_value(Value::Date(*d));
            }
            Value::Time(t) => {
                self.nulls.push(false);
                self.data.push_value(Value::Time(*t));
            }
            Value::Decimal(v, s) => {
                self.nulls.push(false);
                self.data.push_decimal(*v, *s);
            }
            Value::Varchar(s) => {
                self.nulls.push(false);
                self.data.push_str(s.clone());
            }
            Value::Binary(b) => {
                self.nulls.push(false);
                self.data.push_value(Value::Binary(b.clone()));
            }
            Value::Json(s) => {
                self.nulls.push(false);
                self.data.push_value(Value::Json(s.clone()));
            }
            Value::Uuid(s) => {
                self.nulls.push(false);
                self.data.push_value(Value::Uuid(s.clone()));
            }
        }
    }

    /// Get the Value at the given index.
    pub fn get(&self, idx: usize) -> Value {
        if idx < self.nulls.len() && self.nulls[idx] {
            return Value::Null;
        }
        self.data.get_value(idx)
    }

    /// Filter this column in-place using a selection vector.
    fn compact(&mut self, selection: &[bool]) {
        let mut write = 0;
        for read in 0..self.nulls.len().min(selection.len()) {
            if selection[read] {
                if write != read {
                    self.nulls[write] = self.nulls[read];
                    self.data.swap_move(write, read);
                }
                write += 1;
            }
        }
        self.nulls.truncate(write);
        self.data.truncate(write);
    }

    /// Truncate to at most `len` elements.
    fn truncate(&mut self, len: usize) {
        self.nulls.truncate(len);
        self.data.truncate(len);
    }
}

// =========================================================================
// ColumnData — typed storage
// =========================================================================

/// The underlying typed storage for a column vector.
///
/// Native typed arrays (`Int32`, `Int64`, etc.) are used when the column type
/// is known from the schema, enabling cache-friendly batch processing.
///
/// The `Values` variant stores `Value` enum directly, preserving types when
/// the output type is unknown (e.g., projection expressions, aggregates).
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Bool(Vec<bool>),
    Str(Vec<String>),
    /// Decimal: (unscaled_value, scale) pairs
    Decimal(Vec<(i64, u8)>),
    /// Variant storage: preserves original Value types exactly.
    /// Used when column type is unknown at construction time.
    Values(Vec<Value>),
}

impl ColumnData {
    /// Push a default/zero value (used for null slots).
    fn push_default(&mut self) {
        match self {
            ColumnData::Int32(v) => v.push(0),
            ColumnData::Int64(v) => v.push(0),
            ColumnData::Float64(v) => v.push(0.0),
            ColumnData::Bool(v) => v.push(false),
            ColumnData::Str(v) => v.push(String::new()),
            ColumnData::Decimal(v) => v.push((0, 0)),
            ColumnData::Values(v) => v.push(Value::Null),
        }
    }

    fn push_i32(&mut self, val: i32) {
        match self {
            ColumnData::Int32(v) => v.push(val),
            ColumnData::Int64(v) => v.push(val as i64),
            ColumnData::Float64(v) => v.push(val as f64),
            ColumnData::Str(v) => v.push(val.to_string()),
            ColumnData::Decimal(v) => v.push((val as i64, 0)),
            ColumnData::Values(v) => v.push(Value::Integer(val)),
            _ => { /* type mismatch: store as default */ }
        }
    }

    fn push_i64(&mut self, val: i64) {
        match self {
            ColumnData::Int64(v) => v.push(val),
            ColumnData::Int32(v) => v.push(val as i32),
            ColumnData::Float64(v) => v.push(val as f64),
            ColumnData::Str(v) => v.push(val.to_string()),
            ColumnData::Decimal(v) => v.push((val, 0)),
            ColumnData::Values(v) => v.push(Value::BigInt(val)),
            _ => { /* type mismatch */ }
        }
    }

    fn push_f64(&mut self, val: f64) {
        match self {
            ColumnData::Float64(v) => v.push(val),
            ColumnData::Int32(v) => v.push(val as i32),
            ColumnData::Int64(v) => v.push(val as i64),
            ColumnData::Str(v) => v.push(val.to_string()),
            ColumnData::Values(v) => v.push(Value::Float(val)),
            _ => { /* type mismatch */ }
        }
    }

    fn push_bool(&mut self, val: bool) {
        match self {
            ColumnData::Bool(v) => v.push(val),
            ColumnData::Int32(v) => v.push(if val { 1 } else { 0 }),
            ColumnData::Str(v) => v.push(val.to_string()),
            ColumnData::Values(v) => v.push(Value::Boolean(val)),
            _ => { /* type mismatch */ }
        }
    }

    fn push_str(&mut self, val: String) {
        match self {
            ColumnData::Str(v) => v.push(val),
            ColumnData::Int32(v) => v.push(val.parse().unwrap_or(0)),
            ColumnData::Int64(v) => v.push(val.parse().unwrap_or(0)),
            ColumnData::Float64(v) => v.push(val.parse().unwrap_or(0.0)),
            ColumnData::Values(v) => v.push(Value::Varchar(val)),
            _ => { /* type mismatch */ }
        }
    }

    fn push_decimal(&mut self, val: i64, scale: u8) {
        match self {
            ColumnData::Decimal(v) => v.push((val, scale)),
            ColumnData::Int64(v) => v.push(val),
            ColumnData::Float64(v) => v.push(val as f64 / 10f64.powi(scale as i32)),
            ColumnData::Str(v) => {
                if scale == 0 {
                    v.push(val.to_string());
                } else {
                    let divisor = 10i64.pow(scale as u32);
                    v.push(format!("{}.{:0>width$}", val / divisor, (val % divisor).unsigned_abs(), width = scale as usize));
                }
            }
            ColumnData::Values(v) => v.push(Value::Decimal(val, scale)),
            _ => { /* type mismatch */ }
        }
    }

    /// Push a Value directly (used for Values variant or uncommon types).
    fn push_value(&mut self, val: Value) {
        match self {
            ColumnData::Values(v) => v.push(val),
            // For typed columns, dispatch to the appropriate push method
            _ => match &val {
                Value::Integer(n) => self.push_i32(*n),
                Value::BigInt(n) => self.push_i64(*n),
                Value::Float(f) => self.push_f64(*f),
                Value::Boolean(b) => self.push_bool(*b),
                Value::Varchar(s) => self.push_str(s.clone()),
                Value::DateTime(e) => self.push_i64(*e),
                Value::Date(d) => self.push_i32(*d),
                Value::Time(t) => self.push_i32(*t),
                Value::Decimal(v, s) => self.push_decimal(*v, *s),
                Value::Null => self.push_default(),
                _ => self.push_str(format!("{}", val)),
            }
        }
    }

    /// Retrieve a Value at the given index.
    fn get_value(&self, idx: usize) -> Value {
        match self {
            ColumnData::Int32(v) => {
                if idx < v.len() { Value::Integer(v[idx]) } else { Value::Null }
            }
            ColumnData::Int64(v) => {
                if idx < v.len() { Value::BigInt(v[idx]) } else { Value::Null }
            }
            ColumnData::Float64(v) => {
                if idx < v.len() { Value::Float(v[idx]) } else { Value::Null }
            }
            ColumnData::Bool(v) => {
                if idx < v.len() { Value::Boolean(v[idx]) } else { Value::Null }
            }
            ColumnData::Str(v) => {
                if idx < v.len() { Value::Varchar(v[idx].clone()) } else { Value::Null }
            }
            ColumnData::Decimal(v) => {
                if idx < v.len() { Value::Decimal(v[idx].0, v[idx].1) } else { Value::Null }
            }
            ColumnData::Values(v) => {
                if idx < v.len() { v[idx].clone() } else { Value::Null }
            }
        }
    }

    /// Move element at `from` to `to` (for in-place compaction).
    fn swap_move(&mut self, to: usize, from: usize) {
        match self {
            ColumnData::Int32(v) => v[to] = v[from],
            ColumnData::Int64(v) => v[to] = v[from],
            ColumnData::Float64(v) => v[to] = v[from],
            ColumnData::Bool(v) => v[to] = v[from],
            ColumnData::Str(v) => {
                let val = std::mem::take(&mut v[from]);
                v[to] = val;
            }
            ColumnData::Decimal(v) => v[to] = v[from],
            ColumnData::Values(v) => {
                let val = std::mem::replace(&mut v[from], Value::Null);
                v[to] = val;
            }
        }
    }

    /// Truncate to the given length.
    fn truncate(&mut self, len: usize) {
        match self {
            ColumnData::Int32(v) => v.truncate(len),
            ColumnData::Int64(v) => v.truncate(len),
            ColumnData::Float64(v) => v.truncate(len),
            ColumnData::Bool(v) => v.truncate(len),
            ColumnData::Str(v) => v.truncate(len),
            ColumnData::Decimal(v) => v.truncate(len),
            ColumnData::Values(v) => v.truncate(len),
        }
    }
}

// =========================================================================
// VectorizedOperator trait
// =========================================================================

/// Vectorized execution operator that processes DataChunks (batches of tuples).
pub trait VectorizedOperator {
    /// Return the output schema of this operator.
    fn schema(&self) -> &Schema;

    /// Pull the next chunk of tuples. Returns `None` when exhausted.
    fn next_chunk(&mut self) -> Result<Option<DataChunk>>;
}

// =========================================================================
// VecSeqScan — reads pages from ConcurrentBufferPool into columnar chunks
// =========================================================================

/// Sequential scan that reads heap pages directly from the ConcurrentBufferPool
/// and deserializes tuples into columnar DataChunks of up to VECTOR_SIZE rows.
pub struct VecSeqScan<'a> {
    schema: Schema,
    raw_schema: Schema,
    cbpm: &'a ConcurrentBufferPool,
    mvcc_enabled: bool,
    txn_ctx: Option<&'a TxnContext>,
    current_page_id: PageId,
    /// Buffer for tuples loaded from the current page.
    page_tuples: Vec<Vec<Value>>,
    page_tuple_idx: usize,
    exhausted: bool,
}

impl<'a> VecSeqScan<'a> {
    pub fn new(
        table_name: &str,
        alias: Option<&str>,
        catalog: &'a Catalog,
        cbpm: &'a ConcurrentBufferPool,
        txn_ctx: Option<&'a TxnContext>,
    ) -> Result<Self> {
        let info = catalog
            .get_table(table_name)
            .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

        let raw_schema = info.schema.clone();
        let mvcc_enabled = info.mvcc_enabled;
        let first_page_id = info.first_page_id;

        let prefix = alias.unwrap_or(table_name);
        let prefixed = Schema::new(
            raw_schema.columns.iter().enumerate().map(|(i, c)| Column {
                name: format!("{}.{}", prefix, c.name),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                column_id: i as u16,
                auto_increment: c.auto_increment,
                default_value: c.default_value.clone(),
                is_primary_key: c.is_primary_key,
                is_unique: false,
                check_expr: None,
                fk_ref: None,
            }).collect(),
        );

        Ok(Self {
            schema: prefixed,
            raw_schema,
            cbpm,
            mvcc_enabled,
            txn_ctx,
            current_page_id: first_page_id,
            page_tuples: Vec::new(),
            page_tuple_idx: 0,
            exhausted: false,
        })
    }

    /// Load all tuples from the current page into the page buffer.
    fn load_next_page(&mut self) -> Result<bool> {
        loop {
            if self.current_page_id.0 == INVALID_PAGE_ID {
                return Ok(false);
            }

            self.cbpm.fetch_page(self.current_page_id)?;
            let guard = self.cbpm.read_page(self.current_page_id)?;
            let data: &[u8; PAGE_SIZE] = guard.data();

            self.page_tuples.clear();
            self.page_tuple_idx = 0;

            let num_slots = heap_page::get_num_slots(data);
            for slot_id in 0..num_slots {
                if let Some((off, len)) = heap_page::get_tuple_slice(data, slot_id) {
                    let raw = &data[off..off + len];

                    if self.mvcc_enabled {
                        if raw.len() < MVCC_HEADER_SIZE {
                            continue;
                        }
                        let (xmin, xmax) = decode_version_header(raw);
                        let tuple_data = &raw[MVCC_HEADER_SIZE..];

                        if let Some(ctx) = self.txn_ctx {
                            if !is_visible(xmin, xmax, &ctx.snapshot) {
                                continue;
                            }
                        } else {
                            if xmax != XMAX_NONE {
                                continue;
                            }
                        }

                        let values = deserialize(tuple_data, &self.raw_schema)?;
                        self.page_tuples.push(values);
                    } else {
                        let values = deserialize(raw, &self.raw_schema)?;
                        self.page_tuples.push(values);
                    }
                }
            }

            let next = heap_page::get_next_page_id(data);
            drop(guard);
            self.cbpm.unpin_page(self.current_page_id, false)?;
            self.current_page_id = PageId(next);

            if !self.page_tuples.is_empty() {
                return Ok(true);
            }
        }
    }
}

impl<'a> VectorizedOperator for VecSeqScan<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.exhausted {
            return Ok(None);
        }

        let mut chunk = DataChunk::with_schema(&self.schema);

        while chunk.len < VECTOR_SIZE {
            // Try to get tuples from the current page buffer
            if self.page_tuple_idx < self.page_tuples.len() {
                let values = &self.page_tuples[self.page_tuple_idx];
                self.page_tuple_idx += 1;
                chunk.push_row(values);
                continue;
            }

            // Need to load next page
            if !self.load_next_page()? {
                self.exhausted = true;
                break;
            }
        }

        if chunk.len == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

// =========================================================================
// VecDual — produces a single empty-row chunk for SELECT without FROM
// =========================================================================

pub struct VecDual {
    schema: Schema,
    emitted: bool,
}

impl VecDual {
    pub fn new() -> Self {
        Self {
            schema: Schema::new(vec![]),
            emitted: false,
        }
    }
}

impl VectorizedOperator for VecDual {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(DataChunk { columns: vec![], len: 1 }))
    }
}

// =========================================================================
// VecFilter — batch predicate evaluation
// =========================================================================

/// Filter operator that evaluates a predicate on each row in a batch.
/// Rows that don't match are removed via selection vector compaction.
pub struct VecFilter<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    predicate: Expr,
}

impl<'a> VecFilter<'a> {
    pub fn new(child: Box<dyn VectorizedOperator + 'a>, predicate: Expr) -> Self {
        Self { child, predicate }
    }
}

impl<'a> VectorizedOperator for VecFilter<'a> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        loop {
            match self.child.next_chunk()? {
                None => return Ok(None),
                Some(mut chunk) => {
                    let schema = self.child.schema();
                    // Build selection vector by evaluating predicate per row
                    let mut selection = Vec::with_capacity(chunk.len);
                    let mut any_selected = false;
                    for row_idx in 0..chunk.len {
                        let row = chunk.get_row(row_idx);
                        let passes = eval_to_bool(&self.predicate, &row, schema)?;
                        selection.push(passes);
                        if passes {
                            any_selected = true;
                        }
                    }

                    if !any_selected {
                        // No rows passed, try next chunk
                        continue;
                    }

                    // Compact chunk using selection vector
                    chunk.compact(&selection);
                    return Ok(Some(chunk));
                }
            }
        }
    }
}

// =========================================================================
// VecProjection — evaluates projection expressions on batch columns
// =========================================================================

/// Projection operator that evaluates expressions on each row in a batch,
/// producing a new DataChunk with the projected columns.
pub struct VecProjection<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    output_schema: Schema,
    /// Evaluation plan: None = AllColumns passthrough, Some = evaluate expression.
    eval_plan: Vec<Option<Expr>>,
}

impl<'a> VecProjection<'a> {
    pub fn new(
        child: Box<dyn VectorizedOperator + 'a>,
        columns: Vec<SelectColumn>,
    ) -> Self {
        let child_schema = child.schema();
        let mut col_names = Vec::new();
        let mut eval_plan: Vec<Option<Expr>> = Vec::new();

        for col in &columns {
            match col {
                SelectColumn::AllColumns(table_filter) => {
                    for c in &child_schema.columns {
                        if let Some(tbl) = table_filter {
                            let prefix = format!("{}.", tbl.to_lowercase());
                            if !c.name.to_lowercase().starts_with(&prefix) {
                                continue;
                            }
                        }
                        let name = if let Some(pos) = c.name.find('.') {
                            c.name[pos + 1..].to_string()
                        } else {
                            c.name.clone()
                        };
                        col_names.push(name);
                        eval_plan.push(None);
                    }
                }
                SelectColumn::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| expr_to_name(expr));
                    col_names.push(name);
                    eval_plan.push(Some(expr.clone()));
                }
            }
        }

        let output_schema = Schema::new(
            col_names.iter().enumerate().map(|(i, name)| Column {
                name: name.clone(),
                data_type: DataType::Varchar(255),
                nullable: true,
                column_id: i as u16,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                is_unique: false,
                check_expr: None,
                fk_ref: None,
            }).collect(),
        );

        Self {
            child,
            output_schema,
            eval_plan,
        }
    }
}

impl<'a> VectorizedOperator for VecProjection<'a> {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        match self.child.next_chunk()? {
            None => Ok(None),
            Some(chunk) => {
                let child_schema = self.child.schema();
                let mut result = DataChunk::new(self.eval_plan.len());

                // Process batch: for each row, evaluate all projection expressions
                for row_idx in 0..chunk.len {
                    let row = chunk.get_row(row_idx);
                    let mut projected = Vec::with_capacity(self.eval_plan.len());
                    let mut all_col_idx = 0usize;

                    for eval in &self.eval_plan {
                        match eval {
                            None => {
                                if all_col_idx < row.len() {
                                    projected.push(row[all_col_idx].clone());
                                }
                                all_col_idx += 1;
                            }
                            Some(expr) => {
                                projected.push(evaluate(expr, &row, child_schema)?);
                            }
                        }
                    }
                    result.push_row(&projected);
                }

                Ok(Some(result))
            }
        }
    }
}

// =========================================================================
// VecAggregate — accumulates aggregates across chunks (no GROUP BY)
// =========================================================================

/// Aggregate operator that consumes all chunks from child, accumulates
/// COUNT/SUM/AVG/MIN/MAX, and returns a single-row chunk.
pub struct VecAggregate<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    columns: Vec<SelectColumn>,
    output_schema: Schema,
    done: bool,
}

impl<'a> VecAggregate<'a> {
    pub fn new(
        child: Box<dyn VectorizedOperator + 'a>,
        columns: Vec<SelectColumn>,
    ) -> Self {
        let col_names: Vec<String> = columns.iter().map(|c| match c {
            SelectColumn::Expr { expr, alias } => {
                alias.clone().unwrap_or_else(|| agg_expr_name(expr))
            }
            SelectColumn::AllColumns(_) => "*".to_string(),
        }).collect();

        let output_schema = Schema::new(
            col_names.iter().enumerate().map(|(i, name)| Column {
                name: name.clone(),
                data_type: DataType::Varchar(255),
                nullable: true,
                column_id: i as u16,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                is_unique: false,
                check_expr: None,
                fk_ref: None,
            }).collect(),
        );

        Self { child, columns, output_schema, done: false }
    }
}

impl<'a> VectorizedOperator for VecAggregate<'a> {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let child_schema = self.child.schema().clone();

        // Consume all child chunks, collecting rows for aggregate evaluation
        let mut all_rows: Vec<(crate::common::RID, Vec<Value>)> = Vec::new();
        let dummy_rid = crate::common::RID { page_id: PageId(0), slot_id: 0 };
        while let Some(chunk) = self.child.next_chunk()? {
            for row_idx in 0..chunk.len {
                all_rows.push((dummy_rid, chunk.get_row(row_idx)));
            }
        }

        let (_col_names, result_rows) =
            aggregate::execute_aggregate(&self.columns, &all_rows, &child_schema)?;

        if let Some(first_row) = result_rows.into_iter().next() {
            let mut result_chunk = DataChunk::new(first_row.len());
            result_chunk.push_row(&first_row);
            Ok(Some(result_chunk))
        } else {
            Ok(None)
        }
    }
}

// =========================================================================
// VecGroupBy — hash aggregation with chunks
// =========================================================================

/// GROUP BY operator that processes chunks from child, builds hash groups,
/// and emits result groups as chunks.
pub struct VecGroupBy<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    group_exprs: Vec<Expr>,
    having: Option<Expr>,
    select_columns: Vec<SelectColumn>,
    output_schema: Schema,
    /// Materialized output rows (computed on first next_chunk call).
    output: Option<Vec<Vec<Value>>>,
    output_idx: usize,
}

impl<'a> VecGroupBy<'a> {
    pub fn new(
        child: Box<dyn VectorizedOperator + 'a>,
        group_exprs: Vec<Expr>,
        having: Option<Expr>,
        select_columns: Vec<SelectColumn>,
    ) -> Self {
        let col_names: Vec<String> = select_columns.iter().map(|c| match c {
            SelectColumn::Expr { expr, alias } => {
                alias.clone().unwrap_or_else(|| agg_expr_name(expr))
            }
            SelectColumn::AllColumns(_) => "*".to_string(),
        }).collect();

        let output_schema = Schema::new(
            col_names.iter().enumerate().map(|(i, name)| Column {
                name: name.clone(),
                data_type: DataType::Varchar(255),
                nullable: true,
                column_id: i as u16,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                is_unique: false,
                check_expr: None,
                fk_ref: None,
            }).collect(),
        );

        Self {
            child,
            group_exprs,
            having,
            select_columns,
            output_schema,
            output: None,
            output_idx: 0,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        let child_schema = self.child.schema().clone();

        // Consume all child chunks
        let mut all_rows: Vec<(crate::common::RID, Vec<Value>)> = Vec::new();
        let dummy_rid = crate::common::RID { page_id: PageId(0), slot_id: 0 };
        while let Some(chunk) = self.child.next_chunk()? {
            for row_idx in 0..chunk.len {
                all_rows.push((dummy_rid, chunk.get_row(row_idx)));
            }
        }

        // Delegate to existing GROUP BY logic
        let (_col_names, result_rows) = if aggregate::can_stream_group_by(&self.select_columns) {
            aggregate::execute_streaming_group_by(
                &self.group_exprs,
                &self.having,
                &self.select_columns,
                &all_rows,
                &child_schema,
            )?
        } else {
            aggregate::execute_group_by(
                &self.group_exprs,
                &self.having,
                &self.select_columns,
                &all_rows,
                &child_schema,
            )?
        };

        self.output = Some(result_rows);
        Ok(())
    }
}

impl<'a> VectorizedOperator for VecGroupBy<'a> {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.output.is_none() {
            self.materialize()?;
        }

        let output = self.output.as_ref().unwrap();
        if self.output_idx >= output.len() {
            return Ok(None);
        }

        let mut chunk = DataChunk::new(self.output_schema.columns.len());
        let end = (self.output_idx + VECTOR_SIZE).min(output.len());

        for i in self.output_idx..end {
            chunk.push_row(&output[i]);
        }
        self.output_idx = end;

        if chunk.len == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

// =========================================================================
// VecLimit — count tuples across chunks, stop when limit reached
// =========================================================================

/// Limit operator that tracks row count across chunks and stops (or truncates)
/// when the limit is reached. Also supports OFFSET.
pub struct VecLimit<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    count: usize,
    offset: usize,
    emitted: usize,
    skipped: usize,
}

impl<'a> VecLimit<'a> {
    pub fn new(child: Box<dyn VectorizedOperator + 'a>, count: usize, offset: usize) -> Self {
        Self { child, count, offset, emitted: 0, skipped: 0 }
    }
}

impl<'a> VectorizedOperator for VecLimit<'a> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.emitted >= self.count {
            return Ok(None);
        }

        loop {
            match self.child.next_chunk()? {
                None => return Ok(None),
                Some(mut chunk) => {
                    // Handle OFFSET: skip rows
                    if self.skipped < self.offset {
                        let to_skip = (self.offset - self.skipped).min(chunk.len);
                        if to_skip >= chunk.len {
                            self.skipped += chunk.len;
                            continue;
                        }
                        // Partial skip: build a selection vector that keeps only rows after offset
                        let mut selection = vec![false; chunk.len];
                        for i in to_skip..chunk.len {
                            selection[i] = true;
                        }
                        chunk.compact(&selection);
                        self.skipped = self.offset;
                    }

                    // Enforce limit
                    let remaining = self.count - self.emitted;
                    if chunk.len > remaining {
                        chunk.truncate(remaining);
                    }

                    self.emitted += chunk.len;

                    if chunk.len > 0 {
                        return Ok(Some(chunk));
                    }
                }
            }
        }
    }
}

// =========================================================================
// VecSort — materialize all chunks, sort, emit in chunks
// =========================================================================

/// Sort operator that materializes all chunks from child, sorts the data,
/// and then emits sorted results in VECTOR_SIZE chunks.
pub struct VecSort<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    order_by: Vec<OrderByItem>,
    child_schema: Schema,
    /// Sorted rows (materialized on first next_chunk call).
    output: Option<Vec<Vec<Value>>>,
    output_idx: usize,
}

impl<'a> VecSort<'a> {
    pub fn new(child: Box<dyn VectorizedOperator + 'a>, order_by: Vec<OrderByItem>) -> Self {
        let child_schema = child.schema().clone();
        Self {
            child,
            order_by,
            child_schema,
            output: None,
            output_idx: 0,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        // Consume all child chunks
        let mut rows: Vec<Vec<Value>> = Vec::new();
        while let Some(chunk) = self.child.next_chunk()? {
            for row_idx in 0..chunk.len {
                rows.push(chunk.get_row(row_idx));
            }
        }

        // Sort using existing sort logic
        super::sort::execute_sort(&self.order_by, &mut rows, &self.child_schema)?;

        self.output = Some(rows);
        Ok(())
    }
}

impl<'a> VectorizedOperator for VecSort<'a> {
    fn schema(&self) -> &Schema {
        &self.child_schema
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        if self.output.is_none() {
            self.materialize()?;
        }

        let output = self.output.as_ref().unwrap();
        if self.output_idx >= output.len() {
            return Ok(None);
        }

        let mut chunk = DataChunk::with_schema(&self.child_schema);
        let end = (self.output_idx + VECTOR_SIZE).min(output.len());

        for i in self.output_idx..end {
            chunk.push_row(&output[i]);
        }
        self.output_idx = end;

        if chunk.len == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

// =========================================================================
// VecDistinct — deduplicates using a HashSet across chunks
// =========================================================================

pub struct VecDistinct<'a> {
    child: Box<dyn VectorizedOperator + 'a>,
    seen: std::collections::HashSet<Vec<u8>>,
}

impl<'a> VecDistinct<'a> {
    pub fn new(child: Box<dyn VectorizedOperator + 'a>) -> Self {
        Self {
            child,
            seen: std::collections::HashSet::new(),
        }
    }
}

impl<'a> VectorizedOperator for VecDistinct<'a> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next_chunk(&mut self) -> Result<Option<DataChunk>> {
        loop {
            match self.child.next_chunk()? {
                None => return Ok(None),
                Some(chunk) => {
                    let mut selection = Vec::with_capacity(chunk.len);
                    let mut any = false;
                    for row_idx in 0..chunk.len {
                        let row = chunk.get_row(row_idx);
                        let key = aggregate::serialize_row_key(&row);
                        if self.seen.insert(key) {
                            selection.push(true);
                            any = true;
                        } else {
                            selection.push(false);
                        }
                    }

                    if !any {
                        continue;
                    }

                    let mut result = chunk;
                    result.compact(&selection);
                    return Ok(Some(result));
                }
            }
        }
    }
}

// =========================================================================
// Plan-to-operator builder
// =========================================================================

/// Attempt to build a vectorized operator pipeline from a read-only plan.
/// Returns `None` if the plan shape is not supported (JOINs, IndexScans,
/// clustered-index tables, window functions).
pub fn try_build_vectorized<'a>(
    plan: &PlanNode,
    catalog: &'a Catalog,
    cbpm: &'a ConcurrentBufferPool,
    clustered_indexes: &'a HashMap<String, ClusteredIndex>,
    txn_ctx: Option<&'a TxnContext>,
) -> Option<Result<Box<dyn VectorizedOperator + 'a>>> {
    build_vec_inner(plan, catalog, cbpm, clustered_indexes, txn_ctx)
}

fn build_vec_inner<'a>(
    plan: &PlanNode,
    catalog: &'a Catalog,
    cbpm: &'a ConcurrentBufferPool,
    clustered_indexes: &'a HashMap<String, ClusteredIndex>,
    txn_ctx: Option<&'a TxnContext>,
) -> Option<Result<Box<dyn VectorizedOperator + 'a>>> {
    match plan {
        PlanNode::SeqScan { table_name, alias, .. } => {
            // Virtual __dual__ table
            if table_name == "__dual__" {
                return Some(Ok(Box::new(VecDual::new())));
            }

            // Clustered index tables are not supported in vectorized path
            if clustered_indexes.contains_key(&table_name.to_lowercase()) {
                return None;
            }

            let op = VecSeqScan::new(
                table_name, alias.as_deref(), catalog, cbpm, txn_ctx,
            );
            match op {
                Ok(it) => Some(Ok(Box::new(it))),
                Err(e) => Some(Err(e)),
            }
        }

        PlanNode::Filter { predicate, child } => {
            let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_op {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(VecFilter::new(it, predicate.clone())))),
            }
        }

        PlanNode::Projection { columns, child } => {
            // Window functions are not supported in vectorized path
            let has_window = columns.iter().any(|c| {
                if let SelectColumn::Expr { expr, .. } = c {
                    matches!(expr, Expr::WindowFunction { .. })
                } else {
                    false
                }
            });
            if has_window {
                return None;
            }

            // Check if this is an aggregate projection
            if aggregate::has_aggregates(columns) {
                let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
                match child_op {
                    Err(e) => Some(Err(e)),
                    Ok(it) => Some(Ok(Box::new(VecAggregate::new(it, columns.clone())))),
                }
            } else {
                let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
                match child_op {
                    Err(e) => Some(Err(e)),
                    Ok(it) => Some(Ok(Box::new(VecProjection::new(it, columns.clone())))),
                }
            }
        }

        PlanNode::Limit { count, offset, child } => {
            let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_op {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(VecLimit::new(it, *count, *offset)))),
            }
        }

        PlanNode::Sort { order_by, child } => {
            // Special case: Sort(Projection(child)) — sort on full schema, then project
            if let PlanNode::Projection { columns: proj_cols, child: proj_child } = child.as_ref() {
                let inner_op = build_vec_inner(proj_child, catalog, cbpm, clustered_indexes, txn_ctx)?;
                match inner_op {
                    Err(e) => return Some(Err(e)),
                    Ok(it) => {
                        let sorted = VecSort::new(it, order_by.clone());
                        if aggregate::has_aggregates(proj_cols) {
                            let agg = VecAggregate::new(Box::new(sorted), proj_cols.clone());
                            return Some(Ok(Box::new(agg)));
                        } else {
                            let proj = VecProjection::new(Box::new(sorted), proj_cols.clone());
                            return Some(Ok(Box::new(proj)));
                        }
                    }
                }
            }

            let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_op {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(VecSort::new(it, order_by.clone())))),
            }
        }

        PlanNode::GroupBy { group_exprs, having, select_columns, child } => {
            let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_op {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(VecGroupBy::new(
                    it,
                    group_exprs.clone(),
                    having.clone(),
                    select_columns.clone(),
                )))),
            }
        }

        PlanNode::Distinct { child } => {
            let child_op = build_vec_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_op {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(VecDistinct::new(it)))),
            }
        }

        // IndexScan, JOINs, and DML nodes are not supported
        _ => None,
    }
}

// =========================================================================
// Drain — convert chunks to ExecuteResult
// =========================================================================

/// Drain a VectorizedOperator into an ExecuteResult.
/// Converts columnar DataChunks back to row-oriented Vec<Vec<Value>>.
pub fn drain_vectorized(op: &mut dyn VectorizedOperator) -> Result<ExecuteResult> {
    let col_names: Vec<String> = op.schema().columns.iter()
        .map(|c| strip_table_prefix(&c.name))
        .collect();

    let mut rows: Vec<Vec<Value>> = Vec::new();
    while let Some(chunk) = op.next_chunk()? {
        for row_idx in 0..chunk.len {
            rows.push(chunk.get_row(row_idx));
        }
    }

    Ok(ExecuteResult {
        rows,
        columns: col_names,
        rows_affected: 0,
        last_insert_id: 0,
        message: String::new(),
    })
}

// =========================================================================
// Helpers
// =========================================================================

fn strip_table_prefix(name: &str) -> String {
    if let Some(pos) = name.find('.') {
        name[pos + 1..].to_string()
    } else {
        name.to_string()
    }
}

fn expr_to_name(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef { column, .. } => column.clone(),
        Expr::Function { name, .. } => format!("{}(?)", name),
        Expr::WindowFunction { name, .. } => name.clone(),
        _ => "?".to_string(),
    }
}

fn agg_expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Function { name, args, distinct } => {
            if args.is_empty() {
                format!("{}(*)", name)
            } else if *distinct {
                format!("{}(DISTINCT ?)", name)
            } else {
                format!("{}(?)", name)
            }
        }
        Expr::ColumnRef { column, .. } => column.clone(),
        _ => "?".to_string(),
    }
}
