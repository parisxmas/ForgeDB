//! Volcano-style pull-based iterator model for read-only query execution.
//!
//! Each operator implements [`TupleIterator`], which produces one tuple at a
//! time via `next_tuple()`. The top operator pulls from its child, which pulls
//! from its child, etc. Only ONE tuple is in flight at any point, reducing peak
//! memory from O(N) to O(1) for pipeline-breaker-free chains.
//!
//! Supported operators:
//! - [`SeqScanIterator`]: reads heap pages one at a time from ConcurrentBufferPool
//! - [`FilterIterator`]: passes through tuples matching a predicate
//! - [`ProjectionIterator`]: evaluates projection expressions per tuple
//! - [`AggregateIterator`]: computes aggregates (COUNT/SUM/AVG/MIN/MAX) without GROUP BY
//! - [`GroupByIterator`]: groups + aggregates (materializes groups, not input)
//! - [`LimitIterator`]: stops after N tuples
//! - [`SortIterator`]: materializes from child, sorts, then emits one at a time
//! - [`DistinctIterator`]: deduplicates using a HashSet
//!
//! The streaming path is tried first for SELECT queries in `execute_read()`.
//! If the plan shape doesn't match, we fall back to the existing materialization path.

use crate::catalog::Catalog;
use crate::common::{PageId, RID, INVALID_PAGE_ID, PAGE_SIZE};
use crate::error::{ForgeError, Result};
use crate::sql::ast::{Expr, OrderByItem, SelectColumn};
use crate::storage::concurrent_bpm::ConcurrentBufferPool;
use crate::storage::heap_page;
use crate::tuple::schema::{Column, Schema};
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;
use crate::txn::TxnContext;
use crate::txn::mvcc::{MVCC_HEADER_SIZE, XMAX_NONE, decode_version_header, is_visible};

use super::eval::{eval_to_bool, evaluate};
use super::aggregate;

// =========================================================================
// Core trait
// =========================================================================

/// A row produced by a volcano iterator.
pub struct TupleRow {
    pub rid: RID,
    pub values: Vec<Value>,
}

/// Volcano-style pull-based iterator. Each executor node implements this.
pub trait TupleIterator {
    /// Return the output schema of this iterator.
    fn schema(&self) -> &Schema;

    /// Advance to the next tuple. Returns `None` when exhausted.
    fn next_tuple(&mut self) -> Result<Option<TupleRow>>;
}

// =========================================================================
// SeqScanIterator — reads one page at a time from ConcurrentBufferPool
// =========================================================================

/// Sequential scan iterator that reads heap pages directly from the
/// ConcurrentBufferPool, one page at a time. Each `next_tuple()` call
/// returns the next live tuple.
pub struct SeqScanIterator<'a> {
    schema: Schema,
    /// The table schema (without table prefix) for deserialization.
    raw_schema: Schema,
    cbpm: &'a ConcurrentBufferPool,
    mvcc_enabled: bool,
    txn_ctx: Option<&'a TxnContext>,

    // Page iteration state
    current_page_id: PageId,
    /// Pre-fetched tuples from the current page. We load one page worth of
    /// tuples at a time since we can't hold the page read guard across
    /// `next_tuple()` calls (the guard borrows the ConcurrentBufferPool).
    page_tuples: Vec<(RID, Vec<Value>)>,
    page_tuple_idx: usize,
    exhausted: bool,
}

impl<'a> SeqScanIterator<'a> {
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
            raw_schema
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| Column {
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
                })
                .collect(),
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

    /// Load all tuples from the current page into `page_tuples`.
    /// Returns `false` if there are no more pages.
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
                    let rid = RID {
                        page_id: self.current_page_id,
                        slot_id,
                    };
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
                        self.page_tuples.push((rid, values));
                    } else {
                        let values = deserialize(raw, &self.raw_schema)?;
                        self.page_tuples.push((rid, values));
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
            // Empty page, try next page
        }
    }
}

impl<'a> TupleIterator for SeqScanIterator<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            if self.page_tuple_idx < self.page_tuples.len() {
                let (rid, values) = self.page_tuples[self.page_tuple_idx].clone();
                self.page_tuple_idx += 1;
                return Ok(Some(TupleRow { rid, values }));
            }

            // Need to load next page
            if !self.load_next_page()? {
                self.exhausted = true;
                return Ok(None);
            }
        }
    }
}

// =========================================================================
// DualIterator — produces a single empty row for SELECT without FROM
// =========================================================================

pub struct DualIterator {
    schema: Schema,
    emitted: bool,
}

impl DualIterator {
    pub fn new() -> Self {
        Self {
            schema: Schema::new(vec![]),
            emitted: false,
        }
    }
}

impl TupleIterator for DualIterator {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(TupleRow {
            rid: RID {
                page_id: PageId(0),
                slot_id: 0,
            },
            values: vec![],
        }))
    }
}

// =========================================================================
// FilterIterator — passes through tuples that match a predicate
// =========================================================================

pub struct FilterIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    predicate: Expr,
}

impl<'a> FilterIterator<'a> {
    pub fn new(child: Box<dyn TupleIterator + 'a>, predicate: Expr) -> Self {
        Self { child, predicate }
    }
}

impl<'a> TupleIterator for FilterIterator<'a> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        loop {
            match self.child.next_tuple()? {
                None => return Ok(None),
                Some(row) => {
                    if eval_to_bool(&self.predicate, &row.values, self.child.schema())? {
                        return Ok(Some(row));
                    }
                    // Doesn't match, try next
                }
            }
        }
    }
}

// =========================================================================
// ProjectionIterator — evaluates projection expressions per tuple
// =========================================================================

pub struct ProjectionIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    /// Output schema built from column names.
    output_schema: Schema,
    /// Precomputed evaluation plan: None = AllColumns index, Some = expression
    eval_plan: Vec<Option<Expr>>,
}

impl<'a> ProjectionIterator<'a> {
    pub fn new(
        child: Box<dyn TupleIterator + 'a>,
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
            col_names
                .iter()
                .enumerate()
                .map(|(i, name)| Column {
                    name: name.clone(),
                    data_type: crate::tuple::types::DataType::Varchar(255),
                    nullable: true,
                    column_id: i as u16,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                    is_unique: false,
                    check_expr: None,
                    fk_ref: None,
                })
                .collect(),
        );

        Self {
            child,
            output_schema,
            eval_plan,
        }
    }
}

impl<'a> TupleIterator for ProjectionIterator<'a> {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        match self.child.next_tuple()? {
            None => Ok(None),
            Some(row) => {
                let child_schema = self.child.schema();
                let mut projected = Vec::with_capacity(self.eval_plan.len());
                let mut all_col_idx = 0usize;

                for eval in &self.eval_plan {
                    match eval {
                        None => {
                            if all_col_idx < row.values.len() {
                                projected.push(row.values[all_col_idx].clone());
                            }
                            all_col_idx += 1;
                        }
                        Some(expr) => {
                            projected.push(evaluate(expr, &row.values, child_schema)?);
                        }
                    }
                }

                Ok(Some(TupleRow {
                    rid: row.rid,
                    values: projected,
                }))
            }
        }
    }
}

// =========================================================================
// AggregateIterator — computes aggregates without GROUP BY
// =========================================================================

/// Streaming aggregate iterator for queries like `SELECT COUNT(*), SUM(v) FROM t`.
/// Consumes all child tuples in a single `next_tuple()` call, then returns one row.
pub struct AggregateIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    columns: Vec<SelectColumn>,
    output_schema: Schema,
    done: bool,
}

impl<'a> AggregateIterator<'a> {
    pub fn new(
        child: Box<dyn TupleIterator + 'a>,
        columns: Vec<SelectColumn>,
    ) -> Self {
        let col_names: Vec<String> = columns
            .iter()
            .map(|c| match c {
                SelectColumn::Expr { expr, alias } => {
                    alias.clone().unwrap_or_else(|| agg_expr_name(expr))
                }
                SelectColumn::AllColumns(_) => "*".to_string(),
            })
            .collect();

        let output_schema = Schema::new(
            col_names
                .iter()
                .enumerate()
                .map(|(i, name)| Column {
                    name: name.clone(),
                    data_type: crate::tuple::types::DataType::Varchar(255),
                    nullable: true,
                    column_id: i as u16,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                    is_unique: false,
                    check_expr: None,
                    fk_ref: None,
                })
                .collect(),
        );

        Self {
            child,
            columns,
            output_schema,
            done: false,
        }
    }
}

impl<'a> TupleIterator for AggregateIterator<'a> {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let child_schema = self.child.schema().clone();

        // Consume all child tuples, collecting them for aggregate evaluation.
        // Note: we must materialize here because `execute_aggregate` expects
        // all rows. We still save memory by not materializing the full pipeline
        // below the aggregate (filter, scan are streaming).
        let mut rows: Vec<(RID, Vec<Value>)> = Vec::new();
        while let Some(row) = self.child.next_tuple()? {
            rows.push((row.rid, row.values));
        }

        let (_col_names, result_rows) =
            aggregate::execute_aggregate(&self.columns, &rows, &child_schema)?;

        // The result should be a single row
        if let Some(first_row) = result_rows.into_iter().next() {
            Ok(Some(TupleRow {
                rid: RID { page_id: PageId(0), slot_id: 0 },
                values: first_row,
            }))
        } else {
            Ok(None)
        }
    }
}

// =========================================================================
// GroupByIterator — groups + aggregates (materializes groups from child)
// =========================================================================

pub struct GroupByIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    group_exprs: Vec<Expr>,
    having: Option<Expr>,
    select_columns: Vec<SelectColumn>,
    output_schema: Schema,
    /// Materialized output rows (computed on first next_tuple call).
    output: Option<Vec<Vec<Value>>>,
    output_idx: usize,
}

impl<'a> GroupByIterator<'a> {
    pub fn new(
        child: Box<dyn TupleIterator + 'a>,
        group_exprs: Vec<Expr>,
        having: Option<Expr>,
        select_columns: Vec<SelectColumn>,
    ) -> Self {
        let col_names: Vec<String> = select_columns
            .iter()
            .map(|c| match c {
                SelectColumn::Expr { expr, alias } => {
                    alias.clone().unwrap_or_else(|| agg_expr_name(expr))
                }
                SelectColumn::AllColumns(_) => "*".to_string(),
            })
            .collect();

        let output_schema = Schema::new(
            col_names
                .iter()
                .enumerate()
                .map(|(i, name)| Column {
                    name: name.clone(),
                    data_type: crate::tuple::types::DataType::Varchar(255),
                    nullable: true,
                    column_id: i as u16,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                    is_unique: false,
                    check_expr: None,
                    fk_ref: None,
                })
                .collect(),
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

        // Consume all child tuples
        let mut rows: Vec<(RID, Vec<Value>)> = Vec::new();
        while let Some(row) = self.child.next_tuple()? {
            rows.push((row.rid, row.values));
        }

        // Delegate to existing GROUP BY logic
        let (_col_names, result_rows) = if aggregate::can_stream_group_by(&self.select_columns) {
            aggregate::execute_streaming_group_by(
                &self.group_exprs,
                &self.having,
                &self.select_columns,
                &rows,
                &child_schema,
            )?
        } else {
            aggregate::execute_group_by(
                &self.group_exprs,
                &self.having,
                &self.select_columns,
                &rows,
                &child_schema,
            )?
        };

        self.output = Some(result_rows);
        Ok(())
    }
}

impl<'a> TupleIterator for GroupByIterator<'a> {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        if self.output.is_none() {
            self.materialize()?;
        }

        let output = self.output.as_ref().unwrap();
        if self.output_idx >= output.len() {
            return Ok(None);
        }

        let values = output[self.output_idx].clone();
        self.output_idx += 1;

        Ok(Some(TupleRow {
            rid: RID {
                page_id: PageId(0),
                slot_id: 0,
            },
            values,
        }))
    }
}

// =========================================================================
// LimitIterator — stops after N tuples, with optional offset
// =========================================================================

pub struct LimitIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    count: usize,
    offset: usize,
    emitted: usize,
    skipped: usize,
    offset_done: bool,
}

impl<'a> LimitIterator<'a> {
    pub fn new(
        child: Box<dyn TupleIterator + 'a>,
        count: usize,
        offset: usize,
    ) -> Self {
        Self {
            child,
            count,
            offset,
            emitted: 0,
            skipped: 0,
            offset_done: offset == 0,
        }
    }
}

impl<'a> TupleIterator for LimitIterator<'a> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        // Skip offset rows first
        while !self.offset_done {
            match self.child.next_tuple()? {
                None => return Ok(None),
                Some(_) => {
                    self.skipped += 1;
                    if self.skipped >= self.offset {
                        self.offset_done = true;
                    }
                }
            }
        }

        if self.emitted >= self.count {
            return Ok(None);
        }

        match self.child.next_tuple()? {
            None => Ok(None),
            Some(row) => {
                self.emitted += 1;
                Ok(Some(row))
            }
        }
    }
}

// =========================================================================
// SortIterator — materializes from child, sorts, emits one at a time
// =========================================================================

pub struct SortIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    order_by: Vec<OrderByItem>,
    /// Sorted output rows (materialized on first next_tuple call).
    output: Option<Vec<Vec<Value>>>,
    output_idx: usize,
    child_schema: Schema,
}

impl<'a> SortIterator<'a> {
    pub fn new(
        child: Box<dyn TupleIterator + 'a>,
        order_by: Vec<OrderByItem>,
    ) -> Self {
        let child_schema = child.schema().clone();
        Self {
            child,
            order_by,
            output: None,
            output_idx: 0,
            child_schema,
        }
    }

    fn materialize(&mut self) -> Result<()> {
        // Consume all child tuples
        let mut rows: Vec<Vec<Value>> = Vec::new();
        while let Some(row) = self.child.next_tuple()? {
            rows.push(row.values);
        }

        // Sort using existing sort logic
        super::sort::execute_sort(&self.order_by, &mut rows, &self.child_schema)?;

        self.output = Some(rows);
        Ok(())
    }
}

impl<'a> TupleIterator for SortIterator<'a> {
    fn schema(&self) -> &Schema {
        &self.child_schema
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        if self.output.is_none() {
            self.materialize()?;
        }

        let output = self.output.as_ref().unwrap();
        if self.output_idx >= output.len() {
            return Ok(None);
        }

        let values = output[self.output_idx].clone();
        self.output_idx += 1;

        Ok(Some(TupleRow {
            rid: RID {
                page_id: PageId(0),
                slot_id: 0,
            },
            values,
        }))
    }
}

// =========================================================================
// DistinctIterator — deduplicates using a HashSet
// =========================================================================

pub struct DistinctIterator<'a> {
    child: Box<dyn TupleIterator + 'a>,
    seen: std::collections::HashSet<Vec<u8>>,
}

impl<'a> DistinctIterator<'a> {
    pub fn new(child: Box<dyn TupleIterator + 'a>) -> Self {
        Self {
            child,
            seen: std::collections::HashSet::new(),
        }
    }
}

impl<'a> TupleIterator for DistinctIterator<'a> {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn next_tuple(&mut self) -> Result<Option<TupleRow>> {
        loop {
            match self.child.next_tuple()? {
                None => return Ok(None),
                Some(row) => {
                    let key = aggregate::serialize_row_key(&row.values);
                    if self.seen.insert(key) {
                        return Ok(Some(row));
                    }
                    // Duplicate, try next
                }
            }
        }
    }
}

// =========================================================================
// Plan-to-iterator builder
// =========================================================================

use crate::planner::plan::PlanNode;
use crate::index::ClusteredIndex;

/// Attempt to build a volcano iterator pipeline from a read-only plan.
/// Returns `None` if the plan shape is not supported by the streaming path
/// (e.g., contains JOINs, IndexScans, or other unsupported nodes).
///
/// When this returns `Some`, the caller should drain the iterator to produce
/// the final `ExecuteResult`.
pub fn try_build_iterator<'a>(
    plan: &PlanNode,
    catalog: &'a Catalog,
    cbpm: &'a ConcurrentBufferPool,
    clustered_indexes: &'a std::collections::HashMap<String, ClusteredIndex>,
    txn_ctx: Option<&'a TxnContext>,
) -> Option<Result<Box<dyn TupleIterator + 'a>>> {
    build_iterator_inner(plan, catalog, cbpm, clustered_indexes, txn_ctx)
}

fn build_iterator_inner<'a>(
    plan: &PlanNode,
    catalog: &'a Catalog,
    cbpm: &'a ConcurrentBufferPool,
    clustered_indexes: &'a std::collections::HashMap<String, ClusteredIndex>,
    txn_ctx: Option<&'a TxnContext>,
) -> Option<Result<Box<dyn TupleIterator + 'a>>> {
    match plan {
        PlanNode::SeqScan {
            table_name, alias, ..
        } => {
            // Virtual __dual__ table
            if table_name == "__dual__" {
                return Some(Ok(Box::new(DualIterator::new())));
            }

            // Clustered index tables are not supported in the streaming path
            // because ClusteredIndex::scan_all requires &mut LocalBpm.
            if clustered_indexes.contains_key(&table_name.to_lowercase()) {
                return None;
            }

            let iter = SeqScanIterator::new(
                table_name,
                alias.as_deref(),
                catalog,
                cbpm,
                txn_ctx,
            );
            match iter {
                Ok(it) => Some(Ok(Box::new(it))),
                Err(e) => Some(Err(e)),
            }
        }

        PlanNode::Filter { predicate, child } => {
            let child_iter = build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_iter {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(FilterIterator::new(it, predicate.clone())))),
            }
        }

        PlanNode::Projection { columns, child } => {
            // Check for window functions — not supported in streaming path
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
                let child_iter =
                    build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
                match child_iter {
                    Err(e) => Some(Err(e)),
                    Ok(it) => Some(Ok(Box::new(AggregateIterator::new(it, columns.clone())))),
                }
            } else {
                let child_iter =
                    build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
                match child_iter {
                    Err(e) => Some(Err(e)),
                    Ok(it) => Some(Ok(Box::new(ProjectionIterator::new(it, columns.clone())))),
                }
            }
        }

        PlanNode::Limit {
            count,
            offset,
            child,
        } => {
            let child_iter =
                build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_iter {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(LimitIterator::new(it, *count, *offset)))),
            }
        }

        PlanNode::Sort { order_by, child } => {
            // Special case: Sort(Projection(child)) — sort on full schema, then project
            if let PlanNode::Projection {
                columns: proj_cols,
                child: proj_child,
            } = child.as_ref()
            {
                // Build iterator for the projection's child (the full schema)
                let inner_iter = build_iterator_inner(
                    proj_child, catalog, cbpm, clustered_indexes, txn_ctx,
                )?;
                match inner_iter {
                    Err(e) => return Some(Err(e)),
                    Ok(it) => {
                        // Sort on full schema
                        let sorted = SortIterator::new(it, order_by.clone());
                        // Then project
                        if aggregate::has_aggregates(proj_cols) {
                            let agg = AggregateIterator::new(
                                Box::new(sorted),
                                proj_cols.clone(),
                            );
                            return Some(Ok(Box::new(agg)));
                        } else {
                            let proj = ProjectionIterator::new(
                                Box::new(sorted),
                                proj_cols.clone(),
                            );
                            return Some(Ok(Box::new(proj)));
                        }
                    }
                }
            }

            let child_iter =
                build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_iter {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(SortIterator::new(it, order_by.clone())))),
            }
        }

        PlanNode::GroupBy {
            group_exprs,
            having,
            select_columns,
            child,
        } => {
            let child_iter =
                build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_iter {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(GroupByIterator::new(
                    it,
                    group_exprs.clone(),
                    having.clone(),
                    select_columns.clone(),
                )))),
            }
        }

        PlanNode::Distinct { child } => {
            let child_iter =
                build_iterator_inner(child, catalog, cbpm, clustered_indexes, txn_ctx)?;
            match child_iter {
                Err(e) => Some(Err(e)),
                Ok(it) => Some(Ok(Box::new(DistinctIterator::new(it)))),
            }
        }

        // IndexScan, JOINs, and DML nodes are not supported in the streaming path
        _ => None,
    }
}

/// Drain a TupleIterator into an ExecuteResult.
/// The column names come from the iterator's output schema.
pub fn drain_iterator(
    iter: &mut dyn TupleIterator,
) -> Result<super::executor::ExecuteResult> {
    let col_names: Vec<String> = iter
        .schema()
        .columns
        .iter()
        .map(|c| strip_table_prefix(&c.name))
        .collect();

    let mut rows: Vec<Vec<Value>> = Vec::new();
    while let Some(row) = iter.next_tuple()? {
        rows.push(row.values);
    }

    Ok(super::executor::ExecuteResult {
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
