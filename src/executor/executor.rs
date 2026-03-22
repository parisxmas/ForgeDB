use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::Result;
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::planner::plan::PlanNode;
use crate::sql::ast::{Expr, LiteralValue, SelectColumn};
use crate::storage::local_bpm::LocalBpm;
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;
use crate::txn::TxnContext;

use super::{
    aggregate, create_index, create_table, delete, drop_table, filter, grace_hash_join,
    hash_join, index_scan, insert, limit, projection, seq_scan, sort, update,
};

/// Context passed to executors.
/// DDL operations (CREATE TABLE, DROP TABLE, CREATE INDEX) use `catalog_mut`,
/// `indexes_mut`, and `clustered_indexes_mut` for structural mutations.
/// DML operations (INSERT/UPDATE/DELETE) only need immutable index access
/// since BTreeIndex::insert/delete use AtomicU32 for root_page_id.
pub struct ExecutorContext<'a, 'b> {
    pub bpm: &'a mut LocalBpm<'b>,
    pub catalog: &'a mut Catalog,
    pub indexes: &'a mut Vec<(String, BTreeIndex)>,
    pub clustered_indexes: &'a mut std::collections::HashMap<String, ClusteredIndex>,
    pub auto_increment_counters: &'a mut std::collections::HashMap<String, i64>,
    pub txn_ctx: Option<TxnContext>,
}

/// DML-only context — holds immutable index references.
/// This allows the DML path to use read locks on the global index collections,
/// enabling concurrent DML on different tables.
pub struct DmlContext<'a, 'b> {
    pub bpm: &'a mut LocalBpm<'b>,
    pub catalog: &'a Catalog,
    pub indexes: &'a [(String, BTreeIndex)],
    pub clustered_indexes: &'a std::collections::HashMap<String, ClusteredIndex>,
    pub auto_increment_counters: &'a mut std::collections::HashMap<String, i64>,
    pub txn_ctx: Option<TxnContext>,
}

/// Read-only context for queries — borrows catalog/indexes immutably.
/// This avoids cloning the entire catalog on every SELECT.
pub struct ReadContext<'a, 'b> {
    pub bpm: &'a mut LocalBpm<'b>,
    pub catalog: &'a Catalog,
    pub indexes: &'a [(String, BTreeIndex)],
    pub clustered_indexes: &'a std::collections::HashMap<String, ClusteredIndex>,
    pub txn_ctx: Option<TxnContext>,
}

/// Result of executing a SQL statement.
#[derive(Debug)]
pub struct ExecuteResult {
    pub rows: Vec<Vec<Value>>,
    pub columns: Vec<String>,
    pub rows_affected: usize,
    pub last_insert_id: u64,
    pub message: String,
}

/// Execute a physical plan and return the result.
pub fn execute(plan: PlanNode, ctx: &mut ExecutorContext) -> Result<ExecuteResult> {
    match plan {
        PlanNode::CreateTable {
            table_name,
            columns,
        } => {
            if table_name.is_empty() {
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![], rows_affected: 0, last_insert_id: 0,
                    message: "OK (table already exists)".into(),
                });
            }
            let result = create_table::execute_create_table(&table_name, &columns, ctx.bpm, ctx.catalog)?;
            // Auto-create clustered B+ tree index on PK for fixed-size tables only.
            // Tables with VARCHAR columns can produce rows exceeding 4KB page size,
            // so we skip clustered index and use heap file + secondary B-tree instead.
            let has_pk = columns.iter().any(|c| c.is_primary_key);
            let has_varchar = columns.iter().any(|c| {
                matches!(c.data_type, crate::tuple::types::DataType::Varchar(_))
            });
            if has_pk && !has_varchar {
                let pk_col_idx = match columns.iter().position(|c| c.is_primary_key) {
                    Some(idx) => idx,
                    None => return Err(crate::error::ForgeError::Execution("primary key column not found".into())),
                };
                let cidx = ClusteredIndex::create(ctx.bpm, pk_col_idx)
                    .map_err(|e| crate::error::ForgeError::Execution(format!("failed to create clustered index: {}", e)))?;
                ctx.clustered_indexes.insert(table_name.to_lowercase(), cidx);
            }
            // Also create secondary B-tree index on PK for backward compat
            for col_def in &columns {
                if col_def.is_primary_key {
                    let _ = create_index::execute_create_index(
                        &format!("pk_{}_{}", table_name, col_def.name),
                        &table_name,
                        &[col_def.name.clone()],
                        true,
                        ctx.bpm,
                        ctx.catalog,
                        ctx.indexes,
                    );
                }
            }
            Ok(result)
        }

        PlanNode::DropTable { table_name } => {
            if table_name.is_empty() {
                return Ok(ExecuteResult {
                    rows: vec![], columns: vec![], rows_affected: 0, last_insert_id: 0,
                    message: "OK (table does not exist)".into(),
                });
            }
            drop_table::execute_drop_table(&table_name, ctx.catalog, ctx.indexes)
        }

        PlanNode::CreateIndex {
            index_name,
            table_name,
            columns,
            unique,
            include_columns: _,
        } => create_index::execute_create_index(
            &index_name,
            &table_name,
            &columns,
            unique,
            ctx.bpm,
            ctx.catalog,
            ctx.indexes,
        ),

        PlanNode::Insert {
            table_name,
            columns,
            values,
            on_conflict,
        } => insert::execute_insert(
            &table_name,
            &columns,
            &values,
            ctx.bpm,
            ctx.catalog,
            ctx.indexes,
            ctx.clustered_indexes,
            ctx.auto_increment_counters,
            &mut ctx.txn_ctx,
            &on_conflict,
        ),

        PlanNode::Update {
            table_name,
            assignments,
            child,
        } => {
            let (_schema, rows) = execute_scan(*child, ctx)?;
            update::execute_update(
                &table_name,
                &assignments,
                rows,
                ctx.bpm,
                ctx.catalog,
                ctx.indexes,
                &mut ctx.txn_ctx,
            )
        }

        PlanNode::Delete {
            table_name,
            child,
        } => {
            let (_schema, rows) = execute_scan(*child, ctx)?;
            delete::execute_delete(&table_name, rows, ctx.bpm, ctx.catalog, ctx.indexes, ctx.clustered_indexes, &mut ctx.txn_ctx)
        }

        // Query pipeline: SeqScan / Filter / Join optionally wrapped in Projection / Sort / Limit
        PlanNode::Limit { count, offset, child } => {
            let mut result = execute(*child, ctx)?;
            if offset > 0 {
                result.rows = result.rows.into_iter().skip(offset).collect();
            }
            result.rows = limit::execute_limit(count, result.rows);
            Ok(result)
        }

        PlanNode::Sort { order_by, child } => {
            // If child is Projection, sort on full schema THEN project
            if let PlanNode::Projection { columns: proj_cols, child: proj_child } = *child {
                let (schema, rows) = execute_scan(*proj_child, ctx)?;
                // Sort on full schema (has all columns including ORDER BY targets)
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                let mut value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
                sort::execute_sort(&order_by, &mut value_rows, &schema)?;
                // Now project
                let rows_with_rid: Vec<_> = value_rows.into_iter().map(|v| (dummy_rid, v)).collect();
                if aggregate::has_aggregates(&proj_cols) {
                    let (col_names, result_rows) = aggregate::execute_aggregate(&proj_cols, &rows_with_rid, &schema)?;
                    return Ok(ExecuteResult { rows: result_rows, columns: col_names, rows_affected: 0, last_insert_id: 0, message: String::new() });
                }
                let (col_names, projected_rows) = projection::execute_projection(&proj_cols, &rows_with_rid, &schema)?;
                return Ok(ExecuteResult { rows: projected_rows, columns: col_names, rows_affected: 0, last_insert_id: 0, message: String::new() });
            }
            // Otherwise sort on result columns
            let mut result = execute(*child, ctx)?;
            let temp_schema = columns_to_schema(&result.columns);
            sort::execute_sort(&order_by, &mut result.rows, &temp_schema)?;
            Ok(result)
        }

        PlanNode::Projection { columns, child } => {
            let (schema, rows) = execute_scan(*child, ctx)?;
            if aggregate::has_aggregates(&columns) {
                let (col_names, result_rows) =
                    aggregate::execute_aggregate(&columns, &rows, &schema)?;
                Ok(ExecuteResult {
                    rows: result_rows,
                    columns: col_names,
                    rows_affected: 0, last_insert_id: 0,
                    message: String::new(),
                })
            } else {
                let (col_names, projected_rows) =
                    projection::execute_projection(&columns, &rows, &schema)?;
                Ok(ExecuteResult {
                    rows: projected_rows,
                    columns: col_names,
                    rows_affected: 0, last_insert_id: 0,
                    message: String::new(),
                })
            }
        }

        // GROUP BY
        PlanNode::GroupBy {
            group_exprs,
            having,
            select_columns,
            child,
        } => {
            let (schema, rows) = execute_scan(*child, ctx)?;
            // Use streaming GROUP BY when all select columns support it
            let (col_names, result_rows) = if aggregate::can_stream_group_by(&select_columns) {
                aggregate::execute_streaming_group_by(
                    &group_exprs, &having, &select_columns, &rows, &schema,
                )?
            } else {
                aggregate::execute_group_by(
                    &group_exprs, &having, &select_columns, &rows, &schema,
                )?
            };
            Ok(ExecuteResult {
                rows: result_rows,
                columns: col_names,
                rows_affected: 0, last_insert_id: 0,
                message: String::new(),
            })
        }

        // DISTINCT
        PlanNode::Distinct { child } => {
            let mut result = execute(*child, ctx)?;
            let mut seen = std::collections::HashSet::new();
            result.rows.retain(|row| {
                // Use binary serialization instead of format! to avoid String allocation per row
                let key = super::aggregate::serialize_row_key(row);
                seen.insert(key)
            });
            Ok(result)
        }

        // Bare scan at top level (SELECT * FROM table)
        PlanNode::SeqScan { .. }
        | PlanNode::IndexScan { .. }
        | PlanNode::Filter { .. }
        | PlanNode::NestedLoopJoin { .. }
        | PlanNode::HashJoin { .. } => {
            let (schema, rows) = execute_scan(plan, ctx)?;
            let col_names: Vec<String> = schema.columns.iter()
                .map(|c| strip_table_prefix(&c.name))
                .collect();
            let value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
            Ok(ExecuteResult {
                rows: value_rows,
                columns: col_names,
                rows_affected: 0, last_insert_id: 0,
                message: String::new(),
            })
        }
    }
}

/// Execute a scan-type plan node, returning schema and rows with RIDs.
fn execute_scan(
    plan: PlanNode,
    ctx: &mut ExecutorContext,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    match plan {
        PlanNode::SeqScan { table_name, alias, .. } => {
            // Virtual __dual__ table for SELECT without FROM
            if table_name == "__dual__" {
                let schema = Schema::new(vec![]);
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                return Ok((schema, vec![(dummy_rid, vec![])]));
            }
            let prefix = alias.as_deref().unwrap_or(&table_name);
            // Use clustered index scan if available (single-hop, sorted, cache-friendly)
            let (schema, rows) = if let Some(cidx) = ctx.clustered_indexes.get(&table_name.to_lowercase()) {
                let info = ctx.catalog.get_table(&table_name)
                    .ok_or_else(|| crate::error::ForgeError::Execution(format!("table '{}' not found", table_name)))?;
                let schema = info.schema.clone();
                let raw_rows = cidx.scan_all(ctx.bpm)?;
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                let mut rows = Vec::with_capacity(raw_rows.len());
                for raw in raw_rows {
                    let values = crate::tuple::tuple::deserialize(&raw, &schema)?;
                    rows.push((dummy_rid, values));
                }
                (schema, rows)
            } else {
                seq_scan::execute_seq_scan_full(
                    &table_name, ctx.bpm, ctx.catalog, None,
                    ctx.txn_ctx.as_ref(),
                )?
            };
            // Prefix column names with table name/alias for JOIN disambiguation
            let prefixed = Schema::new(
                schema.columns.iter().enumerate().map(|(i, c)| {
                    crate::tuple::schema::Column {
                        name: format!("{}.{}", prefix, c.name),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        column_id: i as u16,
                        auto_increment: c.auto_increment,
                        default_value: c.default_value.clone(),
                        is_primary_key: c.is_primary_key,
                        is_unique: false,
                        check_expr: None, fk_ref: None,
                    }
                }).collect(),
            );
            Ok((prefixed, rows))
        }
        PlanNode::IndexScan {
            table_name,
            index_column,
            lookup_value,
            index_only,
        } => {
            if index_only {
                index_scan::execute_index_only_scan(
                    &table_name, &index_column, &lookup_value,
                    ctx.bpm, ctx.catalog, ctx.indexes,
                )
            } else {
                index_scan::execute_index_scan(
                    &table_name, &index_column, &lookup_value,
                    ctx.bpm, ctx.catalog, ctx.indexes,
                )
            }
        }
        PlanNode::Filter { predicate, child } => {
            let (schema, rows) = execute_scan(*child, ctx)?;
            let filtered = filter::execute_filter(&predicate, rows, &schema)?;
            Ok((schema, filtered))
        }
        PlanNode::NestedLoopJoin {
            left,
            right,
            join_type,
            on,
        } => {
            let (left_schema, left_rows) = execute_scan(*left, ctx)?;
            let (right_schema, right_rows) = execute_scan(*right, ctx)?;

            // Auto-select grace hash join for large datasets
            if left_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
                || right_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
            {
                if let Some(on_expr) = &on {
                    if let Some((lk, rk)) = extract_equi_join_key_indices(on_expr, &left_schema, &right_schema) {
                        return grace_hash_join::grace_hash_join(
                            &left_rows, &right_rows, &join_type,
                            lk, rk, &left_schema, &right_schema,
                        );
                    }
                }
            }

            hash_join::execute_join(
                &left_rows,
                &right_rows,
                &join_type,
                &on,
                &left_schema,
                &right_schema,
            )
        }
        PlanNode::HashJoin {
            left,
            right,
            join_type,
            on,
        } => {
            let (left_schema, left_rows) = execute_scan(*left, ctx)?;
            let (right_schema, right_rows) = execute_scan(*right, ctx)?;

            // Auto-select grace hash join for large datasets
            if left_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
                || right_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
            {
                if let Some(on_expr) = &on {
                    if let Some((lk, rk)) = extract_equi_join_key_indices(on_expr, &left_schema, &right_schema) {
                        return grace_hash_join::grace_hash_join(
                            &left_rows, &right_rows, &join_type,
                            lk, rk, &left_schema, &right_schema,
                        );
                    }
                }
            }

            hash_join::execute_join(
                &left_rows,
                &right_rows,
                &join_type,
                &on,
                &left_schema,
                &right_schema,
            )
        }
        // If we get a projection/sort/limit here, execute inner scan
        other => Err(crate::error::ForgeError::Execution(format!(
            "unexpected plan node in scan context: {:?}",
            other
        ))),
    }
}

/// Extract equi-join key column indices from an ON expression for grace hash join.
fn extract_equi_join_key_indices(
    on: &crate::sql::ast::Expr,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(usize, usize)> {
    if let crate::sql::ast::Expr::BinaryOp {
        left,
        op: crate::sql::ast::BinaryOperator::Eq,
        right,
    } = on
    {
        let find_col = |expr: &crate::sql::ast::Expr, schema: &Schema| -> Option<usize> {
            if let crate::sql::ast::Expr::ColumnRef { table, column } = expr {
                // Try qualified name first
                if let Some(tbl) = table {
                    let qualified = format!("{}.{}", tbl, column);
                    if let Some((idx, _)) = schema.get_column(&qualified) {
                        return Some(idx);
                    }
                }
                // Try bare name
                if let Some((idx, _)) = schema.get_column(column) {
                    return Some(idx);
                }
                // Suffix match
                let suffix = format!(".{}", column.to_lowercase());
                for (i, c) in schema.columns.iter().enumerate() {
                    if c.name.to_lowercase().ends_with(&suffix) {
                        return Some(i);
                    }
                }
            }
            None
        };

        // Try left=left_schema, right=right_schema
        if let (Some(l), Some(r)) = (find_col(left, left_schema), find_col(right, right_schema)) {
            return Some((l, r));
        }
        // Try reversed
        if let (Some(l), Some(r)) = (find_col(right, left_schema), find_col(left, right_schema)) {
            return Some((l, r));
        }
    }
    None
}

// =========================================================================
// Read-only execution path — zero catalog/index cloning
// =========================================================================

/// Execute a read-only plan using borrowed (not cloned) catalog and indexes.
pub fn execute_read(plan: PlanNode, ctx: &mut ReadContext) -> Result<ExecuteResult> {
    // ── Volcano iterator fast path ────────────────────────────────────
    // Try to build a pull-based iterator pipeline for this plan.
    // This avoids materializing intermediate Vec<(RID, Vec<Value>)> between
    // ── COUNT(*) fast path (highest priority) ───────────────────────
    // Must run BEFORE the volcano iterator to avoid per-tuple overhead.
    if let PlanNode::Projection { ref columns, ref child } = plan {
        if let Some(fast_result) = try_count_star_fast(columns, child, ctx) {
            return fast_result;
        }
    }

    // ── Volcano iterator fast path ────────────────────────────────
    // Streaming execution: pull one tuple at a time through the pipeline.
    // If the plan contains unsupported nodes (JOINs, IndexScan,
    // clustered-index tables), returns None and falls through.
    {
        let cbpm = ctx.bpm.get_cbpm();
        if let Some(iter_result) = super::iterator::try_build_iterator(
            &plan, ctx.catalog, cbpm, ctx.clustered_indexes, ctx.txn_ctx.as_ref(),
        ) {
            match iter_result {
                Ok(mut iter) => return super::iterator::drain_iterator(iter.as_mut()),
                Err(e) => return Err(e),
            }
        }
    }

    match plan {
        PlanNode::Limit { count, offset, child } => {
            // LIMIT pushdown: if child is a bare scan (no sort), pass the limit
            // down so we stop scanning early instead of reading the whole table.
            if offset == 0 && is_pushdown_safe(&child) {
                let mut result = execute_read_with_limit(*child, count, ctx)?;
                result.rows.truncate(count);
                return Ok(result);
            }
            let mut result = execute_read(*child, ctx)?;
            if offset > 0 {
                result.rows = result.rows.into_iter().skip(offset).collect();
            }
            result.rows = limit::execute_limit(count, result.rows);
            Ok(result)
        }

        PlanNode::Sort { order_by, child } => {
            if let PlanNode::Projection { columns: proj_cols, child: proj_child } = *child {
                let (schema, rows) = execute_read_scan(*proj_child, ctx)?;
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                let mut value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
                sort::execute_sort(&order_by, &mut value_rows, &schema)?;
                let rows_with_rid: Vec<_> = value_rows.into_iter().map(|v| (dummy_rid, v)).collect();
                if aggregate::has_aggregates(&proj_cols) {
                    let (col_names, result_rows) = aggregate::execute_aggregate(&proj_cols, &rows_with_rid, &schema)?;
                    return Ok(ExecuteResult { rows: result_rows, columns: col_names, rows_affected: 0, last_insert_id: 0, message: String::new() });
                }
                let (col_names, projected_rows) = projection::execute_projection(&proj_cols, &rows_with_rid, &schema)?;
                return Ok(ExecuteResult { rows: projected_rows, columns: col_names, rows_affected: 0, last_insert_id: 0, message: String::new() });
            }
            let mut result = execute_read(*child, ctx)?;
            let temp_schema = columns_to_schema(&result.columns);
            sort::execute_sort(&order_by, &mut result.rows, &temp_schema)?;
            Ok(result)
        }

        PlanNode::Projection { columns, child } => {
            // ── COUNT(*) fast path ──────────────────────────────────────
            // Detect `SELECT COUNT(*) FROM table` (no WHERE, no GROUP BY)
            // and short-circuit with a slot-directory scan that avoids all
            // tuple deserialization, Vec allocation, and LocalBpm overhead.
            if let Some(fast_result) = try_count_star_fast(&columns, &*child, ctx) {
                return fast_result;
            }

            let (schema, rows) = execute_read_scan(*child, ctx)?;
            if aggregate::has_aggregates(&columns) {
                let (col_names, result_rows) =
                    aggregate::execute_aggregate(&columns, &rows, &schema)?;
                Ok(ExecuteResult {
                    rows: result_rows, columns: col_names,
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                })
            } else {
                let (col_names, projected_rows) =
                    projection::execute_projection(&columns, &rows, &schema)?;
                Ok(ExecuteResult {
                    rows: projected_rows, columns: col_names,
                    rows_affected: 0, last_insert_id: 0, message: String::new(),
                })
            }
        }

        PlanNode::GroupBy { group_exprs, having, select_columns, child } => {
            let (schema, rows) = execute_read_scan(*child, ctx)?;
            // Use streaming GROUP BY when all select columns support it
            let (col_names, result_rows) = if aggregate::can_stream_group_by(&select_columns) {
                aggregate::execute_streaming_group_by(
                    &group_exprs, &having, &select_columns, &rows, &schema,
                )?
            } else {
                aggregate::execute_group_by(
                    &group_exprs, &having, &select_columns, &rows, &schema,
                )?
            };
            Ok(ExecuteResult {
                rows: result_rows, columns: col_names,
                rows_affected: 0, last_insert_id: 0, message: String::new(),
            })
        }

        PlanNode::Distinct { child } => {
            let mut result = execute_read(*child, ctx)?;
            let mut seen = std::collections::HashSet::new();
            result.rows.retain(|row| {
                let key = super::aggregate::serialize_row_key(row);
                seen.insert(key)
            });
            Ok(result)
        }

        PlanNode::SeqScan { .. }
        | PlanNode::IndexScan { .. }
        | PlanNode::Filter { .. }
        | PlanNode::NestedLoopJoin { .. }
        | PlanNode::HashJoin { .. } => {
            let (schema, rows) = execute_read_scan(plan, ctx)?;
            let col_names: Vec<String> = schema.columns.iter()
                .map(|c| strip_table_prefix(&c.name))
                .collect();
            let value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
            Ok(ExecuteResult {
                rows: value_rows, columns: col_names,
                rows_affected: 0, last_insert_id: 0, message: String::new(),
            })
        }

        // Write operations should never reach here
        other => Err(crate::error::ForgeError::Execution(format!(
            "write plan in read-only context: {:?}", other
        ))),
    }
}

/// Execute a scan-type plan node in read-only mode.
fn execute_read_scan(
    plan: PlanNode,
    ctx: &mut ReadContext,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    match plan {
        PlanNode::SeqScan { table_name, alias, .. } => {
            // Virtual __dual__ table for SELECT without FROM
            if table_name == "__dual__" {
                let schema = Schema::new(vec![]);
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                return Ok((schema, vec![(dummy_rid, vec![])]));
            }
            let prefix = alias.as_deref().unwrap_or(&table_name);
            let (schema, rows) = if let Some(cidx) = ctx.clustered_indexes.get(&table_name.to_lowercase()) {
                let info = ctx.catalog.get_table(&table_name)
                    .ok_or_else(|| crate::error::ForgeError::Execution(format!("table '{}' not found", table_name)))?;
                let schema = info.schema.clone();
                let raw_rows = cidx.scan_all(ctx.bpm)?;
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                let mut rows = Vec::with_capacity(raw_rows.len());
                for raw in raw_rows {
                    let values = crate::tuple::tuple::deserialize(&raw, &schema)?;
                    rows.push((dummy_rid, values));
                }
                (schema, rows)
            } else {
                // Direct scan: bypass LocalBpm for read-only heap scans
                let cbpm = ctx.bpm.get_cbpm();
                seq_scan::execute_seq_scan_direct(
                    &table_name, ctx.catalog, cbpm, None,
                    ctx.txn_ctx.as_ref(),
                )?
            };
            let prefixed = Schema::new(
                schema.columns.iter().enumerate().map(|(i, c)| {
                    crate::tuple::schema::Column {
                        name: format!("{}.{}", prefix, c.name),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        column_id: i as u16,
                        auto_increment: c.auto_increment,
                        default_value: c.default_value.clone(),
                        is_primary_key: c.is_primary_key,
                        is_unique: false,
                        check_expr: None, fk_ref: None,
                    }
                }).collect(),
            );
            Ok((prefixed, rows))
        }
        PlanNode::IndexScan { table_name, index_column, lookup_value, index_only } => {
            if index_only {
                index_scan::execute_index_only_scan(
                    &table_name, &index_column, &lookup_value,
                    ctx.bpm, ctx.catalog, ctx.indexes,
                )
            } else {
                index_scan::execute_index_scan(
                    &table_name, &index_column, &lookup_value,
                    ctx.bpm, ctx.catalog, ctx.indexes,
                )
            }
        }
        PlanNode::Filter { predicate, child } => {
            let (schema, rows) = execute_read_scan(*child, ctx)?;
            let filtered = filter::execute_filter(&predicate, rows, &schema)?;
            Ok((schema, filtered))
        }
        PlanNode::NestedLoopJoin { left, right, join_type, on } => {
            let (left_schema, left_rows) = execute_read_scan(*left, ctx)?;
            let (right_schema, right_rows) = execute_read_scan(*right, ctx)?;

            // Auto-select grace hash join for large datasets
            if left_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
                || right_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
            {
                if let Some(on_expr) = &on {
                    if let Some((lk, rk)) = extract_equi_join_key_indices(on_expr, &left_schema, &right_schema) {
                        return grace_hash_join::grace_hash_join(
                            &left_rows, &right_rows, &join_type,
                            lk, rk, &left_schema, &right_schema,
                        );
                    }
                }
            }

            hash_join::execute_join(
                &left_rows, &right_rows, &join_type, &on,
                &left_schema, &right_schema,
            )
        }
        PlanNode::HashJoin { left, right, join_type, on } => {
            let (left_schema, left_rows) = execute_read_scan(*left, ctx)?;
            let (right_schema, right_rows) = execute_read_scan(*right, ctx)?;

            // Auto-select grace hash join for large datasets
            if left_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
                || right_rows.len() > grace_hash_join::HASH_JOIN_MEMORY_LIMIT
            {
                if let Some(on_expr) = &on {
                    if let Some((lk, rk)) = extract_equi_join_key_indices(on_expr, &left_schema, &right_schema) {
                        return grace_hash_join::grace_hash_join(
                            &left_rows, &right_rows, &join_type,
                            lk, rk, &left_schema, &right_schema,
                        );
                    }
                }
            }

            hash_join::execute_join(
                &left_rows, &right_rows, &join_type, &on,
                &left_schema, &right_schema,
            )
        }
        other => Err(crate::error::ForgeError::Execution(format!(
            "unexpected plan node in read scan context: {:?}", other
        ))),
    }
}

/// Check if LIMIT can be pushed down into a scan node safely.
/// Safe when there's no Sort between LIMIT and the scan — the order
/// is already determined by storage order, so taking the first N is correct.
fn is_pushdown_safe(plan: &PlanNode) -> bool {
    matches!(
        plan,
        PlanNode::SeqScan { .. }
        | PlanNode::Filter { .. }
        | PlanNode::Projection { .. }
    )
}

/// Execute a read-only plan with a row limit hint pushed into scans.
fn execute_read_with_limit(plan: PlanNode, limit: usize, ctx: &mut ReadContext) -> Result<ExecuteResult> {
    match plan {
        PlanNode::SeqScan { ref table_name, ref alias, .. } => {
            let prefix = alias.as_deref().unwrap_or(table_name);
            let (schema, rows) = if let Some(cidx) = ctx.clustered_indexes.get(&table_name.to_lowercase()) {
                let info = ctx.catalog.get_table(table_name)
                    .ok_or_else(|| crate::error::ForgeError::Execution(format!("table '{}' not found", table_name)))?;
                let schema = info.schema.clone();
                // Clustered index doesn't support limit yet — scan all
                let raw_rows = cidx.scan_all(ctx.bpm)?;
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                let mut rows = Vec::with_capacity(raw_rows.len().min(limit));
                for raw in raw_rows {
                    let values = crate::tuple::tuple::deserialize(&raw, &schema)?;
                    rows.push((dummy_rid, values));
                    if rows.len() >= limit {
                        break;
                    }
                }
                (schema, rows)
            } else {
                // Direct scan: bypass LocalBpm for read-only heap scans with limit
                let cbpm = ctx.bpm.get_cbpm();
                seq_scan::execute_seq_scan_direct(table_name, ctx.catalog, cbpm, Some(limit), ctx.txn_ctx.as_ref())?
            };
            let prefixed = Schema::new(
                schema.columns.iter().enumerate().map(|(i, c)| {
                    crate::tuple::schema::Column {
                        name: format!("{}.{}", prefix, c.name),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        column_id: i as u16,
                        auto_increment: c.auto_increment,
                        default_value: c.default_value.clone(),
                        is_primary_key: c.is_primary_key,
                        is_unique: false,
                        check_expr: None, fk_ref: None,
                    }
                }).collect(),
            );
            let col_names: Vec<String> = prefixed.columns.iter()
                .map(|c| strip_table_prefix(&c.name))
                .collect();
            let value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
            Ok(ExecuteResult {
                rows: value_rows, columns: col_names,
                rows_affected: 0, last_insert_id: 0, message: String::new(),
            })
        }
        PlanNode::Filter { predicate, child } => {
            // For Filter+Limit, we can't just scan `limit` rows — we need to scan
            // until we've found `limit` rows that pass the filter. Pass a larger
            // hint to the scan and filter in a streaming fashion.
            let (schema, rows) = execute_read_scan_with_filter_limit(*child, &predicate, limit, ctx)?;
            let col_names: Vec<String> = schema.columns.iter()
                .map(|c| strip_table_prefix(&c.name))
                .collect();
            let value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
            Ok(ExecuteResult {
                rows: value_rows, columns: col_names,
                rows_affected: 0, last_insert_id: 0, message: String::new(),
            })
        }
        PlanNode::Projection { columns: _, child } => {
            // Push limit into child, then project
            let mut result = execute_read_with_limit(*child, limit, ctx)?;
            result.rows.truncate(limit);
            Ok(result)
        }
        // Fallback: execute without pushdown
        other => execute_read(other, ctx),
    }
}

/// Scan with filter and early termination after `limit` matching rows.
fn execute_read_scan_with_filter_limit(
    scan_plan: PlanNode,
    predicate: &crate::sql::ast::Expr,
    limit: usize,
    ctx: &mut ReadContext,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    // For SeqScan, we can scan row-by-row and stop after `limit` matches
    if let PlanNode::SeqScan { ref table_name, ref alias, .. } = scan_plan {
        let prefix = alias.as_deref().unwrap_or(table_name);
        let info = ctx.catalog.get_table(table_name)
            .ok_or_else(|| crate::error::ForgeError::Execution(format!("table '{}' not found", table_name)))?;
        let schema = info.schema.clone();

        let prefixed = Schema::new(
            schema.columns.iter().enumerate().map(|(i, c)| {
                crate::tuple::schema::Column {
                    name: format!("{}.{}", prefix, c.name),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    column_id: i as u16,
                    auto_increment: c.auto_increment,
                    default_value: c.default_value.clone(),
                    is_primary_key: c.is_primary_key,
                        is_unique: false,
                        check_expr: None, fk_ref: None,
                }
            }).collect(),
        );

        if let Some(cidx) = ctx.clustered_indexes.get(&table_name.to_lowercase()) {
            let raw_rows = cidx.scan_all(ctx.bpm)?;
            let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
            let mut result = Vec::new();
            for raw in raw_rows {
                let values = crate::tuple::tuple::deserialize(&raw, &schema)?;
                if super::eval::eval_to_bool(predicate, &values, &prefixed)? {
                    result.push((dummy_rid, values));
                    if result.len() >= limit {
                        break;
                    }
                }
            }
            return Ok((prefixed, result));
        }

        // Heap scan with early termination (MVCC-aware)
        let mvcc_enabled = info.mvcc_enabled;
        let mut iter = crate::storage::table_iterator::TableIterator::new(info.first_page_id);
        let mut result = Vec::new();
        while let Some((rid, raw)) = iter.next(ctx.bpm)? {
            let tuple_data = if mvcc_enabled && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                let (xmin, xmax) = crate::txn::mvcc::decode_version_header(&raw);
                if let Some(ref tc) = ctx.txn_ctx {
                    if !crate::txn::mvcc::is_visible(xmin, xmax, &tc.snapshot) {
                        continue;
                    }
                } else if xmax != crate::txn::mvcc::XMAX_NONE {
                    continue;
                }
                &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
            } else {
                &raw[..]
            };
            let values = crate::tuple::tuple::deserialize(tuple_data, &schema)?;
            if super::eval::eval_to_bool(predicate, &values, &prefixed)? {
                result.push((rid, values));
                if result.len() >= limit {
                    break;
                }
            }
        }
        return Ok((prefixed, result));
    }

    // Fallback: full scan then filter then truncate
    let (schema, rows) = execute_read_scan(scan_plan, ctx)?;
    let mut filtered = filter::execute_filter(predicate, rows, &schema)?;
    filtered.truncate(limit);
    Ok((schema, filtered))
}

/// Build a minimal schema from column names (all Varchar, nullable) for sort evaluation.
fn columns_to_schema(col_names: &[String]) -> Schema {
    use crate::tuple::schema::Column;
    use crate::tuple::types::DataType;

    Schema::new(
        col_names
            .iter()
            .enumerate()
            .map(|(i, name)| Column {
                name: name.clone(),
                data_type: DataType::Varchar(255),
                nullable: true,
                column_id: i as u16,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
            })
            .collect(),
    )
}

// =========================================================================
// DML execution path — uses immutable index references for concurrency
// =========================================================================

/// Execute a DML plan (INSERT/UPDATE/DELETE) using immutable index refs.
/// This allows the caller to hold only read locks on the index collections.
pub fn execute_dml(plan: PlanNode, ctx: &mut DmlContext) -> Result<ExecuteResult> {
    match plan {
        PlanNode::Insert {
            table_name,
            columns,
            values,
            on_conflict,
        } => insert::execute_insert(
            &table_name,
            &columns,
            &values,
            ctx.bpm,
            ctx.catalog,
            ctx.indexes,
            ctx.clustered_indexes,
            ctx.auto_increment_counters,
            &mut ctx.txn_ctx,
            &on_conflict,
        ),

        PlanNode::Update {
            table_name,
            assignments,
            child,
        } => {
            let (_schema, rows) = execute_dml_scan(*child, ctx)?;
            update::execute_update(
                &table_name,
                &assignments,
                rows,
                ctx.bpm,
                ctx.catalog,
                ctx.indexes,
                &mut ctx.txn_ctx,
            )
        }

        PlanNode::Delete {
            table_name,
            child,
        } => {
            let (_schema, rows) = execute_dml_scan(*child, ctx)?;
            delete::execute_delete(
                &table_name, rows, ctx.bpm, ctx.catalog,
                ctx.indexes, ctx.clustered_indexes, &mut ctx.txn_ctx,
            )
        }

        other => Err(crate::error::ForgeError::Execution(format!(
            "non-DML plan in DML context: {:?}", other
        ))),
    }
}

/// Execute a scan-type plan node in DML mode (immutable index refs).
fn execute_dml_scan(
    plan: PlanNode,
    ctx: &mut DmlContext,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    match plan {
        PlanNode::SeqScan { table_name, alias, .. } => {
            if table_name == "__dual__" {
                let schema = Schema::new(vec![]);
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                return Ok((schema, vec![(dummy_rid, vec![])]));
            }
            let prefix = alias.as_deref().unwrap_or(&table_name);
            let (schema, rows) = if let Some(cidx) = ctx.clustered_indexes.get(&table_name.to_lowercase()) {
                let info = ctx.catalog.get_table(&table_name)
                    .ok_or_else(|| crate::error::ForgeError::Execution(format!("table '{}' not found", table_name)))?;
                let schema = info.schema.clone();
                let raw_rows = cidx.scan_all(ctx.bpm)?;
                let dummy_rid = crate::common::RID { page_id: crate::common::PageId(0), slot_id: 0 };
                let mut rows = Vec::with_capacity(raw_rows.len());
                for raw in raw_rows {
                    let values = crate::tuple::tuple::deserialize(&raw, &schema)?;
                    rows.push((dummy_rid, values));
                }
                (schema, rows)
            } else {
                seq_scan::execute_seq_scan_full(
                    &table_name, ctx.bpm, ctx.catalog, None,
                    ctx.txn_ctx.as_ref(),
                )?
            };
            let prefixed = Schema::new(
                schema.columns.iter().enumerate().map(|(i, c)| {
                    crate::tuple::schema::Column {
                        name: format!("{}.{}", prefix, c.name),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                        column_id: i as u16,
                        auto_increment: c.auto_increment,
                        default_value: c.default_value.clone(),
                        is_primary_key: c.is_primary_key,
                        is_unique: false,
                        check_expr: None, fk_ref: None,
                    }
                }).collect(),
            );
            Ok((prefixed, rows))
        }
        PlanNode::IndexScan { table_name, index_column, lookup_value, index_only } => {
            if index_only {
                index_scan::execute_index_only_scan(
                    &table_name, &index_column, &lookup_value,
                    ctx.bpm, ctx.catalog, ctx.indexes,
                )
            } else {
                index_scan::execute_index_scan(
                    &table_name, &index_column, &lookup_value,
                    ctx.bpm, ctx.catalog, ctx.indexes,
                )
            }
        }
        PlanNode::Filter { predicate, child } => {
            let (schema, rows) = execute_dml_scan(*child, ctx)?;
            let filtered = filter::execute_filter(&predicate, rows, &schema)?;
            Ok((schema, filtered))
        }
        PlanNode::NestedLoopJoin { left, right, join_type, on } => {
            let (left_schema, left_rows) = execute_dml_scan(*left, ctx)?;
            let (right_schema, right_rows) = execute_dml_scan(*right, ctx)?;
            hash_join::execute_join(
                &left_rows, &right_rows, &join_type, &on,
                &left_schema, &right_schema,
            )
        }
        PlanNode::HashJoin { left, right, join_type, on } => {
            let (left_schema, left_rows) = execute_dml_scan(*left, ctx)?;
            let (right_schema, right_rows) = execute_dml_scan(*right, ctx)?;
            hash_join::execute_join(
                &left_rows, &right_rows, &join_type, &on,
                &left_schema, &right_schema,
            )
        }
        other => Err(crate::error::ForgeError::Execution(format!(
            "unexpected plan node in DML scan context: {:?}", other
        ))),
    }
}

/// Strip "table." prefix from a column name for output.
fn strip_table_prefix(name: &str) -> String {
    if let Some(pos) = name.find('.') {
        name[pos + 1..].to_string()
    } else {
        name.to_string()
    }
}

// =========================================================================
// COUNT(*) fast-path detection and execution
// =========================================================================

/// Check if a Projection + child is a simple `SELECT COUNT(*) FROM table`
/// pattern (possibly with multiple COUNT(*) columns, but no WHERE/GROUP BY).
/// If so, execute the fast path and return the result.
///
/// Returns `None` if the pattern doesn't match, allowing fallthrough to
/// the normal execution path.
fn try_count_star_fast(
    columns: &[SelectColumn],
    child: &PlanNode,
    ctx: &mut ReadContext,
) -> Option<Result<ExecuteResult>> {
    // Child must be a bare SeqScan (no Filter wrapping it)
    let table_name = match child {
        PlanNode::SeqScan { table_name, .. } => {
            // Skip virtual dual table
            if table_name == "__dual__" {
                return None;
            }
            table_name
        }
        _ => return None,
    };

    // All projection columns must be COUNT(*) — i.e., COUNT with no args or
    // with a single "*" string arg, and no DISTINCT.
    if columns.is_empty() {
        return None;
    }

    let mut col_names = Vec::with_capacity(columns.len());
    for col in columns {
        match col {
            SelectColumn::Expr { expr, alias } => {
                if !is_count_star(expr) {
                    return None;
                }
                let name = alias.clone().unwrap_or_else(|| format_count_star_name(expr));
                col_names.push(name);
            }
            _ => return None,
        }
    }

    // Pattern matches! Use the fast path.
    let cbpm = ctx.bpm.get_cbpm();
    let result = seq_scan::count_tuples_fast(
        table_name,
        ctx.catalog,
        cbpm,
        ctx.txn_ctx.as_ref(),
    );

    Some(result.map(|count| {
        // All columns get the same count value
        let row: Vec<Value> = col_names.iter().map(|_| Value::BigInt(count)).collect();
        ExecuteResult {
            rows: vec![row],
            columns: col_names,
            rows_affected: 0,
            last_insert_id: 0,
            message: String::new(),
        }
    }))
}

/// Check if an expression is COUNT(*) — either COUNT() with no args or
/// COUNT(Literal("*")). Must not be DISTINCT.
fn is_count_star(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, distinct } => {
            if *distinct {
                return false;
            }
            let upper = name.to_uppercase();
            if upper != "COUNT" {
                return false;
            }
            // COUNT(*) or COUNT()
            args.is_empty()
                || matches!(
                    args.first(),
                    Some(Expr::Literal(LiteralValue::String(s))) if s == "*"
                )
        }
        _ => false,
    }
}

/// Build a display name for COUNT(*).
fn format_count_star_name(expr: &Expr) -> String {
    match expr {
        Expr::Function { name, args, .. } => {
            if args.is_empty() {
                format!("{}(*)", name)
            } else {
                format!("{}(*)", name)
            }
        }
        _ => "COUNT(*)".to_string(),
    }
}
