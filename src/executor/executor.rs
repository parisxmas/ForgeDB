use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::Result;
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::planner::plan::PlanNode;
use crate::storage::BufferPoolManager;
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::{
    aggregate, create_index, create_table, delete, drop_table, filter, hash_join, index_scan,
    insert, limit, projection, seq_scan, sort, update,
};

/// Context passed to executors.
pub struct ExecutorContext<'a> {
    pub bpm: &'a mut BufferPoolManager,
    pub catalog: &'a mut Catalog,
    pub indexes: &'a mut Vec<(String, BTreeIndex)>,
    pub clustered_indexes: &'a mut std::collections::HashMap<String, ClusteredIndex>,
    pub auto_increment_counters: &'a mut std::collections::HashMap<String, i64>,
}

/// Result of executing a SQL statement.
#[derive(Debug)]
pub struct ExecuteResult {
    pub rows: Vec<Vec<Value>>,
    pub columns: Vec<String>,
    pub rows_affected: usize,
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
                    rows: vec![], columns: vec![], rows_affected: 0,
                    message: "OK (table already exists)".into(),
                });
            }
            let result = create_table::execute_create_table(&table_name, &columns, ctx.bpm, ctx.catalog)?;
            // Auto-create clustered B+ tree index on PRIMARY KEY columns
            let has_pk = columns.iter().any(|c| c.is_primary_key);
            if has_pk {
                let pk_col_idx = columns.iter().position(|c| c.is_primary_key).unwrap();
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
                    rows: vec![], columns: vec![], rows_affected: 0,
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
        } => insert::execute_insert(
            &table_name,
            &columns,
            &values,
            ctx.bpm,
            ctx.catalog,
            ctx.indexes,
            ctx.clustered_indexes,
            ctx.auto_increment_counters,
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
            )
        }

        PlanNode::Delete {
            table_name,
            child,
        } => {
            let (_schema, rows) = execute_scan(*child, ctx)?;
            delete::execute_delete(&table_name, rows, ctx.bpm, ctx.catalog, ctx.indexes, ctx.clustered_indexes)
        }

        // Query pipeline: SeqScan / Filter / Join optionally wrapped in Projection / Sort / Limit
        PlanNode::Limit { count, child } => {
            let mut result = execute(*child, ctx)?;
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
                    return Ok(ExecuteResult { rows: result_rows, columns: col_names, rows_affected: 0, message: String::new() });
                }
                let (col_names, projected_rows) = projection::execute_projection(&proj_cols, &rows_with_rid, &schema)?;
                return Ok(ExecuteResult { rows: projected_rows, columns: col_names, rows_affected: 0, message: String::new() });
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
                    rows_affected: 0,
                    message: String::new(),
                })
            } else {
                let (col_names, projected_rows) =
                    projection::execute_projection(&columns, &rows, &schema)?;
                Ok(ExecuteResult {
                    rows: projected_rows,
                    columns: col_names,
                    rows_affected: 0,
                    message: String::new(),
                })
            }
        }

        // Bare scan at top level (SELECT * FROM table)
        PlanNode::SeqScan { .. }
        | PlanNode::IndexScan { .. }
        | PlanNode::Filter { .. }
        | PlanNode::NestedLoopJoin { .. } => {
            let (schema, rows) = execute_scan(plan, ctx)?;
            let col_names: Vec<String> = schema.columns.iter()
                .map(|c| strip_table_prefix(&c.name))
                .collect();
            let value_rows: Vec<Vec<Value>> = rows.into_iter().map(|(_, v)| v).collect();
            Ok(ExecuteResult {
                rows: value_rows,
                columns: col_names,
                rows_affected: 0,
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
        PlanNode::SeqScan { table_name, alias } => {
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
                seq_scan::execute_seq_scan(&table_name, ctx.bpm, ctx.catalog)?
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
                    }
                }).collect(),
            );
            Ok((prefixed, rows))
        }
        PlanNode::IndexScan {
            table_name,
            index_column,
            lookup_value,
        } => index_scan::execute_index_scan(
            &table_name,
            &index_column,
            &lookup_value,
            ctx.bpm,
            ctx.catalog,
            ctx.indexes,
        ),
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
            hash_join::execute_hash_join(
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
            })
            .collect(),
    )
}

/// Strip "table." prefix from a column name for output.
fn strip_table_prefix(name: &str) -> String {
    if let Some(pos) = name.find('.') {
        name[pos + 1..].to_string()
    } else {
        name.to_string()
    }
}
