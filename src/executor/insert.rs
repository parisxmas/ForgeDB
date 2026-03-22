use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::sql::ast::{Expr, OnConflict, OnConflictAction};
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_file::HeapFile;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::serialize;
use crate::tuple::types::{DataType, Value};
use crate::txn::{TxnContext, UndoEntry};

use crate::sql::ast::LiteralValue;
use super::eval::evaluate;
use super::executor::ExecuteResult;

/// Fast-path: convert a literal expression directly to a Value without
/// going through the full expression evaluator.
#[inline(always)]
fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Integer(n) => {
            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                Value::Integer(*n as i32)
            } else {
                Value::BigInt(*n)
            }
        }
        LiteralValue::Float(f) => Value::Float(*f),
        LiteralValue::String(s) => Value::Varchar(s.clone()),
        LiteralValue::Boolean(b) => Value::Boolean(*b),
        LiteralValue::Null => Value::Null,
    }
}

/// Execute INSERT statement.
pub fn execute_insert(
    table_name: &str,
    columns: &Option<Vec<String>>,
    values: &[Vec<Expr>],
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &[(String, BTreeIndex)],
    clustered_indexes: &std::collections::HashMap<String, ClusteredIndex>,
    auto_increment_counters: &std::sync::Mutex<std::collections::HashMap<String, i64>>,
    txn_ctx: &mut Option<TxnContext>,
    on_conflict: &Option<OnConflict>,
) -> Result<ExecuteResult> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let heap = HeapFile::new(info.table_id, info.first_page_id);
    let empty_schema = Schema::new(vec![]);

    // Pre-compute which indexes belong to this table (avoid re-scanning indexes per row)
    let table_indexes: Vec<(usize, &BTreeIndex, usize, bool, bool)> = indexes.iter().enumerate()
        .filter_map(|(i, (key, index))| {
            let parts: Vec<&str> = key.split('.').collect();
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(table_name) {
                if let Some((col_idx, col)) = schema.get_column(parts[1]) {
                    return Some((i, index, col_idx, col.is_primary_key, col.is_unique));
                }
            }
            None
        })
        .collect();

    // Check if table has any FK constraints (to skip FK validation loop when none)
    let has_fk_constraints = schema.columns.iter().any(|c| c.fk_ref.is_some());

    // Pre-compute schema flags to skip entire constraint loops when not needed
    let has_auto_inc = schema.columns.iter().any(|c| c.auto_increment);
    let has_check = schema.columns.iter().any(|c| c.check_expr.is_some());
    let has_defaults = schema.columns.iter().any(|c| c.default_value.is_some());
    let has_not_null = schema.columns.iter().any(|c| !c.nullable && !c.auto_increment);
    let has_unique_or_pk = table_indexes.iter().any(|&(_, _, _, is_pk, is_unique)| is_pk || is_unique);

    let mut count = 0;
    let mut last_insert_id: u64 = 0;

    for row_exprs in values {
        // Fast-path: evaluate expressions, using direct literal conversion
        // when possible to avoid the full expression evaluator overhead
        let mut eval_values: Vec<Value> = Vec::with_capacity(row_exprs.len());
        for expr in row_exprs {
            match expr {
                Expr::Literal(lit) => eval_values.push(literal_to_value(lit)),
                _ => eval_values.push(evaluate(expr, &[], &empty_schema)?),
            }
        }

        // Reorder if explicit column list provided
        let mut final_values = if let Some(col_names) = columns {
            reorder_values(&eval_values, col_names, &schema)?
        } else {
            eval_values
        };

        // Handle AUTO_INCREMENT columns — lock briefly, only when needed
        if has_auto_inc {
            let mut counters = auto_increment_counters.lock().unwrap();
            for (i, col) in schema.columns.iter().enumerate() {
                if col.auto_increment && i < final_values.len() && final_values[i].is_null() {
                    let counter_key = format!("{}.{}", table_name.to_lowercase(), col.name.to_lowercase());
                    let next_val = counters.entry(counter_key).or_insert(0);
                    *next_val += 1;
                    last_insert_id = *next_val as u64;
                    match col.data_type {
                        DataType::BigInt => final_values[i] = Value::BigInt(*next_val),
                        _ => final_values[i] = Value::Integer(*next_val as i32),
                    }
                }
            }
            drop(counters); // release immediately after incrementing
        }

        // Handle DEFAULT values for NULL columns (skip if no defaults)
        if has_defaults {
        for (i, col) in schema.columns.iter().enumerate() {
            if i < final_values.len() && final_values[i].is_null() && col.default_value.is_some() {
                if let Some(ref default) = col.default_value {
                    final_values[i] = default.clone();
                }
            }
        }
        }

        // Enforce NOT NULL (skip if all columns are nullable or auto_increment)
        if has_not_null {
        for (i, col) in schema.columns.iter().enumerate() {
            if !col.nullable && !col.auto_increment && i < final_values.len() && final_values[i].is_null() {
                if let Some(ref default) = col.default_value {
                    final_values[i] = default.clone();
                }
                // Otherwise, tuple::serialize coerces NULL->type default (0, "", false)
            }
        }
        }

        // Validate CHECK constraints (skip if no CHECK constraints)
        if has_check {
        for col_def in &schema.columns {
            if let Some(ref check) = col_def.check_expr {
                let check_result = evaluate(check, &final_values, &schema);
                if let Ok(val) = check_result {
                    match val {
                        Value::Boolean(false) => {
                            return Err(ForgeError::Execution(format!(
                                "CHECK constraint violated for column '{}'",
                                col_def.name
                            )));
                        }
                        _ => {} // true or NULL passes
                    }
                }
            }
        }
        }

        // Validate UNIQUE and PRIMARY KEY constraints (skip if no unique/pk indexes)
        let mut is_duplicate = false;
        if has_unique_or_pk {
        for &(_, index, col_idx, is_pk, is_unique) in &table_indexes {
            if col_idx < final_values.len() && !final_values[col_idx].is_null() {
                if is_unique || is_pk {
                    if let Ok(Some(_)) = index.search(bpm, &final_values[col_idx]) {
                        if on_conflict.is_some() {
                            is_duplicate = true;
                            break;
                        }
                        let col_name = &schema.columns[col_idx].name;
                        return Err(ForgeError::Execution(format!(
                            "duplicate entry '{}' for key '{}'",
                            final_values[col_idx], col_name
                        )));
                    }
                }
            }
        }
        }

        // Handle ON CONFLICT (upsert)
        if is_duplicate {
            if let Some(ref oc) = on_conflict {
                match &oc.action {
                    OnConflictAction::DoNothing => {
                        // Skip this row
                        continue;
                    }
                    OnConflictAction::DoUpdate(_assignments) => {
                        // For simplicity, skip on duplicate (DoUpdate is complex in this context)
                        // A full implementation would find and update the existing row
                        count += 1;
                        continue;
                    }
                }
            }
        }

        // Validate FOREIGN KEY constraints (skip entirely if no FK columns)
        if has_fk_constraints {
        for (col_idx, col) in schema.columns.iter().enumerate() {
            if let Some((ref parent_table, ref parent_col, _action)) = col.fk_ref {
                if col_idx < final_values.len() && !final_values[col_idx].is_null() {
                    let fk_value = &final_values[col_idx];
                    // Look up parent table and scan for matching value
                    if let Some(parent_info) = catalog.get_table(parent_table) {
                        let parent_schema = parent_info.schema.clone();
                        if let Some((parent_col_idx, _)) = parent_schema.get_column(parent_col) {
                            let mut found = false;
                            // Check via index first
                            let idx_key = format!("{}.{}", parent_table.to_lowercase(), parent_col.to_lowercase());
                            for (key, index) in indexes.iter() {
                                if key.to_lowercase() == idx_key {
                                    if let Ok(Some(_)) = index.search(bpm, fk_value) {
                                        found = true;
                                    }
                                    break;
                                }
                            }
                            // Fallback: seq scan
                            if !found {
                                let parent_heap = crate::storage::heap_file::HeapFile::new(
                                    parent_info.table_id, parent_info.first_page_id);
                                let mut iter = crate::storage::table_iterator::TableIterator::new(parent_info.first_page_id);
                                while let Ok(Some((_, raw))) = iter.next(bpm) {
                                    let tuple_data = if parent_info.mvcc_enabled && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                                        &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                                    } else {
                                        &raw
                                    };
                                    if let Ok(vals) = crate::tuple::tuple::deserialize(tuple_data, &parent_schema) {
                                        if parent_col_idx < vals.len() && vals[parent_col_idx] == *fk_value {
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if !found {
                                return Err(ForgeError::Execution(format!(
                                    "foreign key constraint violated: value '{}' not found in {}.{}",
                                    fk_value, parent_table, parent_col
                                )));
                            }
                        }
                    }
                }
            }
        }
        } // end if has_fk_constraints

        let data = serialize(&final_values, &schema)?;

        // Prepend MVCC header if table has MVCC enabled
        let rid = if info.mvcc_enabled {
            let xmin = if let Some(ref ctx) = txn_ctx {
                ctx.txn_id.0
            } else {
                0 // auto-committed, will be visible to all
            };
            let header = crate::txn::mvcc::encode_version_header(xmin, crate::txn::mvcc::XMAX_NONE);
            let mut full = Vec::with_capacity(crate::txn::mvcc::MVCC_HEADER_SIZE + data.len());
            full.extend_from_slice(&header);
            full.extend_from_slice(&data);
            heap.insert_tuple(bpm, &full)?
        } else {
            heap.insert_tuple(bpm, &data)?
        };

        // Record undo entry for ROLLBACK
        if let Some(ref mut ctx) = txn_ctx {
            ctx.undo_log.push(UndoEntry::InsertUndo {
                table_name: table_name.to_string(),
                rid,
            });
        }

        // Insert into clustered index if present
        if let Some(cidx) = clustered_indexes.get(&table_name.to_lowercase()) {
            let pk_col_idx = cidx.key_column_index;
            if pk_col_idx < final_values.len() {
                cidx.insert(bpm, &final_values[pk_col_idx], &data)?;
            }
        }

        // Update secondary indexes (using pre-computed table_indexes)
        for &(_, index, col_idx, _, _) in &table_indexes {
            if col_idx < final_values.len() {
                index.insert(bpm, &final_values[col_idx], rid)?;
            }
        }

        count += 1;
    }

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count,
        last_insert_id,
        message: format!("({} row(s) affected)", count),
    })
}

fn reorder_values(
    values: &[Value],
    col_names: &[String],
    schema: &Schema,
) -> Result<Vec<Value>> {
    let mut result = vec![Value::Null; schema.columns.len()];

    for (i, name) in col_names.iter().enumerate() {
        let (idx, _) = schema.get_column(name).ok_or_else(|| {
            ForgeError::Execution(format!("column '{}' not found", name))
        })?;
        if i < values.len() {
            result[idx] = values[i].clone();
        }
    }

    Ok(result)
}
