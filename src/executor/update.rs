use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::sql::ast::Assignment;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_file::HeapFile;
use crate::tuple::tuple::serialize;
use crate::tuple::types::Value;
use crate::txn::TxnContext;

use super::eval::evaluate;
use super::executor::ExecuteResult;

/// Execute UPDATE statement on pre-filtered rows.
pub fn execute_update(
    table_name: &str,
    assignments: &[Assignment],
    rows: Vec<(RID, Vec<Value>)>,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &[(String, BTreeIndex)],
    txn_ctx: &mut Option<TxnContext>,
) -> Result<ExecuteResult> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let mvcc_enabled = info.mvcc_enabled;
    let heap = HeapFile::new(info.table_id, info.first_page_id);

    let mut count = 0;

    for (rid, values) in &rows {
        let mut new_values = values.clone();

        // Apply assignments
        for assignment in assignments {
            let (col_idx, _) = schema.get_column(&assignment.column).ok_or_else(|| {
                ForgeError::Execution(format!("column '{}' not found", assignment.column))
            })?;
            let new_val = evaluate(&assignment.value, values, &schema)?;
            new_values[col_idx] = new_val;
        }

        // Validate CHECK constraints on updated values
        for col in &schema.columns {
            if let Some(ref check) = col.check_expr {
                let check_result = evaluate(check, &new_values, &schema);
                if let Ok(crate::tuple::types::Value::Boolean(false)) = check_result {
                    return Err(ForgeError::Execution(format!(
                        "CHECK constraint violated for column '{}'",
                        col.name
                    )));
                }
            }
        }

        // Validate FK constraints on updated values
        for (col_idx, col) in schema.columns.iter().enumerate() {
            if let Some((ref parent_table, ref parent_col, _action)) = col.fk_ref {
                if col_idx < new_values.len() && !new_values[col_idx].is_null() {
                    // Check if old value changed
                    if col_idx < values.len() && values[col_idx] == new_values[col_idx] {
                        continue; // FK column unchanged, skip validation
                    }
                    let fk_value = &new_values[col_idx];
                    if let Some(parent_info) = catalog.get_table(parent_table) {
                        let parent_schema = parent_info.schema.clone();
                        if let Some((parent_col_idx, _)) = parent_schema.get_column(parent_col) {
                            let mut found = false;
                            let mut iter = crate::storage::table_iterator::TableIterator::new(parent_info.first_page_id);
                            while let Ok(Some((_, raw))) = iter.next(bpm) {
                                let tuple_data = if parent_info.mvcc_enabled && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                                    &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                                } else { &raw };
                                if let Ok(vals) = crate::tuple::tuple::deserialize(tuple_data, &parent_schema) {
                                    if parent_col_idx < vals.len() && vals[parent_col_idx] == *fk_value {
                                        found = true;
                                        break;
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

        // Delete old index entries
        for (key, index) in indexes.iter() {
            let parts: Vec<&str> = key.split('.').collect();
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(table_name) {
                if let Some((col_idx, _)) = schema.get_column(parts[1]) {
                    if col_idx < values.len() {
                        let _ = index.delete(bpm, &values[col_idx]);
                    }
                }
            }
        }

        // Capture old tuple data for undo BEFORE modifying
        let old_data = if txn_ctx.is_some() {
            heap.get_tuple(bpm, *rid).ok()
        } else {
            None
        };

        // Serialize and update (with MVCC header if enabled)
        let data = serialize(&new_values, &schema)?;
        let update_data = if mvcc_enabled {
            let xmin = if let Some(ref ctx) = txn_ctx {
                ctx.txn_id.0
            } else {
                0
            };
            let header = crate::txn::mvcc::encode_version_header(xmin, crate::txn::mvcc::XMAX_NONE);
            let mut full = Vec::with_capacity(crate::txn::mvcc::MVCC_HEADER_SIZE + data.len());
            full.extend_from_slice(&header);
            full.extend_from_slice(&data);
            full
        } else {
            data
        };
        let new_rid = heap.update_tuple(bpm, *rid, &update_data)?;

        // Record undo entry with old data
        if let Some(ref mut ctx) = txn_ctx {
            ctx.undo_log.push(crate::txn::UndoEntry::UpdateUndo {
                table_name: table_name.to_string(),
                old_rid: *rid,
                old_xmax: 0,
                new_rid,
                old_data: old_data.unwrap_or_default(),
            });
        }

        // Insert new index entries
        for (key, index) in indexes.iter() {
            let parts: Vec<&str> = key.split('.').collect();
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(table_name) {
                if let Some((col_idx, _)) = schema.get_column(parts[1]) {
                    if col_idx < new_values.len() {
                        index.insert(bpm, &new_values[col_idx], new_rid)?;
                    }
                }
            }
        }

        count += 1;
    }

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count, last_insert_id: 0,
        message: format!("({} row(s) affected)", count),
    })
}
