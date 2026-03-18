use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::sql::ast::Assignment;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_file::HeapFile;
use crate::tuple::tuple::serialize;
use crate::tuple::types::Value;

use super::eval::evaluate;
use super::executor::ExecuteResult;

/// Execute UPDATE statement on pre-filtered rows.
pub fn execute_update(
    table_name: &str,
    assignments: &[Assignment],
    rows: Vec<(RID, Vec<Value>)>,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &mut Vec<(String, BTreeIndex)>,
) -> Result<ExecuteResult> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
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

        // Delete old index entries
        for (key, index) in indexes.iter_mut() {
            let parts: Vec<&str> = key.split('.').collect();
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(table_name) {
                if let Some((col_idx, _)) = schema.get_column(parts[1]) {
                    if col_idx < values.len() {
                        let _ = index.delete(bpm, &values[col_idx]);
                    }
                }
            }
        }

        // Serialize and update
        let data = serialize(&new_values, &schema)?;
        let new_rid = heap.update_tuple(bpm, *rid, &data)?;

        // Insert new index entries
        for (key, index) in indexes.iter_mut() {
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
