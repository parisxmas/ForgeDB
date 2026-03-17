use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::storage::BufferPoolManager;
use crate::storage::heap_file::HeapFile;
use crate::tuple::types::Value;

use super::executor::ExecuteResult;

/// Execute DELETE statement on pre-filtered rows.
pub fn execute_delete(
    table_name: &str,
    rows: Vec<(RID, Vec<Value>)>,
    bpm: &mut BufferPoolManager,
    catalog: &Catalog,
    indexes: &mut Vec<(String, BTreeIndex)>,
    clustered_indexes: &mut std::collections::HashMap<String, ClusteredIndex>,
) -> Result<ExecuteResult> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let has_clustered = clustered_indexes.contains_key(&table_name.to_lowercase());

    let mut count = 0;

    for (_rid, values) in &rows {
        // Delete from secondary indexes
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

        // Delete from clustered index (by PK value) or heap file (by RID)
        if has_clustered {
            if let Some(cidx) = clustered_indexes.get_mut(&table_name.to_lowercase()) {
                let pk_col_idx = cidx.key_column_index;
                if pk_col_idx < values.len() {
                    cidx.delete(bpm, &values[pk_col_idx])?;
                }
            }
        }
        // Also delete from heap file (data is stored in both for now)
        let heap = HeapFile::new(info.table_id, info.first_page_id);
        let _ = heap.delete_tuple(bpm, *_rid); // ignore error if RID is dummy

        count += 1;
    }

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count,
        message: format!("({} row(s) affected)", count),
    })
}
