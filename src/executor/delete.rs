use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_file::HeapFile;
use crate::tuple::types::Value;
use crate::txn::TxnContext;

use super::executor::ExecuteResult;

/// Execute DELETE statement on pre-filtered rows.
pub fn execute_delete(
    table_name: &str,
    rows: Vec<(RID, Vec<Value>)>,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &[(String, BTreeIndex)],
    clustered_indexes: &std::collections::HashMap<String, ClusteredIndex>,
    txn_ctx: &mut Option<TxnContext>,
) -> Result<ExecuteResult> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let has_clustered = clustered_indexes.contains_key(&table_name.to_lowercase());

    // Check FK constraints: find child tables that reference this table
    // (child_table, child_col_idx, child_col_name, parent_col_idx, on_delete_action)
    let child_fks: Vec<(String, usize, String, usize, u8)> = {
        let mut fks = Vec::new();
        for child_info in catalog.list_tables() {
            for (ci, col) in child_info.schema.columns.iter().enumerate() {
                if let Some((ref parent_tbl, ref parent_col, ref action)) = col.fk_ref {
                    if parent_tbl.eq_ignore_ascii_case(table_name) {
                        if let Some((pi, _)) = schema.get_column(parent_col) {
                            fks.push((child_info.name.clone(), ci, col.name.clone(), pi, *action));
                        }
                    }
                }
            }
        }
        fks
    };

    let mut count = 0;

    for (_rid, values) in &rows {
        // Handle FK actions for each child FK relationship
        for (child_table, child_col_idx, child_col_name, parent_col_idx, action) in &child_fks {
            if *parent_col_idx < values.len() {
                let parent_val = &values[*parent_col_idx];
                if !parent_val.is_null() {
                    if let Some(child_info) = catalog.get_table(child_table) {
                        let child_schema = child_info.schema.clone();
                        // Find matching child rows
                        let child_heap = HeapFile::new(child_info.table_id, child_info.first_page_id);
                        let mut matching_rids = Vec::new();
                        let mut iter = crate::storage::table_iterator::TableIterator::new(child_info.first_page_id);
                        while let Ok(Some((rid, raw))) = iter.next(bpm) {
                            let tuple_data = if child_info.mvcc_enabled && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
                                &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
                            } else {
                                &raw
                            };
                            if let Ok(vals) = crate::tuple::tuple::deserialize(tuple_data, &child_schema) {
                                if *child_col_idx < vals.len() && vals[*child_col_idx] == *parent_val {
                                    matching_rids.push((rid, vals));
                                }
                            }
                        }

                        if !matching_rids.is_empty() {
                            match action {
                                0 => {
                                    // RESTRICT: reject
                                    return Err(ForgeError::Execution(format!(
                                        "foreign key constraint violated: cannot delete row, referenced by {}.{}",
                                        child_table, child_col_name
                                    )));
                                }
                                1 => {
                                    // CASCADE: delete matching child rows
                                    for (rid, _) in &matching_rids {
                                        let _ = child_heap.delete_tuple(bpm, *rid);
                                    }
                                }
                                2 => {
                                    // SET NULL: set FK column to NULL in matching child rows
                                    for (rid, mut vals) in matching_rids {
                                        if *child_col_idx < vals.len() {
                                            vals[*child_col_idx] = Value::Null;
                                            let data = crate::tuple::tuple::serialize(&vals, &child_schema)?;
                                            let update_data = if child_info.mvcc_enabled {
                                                let header = crate::txn::mvcc::encode_version_header(0, crate::txn::mvcc::XMAX_NONE);
                                                let mut full = Vec::with_capacity(crate::txn::mvcc::MVCC_HEADER_SIZE + data.len());
                                                full.extend_from_slice(&header);
                                                full.extend_from_slice(&data);
                                                full
                                            } else {
                                                data
                                            };
                                            let _ = child_heap.update_tuple(bpm, rid, &update_data);
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        // Delete from secondary indexes
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

        // Delete from clustered index (by PK value) or heap file (by RID)
        if has_clustered {
            if let Some(cidx) = clustered_indexes.get(&table_name.to_lowercase()) {
                let pk_col_idx = cidx.key_column_index;
                if pk_col_idx < values.len() {
                    cidx.delete(bpm, &values[pk_col_idx])?;
                }
            }
        }
        // Also delete from heap file (data is stored in both for now)
        let heap = HeapFile::new(info.table_id, info.first_page_id);

        // Capture old tuple data and record undo entry before physical delete
        if let Some(ref mut ctx) = txn_ctx {
            let old_data = heap.get_tuple(bpm, *_rid).unwrap_or_default();
            ctx.undo_log.push(crate::txn::UndoEntry::DeleteUndo {
                table_name: table_name.to_string(),
                rid: *_rid,
                old_xmax: 0,
                old_data,
            });
        }

        let _ = heap.delete_tuple(bpm, *_rid); // ignore error if RID is dummy

        count += 1;
    }

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count, last_insert_id: 0,
        message: format!("({} row(s) affected)", count),
    })
}
