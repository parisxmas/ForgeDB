use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::table_iterator::TableIterator;
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;

use super::executor::ExecuteResult;

/// Execute CREATE INDEX: build a B-tree index on an existing table column.
pub fn execute_create_index(
    index_name: &str,
    table_name: &str,
    columns: &[String],
    _unique: bool,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &mut Vec<(String, BTreeIndex)>,
) -> Result<ExecuteResult> {
    if columns.is_empty() {
        return Err(ForgeError::Execution("CREATE INDEX requires at least one column".into()));
    }

    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();

    // For now, only single-column indexes
    let col_name = &columns[0];
    let (col_idx, col) = schema.get_column(col_name).ok_or_else(|| {
        ForgeError::Execution(format!("column '{}' not found in table '{}'", col_name, table_name))
    })?;

    let key_type = col.data_type.clone();

    // Check if index already exists
    let index_key = format!("{}.{}", table_name.to_lowercase(), col_name.to_lowercase());
    if indexes.iter().any(|(k, _)| k.to_lowercase() == index_key) {
        return Ok(ExecuteResult {
            rows: vec![],
            columns: vec![],
            rows_affected: 0, last_insert_id: 0,
            message: format!("Index already exists on {}.{}", table_name, col_name),
        });
    }

    // Create the B-tree index
    let mut btree = BTreeIndex::create(bpm, key_type, info.table_id, col_idx)?;

    // Scan all existing rows and insert into index
    let mut iter = TableIterator::new(info.first_page_id);
    let mut count = 0u64;
    while let Some((rid, raw)) = iter.next(bpm)? {
        let values = deserialize(&raw, &schema)?;
        if col_idx < values.len() && !values[col_idx].is_null() {
            btree.insert(bpm, &values[col_idx], rid)?;
            count += 1;
        }
    }

    indexes.push((index_key, btree));

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count as usize, last_insert_id: 0,
        message: format!(
            "Index '{}' created on {}.{} ({} entries)",
            index_name, table_name, col_name, count
        ),
    })
}
