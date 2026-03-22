use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::table_iterator::TableIterator;
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;

use super::executor::ExecuteResult;

/// Execute CREATE INDEX: build a B-tree index on an existing table column(s).
///
/// Supports:
/// - Single-column indexes: `CREATE INDEX idx ON table(col)`
/// - Composite indexes: `CREATE INDEX idx ON table(col1, col2)`
///   Composite keys are created by concatenating the sort-key bytes of all columns.
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

    // Resolve all column indices
    let mut col_indices: Vec<usize> = Vec::with_capacity(columns.len());
    for col_name in columns {
        let (col_idx, _) = schema.get_column(col_name).ok_or_else(|| {
            ForgeError::Execution(format!("column '{}' not found in table '{}'", col_name, table_name))
        })?;
        col_indices.push(col_idx);
    }

    let first_col_name = &columns[0];
    let (first_col_idx, first_col) = schema.get_column(first_col_name).ok_or_else(|| {
        ForgeError::Execution(format!("column '{}' not found in table '{}'", first_col_name, table_name))
    })?;
    let key_type = first_col.data_type.clone();

    // Build the index key: for single-column use "table.col", for composite use "table.col1.col2"
    let index_key = if columns.len() == 1 {
        format!("{}.{}", table_name.to_lowercase(), first_col_name.to_lowercase())
    } else {
        let col_parts: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
        format!("{}.{}", table_name.to_lowercase(), col_parts.join("."))
    };

    // Check if index already exists
    if indexes.iter().any(|(k, _)| k.to_lowercase() == index_key) {
        return Ok(ExecuteResult {
            rows: vec![],
            columns: vec![],
            rows_affected: 0, last_insert_id: 0,
            message: format!("Index already exists: {}", index_key),
        });
    }

    // Create the B-tree index with composite column info
    let mut btree = if columns.len() > 1 {
        BTreeIndex::create_composite(
            bpm,
            key_type,
            info.table_id,
            first_col_idx,
            columns.to_vec(),
            Vec::new(), // no include columns via CREATE INDEX ... ON (cols) syntax
        )?
    } else {
        BTreeIndex::create(bpm, key_type, info.table_id, first_col_idx)?
    };

    // Scan all existing rows and insert into index
    let mut iter = TableIterator::new(info.first_page_id);
    let mut count = 0u64;
    while let Some((rid, raw)) = iter.next(bpm)? {
        let values = deserialize(&raw, &schema)?;

        if columns.len() == 1 {
            // Single-column index: use the column value directly
            if first_col_idx < values.len() && !values[first_col_idx].is_null() {
                btree.insert(bpm, &values[first_col_idx], rid)?;
                count += 1;
            }
        } else {
            // Composite index: create a composite key from all column values
            let composite_key = build_composite_key(&values, &col_indices);
            if let Some(key) = composite_key {
                btree.insert(bpm, &key, rid)?;
                count += 1;
            }
        }
    }

    indexes.push((index_key, btree));

    let col_list = columns.join(", ");
    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count as usize, last_insert_id: 0,
        message: format!(
            "Index '{}' created on {}({}) ({} entries)",
            index_name, table_name, col_list, count
        ),
    })
}

/// Build a composite key from multiple column values by concatenating their
/// sort-key bytes with length prefixes for unambiguous decoding.
fn build_composite_key(values: &[Value], col_indices: &[usize]) -> Option<Value> {
    let mut composite_bytes = Vec::new();
    for &idx in col_indices {
        if idx >= values.len() || values[idx].is_null() {
            return None; // Skip rows with NULL in any key column
        }
        let part = values[idx].to_sort_key_bytes();
        // Prefix each part with its length (4 bytes LE) for unambiguous decoding
        composite_bytes.extend_from_slice(&(part.len() as u32).to_le_bytes());
        composite_bytes.extend_from_slice(&part);
    }
    // Store composite key as a Varchar to leverage existing BTree infrastructure
    // The sort-key bytes are already in correct sort order
    Some(Value::Varchar(
        composite_bytes.iter().map(|b| *b as char).collect::<String>()
    ))
}
