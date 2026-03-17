use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::sql::ast::Expr;
use crate::storage::BufferPoolManager;
use crate::storage::heap_file::HeapFile;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;

use super::eval::evaluate;

/// Execute an index scan: look up by key and return matching rows.
pub fn execute_index_scan(
    table_name: &str,
    index_column: &str,
    lookup_value: &Expr,
    bpm: &mut BufferPoolManager,
    catalog: &Catalog,
    indexes: &[(String, BTreeIndex)],
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();

    // Find the index
    let index_key = format!(
        "{}.{}",
        table_name.to_lowercase(),
        index_column.to_lowercase()
    );
    let index = indexes
        .iter()
        .find(|(k, _)| k.to_lowercase() == index_key)
        .map(|(_, idx)| idx)
        .ok_or_else(|| {
            ForgeError::Execution(format!("index not found for {}", index_key))
        })?;

    // Evaluate the lookup value using an empty tuple/schema since it should be a literal
    let empty_schema = Schema::new(vec![]);
    let key = evaluate(lookup_value, &[], &empty_schema)?;

    // Search index
    let rid = index.search(bpm, &key)?;

    let mut rows = Vec::new();
    if let Some(rid) = rid {
        let heap = HeapFile::new(info.table_id, info.first_page_id);
        let raw = heap.get_tuple(bpm, rid)?;
        let values = deserialize(&raw, &schema)?;
        rows.push((rid, values));
    }

    Ok((schema, rows))
}
