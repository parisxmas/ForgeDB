use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::index::BTreeIndex;
use crate::sql::ast::Expr;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_file::HeapFile;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;

use super::eval::evaluate;

/// Execute an index scan: look up by key and return matching rows.
/// For regular index scans, looks up the RID in the index then fetches the
/// full tuple from the heap. For index-only scans (when `index_only` is true
/// in the plan), the heap lookup can be skipped if the index covers all
/// needed columns.
pub fn execute_index_scan(
    table_name: &str,
    index_column: &str,
    lookup_value: &Expr,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &[(String, BTreeIndex)],
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();

    // Find the index (exact match or composite prefix match)
    let index_key = format!(
        "{}.{}",
        table_name.to_lowercase(),
        index_column.to_lowercase()
    );
    let index = indexes
        .iter()
        .find(|(k, _)| {
            let kl = k.to_lowercase();
            kl == index_key || kl.starts_with(&format!("{}.", index_key))
        })
        .map(|(_, idx)| idx)
        .ok_or_else(|| {
            ForgeError::Execution(format!("index not found for {}", index_key))
        })?;

    // Evaluate the lookup value and coerce to match the index key type
    let empty_schema = Schema::new(vec![]);
    let key = evaluate(lookup_value, &[], &empty_schema)?;
    let key = crate::tuple::tuple::coerce_value_pub(&key, &index.key_type);

    // Search index
    let rid = index.search(bpm, &key)?;

    let mut rows = Vec::new();
    if let Some(rid) = rid {
        let heap = HeapFile::new(info.table_id, info.first_page_id);
        let raw = heap.get_tuple(bpm, rid)?;
        // Strip MVCC header if table uses MVCC
        let tuple_data = if info.mvcc_enabled && raw.len() >= crate::txn::mvcc::MVCC_HEADER_SIZE {
            &raw[crate::txn::mvcc::MVCC_HEADER_SIZE..]
        } else {
            &raw
        };
        let values = deserialize(tuple_data, &schema)?;
        rows.push((rid, values));
    }

    Ok((schema, rows))
}

/// Execute an index-only scan: return results directly from the index
/// without accessing the heap file. This is possible when the query only
/// needs columns that are present in the index (key + included columns).
///
/// Returns a single-column schema containing only the indexed column.
pub fn execute_index_only_scan(
    table_name: &str,
    index_column: &str,
    lookup_value: &Expr,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &[(String, BTreeIndex)],
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    // Find the index (exact match or composite prefix match)
    let index_key = format!(
        "{}.{}",
        table_name.to_lowercase(),
        index_column.to_lowercase()
    );
    let index = indexes
        .iter()
        .find(|(k, _)| {
            let kl = k.to_lowercase();
            kl == index_key || kl.starts_with(&format!("{}.", index_key))
        })
        .map(|(_, idx)| idx)
        .ok_or_else(|| {
            ForgeError::Execution(format!("index not found for {}", index_key))
        })?;

    // Evaluate the lookup value and coerce to match the index key type
    let empty_schema = Schema::new(vec![]);
    let key = evaluate(lookup_value, &[], &empty_schema)?;
    let key = crate::tuple::tuple::coerce_value_pub(&key, &index.key_type);

    // Search index — for index-only scan we just need to know the key exists
    let rid = index.search(bpm, &key)?;

    // Build a minimal schema for the indexed column only
    let col = info.schema.columns.iter()
        .find(|c| c.name.to_lowercase() == index_column.to_lowercase())
        .cloned()
        .ok_or_else(|| {
            ForgeError::Execution(format!("column '{}' not found", index_column))
        })?;

    let index_only_schema = Schema::new(vec![crate::tuple::schema::Column {
        name: col.name.clone(),
        data_type: col.data_type.clone(),
        nullable: col.nullable,
        column_id: 0,
        auto_increment: false,
        default_value: None,
        is_primary_key: col.is_primary_key,
        is_unique: col.is_unique,
        check_expr: None,
        fk_ref: None,
    }]);

    let mut rows = Vec::new();
    if rid.is_some() {
        // The key exists in the index — return the lookup value directly
        // without fetching the tuple from the heap.
        let dummy_rid = crate::common::RID {
            page_id: crate::common::PageId(0),
            slot_id: 0,
        };
        rows.push((dummy_rid, vec![key]));
    }

    Ok((index_only_schema, rows))
}
