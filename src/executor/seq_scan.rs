use crate::catalog::Catalog;
use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::storage::local_bpm::LocalBpm;
use crate::storage::table_iterator::TableIterator;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::deserialize;
use crate::tuple::types::Value;

/// Execute a sequential scan of all tuples in a table.
pub fn execute_seq_scan(
    table_name: &str,
    bpm: &mut LocalBpm,
    catalog: &Catalog,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let mut iter = TableIterator::new(info.first_page_id);
    let mut rows = Vec::new();

    while let Some((rid, raw)) = iter.next(bpm)? {
        let values = deserialize(&raw, &schema)?;
        rows.push((rid, values));
    }

    Ok((schema, rows))
}
