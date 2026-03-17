use crate::catalog::Catalog;
use crate::error::Result;
use crate::index::BTreeIndex;

use super::executor::ExecuteResult;

/// Execute DROP TABLE statement.
pub fn execute_drop_table(
    table_name: &str,
    catalog: &mut Catalog,
    indexes: &mut Vec<(String, BTreeIndex)>,
) -> Result<ExecuteResult> {
    catalog.drop_table(table_name)?;

    // Remove any indexes for this table
    let prefix = format!("{}.", table_name.to_lowercase());
    indexes.retain(|(k, _)| !k.to_lowercase().starts_with(&prefix));

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: 0, last_insert_id: 0,
        message: format!("Table '{}' dropped.", table_name),
    })
}
