use crate::catalog::Catalog;
use crate::common::PageId;
use crate::error::Result;
use crate::sql::ast::ColumnDef;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_page;
use crate::tuple::schema::{Column, Schema};

use super::executor::ExecuteResult;

/// Execute CREATE TABLE statement.
pub fn execute_create_table(
    table_name: &str,
    column_defs: &[ColumnDef],
    bpm: &mut LocalBpm,
    catalog: &mut Catalog,
) -> Result<ExecuteResult> {
    // Build schema from column definitions
    let columns: Vec<Column> = column_defs
        .iter()
        .enumerate()
        .map(|(i, cd)| {
            // Evaluate DEFAULT expression to a Value
            let default_value = cd.default_value.as_ref().and_then(|expr| {
                use crate::executor::eval::evaluate;
                use crate::tuple::schema::Schema as S;
                let empty = S::new(vec![]);
                evaluate(expr, &[], &empty).ok()
            });
            Column {
                name: cd.name.clone(),
                data_type: cd.data_type.clone(),
                nullable: cd.nullable,
                column_id: i as u16,
                auto_increment: cd.auto_increment,
                default_value,
                is_primary_key: cd.is_primary_key,
                is_unique: cd.is_unique,
                check_expr: cd.check_expr.clone(),
                fk_ref: cd.references.as_ref().map(|r| {
                    let action = match r.on_delete {
                        crate::sql::ast::FkAction::Restrict => 0u8,
                        crate::sql::ast::FkAction::Cascade => 1u8,
                        crate::sql::ast::FkAction::SetNull => 2u8,
                    };
                    (r.table.clone(), r.column.clone(), action)
                }),
            }
        })
        .collect();
    let schema = Schema::new(columns);

    // Allocate first heap page
    let page_id = bpm.new_page()?;
    {
        let page = bpm.get_page_mut(page_id);
        heap_page::init(&mut page.data);
    }
    let _ = bpm.unpin_page(page_id, true);

    // Register in catalog
    catalog.create_table(table_name, schema, PageId(page_id.0))?;

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: 0, last_insert_id: 0,
        message: format!("Table '{}' created.", table_name),
    })
}
