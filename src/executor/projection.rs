use crate::common::RID;
use crate::error::Result;
use crate::sql::ast::{Expr, SelectColumn};
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::eval::evaluate;

/// Project columns from rows, returning (column_names, projected_rows).
pub fn execute_projection(
    columns: &[SelectColumn],
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut col_names = Vec::new();
    let mut eval_exprs: Vec<Option<Expr>> = Vec::new();

    for col in columns {
        match col {
            SelectColumn::AllColumns(table_filter) => {
                for c in &schema.columns {
                    // If table qualifier given (e.g., t.*), only include that table's columns
                    if let Some(tbl) = table_filter {
                        let prefix = format!("{}.", tbl.to_lowercase());
                        if !c.name.to_lowercase().starts_with(&prefix) {
                            continue;
                        }
                    }
                    // Strip table prefix for output: "t.term_id" -> "term_id"
                    let name = if let Some(pos) = c.name.find('.') {
                        c.name[pos + 1..].to_string()
                    } else {
                        c.name.clone()
                    };
                    col_names.push(name);
                    eval_exprs.push(None);
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_to_name(expr));
                col_names.push(name);
                eval_exprs.push(Some(expr.clone()));
            }
        }
    }

    let mut result_rows = Vec::new();
    for (_, values) in rows {
        let mut row = Vec::new();
        let mut all_col_idx = 0;
        for eval in &eval_exprs {
            match eval {
                None => {
                    // All columns: use index directly
                    if all_col_idx < values.len() {
                        row.push(values[all_col_idx].clone());
                    }
                    all_col_idx += 1;
                }
                Some(expr) => {
                    row.push(evaluate(expr, values, schema)?);
                }
            }
        }
        result_rows.push(row);
    }

    Ok((col_names, result_rows))
}

fn expr_to_name(expr: &Expr) -> String {
    match expr {
        Expr::ColumnRef { column, .. } => column.clone(),
        Expr::Function { name, .. } => format!("{}(?)", name),
        _ => "?".to_string(),
    }
}
