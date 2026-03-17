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
            SelectColumn::AllColumns(_) => {
                for c in &schema.columns {
                    col_names.push(c.name.clone());
                    eval_exprs.push(None); // marker for "use index directly"
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
        Expr::ColumnRef { table, column } => {
            if let Some(t) = table {
                format!("{}.{}", t, column)
            } else {
                column.clone()
            }
        }
        _ => "?".to_string(),
    }
}
