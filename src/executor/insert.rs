use crate::catalog::Catalog;
use crate::error::{ForgeError, Result};
use crate::index::{BTreeIndex, ClusteredIndex};
use crate::sql::ast::Expr;
use crate::storage::local_bpm::LocalBpm;
use crate::storage::heap_file::HeapFile;
use crate::tuple::schema::Schema;
use crate::tuple::tuple::serialize;
use crate::tuple::types::{DataType, Value};

use super::eval::evaluate;
use super::executor::ExecuteResult;

/// Execute INSERT statement.
pub fn execute_insert(
    table_name: &str,
    columns: &Option<Vec<String>>,
    values: &[Vec<Expr>],
    bpm: &mut LocalBpm,
    catalog: &Catalog,
    indexes: &mut Vec<(String, BTreeIndex)>,
    clustered_indexes: &mut std::collections::HashMap<String, ClusteredIndex>,
    auto_increment_counters: &mut std::collections::HashMap<String, i64>,
) -> Result<ExecuteResult> {
    let info = catalog
        .get_table(table_name)
        .ok_or_else(|| ForgeError::Execution(format!("table '{}' not found", table_name)))?;

    let schema = info.schema.clone();
    let heap = HeapFile::new(info.table_id, info.first_page_id);
    let empty_schema = Schema::new(vec![]);

    let mut count = 0;
    let mut last_insert_id: u64 = 0;

    for row_exprs in values {
        let mut eval_values: Vec<Value> = Vec::new();
        for expr in row_exprs {
            eval_values.push(evaluate(expr, &[], &empty_schema)?);
        }

        // Reorder if explicit column list provided
        let mut final_values = if let Some(col_names) = columns {
            reorder_values(&eval_values, col_names, &schema)?
        } else {
            eval_values
        };

        // Handle AUTO_INCREMENT columns
        for (i, col) in schema.columns.iter().enumerate() {
            if col.auto_increment && i < final_values.len() && final_values[i].is_null() {
                let counter_key = format!("{}.{}", table_name.to_lowercase(), col.name.to_lowercase());
                let next_val = auto_increment_counters.entry(counter_key).or_insert(0);
                *next_val += 1;
                last_insert_id = *next_val as u64;
                match col.data_type {
                    DataType::BigInt => final_values[i] = Value::BigInt(*next_val),
                    _ => final_values[i] = Value::Integer(*next_val as i32),
                }
            }
        }

        // Handle DEFAULT values for NULL columns
        for (i, col) in schema.columns.iter().enumerate() {
            if i < final_values.len() && final_values[i].is_null() && col.default_value.is_some() {
                if let Some(ref default) = col.default_value {
                    final_values[i] = default.clone();
                }
            }
        }

        let data = serialize(&final_values, &schema)?;
        let rid = heap.insert_tuple(bpm, &data)?;

        // Insert into clustered index if present
        if let Some(cidx) = clustered_indexes.get_mut(&table_name.to_lowercase()) {
            let pk_col_idx = cidx.key_column_index;
            if pk_col_idx < final_values.len() {
                cidx.insert(bpm, &final_values[pk_col_idx], &data)?;
            }
        }

        // Update secondary indexes
        for (key, index) in indexes.iter_mut() {
            let parts: Vec<&str> = key.split('.').collect();
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case(table_name) {
                if let Some((col_idx, _)) = schema.get_column(parts[1]) {
                    if col_idx < final_values.len() {
                        index.insert(bpm, &final_values[col_idx], rid)?;
                    }
                }
            }
        }

        count += 1;
    }

    Ok(ExecuteResult {
        rows: vec![],
        columns: vec![],
        rows_affected: count,
        last_insert_id,
        message: format!("({} row(s) affected)", count),
    })
}

fn reorder_values(
    values: &[Value],
    col_names: &[String],
    schema: &Schema,
) -> Result<Vec<Value>> {
    let mut result = vec![Value::Null; schema.columns.len()];

    for (i, name) in col_names.iter().enumerate() {
        let (idx, _) = schema.get_column(name).ok_or_else(|| {
            ForgeError::Execution(format!("column '{}' not found", name))
        })?;
        if i < values.len() {
            result[idx] = values[i].clone();
        }
    }

    Ok(result)
}
