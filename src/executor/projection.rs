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
    // Check if any column has a window function
    let has_window_fn = columns.iter().any(|c| {
        if let SelectColumn::Expr { expr, .. } = c {
            contains_window_fn(expr)
        } else {
            false
        }
    });

    if has_window_fn {
        return execute_projection_with_windows(columns, rows, schema);
    }

    let mut col_names = Vec::new();
    // Store references to expressions instead of cloning them
    let mut eval_exprs: Vec<Option<&Expr>> = Vec::new();

    for col in columns {
        match col {
            SelectColumn::AllColumns(table_filter) => {
                for c in &schema.columns {
                    if let Some(tbl) = table_filter {
                        let prefix = format!("{}.", tbl.to_lowercase());
                        if !c.name.to_lowercase().starts_with(&prefix) {
                            continue;
                        }
                    }
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
                eval_exprs.push(Some(expr));
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

/// Check if an expression contains a window function
fn contains_window_fn(expr: &Expr) -> bool {
    matches!(expr, Expr::WindowFunction { .. })
}

/// Execute projection with window function evaluation
fn execute_projection_with_windows(
    columns: &[SelectColumn],
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut col_names = Vec::new();
    let mut col_exprs: Vec<Option<Expr>> = Vec::new();

    for col in columns {
        match col {
            SelectColumn::AllColumns(table_filter) => {
                for c in &schema.columns {
                    if let Some(tbl) = table_filter {
                        let prefix = format!("{}.", tbl.to_lowercase());
                        if !c.name.to_lowercase().starts_with(&prefix) {
                            continue;
                        }
                    }
                    let name = if let Some(pos) = c.name.find('.') {
                        c.name[pos + 1..].to_string()
                    } else {
                        c.name.clone()
                    };
                    col_names.push(name);
                    col_exprs.push(None);
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| expr_to_name(expr));
                col_names.push(name);
                col_exprs.push(Some(expr.clone()));
            }
        }
    }

    // Pre-compute window function values for each row
    let row_values: Vec<&Vec<Value>> = rows.iter().map(|(_, v)| v).collect();

    // For each window function column, compute all values
    let mut window_results: Vec<Option<Vec<Value>>> = vec![None; col_exprs.len()];

    for (col_idx, expr_opt) in col_exprs.iter().enumerate() {
        if let Some(Expr::WindowFunction { name, args: _args, partition_by, order_by }) = expr_opt {
            let upper = name.to_uppercase();
            let mut results = vec![Value::Null; row_values.len()];

            // Build partition groups
            let mut partitions: std::collections::HashMap<Vec<u8>, Vec<usize>> = std::collections::HashMap::new();
            for (i, vals) in row_values.iter().enumerate() {
                let key = if partition_by.is_empty() {
                    vec![0u8] // all rows in one partition
                } else {
                    let mut k = Vec::new();
                    for pb_expr in partition_by {
                        let v = evaluate(pb_expr, vals, schema).unwrap_or(Value::Null);
                        k.extend_from_slice(&v.to_sort_key_bytes());
                    }
                    k
                };
                partitions.entry(key).or_default().push(i);
            }

            // Process each partition
            for (_key, indices) in &partitions {
                // Sort within partition by ORDER BY
                let mut sorted_indices = indices.clone();
                if !order_by.is_empty() {
                    sorted_indices.sort_by(|&a, &b| {
                        for ob in order_by {
                            let va = evaluate(&ob.expr, row_values[a], schema).unwrap_or(Value::Null);
                            let vb = evaluate(&ob.expr, row_values[b], schema).unwrap_or(Value::Null);
                            let cmp = va.compare(&vb).unwrap_or(std::cmp::Ordering::Equal);
                            let cmp = if ob.ascending { cmp } else { cmp.reverse() };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                match upper.as_str() {
                    "ROW_NUMBER" => {
                        for (rank, &idx) in sorted_indices.iter().enumerate() {
                            results[idx] = Value::BigInt((rank + 1) as i64);
                        }
                    }
                    "RANK" => {
                        let mut rank = 1i64;
                        let mut i = 0;
                        while i < sorted_indices.len() {
                            let current_rank = rank;
                            let mut j = i + 1;
                            // Find ties
                            while j < sorted_indices.len() {
                                let mut equal = true;
                                for ob in order_by {
                                    let va = evaluate(&ob.expr, row_values[sorted_indices[i]], schema).unwrap_or(Value::Null);
                                    let vb = evaluate(&ob.expr, row_values[sorted_indices[j]], schema).unwrap_or(Value::Null);
                                    if va.compare(&vb) != Some(std::cmp::Ordering::Equal) {
                                        equal = false;
                                        break;
                                    }
                                }
                                if !equal { break; }
                                j += 1;
                            }
                            for k in i..j {
                                results[sorted_indices[k]] = Value::BigInt(current_rank);
                            }
                            rank += (j - i) as i64;
                            i = j;
                        }
                    }
                    "DENSE_RANK" => {
                        let mut rank = 1i64;
                        let mut i = 0;
                        while i < sorted_indices.len() {
                            let current_rank = rank;
                            let mut j = i + 1;
                            while j < sorted_indices.len() {
                                let mut equal = true;
                                for ob in order_by {
                                    let va = evaluate(&ob.expr, row_values[sorted_indices[i]], schema).unwrap_or(Value::Null);
                                    let vb = evaluate(&ob.expr, row_values[sorted_indices[j]], schema).unwrap_or(Value::Null);
                                    if va.compare(&vb) != Some(std::cmp::Ordering::Equal) {
                                        equal = false;
                                        break;
                                    }
                                }
                                if !equal { break; }
                                j += 1;
                            }
                            for k in i..j {
                                results[sorted_indices[k]] = Value::BigInt(current_rank);
                            }
                            rank += 1;
                            i = j;
                        }
                    }
                    "LAG" => {
                        // LAG(col, offset, default)
                        let lag_offset = if _args.len() >= 2 {
                            evaluate(&_args[1], &[], schema).ok()
                                .and_then(|v| match v { Value::Integer(n) => Some(n as usize), Value::BigInt(n) => Some(n as usize), _ => Some(1) })
                                .unwrap_or(1)
                        } else {
                            1
                        };
                        let default_val = if _args.len() >= 3 {
                            evaluate(&_args[2], &[], schema).unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            if pos >= lag_offset {
                                let prev_idx = sorted_indices[pos - lag_offset];
                                if let Some(arg) = _args.first() {
                                    results[idx] = evaluate(arg, row_values[prev_idx], schema).unwrap_or(default_val.clone());
                                }
                            } else {
                                results[idx] = default_val.clone();
                            }
                        }
                    }
                    "LEAD" => {
                        let lead_offset = if _args.len() >= 2 {
                            evaluate(&_args[1], &[], schema).ok()
                                .and_then(|v| match v { Value::Integer(n) => Some(n as usize), Value::BigInt(n) => Some(n as usize), _ => Some(1) })
                                .unwrap_or(1)
                        } else {
                            1
                        };
                        let default_val = if _args.len() >= 3 {
                            evaluate(&_args[2], &[], schema).unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            if pos + lead_offset < sorted_indices.len() {
                                let next_idx = sorted_indices[pos + lead_offset];
                                if let Some(arg) = _args.first() {
                                    results[idx] = evaluate(arg, row_values[next_idx], schema).unwrap_or(default_val.clone());
                                }
                            } else {
                                results[idx] = default_val.clone();
                            }
                        }
                    }
                    "NTILE" => {
                        let n = if let Some(arg) = _args.first() {
                            evaluate(arg, &[], schema).ok()
                                .and_then(|v| match v { Value::Integer(n) => Some(n as usize), Value::BigInt(n) => Some(n as usize), _ => Some(1) })
                                .unwrap_or(1)
                        } else {
                            1
                        };
                        let total = sorted_indices.len();
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            let tile = if n == 0 { 1 } else { (pos * n / total) + 1 };
                            results[idx] = Value::BigInt(tile as i64);
                        }
                    }
                    "SUM" => {
                        // SUM(col) OVER (PARTITION BY ... ORDER BY ...)
                        // Running sum within the partition
                        if let Some(arg) = _args.first() {
                            if order_by.is_empty() {
                                // No ORDER BY: full partition sum
                                let mut total = Value::Float(0.0);
                                for &idx in &sorted_indices {
                                    let v = evaluate(arg, row_values[idx], schema).unwrap_or(Value::Null);
                                    if !v.is_null() {
                                        total = total.add(&v).unwrap_or(total.clone());
                                    }
                                }
                                for &idx in &sorted_indices {
                                    results[idx] = total.clone();
                                }
                            } else {
                                // With ORDER BY: running sum
                                let mut running = Value::Float(0.0);
                                for &idx in &sorted_indices {
                                    let v = evaluate(arg, row_values[idx], schema).unwrap_or(Value::Null);
                                    if !v.is_null() {
                                        running = running.add(&v).unwrap_or(running.clone());
                                    }
                                    results[idx] = running.clone();
                                }
                            }
                        }
                    }
                    "AVG" => {
                        // AVG(col) OVER (PARTITION BY ...)
                        if let Some(arg) = _args.first() {
                            let mut total = 0.0f64;
                            let mut count = 0usize;
                            for &idx in &sorted_indices {
                                let v = evaluate(arg, row_values[idx], schema).unwrap_or(Value::Null);
                                match v {
                                    Value::Integer(n) => { total += n as f64; count += 1; }
                                    Value::BigInt(n) => { total += n as f64; count += 1; }
                                    Value::Float(f) => { total += f; count += 1; }
                                    _ => {}
                                }
                            }
                            let avg = if count > 0 { Value::Float(total / count as f64) } else { Value::Null };
                            for &idx in &sorted_indices {
                                results[idx] = avg.clone();
                            }
                        }
                    }
                    "COUNT" => {
                        // COUNT(*) OVER or COUNT(col) OVER
                        let cnt = sorted_indices.len() as i64;
                        for &idx in &sorted_indices {
                            results[idx] = Value::BigInt(cnt);
                        }
                    }
                    "MIN" => {
                        if let Some(arg) = _args.first() {
                            let mut min_val = Value::Null;
                            for &idx in &sorted_indices {
                                let v = evaluate(arg, row_values[idx], schema).unwrap_or(Value::Null);
                                if !v.is_null() {
                                    if min_val.is_null() || v.compare(&min_val) == Some(std::cmp::Ordering::Less) {
                                        min_val = v;
                                    }
                                }
                            }
                            for &idx in &sorted_indices {
                                results[idx] = min_val.clone();
                            }
                        }
                    }
                    "MAX" => {
                        if let Some(arg) = _args.first() {
                            let mut max_val = Value::Null;
                            for &idx in &sorted_indices {
                                let v = evaluate(arg, row_values[idx], schema).unwrap_or(Value::Null);
                                if !v.is_null() {
                                    if max_val.is_null() || v.compare(&max_val) == Some(std::cmp::Ordering::Greater) {
                                        max_val = v;
                                    }
                                }
                            }
                            for &idx in &sorted_indices {
                                results[idx] = max_val.clone();
                            }
                        }
                    }
                    _ => {
                        // Unknown window function, leave as NULL
                    }
                }
            }

            window_results[col_idx] = Some(results);
        }
    }

    // Build result rows
    let mut result_rows = Vec::new();
    for (row_idx, (_, values)) in rows.iter().enumerate() {
        let mut row = Vec::new();
        let mut all_col_idx = 0;
        for (col_idx, expr_opt) in col_exprs.iter().enumerate() {
            match expr_opt {
                None => {
                    if all_col_idx < values.len() {
                        row.push(values[all_col_idx].clone());
                    }
                    all_col_idx += 1;
                }
                Some(Expr::WindowFunction { .. }) => {
                    if let Some(ref results) = window_results[col_idx] {
                        row.push(results[row_idx].clone());
                    } else {
                        row.push(Value::Null);
                    }
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
        Expr::WindowFunction { name, .. } => name.clone(),
        _ => "?".to_string(),
    }
}
