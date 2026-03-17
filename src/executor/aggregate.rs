use crate::common::RID;
use crate::error::{ForgeError, Result};
use crate::sql::ast::{Expr, LiteralValue, SelectColumn};
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::eval::evaluate;

/// Check if any select columns contain aggregate functions.
pub fn has_aggregates(columns: &[SelectColumn]) -> bool {
    columns.iter().any(|c| match c {
        SelectColumn::Expr { expr, .. } => expr_has_aggregate(expr),
        _ => false,
    })
}

fn expr_has_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, .. } => {
            let upper = name.to_uppercase();
            matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            )
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        _ => false,
    }
}

/// Execute aggregate projection over rows.
/// Returns (column_names, result_rows) - one row for no GROUP BY.
pub fn execute_aggregate(
    columns: &[SelectColumn],
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let mut col_names = Vec::new();
    let mut result_values = Vec::new();

    for col in columns {
        match col {
            SelectColumn::Expr { expr, alias } => {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| expr_display_name(expr));
                col_names.push(name);
                let val = evaluate_aggregate(expr, rows, schema)?;
                result_values.push(val);
            }
            SelectColumn::AllColumns(_) => {
                return Err(ForgeError::Execution(
                    "cannot use * with aggregate functions".into(),
                ));
            }
        }
    }

    Ok((col_names, vec![result_values]))
}

fn evaluate_aggregate(
    expr: &Expr,
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<Value> {
    match expr {
        Expr::Function { name, args } => {
            let upper = name.to_uppercase();
            match upper.as_str() {
                "COUNT" => {
                    if args.is_empty()
                        || matches!(
                            args.first(),
                            Some(Expr::Literal(LiteralValue::String(s))) if s == "*"
                        )
                    {
                        Ok(Value::BigInt(rows.len() as i64))
                    } else {
                        let mut count = 0i64;
                        for (_, vals) in rows {
                            let v = evaluate(&args[0], vals, schema)?;
                            if !v.is_null() {
                                count += 1;
                            }
                        }
                        Ok(Value::BigInt(count))
                    }
                }
                "SUM" => {
                    if args.is_empty() {
                        return Err(ForgeError::Execution("SUM requires argument".into()));
                    }
                    let mut sum = Value::Null;
                    for (_, vals) in rows {
                        let v = evaluate(&args[0], vals, schema)?;
                        if v.is_null() {
                            continue;
                        }
                        sum = if sum.is_null() { v } else { sum.add(&v)? };
                    }
                    Ok(sum)
                }
                "AVG" => {
                    if args.is_empty() {
                        return Err(ForgeError::Execution("AVG requires argument".into()));
                    }
                    let mut sum = 0.0f64;
                    let mut count = 0i64;
                    for (_, vals) in rows {
                        let v = evaluate(&args[0], vals, schema)?;
                        match v {
                            Value::Integer(n) => {
                                sum += n as f64;
                                count += 1;
                            }
                            Value::BigInt(n) => {
                                sum += n as f64;
                                count += 1;
                            }
                            Value::Float(f) => {
                                sum += f;
                                count += 1;
                            }
                            Value::Null => {}
                            _ => {
                                return Err(ForgeError::Execution(
                                    "AVG requires numeric values".into(),
                                ))
                            }
                        }
                    }
                    if count == 0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Float(sum / count as f64))
                    }
                }
                "MIN" => {
                    if args.is_empty() {
                        return Err(ForgeError::Execution("MIN requires argument".into()));
                    }
                    let mut min = Value::Null;
                    for (_, vals) in rows {
                        let v = evaluate(&args[0], vals, schema)?;
                        if v.is_null() {
                            continue;
                        }
                        if min.is_null() {
                            min = v;
                        } else if let Some(std::cmp::Ordering::Less) = v.compare(&min) {
                            min = v;
                        }
                    }
                    Ok(min)
                }
                "MAX" => {
                    if args.is_empty() {
                        return Err(ForgeError::Execution("MAX requires argument".into()));
                    }
                    let mut max = Value::Null;
                    for (_, vals) in rows {
                        let v = evaluate(&args[0], vals, schema)?;
                        if v.is_null() {
                            continue;
                        }
                        if max.is_null() {
                            max = v;
                        } else if let Some(std::cmp::Ordering::Greater) = v.compare(&max) {
                            max = v;
                        }
                    }
                    Ok(max)
                }
                _ => Err(ForgeError::Execution(format!(
                    "unknown aggregate function: {}",
                    name
                ))),
            }
        }
        // Non-aggregate: evaluate against first row
        other => {
            if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(other, &rows[0].1, schema)
            }
        }
    }
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Function { name, args } => {
            if args.is_empty() {
                format!("{}(*)", name)
            } else {
                format!("{}(?)", name)
            }
        }
        Expr::ColumnRef { column, .. } => column.clone(),
        _ => "?".to_string(),
    }
}
