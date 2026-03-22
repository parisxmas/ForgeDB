use std::collections::HashMap;

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
            if matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            ) {
                return true;
            }
            // Not an aggregate function itself, but check args recursively
            false
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_aggregate(expr),
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(op) = operand {
                if expr_has_aggregate(op) {
                    return true;
                }
            }
            for (cond, result) in when_clauses {
                if expr_has_aggregate(cond) || expr_has_aggregate(result) {
                    return true;
                }
            }
            if let Some(el) = else_result {
                if expr_has_aggregate(el) {
                    return true;
                }
            }
            false
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_has_aggregate(inner),
        Expr::Like { expr, pattern } | Expr::NotLike { expr, pattern } => {
            expr_has_aggregate(expr) || expr_has_aggregate(pattern)
        }
        Expr::In { expr, list } | Expr::NotIn { expr, list } => {
            expr_has_aggregate(expr) || list.iter().any(|e| expr_has_aggregate(e))
        }
        Expr::Between { expr, low, high } => {
            expr_has_aggregate(expr) || expr_has_aggregate(low) || expr_has_aggregate(high)
        }
        Expr::Cast { expr, .. } => expr_has_aggregate(expr),
        Expr::Concat { left, right } => {
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

/// Execute aggregate projection with GROUP BY.
///
/// Groups rows by evaluating `group_exprs`, computes aggregates per group,
/// and optionally filters groups with `having`.
///
/// Returns (column_names, result_rows).
pub fn execute_group_by(
    group_exprs: &[Expr],
    having: &Option<Expr>,
    select_columns: &[SelectColumn],
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    // 1. Group rows by evaluating group_exprs to produce a key per row.
    //    Instead of cloning all rows into group buckets, we store only row indices.
    //    This avoids doubling memory consumption.
    let mut group_order: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
    let mut key_to_index: HashMap<Vec<u8>, usize> = HashMap::new();

    for (i, (_rid, vals)) in rows.iter().enumerate() {
        let mut group_key = Vec::with_capacity(group_exprs.len());
        for ge in group_exprs {
            let v = evaluate(ge, vals, schema)?;
            group_key.push(v);
        }

        let serialized = serialize_group_key(&group_key);

        if let Some(&idx) = key_to_index.get(&serialized) {
            group_order[idx].1.push(i);
        } else {
            let idx = group_order.len();
            key_to_index.insert(serialized, idx);
            group_order.push((group_key, vec![i]));
        }
    }
    // Free the key-to-index map immediately — no longer needed
    drop(key_to_index);

    if group_order.is_empty() {
        let col_names = build_column_names(select_columns);
        return Ok((col_names, vec![]));
    }

    // 2. For each group, build a temporary slice of row references and evaluate.
    let col_names = build_column_names(select_columns);
    let mut result_rows = Vec::new();

    for (_group_key, row_indices) in &group_order {
        // Build group rows once — used for both SELECT columns and HAVING.
        // Use borrows via index access instead of cloning when possible.
        let group_rows: Vec<(RID, &[Value])> = row_indices
            .iter()
            .map(|&i| (rows[i].0, rows[i].1.as_slice()))
            .collect();

        // HAVING check first: if the group will be filtered out, skip the
        // expensive SELECT column evaluation entirely
        if let Some(having_expr) = having {
            let having_val = evaluate_aggregate_expr_ref(having_expr, &group_rows, schema)?;
            match having_val {
                Value::Boolean(true) => {}
                Value::Boolean(false) | Value::Null => continue,
                Value::Integer(n) => {
                    if n == 0 {
                        continue;
                    }
                }
                Value::BigInt(n) => {
                    if n == 0 {
                        continue;
                    }
                }
                _ => continue,
            }
        }

        let mut row_values = Vec::with_capacity(select_columns.len());
        for col in select_columns {
            match col {
                SelectColumn::Expr { expr, .. } => {
                    let val = evaluate_aggregate_ref(expr, &group_rows, schema)?;
                    row_values.push(val);
                }
                SelectColumn::AllColumns(_) => {
                    return Err(ForgeError::Execution(
                        "cannot use * with GROUP BY".into(),
                    ));
                }
            }
        }

        result_rows.push(row_values);
    }

    Ok((col_names, result_rows))
}

/// Evaluate an expression that may contain aggregates, used for HAVING clauses.
/// This handles aggregate functions as well as boolean/comparison expressions
/// containing aggregates.
fn evaluate_aggregate_expr(
    expr: &Expr,
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<Value> {
    match expr {
        Expr::Function { name, args, distinct } => {
            let upper = name.to_uppercase();
            if matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
            ) {
                return compute_aggregate(&upper, args, *distinct, rows, schema);
            }
            // Non-aggregate function: evaluate against first row
            if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(expr, &rows[0].1, schema)
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = evaluate_aggregate_expr(left, rows, schema)?;
            let r = evaluate_aggregate_expr(right, rows, schema)?;
            // Use the same binary op evaluation as eval.rs
            crate::executor::eval::eval_binary_op_values(&l, op, &r)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = evaluate_aggregate_expr(inner, rows, schema)?;
            match op {
                crate::sql::ast::UnaryOperator::Not => match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ForgeError::Execution("NOT requires boolean".into())),
                },
                crate::sql::ast::UnaryOperator::Neg => match val {
                    Value::Integer(n) => Ok(Value::Integer(-n)),
                    Value::BigInt(n) => Ok(Value::BigInt(-n)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ForgeError::Execution("negation requires number".into())),
                },
            }
        }
        Expr::IsNull(inner) => {
            let val = evaluate_aggregate_expr(inner, rows, schema)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = evaluate_aggregate_expr(inner, rows, schema)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if let Some(op) = operand {
                let op_val = evaluate_aggregate_expr(op, rows, schema)?;
                for (cond, result) in when_clauses {
                    let cond_val = evaluate_aggregate_expr(cond, rows, schema)?;
                    if !op_val.is_null() && !cond_val.is_null() {
                        if let Some(std::cmp::Ordering::Equal) = op_val.compare(&cond_val) {
                            return evaluate_aggregate_expr(result, rows, schema);
                        }
                    }
                }
            } else {
                for (cond, result) in when_clauses {
                    let cond_val = evaluate_aggregate_expr(cond, rows, schema)?;
                    match cond_val {
                        Value::Boolean(true) => {
                            return evaluate_aggregate_expr(result, rows, schema);
                        }
                        _ => continue,
                    }
                }
            }
            if let Some(el) = else_result {
                evaluate_aggregate_expr(el, rows, schema)
            } else {
                Ok(Value::Null)
            }
        }
        // Non-aggregate expression: evaluate against first row of the group
        other => {
            if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(other, &rows[0].1, schema)
            }
        }
    }
}

fn evaluate_aggregate(
    expr: &Expr,
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<Value> {
    match expr {
        Expr::Function { name, args, distinct } => {
            let upper = name.to_uppercase();
            match upper.as_str() {
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                    compute_aggregate(&upper, args, *distinct, rows, schema)
                }
                _ => {
                    // Non-aggregate function: evaluate against first row
                    if rows.is_empty() {
                        Ok(Value::Null)
                    } else {
                        evaluate(expr, &rows[0].1, schema)
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            // If either side contains an aggregate, evaluate via aggregate_expr
            if expr_has_aggregate(left) || expr_has_aggregate(right) {
                evaluate_aggregate_expr(expr, rows, schema)
            } else {
                // Pure non-aggregate binary expression
                if rows.is_empty() {
                    Ok(Value::Null)
                } else {
                    evaluate(expr, &rows[0].1, schema)
                }
            }
        }
        Expr::Case { .. } => {
            if expr_has_aggregate(expr) {
                evaluate_aggregate_expr(expr, rows, schema)
            } else if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(expr, &rows[0].1, schema)
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

/// Core aggregate computation shared by execute_aggregate and execute_group_by.
fn compute_aggregate(
    func_name: &str,
    args: &[Expr],
    distinct: bool,
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<Value> {
    match func_name {
        "COUNT" => {
            if args.is_empty()
                || matches!(
                    args.first(),
                    Some(Expr::Literal(LiteralValue::String(s))) if s == "*"
                )
            {
                Ok(Value::BigInt(rows.len() as i64))
            } else if distinct {
                // COUNT(DISTINCT expr) — use HashSet with binary keys for O(1) lookup
                let mut seen = std::collections::HashSet::new();
                for (_, vals) in rows {
                    let v = evaluate(&args[0], vals, schema)?;
                    if v.is_null() {
                        continue;
                    }
                    seen.insert(serialize_group_key(&[v]));
                }
                Ok(Value::BigInt(seen.len() as i64))
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
            if distinct {
                let mut seen = std::collections::HashSet::new();
                let mut sum = Value::Null;
                for (_, vals) in rows {
                    let v = evaluate(&args[0], vals, schema)?;
                    if v.is_null() {
                        continue;
                    }
                    let key = serialize_group_key(&[v.clone()]);
                    if seen.insert(key) {
                        sum = if sum.is_null() { v } else { sum.add(&v)? };
                    }
                }
                Ok(sum)
            } else {
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
        }
        "AVG" => {
            if args.is_empty() {
                return Err(ForgeError::Execution("AVG requires argument".into()));
            }
            let mut sum = 0.0f64;
            let mut count = 0i64;
            if distinct {
                let mut seen = std::collections::HashSet::new();
                for (_, vals) in rows {
                    let v = evaluate(&args[0], vals, schema)?;
                    if v.is_null() {
                        continue;
                    }
                    let key = serialize_group_key(&[v.clone()]);
                    if seen.insert(key) {
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
                            _ => {
                                return Err(ForgeError::Execution(
                                    "AVG requires numeric values".into(),
                                ))
                            }
                        }
                    }
                }
            } else {
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
            func_name
        ))),
    }
}

// =========================================================================
// Reference-based aggregate helpers — avoid cloning Vec<Value> per group row.
// These take &[(RID, &[Value])] instead of &[(RID, Vec<Value>)].
// =========================================================================

/// Like `evaluate_aggregate` but works on borrowed row slices.
fn evaluate_aggregate_ref(
    expr: &Expr,
    rows: &[(RID, &[Value])],
    schema: &Schema,
) -> Result<Value> {
    match expr {
        Expr::Function { name, args, distinct } => {
            let upper = name.to_uppercase();
            match upper.as_str() {
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                    compute_aggregate_ref(&upper, args, *distinct, rows, schema)
                }
                _ => {
                    if rows.is_empty() {
                        Ok(Value::Null)
                    } else {
                        evaluate(expr, rows[0].1, schema)
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            if expr_has_aggregate(left) || expr_has_aggregate(right) {
                evaluate_aggregate_expr_ref(expr, rows, schema)
            } else if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(expr, rows[0].1, schema)
            }
        }
        Expr::Case { .. } => {
            if expr_has_aggregate(expr) {
                evaluate_aggregate_expr_ref(expr, rows, schema)
            } else if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(expr, rows[0].1, schema)
            }
        }
        other => {
            if rows.is_empty() {
                Ok(Value::Null)
            } else {
                evaluate(other, rows[0].1, schema)
            }
        }
    }
}

/// Like `evaluate_aggregate_expr` but works on borrowed row slices.
fn evaluate_aggregate_expr_ref(
    expr: &Expr,
    rows: &[(RID, &[Value])],
    schema: &Schema,
) -> Result<Value> {
    match expr {
        Expr::Function { name, args, distinct } => {
            let upper = name.to_uppercase();
            if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                return compute_aggregate_ref(&upper, args, *distinct, rows, schema);
            }
            if rows.is_empty() { Ok(Value::Null) } else { evaluate(expr, rows[0].1, schema) }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = evaluate_aggregate_expr_ref(left, rows, schema)?;
            let r = evaluate_aggregate_expr_ref(right, rows, schema)?;
            crate::executor::eval::eval_binary_op_values(&l, op, &r)
        }
        Expr::UnaryOp { op, expr: inner } => {
            let val = evaluate_aggregate_expr_ref(inner, rows, schema)?;
            match op {
                crate::sql::ast::UnaryOperator::Not => match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ForgeError::Execution("NOT requires boolean".into())),
                },
                crate::sql::ast::UnaryOperator::Neg => match val {
                    Value::Integer(n) => Ok(Value::Integer(-n)),
                    Value::BigInt(n) => Ok(Value::BigInt(-n)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ForgeError::Execution("negation requires number".into())),
                },
            }
        }
        Expr::IsNull(inner) => {
            let val = evaluate_aggregate_expr_ref(inner, rows, schema)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = evaluate_aggregate_expr_ref(inner, rows, schema)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                let op_val = evaluate_aggregate_expr_ref(op, rows, schema)?;
                for (cond, result) in when_clauses {
                    let cond_val = evaluate_aggregate_expr_ref(cond, rows, schema)?;
                    if !op_val.is_null() && !cond_val.is_null() {
                        if let Some(std::cmp::Ordering::Equal) = op_val.compare(&cond_val) {
                            return evaluate_aggregate_expr_ref(result, rows, schema);
                        }
                    }
                }
            } else {
                for (cond, result) in when_clauses {
                    let cond_val = evaluate_aggregate_expr_ref(cond, rows, schema)?;
                    match cond_val {
                        Value::Boolean(true) => {
                            return evaluate_aggregate_expr_ref(result, rows, schema);
                        }
                        _ => continue,
                    }
                }
            }
            if let Some(el) = else_result {
                evaluate_aggregate_expr_ref(el, rows, schema)
            } else {
                Ok(Value::Null)
            }
        }
        other => {
            if rows.is_empty() { Ok(Value::Null) } else { evaluate(other, rows[0].1, schema) }
        }
    }
}

/// Like `compute_aggregate` but works on borrowed row slices.
fn compute_aggregate_ref(
    func_name: &str,
    args: &[Expr],
    distinct: bool,
    rows: &[(RID, &[Value])],
    schema: &Schema,
) -> Result<Value> {
    match func_name {
        "COUNT" => {
            if args.is_empty()
                || matches!(args.first(), Some(Expr::Literal(LiteralValue::String(s))) if s == "*")
            {
                Ok(Value::BigInt(rows.len() as i64))
            } else if distinct {
                let mut seen = std::collections::HashSet::new();
                for (_, vals) in rows {
                    let v = evaluate(&args[0], *vals, schema)?;
                    if v.is_null() { continue; }
                    seen.insert(serialize_group_key(&[v]));
                }
                Ok(Value::BigInt(seen.len() as i64))
            } else {
                let mut count = 0i64;
                for (_, vals) in rows {
                    let v = evaluate(&args[0], *vals, schema)?;
                    if !v.is_null() { count += 1; }
                }
                Ok(Value::BigInt(count))
            }
        }
        "SUM" => {
            if args.is_empty() { return Err(ForgeError::Execution("SUM requires argument".into())); }
            if distinct {
                let mut seen = std::collections::HashSet::new();
                let mut sum = Value::Null;
                for (_, vals) in rows {
                    let v = evaluate(&args[0], *vals, schema)?;
                    if v.is_null() { continue; }
                    let key = serialize_group_key(&[v.clone()]);
                    if seen.insert(key) {
                        sum = if sum.is_null() { v } else { sum.add(&v)? };
                    }
                }
                Ok(sum)
            } else {
                let mut sum = Value::Null;
                for (_, vals) in rows {
                    let v = evaluate(&args[0], *vals, schema)?;
                    if v.is_null() { continue; }
                    sum = if sum.is_null() { v } else { sum.add(&v)? };
                }
                Ok(sum)
            }
        }
        "AVG" => {
            if args.is_empty() { return Err(ForgeError::Execution("AVG requires argument".into())); }
            let mut sum = 0.0f64;
            let mut count = 0i64;
            if distinct {
                let mut seen = std::collections::HashSet::new();
                for (_, vals) in rows {
                    let v = evaluate(&args[0], *vals, schema)?;
                    if v.is_null() { continue; }
                    let key = serialize_group_key(&[v.clone()]);
                    if seen.insert(key) {
                        match v {
                            Value::Integer(n) => { sum += n as f64; count += 1; }
                            Value::BigInt(n) => { sum += n as f64; count += 1; }
                            Value::Float(f) => { sum += f; count += 1; }
                            _ => return Err(ForgeError::Execution("AVG requires numeric values".into())),
                        }
                    }
                }
            } else {
                for (_, vals) in rows {
                    let v = evaluate(&args[0], *vals, schema)?;
                    match v {
                        Value::Integer(n) => { sum += n as f64; count += 1; }
                        Value::BigInt(n) => { sum += n as f64; count += 1; }
                        Value::Float(f) => { sum += f; count += 1; }
                        Value::Null => {}
                        _ => return Err(ForgeError::Execution("AVG requires numeric values".into())),
                    }
                }
            }
            if count == 0 { Ok(Value::Null) } else { Ok(Value::Float(sum / count as f64)) }
        }
        "MIN" => {
            if args.is_empty() { return Err(ForgeError::Execution("MIN requires argument".into())); }
            let mut min = Value::Null;
            for (_, vals) in rows {
                let v = evaluate(&args[0], *vals, schema)?;
                if v.is_null() { continue; }
                if min.is_null() {
                    min = v;
                } else if let Some(std::cmp::Ordering::Less) = v.compare(&min) {
                    min = v;
                }
            }
            Ok(min)
        }
        "MAX" => {
            if args.is_empty() { return Err(ForgeError::Execution("MAX requires argument".into())); }
            let mut max = Value::Null;
            for (_, vals) in rows {
                let v = evaluate(&args[0], *vals, schema)?;
                if v.is_null() { continue; }
                if max.is_null() {
                    max = v;
                } else if let Some(std::cmp::Ordering::Greater) = v.compare(&max) {
                    max = v;
                }
            }
            Ok(max)
        }
        _ => Err(ForgeError::Execution(format!("unknown aggregate function: {}", func_name))),
    }
}

/// Compare two Values for equality (used for DISTINCT tracking).
fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => a.compare(b) == Some(std::cmp::Ordering::Equal),
    }
}

/// Serialize a row of Values to a compact binary key for use in HashSets/HashMaps.
/// Used by DISTINCT, GROUP BY, and UNION deduplication.
/// Each value is prefixed with a type tag so different types don't collide.
pub fn serialize_row_key(key: &[Value]) -> Vec<u8> {
    serialize_group_key(key)
}

/// Serialize a group key (Vec<Value>) to bytes for use as a HashMap key.
/// Each value is prefixed with a type tag so different types don't collide.
fn serialize_group_key(key: &[Value]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(key.len() * 16);
    for v in key {
        match v {
            Value::Null => buf.push(0x00),
            Value::Boolean(b) => {
                buf.push(0x01);
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Integer(n) => {
                buf.push(0x02);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::BigInt(n) => {
                buf.push(0x03);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(0x04);
                buf.extend_from_slice(&f.to_bits().to_le_bytes());
            }
            Value::Varchar(s) => {
                buf.push(0x05);
                let lower = s.to_lowercase();
                buf.extend_from_slice(&(lower.len() as u32).to_le_bytes());
                buf.extend_from_slice(lower.as_bytes());
            }
            Value::DateTime(epoch) => {
                buf.push(0x06);
                buf.extend_from_slice(&epoch.to_le_bytes());
            }
            Value::Decimal(v, scale) => {
                buf.push(0x07);
                buf.extend_from_slice(&v.to_le_bytes());
                buf.push(*scale);
            }
            Value::Date(d) => {
                buf.push(0x08);
                buf.extend_from_slice(&d.to_le_bytes());
            }
            Value::Time(t) => {
                buf.push(0x09);
                buf.extend_from_slice(&t.to_le_bytes());
            }
            Value::Binary(b) => {
                buf.push(0x0A);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Json(s) => {
                buf.push(0x0B);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Uuid(s) => {
                buf.push(0x0C);
                buf.extend_from_slice(s.as_bytes());
            }
        }
    }
    buf
}

/// Build column names from select columns.
fn build_column_names(select_columns: &[SelectColumn]) -> Vec<String> {
    select_columns
        .iter()
        .map(|col| match col {
            SelectColumn::Expr { expr, alias } => alias
                .clone()
                .unwrap_or_else(|| expr_display_name(expr)),
            SelectColumn::AllColumns(_) => "*".to_string(),
        })
        .collect()
}

fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Function { name, args, distinct } => {
            if args.is_empty() {
                format!("{}(*)", name)
            } else if *distinct {
                format!("{}(DISTINCT ?)", name)
            } else {
                format!("{}(?)", name)
            }
        }
        Expr::ColumnRef { column, .. } => column.clone(),
        _ => "?".to_string(),
    }
}

// =========================================================================
// Streaming GROUP BY — accumulates aggregates per-group during scan,
// avoiding the intermediate Vec<(RID, Vec<Value>)> materialization.
// =========================================================================

/// Per-column accumulator for streaming aggregation.
#[derive(Clone)]
enum Accumulator {
    /// COUNT(*) or COUNT(expr)
    Count(i64),
    /// SUM — accumulates as Value to handle int/float polymorphism
    Sum(Value),
    /// AVG — sum as f64 + count
    Avg(f64, i64),
    /// MIN
    Min(Value),
    /// MAX
    Max(Value),
    /// Non-aggregate column — stores the first value seen
    FirstValue(Value),
}

/// Descriptor for each select column in streaming mode.
#[derive(Clone)]
enum StreamColDesc {
    /// COUNT(*) — no argument evaluation needed
    CountStar,
    /// Aggregate with column index in schema (or expression)
    Agg { func: String, arg: Expr, distinct: bool },
    /// Non-aggregate expression (e.g. the group key column)
    Plain(Expr),
}

/// Check if a GROUP BY plan is suitable for streaming aggregation.
/// Returns true if all select columns are either:
/// - Simple aggregate functions (COUNT, SUM, AVG, MIN, MAX) with non-DISTINCT
/// - Column references (group key columns)
/// - Literals
pub fn can_stream_group_by(select_columns: &[SelectColumn]) -> bool {
    for col in select_columns {
        match col {
            SelectColumn::AllColumns(_) => return false,
            SelectColumn::Expr { expr, .. } => {
                if !can_stream_expr(expr) {
                    return false;
                }
            }
        }
    }
    true
}

fn can_stream_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Function { name, args, distinct } => {
            let upper = name.to_uppercase();
            if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                // DISTINCT aggregates need HashSet tracking — skip streaming for now
                if *distinct { return false; }
                // Arguments must be simple (column refs or literals)
                args.iter().all(|a| matches!(a, Expr::ColumnRef { .. } | Expr::Literal(_)))
            } else {
                false
            }
        }
        Expr::ColumnRef { .. } | Expr::Literal(_) => true,
        Expr::BinaryOp { left, right, .. } => can_stream_expr(left) && can_stream_expr(right),
        Expr::Cast { expr, .. } => can_stream_expr(expr),
        _ => false,
    }
}

/// Execute a streaming GROUP BY: scans rows and accumulates aggregates in a
/// single pass. This avoids materializing all rows into a Vec first.
///
/// `rows` is iterated exactly once. For each row, the group key is computed,
/// and all accumulators for that group are updated incrementally.
///
/// Falls back to the non-streaming path when HAVING is present, since HAVING
/// requires re-evaluating aggregate expressions against the group's raw rows.
pub fn execute_streaming_group_by(
    group_exprs: &[Expr],
    having: &Option<Expr>,
    select_columns: &[SelectColumn],
    rows: &[(RID, Vec<Value>)],
    schema: &Schema,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    // HAVING requires aggregate evaluation against raw group rows, which
    // streaming doesn't preserve. Fall back to the non-streaming path.
    if having.is_some() {
        return execute_group_by(group_exprs, having, select_columns, rows, schema);
    }

    // Build column descriptors
    let mut col_descs = Vec::with_capacity(select_columns.len());
    for col in select_columns {
        match col {
            SelectColumn::AllColumns(_) => {
                return Err(ForgeError::Execution("cannot use * with GROUP BY".into()));
            }
            SelectColumn::Expr { expr, .. } => {
                col_descs.push(classify_stream_col(expr));
            }
        }
    }

    // Groups: maintain insertion order for deterministic output
    let mut group_order: Vec<(Vec<Value>, Vec<Accumulator>)> = Vec::new();
    let mut key_to_index: HashMap<Vec<u8>, usize> = HashMap::new();

    let dummy_rid = RID { page_id: crate::common::PageId(0), slot_id: 0 };

    for (_rid, vals) in rows {
        // Compute group key
        let mut group_key = Vec::with_capacity(group_exprs.len());
        for ge in group_exprs {
            let v = evaluate(ge, vals, schema)?;
            group_key.push(v);
        }

        let serialized = serialize_group_key(&group_key);

        let group_idx = if let Some(&idx) = key_to_index.get(&serialized) {
            idx
        } else {
            let idx = group_order.len();
            // Initialize accumulators
            let accs = init_accumulators(&col_descs, vals, schema)?;
            key_to_index.insert(serialized, idx);
            group_order.push((group_key, accs));
            // Skip update for the first row — already initialized
            continue;
        };

        // Update accumulators for existing group
        let accs = &mut group_order[group_idx].1;
        for (i, desc) in col_descs.iter().enumerate() {
            update_accumulator(&mut accs[i], desc, vals, schema)?;
        }
    }
    drop(key_to_index);

    let col_names = build_column_names(select_columns);

    if group_order.is_empty() {
        return Ok((col_names, vec![]));
    }

    // Finalize and apply HAVING filter
    let mut result_rows = Vec::new();

    for (group_key, accs) in &group_order {
        let row: Vec<Value> = accs.iter().map(|acc| finalize_accumulator(acc)).collect();

        // HAVING check
        if let Some(having_expr) = having {
            // For HAVING we need to evaluate against the group.
            // Build a temporary row with the finalized values and evaluate.
            // Map against the output schema built from select_columns.
            let having_schema = Schema::new(
                col_names.iter().enumerate().map(|(i, name)| {
                    crate::tuple::schema::Column {
                        name: name.clone(),
                        data_type: crate::tuple::types::DataType::Varchar(255),
                        nullable: true,
                        column_id: i as u16,
                        auto_increment: false,
                        default_value: None,
                        is_primary_key: false,
                        is_unique: false,
                        check_expr: None, fk_ref: None,
                    }
                }).collect(),
            );

            // For HAVING, we need to re-evaluate aggregates. Use the original
            // non-streaming path for the HAVING expression evaluation.
            // Since we don't keep all rows, we fall back for HAVING.
            // For now, evaluate the HAVING expr against the finalized row using
            // the aggregate eval with a single-row group.
            let group_rows = vec![(dummy_rid, row.as_slice())];
            let having_val = evaluate_aggregate_expr_ref(having_expr, &group_rows, &having_schema);
            match having_val {
                Ok(Value::Boolean(true)) => {}
                Ok(Value::Boolean(false)) | Ok(Value::Null) => continue,
                Ok(Value::Integer(n)) if n == 0 => continue,
                Ok(Value::BigInt(n)) if n == 0 => continue,
                Err(_) => {
                    // If HAVING evaluation fails against the output schema,
                    // try evaluating against the input schema.
                    // This handles cases like HAVING COUNT(*) > 5 where
                    // the HAVING expression references aggregate functions.
                    // Fall back to non-streaming path.
                    return execute_group_by(group_exprs, having, select_columns, rows, schema);
                }
                _ => continue,
            }
        }

        result_rows.push(row);
    }

    Ok((col_names, result_rows))
}

fn classify_stream_col(expr: &Expr) -> StreamColDesc {
    match expr {
        Expr::Function { name, args, distinct } => {
            let upper = name.to_uppercase();
            if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                if args.is_empty() || matches!(args.first(), Some(Expr::Literal(LiteralValue::String(s))) if s == "*") {
                    return StreamColDesc::CountStar;
                }
                return StreamColDesc::Agg {
                    func: upper,
                    arg: args[0].clone(),
                    distinct: *distinct,
                };
            }
            StreamColDesc::Plain(expr.clone())
        }
        _ => StreamColDesc::Plain(expr.clone()),
    }
}

fn init_accumulators(
    descs: &[StreamColDesc],
    first_vals: &[Value],
    schema: &Schema,
) -> Result<Vec<Accumulator>> {
    let mut accs = Vec::with_capacity(descs.len());
    for desc in descs {
        match desc {
            StreamColDesc::CountStar => {
                accs.push(Accumulator::Count(1));
            }
            StreamColDesc::Agg { func, arg, .. } => {
                let v = evaluate(arg, first_vals, schema)?;
                match func.as_str() {
                    "COUNT" => {
                        accs.push(Accumulator::Count(if v.is_null() { 0 } else { 1 }));
                    }
                    "SUM" => {
                        accs.push(Accumulator::Sum(if v.is_null() { Value::Null } else { v }));
                    }
                    "AVG" => {
                        let (s, c) = match &v {
                            Value::Integer(n) => (*n as f64, 1i64),
                            Value::BigInt(n) => (*n as f64, 1i64),
                            Value::Float(f) => (*f, 1i64),
                            Value::Null => (0.0, 0i64),
                            _ => (0.0, 0i64),
                        };
                        accs.push(Accumulator::Avg(s, c));
                    }
                    "MIN" => {
                        accs.push(Accumulator::Min(v));
                    }
                    "MAX" => {
                        accs.push(Accumulator::Max(v));
                    }
                    _ => accs.push(Accumulator::FirstValue(v)),
                }
            }
            StreamColDesc::Plain(expr) => {
                let v = evaluate(expr, first_vals, schema)?;
                accs.push(Accumulator::FirstValue(v));
            }
        }
    }
    Ok(accs)
}

fn update_accumulator(
    acc: &mut Accumulator,
    desc: &StreamColDesc,
    vals: &[Value],
    schema: &Schema,
) -> Result<()> {
    match (acc, desc) {
        (Accumulator::Count(ref mut c), StreamColDesc::CountStar) => {
            *c += 1;
        }
        (Accumulator::Count(ref mut c), StreamColDesc::Agg { arg, .. }) => {
            let v = evaluate(arg, vals, schema)?;
            if !v.is_null() {
                *c += 1;
            }
        }
        (Accumulator::Sum(ref mut sum), StreamColDesc::Agg { arg, .. }) => {
            let v = evaluate(arg, vals, schema)?;
            if !v.is_null() {
                if sum.is_null() {
                    *sum = v;
                } else {
                    *sum = sum.add(&v)?;
                }
            }
        }
        (Accumulator::Avg(ref mut s, ref mut c), StreamColDesc::Agg { arg, .. }) => {
            let v = evaluate(arg, vals, schema)?;
            match v {
                Value::Integer(n) => { *s += n as f64; *c += 1; }
                Value::BigInt(n) => { *s += n as f64; *c += 1; }
                Value::Float(f) => { *s += f; *c += 1; }
                Value::Null => {}
                _ => {}
            }
        }
        (Accumulator::Min(ref mut min), StreamColDesc::Agg { arg, .. }) => {
            let v = evaluate(arg, vals, schema)?;
            if !v.is_null() {
                if min.is_null() {
                    *min = v;
                } else if let Some(std::cmp::Ordering::Less) = v.compare(min) {
                    *min = v;
                }
            }
        }
        (Accumulator::Max(ref mut max), StreamColDesc::Agg { arg, .. }) => {
            let v = evaluate(arg, vals, schema)?;
            if !v.is_null() {
                if max.is_null() {
                    *max = v;
                } else if let Some(std::cmp::Ordering::Greater) = v.compare(max) {
                    *max = v;
                }
            }
        }
        // FirstValue doesn't update — keeps the first value seen
        (Accumulator::FirstValue(_), _) => {}
        _ => {}
    }
    Ok(())
}

fn finalize_accumulator(acc: &Accumulator) -> Value {
    match acc {
        Accumulator::Count(c) => Value::BigInt(*c),
        Accumulator::Sum(v) => v.clone(),
        Accumulator::Avg(sum, count) => {
            if *count == 0 {
                Value::Null
            } else {
                Value::Float(*sum / *count as f64)
            }
        }
        Accumulator::Min(v) => v.clone(),
        Accumulator::Max(v) => v.clone(),
        Accumulator::FirstValue(v) => v.clone(),
    }
}
