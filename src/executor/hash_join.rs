use std::collections::{HashMap, HashSet};

use crate::common::{PageId, RID};
use crate::error::Result;
use crate::sql::ast::{BinaryOperator, Expr, JoinType};
use crate::tuple::schema::{Column, Schema};
use crate::tuple::types::Value;

use super::eval::eval_to_bool;

/// Try to extract an equi-join condition: returns (left_col, right_col) indices
/// in the combined schema if the ON condition is a simple `left.col = right.col`.
fn extract_equi_join_keys(
    on: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(usize, usize)> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = on
    {
        let left_col = extract_column_index(left, left_schema, right_schema, true);
        let right_col = extract_column_index(right, left_schema, right_schema, false);

        if let (Some(l), Some(r)) = (left_col, right_col) {
            return Some((l, r));
        }

        // Try reversed: right = left
        let left_col = extract_column_index(right, left_schema, right_schema, true);
        let right_col = extract_column_index(left, left_schema, right_schema, false);
        if let (Some(l), Some(r)) = (left_col, right_col) {
            return Some((l, r));
        }
    }
    None
}

fn extract_column_index(
    expr: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
    want_left: bool,
) -> Option<usize> {
    if let Expr::ColumnRef { table, column } = expr {
        let target_schema = if want_left { left_schema } else { right_schema };

        // Try qualified name first
        if let Some(tbl) = table {
            let qualified = format!("{}.{}", tbl, column);
            if let Some((idx, _)) = target_schema.get_column(&qualified) {
                return Some(idx);
            }
        }

        // Try bare column name
        if let Some((idx, _)) = target_schema.get_column(column) {
            return Some(idx);
        }

        // Try suffix match (column name without table prefix)
        let suffix = format!(".{}", column.to_lowercase());
        for (i, c) in target_schema.columns.iter().enumerate() {
            if c.name.to_lowercase().ends_with(&suffix) {
                return Some(i);
            }
        }
    }
    None
}

/// Execute a join with an optional ON condition.
///
/// For CROSS JOIN, `on` should be `None` (produces Cartesian product).
/// For all other join types, delegates to `execute_hash_join` with the ON condition.
pub fn execute_join(
    left_rows: &[(RID, Vec<Value>)],
    right_rows: &[(RID, Vec<Value>)],
    join_type: &JoinType,
    on: &Option<Expr>,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let combined_schema = build_combined_schema(left_schema, right_schema);
    let dummy_rid = RID { page_id: PageId(0), slot_id: 0 };

    match (join_type, on) {
        // CROSS JOIN: Cartesian product, no ON condition needed
        (JoinType::Cross, _) => {
            let mut result = Vec::with_capacity(left_rows.len() * right_rows.len());
            for (_, lvals) in left_rows {
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    result.push((dummy_rid, combined));
                }
            }
            Ok((combined_schema, result))
        }
        // Other join types with an ON condition
        (_, Some(expr)) => {
            execute_hash_join(left_rows, right_rows, join_type, expr, left_schema, right_schema)
        }
        // Non-CROSS join without ON condition: treat as CROSS (Cartesian product)
        (_, None) => {
            let mut result = Vec::with_capacity(left_rows.len() * right_rows.len());
            for (_, lvals) in left_rows {
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    result.push((dummy_rid, combined));
                }
            }
            Ok((combined_schema, result))
        }
    }
}

/// Execute a hash join for equi-join conditions.
/// Falls back to nested-loop for non-equi joins.
pub fn execute_hash_join(
    left_rows: &[(RID, Vec<Value>)],
    right_rows: &[(RID, Vec<Value>)],
    join_type: &JoinType,
    on: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    // Build combined schema
    let combined_schema = build_combined_schema(left_schema, right_schema);
    let dummy_rid = RID { page_id: PageId(0), slot_id: 0 };
    let right_null_count = right_schema.columns.len();
    let left_null_count = left_schema.columns.len();

    // CROSS JOIN: Cartesian product (ignore ON condition)
    if matches!(join_type, JoinType::Cross) {
        let mut result = Vec::with_capacity(left_rows.len() * right_rows.len());
        for (_, lvals) in left_rows {
            for (_, rvals) in right_rows {
                let mut combined = lvals.clone();
                combined.extend(rvals.iter().cloned());
                result.push((dummy_rid, combined));
            }
        }
        return Ok((combined_schema, result));
    }

    // Try equi-join optimization
    if let Some((left_key_idx, right_key_idx)) = extract_equi_join_keys(on, left_schema, right_schema) {
        let combined_width = left_schema.columns.len() + right_schema.columns.len();

        // Build hash table on the smaller side
        let (build_rows, probe_rows, build_key_idx, probe_key_idx, build_is_left) =
            if right_rows.len() <= left_rows.len() {
                (right_rows, left_rows, right_key_idx, left_key_idx, false)
            } else {
                (left_rows, right_rows, left_key_idx, right_key_idx, true)
            };

        let mut hash_table: HashMap<HashKey, Vec<usize>> =
            HashMap::with_capacity(build_rows.len());
        for (i, (_, vals)) in build_rows.iter().enumerate() {
            if build_key_idx < vals.len() {
                let key = HashKey::from_value(&vals[build_key_idx]);
                hash_table.entry(key).or_default().push(i);
            }
        }

        // Estimate result size
        let mut result = Vec::with_capacity(std::cmp::max(left_rows.len(), right_rows.len()));

        match join_type {
            JoinType::Inner => {
                // Pre-allocate a reusable buffer for combined rows
                let mut combined = Vec::with_capacity(combined_width);
                for (_, pvals) in probe_rows {
                    if probe_key_idx < pvals.len() {
                        let key = HashKey::from_value(&pvals[probe_key_idx]);
                        if let Some(indices) = hash_table.get(&key) {
                            for &bi in indices {
                                combined.clear();
                                if build_is_left {
                                    combined.extend_from_slice(&build_rows[bi].1);
                                    combined.extend_from_slice(pvals);
                                } else {
                                    combined.extend_from_slice(pvals);
                                    combined.extend_from_slice(&build_rows[bi].1);
                                }
                                result.push((dummy_rid, combined.clone()));
                            }
                        }
                    }
                }
            }
            JoinType::Left => {
                // Build hash on right side
                let mut right_hash: HashMap<HashKey, Vec<usize>> =
                    HashMap::with_capacity(right_rows.len());
                for (i, (_, rvals)) in right_rows.iter().enumerate() {
                    if right_key_idx < rvals.len() {
                        let key = HashKey::from_value(&rvals[right_key_idx]);
                        right_hash.entry(key).or_default().push(i);
                    }
                }
                for (_, lvals) in left_rows {
                    let key = if left_key_idx < lvals.len() {
                        HashKey::from_value(&lvals[left_key_idx])
                    } else {
                        HashKey::Null
                    };
                    if let Some(indices) = right_hash.get(&key) {
                        for &ri in indices {
                            let mut combined = Vec::with_capacity(combined_width);
                            combined.extend_from_slice(lvals);
                            combined.extend_from_slice(&right_rows[ri].1);
                            result.push((dummy_rid, combined));
                        }
                    } else {
                        let mut combined = Vec::with_capacity(combined_width);
                        combined.extend_from_slice(lvals);
                        combined.extend(std::iter::repeat(Value::Null).take(right_null_count));
                        result.push((dummy_rid, combined));
                    }
                }
            }
            JoinType::Right => {
                // Build hash on left side
                let mut left_hash: HashMap<HashKey, Vec<usize>> =
                    HashMap::with_capacity(left_rows.len());
                for (i, (_, lvals)) in left_rows.iter().enumerate() {
                    if left_key_idx < lvals.len() {
                        let key = HashKey::from_value(&lvals[left_key_idx]);
                        left_hash.entry(key).or_default().push(i);
                    }
                }
                for (_, rvals) in right_rows {
                    let key = if right_key_idx < rvals.len() {
                        HashKey::from_value(&rvals[right_key_idx])
                    } else {
                        HashKey::Null
                    };
                    if let Some(indices) = left_hash.get(&key) {
                        for &li in indices {
                            let mut combined = Vec::with_capacity(combined_width);
                            combined.extend_from_slice(&left_rows[li].1);
                            combined.extend_from_slice(rvals);
                            result.push((dummy_rid, combined));
                        }
                    } else {
                        let mut combined = Vec::with_capacity(combined_width);
                        combined.extend(std::iter::repeat(Value::Null).take(left_null_count));
                        combined.extend_from_slice(rvals);
                        result.push((dummy_rid, combined));
                    }
                }
            }
            JoinType::Full => {
                // FULL OUTER JOIN with equi-join optimization:
                // 1. Do a LEFT JOIN, tracking which right rows matched
                // 2. Append unmatched right rows with NULLs for left columns
                let mut right_hash: HashMap<HashKey, Vec<usize>> =
                    HashMap::with_capacity(right_rows.len());
                for (i, (_, rvals)) in right_rows.iter().enumerate() {
                    if right_key_idx < rvals.len() {
                        let key = HashKey::from_value(&rvals[right_key_idx]);
                        right_hash.entry(key).or_default().push(i);
                    }
                }

                let mut matched_right: HashSet<usize> = HashSet::new();

                // LEFT JOIN pass
                for (_, lvals) in left_rows {
                    let key = if left_key_idx < lvals.len() {
                        HashKey::from_value(&lvals[left_key_idx])
                    } else {
                        HashKey::Null
                    };
                    if let Some(indices) = right_hash.get(&key) {
                        for &ri in indices {
                            matched_right.insert(ri);
                            let mut combined = Vec::with_capacity(combined_width);
                            combined.extend_from_slice(lvals);
                            combined.extend_from_slice(&right_rows[ri].1);
                            result.push((dummy_rid, combined));
                        }
                    } else {
                        let mut combined = Vec::with_capacity(combined_width);
                        combined.extend_from_slice(lvals);
                        combined.extend(std::iter::repeat(Value::Null).take(right_null_count));
                        result.push((dummy_rid, combined));
                    }
                }

                // Add unmatched right rows
                for (ri, (_, rvals)) in right_rows.iter().enumerate() {
                    if !matched_right.contains(&ri) {
                        let mut combined = Vec::with_capacity(combined_width);
                        combined.extend(std::iter::repeat(Value::Null).take(left_null_count));
                        combined.extend_from_slice(rvals);
                        result.push((dummy_rid, combined));
                    }
                }
            }
            JoinType::Cross => {
                // Already handled above, but match for completeness
                unreachable!("CROSS JOIN handled before equi-join path");
            }
        }

        return Ok((combined_schema, result));
    }

    // Fallback: nested-loop join for non-equi conditions
    let mut result = Vec::new();
    match join_type {
        JoinType::Inner => {
            for (_, lvals) in left_rows {
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                    }
                }
            }
        }
        JoinType::Left => {
            for (_, lvals) in left_rows {
                let mut matched = false;
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined = lvals.clone();
                    combined.extend(std::iter::repeat(Value::Null).take(right_null_count));
                    result.push((dummy_rid, combined));
                }
            }
        }
        JoinType::Right => {
            for (_, rvals) in right_rows {
                let mut matched = false;
                for (_, lvals) in left_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                        matched = true;
                    }
                }
                if !matched {
                    let mut combined: Vec<Value> =
                        std::iter::repeat(Value::Null).take(left_null_count).collect();
                    combined.extend(rvals.iter().cloned());
                    result.push((dummy_rid, combined));
                }
            }
        }
        JoinType::Full => {
            // FULL OUTER JOIN nested-loop fallback:
            // 1. Do a LEFT JOIN pass, tracking which right rows matched
            // 2. Append unmatched right rows with NULLs for left columns
            let mut matched_right: HashSet<usize> = HashSet::new();

            for (_, lvals) in left_rows {
                let mut matched = false;
                for (ri, (_, rvals)) in right_rows.iter().enumerate() {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    if eval_to_bool(on, &combined, &combined_schema)? {
                        result.push((dummy_rid, combined));
                        matched = true;
                        matched_right.insert(ri);
                    }
                }
                if !matched {
                    let mut combined = lvals.clone();
                    combined.extend(std::iter::repeat(Value::Null).take(right_null_count));
                    result.push((dummy_rid, combined));
                }
            }

            // Add unmatched right rows
            for (ri, (_, rvals)) in right_rows.iter().enumerate() {
                if !matched_right.contains(&ri) {
                    let mut combined: Vec<Value> =
                        std::iter::repeat(Value::Null).take(left_null_count).collect();
                    combined.extend(rvals.iter().cloned());
                    result.push((dummy_rid, combined));
                }
            }
        }
        JoinType::Cross => {
            // CROSS JOIN: Cartesian product (ignore ON condition)
            for (_, lvals) in left_rows {
                for (_, rvals) in right_rows {
                    let mut combined = lvals.clone();
                    combined.extend(rvals.iter().cloned());
                    result.push((dummy_rid, combined));
                }
            }
        }
    }

    Ok((combined_schema, result))
}

/// Public version of build_combined_schema for use by grace_hash_join.
pub fn build_combined_schema_pub(left: &Schema, right: &Schema) -> Schema {
    build_combined_schema(left, right)
}

fn build_combined_schema(left: &Schema, right: &Schema) -> Schema {
    let mut cols = Vec::new();
    for col in &left.columns {
        cols.push(Column {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
            column_id: cols.len() as u16,
            auto_increment: false,
            default_value: None,
            is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
        });
    }
    for col in &right.columns {
        cols.push(Column {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: true,
            column_id: cols.len() as u16,
            auto_increment: false,
            default_value: None,
            is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
        });
    }
    Schema::new(cols)
}

/// A hashable key for join lookups.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum HashKey {
    Integer(i32),
    BigInt(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl HashKey {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Integer(n) => HashKey::Integer(*n),
            Value::BigInt(n) => HashKey::BigInt(*n),
            Value::Float(f) => HashKey::BigInt((*f) as i64), // lossy but works for equi-join
            Value::Varchar(s) => HashKey::Str(s.clone()),
            Value::Boolean(b) => HashKey::Bool(*b),
            Value::DateTime(t) => HashKey::BigInt(*t),
            Value::Decimal(v, _) => HashKey::BigInt(*v),
            Value::Date(d) => HashKey::Integer(*d),
            Value::Time(t) => HashKey::Integer(*t),
            Value::Binary(_) => HashKey::Str(v.to_string()),
            Value::Json(s) => HashKey::Str(s.clone()),
            Value::Uuid(s) => HashKey::Str(s.clone()),
            Value::Null => HashKey::Null,
        }
    }
}
