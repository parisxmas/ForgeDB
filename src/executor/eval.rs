use crate::error::{ForgeError, Result};
use crate::sql::ast::{BinaryOperator, Expr, LiteralValue, UnaryOperator};
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

/// Evaluate an expression against a tuple with the given schema.
pub fn evaluate(expr: &Expr, tuple: &[Value], schema: &Schema) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => match lit {
            LiteralValue::Integer(n) => {
                if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                    Ok(Value::Integer(*n as i32))
                } else {
                    Ok(Value::BigInt(*n))
                }
            }
            LiteralValue::Float(f) => Ok(Value::Float(*f)),
            LiteralValue::String(s) => Ok(Value::Varchar(s.clone())),
            LiteralValue::Boolean(b) => Ok(Value::Boolean(*b)),
            LiteralValue::Null => Ok(Value::Null),
        },
        Expr::ColumnRef { table, column } => {
            // Resolve column with multiple fallback strategies
            let col_lower = column.to_lowercase();
            let idx = if let Some(tbl) = table {
                let qualified = format!("{}.{}", tbl, column);
                // 1. Exact qualified match
                schema.get_column(&qualified).map(|(i, _)| i)
                    // 2. Bare column name
                    .or_else(|| schema.get_column(column).map(|(i, _)| i))
                    // 3. Any column ending with .column
                    .or_else(|| {
                        let suffix = format!(".{}", col_lower);
                        schema.columns.iter().enumerate()
                            .find(|(_, c)| c.name.to_lowercase().ends_with(&suffix))
                            .map(|(i, _)| i)
                    })
            } else {
                // 1. Exact bare name
                schema.get_column(column).map(|(i, _)| i)
                    // 2. Any column ending with .column (prefixed schema)
                    .or_else(|| {
                        let suffix = format!(".{}", col_lower);
                        schema.columns.iter().enumerate()
                            .find(|(_, c)| c.name.to_lowercase().ends_with(&suffix))
                            .map(|(i, _)| i)
                    })
                    // 3. Any column whose bare name (after .) matches
                    .or_else(|| {
                        schema.columns.iter().enumerate()
                            .find(|(_, c)| {
                                let name = c.name.to_lowercase();
                                let bare = name.rsplit('.').next().unwrap_or(&name);
                                bare == col_lower
                            })
                            .map(|(i, _)| i)
                    })
            };
            let idx = idx.ok_or_else(|| {
                ForgeError::Execution(format!("column '{}' not found in schema", column))
            })?;
            if idx < tuple.len() {
                Ok(tuple[idx].clone())
            } else {
                Err(ForgeError::Execution(format!(
                    "column index {} out of bounds (tuple has {} values)",
                    idx,
                    tuple.len()
                )))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let l = evaluate(left, tuple, schema)?;
            let r = evaluate(right, tuple, schema)?;
            eval_binary_op(&l, op, &r)
        }
        Expr::UnaryOp { op, expr } => {
            let val = evaluate(expr, tuple, schema)?;
            match op {
                UnaryOperator::Not => match val {
                    Value::Boolean(b) => Ok(Value::Boolean(!b)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ForgeError::Execution("NOT requires boolean".into())),
                },
                UnaryOperator::Neg => match val {
                    Value::Integer(n) => Ok(Value::Integer(-n)),
                    Value::BigInt(n) => Ok(Value::BigInt(-n)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Err(ForgeError::Execution("negation requires number".into())),
                },
            }
        }
        Expr::IsNull(inner) => {
            let val = evaluate(inner, tuple, schema)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = evaluate(inner, tuple, schema)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::Function { name, args } => {
            let upper = name.to_uppercase();
            match upper.as_str() {
                "YEAR" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::DateTime(epoch) => {
                                let (y, _, _) = epoch_to_ymd(epoch);
                                Ok(Value::Integer(y as i32))
                            }
                            Value::Varchar(s) => {
                                let epoch = crate::tuple::tuple::parse_datetime_to_epoch(&s);
                                let (y, _, _) = epoch_to_ymd(epoch);
                                Ok(Value::Integer(y as i32))
                            }
                            _ => Ok(Value::Null),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "MONTH" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::DateTime(epoch) => {
                                let (_, m, _) = epoch_to_ymd(epoch);
                                Ok(Value::Integer(m as i32))
                            }
                            Value::Varchar(s) => {
                                let epoch = crate::tuple::tuple::parse_datetime_to_epoch(&s);
                                let (_, m, _) = epoch_to_ymd(epoch);
                                Ok(Value::Integer(m as i32))
                            }
                            _ => Ok(Value::Null),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "DAY" | "DAYOFMONTH" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::DateTime(epoch) => {
                                let (_, _, d) = epoch_to_ymd(epoch);
                                Ok(Value::Integer(d as i32))
                            }
                            _ => Ok(Value::Null),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "NOW" | "CURRENT_TIMESTAMP" => {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let epoch = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    Ok(Value::DateTime(epoch))
                }
                "CONCAT" => {
                    let mut result = String::new();
                    for arg in args {
                        let val = evaluate(arg, tuple, schema)?;
                        if val.is_null() {
                            return Ok(Value::Null);
                        }
                        result.push_str(&val.to_string());
                    }
                    Ok(Value::Varchar(result))
                }
                "IF" => {
                    if args.len() >= 3 {
                        let cond = eval_to_bool(&args[0], tuple, schema)?;
                        if cond {
                            evaluate(&args[1], tuple, schema)
                        } else {
                            evaluate(&args[2], tuple, schema)
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "IFNULL" | "COALESCE" => {
                    for arg in args {
                        let val = evaluate(arg, tuple, schema)?;
                        if !val.is_null() {
                            return Ok(val);
                        }
                    }
                    Ok(Value::Null)
                }
                // Aggregate functions should not reach here in normal flow
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => Err(ForgeError::Execution(
                    format!("aggregate function '{}' used outside aggregate context", name),
                )),
                _ => {
                    // Unknown function: return NULL rather than error for compatibility
                    Ok(Value::Null)
                }
            }
        }
        Expr::Like { expr, pattern } => {
            let val = evaluate(expr, tuple, schema)?;
            let pat = evaluate(pattern, tuple, schema)?;
            match (&val, &pat) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Varchar(s), Value::Varchar(p)) => {
                    Ok(Value::Boolean(like_match(s, p)))
                }
                _ => Err(ForgeError::Execution("LIKE requires string operands".into())),
            }
        }
        Expr::NotLike { expr, pattern } => {
            let val = evaluate(expr, tuple, schema)?;
            let pat = evaluate(pattern, tuple, schema)?;
            match (&val, &pat) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Varchar(s), Value::Varchar(p)) => {
                    Ok(Value::Boolean(!like_match(s, p)))
                }
                _ => Err(ForgeError::Execution("NOT LIKE requires string operands".into())),
            }
        }
        Expr::In { expr, list } => {
            let val = evaluate(expr, tuple, schema)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            let mut found = false;
            for item in list {
                let item_val = evaluate(item, tuple, schema)?;
                if !item_val.is_null() {
                    let (l, r) = coerce_for_comparison(&val, &item_val);
                    if let Some(std::cmp::Ordering::Equal) = l.compare(&r) {
                        found = true;
                        break;
                    }
                }
            }
            Ok(Value::Boolean(found))
        }
        Expr::NotIn { expr, list } => {
            let val = evaluate(expr, tuple, schema)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            let mut found = false;
            for item in list {
                let item_val = evaluate(item, tuple, schema)?;
                if !item_val.is_null() {
                    let (l, r) = coerce_for_comparison(&val, &item_val);
                    if let Some(std::cmp::Ordering::Equal) = l.compare(&r) {
                        found = true;
                        break;
                    }
                }
            }
            Ok(Value::Boolean(!found))
        }
        Expr::Between { expr, low, high } => {
            let val = evaluate(expr, tuple, schema)?;
            let lo = evaluate(low, tuple, schema)?;
            let hi = evaluate(high, tuple, schema)?;
            if val.is_null() || lo.is_null() || hi.is_null() {
                return Ok(Value::Null);
            }
            let (val_lo, lo_cmp) = coerce_for_comparison(&val, &lo);
            let (val_hi, hi_cmp) = coerce_for_comparison(&val, &hi);
            let ge_low = match val_lo.compare(&lo_cmp) {
                Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal) => true,
                _ => false,
            };
            let le_high = match val_hi.compare(&hi_cmp) {
                Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => true,
                _ => false,
            };
            Ok(Value::Boolean(ge_low && le_high))
        }
    }
}

/// Evaluate an expression and coerce to bool (NULL -> false for WHERE clauses).
pub fn eval_to_bool(expr: &Expr, tuple: &[Value], schema: &Schema) -> Result<bool> {
    let val = evaluate(expr, tuple, schema)?;
    match val {
        Value::Boolean(b) => Ok(b),
        Value::Null => Ok(false),
        Value::Integer(n) => Ok(n != 0),
        Value::BigInt(n) => Ok(n != 0),
        _ => Err(ForgeError::Execution(format!(
            "expected boolean, got {:?}",
            val
        ))),
    }
}

/// SQL LIKE pattern matching (case-insensitive).
/// `%` matches any sequence of characters (including empty).
/// `_` matches exactly one character.
fn like_match(s: &str, pattern: &str) -> bool {
    let s_lower: Vec<char> = s.to_lowercase().chars().collect();
    let p_lower: Vec<char> = pattern.to_lowercase().chars().collect();
    like_match_inner(&s_lower, &p_lower)
}

fn like_match_inner(s: &[char], p: &[char]) -> bool {
    let mut si = 0;
    let mut pi = 0;
    let mut star_pi = None; // position after the last '%' in pattern
    let mut star_si = 0; // position in s when we last matched '%'

    while si < s.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == s[si]) {
            // Current characters match (or pattern has _)
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            // Wildcard: record this position and try matching zero chars
            star_pi = Some(pi);
            star_si = si;
            pi += 1;
        } else if let Some(sp) = star_pi {
            // Mismatch but we have a previous '%': backtrack
            pi = sp + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    // Consume remaining '%' in pattern
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }

    pi == p.len()
}

fn eval_binary_op(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
    match op {
        // Comparison operators
        BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq => {
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            // Coerce for cross-type comparisons (Boolean<->Integer, Integer<->Float)
            let (l, r) = coerce_for_comparison(left, right);
            let ordering = l.compare(&r).ok_or_else(|| {
                ForgeError::Execution(format!("cannot compare {:?} with {:?}", left, right))
            })?;
            let result = match op {
                BinaryOperator::Eq => ordering == std::cmp::Ordering::Equal,
                BinaryOperator::NotEq => ordering != std::cmp::Ordering::Equal,
                BinaryOperator::Lt => ordering == std::cmp::Ordering::Less,
                BinaryOperator::LtEq => ordering != std::cmp::Ordering::Greater,
                BinaryOperator::Gt => ordering == std::cmp::Ordering::Greater,
                BinaryOperator::GtEq => ordering != std::cmp::Ordering::Less,
                _ => unreachable!(),
            };
            Ok(Value::Boolean(result))
        }
        // Logical operators
        BinaryOperator::And => {
            let l = to_tribool(left);
            let r = to_tribool(right);
            // SQL three-valued logic for AND
            match (l, r) {
                (Some(false), _) | (_, Some(false)) => Ok(Value::Boolean(false)),
                (Some(true), Some(true)) => Ok(Value::Boolean(true)),
                _ => Ok(Value::Null),
            }
        }
        BinaryOperator::Or => {
            let l = to_tribool(left);
            let r = to_tribool(right);
            match (l, r) {
                (Some(true), _) | (_, Some(true)) => Ok(Value::Boolean(true)),
                (Some(false), Some(false)) => Ok(Value::Boolean(false)),
                _ => Ok(Value::Null),
            }
        }
        // Arithmetic operators
        BinaryOperator::Add => left.add(right),
        BinaryOperator::Sub => left.sub(right),
        BinaryOperator::Mul => left.mul(right),
        BinaryOperator::Div => left.div(right),
    }
}

/// Coerce two values so they can be compared across types.
pub fn coerce_for_comparison(left: &Value, right: &Value) -> (Value, Value) {
    match (left, right) {
        // Boolean <-> Integer
        (Value::Boolean(b), Value::Integer(_)) => {
            (Value::Integer(if *b { 1 } else { 0 }), right.clone())
        }
        (Value::Integer(_), Value::Boolean(b)) => {
            (left.clone(), Value::Integer(if *b { 1 } else { 0 }))
        }
        // Boolean <-> BigInt
        (Value::Boolean(b), Value::BigInt(_)) => {
            (Value::BigInt(if *b { 1 } else { 0 }), right.clone())
        }
        (Value::BigInt(_), Value::Boolean(b)) => {
            (left.clone(), Value::BigInt(if *b { 1 } else { 0 }))
        }
        // Integer <-> Float
        (Value::Integer(n), Value::Float(_)) => (Value::Float(*n as f64), right.clone()),
        (Value::Float(_), Value::Integer(n)) => (left.clone(), Value::Float(*n as f64)),
        // BigInt <-> Float
        (Value::BigInt(n), Value::Float(_)) => (Value::Float(*n as f64), right.clone()),
        (Value::Float(_), Value::BigInt(n)) => (left.clone(), Value::Float(*n as f64)),
        // Integer <-> BigInt
        (Value::Integer(n), Value::BigInt(_)) => (Value::BigInt(*n as i64), right.clone()),
        (Value::BigInt(_), Value::Integer(n)) => (left.clone(), Value::BigInt(*n as i64)),
        // DateTime <-> Varchar (parse date string to epoch for comparison)
        (Value::DateTime(_), Value::Varchar(s)) => {
            let epoch = crate::tuple::tuple::parse_datetime_to_epoch(s);
            (left.clone(), Value::DateTime(epoch))
        }
        (Value::Varchar(s), Value::DateTime(_)) => {
            let epoch = crate::tuple::tuple::parse_datetime_to_epoch(s);
            (Value::DateTime(epoch), right.clone())
        }
        _ => (left.clone(), right.clone()),
    }
}

/// Convert epoch seconds to (year, month, day).
fn epoch_to_ymd(epoch: i64) -> (i64, i64, i64) {
    let days = epoch / 86400;
    // Civil calendar algorithm
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as i64, d as i64)
}

fn to_tribool(val: &Value) -> Option<bool> {
    match val {
        Value::Boolean(b) => Some(*b),
        Value::Integer(n) => Some(*n != 0),
        Value::BigInt(n) => Some(*n != 0),
        Value::Null => None,
        _ => Some(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::schema::Column;
    use crate::tuple::types::DataType;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Column {
                name: "id".into(),
                data_type: DataType::Integer,
                nullable: false,
                column_id: 0,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(100),
                nullable: true,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
            },
        ])
    }

    #[test]
    fn test_literal() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("Alice".into())];
        let expr = Expr::Literal(LiteralValue::Integer(42));
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Integer(42));
    }

    #[test]
    fn test_column_ref() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("Alice".into())];
        let expr = Expr::ColumnRef {
            table: None,
            column: "name".into(),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("Alice".into())
        );
    }

    #[test]
    fn test_comparison() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(5), Value::Varchar("Alice".into())];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_null_comparison_returns_null() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Null];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(LiteralValue::String("x".into()))),
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_is_null() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Null];
        let expr = Expr::IsNull(Box::new(Expr::ColumnRef {
            table: None,
            column: "name".into(),
        }));
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    // ---- LIKE tests ----

    #[test]
    fn test_like_percent_suffix() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let expr = Expr::Like {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("hel%".into()))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_like_underscore() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let expr = Expr::Like {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("h_llo".into()))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_like_case_insensitive() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("Hello".into())];
        let expr = Expr::Like {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("hello".into()))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_like_no_match() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let expr = Expr::Like {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("world%".into()))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_like_null_returns_null() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Null];
        let expr = Expr::Like {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("%".into()))),
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_not_like() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let expr = Expr::NotLike {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("world%".into()))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_like_percent_both_sides() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello world".into())];
        let expr = Expr::Like {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "name".into(),
            }),
            pattern: Box::new(Expr::Literal(LiteralValue::String("%lo wo%".into()))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    // ---- IN tests ----

    #[test]
    fn test_in_found() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(2), Value::Varchar("Alice".into())];
        let expr = Expr::In {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            list: vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(2)),
                Expr::Literal(LiteralValue::Integer(3)),
            ],
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_in_not_found() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(5), Value::Varchar("Alice".into())];
        let expr = Expr::In {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            list: vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(2)),
                Expr::Literal(LiteralValue::Integer(3)),
            ],
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_in_null_expr_returns_null() {
        let schema = test_schema();
        let tuple = vec![Value::Null, Value::Varchar("Alice".into())];
        let expr = Expr::In {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            list: vec![
                Expr::Literal(LiteralValue::Integer(1)),
            ],
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_not_in() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(5), Value::Varchar("Alice".into())];
        let expr = Expr::NotIn {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            list: vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(2)),
                Expr::Literal(LiteralValue::Integer(3)),
            ],
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    // ---- BETWEEN tests ----

    #[test]
    fn test_between_in_range() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(5), Value::Varchar("Alice".into())];
        let expr = Expr::Between {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_between_at_boundary() {
        let schema = test_schema();
        // Test lower boundary
        let tuple = vec![Value::Integer(1), Value::Varchar("Alice".into())];
        let expr = Expr::Between {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );

        // Test upper boundary
        let tuple2 = vec![Value::Integer(10), Value::Varchar("Alice".into())];
        assert_eq!(
            evaluate(&expr, &tuple2, &schema).unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_between_out_of_range() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(15), Value::Varchar("Alice".into())];
        let expr = Expr::Between {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_between_null_returns_null() {
        let schema = test_schema();
        let tuple = vec![Value::Null, Value::Varchar("Alice".into())];
        let expr = Expr::Between {
            expr: Box::new(Expr::ColumnRef {
                table: None,
                column: "id".into(),
            }),
            low: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            high: Box::new(Expr::Literal(LiteralValue::Integer(10))),
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    // ---- like_match unit tests ----

    #[test]
    fn test_like_match_exact() {
        assert!(like_match("hello", "hello"));
        assert!(!like_match("hello", "world"));
    }

    #[test]
    fn test_like_match_percent() {
        assert!(like_match("hello", "%"));
        assert!(like_match("hello", "h%"));
        assert!(like_match("hello", "%o"));
        assert!(like_match("hello", "%ell%"));
        assert!(!like_match("hello", "x%"));
    }

    #[test]
    fn test_like_match_underscore() {
        assert!(like_match("hello", "_ello"));
        assert!(like_match("hello", "hell_"));
        assert!(like_match("hello", "h_ll_"));
        assert!(!like_match("hello", "_"));
        assert!(!like_match("hello", "______"));
    }

    #[test]
    fn test_like_match_empty() {
        assert!(like_match("", ""));
        assert!(like_match("", "%"));
        assert!(!like_match("", "_"));
        assert!(!like_match("hello", ""));
    }
}
