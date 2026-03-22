use crate::error::{ForgeError, Result};
use crate::sql::ast::{BinaryOperator, Expr, LiteralValue, UnaryOperator};
use crate::tuple::schema::Schema;
use crate::tuple::types::{DataType, Value};

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
            // Fast path: try exact name match (zero allocation via eq_ignore_ascii_case)
            let idx = schema.get_column(column).map(|(i, _)| i)
                .or_else(|| {
                    // Try qualified name
                    if let Some(tbl) = table {
                        let mut qualified = String::with_capacity(tbl.len() + 1 + column.len());
                        qualified.push_str(tbl);
                        qualified.push('.');
                        qualified.push_str(column);
                        schema.get_column(&qualified).map(|(i, _)| i)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    // Suffix match: column name after '.' matches
                    schema.columns.iter().enumerate()
                        .find(|(_, c)| {
                            if let Some(dot) = c.name.rfind('.') {
                                c.name[dot+1..].eq_ignore_ascii_case(column)
                            } else {
                                false
                            }
                        })
                        .map(|(i, _)| i)
                });
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
        Expr::Function { name, args, .. } => {
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
                // ---- String functions ----
                "UPPER" | "UCASE" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Varchar(s.to_uppercase())),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Varchar(other.to_string().to_uppercase())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LOWER" | "LCASE" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Varchar(s.to_lowercase())),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Varchar(other.to_string().to_lowercase())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "TRIM" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Varchar(s.trim().to_string())),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Varchar(other.to_string().trim().to_string())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LTRIM" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Varchar(s.trim_start().to_string())),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Varchar(other.to_string().trim_start().to_string())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "RTRIM" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Varchar(s.trim_end().to_string())),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Varchar(other.to_string().trim_end().to_string())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Integer(s.chars().count() as i32)),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Integer(other.to_string().chars().count() as i32)),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "SUBSTRING" | "SUBSTR" | "MID" => {
                    // SUBSTRING(str, start[, len])
                    // SQL uses 1-based indexing
                    if args.len() >= 2 {
                        let val = evaluate(&args[0], tuple, schema)?;
                        let start_val = evaluate(&args[1], tuple, schema)?;
                        if val.is_null() || start_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let s = match &val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let start = value_to_i64(&start_val);
                        let chars: Vec<char> = s.chars().collect();
                        // SQL 1-based to 0-based; handle negative/zero start
                        let start_idx = if start < 1 { 0 } else { (start - 1) as usize };
                        if start_idx >= chars.len() {
                            return Ok(Value::Varchar(String::new()));
                        }
                        let result = if args.len() >= 3 {
                            let len_val = evaluate(&args[2], tuple, schema)?;
                            if len_val.is_null() {
                                return Ok(Value::Null);
                            }
                            let len = value_to_i64(&len_val).max(0) as usize;
                            let end = (start_idx + len).min(chars.len());
                            chars[start_idx..end].iter().collect::<String>()
                        } else {
                            chars[start_idx..].iter().collect::<String>()
                        };
                        Ok(Value::Varchar(result))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LEFT" => {
                    if args.len() >= 2 {
                        let val = evaluate(&args[0], tuple, schema)?;
                        let n_val = evaluate(&args[1], tuple, schema)?;
                        if val.is_null() || n_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let s = match &val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let n = value_to_i64(&n_val).max(0) as usize;
                        let result: String = s.chars().take(n).collect();
                        Ok(Value::Varchar(result))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "RIGHT" => {
                    if args.len() >= 2 {
                        let val = evaluate(&args[0], tuple, schema)?;
                        let n_val = evaluate(&args[1], tuple, schema)?;
                        if val.is_null() || n_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let s = match &val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let n = value_to_i64(&n_val).max(0) as usize;
                        let chars: Vec<char> = s.chars().collect();
                        let start = if n >= chars.len() { 0 } else { chars.len() - n };
                        let result: String = chars[start..].iter().collect();
                        Ok(Value::Varchar(result))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "REPLACE" => {
                    if args.len() >= 3 {
                        let val = evaluate(&args[0], tuple, schema)?;
                        let from_val = evaluate(&args[1], tuple, schema)?;
                        let to_val = evaluate(&args[2], tuple, schema)?;
                        if val.is_null() || from_val.is_null() || to_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let s = match &val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let from = match &from_val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let to = match &to_val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        Ok(Value::Varchar(s.replace(&from, &to)))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "REVERSE" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Varchar(s) => Ok(Value::Varchar(s.chars().rev().collect())),
                            Value::Null => Ok(Value::Null),
                            other => Ok(Value::Varchar(other.to_string().chars().rev().collect())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "LPAD" => {
                    // LPAD(str, len, pad_str)
                    if args.len() >= 3 {
                        let val = evaluate(&args[0], tuple, schema)?;
                        let len_val = evaluate(&args[1], tuple, schema)?;
                        let pad_val = evaluate(&args[2], tuple, schema)?;
                        if val.is_null() || len_val.is_null() || pad_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let s = match &val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let target_len = value_to_i64(&len_val).max(0) as usize;
                        let pad = match &pad_val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let chars: Vec<char> = s.chars().collect();
                        if chars.len() >= target_len {
                            // Truncate from right
                            Ok(Value::Varchar(chars[..target_len].iter().collect()))
                        } else if pad.is_empty() {
                            Ok(Value::Varchar(s))
                        } else {
                            let needed = target_len - chars.len();
                            let pad_chars: Vec<char> = pad.chars().collect();
                            let mut result = String::with_capacity(target_len);
                            let mut i = 0;
                            for _ in 0..needed {
                                result.push(pad_chars[i % pad_chars.len()]);
                                i += 1;
                            }
                            result.push_str(&s);
                            Ok(Value::Varchar(result))
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "RPAD" => {
                    // RPAD(str, len, pad_str)
                    if args.len() >= 3 {
                        let val = evaluate(&args[0], tuple, schema)?;
                        let len_val = evaluate(&args[1], tuple, schema)?;
                        let pad_val = evaluate(&args[2], tuple, schema)?;
                        if val.is_null() || len_val.is_null() || pad_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let s = match &val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let target_len = value_to_i64(&len_val).max(0) as usize;
                        let pad = match &pad_val {
                            Value::Varchar(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let chars: Vec<char> = s.chars().collect();
                        if chars.len() >= target_len {
                            Ok(Value::Varchar(chars[..target_len].iter().collect()))
                        } else if pad.is_empty() {
                            Ok(Value::Varchar(s))
                        } else {
                            let needed = target_len - chars.len();
                            let pad_chars: Vec<char> = pad.chars().collect();
                            let mut result = s;
                            let mut i = 0;
                            for _ in 0..needed {
                                result.push(pad_chars[i % pad_chars.len()]);
                                i += 1;
                            }
                            Ok(Value::Varchar(result))
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                // ---- Numeric / Math functions ----
                "ABS" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Integer(n) => Ok(Value::Integer(n.wrapping_abs())),
                            Value::BigInt(n) => Ok(Value::BigInt(n.wrapping_abs())),
                            Value::Float(f) => Ok(Value::Float(f.abs())),
                            Value::Null => Ok(Value::Null),
                            _ => Err(ForgeError::Execution("ABS requires a numeric argument".into())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "ROUND" => {
                    // ROUND(n[, decimals])
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        if val.is_null() {
                            return Ok(Value::Null);
                        }
                        let decimals = if args.len() >= 2 {
                            let d = evaluate(&args[1], tuple, schema)?;
                            if d.is_null() { return Ok(Value::Null); }
                            value_to_i64(&d) as i32
                        } else {
                            0
                        };
                        let f = value_to_f64(&val);
                        if decimals >= 0 {
                            let factor = 10_f64.powi(decimals);
                            let rounded = (f * factor).round() / factor;
                            if decimals == 0 {
                                // Return integer type when rounding to 0 decimals
                                if rounded >= i32::MIN as f64 && rounded <= i32::MAX as f64 {
                                    Ok(Value::Integer(rounded as i32))
                                } else {
                                    Ok(Value::BigInt(rounded as i64))
                                }
                            } else {
                                Ok(Value::Float(rounded))
                            }
                        } else {
                            let factor = 10_f64.powi(-decimals);
                            let rounded = (f / factor).round() * factor;
                            if rounded >= i32::MIN as f64 && rounded <= i32::MAX as f64 {
                                Ok(Value::Integer(rounded as i32))
                            } else {
                                Ok(Value::BigInt(rounded as i64))
                            }
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "CEIL" | "CEILING" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Integer(_) | Value::BigInt(_) => Ok(val),
                            Value::Float(f) => {
                                let c = f.ceil();
                                if c >= i32::MIN as f64 && c <= i32::MAX as f64 {
                                    Ok(Value::Integer(c as i32))
                                } else {
                                    Ok(Value::BigInt(c as i64))
                                }
                            }
                            Value::Null => Ok(Value::Null),
                            _ => Err(ForgeError::Execution("CEIL requires a numeric argument".into())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "FLOOR" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Integer(_) | Value::BigInt(_) => Ok(val),
                            Value::Float(f) => {
                                let c = f.floor();
                                if c >= i32::MIN as f64 && c <= i32::MAX as f64 {
                                    Ok(Value::Integer(c as i32))
                                } else {
                                    Ok(Value::BigInt(c as i64))
                                }
                            }
                            Value::Null => Ok(Value::Null),
                            _ => Err(ForgeError::Execution("FLOOR requires a numeric argument".into())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "POWER" | "POW" => {
                    if args.len() >= 2 {
                        let base_val = evaluate(&args[0], tuple, schema)?;
                        let exp_val = evaluate(&args[1], tuple, schema)?;
                        if base_val.is_null() || exp_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let base = value_to_f64(&base_val);
                        let exp = value_to_f64(&exp_val);
                        Ok(Value::Float(base.powf(exp)))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "SQRT" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        if val.is_null() {
                            return Ok(Value::Null);
                        }
                        let f = value_to_f64(&val);
                        if f < 0.0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Float(f.sqrt()))
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "MOD" => {
                    if args.len() >= 2 {
                        let a_val = evaluate(&args[0], tuple, schema)?;
                        let b_val = evaluate(&args[1], tuple, schema)?;
                        if a_val.is_null() || b_val.is_null() {
                            return Ok(Value::Null);
                        }
                        eval_modulo(&a_val, &b_val)
                    } else {
                        Ok(Value::Null)
                    }
                }
                "SIGN" => {
                    if let Some(arg) = args.first() {
                        let val = evaluate(arg, tuple, schema)?;
                        match val {
                            Value::Integer(n) => {
                                Ok(Value::Integer(if n > 0 { 1 } else if n < 0 { -1 } else { 0 }))
                            }
                            Value::BigInt(n) => {
                                Ok(Value::Integer(if n > 0 { 1 } else if n < 0 { -1 } else { 0 }))
                            }
                            Value::Float(f) => {
                                Ok(Value::Integer(if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 }))
                            }
                            Value::Null => Ok(Value::Null),
                            _ => Err(ForgeError::Execution("SIGN requires a numeric argument".into())),
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                // ---- Date functions ----
                "DATE_ADD" | "ADDDATE" => {
                    if args.len() >= 2 {
                        let date_val = evaluate(&args[0], tuple, schema)?;
                        let days_val = evaluate(&args[1], tuple, schema)?;
                        if date_val.is_null() || days_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let epoch = value_to_epoch(&date_val);
                        let days = value_to_i64(&days_val);
                        Ok(Value::DateTime(epoch + days * 86400))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "DATE_SUB" | "SUBDATE" => {
                    if args.len() >= 2 {
                        let date_val = evaluate(&args[0], tuple, schema)?;
                        let days_val = evaluate(&args[1], tuple, schema)?;
                        if date_val.is_null() || days_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let epoch = value_to_epoch(&date_val);
                        let days = value_to_i64(&days_val);
                        Ok(Value::DateTime(epoch - days * 86400))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "DATEDIFF" => {
                    if args.len() >= 2 {
                        let d1_val = evaluate(&args[0], tuple, schema)?;
                        let d2_val = evaluate(&args[1], tuple, schema)?;
                        if d1_val.is_null() || d2_val.is_null() {
                            return Ok(Value::Null);
                        }
                        let e1 = value_to_epoch(&d1_val);
                        let e2 = value_to_epoch(&d2_val);
                        // DATEDIFF returns days: date1 - date2
                        let diff_days = (e1 / 86400) - (e2 / 86400);
                        if diff_days >= i32::MIN as i64 && diff_days <= i32::MAX as i64 {
                            Ok(Value::Integer(diff_days as i32))
                        } else {
                            Ok(Value::BigInt(diff_days))
                        }
                    } else {
                        Ok(Value::Null)
                    }
                }
                "CURDATE" | "CURRENT_DATE" => {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let epoch = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    // Truncate to midnight
                    let day_epoch = (epoch / 86400) * 86400;
                    Ok(Value::DateTime(day_epoch))
                }
                // ---- CAST as function ----
                "CAST" => {
                    // CAST is normally handled by the Expr::Cast variant.
                    // If it arrives here as a function call, try to evaluate the first arg.
                    if let Some(arg) = args.first() {
                        evaluate(arg, tuple, schema)
                    } else {
                        Ok(Value::Null)
                    }
                }
                // ---- NULLIF ----
                "NULLIF" => {
                    if args.len() >= 2 {
                        let a = evaluate(&args[0], tuple, schema)?;
                        let b = evaluate(&args[1], tuple, schema)?;
                        if a.is_null() && b.is_null() {
                            return Ok(Value::Null);
                        }
                        if !a.is_null() && !b.is_null() {
                            let (l, r) = coerce_for_comparison(&a, &b);
                            if let Some(std::cmp::Ordering::Equal) = l.compare(&r) {
                                return Ok(Value::Null);
                            }
                        }
                        Ok(a)
                    } else {
                        Ok(Value::Null)
                    }
                }
                // ---- GREATEST / LEAST ----
                "GREATEST" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut best = evaluate(&args[0], tuple, schema)?;
                    if best.is_null() {
                        return Ok(Value::Null);
                    }
                    for arg in &args[1..] {
                        let val = evaluate(arg, tuple, schema)?;
                        if val.is_null() {
                            return Ok(Value::Null);
                        }
                        let (l, r) = coerce_for_comparison(&best, &val);
                        match l.compare(&r) {
                            Some(std::cmp::Ordering::Less) => best = val,
                            None => return Ok(Value::Null),
                            _ => {}
                        }
                    }
                    Ok(best)
                }
                "LEAST" => {
                    if args.is_empty() {
                        return Ok(Value::Null);
                    }
                    let mut best = evaluate(&args[0], tuple, schema)?;
                    if best.is_null() {
                        return Ok(Value::Null);
                    }
                    for arg in &args[1..] {
                        let val = evaluate(arg, tuple, schema)?;
                        if val.is_null() {
                            return Ok(Value::Null);
                        }
                        let (l, r) = coerce_for_comparison(&best, &val);
                        match l.compare(&r) {
                            Some(std::cmp::Ordering::Greater) => best = val,
                            None => return Ok(Value::Null),
                            _ => {}
                        }
                    }
                    Ok(best)
                }
                // UUID generation
                "NEWID" | "UUID" | "GEN_RANDOM_UUID" | "UUID_GENERATE_V4" => {
                    // Generate a simple UUID v4
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
                    let nanos = now.as_nanos();
                    // Simple pseudo-UUID using timestamp and counter
                    let uuid = format!(
                        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
                        (nanos >> 96) as u32,
                        (nanos >> 80) as u16,
                        (nanos >> 64) as u16 & 0x0FFF,
                        ((nanos >> 48) as u16 & 0x3FFF) | 0x8000,
                        nanos as u64 & 0xFFFF_FFFF_FFFF
                    );
                    Ok(Value::Uuid(uuid))
                }
                // Full-text search: CONTAINS(column, 'search term')
                // Returns TRUE if all words in the search term appear in the column value (case-insensitive).
                "CONTAINS" => {
                    if args.len() >= 2 {
                        let col_val = evaluate(&args[0], tuple, schema)?;
                        let search_val = evaluate(&args[1], tuple, schema)?;
                        if col_val.is_null() || search_val.is_null() {
                            return Ok(Value::Boolean(false));
                        }
                        let col_str = match &col_val {
                            Value::Varchar(s) => s.to_lowercase(),
                            other => other.to_string().to_lowercase(),
                        };
                        let search_str = match &search_val {
                            Value::Varchar(s) => s.to_lowercase(),
                            other => other.to_string().to_lowercase(),
                        };
                        // Handle quoted phrases: "exact phrase"
                        let search_trimmed = search_str.trim().trim_matches('"');
                        if search_str.starts_with('"') && search_str.ends_with('"') {
                            // Exact phrase match
                            Ok(Value::Boolean(col_str.contains(search_trimmed)))
                        } else {
                            // All words must appear
                            let words: Vec<&str> = search_trimmed.split_whitespace()
                                .filter(|w| !w.is_empty() && *w != "and" && *w != "or" && *w != "not")
                                .collect();
                            let all_found = words.iter().all(|word| {
                                // Support prefix search with *
                                if word.ends_with('*') {
                                    let prefix = &word[..word.len() - 1];
                                    col_str.split_whitespace().any(|w| w.starts_with(prefix))
                                } else {
                                    col_str.contains(word)
                                }
                            });
                            Ok(Value::Boolean(all_found))
                        }
                    } else {
                        Ok(Value::Boolean(false))
                    }
                }
                // FREETEXT(column, 'text') — synonym for CONTAINS with word-level matching
                "FREETEXT" => {
                    if args.len() >= 2 {
                        let col_val = evaluate(&args[0], tuple, schema)?;
                        let search_val = evaluate(&args[1], tuple, schema)?;
                        if col_val.is_null() || search_val.is_null() {
                            return Ok(Value::Boolean(false));
                        }
                        let col_str = match &col_val {
                            Value::Varchar(s) => s.to_lowercase(),
                            other => other.to_string().to_lowercase(),
                        };
                        let search_str = match &search_val {
                            Value::Varchar(s) => s.to_lowercase(),
                            other => other.to_string().to_lowercase(),
                        };
                        // FREETEXT: any word matches (more lenient than CONTAINS)
                        let words: Vec<&str> = search_str.split_whitespace()
                            .filter(|w| !w.is_empty())
                            .collect();
                        let any_found = words.iter().any(|word| col_str.contains(word));
                        Ok(Value::Boolean(any_found))
                    } else {
                        Ok(Value::Boolean(false))
                    }
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
        Expr::InValues { expr, keys, negated, .. } => {
            let val = evaluate(expr, tuple, schema)?;
            if val.is_null() { return Ok(Value::Null); }
            let key = val.to_sort_key_bytes();
            let found = keys.contains(&key);
            Ok(Value::Boolean(if *negated { !found } else { found }))
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
        // ---- CASE expression ----
        Expr::Case { operand, when_clauses, else_result } => {
            if let Some(op_expr) = operand {
                // Simple CASE: CASE operand WHEN value THEN result ...
                let op_val = evaluate(op_expr, tuple, schema)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = evaluate(when_expr, tuple, schema)?;
                    if !op_val.is_null() && !when_val.is_null() {
                        let (l, r) = coerce_for_comparison(&op_val, &when_val);
                        if let Some(std::cmp::Ordering::Equal) = l.compare(&r) {
                            return evaluate(then_expr, tuple, schema);
                        }
                    }
                }
            } else {
                // Searched CASE: CASE WHEN condition THEN result ...
                for (when_expr, then_expr) in when_clauses {
                    let cond = eval_to_bool(when_expr, tuple, schema)?;
                    if cond {
                        return evaluate(then_expr, tuple, schema);
                    }
                }
            }
            // No WHEN matched: evaluate ELSE or return NULL
            if let Some(else_expr) = else_result {
                evaluate(else_expr, tuple, schema)
            } else {
                Ok(Value::Null)
            }
        }
        // ---- Subquery expressions (evaluated at executor level) ----
        Expr::Subquery(_) => {
            Err(ForgeError::Execution("subqueries evaluated at executor level".into()))
        }
        Expr::InSubquery { .. } => {
            Err(ForgeError::Execution("subqueries evaluated at executor level".into()))
        }
        Expr::Exists { .. } => {
            Err(ForgeError::Execution("subqueries evaluated at executor level".into()))
        }
        // ---- CAST expression ----
        Expr::Cast { expr, data_type } => {
            let val = evaluate(expr, tuple, schema)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            eval_cast(&val, data_type)
        }
        // ---- Window function (evaluated at projection level, not here) ----
        Expr::WindowFunction { .. } => {
            // Window functions are evaluated in the projection/aggregate layer,
            // not in the per-row eval. Return NULL as fallback.
            Ok(Value::Null)
        }
        // ---- Concat (|| operator) ----
        Expr::Concat { left, right } => {
            let l = evaluate(left, tuple, schema)?;
            let r = evaluate(right, tuple, schema)?;
            if l.is_null() || r.is_null() {
                return Ok(Value::Null);
            }
            let ls = l.to_string();
            let rs = r.to_string();
            Ok(Value::Varchar(format!("{}{}", ls, rs)))
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

/// Evaluate a binary operation on two already-computed values.
/// Public so that the aggregate module can reuse this for HAVING expressions.
pub fn eval_binary_op_values(left: &Value, op: &BinaryOperator, right: &Value) -> Result<Value> {
    eval_binary_op(left, op, right)
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
            // Fast path: Value::compare already handles cross-type promotion
            // (Int<->Float, Int<->BigInt, etc.) — skip cloning via coerce_for_comparison
            // when possible.
            let ordering = left.compare(right).or_else(|| {
                // Fallback: coerce for exotic cross-type pairs (Bool<->Int, DateTime<->Varchar)
                let (l, r) = coerce_for_comparison(left, right);
                l.compare(&r)
            }).ok_or_else(|| {
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
        BinaryOperator::Modulo => {
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            eval_modulo(left, right)
        }
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
        // Decimal <-> Integer
        (Value::Decimal(_, _), Value::Integer(_)) | (Value::Integer(_), Value::Decimal(_, _)) => {
            (left.clone(), right.clone()) // Value::compare handles cross-type
        }
        // Decimal <-> Float
        (Value::Decimal(_, _), Value::Float(_)) | (Value::Float(_), Value::Decimal(_, _)) => {
            (left.clone(), right.clone()) // Value::compare handles cross-type
        }
        // Varchar <-> Decimal (parse string to decimal)
        (Value::Varchar(s), Value::Decimal(_, scale)) => {
            if let Ok(f) = s.parse::<f64>() {
                let factor = 10f64.powi(*scale as i32);
                (Value::Decimal((f * factor).round() as i64, *scale), right.clone())
            } else {
                (left.clone(), right.clone())
            }
        }
        (Value::Decimal(_, scale), Value::Varchar(s)) => {
            if let Ok(f) = s.parse::<f64>() {
                let factor = 10f64.powi(*scale as i32);
                (left.clone(), Value::Decimal((f * factor).round() as i64, *scale))
            } else {
                (left.clone(), right.clone())
            }
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

// ------------------------------------------------------------------
// Helper functions for new expression types
// ------------------------------------------------------------------

/// Extract an i64 from a numeric Value.
fn value_to_i64(val: &Value) -> i64 {
    match val {
        Value::Integer(n) => *n as i64,
        Value::BigInt(n) => *n,
        Value::Float(f) => *f as i64,
        Value::Boolean(b) => if *b { 1 } else { 0 },
        Value::Decimal(v, scale) => v / 10i64.pow(*scale as u32),
        Value::Date(d) => *d as i64,
        Value::Time(t) => *t as i64,
        _ => 0,
    }
}

/// Extract an f64 from a numeric Value.
fn value_to_f64(val: &Value) -> f64 {
    match val {
        Value::Integer(n) => *n as f64,
        Value::BigInt(n) => *n as f64,
        Value::Float(f) => *f,
        Value::Boolean(b) => if *b { 1.0 } else { 0.0 },
        Value::Decimal(v, scale) => *v as f64 / 10f64.powi(*scale as i32),
        _ => 0.0,
    }
}

/// Convert a Value to an epoch timestamp (seconds since Unix epoch).
fn value_to_epoch(val: &Value) -> i64 {
    match val {
        Value::DateTime(e) => *e,
        Value::Varchar(s) => crate::tuple::tuple::parse_datetime_to_epoch(s),
        Value::Integer(n) => *n as i64,
        Value::BigInt(n) => *n,
        Value::Date(d) => (*d as i64) * 86400,
        _ => 0,
    }
}

/// Evaluate modulo operation between two numeric values.
fn eval_modulo(left: &Value, right: &Value) -> Result<Value> {
    match (left, right) {
        (Value::Integer(_), Value::Integer(0))
        | (Value::BigInt(_), Value::BigInt(0))
        | (Value::BigInt(_), Value::Integer(0))
        | (Value::Integer(_), Value::BigInt(0)) => {
            Err(ForgeError::Execution("modulo by zero".into()))
        }
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a % b)),
        (Value::BigInt(a), Value::BigInt(b)) => Ok(Value::BigInt(a % b)),
        (Value::BigInt(a), Value::Integer(b)) => Ok(Value::BigInt(a % *b as i64)),
        (Value::Integer(a), Value::BigInt(b)) => Ok(Value::BigInt(*a as i64 % b)),
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(ForgeError::Execution("modulo by zero".into()))
            } else {
                Ok(Value::Float(a % b))
            }
        }
        (Value::Integer(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(ForgeError::Execution("modulo by zero".into()))
            } else {
                Ok(Value::Float(*a as f64 % b))
            }
        }
        (Value::Float(a), Value::Integer(b)) => {
            if *b == 0 {
                Err(ForgeError::Execution("modulo by zero".into()))
            } else {
                Ok(Value::Float(a % *b as f64))
            }
        }
        (Value::BigInt(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(ForgeError::Execution("modulo by zero".into()))
            } else {
                Ok(Value::Float(*a as f64 % b))
            }
        }
        (Value::Float(a), Value::BigInt(b)) => {
            if *b == 0 {
                Err(ForgeError::Execution("modulo by zero".into()))
            } else {
                Ok(Value::Float(a % *b as f64))
            }
        }
        _ => Err(ForgeError::Execution(format!(
            "cannot compute modulo of {:?} and {:?}",
            left, right
        ))),
    }
}

/// Evaluate a CAST expression: convert a value to the target DataType.
fn eval_cast(val: &Value, target: &DataType) -> Result<Value> {
    match target {
        DataType::Integer => match val {
            Value::Integer(_) => Ok(val.clone()),
            Value::BigInt(n) => Ok(Value::Integer(*n as i32)),
            Value::Float(f) => Ok(Value::Integer(*f as i32)),
            Value::Varchar(s) => {
                // Try parsing as integer, then float
                if let Ok(n) = s.trim().parse::<i32>() {
                    Ok(Value::Integer(n))
                } else if let Ok(f) = s.trim().parse::<f64>() {
                    Ok(Value::Integer(f as i32))
                } else {
                    Ok(Value::Integer(0))
                }
            }
            Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            Value::DateTime(e) => Ok(Value::Integer(*e as i32)),
            Value::Null => Ok(Value::Null),
            Value::Decimal(v, s) => Ok(Value::Integer((v / 10i64.pow(*s as u32)) as i32)),
            _ => Ok(Value::Integer(0)),
        },
        DataType::BigInt => match val {
            Value::Integer(n) => Ok(Value::BigInt(*n as i64)),
            Value::BigInt(_) => Ok(val.clone()),
            Value::Float(f) => Ok(Value::BigInt(*f as i64)),
            Value::Varchar(s) => {
                if let Ok(n) = s.trim().parse::<i64>() {
                    Ok(Value::BigInt(n))
                } else if let Ok(f) = s.trim().parse::<f64>() {
                    Ok(Value::BigInt(f as i64))
                } else {
                    Ok(Value::BigInt(0))
                }
            }
            Value::Boolean(b) => Ok(Value::BigInt(if *b { 1 } else { 0 })),
            Value::DateTime(e) => Ok(Value::BigInt(*e)),
            Value::Null => Ok(Value::Null),
            Value::Decimal(v, s) => Ok(Value::BigInt(v / 10i64.pow(*s as u32))),
            _ => Ok(Value::BigInt(0)),
        },
        DataType::Float => match val {
            Value::Integer(n) => Ok(Value::Float(*n as f64)),
            Value::BigInt(n) => Ok(Value::Float(*n as f64)),
            Value::Float(_) => Ok(val.clone()),
            Value::Varchar(s) => {
                if let Ok(f) = s.trim().parse::<f64>() {
                    Ok(Value::Float(f))
                } else {
                    Ok(Value::Float(0.0))
                }
            }
            Value::Boolean(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
            Value::DateTime(e) => Ok(Value::Float(*e as f64)),
            Value::Null => Ok(Value::Null),
            Value::Decimal(v, s) => Ok(Value::Float(*v as f64 / 10f64.powi(*s as i32))),
            _ => Ok(Value::Float(0.0)),
        },
        DataType::Varchar(_) => {
            // Convert any value to its string representation
            Ok(Value::Varchar(val.to_string()))
        }
        DataType::Boolean => match val {
            Value::Boolean(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Boolean(*n != 0)),
            Value::BigInt(n) => Ok(Value::Boolean(*n != 0)),
            Value::Float(f) => Ok(Value::Boolean(*f != 0.0)),
            Value::Varchar(s) => {
                let lower = s.trim().to_lowercase();
                Ok(Value::Boolean(
                    lower == "true" || lower == "1" || lower == "yes" || lower == "on",
                ))
            }
            Value::DateTime(_) => Ok(Value::Boolean(true)),
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::Boolean(false)),
        },
        DataType::DateTime => match val {
            Value::DateTime(_) => Ok(val.clone()),
            Value::Varchar(s) => {
                let epoch = crate::tuple::tuple::parse_datetime_to_epoch(s);
                Ok(Value::DateTime(epoch))
            }
            Value::Integer(n) => Ok(Value::DateTime(*n as i64)),
            Value::BigInt(n) => Ok(Value::DateTime(*n)),
            Value::Float(f) => Ok(Value::DateTime(*f as i64)),
            Value::Boolean(_) => Err(ForgeError::Execution(
                "cannot cast boolean to datetime".into(),
            )),
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::DateTime(0)),
        },
        DataType::Decimal(_, scale) => {
            let factor = 10f64.powi(*scale as i32);
            match val {
                Value::Decimal(_, _) => Ok(val.clone()),
                Value::Integer(n) => Ok(Value::Decimal((*n as i64) * factor as i64, *scale)),
                Value::BigInt(n) => Ok(Value::Decimal(*n * factor as i64, *scale)),
                Value::Float(f) => Ok(Value::Decimal((*f * factor).round() as i64, *scale)),
                Value::Varchar(s) => {
                    if let Ok(f) = s.trim().parse::<f64>() {
                        Ok(Value::Decimal((f * factor).round() as i64, *scale))
                    } else {
                        Ok(Value::Decimal(0, *scale))
                    }
                }
                _ => Ok(Value::Decimal(0, *scale)),
            }
        }
        DataType::Date => match val {
            Value::Date(_) => Ok(val.clone()),
            Value::DateTime(e) => Ok(Value::Date((*e / 86400) as i32)),
            Value::Varchar(s) => {
                let epoch = crate::tuple::tuple::parse_datetime_to_epoch(s);
                Ok(Value::Date((epoch / 86400) as i32))
            }
            Value::Integer(n) => Ok(Value::Date(*n)),
            _ => Ok(Value::Date(0)),
        },
        DataType::Time => match val {
            Value::Time(_) => Ok(val.clone()),
            Value::DateTime(e) => Ok(Value::Time((*e % 86400) as i32)),
            Value::Varchar(s) => {
                let parts: Vec<&str> = s.split(':').collect();
                let h: i32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
                let m: i32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
                let sec: i32 = parts.get(2).and_then(|p| p.parse().ok()).unwrap_or(0);
                Ok(Value::Time(h * 3600 + m * 60 + sec))
            }
            _ => Ok(Value::Time(0)),
        },
        DataType::VarBinary(_) => match val {
            Value::Binary(_) => Ok(val.clone()),
            Value::Varchar(s) => Ok(Value::Binary(s.as_bytes().to_vec())),
            _ => Ok(Value::Binary(vec![])),
        },
        DataType::Json => match val {
            Value::Json(_) => Ok(val.clone()),
            Value::Varchar(s) => Ok(Value::Json(s.clone())),
            _ => Ok(Value::Json(val.to_string())),
        },
        DataType::Uuid => match val {
            Value::Uuid(_) => Ok(val.clone()),
            Value::Varchar(s) => Ok(Value::Uuid(s.clone())),
            _ => Ok(Value::Uuid(val.to_string())),
        },
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
                    is_unique: false,
                    check_expr: None, fk_ref: None,
            },
            Column {
                name: "name".into(),
                data_type: DataType::Varchar(100),
                nullable: true,
                column_id: 1,
                auto_increment: false,
                default_value: None,
                is_primary_key: false,
                    is_unique: false,
                    check_expr: None, fk_ref: None,
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

    // ---- New expression tests ----

    #[test]
    fn test_case_searched() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(5), Value::Varchar("Alice".into())];
        // CASE WHEN id > 3 THEN 'big' ELSE 'small' END
        let expr = Expr::Case {
            operand: None,
            when_clauses: vec![(
                Expr::BinaryOp {
                    left: Box::new(Expr::ColumnRef { table: None, column: "id".into() }),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
                },
                Expr::Literal(LiteralValue::String("big".into())),
            )],
            else_result: Some(Box::new(Expr::Literal(LiteralValue::String("small".into())))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("big".into())
        );
    }

    #[test]
    fn test_case_simple() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(2), Value::Varchar("Alice".into())];
        // CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END
        let expr = Expr::Case {
            operand: Some(Box::new(Expr::ColumnRef { table: None, column: "id".into() })),
            when_clauses: vec![
                (
                    Expr::Literal(LiteralValue::Integer(1)),
                    Expr::Literal(LiteralValue::String("one".into())),
                ),
                (
                    Expr::Literal(LiteralValue::Integer(2)),
                    Expr::Literal(LiteralValue::String("two".into())),
                ),
            ],
            else_result: Some(Box::new(Expr::Literal(LiteralValue::String("other".into())))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("two".into())
        );
    }

    #[test]
    fn test_case_no_match_no_else() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(99), Value::Varchar("Alice".into())];
        let expr = Expr::Case {
            operand: Some(Box::new(Expr::ColumnRef { table: None, column: "id".into() })),
            when_clauses: vec![(
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::String("one".into())),
            )],
            else_result: None,
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_cast_int_to_varchar() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(42), Value::Varchar("Alice".into())];
        let expr = Expr::Cast {
            expr: Box::new(Expr::ColumnRef { table: None, column: "id".into() }),
            data_type: DataType::Varchar(50),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("42".into())
        );
    }

    #[test]
    fn test_cast_varchar_to_int() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("123".into())];
        let expr = Expr::Cast {
            expr: Box::new(Expr::ColumnRef { table: None, column: "name".into() }),
            data_type: DataType::Integer,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Integer(123)
        );
    }

    #[test]
    fn test_cast_null() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Null];
        let expr = Expr::Cast {
            expr: Box::new(Expr::ColumnRef { table: None, column: "name".into() }),
            data_type: DataType::Integer,
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_concat_operator() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("Alice".into())];
        let expr = Expr::Concat {
            left: Box::new(Expr::Literal(LiteralValue::String("Hello ".into()))),
            right: Box::new(Expr::ColumnRef { table: None, column: "name".into() }),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("Hello Alice".into())
        );
    }

    #[test]
    fn test_concat_null() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Null];
        let expr = Expr::Concat {
            left: Box::new(Expr::Literal(LiteralValue::String("Hello ".into()))),
            right: Box::new(Expr::ColumnRef { table: None, column: "name".into() }),
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
    }

    #[test]
    fn test_modulo() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(10), Value::Varchar("x".into())];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef { table: None, column: "id".into() }),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Literal(LiteralValue::Integer(3))),
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Integer(1)
        );
    }

    #[test]
    fn test_modulo_by_zero() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(10), Value::Varchar("x".into())];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef { table: None, column: "id".into() }),
            op: BinaryOperator::Modulo,
            right: Box::new(Expr::Literal(LiteralValue::Integer(0))),
        };
        assert!(evaluate(&expr, &tuple, &schema).is_err());
    }

    #[test]
    fn test_upper_lower() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("Hello".into())];
        let upper_expr = Expr::Function {
            name: "UPPER".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&upper_expr, &tuple, &schema).unwrap(),
            Value::Varchar("HELLO".into())
        );
        let lower_expr = Expr::Function {
            name: "LOWER".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&lower_expr, &tuple, &schema).unwrap(),
            Value::Varchar("hello".into())
        );
    }

    #[test]
    fn test_trim_functions() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("  hi  ".into())];
        let trim = Expr::Function {
            name: "TRIM".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&trim, &tuple, &schema).unwrap(),
            Value::Varchar("hi".into())
        );
        let ltrim = Expr::Function {
            name: "LTRIM".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&ltrim, &tuple, &schema).unwrap(),
            Value::Varchar("hi  ".into())
        );
        let rtrim = Expr::Function {
            name: "RTRIM".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&rtrim, &tuple, &schema).unwrap(),
            Value::Varchar("  hi".into())
        );
    }

    #[test]
    fn test_length() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let expr = Expr::Function {
            name: "LENGTH".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Integer(5)
        );
    }

    #[test]
    fn test_substring() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello world".into())];
        // SUBSTRING('hello world', 7, 5) -> 'world'
        let expr = Expr::Function {
            name: "SUBSTRING".into(),
            args: vec![
                Expr::ColumnRef { table: None, column: "name".into() },
                Expr::Literal(LiteralValue::Integer(7)),
                Expr::Literal(LiteralValue::Integer(5)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("world".into())
        );
    }

    #[test]
    fn test_left_right() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let left_expr = Expr::Function {
            name: "LEFT".into(),
            args: vec![
                Expr::ColumnRef { table: None, column: "name".into() },
                Expr::Literal(LiteralValue::Integer(3)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&left_expr, &tuple, &schema).unwrap(),
            Value::Varchar("hel".into())
        );
        let right_expr = Expr::Function {
            name: "RIGHT".into(),
            args: vec![
                Expr::ColumnRef { table: None, column: "name".into() },
                Expr::Literal(LiteralValue::Integer(3)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&right_expr, &tuple, &schema).unwrap(),
            Value::Varchar("llo".into())
        );
    }

    #[test]
    fn test_replace() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello world".into())];
        let expr = Expr::Function {
            name: "REPLACE".into(),
            args: vec![
                Expr::ColumnRef { table: None, column: "name".into() },
                Expr::Literal(LiteralValue::String("world".into())),
                Expr::Literal(LiteralValue::String("rust".into())),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("hello rust".into())
        );
    }

    #[test]
    fn test_reverse() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hello".into())];
        let expr = Expr::Function {
            name: "REVERSE".into(),
            args: vec![Expr::ColumnRef { table: None, column: "name".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Varchar("olleh".into())
        );
    }

    #[test]
    fn test_lpad_rpad() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("hi".into())];
        let lpad = Expr::Function {
            name: "LPAD".into(),
            args: vec![
                Expr::ColumnRef { table: None, column: "name".into() },
                Expr::Literal(LiteralValue::Integer(5)),
                Expr::Literal(LiteralValue::String("*".into())),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&lpad, &tuple, &schema).unwrap(),
            Value::Varchar("***hi".into())
        );
        let rpad = Expr::Function {
            name: "RPAD".into(),
            args: vec![
                Expr::ColumnRef { table: None, column: "name".into() },
                Expr::Literal(LiteralValue::Integer(5)),
                Expr::Literal(LiteralValue::String("*".into())),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&rpad, &tuple, &schema).unwrap(),
            Value::Varchar("hi***".into())
        );
    }

    #[test]
    fn test_abs() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(-5), Value::Varchar("x".into())];
        let expr = Expr::Function {
            name: "ABS".into(),
            args: vec![Expr::ColumnRef { table: None, column: "id".into() }],
            distinct: false,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Integer(5)
        );
    }

    #[test]
    fn test_round() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let expr = Expr::Function {
            name: "ROUND".into(),
            args: vec![
                Expr::Literal(LiteralValue::Float(3.456)),
                Expr::Literal(LiteralValue::Integer(2)),
            ],
            distinct: false,
        };
        if let Value::Float(f) = evaluate(&expr, &tuple, &schema).unwrap() {
            assert!((f - 3.46).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_ceil_floor() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let ceil = Expr::Function {
            name: "CEIL".into(),
            args: vec![Expr::Literal(LiteralValue::Float(3.2))],
            distinct: false,
        };
        assert_eq!(
            evaluate(&ceil, &tuple, &schema).unwrap(),
            Value::Integer(4)
        );
        let floor = Expr::Function {
            name: "FLOOR".into(),
            args: vec![Expr::Literal(LiteralValue::Float(3.8))],
            distinct: false,
        };
        assert_eq!(
            evaluate(&floor, &tuple, &schema).unwrap(),
            Value::Integer(3)
        );
    }

    #[test]
    fn test_power_sqrt() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let power = Expr::Function {
            name: "POWER".into(),
            args: vec![
                Expr::Literal(LiteralValue::Integer(2)),
                Expr::Literal(LiteralValue::Integer(3)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&power, &tuple, &schema).unwrap(),
            Value::Float(8.0)
        );
        let sqrt = Expr::Function {
            name: "SQRT".into(),
            args: vec![Expr::Literal(LiteralValue::Integer(16))],
            distinct: false,
        };
        assert_eq!(
            evaluate(&sqrt, &tuple, &schema).unwrap(),
            Value::Float(4.0)
        );
    }

    #[test]
    fn test_sign() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let pos = Expr::Function {
            name: "SIGN".into(),
            args: vec![Expr::Literal(LiteralValue::Integer(42))],
            distinct: false,
        };
        assert_eq!(evaluate(&pos, &tuple, &schema).unwrap(), Value::Integer(1));
        let neg = Expr::Function {
            name: "SIGN".into(),
            args: vec![Expr::Literal(LiteralValue::Integer(-7))],
            distinct: false,
        };
        assert_eq!(evaluate(&neg, &tuple, &schema).unwrap(), Value::Integer(-1));
        let zero = Expr::Function {
            name: "SIGN".into(),
            args: vec![Expr::Literal(LiteralValue::Integer(0))],
            distinct: false,
        };
        assert_eq!(evaluate(&zero, &tuple, &schema).unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_mod_function() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let expr = Expr::Function {
            name: "MOD".into(),
            args: vec![
                Expr::Literal(LiteralValue::Integer(10)),
                Expr::Literal(LiteralValue::Integer(3)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Integer(1)
        );
    }

    #[test]
    fn test_nullif() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        // NULLIF(1, 1) -> NULL
        let expr = Expr::Function {
            name: "NULLIF".into(),
            args: vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(1)),
            ],
            distinct: false,
        };
        assert_eq!(evaluate(&expr, &tuple, &schema).unwrap(), Value::Null);
        // NULLIF(1, 2) -> 1
        let expr2 = Expr::Function {
            name: "NULLIF".into(),
            args: vec![
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(2)),
            ],
            distinct: false,
        };
        assert_eq!(evaluate(&expr2, &tuple, &schema).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_greatest_least() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let greatest = Expr::Function {
            name: "GREATEST".into(),
            args: vec![
                Expr::Literal(LiteralValue::Integer(3)),
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(7)),
                Expr::Literal(LiteralValue::Integer(2)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&greatest, &tuple, &schema).unwrap(),
            Value::Integer(7)
        );
        let least = Expr::Function {
            name: "LEAST".into(),
            args: vec![
                Expr::Literal(LiteralValue::Integer(3)),
                Expr::Literal(LiteralValue::Integer(1)),
                Expr::Literal(LiteralValue::Integer(7)),
                Expr::Literal(LiteralValue::Integer(2)),
            ],
            distinct: false,
        };
        assert_eq!(
            evaluate(&least, &tuple, &schema).unwrap(),
            Value::Integer(1)
        );
    }

    #[test]
    fn test_subquery_returns_error() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        use crate::sql::ast::{Statement, SelectColumn, FromClause};
        let subq = Expr::Subquery(Box::new(Statement::Select {
            distinct: false,
            columns: vec![SelectColumn::Expr {
                expr: Expr::Literal(LiteralValue::Integer(1)),
                alias: None,
            }],
            from: FromClause::Table { name: "t".into(), alias: None },
            r#where: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
            ctes: vec![],
        }));
        let result = evaluate(&subq, &tuple, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_cast_float_to_int() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal(LiteralValue::Float(3.7))),
            data_type: DataType::Integer,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Integer(3)
        );
    }

    #[test]
    fn test_cast_int_to_float() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal(LiteralValue::Integer(42))),
            data_type: DataType::Float,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Float(42.0)
        );
    }

    #[test]
    fn test_cast_to_boolean() {
        let schema = test_schema();
        let tuple = vec![Value::Integer(1), Value::Varchar("x".into())];
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal(LiteralValue::Integer(0))),
            data_type: DataType::Boolean,
        };
        assert_eq!(
            evaluate(&expr, &tuple, &schema).unwrap(),
            Value::Boolean(false)
        );
        let expr2 = Expr::Cast {
            expr: Box::new(Expr::Literal(LiteralValue::Integer(1))),
            data_type: DataType::Boolean,
        };
        assert_eq!(
            evaluate(&expr2, &tuple, &schema).unwrap(),
            Value::Boolean(true)
        );
    }
}
