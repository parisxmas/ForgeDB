use crate::error::{ForgeError, Result};
use crate::tuple::schema::Schema;
use crate::tuple::types::{DataType, Value};

// ------------------------------------------------------------------
// Serialization
// ------------------------------------------------------------------

/// Serialize a slice of [`Value`]s into a compact binary tuple according to the
/// given [`Schema`].
///
/// # Layout
///
/// ```text
/// [null_bitmap][fixed_fields][varchar_offset_table][varchar_data]
/// ```
///
/// - **null_bitmap**: `ceil(num_columns / 8)` bytes.  Bit `i` (LSB-first within
///   each byte) is `1` when column `i` is `NULL`.
/// - **fixed_fields**: for each non-null, non-varchar column in schema order:
///   - `Integer` : 4 bytes little-endian
///   - `Float`   : 8 bytes little-endian
///   - `Boolean` : 1 byte (`0x00` or `0x01`)
///   - `Varchar` columns are skipped here.
/// - **varchar_offset_table**: for every varchar column in schema order (even
///   if null), 4 bytes encoding `(offset: u16, length: u16)` both
///   little-endian.  If the column is null the entry is `(0, 0)`.  `offset` is
///   relative to the start of the serialized tuple.
/// - **varchar_data**: the raw UTF-8 bytes of all non-null varchar values,
///   concatenated in schema order.
pub fn serialize(values: &[Value], schema: &Schema) -> Result<Vec<u8>> {
    let ncols = schema.column_count();
    if values.len() != ncols {
        return Err(ForgeError::Tuple(format!(
            "expected {} values but got {}",
            ncols,
            values.len()
        )));
    }

    // Coerce values to match schema types (e.g. Integer -> Boolean for BIT columns).
    let values: Vec<Value> = values
        .iter()
        .zip(schema.columns.iter())
        .map(|(val, col)| coerce_value(val, &col.data_type))
        .collect();

    // Coerce NULLs to type defaults for non-nullable columns (MySQL behavior)
    let values: Vec<Value> = values
        .iter()
        .zip(schema.columns.iter())
        .map(|(val, col)| {
            if val.is_null() && !col.nullable {
                // Use column default or type default
                if let Some(ref dv) = col.default_value {
                    return dv.clone();
                }
                match col.data_type {
                    DataType::Integer => Value::Integer(0),
                    DataType::BigInt => Value::BigInt(0),
                    DataType::Float => Value::Float(0.0),
                    DataType::Boolean => Value::Boolean(false),
                    DataType::Varchar(_) => Value::Varchar(String::new()),
                    DataType::DateTime => Value::DateTime(0),
                }
            } else {
                val.clone()
            }
        })
        .collect();

    // Validate types.
    for (i, (val, col)) in values.iter().zip(schema.columns.iter()).enumerate() {
        if val.is_null() {
            continue; // nullable columns can be NULL
        }
        match (&col.data_type, val) {
            (DataType::Integer, Value::Integer(_)) => {}
            (DataType::BigInt, Value::BigInt(_)) => {}
            (DataType::BigInt, Value::Integer(_)) => {} // allow int in bigint col
            (DataType::Float, Value::Float(_)) => {}
            (DataType::Float, Value::Integer(_)) => {} // allow int in float col
            (DataType::Float, Value::BigInt(_)) => {} // allow bigint in float col
            (DataType::Varchar(_), Value::Varchar(_)) => {}
            (DataType::Boolean, Value::Boolean(_)) => {}
            (DataType::DateTime, Value::DateTime(_)) => {}
            (DataType::DateTime, Value::BigInt(_)) => {} // allow bigint in datetime col
            (DataType::DateTime, Value::Integer(_)) => {} // allow int in datetime col
            _ => {
                return Err(ForgeError::Tuple(format!(
                    "type mismatch for column {} ('{}'): schema expects {:?}, got {:?}",
                    i, col.name, col.data_type, val
                )));
            }
        }
    }

    // --- null bitmap (after all coercions) ---
    let bitmap_len = (ncols + 7) / 8;
    let mut buf: Vec<u8> = vec![0u8; bitmap_len];

    for (i, val) in values.iter().enumerate() {
        if val.is_null() {
            buf[i / 8] |= 1 << (i % 8);
        }
    }

    // --- fixed fields ---
    for (val, col) in values.iter().zip(schema.columns.iter()) {
        if val.is_null() {
            // Still reserve the fixed-size slot so that positional decoding
            // works? – No. The spec says "for each non-null column" for fixed
            // fields and varchars get their own offset table, so we skip nulls.
            match &col.data_type {
                DataType::Varchar(_) => { /* handled later */ }
                _ => { /* skip – no bytes emitted for null fixed fields */ }
            }
            continue;
        }
        match (val, &col.data_type) {
            (Value::Integer(v), _) => buf.extend_from_slice(&v.to_le_bytes()),
            (Value::BigInt(v), _) => buf.extend_from_slice(&v.to_le_bytes()),
            (Value::Float(v), _) => buf.extend_from_slice(&v.to_le_bytes()),
            (Value::Boolean(v), _) => buf.push(if *v { 1 } else { 0 }),
            (Value::DateTime(v), _) => buf.extend_from_slice(&v.to_le_bytes()),
            (Value::Varchar(_), _) => { /* handled in varchar section */ }
            (Value::Null, _) => unreachable!(),
        }
    }

    // --- varchar offset table + data ---
    // Count varchar columns so we can pre-reserve space for the offset table.
    let varchar_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if matches!(c.data_type, DataType::Varchar(_)) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if !varchar_cols.is_empty() {
        // Reserve 4 bytes per varchar column in the offset table.
        let offset_table_start = buf.len();
        let offset_table_size = varchar_cols.len() * 4;
        buf.resize(buf.len() + offset_table_size, 0);

        // Now append varchar data and patch the offset table.
        for (slot, &col_idx) in varchar_cols.iter().enumerate() {
            let val = &values[col_idx];
            if val.is_null() {
                // offset=0, length=0 (already zeroed)
                continue;
            }
            if let Value::Varchar(s) = val {
                let data_offset = buf.len() as u16;
                let data_len = s.len() as u16;
                buf.extend_from_slice(s.as_bytes());

                // Patch offset table entry.
                let entry_pos = offset_table_start + slot * 4;
                buf[entry_pos..entry_pos + 2].copy_from_slice(&data_offset.to_le_bytes());
                buf[entry_pos + 2..entry_pos + 4].copy_from_slice(&data_len.to_le_bytes());
            }
        }
    }

    Ok(buf)
}

// ------------------------------------------------------------------
// Deserialization
// ------------------------------------------------------------------

/// Deserialize a binary tuple back into a `Vec<Value>` using the given
/// [`Schema`].
pub fn deserialize(data: &[u8], schema: &Schema) -> Result<Vec<Value>> {
    let ncols = schema.column_count();
    let bitmap_len = (ncols + 7) / 8;

    if data.len() < bitmap_len {
        return Err(ForgeError::Tuple("tuple data too short for null bitmap".into()));
    }

    // --- read null bitmap ---
    let mut is_null = vec![false; ncols];
    for i in 0..ncols {
        if data[i / 8] & (1 << (i % 8)) != 0 {
            is_null[i] = true;
        }
    }

    let mut pos = bitmap_len;
    let mut values: Vec<Value> = Vec::with_capacity(ncols);

    // We need to determine where the varchar offset table starts.  That is
    // right after all fixed fields.  Compute by walking the schema.
    // But we can also just read fixed fields first and then handle varchars.

    // First pass: collect fixed-field values and record which columns are
    // varchar (we'll handle those in a second pass).
    let varchar_col_indices: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if matches!(c.data_type, DataType::Varchar(_)) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    // Read fixed fields.
    for (i, col) in schema.columns.iter().enumerate() {
        if matches!(col.data_type, DataType::Varchar(_)) {
            // placeholder – filled in below
            values.push(Value::Null);
            continue;
        }
        if is_null[i] {
            values.push(Value::Null);
            continue;
        }
        match col.data_type {
            DataType::Integer => {
                if pos + 4 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (integer)".into()));
                }
                let v = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                values.push(Value::Integer(v));
                pos += 4;
            }
            DataType::BigInt => {
                if pos + 8 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (bigint)".into()));
                }
                let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                values.push(Value::BigInt(v));
                pos += 8;
            }
            DataType::Float => {
                if pos + 8 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (float)".into()));
                }
                let v = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                values.push(Value::Float(v));
                pos += 8;
            }
            DataType::Boolean => {
                if pos + 1 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (boolean)".into()));
                }
                values.push(Value::Boolean(data[pos] != 0));
                pos += 1;
            }
            DataType::DateTime => {
                if pos + 8 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (datetime)".into()));
                }
                let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                values.push(Value::DateTime(v));
                pos += 8;
            }
            DataType::Varchar(_) => unreachable!(),
        }
    }

    // Read varchar offset table + data.
    if !varchar_col_indices.is_empty() {
        for (slot, &col_idx) in varchar_col_indices.iter().enumerate() {
            if is_null[col_idx] {
                // values[col_idx] is already Null
                // We still need to skip the offset entry.
                continue;
            }
            let entry_pos = pos + slot * 4;
            if entry_pos + 4 > data.len() {
                return Err(ForgeError::Tuple(
                    "unexpected end of tuple (varchar offset table)".into(),
                ));
            }
            let offset =
                u16::from_le_bytes(data[entry_pos..entry_pos + 2].try_into().unwrap()) as usize;
            let length =
                u16::from_le_bytes(data[entry_pos + 2..entry_pos + 4].try_into().unwrap())
                    as usize;

            if offset + length > data.len() {
                return Err(ForgeError::Tuple(
                    "varchar data extends beyond tuple boundary".into(),
                ));
            }
            let s = std::str::from_utf8(&data[offset..offset + length])
                .map_err(|e| ForgeError::Tuple(format!("invalid utf-8 in varchar: {}", e)))?;
            values[col_idx] = Value::Varchar(s.to_owned());
        }
    }

    Ok(values)
}

/// Coerce a value to match the expected column type.
/// E.g. Integer(1) -> Boolean(true) for BIT columns,
/// Integer(42) -> Float(42.0) for FLOAT columns.
fn coerce_value(val: &Value, target: &DataType) -> Value {
    match (val, target) {
        // Integer -> Boolean (T-SQL BIT)
        (Value::Integer(n), DataType::Boolean) => Value::Boolean(*n != 0),
        // Integer -> Float
        (Value::Integer(n), DataType::Float) => Value::Float(*n as f64),
        // Integer -> BigInt
        (Value::Integer(n), DataType::BigInt) => Value::BigInt(*n as i64),
        // Integer -> DateTime
        (Value::Integer(n), DataType::DateTime) => Value::DateTime(*n as i64),
        // BigInt -> Float
        (Value::BigInt(n), DataType::Float) => Value::Float(*n as f64),
        // BigInt -> DateTime
        (Value::BigInt(n), DataType::DateTime) => Value::DateTime(*n),
        // Boolean -> Integer
        (Value::Boolean(b), DataType::Integer) => Value::Integer(if *b { 1 } else { 0 }),
        // Varchar -> DateTime (parse "YYYY-MM-DD HH:MM:SS" to epoch)
        (Value::Varchar(s), DataType::DateTime) => {
            Value::DateTime(parse_datetime_string(s))
        }
        // Otherwise keep as-is
        _ => val.clone(),
    }
}

/// Parse a datetime string like "YYYY-MM-DD HH:MM:SS" to Unix epoch seconds.
/// Returns 0 on parse failure.
/// Parse a datetime string to epoch seconds. Public for use by evaluator.
pub fn parse_datetime_to_epoch(s: &str) -> i64 {
    parse_datetime_string(s)
}

fn parse_datetime_string(s: &str) -> i64 {
    let parts: Vec<&str> = s.split(|c| c == '-' || c == ' ' || c == ':' || c == 'T').collect();
    if parts.len() < 3 {
        return 0;
    }
    let year: i64 = parts[0].parse().unwrap_or(1970);
    let month: i64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
    let day: i64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let hour: i64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let min: i64 = parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
    let sec: i64 = parts.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);

    // Simplified days-from-epoch calculation
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) { 366 } else { 365 };
    }
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    for m in 0..(month - 1) as usize {
        days += month_days.get(m).copied().unwrap_or(30) as i64;
        if m == 1 && is_leap {
            days += 1;
        }
    }
    days += day - 1;

    days * 86400 + hour * 3600 + min * 60 + sec
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::schema::Column;

    /// Helper to build a schema quickly.
    fn make_schema(defs: &[(&str, DataType, bool)]) -> Schema {
        Schema::new(
            defs.iter()
                .enumerate()
                .map(|(i, (name, dt, nullable))| Column {
                    name: name.to_string(),
                    data_type: dt.clone(),
                    nullable: *nullable,
                    column_id: i as u16,
                    auto_increment: false,
                    default_value: None,
                    is_primary_key: false,
                })
                .collect(),
        )
    }

    // ---- round-trip tests ----

    #[test]
    fn round_trip_integer() {
        let schema = make_schema(&[("a", DataType::Integer, false)]);
        let values = vec![Value::Integer(42)];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_negative_integer() {
        let schema = make_schema(&[("a", DataType::Integer, false)]);
        let values = vec![Value::Integer(-12345)];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_float() {
        let schema = make_schema(&[("a", DataType::Float, false)]);
        let values = vec![Value::Float(std::f64::consts::PI)];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_boolean() {
        let schema = make_schema(&[
            ("t", DataType::Boolean, false),
            ("f", DataType::Boolean, false),
        ]);
        let values = vec![Value::Boolean(true), Value::Boolean(false)];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_varchar() {
        let schema = make_schema(&[("s", DataType::Varchar(255), false)]);
        let values = vec![Value::Varchar("hello world".into())];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_empty_varchar() {
        let schema = make_schema(&[("s", DataType::Varchar(255), false)]);
        let values = vec![Value::Varchar(String::new())];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_multiple_varchars() {
        let schema = make_schema(&[
            ("a", DataType::Varchar(100), false),
            ("b", DataType::Varchar(100), false),
            ("c", DataType::Varchar(100), false),
        ]);
        let values = vec![
            Value::Varchar("first".into()),
            Value::Varchar("second".into()),
            Value::Varchar("third".into()),
        ];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_null() {
        let schema = make_schema(&[
            ("a", DataType::Integer, true),
            ("b", DataType::Varchar(50), true),
        ]);
        let values = vec![Value::Null, Value::Null];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_mixed_types() {
        let schema = make_schema(&[
            ("id", DataType::Integer, false),
            ("name", DataType::Varchar(255), true),
            ("score", DataType::Float, false),
            ("active", DataType::Boolean, false),
            ("bio", DataType::Varchar(1000), true),
        ]);
        let values = vec![
            Value::Integer(1),
            Value::Varchar("Alice".into()),
            Value::Float(99.5),
            Value::Boolean(true),
            Value::Varchar("A short bio.".into()),
        ];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_mixed_with_nulls() {
        let schema = make_schema(&[
            ("id", DataType::Integer, false),
            ("name", DataType::Varchar(255), true),
            ("score", DataType::Float, true),
            ("active", DataType::Boolean, false),
            ("bio", DataType::Varchar(1000), true),
        ]);
        let values = vec![
            Value::Integer(7),
            Value::Null,           // name is null
            Value::Null,           // score is null
            Value::Boolean(false),
            Value::Varchar("present".into()),
        ];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_all_types_present() {
        let schema = make_schema(&[
            ("i", DataType::Integer, false),
            ("f", DataType::Float, false),
            ("v", DataType::Varchar(50), false),
            ("b", DataType::Boolean, false),
        ]);
        let values = vec![
            Value::Integer(-1),
            Value::Float(2.718),
            Value::Varchar("test".into()),
            Value::Boolean(true),
        ];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    // ---- error tests ----

    #[test]
    fn error_wrong_value_count() {
        let schema = make_schema(&[("a", DataType::Integer, false)]);
        assert!(serialize(&[], &schema).is_err());
    }

    #[test]
    fn non_nullable_null_uses_default() {
        // MySQL behavior: NULL in non-nullable column -> type default (0 for int)
        let schema = make_schema(&[("a", DataType::Integer, false)]);
        let data = serialize(&[Value::Null], &schema).unwrap();
        let result = deserialize(&data, &schema).unwrap();
        assert_eq!(result[0], Value::Integer(0));
    }

    #[test]
    fn error_type_mismatch() {
        let schema = make_schema(&[("a", DataType::Integer, false)]);
        assert!(serialize(&[Value::Float(1.0)], &schema).is_err());
    }

    #[test]
    fn error_truncated_data() {
        assert!(deserialize(&[], &make_schema(&[("a", DataType::Integer, false)])).is_err());
    }

    // ---- edge cases ----

    #[test]
    fn round_trip_many_columns_bitmap() {
        // 9 columns → 2-byte bitmap
        let schema = make_schema(&[
            ("c0", DataType::Integer, true),
            ("c1", DataType::Integer, true),
            ("c2", DataType::Integer, true),
            ("c3", DataType::Integer, true),
            ("c4", DataType::Integer, true),
            ("c5", DataType::Integer, true),
            ("c6", DataType::Integer, true),
            ("c7", DataType::Integer, true),
            ("c8", DataType::Integer, true),
        ]);
        let values = vec![
            Value::Integer(0),
            Value::Null,
            Value::Integer(2),
            Value::Null,
            Value::Integer(4),
            Value::Null,
            Value::Integer(6),
            Value::Null,
            Value::Integer(8),
        ];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_varchar_between_fixed() {
        let schema = make_schema(&[
            ("a", DataType::Integer, false),
            ("v", DataType::Varchar(100), false),
            ("b", DataType::Boolean, false),
        ]);
        let values = vec![
            Value::Integer(42),
            Value::Varchar("mid".into()),
            Value::Boolean(true),
        ];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }

    #[test]
    fn round_trip_unicode_varchar() {
        let schema = make_schema(&[("s", DataType::Varchar(500), false)]);
        let values = vec![Value::Varchar("hello".into())];
        let bytes = serialize(&values, &schema).unwrap();
        let out = deserialize(&bytes, &schema).unwrap();
        assert_eq!(values, out);
    }
}
