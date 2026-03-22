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
                    DataType::Decimal(_, s) => Value::Decimal(0, s),
                    DataType::Date => Value::Date(0),
                    DataType::Time => Value::Time(0),
                    DataType::VarBinary(_) => Value::Binary(vec![]),
                    DataType::Json => Value::Json(String::new()),
                    DataType::Uuid => Value::Uuid(String::new()),
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
            // New types
            (DataType::Decimal(_, _), Value::Decimal(_, _)) => {}
            (DataType::Decimal(_, _), Value::Integer(_)) => {} // allow int in decimal col
            (DataType::Decimal(_, _), Value::BigInt(_)) => {} // allow bigint in decimal col
            (DataType::Decimal(_, _), Value::Float(_)) => {} // allow float in decimal col
            (DataType::Date, Value::Date(_)) => {}
            (DataType::Date, Value::DateTime(_)) => {} // allow datetime in date col
            (DataType::Date, Value::Integer(_)) => {} // allow int in date col
            (DataType::Time, Value::Time(_)) => {}
            (DataType::Time, Value::Integer(_)) => {} // allow int in time col
            (DataType::VarBinary(_), Value::Binary(_)) => {}
            (DataType::VarBinary(_), Value::Varchar(_)) => {} // allow varchar in binary col
            (DataType::Json, Value::Json(_)) => {}
            (DataType::Json, Value::Varchar(_)) => {} // allow varchar in json col
            (DataType::Uuid, Value::Uuid(_)) => {}
            (DataType::Uuid, Value::Varchar(_)) => {} // allow varchar in uuid col
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
            (Value::Decimal(v, scale), _) => {
                buf.extend_from_slice(&v.to_le_bytes());
                buf.push(*scale);
            }
            (Value::Date(v), _) => buf.extend_from_slice(&v.to_le_bytes()),
            (Value::Time(v), _) => buf.extend_from_slice(&v.to_le_bytes()),
            (Value::Varchar(_), _) => { /* handled in varchar section */ }
            (Value::Binary(_), _) => { /* handled in varchar section as variable-length */ }
            (Value::Json(_), _) => { /* handled in varchar section as variable-length */ }
            (Value::Uuid(_), _) => { /* handled in varchar section as variable-length */ }
            (Value::Null, _) => unreachable!(),
        }
    }

    // --- varchar offset table + data ---
    // Count varchar-like columns (variable-length) so we can pre-reserve space.
    let varchar_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if matches!(c.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
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
            let (var_data, var_len) = match val {
                Value::Varchar(s) => (s.as_bytes().to_vec(), s.len()),
                Value::Json(s) => (s.as_bytes().to_vec(), s.len()),
                Value::Uuid(s) => (s.as_bytes().to_vec(), s.len()),
                Value::Binary(b) => (b.clone(), b.len()),
                _ => continue,
            };
            let data_offset = buf.len() as u16;
            let data_len = var_len as u16;
            buf.extend_from_slice(&var_data);

            // Patch offset table entry.
            let entry_pos = offset_table_start + slot * 4;
            buf[entry_pos..entry_pos + 2].copy_from_slice(&data_offset.to_le_bytes());
            buf[entry_pos + 2..entry_pos + 4].copy_from_slice(&data_len.to_le_bytes());
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
            if matches!(c.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    // Read fixed fields.
    for (i, col) in schema.columns.iter().enumerate() {
        if matches!(col.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
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
            DataType::Decimal(_, _) => {
                if pos + 9 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (decimal)".into()));
                }
                let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                let scale = data[pos + 8];
                values.push(Value::Decimal(v, scale));
                pos += 9;
            }
            DataType::Date => {
                if pos + 4 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (date)".into()));
                }
                let v = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                values.push(Value::Date(v));
                pos += 4;
            }
            DataType::Time => {
                if pos + 4 > data.len() {
                    return Err(ForgeError::Tuple("unexpected end of tuple (time)".into()));
                }
                let v = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                values.push(Value::Time(v));
                pos += 4;
            }
            DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid => unreachable!(),
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
            match schema.columns[col_idx].data_type {
                DataType::VarBinary(_) => {
                    values[col_idx] = Value::Binary(data[offset..offset + length].to_vec());
                }
                DataType::Json => {
                    let s = std::str::from_utf8(&data[offset..offset + length])
                        .map_err(|e| ForgeError::Tuple(format!("invalid utf-8 in json: {}", e)))?;
                    values[col_idx] = Value::Json(s.to_owned());
                }
                DataType::Uuid => {
                    let s = std::str::from_utf8(&data[offset..offset + length])
                        .map_err(|e| ForgeError::Tuple(format!("invalid utf-8 in uuid: {}", e)))?;
                    values[col_idx] = Value::Uuid(s.to_owned());
                }
                _ => {
                    let s = std::str::from_utf8(&data[offset..offset + length])
                        .map_err(|e| ForgeError::Tuple(format!("invalid utf-8 in varchar: {}", e)))?;
                    values[col_idx] = Value::Varchar(s.to_owned());
                }
            }
        }
    }

    Ok(values)
}

/// Deserialize a single column from a binary tuple without deserializing
/// all columns. Significantly faster when only one column value is needed
/// (e.g., for SUM/AVG aggregates on a single column).
///
/// Returns `Value::Null` for null columns. Skips all other columns.
pub fn deserialize_single_column(data: &[u8], schema: &Schema, target_col: usize) -> Result<Value> {
    let ncols = schema.column_count();
    if target_col >= ncols {
        return Err(ForgeError::Tuple(format!(
            "column index {} out of range ({})", target_col, ncols
        )));
    }
    let bitmap_len = (ncols + 7) / 8;
    if data.len() < bitmap_len {
        return Err(ForgeError::Tuple("tuple data too short for null bitmap".into()));
    }

    // Check if target column is null
    if data[target_col / 8] & (1 << (target_col % 8)) != 0 {
        return Ok(Value::Null);
    }

    let target_type = &schema.columns[target_col].data_type;

    // If the target is a variable-length type, we need to find the offset table
    if matches!(target_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
        // Count varchar columns before the target and compute the slot index
        let mut varchar_slot = 0;
        let mut pos = bitmap_len;
        for (i, col) in schema.columns.iter().enumerate() {
            if matches!(col.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
                if i == target_col {
                    // Found it — read from offset table at position `pos`
                    // First, skip all fixed fields to find offset table start
                    let mut fixed_pos = bitmap_len;
                    for (j, c) in schema.columns.iter().enumerate() {
                        if matches!(c.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
                            continue;
                        }
                        if data[j / 8] & (1 << (j % 8)) != 0 {
                            continue; // null
                        }
                        fixed_pos += fixed_field_size(&c.data_type);
                    }
                    // offset table starts at fixed_pos
                    let entry_pos = fixed_pos + varchar_slot * 4;
                    if entry_pos + 4 > data.len() {
                        return Err(ForgeError::Tuple("unexpected end of tuple (varchar offset)".into()));
                    }
                    let offset = u16::from_le_bytes(data[entry_pos..entry_pos + 2].try_into().unwrap()) as usize;
                    let length = u16::from_le_bytes(data[entry_pos + 2..entry_pos + 4].try_into().unwrap()) as usize;
                    if offset + length > data.len() {
                        return Err(ForgeError::Tuple("varchar data extends beyond tuple boundary".into()));
                    }
                    return match target_type {
                        DataType::VarBinary(_) => Ok(Value::Binary(data[offset..offset + length].to_vec())),
                        DataType::Json => {
                            let s = std::str::from_utf8(&data[offset..offset + length])
                                .map_err(|e| ForgeError::Tuple(format!("invalid utf-8: {}", e)))?;
                            Ok(Value::Json(s.to_owned()))
                        }
                        DataType::Uuid => {
                            let s = std::str::from_utf8(&data[offset..offset + length])
                                .map_err(|e| ForgeError::Tuple(format!("invalid utf-8: {}", e)))?;
                            Ok(Value::Uuid(s.to_owned()))
                        }
                        _ => {
                            let s = std::str::from_utf8(&data[offset..offset + length])
                                .map_err(|e| ForgeError::Tuple(format!("invalid utf-8: {}", e)))?;
                            Ok(Value::Varchar(s.to_owned()))
                        }
                    };
                }
                varchar_slot += 1;
            }
        }
        return Ok(Value::Null); // shouldn't reach here
    }

    // Fixed-size column: skip all preceding non-null fixed columns
    let mut pos = bitmap_len;
    for (i, col) in schema.columns.iter().enumerate() {
        if matches!(col.data_type, DataType::Varchar(_) | DataType::VarBinary(_) | DataType::Json | DataType::Uuid) {
            continue; // skip varchars in the fixed region
        }
        if i == target_col {
            // Read the value at this position
            return read_fixed_value(data, pos, target_type);
        }
        if data[i / 8] & (1 << (i % 8)) != 0 {
            continue; // null — no bytes emitted
        }
        pos += fixed_field_size(&col.data_type);
    }

    Ok(Value::Null)
}

#[inline]
fn fixed_field_size(dt: &DataType) -> usize {
    match dt {
        DataType::Integer => 4,
        DataType::BigInt => 8,
        DataType::Float => 8,
        DataType::Boolean => 1,
        DataType::DateTime => 8,
        DataType::Decimal(_, _) => 9,
        DataType::Date => 4,
        DataType::Time => 4,
        _ => 0,
    }
}

fn read_fixed_value(data: &[u8], pos: usize, dt: &DataType) -> Result<Value> {
    match dt {
        DataType::Integer => {
            if pos + 4 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::Integer(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap())))
        }
        DataType::BigInt => {
            if pos + 8 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::BigInt(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap())))
        }
        DataType::Float => {
            if pos + 8 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::Float(f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap())))
        }
        DataType::Boolean => {
            if pos + 1 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::Boolean(data[pos] != 0))
        }
        DataType::DateTime => {
            if pos + 8 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::DateTime(i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap())))
        }
        DataType::Decimal(_, _) => {
            if pos + 9 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            let v = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            let scale = data[pos + 8];
            Ok(Value::Decimal(v, scale))
        }
        DataType::Date => {
            if pos + 4 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::Date(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap())))
        }
        DataType::Time => {
            if pos + 4 > data.len() {
                return Err(ForgeError::Tuple("unexpected end of tuple".into()));
            }
            Ok(Value::Time(i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap())))
        }
        _ => Ok(Value::Null),
    }
}

/// Coerce a value to match the expected column type.
/// E.g. Integer(1) -> Boolean(true) for BIT columns,
/// Integer(42) -> Float(42.0) for FLOAT columns.
/// Public coercion for index lookups.
pub fn coerce_value_pub(val: &Value, target: &DataType) -> Value {
    coerce_value(val, target)
}

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
        // Varchar -> Integer (MySQL sends quoted numbers: '1', '0')
        (Value::Varchar(s), DataType::Integer) => {
            s.parse::<i32>().map(Value::Integer).unwrap_or(Value::Integer(0))
        }
        // Varchar -> BigInt
        (Value::Varchar(s), DataType::BigInt) => {
            s.parse::<i64>().map(Value::BigInt).unwrap_or(Value::BigInt(0))
        }
        // Varchar -> Float
        (Value::Varchar(s), DataType::Float) => {
            s.parse::<f64>().map(Value::Float).unwrap_or(Value::Float(0.0))
        }
        // Varchar -> Boolean
        (Value::Varchar(s), DataType::Boolean) => {
            Value::Boolean(s != "0" && !s.is_empty())
        }
        // Varchar -> DateTime (parse "YYYY-MM-DD HH:MM:SS" to epoch)
        (Value::Varchar(s), DataType::DateTime) => {
            Value::DateTime(parse_datetime_string(s))
        }
        // Integer -> Decimal
        (Value::Integer(n), DataType::Decimal(_, scale)) => {
            Value::Decimal((*n as i64) * 10i64.pow(*scale as u32), *scale)
        }
        // BigInt -> Decimal
        (Value::BigInt(n), DataType::Decimal(_, scale)) => {
            Value::Decimal(*n * 10i64.pow(*scale as u32), *scale)
        }
        // Float -> Decimal
        (Value::Float(f), DataType::Decimal(_, scale)) => {
            let factor = 10f64.powi(*scale as i32);
            Value::Decimal((*f * factor).round() as i64, *scale)
        }
        // Varchar -> Decimal
        (Value::Varchar(s), DataType::Decimal(_, scale)) => {
            if let Ok(f) = s.parse::<f64>() {
                let factor = 10f64.powi(*scale as i32);
                Value::Decimal((f * factor).round() as i64, *scale)
            } else {
                Value::Decimal(0, *scale)
            }
        }
        // DateTime -> Date (extract date part)
        (Value::DateTime(e), DataType::Date) => {
            Value::Date((*e / 86400) as i32)
        }
        // Varchar -> Date
        (Value::Varchar(s), DataType::Date) => {
            let epoch = parse_datetime_string(s);
            Value::Date((epoch / 86400) as i32)
        }
        // Varchar -> Time
        (Value::Varchar(s), DataType::Time) => {
            // Parse HH:MM:SS
            let parts: Vec<&str> = s.split(':').collect();
            let h: i32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
            let m: i32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            let sec: i32 = parts.get(2).and_then(|p| p.parse().ok()).unwrap_or(0);
            Value::Time(h * 3600 + m * 60 + sec)
        }
        // Integer -> Date
        (Value::Integer(n), DataType::Date) => Value::Date(*n),
        // Integer -> Time
        (Value::Integer(n), DataType::Time) => Value::Time(*n),
        // Varchar -> Json
        (Value::Varchar(s), DataType::Json) => Value::Json(s.clone()),
        // Varchar -> Uuid
        (Value::Varchar(s), DataType::Uuid) => Value::Uuid(s.clone()),
        // Varchar -> Binary
        (Value::Varchar(s), DataType::VarBinary(_)) => Value::Binary(s.as_bytes().to_vec()),
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
                    is_unique: false,
                    check_expr: None, fk_ref: None,
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
