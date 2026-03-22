use std::cmp::Ordering;
use std::fmt;

use crate::error::{ForgeError, Result};

/// The data types supported by ForgeDB.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// Signed 32-bit integer, 4 bytes.
    Integer,
    /// Signed 64-bit integer, 8 bytes.
    BigInt,
    /// 64-bit IEEE 754 floating point, 8 bytes.
    Float,
    /// Variable-length string with a maximum byte length.
    Varchar(u16),
    /// Single-byte boolean (0 or 1).
    Boolean,
    /// Date-time stored as i64 timestamp (seconds since epoch), 8 bytes.
    DateTime,
    /// Fixed-precision decimal: (precision, scale). Stored as i64 unscaled + u8 scale.
    Decimal(u8, u8),
    /// Date only (days since epoch), 4 bytes.
    Date,
    /// Time only (seconds since midnight), 4 bytes.
    Time,
    /// Variable-length binary data.
    VarBinary(u16),
    /// JSON stored as string internally.
    Json,
    /// UUID stored as string internally.
    Uuid,
}

/// A runtime value that can be stored in a tuple column.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    Float(f64),
    Varchar(String),
    Boolean(bool),
    DateTime(i64), // seconds since epoch
    /// Decimal: (unscaled_value, scale). E.g. 123.45 => (12345, 2)
    Decimal(i64, u8),
    /// Date: days since Unix epoch
    Date(i32),
    /// Time: seconds since midnight
    Time(i32),
    /// Binary data
    Binary(Vec<u8>),
    /// JSON stored as string
    Json(String),
    /// UUID stored as string
    Uuid(String),
    Null,
}

impl Value {
    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the [`DataType`] that corresponds to this value, or `None` for
    /// `Null` (which is type-less).
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Integer(_) => Some(DataType::Integer),
            Value::BigInt(_) => Some(DataType::BigInt),
            Value::Float(_) => Some(DataType::Float),
            Value::Varchar(s) => Some(DataType::Varchar(s.len() as u16)),
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::DateTime(_) => Some(DataType::DateTime),
            Value::Decimal(_, scale) => Some(DataType::Decimal(18, *scale)),
            Value::Date(_) => Some(DataType::Date),
            Value::Time(_) => Some(DataType::Time),
            Value::Binary(b) => Some(DataType::VarBinary(b.len() as u16)),
            Value::Json(_) => Some(DataType::Json),
            Value::Uuid(_) => Some(DataType::Uuid),
            Value::Null => None,
        }
    }

    /// Compare two values.
    ///
    /// Returns `None` when either operand is `Null` or when the types are
    /// incompatible. Integers and floats are cross-comparable (the integer is
    /// promoted to f64).
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,

            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),

            // Integer <-> Float promotion
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),

            // Integer <-> BigInt promotion
            (Value::Integer(a), Value::BigInt(b)) => (*a as i64).partial_cmp(b),
            (Value::BigInt(a), Value::Integer(b)) => a.partial_cmp(&(*b as i64)),

            // BigInt <-> Float promotion
            (Value::BigInt(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::BigInt(b)) => a.partial_cmp(&(*b as f64)),

            // DateTime <-> DateTime
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),

            (Value::Varchar(a), Value::Varchar(b)) => Some(a.cmp(b)),
            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),

            // Decimal comparisons
            (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
                // Normalize to same scale for comparison
                if sa == sb {
                    a.partial_cmp(b)
                } else if sa > sb {
                    let factor = 10i64.pow((*sa - *sb) as u32);
                    a.partial_cmp(&(b * factor))
                } else {
                    let factor = 10i64.pow((*sb - *sa) as u32);
                    (a * factor).partial_cmp(b)
                }
            }
            (Value::Decimal(a, sa), Value::Integer(b)) => {
                let factor = 10i64.pow(*sa as u32);
                a.partial_cmp(&((*b as i64) * factor))
            }
            (Value::Integer(a), Value::Decimal(b, sb)) => {
                let factor = 10i64.pow(*sb as u32);
                ((*a as i64) * factor).partial_cmp(b)
            }
            (Value::Decimal(a, sa), Value::Float(b)) => {
                let f = *a as f64 / 10f64.powi(*sa as i32);
                f.partial_cmp(b)
            }
            (Value::Float(a), Value::Decimal(b, sb)) => {
                let f = *b as f64 / 10f64.powi(*sb as i32);
                a.partial_cmp(&f)
            }

            // Date/Time comparisons
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),

            // JSON/UUID compare as strings
            (Value::Json(a), Value::Json(b)) => Some(a.cmp(b)),
            (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),

            // Binary compare
            (Value::Binary(a), Value::Binary(b)) => Some(a.cmp(b)),

            _ => None,
        }
    }

    // ------------------------------------------------------------------
    // Arithmetic helpers
    // ------------------------------------------------------------------

    /// Addition (`self + other`).
    pub fn add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_add(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| ForgeError::Tuple("integer overflow in add".into()))
            }
            (Value::BigInt(a), Value::BigInt(b)) => {
                a.checked_add(*b)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in add".into()))
            }
            (Value::BigInt(a), Value::Integer(b)) => {
                a.checked_add(*b as i64)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in add".into()))
            }
            (Value::Integer(a), Value::BigInt(b)) => {
                (*a as i64).checked_add(*b)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in add".into()))
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::BigInt(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::BigInt(b)) => Ok(Value::Float(a + *b as f64)),
            // Decimal arithmetic
            (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
                if sa == sb {
                    Ok(Value::Decimal(a + b, *sa))
                } else if sa > sb {
                    let factor = 10i64.pow((*sa - *sb) as u32);
                    Ok(Value::Decimal(a + b * factor, *sa))
                } else {
                    let factor = 10i64.pow((*sb - *sa) as u32);
                    Ok(Value::Decimal(a * factor + b, *sb))
                }
            }
            (Value::Decimal(a, sa), Value::Integer(b)) => {
                let factor = 10i64.pow(*sa as u32);
                Ok(Value::Decimal(a + (*b as i64) * factor, *sa))
            }
            (Value::Integer(a), Value::Decimal(b, sb)) => {
                let factor = 10i64.pow(*sb as u32);
                Ok(Value::Decimal((*a as i64) * factor + b, *sb))
            }
            _ => Err(ForgeError::Tuple(format!(
                "cannot add {:?} and {:?}",
                self, other
            ))),
        }
    }

    /// Subtraction (`self - other`).
    pub fn sub(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_sub(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| ForgeError::Tuple("integer overflow in sub".into()))
            }
            (Value::BigInt(a), Value::BigInt(b)) => {
                a.checked_sub(*b)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in sub".into()))
            }
            (Value::BigInt(a), Value::Integer(b)) => {
                a.checked_sub(*b as i64)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in sub".into()))
            }
            (Value::Integer(a), Value::BigInt(b)) => {
                (*a as i64).checked_sub(*b)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in sub".into()))
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
            (Value::BigInt(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::BigInt(b)) => Ok(Value::Float(a - *b as f64)),
            (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
                if sa == sb {
                    Ok(Value::Decimal(a - b, *sa))
                } else if sa > sb {
                    let factor = 10i64.pow((*sa - *sb) as u32);
                    Ok(Value::Decimal(a - b * factor, *sa))
                } else {
                    let factor = 10i64.pow((*sb - *sa) as u32);
                    Ok(Value::Decimal(a * factor - b, *sb))
                }
            }
            (Value::Decimal(a, sa), Value::Integer(b)) => {
                let factor = 10i64.pow(*sa as u32);
                Ok(Value::Decimal(a - (*b as i64) * factor, *sa))
            }
            (Value::Integer(a), Value::Decimal(b, sb)) => {
                let factor = 10i64.pow(*sb as u32);
                Ok(Value::Decimal((*a as i64) * factor - b, *sb))
            }
            _ => Err(ForgeError::Tuple(format!(
                "cannot subtract {:?} and {:?}",
                self, other
            ))),
        }
    }

    /// Multiplication (`self * other`).
    pub fn mul(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_mul(*b)
                    .map(Value::Integer)
                    .ok_or_else(|| ForgeError::Tuple("integer overflow in mul".into()))
            }
            (Value::BigInt(a), Value::BigInt(b)) => {
                a.checked_mul(*b)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in mul".into()))
            }
            (Value::BigInt(a), Value::Integer(b)) => {
                a.checked_mul(*b as i64)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in mul".into()))
            }
            (Value::Integer(a), Value::BigInt(b)) => {
                (*a as i64).checked_mul(*b)
                    .map(Value::BigInt)
                    .ok_or_else(|| ForgeError::Tuple("bigint overflow in mul".into()))
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
            (Value::BigInt(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::BigInt(b)) => Ok(Value::Float(a * *b as f64)),
            (Value::Decimal(a, sa), Value::Decimal(b, sb)) => {
                Ok(Value::Decimal(a * b / 10i64.pow(*sb as u32), *sa))
            }
            (Value::Decimal(a, sa), Value::Integer(b)) => {
                Ok(Value::Decimal(a * (*b as i64), *sa))
            }
            (Value::Integer(a), Value::Decimal(b, sb)) => {
                Ok(Value::Decimal((*a as i64) * b, *sb))
            }
            _ => Err(ForgeError::Tuple(format!(
                "cannot multiply {:?} and {:?}",
                self, other
            ))),
        }
    }

    /// Division (`self / other`).
    pub fn div(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(_), Value::Integer(0)) => {
                Err(ForgeError::Tuple("division by zero".into()))
            }
            (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(a / b)),
            (Value::BigInt(_), Value::BigInt(0)) => {
                Err(ForgeError::Tuple("division by zero".into()))
            }
            (Value::BigInt(a), Value::BigInt(b)) => Ok(Value::BigInt(a / b)),
            (Value::BigInt(_), Value::Integer(0)) => {
                Err(ForgeError::Tuple("division by zero".into()))
            }
            (Value::BigInt(a), Value::Integer(b)) => Ok(Value::BigInt(a / *b as i64)),
            (Value::Integer(_), Value::BigInt(0)) => {
                Err(ForgeError::Tuple("division by zero".into()))
            }
            (Value::Integer(a), Value::BigInt(b)) => Ok(Value::BigInt(*a as i64 / b)),
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(ForgeError::Tuple("division by zero".into()))
                } else {
                    Ok(Value::Float(a / b))
                }
            }
            (Value::Integer(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(ForgeError::Tuple("division by zero".into()))
                } else {
                    Ok(Value::Float(*a as f64 / b))
                }
            }
            (Value::Float(a), Value::Integer(b)) => {
                if *b == 0 {
                    Err(ForgeError::Tuple("division by zero".into()))
                } else {
                    Ok(Value::Float(a / *b as f64))
                }
            }
            (Value::BigInt(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(ForgeError::Tuple("division by zero".into()))
                } else {
                    Ok(Value::Float(*a as f64 / b))
                }
            }
            (Value::Float(a), Value::BigInt(b)) => {
                if *b == 0 {
                    Err(ForgeError::Tuple("division by zero".into()))
                } else {
                    Ok(Value::Float(a / *b as f64))
                }
            }
            _ => Err(ForgeError::Tuple(format!(
                "cannot divide {:?} by {:?}",
                self, other
            ))),
        }
    }

    // ------------------------------------------------------------------
    // Sort-key encoding for B-tree indices
    // ------------------------------------------------------------------

    /// Produce a byte sequence that preserves the natural ordering of the
    /// value so that `memcmp`-style comparison on the resulting bytes yields
    /// the same ordering as [`Value::compare`].
    ///
    /// Encoding scheme:
    /// - **Integer**: tag `0x02`, then `i32` with its sign bit flipped so that
    ///   negative values sort before positive ones (big-endian).
    /// - **Float**: tag `0x03`, then `f64` bits with sign handling so that
    ///   ordering is preserved (big-endian).
    /// - **Varchar**: tag `0x04`, then raw UTF-8 bytes.
    /// - **Boolean**: tag `0x05`, then `0x00` (false) or `0x01` (true).
    /// - **Null**: tag `0x00` (sorts before every non-null value).
    pub fn to_sort_key_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![0x00],

            Value::Integer(v) => {
                let mut buf = Vec::with_capacity(5);
                buf.push(0x02);
                // Flip sign bit so that ordering is preserved under unsigned
                // byte comparison.
                let flipped = (*v as u32) ^ 0x8000_0000;
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }

            Value::BigInt(v) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(0x06);
                // Flip sign bit so that ordering is preserved under unsigned
                // byte comparison (8-byte version).
                let flipped = (*v as u64) ^ 0x8000_0000_0000_0000;
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }

            Value::Float(v) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(0x03);
                let bits = v.to_bits();
                // If the sign bit is set (negative number or -0), flip all
                // bits; otherwise flip only the sign bit.  This maps the IEEE
                // 754 total order to an unsigned byte order.
                let encoded = if bits & (1u64 << 63) != 0 {
                    !bits
                } else {
                    bits ^ (1u64 << 63)
                };
                buf.extend_from_slice(&encoded.to_be_bytes());
                buf
            }

            Value::Varchar(s) => {
                let mut buf = Vec::with_capacity(1 + s.len());
                buf.push(0x04);
                buf.extend_from_slice(s.as_bytes());
                buf
            }

            Value::Boolean(b) => {
                vec![0x05, if *b { 0x01 } else { 0x00 }]
            }

            Value::DateTime(v) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(0x07);
                let flipped = (*v as u64) ^ 0x8000_0000_0000_0000;
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }

            Value::Decimal(v, _scale) => {
                let mut buf = Vec::with_capacity(9);
                buf.push(0x08);
                let flipped = (*v as u64) ^ 0x8000_0000_0000_0000;
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }

            Value::Date(v) => {
                let mut buf = Vec::with_capacity(5);
                buf.push(0x09);
                let flipped = (*v as u32) ^ 0x8000_0000;
                buf.extend_from_slice(&flipped.to_be_bytes());
                buf
            }

            Value::Time(v) => {
                let mut buf = Vec::with_capacity(5);
                buf.push(0x0A);
                buf.extend_from_slice(&(*v as u32).to_be_bytes());
                buf
            }

            Value::Binary(b) => {
                let mut buf = Vec::with_capacity(1 + b.len());
                buf.push(0x0B);
                buf.extend_from_slice(b);
                buf
            }

            Value::Json(s) => {
                let mut buf = Vec::with_capacity(1 + s.len());
                buf.push(0x0C);
                buf.extend_from_slice(s.as_bytes());
                buf
            }

            Value::Uuid(s) => {
                let mut buf = Vec::with_capacity(1 + s.len());
                buf.push(0x0D);
                buf.extend_from_slice(s.as_bytes());
                buf
            }
        }
    }
}

// ------------------------------------------------------------------
// Display
// ------------------------------------------------------------------

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{}", v),
            Value::BigInt(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "{}", v),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::DateTime(epoch) => write!(f, "{}", format_epoch_datetime(*epoch)),
            Value::Decimal(unscaled, scale) => {
                if *scale == 0 {
                    write!(f, "{}", unscaled)
                } else {
                    let divisor = 10i64.pow(*scale as u32);
                    let int_part = unscaled / divisor;
                    let frac_part = (unscaled % divisor).unsigned_abs();
                    if *unscaled < 0 && int_part == 0 {
                        write!(f, "-0.{:0>width$}", frac_part, width = *scale as usize)
                    } else {
                        write!(f, "{}.{:0>width$}", int_part, frac_part, width = *scale as usize)
                    }
                }
            }
            Value::Date(days) => {
                let epoch = (*days as i64) * 86400;
                let s = format_epoch_datetime(epoch);
                // Return only the date portion
                write!(f, "{}", &s[..10])
            }
            Value::Time(secs) => {
                let h = secs / 3600;
                let m = (secs % 3600) / 60;
                let s = secs % 60;
                write!(f, "{:02}:{:02}:{:02}", h, m, s)
            }
            Value::Binary(b) => {
                write!(f, "0x")?;
                for byte in b {
                    write!(f, "{:02X}", byte)?;
                }
                Ok(())
            }
            Value::Json(s) => write!(f, "{}", s),
            Value::Uuid(s) => write!(f, "{}", s),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// Convert epoch seconds to "YYYY-MM-DD HH:MM:SS" string without external crates.
fn format_epoch_datetime(epoch: i64) -> String {
    // Handle negative epochs (before 1970) by clamping to epoch 0 for simplicity.
    let secs = if epoch < 0 { 0 } else { epoch as u64 };

    let sec_of_day = (secs % 86400) as u32;
    let hour = sec_of_day / 3600;
    let minute = (sec_of_day % 3600) / 60;
    let second = sec_of_day % 60;

    // Compute date from days since 1970-01-01 using a civil calendar algorithm.
    let mut days = (secs / 86400) as i64;

    // Shift epoch from 1970-01-01 to 0000-03-01 for easier leap year handling.
    days += 719468; // days from 0000-03-01 to 1970-01-01

    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u64; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month index [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let year = if m <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, m, d, hour, minute, second
    )
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Integer(1).is_null());
    }

    #[test]
    fn test_data_type() {
        assert_eq!(Value::Integer(0).data_type(), Some(DataType::Integer));
        assert_eq!(Value::Float(0.0).data_type(), Some(DataType::Float));
        assert_eq!(
            Value::Varchar("hi".into()).data_type(),
            Some(DataType::Varchar(2))
        );
        assert_eq!(Value::Boolean(true).data_type(), Some(DataType::Boolean));
        assert_eq!(Value::Null.data_type(), None);
    }

    #[test]
    fn test_compare_same_type() {
        assert_eq!(
            Value::Integer(1).compare(&Value::Integer(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Float(3.14).compare(&Value::Float(2.71)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Varchar("abc".into()).compare(&Value::Varchar("abd".into())),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Boolean(false).compare(&Value::Boolean(true)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn test_compare_cross_numeric() {
        assert_eq!(
            Value::Integer(2).compare(&Value::Float(2.0)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_compare_null_is_none() {
        assert_eq!(Value::Null.compare(&Value::Null), None);
        assert_eq!(Value::Integer(1).compare(&Value::Null), None);
    }

    #[test]
    fn test_compare_incompatible_types() {
        assert_eq!(Value::Integer(1).compare(&Value::Boolean(true)), None);
    }

    #[test]
    fn test_arithmetic_integer() {
        let a = Value::Integer(10);
        let b = Value::Integer(3);
        assert_eq!(a.add(&b).unwrap(), Value::Integer(13));
        assert_eq!(a.sub(&b).unwrap(), Value::Integer(7));
        assert_eq!(a.mul(&b).unwrap(), Value::Integer(30));
        assert_eq!(a.div(&b).unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_arithmetic_float() {
        let a = Value::Float(10.0);
        let b = Value::Float(3.0);
        assert_eq!(a.add(&b).unwrap(), Value::Float(13.0));
        assert_eq!(a.sub(&b).unwrap(), Value::Float(7.0));
        assert_eq!(a.mul(&b).unwrap(), Value::Float(30.0));
        // Float division is not exact, but 10/3 should be close.
        if let Value::Float(r) = a.div(&b).unwrap() {
            assert!((r - 10.0 / 3.0).abs() < 1e-12);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn test_arithmetic_mixed() {
        assert_eq!(
            Value::Integer(2).add(&Value::Float(0.5)).unwrap(),
            Value::Float(2.5)
        );
    }

    #[test]
    fn test_div_by_zero() {
        assert!(Value::Integer(1).div(&Value::Integer(0)).is_err());
        assert!(Value::Float(1.0).div(&Value::Float(0.0)).is_err());
    }

    #[test]
    fn test_arithmetic_type_error() {
        assert!(Value::Integer(1).add(&Value::Boolean(true)).is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Boolean(true)), "true");
        assert_eq!(format!("{}", Value::Varchar("hello".into())), "hello");
    }

    #[test]
    fn test_sort_key_ordering_integers() {
        let vals = [
            Value::Integer(i32::MIN),
            Value::Integer(-1),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(i32::MAX),
        ];
        for w in vals.windows(2) {
            assert!(
                w[0].to_sort_key_bytes() < w[1].to_sort_key_bytes(),
                "{:?} should sort before {:?}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn test_sort_key_ordering_floats() {
        let vals = [
            Value::Float(-1000.0),
            Value::Float(-1.0),
            Value::Float(0.0),
            Value::Float(1.0),
            Value::Float(1000.0),
        ];
        for w in vals.windows(2) {
            assert!(
                w[0].to_sort_key_bytes() < w[1].to_sort_key_bytes(),
                "{:?} should sort before {:?}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn test_sort_key_null_sorts_first() {
        assert!(Value::Null.to_sort_key_bytes() < Value::Integer(i32::MIN).to_sort_key_bytes());
        assert!(Value::Null.to_sort_key_bytes() < Value::Boolean(false).to_sort_key_bytes());
    }
}
