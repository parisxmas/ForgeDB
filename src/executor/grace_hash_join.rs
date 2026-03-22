//! Grace hash join for large data sets.
//!
//! When the build side exceeds `HASH_JOIN_MEMORY_LIMIT` rows, the data is
//! partitioned by `hash(join_key) % num_partitions`, written to temp files,
//! then each partition pair is joined in memory and the partition is freed
//! before moving to the next.

use std::collections::HashMap;

use crate::common::{PageId, RID};
use crate::error::{ForgeError, Result};
use crate::sql::ast::JoinType;
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::hash_join;
use super::temp_storage::{self, TempFileManager};

/// Maximum rows on the build side before switching to grace hash join.
pub const HASH_JOIN_MEMORY_LIMIT: usize = 50_000;

/// Number of partitions for grace hash join.
const NUM_PARTITIONS: usize = 16;

/// Execute a grace hash join by partitioning both sides.
///
/// Memory strategy:
/// 1. Partition both sides into temp files, freeing each row after writing
/// 2. For each partition pair, read from disk, join in memory, free, move on
/// Peak memory ≈ max(single partition) instead of entire dataset.
pub fn grace_hash_join(
    left_rows: &[(RID, Vec<Value>)],
    right_rows: &[(RID, Vec<Value>)],
    join_type: &JoinType,
    left_key_idx: usize,
    right_key_idx: usize,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<(Schema, Vec<(RID, Vec<Value>)>)> {
    let mut temp_mgr = TempFileManager::new()
        .map_err(|e| ForgeError::Execution(format!("grace hash join temp dir: {}", e)))?;

    // Phase 1: Partition both sides to temp files
    // We write each partition to a separate temp file so we can read them
    // back one at a time without holding all data in memory.
    let mut left_writers: Vec<Option<std::io::BufWriter<std::fs::File>>> = Vec::new();
    let mut right_writers: Vec<Option<std::io::BufWriter<std::fs::File>>> = Vec::new();
    let mut left_paths = Vec::new();
    let mut right_paths = Vec::new();

    for i in 0..NUM_PARTITIONS {
        let ltf = temp_mgr.create_file(&format!("ghj_left_{}", i))
            .map_err(|e| ForgeError::Execution(format!("temp file: {}", e)))?;
        left_paths.push(ltf.path().to_path_buf());
        left_writers.push(Some(ltf.writer()
            .map_err(|e| ForgeError::Execution(format!("temp writer: {}", e)))?));

        let rtf = temp_mgr.create_file(&format!("ghj_right_{}", i))
            .map_err(|e| ForgeError::Execution(format!("temp file: {}", e)))?;
        right_paths.push(rtf.path().to_path_buf());
        right_writers.push(Some(rtf.writer()
            .map_err(|e| ForgeError::Execution(format!("temp writer: {}", e)))?));
    }

    // Write left rows to partition files
    for (_, vals) in left_rows {
        let idx = left_key_idx.min(vals.len().saturating_sub(1));
        let part = hash_value(&vals[idx]) % NUM_PARTITIONS;
        let data = temp_storage::serialize_row(vals);
        if let Some(ref mut w) = left_writers[part] {
            std::io::Write::write_all(w, &data)
                .map_err(|e| ForgeError::Execution(format!("write partition: {}", e)))?;
        }
    }

    // Write right rows to partition files
    for (_, vals) in right_rows {
        let idx = right_key_idx.min(vals.len().saturating_sub(1));
        let part = hash_value(&vals[idx]) % NUM_PARTITIONS;
        let data = temp_storage::serialize_row(vals);
        if let Some(ref mut w) = right_writers[part] {
            std::io::Write::write_all(w, &data)
                .map_err(|e| ForgeError::Execution(format!("write partition: {}", e)))?;
        }
    }

    // Flush and close all writers — releases file handles and write buffers
    for w in left_writers.iter_mut() {
        if let Some(writer) = w.take() {
            drop(writer);
        }
    }
    for w in right_writers.iter_mut() {
        if let Some(writer) = w.take() {
            drop(writer);
        }
    }
    drop(left_writers);
    drop(right_writers);

    // Phase 2: Join each partition pair, reading from disk
    let combined_schema = hash_join::build_combined_schema_pub(left_schema, right_schema);
    let mut all_results = Vec::new();
    let dummy_rid = RID { page_id: PageId(0), slot_id: 0 };

    for part_idx in 0..NUM_PARTITIONS {
        // Read left partition from disk
        let left_part = read_partition(&left_paths[part_idx])?;
        let right_part = read_partition(&right_paths[part_idx])?;

        if left_part.is_empty() && right_part.is_empty() {
            continue;
        }
        if matches!(join_type, JoinType::Inner | JoinType::Cross) {
            if left_part.is_empty() || right_part.is_empty() {
                continue;
            }
        }

        // Join this partition pair in memory
        let partition_results = partition_hash_join(
            &left_part,
            &right_part,
            join_type,
            left_key_idx,
            right_key_idx,
            left_schema,
            right_schema,
        )?;

        // left_part and right_part are dropped here — memory freed before next partition
        all_results.extend(partition_results);
    }

    // Temp files cleaned up when temp_mgr drops
    drop(temp_mgr);

    Ok((combined_schema, all_results))
}

/// Read all rows from a partition temp file.
fn read_partition(path: &std::path::Path) -> Result<Vec<Vec<Value>>> {
    let f = std::fs::File::open(path)
        .map_err(|e| ForgeError::Execution(format!("open partition: {}", e)))?;
    let mut reader = std::io::BufReader::with_capacity(65536, f);
    temp_storage::read_all_rows(&mut reader)
        .map_err(|e| ForgeError::Execution(format!("read partition: {}", e)))
}

/// In-memory hash join for a single partition.
fn partition_hash_join(
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    join_type: &JoinType,
    left_key_idx: usize,
    right_key_idx: usize,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Vec<(RID, Vec<Value>)>> {
    let dummy_rid = RID { page_id: PageId(0), slot_id: 0 };
    let right_null_count = right_schema.columns.len();
    let left_null_count = left_schema.columns.len();
    let combined_width = left_null_count + right_null_count;

    // Build hash table on right side
    let mut right_hash: HashMap<u64, Vec<usize>> = HashMap::with_capacity(right_rows.len());
    for (i, vals) in right_rows.iter().enumerate() {
        if right_key_idx < vals.len() {
            let key = hash_value_u64(&vals[right_key_idx]);
            right_hash.entry(key).or_default().push(i);
        }
    }

    let mut result = Vec::new();

    match join_type {
        JoinType::Inner => {
            for lvals in left_rows {
                if left_key_idx < lvals.len() {
                    let key = hash_value_u64(&lvals[left_key_idx]);
                    if let Some(indices) = right_hash.get(&key) {
                        for &ri in indices {
                            if values_equal(&lvals[left_key_idx], &right_rows[ri][right_key_idx]) {
                                let mut combined = Vec::with_capacity(combined_width);
                                combined.extend_from_slice(lvals);
                                combined.extend_from_slice(&right_rows[ri]);
                                result.push((dummy_rid, combined));
                            }
                        }
                    }
                }
            }
        }
        JoinType::Left => {
            for lvals in left_rows {
                let mut matched = false;
                if left_key_idx < lvals.len() {
                    let key = hash_value_u64(&lvals[left_key_idx]);
                    if let Some(indices) = right_hash.get(&key) {
                        for &ri in indices {
                            if values_equal(&lvals[left_key_idx], &right_rows[ri][right_key_idx]) {
                                let mut combined = Vec::with_capacity(combined_width);
                                combined.extend_from_slice(lvals);
                                combined.extend_from_slice(&right_rows[ri]);
                                result.push((dummy_rid, combined));
                                matched = true;
                            }
                        }
                    }
                }
                if !matched {
                    let mut combined = Vec::with_capacity(combined_width);
                    combined.extend_from_slice(lvals);
                    combined.extend(std::iter::repeat(Value::Null).take(right_null_count));
                    result.push((dummy_rid, combined));
                }
            }
        }
        JoinType::Right | JoinType::Full | JoinType::Cross => {
            for lvals in left_rows {
                for rvals in right_rows {
                    if left_key_idx < lvals.len()
                        && right_key_idx < rvals.len()
                        && values_equal(&lvals[left_key_idx], &rvals[right_key_idx])
                    {
                        let mut combined = Vec::with_capacity(combined_width);
                        combined.extend_from_slice(lvals);
                        combined.extend_from_slice(rvals);
                        result.push((dummy_rid, combined));
                    }
                }
            }
        }
    }

    Ok(result)
}

fn hash_value(v: &Value) -> usize {
    hash_value_u64(v) as usize
}

fn hash_value_u64(v: &Value) -> u64 {
    match v {
        Value::Integer(n) => *n as u64,
        Value::BigInt(n) => *n as u64,
        Value::Float(f) => (*f).to_bits(),
        Value::Varchar(s) => {
            // FNV-1a hash
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in s.bytes() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hash
        }
        Value::Boolean(b) => *b as u64,
        Value::DateTime(t) => *t as u64,
        Value::Decimal(v, _) => *v as u64,
        Value::Date(d) => *d as u64,
        Value::Time(t) => *t as u64,
        Value::Binary(b) => {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in b {
                hash ^= *byte as u64;
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hash
        }
        Value::Json(s) | Value::Uuid(s) => {
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in s.bytes() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hash
        }
        Value::Null => 0,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    matches!(a.compare(b), Some(std::cmp::Ordering::Equal))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::schema::{Column, Schema};
    use crate::tuple::types::DataType;

    fn make_schema(cols: &[(&str, DataType)]) -> Schema {
        Schema::new(
            cols.iter()
                .enumerate()
                .map(|(i, (name, dt))| Column {
                    name: name.to_string(),
                    data_type: dt.clone(),
                    nullable: true,
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

    #[test]
    fn test_grace_hash_join_inner() {
        let left_schema = make_schema(&[("id", DataType::Integer), ("name", DataType::Varchar(100))]);
        let right_schema = make_schema(&[("user_id", DataType::Integer), ("total", DataType::Float)]);

        let dummy_rid = RID { page_id: PageId(0), slot_id: 0 };
        let left_rows: Vec<(RID, Vec<Value>)> = (0..100)
            .map(|i| (dummy_rid, vec![Value::Integer(i), Value::Varchar(format!("user_{}", i))]))
            .collect();
        let right_rows: Vec<(RID, Vec<Value>)> = (50..150)
            .map(|i| (dummy_rid, vec![Value::Integer(i), Value::Float(i as f64 * 1.5)]))
            .collect();

        let (schema, result) = grace_hash_join(
            &left_rows,
            &right_rows,
            &JoinType::Inner,
            0,
            0,
            &left_schema,
            &right_schema,
        )
        .unwrap();

        // Overlap is 50..100 = 50 rows
        assert_eq!(result.len(), 50);
        assert_eq!(schema.columns.len(), 4);
    }
}
