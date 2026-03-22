//! External merge sort for large result sets.
//!
//! When the number of rows exceeds `SORT_MEMORY_LIMIT`, the data is split
//! into sorted runs written to temp files, then merged using a streaming
//! k-way merge with a min-heap — only one row per run is in memory at a time.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::BufReader;

use crate::error::{ForgeError, Result};
use crate::sql::ast::OrderByItem;
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::eval::evaluate;
use super::temp_storage::{self, TempFileManager};

/// Maximum number of rows to sort in memory before spilling.
pub const SORT_MEMORY_LIMIT: usize = 10_000;

/// Perform an external merge sort on the given rows.
///
/// 1. Split input into chunks of `SORT_MEMORY_LIMIT` rows
/// 2. Sort each chunk in memory, write to temp file, **drop the chunk**
/// 3. Streaming k-way merge: read one row at a time from each run file
pub fn external_sort(
    order_by: &[OrderByItem],
    rows: &mut Vec<Vec<Value>>,
    schema: &Schema,
) -> Result<()> {
    if rows.len() <= SORT_MEMORY_LIMIT {
        in_memory_sort(order_by, rows, schema)?;
        return Ok(());
    }

    let mut temp_mgr = TempFileManager::new()
        .map_err(|e| ForgeError::Execution(format!("failed to create temp dir: {}", e)))?;

    // Phase 1: Create sorted runs, writing each chunk to disk and freeing it
    let total_rows = rows.len();
    let mut run_files = Vec::new();
    let mut run_count = 0usize;

    {
        // Take ownership of rows so we can drain them chunk by chunk
        let mut offset = 0;
        while offset < total_rows {
            let end = (offset + SORT_MEMORY_LIMIT).min(total_rows);

            // Clone this chunk (we can't drain a Vec by range easily)
            let mut chunk: Vec<Vec<Value>> = rows[offset..end].to_vec();
            // Clear the source slots to free their memory immediately
            for row in &mut rows[offset..end] {
                *row = Vec::new();
            }

            in_memory_sort(order_by, &mut chunk, schema)?;

            let tf = temp_mgr
                .create_file(&format!("sort_run_{}", run_count))
                .map_err(|e| ForgeError::Execution(format!("temp file: {}", e)))?;

            let mut writer = tf
                .writer()
                .map_err(|e| ForgeError::Execution(format!("temp writer: {}", e)))?;
            temp_storage::write_rows(&mut writer, &chunk)
                .map_err(|e| ForgeError::Execution(format!("write run: {}", e)))?;
            drop(writer); // flush and close
            drop(chunk); // free the sorted chunk memory

            run_files.push(tf.path().to_path_buf());
            run_count += 1;
            offset = end;
        }
    }

    // The original rows are now empty shells — free that allocation entirely
    rows.clear();
    rows.shrink_to_fit();

    // Phase 2: Streaming k-way merge from disk
    // Open one reader per run, read one row ahead from each
    let ascending_flags: Vec<bool> = order_by.iter().map(|o| o.ascending).collect();
    let mut readers: Vec<BufReader<std::fs::File>> = Vec::with_capacity(run_count);
    for path in &run_files {
        let f = std::fs::File::open(path)
            .map_err(|e| ForgeError::Execution(format!("open run: {}", e)))?;
        readers.push(BufReader::with_capacity(65536, f));
    }

    // Seed the heap with the first row from each run
    let mut heap: BinaryHeap<StreamEntry> = BinaryHeap::new();
    for (run_idx, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = temp_storage::deserialize_row(reader)
            .map_err(|e| ForgeError::Execution(format!("read run: {}", e)))?
        {
            let keys = compute_sort_keys(order_by, &row, schema)?;
            heap.push(StreamEntry {
                keys,
                row,
                ascending: ascending_flags.clone(),
                run_idx,
            });
        }
    }

    // Merge: pop smallest, push next from the same run
    let mut result = Vec::with_capacity(total_rows);
    while let Some(entry) = heap.pop() {
        let run_idx = entry.run_idx;
        result.push(entry.row);
        // entry.keys is dropped here — freed

        // Read next row from the same run
        if let Some(row) = temp_storage::deserialize_row(&mut readers[run_idx])
            .map_err(|e| ForgeError::Execution(format!("read run: {}", e)))?
        {
            let keys = compute_sort_keys(order_by, &row, schema)?;
            heap.push(StreamEntry {
                keys,
                row,
                ascending: ascending_flags.clone(),
                run_idx,
            });
        }
    }

    // Drop readers (closes file handles) and temp files
    drop(readers);
    drop(temp_mgr);

    *rows = result;
    Ok(())
}

fn compute_sort_keys(
    order_by: &[OrderByItem],
    row: &[Value],
    schema: &Schema,
) -> Result<Vec<Value>> {
    let mut keys = Vec::with_capacity(order_by.len());
    for item in order_by {
        let val = evaluate(&item.expr, row, schema)?;
        keys.push(val);
    }
    Ok(keys)
}

fn in_memory_sort(
    order_by: &[OrderByItem],
    rows: &mut [Vec<Value>],
    schema: &Schema,
) -> Result<()> {
    super::sort::execute_sort(order_by, rows, schema)
}

/// Entry in the streaming merge heap. Carries the actual row so we only
/// hold one row per run in memory at any time.
/// Implements reverse ordering so `BinaryHeap` acts as a min-heap.
struct StreamEntry {
    keys: Vec<Value>,
    row: Vec<Value>,
    ascending: Vec<bool>,
    run_idx: usize,
}

impl PartialEq for StreamEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for StreamEntry {}

impl PartialOrd for StreamEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
        for (i, asc) in self.ascending.iter().enumerate() {
            let a = &self.keys[i];
            let b = &other.keys[i];

            match (a.is_null(), b.is_null()) {
                (true, true) => continue,
                (true, false) => return Ordering::Less,  // NULL last → goes to bottom of heap
                (false, true) => return Ordering::Greater,
                _ => {}
            }

            if let Some(ord) = a.compare(b) {
                let ord = if *asc { ord } else { ord.reverse() };
                if ord != Ordering::Equal {
                    return ord.reverse(); // reverse for min-heap
                }
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{Expr, OrderByItem};
    use crate::tuple::schema::{Column, Schema};
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
    fn test_external_sort_small() {
        let schema = test_schema();
        let order_by = vec![OrderByItem {
            expr: Expr::ColumnRef {
                table: None,
                column: "id".into(),
            },
            ascending: true,
        }];

        let mut rows: Vec<Vec<Value>> = (0..100)
            .rev()
            .map(|i| vec![Value::Integer(i), Value::Varchar(format!("row_{}", i))])
            .collect();

        external_sort(&order_by, &mut rows, &schema).unwrap();
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], Value::Integer(i as i32));
        }
    }
}
