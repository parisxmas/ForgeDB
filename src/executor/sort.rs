use crate::error::Result;
use crate::sql::ast::OrderByItem;
use crate::tuple::schema::Schema;
use crate::tuple::types::Value;

use super::eval::evaluate;

/// Sort rows in-place by order_by expressions.
pub fn execute_sort(
    order_by: &[OrderByItem],
    rows: &mut [Vec<Value>],
    schema: &Schema,
) -> Result<()> {
    // Pre-compute sort keys for each row
    let mut keys: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let mut row_keys = Vec::new();
        for item in order_by {
            let val = evaluate(&item.expr, row, schema)?;
            row_keys.push(val);
        }
        keys.push(row_keys);
    }

    // Create index array and sort it
    let mut indices: Vec<usize> = (0..rows.len()).collect();
    indices.sort_by(|&a, &b| {
        for (i, item) in order_by.iter().enumerate() {
            let cmp = keys[a][i].compare(&keys[b][i]);
            if let Some(ordering) = cmp {
                let ordering = if item.ascending {
                    ordering
                } else {
                    ordering.reverse()
                };
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            // NULL handling: NULLs sort last
            if keys[a][i].is_null() && !keys[b][i].is_null() {
                return std::cmp::Ordering::Greater;
            }
            if !keys[a][i].is_null() && keys[b][i].is_null() {
                return std::cmp::Ordering::Less;
            }
        }
        std::cmp::Ordering::Equal
    });

    // Reorder rows according to sorted indices
    let sorted: Vec<Vec<Value>> = indices.into_iter().map(|i| rows[i].clone()).collect();
    for (i, row) in sorted.into_iter().enumerate() {
        rows[i] = row;
    }

    Ok(())
}
