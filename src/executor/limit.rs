use crate::tuple::types::Value;

/// Truncate rows to at most `count` entries.
pub fn execute_limit(count: usize, rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.into_iter().take(count).collect()
}
