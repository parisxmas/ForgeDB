//! Table and column statistics for cost-based optimization.
//!
//! Statistics are gathered by the ANALYZE TABLE command and stored
//! alongside table metadata. The cost model uses them to choose
//! between sequential and index scans, and to estimate join cardinality.

use crate::tuple::types::Value;

/// Statistics for an entire table.
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Total number of rows in the table.
    pub row_count: u64,
    /// Number of pages used by the table's heap file.
    pub page_count: u64,
}

impl Default for TableStatistics {
    fn default() -> Self {
        Self {
            row_count: 0,
            page_count: 0,
        }
    }
}

/// Statistics for a single column.
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Approximate number of distinct non-NULL values.
    pub distinct_count: u64,
    /// Number of NULL values.
    pub null_count: u64,
    /// Minimum value (if any).
    pub min_value: Option<Value>,
    /// Maximum value (if any).
    pub max_value: Option<Value>,
}

impl Default for ColumnStatistics {
    fn default() -> Self {
        Self {
            distinct_count: 0,
            null_count: 0,
            min_value: None,
            max_value: None,
        }
    }
}

impl ColumnStatistics {
    /// Selectivity estimate for an equality predicate (col = value).
    /// Returns 1/distinct_count, or 0.1 if no statistics are available.
    pub fn selectivity_eq(&self) -> f64 {
        if self.distinct_count > 0 {
            1.0 / self.distinct_count as f64
        } else {
            0.1 // default selectivity when no stats
        }
    }

    /// Selectivity estimate for a range predicate (col < value, col > value, etc.).
    /// Without histograms, we use a fixed estimate of 1/3.
    pub fn selectivity_range(&self) -> f64 {
        0.33
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selectivity_eq() {
        let stats = ColumnStatistics {
            distinct_count: 100,
            null_count: 0,
            min_value: Some(Value::Integer(1)),
            max_value: Some(Value::Integer(100)),
        };
        let sel = stats.selectivity_eq();
        assert!((sel - 0.01).abs() < 1e-9);
    }

    #[test]
    fn test_selectivity_eq_no_stats() {
        let stats = ColumnStatistics::default();
        let sel = stats.selectivity_eq();
        assert!((sel - 0.1).abs() < 1e-9);
    }

    #[test]
    fn test_selectivity_range() {
        let stats = ColumnStatistics::default();
        assert!((stats.selectivity_range() - 0.33).abs() < 1e-9);
    }

    #[test]
    fn test_table_stats_default() {
        let stats = TableStatistics::default();
        assert_eq!(stats.row_count, 0);
        assert_eq!(stats.page_count, 0);
    }
}
