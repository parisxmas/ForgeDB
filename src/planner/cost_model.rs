//! Cost model for the query optimizer.
//!
//! Provides cost estimates for different physical operations based on
//! table and column statistics. Falls back to defaults when statistics
//! are not available (ANALYZE has not been run).

use super::statistics::{ColumnStatistics, TableStatistics};

/// Cost of a sequential scan: proportional to the number of pages.
pub fn seq_scan_cost(stats: &TableStatistics) -> f64 {
    stats.page_count as f64
}

/// Cost of an index scan: startup cost + selectivity * row_count.
/// The startup cost accounts for B-tree traversal.
pub fn index_scan_cost(selectivity: f64, stats: &TableStatistics) -> f64 {
    3.0 + selectivity * stats.row_count as f64
}

/// Compare sequential scan vs index scan cost.
/// Returns true if index scan is cheaper.
pub fn prefer_index_scan(col_stats: &ColumnStatistics, table_stats: &TableStatistics) -> bool {
    let seq_cost = seq_scan_cost(table_stats);
    let idx_cost = index_scan_cost(col_stats.selectivity_eq(), table_stats);
    idx_cost < seq_cost
}

/// Estimate the output cardinality of an equi-join.
/// Uses the formula: |L| * |R| / max(distinct_L, distinct_R)
pub fn join_cardinality(
    left_rows: u64,
    right_rows: u64,
    left_distinct: u64,
    right_distinct: u64,
) -> u64 {
    let max_distinct = left_distinct.max(right_distinct).max(1);
    (left_rows * right_rows) / max_distinct
}

/// Estimate the output cardinality after applying a filter.
pub fn filter_cardinality(input_rows: u64, selectivity: f64) -> u64 {
    (input_rows as f64 * selectivity).max(1.0) as u64
}

/// Estimate the cost of a nested-loop join.
pub fn nested_loop_join_cost(left_rows: u64, right_pages: u64) -> f64 {
    left_rows as f64 * right_pages as f64
}

/// Estimate the cost of a hash join.
pub fn hash_join_cost(left_rows: u64, right_rows: u64) -> f64 {
    // Build cost + probe cost
    3.0 * (left_rows + right_rows) as f64
}

/// Row count threshold above which parallel/batch scan is enabled.
pub const PARALLEL_SCAN_THRESHOLD: u64 = 1000;

/// Check whether a table is large enough to benefit from parallel scanning.
pub fn should_enable_parallel_scan(stats: &TableStatistics) -> bool {
    stats.row_count > PARALLEL_SCAN_THRESHOLD
}

/// Estimate the cost of a grace hash join (partitioned on disk).
/// Slightly higher than in-memory hash join due to I/O for writing/reading partitions.
pub fn grace_hash_join_cost(left_rows: u64, right_rows: u64) -> f64 {
    // Two passes over data: partition + join, so ~6x total row count
    6.0 * (left_rows + right_rows) as f64
}

/// Decide whether to use grace hash join based on memory limits.
/// Returns true when either side exceeds the in-memory hash join limit.
pub fn should_use_grace_hash_join(left_rows: u64, right_rows: u64, memory_limit: usize) -> bool {
    left_rows as usize > memory_limit || right_rows as usize > memory_limit
}

/// Given multiple tables to join, find the pair with smallest estimated
/// join output. Returns (left_index, right_index).
pub fn greedy_join_order(
    table_stats: &[(u64, u64)], // (row_count, distinct_count) per table
) -> Vec<(usize, usize)> {
    if table_stats.len() <= 1 {
        return vec![];
    }

    let n = table_stats.len();
    let mut joined: Vec<bool> = vec![false; n];
    let mut result_rows: Vec<u64> = table_stats.iter().map(|(r, _)| *r).collect();
    let mut order = Vec::new();

    // Start with the smallest table
    let first = result_rows
        .iter()
        .enumerate()
        .min_by_key(|(_, &r)| r)
        .map(|(i, _)| i)
        .unwrap_or(0);
    joined[first] = true;

    for _ in 1..n {
        let mut best_pair = (0, 0);
        let mut best_cost = u64::MAX;

        for (i, &done) in joined.iter().enumerate() {
            if !done {
                continue;
            }
            for (j, &done_j) in joined.iter().enumerate() {
                if done_j || i == j {
                    continue;
                }
                let card = join_cardinality(
                    result_rows[i],
                    result_rows[j],
                    table_stats[i].1.max(1),
                    table_stats[j].1.max(1),
                );
                if card < best_cost {
                    best_cost = card;
                    best_pair = (i, j);
                }
            }
        }

        let (left, right) = best_pair;
        order.push((left, right));
        joined[right] = true;
        // Update cardinality estimate for the joined result
        result_rows[left] = best_cost;
    }

    order
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_scan_cost() {
        let stats = TableStatistics {
            row_count: 1000,
            page_count: 50,
        };
        assert_eq!(seq_scan_cost(&stats), 50.0);
    }

    #[test]
    fn test_index_scan_cost() {
        let stats = TableStatistics {
            row_count: 1000,
            page_count: 50,
        };
        // selectivity = 0.01 (100 distinct values)
        let cost = index_scan_cost(0.01, &stats);
        assert!((cost - 13.0).abs() < 1e-9); // 3.0 + 0.01 * 1000
    }

    #[test]
    fn test_prefer_index_scan_high_selectivity() {
        let col_stats = ColumnStatistics {
            distinct_count: 1000,
            null_count: 0,
            min_value: None,
            max_value: None,
        };
        let table_stats = TableStatistics {
            row_count: 10000,
            page_count: 500,
        };
        // seq_scan = 500, index_scan = 3.0 + 0.001 * 10000 = 13.0
        assert!(prefer_index_scan(&col_stats, &table_stats));
    }

    #[test]
    fn test_prefer_seq_scan_low_selectivity() {
        let col_stats = ColumnStatistics {
            distinct_count: 2,
            null_count: 0,
            min_value: None,
            max_value: None,
        };
        let table_stats = TableStatistics {
            row_count: 10000,
            page_count: 50,
        };
        // seq_scan = 50, index_scan = 3.0 + 0.5 * 10000 = 5003.0
        assert!(!prefer_index_scan(&col_stats, &table_stats));
    }

    #[test]
    fn test_join_cardinality() {
        let card = join_cardinality(1000, 500, 100, 50);
        // 1000 * 500 / max(100, 50) = 500000 / 100 = 5000
        assert_eq!(card, 5000);
    }

    #[test]
    fn test_greedy_join_order() {
        // Three tables: A(1000, 100), B(500, 50), C(200, 20)
        let stats = vec![(1000, 100), (500, 50), (200, 20)];
        let order = greedy_join_order(&stats);
        // Should start with C (smallest), then join smallest pair
        assert_eq!(order.len(), 2);
        // First join should involve index 2 (C, smallest)
    }
}
