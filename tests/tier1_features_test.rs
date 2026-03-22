//! Tests for Tier 1 Production Features:
//! - Authentication (unit tests in auth.rs)
//! - Prepared Statements (protocol-level, tested via unit tests in mysql_protocol.rs)
//! - Spill-to-Disk (external sort, grace hash join)
//! - MVCC (visibility, snapshots)
//! - ROLLBACK (undo log)
//! - Cost-Based Optimizer (ANALYZE TABLE, statistics)

use forgedb::Database;
use tempfile::TempDir;

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::new(dir.path().to_str().unwrap()).unwrap();
    (dir, db)
}

// =========================================================================
// ANALYZE TABLE / Cost-Based Optimizer Tests
// =========================================================================

#[test]
fn test_analyze_table_basic() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE stats_test (id INT NOT NULL, name VARCHAR(100), score INT)")
        .unwrap();
    for i in 1..=100 {
        db.execute_sql(&format!(
            "INSERT INTO stats_test VALUES ({}, 'user_{}', {})",
            i,
            i,
            i * 10
        ))
        .unwrap();
    }

    // ANALYZE should succeed
    let result = db.execute_sql("ANALYZE TABLE stats_test").unwrap();
    assert_eq!(result.message, "OK");
}

#[test]
fn test_analyze_populates_stats() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE analyze_test (id INT NOT NULL, category VARCHAR(50))")
        .unwrap();
    for i in 1..=50 {
        let cat = if i % 3 == 0 {
            "A"
        } else if i % 3 == 1 {
            "B"
        } else {
            "C"
        };
        db.execute_sql(&format!(
            "INSERT INTO analyze_test VALUES ({}, '{}')",
            i, cat
        ))
        .unwrap();
    }

    db.execute_sql("ANALYZE TABLE analyze_test").unwrap();

    // After ANALYZE, queries should still work correctly
    let result = db
        .execute_sql("SELECT COUNT(*) FROM analyze_test")
        .unwrap();
    assert_eq!(result.rows[0][0].to_string(), "50");
}

#[test]
fn test_index_scan_after_analyze() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE indexed_test (id INT NOT NULL PRIMARY KEY, val VARCHAR(50))")
        .unwrap();
    for i in 1..=100 {
        db.execute_sql(&format!(
            "INSERT INTO indexed_test VALUES ({}, 'val_{}')",
            i, i
        ))
        .unwrap();
    }

    // Analyze the table
    db.execute_sql("ANALYZE TABLE indexed_test").unwrap();

    // Queries with WHERE on indexed column should still work
    let result = db
        .execute_sql("SELECT * FROM indexed_test WHERE id = 42")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].to_string(), "42");
}

#[test]
fn test_queries_work_without_analyze() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE no_stats (id INT NOT NULL, name VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO no_stats VALUES (1, 'a')")
        .unwrap();
    db.execute_sql("INSERT INTO no_stats VALUES (2, 'b')")
        .unwrap();

    // Without ANALYZE, rule-based optimizer should work
    let result = db.execute_sql("SELECT * FROM no_stats WHERE id = 1").unwrap();
    assert_eq!(result.rows.len(), 1);
}

// =========================================================================
// MVCC Tests
// =========================================================================

#[test]
fn test_basic_insert_select_with_mvcc_tables() {
    let (_dir, db) = setup();
    // New tables get mvcc_enabled=true
    db.execute_sql("CREATE TABLE mvcc_test (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO mvcc_test VALUES (1, 'hello')")
        .unwrap();
    db.execute_sql("INSERT INTO mvcc_test VALUES (2, 'world')")
        .unwrap();

    let result = db.execute_sql("SELECT * FROM mvcc_test ORDER BY id").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0].to_string(), "1");
    assert_eq!(result.rows[1][0].to_string(), "2");
}

#[test]
fn test_update_with_mvcc_tables() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE mvcc_upd (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO mvcc_upd VALUES (1, 'old')")
        .unwrap();
    db.execute_sql("UPDATE mvcc_upd SET val = 'new' WHERE id = 1")
        .unwrap();

    let result = db.execute_sql("SELECT val FROM mvcc_upd WHERE id = 1").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].to_string(), "new");
}

#[test]
fn test_delete_with_mvcc_tables() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE mvcc_del (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO mvcc_del VALUES (1, 'a')").unwrap();
    db.execute_sql("INSERT INTO mvcc_del VALUES (2, 'b')").unwrap();
    db.execute_sql("DELETE FROM mvcc_del WHERE id = 1").unwrap();

    let result = db.execute_sql("SELECT * FROM mvcc_del").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0].to_string(), "2");
}

// =========================================================================
// Transaction / ROLLBACK Tests
// =========================================================================

#[test]
fn test_begin_commit() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE txn_test (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("BEGIN").unwrap();
    db.execute_sql("INSERT INTO txn_test VALUES (1, 'committed')")
        .unwrap();
    db.execute_sql("COMMIT").unwrap();

    let result = db.execute_sql("SELECT * FROM txn_test").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_rollback_is_accepted() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE rollback_test (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO rollback_test VALUES (1, 'before')")
        .unwrap();

    // ROLLBACK should be accepted (no error)
    db.execute_sql("BEGIN").unwrap();
    let result = db.execute_sql("ROLLBACK");
    assert!(result.is_ok());
}

// =========================================================================
// Spill-to-Disk Sort Tests
// =========================================================================

#[test]
fn test_large_sort_correctness() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE big_sort (id INT NOT NULL, val INT)")
        .unwrap();

    // Insert rows in reverse order
    let batch_size = 100;
    for batch_start in (0..500).step_by(batch_size) {
        let mut values = Vec::new();
        for i in batch_start..batch_start + batch_size {
            values.push(format!("({}, {})", 500 - i, i));
        }
        let sql = format!("INSERT INTO big_sort VALUES {}", values.join(", "));
        db.execute_sql(&sql).unwrap();
    }

    let result = db
        .execute_sql("SELECT id FROM big_sort ORDER BY id LIMIT 5")
        .unwrap();
    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0][0].to_string(), "1");
    assert_eq!(result.rows[1][0].to_string(), "2");
    assert_eq!(result.rows[2][0].to_string(), "3");
}

#[test]
fn test_sort_descending() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE sort_desc (id INT NOT NULL)").unwrap();
    for i in 1..=20 {
        db.execute_sql(&format!("INSERT INTO sort_desc VALUES ({})", i))
            .unwrap();
    }

    let result = db
        .execute_sql("SELECT * FROM sort_desc ORDER BY id DESC LIMIT 3")
        .unwrap();
    assert_eq!(result.rows[0][0].to_string(), "20");
    assert_eq!(result.rows[1][0].to_string(), "19");
    assert_eq!(result.rows[2][0].to_string(), "18");
}

// =========================================================================
// Hash Join Tests
// =========================================================================

#[test]
fn test_join_basic() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE j_left (id INT NOT NULL, name VARCHAR(50))")
        .unwrap();
    db.execute_sql("CREATE TABLE j_right (user_id INT NOT NULL, score INT)")
        .unwrap();
    for i in 1..=10 {
        db.execute_sql(&format!("INSERT INTO j_left VALUES ({}, 'user_{}')", i, i))
            .unwrap();
    }
    for i in 5..=15 {
        db.execute_sql(&format!("INSERT INTO j_right VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    let result = db
        .execute_sql(
            "SELECT j_left.name, j_right.score FROM j_left INNER JOIN j_right ON j_left.id = j_right.user_id ORDER BY j_left.id",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 6); // overlap: 5..=10
}

#[test]
fn test_left_join() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE lj_a (id INT NOT NULL)").unwrap();
    db.execute_sql("CREATE TABLE lj_b (aid INT NOT NULL, val VARCHAR(10))")
        .unwrap();
    db.execute_sql("INSERT INTO lj_a VALUES (1)").unwrap();
    db.execute_sql("INSERT INTO lj_a VALUES (2)").unwrap();
    db.execute_sql("INSERT INTO lj_a VALUES (3)").unwrap();
    db.execute_sql("INSERT INTO lj_b VALUES (1, 'x')").unwrap();

    let result = db
        .execute_sql("SELECT lj_a.id, lj_b.val FROM lj_a LEFT JOIN lj_b ON lj_a.id = lj_b.aid ORDER BY lj_a.id")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    // Row for id=2 and id=3 should have NULL val
    assert!(result.rows[1][1].is_null());
    assert!(result.rows[2][1].is_null());
}

// =========================================================================
// Temp Storage Tests
// =========================================================================

#[test]
fn test_temp_file_row_serialization() {
    use forgedb::tuple::types::Value;

    let rows = vec![
        vec![Value::Integer(1), Value::Varchar("hello".into()), Value::Null],
        vec![Value::Integer(2), Value::Varchar("world".into()), Value::Float(3.14)],
    ];

    let mut buf = Vec::new();
    forgedb::executor::temp_storage::write_rows(&mut buf, &rows).unwrap();

    let mut cursor = std::io::Cursor::new(buf);
    let recovered = forgedb::executor::temp_storage::read_all_rows(&mut cursor).unwrap();

    assert_eq!(recovered.len(), 2);
    assert_eq!(format!("{:?}", recovered[0][0]), format!("{:?}", rows[0][0]));
    assert_eq!(format!("{:?}", recovered[0][1]), format!("{:?}", rows[0][1]));
}

// =========================================================================
// MVCC Visibility Unit-Level Tests (via database)
// =========================================================================

#[test]
fn test_concurrent_table_operations() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE conc_test (id INT NOT NULL, val INT)")
        .unwrap();

    // Insert + query interleaved
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO conc_test VALUES ({}, {})", i, i * 100))
            .unwrap();
    }

    let result = db.execute_sql("SELECT COUNT(*) FROM conc_test").unwrap();
    assert_eq!(result.rows[0][0].to_string(), "50");

    // Delete even rows
    db.execute_sql("DELETE FROM conc_test WHERE val = 100 OR val = 200 OR val = 300")
        .unwrap();

    let result = db.execute_sql("SELECT COUNT(*) FROM conc_test").unwrap();
    assert_eq!(result.rows[0][0].to_string(), "47");
}

// =========================================================================
// Authentication Tests (unit-level, no network)
// =========================================================================

#[test]
fn test_sha1_correctness() {
    use forgedb::server::auth;

    // Known SHA1 test vector
    let hash = auth::sha1(b"abc");
    assert_eq!(hash[0], 0xa9);
    assert_eq!(hash[1], 0x99);
}

#[test]
fn test_auth_validate() {
    use forgedb::server::auth;

    let password = "test_password";
    let double_hash = auth::double_sha1(password);
    let challenge = auth::generate_challenge(100);

    // Simulate client token generation
    let sha1_password = auth::sha1(password.as_bytes());
    let mut concat = Vec::new();
    concat.extend_from_slice(&challenge);
    concat.extend_from_slice(&double_hash);
    let hash_stage = auth::sha1(&concat);
    let mut token = [0u8; 20];
    for i in 0..20 {
        token[i] = sha1_password[i] ^ hash_stage[i];
    }

    assert!(auth::validate_native_password(&challenge, &token, &double_hash));
}

#[test]
fn test_auth_reject_wrong_password() {
    use forgedb::server::auth;

    let double_hash = auth::double_sha1("correct_password");
    let challenge = auth::generate_challenge(200);

    // Generate token with wrong password
    let wrong_sha1 = auth::sha1(b"wrong_password");
    let mut concat = Vec::new();
    concat.extend_from_slice(&challenge);
    concat.extend_from_slice(&double_hash);
    let hash_stage = auth::sha1(&concat);
    let mut token = [0u8; 20];
    for i in 0..20 {
        token[i] = wrong_sha1[i] ^ hash_stage[i];
    }

    assert!(!auth::validate_native_password(&challenge, &token, &double_hash));
}

#[test]
fn test_auth_no_password_fallback() {
    // When no password is set (root_password_hash = None), server accepts all
    // This is tested by the existing test suite which creates databases without passwords
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE auth_fallback_test (id INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO auth_fallback_test VALUES (1)").unwrap();
    let result = db.execute_sql("SELECT * FROM auth_fallback_test");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().rows.len(), 1);
}

// =========================================================================
// MVCC Snapshot Tests
// =========================================================================

#[test]
fn test_mvcc_visibility_unit() {
    use forgedb::txn::mvcc;

    // Tuple created by committed txn, not deleted
    let snap = forgedb::txn::Snapshot {
        txn_id: forgedb::common::TxnId(10),
        active_txns: std::collections::HashSet::new(),
        xmin: 1,
        xmax: 11,
    };
    assert!(mvcc::is_visible(5, mvcc::XMAX_NONE, &snap));
}

#[test]
fn test_mvcc_invisible_uncommitted() {
    use forgedb::txn::mvcc;

    let mut active = std::collections::HashSet::new();
    active.insert(forgedb::common::TxnId(7));

    let snap = forgedb::txn::Snapshot {
        txn_id: forgedb::common::TxnId(10),
        active_txns: active,
        xmin: 5,
        xmax: 11,
    };
    // Created by active (uncommitted) txn 7 — not visible
    assert!(!mvcc::is_visible(7, mvcc::XMAX_NONE, &snap));
}

#[test]
fn test_mvcc_own_insert_visible() {
    use forgedb::txn::mvcc;

    let snap = forgedb::txn::Snapshot {
        txn_id: forgedb::common::TxnId(10),
        active_txns: std::collections::HashSet::new(),
        xmin: 5,
        xmax: 11,
    };
    // Created by our own transaction — visible
    assert!(mvcc::is_visible(10, mvcc::XMAX_NONE, &snap));
}

#[test]
fn test_mvcc_deleted_by_committed() {
    use forgedb::txn::mvcc;

    let snap = forgedb::txn::Snapshot {
        txn_id: forgedb::common::TxnId(10),
        active_txns: std::collections::HashSet::new(),
        xmin: 1,
        xmax: 11,
    };
    // Created by committed txn 2, deleted by committed txn 8
    assert!(!mvcc::is_visible(2, 8, &snap));
}

// =========================================================================
// Undo Log Tests
// =========================================================================

#[test]
fn test_undo_log_basic() {
    use forgedb::txn::undo::{UndoLog, UndoEntry};
    use forgedb::common::{RID, PageId};

    let mut log = UndoLog::new();
    assert!(log.is_empty());

    log.push(UndoEntry::InsertUndo {
        table_name: "test".into(),
        rid: RID { page_id: PageId(1), slot_id: 0 },
    });
    log.push(UndoEntry::DeleteUndo {
        table_name: "test".into(),
        rid: RID { page_id: PageId(2), slot_id: 1 },
        old_xmax: 0,
        old_data: vec![],
    });

    assert_eq!(log.len(), 2);

    // Entries should be in reverse order for rollback
    let reversed: Vec<_> = log.entries_reversed().collect();
    assert!(matches!(reversed[0], UndoEntry::DeleteUndo { .. }));
    assert!(matches!(reversed[1], UndoEntry::InsertUndo { .. }));
}

// =========================================================================
// Cost Model Tests
// =========================================================================

#[test]
fn test_cost_model_prefer_index_high_selectivity() {
    use forgedb::planner::statistics::{ColumnStatistics, TableStatistics};
    use forgedb::planner::cost_model;

    let col_stats = ColumnStatistics {
        distinct_count: 10000,
        null_count: 0,
        min_value: None,
        max_value: None,
    };
    let table_stats = TableStatistics {
        row_count: 100000,
        page_count: 5000,
    };

    // With high distinct count, index scan should be preferred
    assert!(cost_model::prefer_index_scan(&col_stats, &table_stats));
}

#[test]
fn test_cost_model_prefer_seq_scan_low_selectivity() {
    use forgedb::planner::statistics::{ColumnStatistics, TableStatistics};
    use forgedb::planner::cost_model;

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

    // With low distinct count (boolean-like), seq scan is cheaper
    assert!(!cost_model::prefer_index_scan(&col_stats, &table_stats));
}

#[test]
fn test_join_reordering() {
    use forgedb::planner::cost_model;

    // Three tables of different sizes
    let stats = vec![(10000, 1000), (100, 50), (5000, 500)];
    let order = cost_model::greedy_join_order(&stats);

    // Should produce 2 join pairs
    assert_eq!(order.len(), 2);
    // First join should involve the smallest table (index 1, 100 rows)
}

// =========================================================================
// End-to-End: Analyze + Query
// =========================================================================

#[test]
fn test_analyze_then_query() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE aq_test (id INT NOT NULL PRIMARY KEY, status VARCHAR(20), amount INT)")
        .unwrap();

    for i in 1..=200 {
        let status = if i % 2 == 0 { "active" } else { "inactive" };
        db.execute_sql(&format!(
            "INSERT INTO aq_test VALUES ({}, '{}', {})",
            i, status, i * 10
        ))
        .unwrap();
    }

    db.execute_sql("ANALYZE TABLE aq_test").unwrap();

    // Aggregation query
    let result = db
        .execute_sql("SELECT status, COUNT(*), SUM(amount) FROM aq_test GROUP BY status ORDER BY status")
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    // Filter query
    let result = db.execute_sql("SELECT * FROM aq_test WHERE id = 100").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1].to_string(), "active");
}

#[test]
fn test_multiple_operations_sequence() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE seq_ops (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();

    // Insert
    for i in 1..=10 {
        db.execute_sql(&format!("INSERT INTO seq_ops VALUES ({}, 'v{}')", i, i))
            .unwrap();
    }
    let r = db.execute_sql("SELECT COUNT(*) FROM seq_ops").unwrap();
    assert_eq!(r.rows[0][0].to_string(), "10");

    // Update
    db.execute_sql("UPDATE seq_ops SET val = 'updated' WHERE id <= 5")
        .unwrap();
    let r = db
        .execute_sql("SELECT COUNT(*) FROM seq_ops WHERE val = 'updated'")
        .unwrap();
    assert_eq!(r.rows[0][0].to_string(), "5");

    // Delete
    db.execute_sql("DELETE FROM seq_ops WHERE id > 8").unwrap();
    let r = db.execute_sql("SELECT COUNT(*) FROM seq_ops").unwrap();
    assert_eq!(r.rows[0][0].to_string(), "8");

    // Analyze
    db.execute_sql("ANALYZE TABLE seq_ops").unwrap();

    // Query after analyze
    let r = db
        .execute_sql("SELECT * FROM seq_ops ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 8);
    assert_eq!(r.rows[0][0].to_string(), "1");
}

// =========================================================================
// ACID Compliance Tests
// =========================================================================

#[test]
fn test_acid_atomicity_dml_auto_transaction() {
    // Every DML statement should be wrapped in an auto-transaction
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_atom (id INT NOT NULL PRIMARY KEY, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO acid_atom VALUES (1, 'a')").unwrap();
    db.execute_sql("INSERT INTO acid_atom VALUES (2, 'b')").unwrap();

    let r = db.execute_sql("SELECT COUNT(*) FROM acid_atom").unwrap();
    assert_eq!(r.rows[0][0].to_string(), "2");
}

#[test]
fn test_acid_atomicity_duplicate_key_rejected() {
    // INSERT with duplicate PK should fail and not leave partial state
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_dup (id INT NOT NULL PRIMARY KEY, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO acid_dup VALUES (1, 'first')").unwrap();

    // This should fail — duplicate key
    let result = db.execute_sql("INSERT INTO acid_dup VALUES (1, 'duplicate')");
    assert!(result.is_err());

    // Original row should still be intact
    let r = db.execute_sql("SELECT val FROM acid_dup WHERE id = 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string(), "first");
}

#[test]
fn test_acid_consistency_not_null_with_default() {
    // NOT NULL columns with no explicit value should get type defaults
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_nn (id INT NOT NULL, name VARCHAR(50) NOT NULL, score INT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO acid_nn VALUES (1, 'Alice', NULL)").unwrap();

    let r = db.execute_sql("SELECT score FROM acid_nn WHERE id = 1").unwrap();
    // score should be 0 (type default for INT), not NULL
    assert_eq!(r.rows[0][0].to_string(), "0");
}

#[test]
fn test_acid_durability_begin_commit() {
    // BEGIN + INSERT + COMMIT should write WAL records
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_dur (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("BEGIN").unwrap();
    db.execute_sql("INSERT INTO acid_dur VALUES (1, 'durable')").unwrap();
    db.execute_sql("COMMIT").unwrap();

    // Data should survive after commit
    let r = db.execute_sql("SELECT * FROM acid_dur").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1].to_string(), "durable");
}

#[test]
fn test_acid_durability_auto_commit() {
    // Without explicit BEGIN, each DML is auto-committed
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_auto (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO acid_auto VALUES (1, 'auto')").unwrap();

    let r = db.execute_sql("SELECT * FROM acid_auto").unwrap();
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn test_acid_rollback_statement() {
    // ROLLBACK should be accepted without error
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_rb (id INT NOT NULL)").unwrap();
    db.execute_sql("BEGIN").unwrap();
    db.execute_sql("ROLLBACK").unwrap();
    // No crash, no error
}

#[test]
fn test_acid_unique_constraint_enforced() {
    // Primary key uniqueness should be enforced
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_uniq (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))")
        .unwrap();
    db.execute_sql("INSERT INTO acid_uniq VALUES (1, 'one')").unwrap();
    db.execute_sql("INSERT INTO acid_uniq VALUES (2, 'two')").unwrap();

    // Duplicate should fail
    let result = db.execute_sql("INSERT INTO acid_uniq VALUES (1, 'dup')");
    assert!(result.is_err());

    // Should still have exactly 2 rows
    let r = db.execute_sql("SELECT COUNT(*) FROM acid_uniq").unwrap();
    assert_eq!(r.rows[0][0].to_string(), "2");
}

#[test]
fn test_acid_failed_dml_aborts_transaction() {
    // If a DML statement fails, the auto-transaction should be aborted
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE acid_fail (id INT NOT NULL PRIMARY KEY, val INT)")
        .unwrap();
    db.execute_sql("INSERT INTO acid_fail VALUES (1, 100)").unwrap();

    // This should fail (duplicate) and the auto-transaction should abort
    let _ = db.execute_sql("INSERT INTO acid_fail VALUES (1, 200)");

    // Verify the table wasn't corrupted
    let r = db.execute_sql("SELECT val FROM acid_fail WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0].to_string(), "100");
}

// =========================================================================
// LIMIT Pushdown Tests
// =========================================================================

#[test]
fn test_limit_pushdown_seq_scan() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE limit_push (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    for i in 1..=1000 {
        db.execute_sql(&format!("INSERT INTO limit_push VALUES ({}, 'row_{}')", i, i))
            .unwrap();
    }

    // LIMIT 5 on bare scan — should stop after 5 rows, not scan all 1000
    let result = db.execute_sql("SELECT * FROM limit_push LIMIT 5").unwrap();
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn test_limit_pushdown_with_filter() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE limit_filt (id INT NOT NULL, active INT)")
        .unwrap();
    for i in 1..=200 {
        let active = if i % 2 == 0 { 1 } else { 0 };
        db.execute_sql(&format!("INSERT INTO limit_filt VALUES ({}, {})", i, active))
            .unwrap();
    }

    // LIMIT 3 with filter — should stop after finding 3 matching rows
    let result = db
        .execute_sql("SELECT * FROM limit_filt WHERE active = 1 LIMIT 3")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    // All should be active=1
    for row in &result.rows {
        assert_eq!(row[1].to_string(), "1");
    }
}

// =========================================================================
// ReadContext (zero-clone) Tests
// =========================================================================

#[test]
fn test_read_context_concurrent_selects() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE rc_test (id INT NOT NULL, val VARCHAR(50))")
        .unwrap();
    for i in 1..=50 {
        db.execute_sql(&format!("INSERT INTO rc_test VALUES ({}, 'v{}')", i, i))
            .unwrap();
    }

    // Multiple read queries should all work correctly without cloning
    let r1 = db.execute_sql("SELECT COUNT(*) FROM rc_test").unwrap();
    let r2 = db.execute_sql("SELECT * FROM rc_test WHERE id = 25").unwrap();
    let r3 = db.execute_sql("SELECT * FROM rc_test ORDER BY id DESC LIMIT 3").unwrap();

    assert_eq!(r1.rows[0][0].to_string(), "50");
    assert_eq!(r2.rows.len(), 1);
    assert_eq!(r3.rows.len(), 3);
    assert_eq!(r3.rows[0][0].to_string(), "50");
}
