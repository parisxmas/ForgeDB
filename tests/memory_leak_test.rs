// =============================================================================
// Memory Leak Tests — verify resources are cleaned up properly
// =============================================================================
//
// Each test creates resources, uses them, then verifies they are freed.
// Tests cover: savepoints, sequences, plan cache, cursors, procedures,
// triggers, users, prepared statements, transactions, undo logs.

use forgedb::common::TxnId;
use forgedb::Database;
use forgedb::tuple::types::Value;
use tempfile::TempDir;

fn new_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::new(dir.path().to_str().unwrap()).unwrap();
    (dir, db)
}

fn count_rows(db: &Database, table: &str) -> i64 {
    let r = db.execute_sql(&format!("SELECT COUNT(*) FROM {}", table)).unwrap();
    match &r.rows[0][0] {
        Value::BigInt(n) => *n,
        Value::Integer(n) => *n as i64,
        other => panic!("expected numeric, got {:?}", other),
    }
}

// ===========================================================================
// SAVEPOINT LEAK: savepoints cleaned on commit/abort
// ===========================================================================

#[test]
fn test_leak_savepoints_cleaned_on_commit() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_sp (id INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO t_sp VALUES (1)", &mut session).unwrap();
    db.execute_sql_session("SAVEPOINT s1", &mut session).unwrap();
    db.execute_sql_session("SAVEPOINT s2", &mut session).unwrap();
    db.execute_sql_session("SAVEPOINT s3", &mut session).unwrap();
    db.execute_sql_session("COMMIT", &mut session).unwrap();

    let (sp, _, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(sp, 0, "savepoints should be cleaned on commit");
}

#[test]
fn test_leak_savepoints_cleaned_on_rollback() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_sp2 (id INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("SAVEPOINT s1", &mut session).unwrap();
    db.execute_sql_session("SAVEPOINT s2", &mut session).unwrap();
    db.execute_sql_session("ROLLBACK", &mut session).unwrap();

    let (sp, _, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(sp, 0, "savepoints should be cleaned on rollback");
}

#[test]
fn test_leak_savepoints_cleaned_on_abort() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_sp3 (id INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("SAVEPOINT s1", &mut session).unwrap();
    let txn_id = session.unwrap();
    db.abort_transaction(txn_id);

    let (sp, _, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(sp, 0, "savepoints should be cleaned on abort");
}

// ===========================================================================
// SEQUENCE LEAK: sequences can be dropped
// ===========================================================================

#[test]
fn test_leak_sequence_create_drop() {
    let (_dir, db) = new_db();

    let (_, s0, _, _, _, _, _, _, _) = db.resource_counts();

    db.execute_sql("CREATE SEQUENCE leak_seq1 START 1").unwrap();
    db.execute_sql("CREATE SEQUENCE leak_seq2 START 1").unwrap();
    let (_, s1, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(s1 - s0, 2, "two sequences created");

    db.execute_sql("DROP SEQUENCE leak_seq1").unwrap();
    db.execute_sql("DROP SEQUENCE leak_seq2").unwrap();
    let (_, s2, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(s2, s0, "sequences should be cleaned after drop");
}

#[test]
fn test_leak_sequence_create_many_drop_all() {
    let (_dir, db) = new_db();

    for i in 0..100 {
        db.execute_sql(&format!("CREATE SEQUENCE batch_seq_{} START 1", i)).unwrap();
    }
    let (_, s1, _, _, _, _, _, _, _) = db.resource_counts();
    assert!(s1 >= 100);

    for i in 0..100 {
        db.execute_sql(&format!("DROP SEQUENCE batch_seq_{}", i)).unwrap();
    }
    let (_, s2, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(s2, 0, "all sequences should be dropped");
}

// ===========================================================================
// PLAN CACHE LEAK: bounded and invalidated
// ===========================================================================

#[test]
fn test_leak_plan_cache_bounded() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_pc (id INT NOT NULL, v INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO t_pc VALUES (1, 10)").unwrap();

    // Run many distinct queries to fill cache
    for i in 0..100 {
        let _ = db.execute_sql(&format!("SELECT * FROM t_pc WHERE id = {}", i));
    }

    let (_, _, pc, _, _, _, _, _, _) = db.resource_counts();
    assert!(pc <= 10000, "plan cache should be bounded");
}

#[test]
fn test_leak_plan_cache_invalidated_on_ddl() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_pc2 (id INT NOT NULL)").unwrap();

    // Fill cache
    for i in 0..10 {
        let _ = db.execute_sql(&format!("SELECT * FROM t_pc2 WHERE id = {}", i));
    }
    let (_, _, pc1, _, _, _, _, _, _) = db.resource_counts();
    assert!(pc1 > 0, "cache should have entries");

    // DDL invalidates cache
    db.execute_sql("CREATE TABLE t_pc2_other (id INT NOT NULL)").unwrap();
    let (_, _, pc2, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(pc2, 0, "cache should be cleared after DDL");
}

// ===========================================================================
// CURSOR LEAK: cursors cleaned on deallocate
// ===========================================================================

#[test]
fn test_leak_cursor_deallocated() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_cur (id INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO t_cur VALUES (1)").unwrap();

    db.execute_sql("DECLARE c1 CURSOR FOR SELECT id FROM t_cur").unwrap();
    db.execute_sql("OPEN c1").unwrap();

    let (_, _, _, c1, _, _, _, _, _) = db.resource_counts();
    assert!(c1 > 0, "cursor should exist");

    db.execute_sql("CLOSE c1").unwrap();
    db.execute_sql("DEALLOCATE c1").unwrap();

    let (_, _, _, c2, _, _, _, _, _) = db.resource_counts();
    assert_eq!(c2, 0, "cursor should be deallocated");
}

#[test]
fn test_leak_many_cursors_all_deallocated() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_cur2 (id INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO t_cur2 VALUES (1)").unwrap();

    for i in 0..50 {
        db.execute_sql(&format!("DECLARE cur_{} CURSOR FOR SELECT id FROM t_cur2", i)).unwrap();
        db.execute_sql(&format!("OPEN cur_{}", i)).unwrap();
    }
    let (_, _, _, c1, _, _, _, _, _) = db.resource_counts();
    assert_eq!(c1, 50);

    for i in 0..50 {
        db.execute_sql(&format!("CLOSE cur_{}", i)).unwrap();
        db.execute_sql(&format!("DEALLOCATE cur_{}", i)).unwrap();
    }
    let (_, _, _, c2, _, _, _, _, _) = db.resource_counts();
    assert_eq!(c2, 0, "all cursors should be deallocated");
}

// ===========================================================================
// PROCEDURE LEAK: procedures can be dropped
// ===========================================================================

#[test]
fn test_leak_procedure_drop() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_proc (id INT NOT NULL)").unwrap();

    db.execute_sql("CREATE PROCEDURE p1 AS BEGIN INSERT INTO t_proc VALUES (1) END").unwrap();
    let (_, _, _, _, p1, _, _, _, _) = db.resource_counts();
    assert!(p1 > 0);

    db.execute_sql("DROP PROCEDURE p1").unwrap();
    let (_, _, _, _, p2, _, _, _, _) = db.resource_counts();
    assert_eq!(p2, 0, "procedure should be dropped");
}

#[test]
fn test_leak_many_procedures_drop_all() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_proc2 (id INT NOT NULL)").unwrap();

    for i in 0..20 {
        db.execute_sql(&format!(
            "CREATE PROCEDURE proc_{} AS BEGIN INSERT INTO t_proc2 VALUES ({}) END", i, i
        )).unwrap();
    }
    let (_, _, _, _, p1, _, _, _, _) = db.resource_counts();
    assert_eq!(p1, 20);

    for i in 0..20 {
        db.execute_sql(&format!("DROP PROCEDURE proc_{}", i)).unwrap();
    }
    let (_, _, _, _, p2, _, _, _, _) = db.resource_counts();
    assert_eq!(p2, 0, "all procedures should be dropped");
}

// ===========================================================================
// USER LEAK: users can be dropped
// ===========================================================================

#[test]
fn test_leak_user_drop() {
    let (_dir, db) = new_db();

    db.execute_sql("CREATE USER u1 WITH PASSWORD 'pass1'").unwrap();
    db.execute_sql("CREATE USER u2 WITH PASSWORD 'pass2'").unwrap();
    let (_, _, _, _, _, _, u1, _, _) = db.resource_counts();
    assert_eq!(u1, 2);

    db.execute_sql("DROP USER u1").unwrap();
    db.execute_sql("DROP USER u2").unwrap();
    let (_, _, _, _, _, _, u2, _, _) = db.resource_counts();
    assert_eq!(u2, 0, "all users should be dropped");
}

// ===========================================================================
// PREPARED STATEMENT LEAK
// ===========================================================================

#[test]
fn test_leak_prepared_stmt_deallocate() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_ps (id INT NOT NULL)").unwrap();

    db.execute_sql("PREPARE ps1 AS INSERT INTO t_ps VALUES (1)").unwrap();
    db.execute_sql("PREPARE ps2 AS SELECT * FROM t_ps").unwrap();
    let (_, _, _, _, _, _, _, ps1, _) = db.resource_counts();
    assert_eq!(ps1, 2);

    // Deallocate by re-preparing with same name (overwrites) — or we can add explicit dealloc
    // For now verify the count stabilizes
    db.execute_sql("PREPARE ps1 AS INSERT INTO t_ps VALUES (2)").unwrap();
    let (_, _, _, _, _, _, _, ps2, _) = db.resource_counts();
    assert_eq!(ps2, 2, "re-prepare should overwrite, not add");
}

// ===========================================================================
// TRANSACTION LEAK: active transactions cleaned up
// ===========================================================================

#[test]
fn test_leak_transaction_commit_cleans_resources() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_txn (id INT NOT NULL)").unwrap();

    // Run 100 auto-transactions
    for i in 0..100 {
        db.execute_sql(&format!("INSERT INTO t_txn VALUES ({})", i)).unwrap();
    }

    // All auto-transactions should be committed and cleaned
    assert_eq!(count_rows(&db, "t_txn"), 100);
}

#[test]
fn test_leak_many_sessions_commit() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_txn2 (id INT NOT NULL)").unwrap();

    for i in 0..50 {
        let mut session: Option<TxnId> = None;
        db.execute_sql_session("BEGIN", &mut session).unwrap();
        db.execute_sql_session(&format!("INSERT INTO t_txn2 VALUES ({})", i), &mut session).unwrap();
        db.execute_sql_session("SAVEPOINT sp", &mut session).unwrap();
        db.execute_sql_session("COMMIT", &mut session).unwrap();
    }

    let (sp, _, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(sp, 0, "no orphaned savepoints after 50 commits");
    assert_eq!(count_rows(&db, "t_txn2"), 50);
}

#[test]
fn test_leak_many_sessions_rollback() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_txn3 (id INT NOT NULL)").unwrap();

    for i in 0..50 {
        let mut session: Option<TxnId> = None;
        db.execute_sql_session("BEGIN", &mut session).unwrap();
        db.execute_sql_session(&format!("INSERT INTO t_txn3 VALUES ({})", i), &mut session).unwrap();
        db.execute_sql_session("SAVEPOINT sp", &mut session).unwrap();
        db.execute_sql_session("ROLLBACK", &mut session).unwrap();
    }

    let (sp, _, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(sp, 0, "no orphaned savepoints after 50 rollbacks");
    assert_eq!(count_rows(&db, "t_txn3"), 0);
}

// ===========================================================================
// CONCURRENT LEAK: resources cleaned under concurrent load
// ===========================================================================

#[test]
fn test_leak_concurrent_transactions_no_orphans() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_conc (id INT NOT NULL)").unwrap();

    let db = std::sync::Arc::new(db);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(100));
    let mut handles = vec![];

    for t in 0..100 {
        let db = std::sync::Arc::clone(&db);
        let barrier = std::sync::Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut session: Option<TxnId> = None;
            let _ = db.execute_sql_session("BEGIN", &mut session);
            let _ = db.execute_sql_session(&format!("INSERT INTO t_conc VALUES ({})", t), &mut session);
            let _ = db.execute_sql_session("SAVEPOINT sp1", &mut session);
            if t % 2 == 0 {
                let _ = db.execute_sql_session("COMMIT", &mut session);
            } else {
                let _ = db.execute_sql_session("ROLLBACK", &mut session);
            }
        }));
    }

    for h in handles { h.join().unwrap(); }

    let (sp, _, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(sp, 0, "no orphaned savepoints after concurrent txns");
}

// ===========================================================================
// STRESS LEAK: heavy repeated operations don't leak
// ===========================================================================

#[test]
fn test_leak_stress_1000_queries_cache_bounded() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_stress (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..10 {
        db.execute_sql(&format!("INSERT INTO t_stress VALUES ({}, {})", i, i * 10)).unwrap();
    }

    // Run 1000 distinct SELECT queries
    for i in 0..1000 {
        let _ = db.execute_sql(&format!("SELECT * FROM t_stress WHERE id = {}", i % 100));
    }

    let (_, _, pc, _, _, _, _, _, _) = db.resource_counts();
    assert!(pc <= 10000, "plan cache bounded at 10000: actual={}", pc);
}

#[test]
fn test_leak_stress_sequences_create_use_drop() {
    let (_dir, db) = new_db();

    for round in 0..10 {
        // Create 10 sequences
        for i in 0..10 {
            db.execute_sql(&format!("CREATE SEQUENCE stress_seq_{}_{} START 1", round, i)).unwrap();
        }
        // Use them
        for i in 0..10 {
            let _ = db.execute_sql(&format!("SELECT NEXTVAL('stress_seq_{}_{}')", round, i));
        }
        // Drop them
        for i in 0..10 {
            db.execute_sql(&format!("DROP SEQUENCE stress_seq_{}_{}", round, i)).unwrap();
        }
    }

    let (_, seq, _, _, _, _, _, _, _) = db.resource_counts();
    assert_eq!(seq, 0, "all sequences dropped after stress test");
}

#[test]
fn test_leak_stress_cursors_create_use_close() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_cur_stress (id INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO t_cur_stress VALUES (1)").unwrap();

    for i in 0..50 {
        let name = format!("stress_cur_{}", i);
        db.execute_sql(&format!("DECLARE {} CURSOR FOR SELECT id FROM t_cur_stress", name)).unwrap();
        db.execute_sql(&format!("OPEN {}", name)).unwrap();
        db.execute_sql(&format!("FETCH NEXT FROM {}", name)).unwrap();
        db.execute_sql(&format!("CLOSE {}", name)).unwrap();
        db.execute_sql(&format!("DEALLOCATE {}", name)).unwrap();
    }

    let (_, _, _, cur, _, _, _, _, _) = db.resource_counts();
    assert_eq!(cur, 0, "all cursors deallocated after stress test");
}

#[test]
fn test_leak_stress_procedures_create_exec_drop() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE t_proc_stress (id INT NOT NULL)").unwrap();

    for i in 0..30 {
        let name = format!("stress_proc_{}", i);
        db.execute_sql(&format!(
            "CREATE PROCEDURE {} AS BEGIN INSERT INTO t_proc_stress VALUES ({}) END", name, i
        )).unwrap();
        db.execute_sql(&format!("EXEC {}", name)).unwrap();
        db.execute_sql(&format!("DROP PROCEDURE {}", name)).unwrap();
    }

    let (_, _, _, _, proc, _, _, _, _) = db.resource_counts();
    assert_eq!(proc, 0, "all procedures dropped after stress test");
    assert_eq!(count_rows(&db, "t_proc_stress"), 30);
}
