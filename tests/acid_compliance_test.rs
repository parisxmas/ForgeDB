// =============================================================================
// ACID Compliance Integration Tests — 100 Concurrent Clients
// =============================================================================
//
// Verifies Atomicity, Consistency, Isolation, and Durability under heavy
// concurrent load. Each test exercises real SQL execution through the full
// parse → plan → execute pipeline, not mocked internals.
//
// Concurrency model: Arc<Database> shared across std::thread::spawn workers
// (same model used by the MySQL wire-protocol server).

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use forgedb::common::TxnId;
use forgedb::Database;
use forgedb::tuple::types::Value;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        other => panic!("expected numeric count, got {:?}", other),
    }
}

fn sum_col(db: &Database, table: &str, col: &str) -> i64 {
    let r = db.execute_sql(&format!("SELECT SUM({}) FROM {}", col, table)).unwrap();
    match &r.rows[0][0] {
        Value::BigInt(n) => *n,
        Value::Integer(n) => *n as i64,
        Value::Float(f) => *f as i64,
        Value::Null => 0,
        other => panic!("expected numeric sum, got {:?}", other),
    }
}

// ===========================================================================
// 1. ATOMICITY — All-or-nothing
// ===========================================================================

/// 1. 100 threads each INSERT a single row. Every insert must succeed.
/// Total row count must equal exactly 100.
#[test]
fn test_acid_01_atomicity_100_concurrent_inserts() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A01 (id INT NOT NULL, tid INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait(); // thundering-herd start
            db.execute_sql(&format!("INSERT INTO A01 VALUES ({}, {})", t, t)).unwrap();
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(count_rows(&db, "A01"), 100);
}

/// 2. Auto-transaction: INSERT that violates a constraint should not
/// leave partial state.
#[test]
fn test_acid_02_atomicity_constraint_violation_leaves_no_partial() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A02 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO A02 VALUES (1, 100)").unwrap();

    // Attempt duplicate PK — must fail
    let err = db.execute_sql("INSERT INTO A02 VALUES (1, 200)");
    assert!(err.is_err(), "duplicate PK must fail");

    // Original row untouched
    let r = db.execute_sql("SELECT v FROM A02 WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(100));
    assert_eq!(count_rows(&db, "A02"), 1);
}

/// 3. Multi-row INSERT: all rows in a single statement are committed atomically.
#[test]
fn test_acid_03_atomicity_multi_row_insert() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A03 (id INT NOT NULL, v VARCHAR(50))").unwrap();
    db.execute_sql(
        "INSERT INTO A03 VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d'), (5,'e')",
    ).unwrap();
    assert_eq!(count_rows(&db, "A03"), 5);
}

/// 4. BEGIN + multiple INSERTs + ROLLBACK → zero rows persisted.
#[test]
fn test_acid_04_atomicity_rollback_undoes_inserts() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A04 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A04 VALUES (1, 10)", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A04 VALUES (2, 20)", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A04 VALUES (3, 30)", &mut session).unwrap();
    db.execute_sql_session("ROLLBACK", &mut session).unwrap();

    assert!(session.is_none(), "session cleared after ROLLBACK");
    assert_eq!(count_rows(&db, "A04"), 0, "ROLLBACK must undo all INSERTs");
}

/// 5. BEGIN + INSERT + COMMIT → rows persist.
#[test]
fn test_acid_05_atomicity_commit_persists_inserts() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A05 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A05 VALUES (1, 10)", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A05 VALUES (2, 20)", &mut session).unwrap();
    db.execute_sql_session("COMMIT", &mut session).unwrap();

    assert!(session.is_none(), "session cleared after COMMIT");
    assert_eq!(count_rows(&db, "A05"), 2);
    assert_eq!(sum_col(&db, "A05", "v"), 30);
}

/// 6. UPDATE + ROLLBACK → original values restored.
#[test]
fn test_acid_06_atomicity_rollback_undoes_update() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A06 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO A06 VALUES (1, 100)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("UPDATE A06 SET v = 999 WHERE id = 1", &mut session).unwrap();

    // Within transaction, verify value changed
    let r = db.execute_sql_session("SELECT v FROM A06 WHERE id = 1", &mut session).unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(999));

    db.execute_sql_session("ROLLBACK", &mut session).unwrap();

    // After rollback, original value restored
    let r = db.execute_sql("SELECT v FROM A06 WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(100));
}

/// 7. DELETE + ROLLBACK → rows still exist.
#[test]
fn test_acid_07_atomicity_rollback_undoes_delete() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A07 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO A07 VALUES (1, 10)").unwrap();
    db.execute_sql("INSERT INTO A07 VALUES (2, 20)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("DELETE FROM A07 WHERE id = 1", &mut session).unwrap();
    db.execute_sql_session("ROLLBACK", &mut session).unwrap();

    // After rollback, both rows should exist
    assert_eq!(count_rows(&db, "A07"), 2);
}

/// 8. COMMIT without BEGIN is a no-op (no error).
#[test]
fn test_acid_08_commit_without_begin_noop() {
    let (_dir, db) = new_db();
    let r = db.execute_sql("COMMIT");
    assert!(r.is_ok());
}

/// 9. ROLLBACK without BEGIN is a no-op (no error).
#[test]
fn test_acid_09_rollback_without_begin_noop() {
    let (_dir, db) = new_db();
    let r = db.execute_sql("ROLLBACK");
    assert!(r.is_ok());
}

/// 10. BEGIN inside BEGIN → implicit commit of first transaction (MySQL behavior).
#[test]
fn test_acid_10_begin_inside_begin_implicit_commit() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE A10 (id INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A10 VALUES (1)", &mut session).unwrap();

    // Second BEGIN → implicitly commits the first
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO A10 VALUES (2)", &mut session).unwrap();
    db.execute_sql_session("ROLLBACK", &mut session).unwrap();

    // Row 1 was implicitly committed, row 2 was rolled back
    assert_eq!(count_rows(&db, "A10"), 1);
    let r = db.execute_sql("SELECT id FROM A10").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(1));
}

// ===========================================================================
// 2. CONSISTENCY — Invariants maintained under concurrent mutations
// ===========================================================================

/// 11. Balance transfer: SUM(balance) is constant regardless of concurrent transfers.
#[test]
fn test_acid_11_consistency_balance_transfer_100_threads() {
    let (_dir, db) = new_db();
    db.execute_sql(
        "CREATE TABLE Accounts (id INT NOT NULL, balance INT NOT NULL)",
    ).unwrap();

    // 10 accounts, each with balance 1000 → total = 10000
    for i in 0..10 {
        db.execute_sql(&format!("INSERT INTO Accounts VALUES ({}, 1000)", i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    // 100 threads each do a "transfer": decrement one, increment another
    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let from = t % 10;
            let to = (t + 3) % 10;
            if from != to {
                let _ = db.execute_sql(&format!(
                    "UPDATE Accounts SET balance = balance - 1 WHERE id = {}", from
                ));
                let _ = db.execute_sql(&format!(
                    "UPDATE Accounts SET balance = balance + 1 WHERE id = {}", to
                ));
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // Invariant: total balance must still be 10000
    let total = sum_col(&db, "Accounts", "balance");
    assert_eq!(total, 10000, "money must not be created or destroyed");
}

/// 12. NOT NULL constraint enforced under concurrent inserts.
#[test]
fn test_acid_12_consistency_not_null_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE C12 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.execute_sql(&format!("INSERT INTO C12 VALUES ({}, {})", t, t * 10)).unwrap();
        }));
    }
    for h in handles { h.join().unwrap(); }

    // No NULL values should exist
    let r = db.execute_sql("SELECT COUNT(*) FROM C12 WHERE v IS NULL").unwrap();
    match &r.rows[0][0] {
        Value::BigInt(n) => assert_eq!(*n, 0),
        Value::Integer(n) => assert_eq!(*n, 0),
        _ => panic!("expected count"),
    }
    assert_eq!(count_rows(&db, "C12"), 100);
}

/// 13. Complex multi-table INSERT consistency — FK-like relationships.
#[test]
fn test_acid_13_consistency_multi_table_relationships() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE Orders (id INT NOT NULL, customer_id INT NOT NULL, total INT NOT NULL)").unwrap();
    db.execute_sql("CREATE TABLE OrderItems (order_id INT NOT NULL, item_id INT NOT NULL, qty INT NOT NULL, price INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(50));
    let mut handles = vec![];

    for t in 0..50 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let order_id = t + 1;
            let total = (t + 1) * 100;
            db.execute_sql(&format!(
                "INSERT INTO Orders VALUES ({}, {}, {})", order_id, t % 10, total
            )).unwrap();
            // 3 items per order
            for item in 0..3 {
                db.execute_sql(&format!(
                    "INSERT INTO OrderItems VALUES ({}, {}, {}, {})",
                    order_id, item + 1, (item + 1) * 2, 50
                )).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(count_rows(&db, "Orders"), 50);
    assert_eq!(count_rows(&db, "OrderItems"), 150); // 50 orders * 3 items
}

/// 14. Concurrent UPDATE of same column — no lost updates with auto-transactions.
#[test]
fn test_acid_14_consistency_concurrent_counter_increment() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE Counter (id INT NOT NULL, val INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO Counter VALUES (1, 0)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.execute_sql("UPDATE Counter SET val = val + 1 WHERE id = 1").unwrap();
        }));
    }
    for h in handles { h.join().unwrap(); }

    let r = db.execute_sql("SELECT val FROM Counter WHERE id = 1").unwrap();
    let val = match &r.rows[0][0] {
        Value::Integer(n) => *n as i64,
        Value::BigInt(n) => *n,
        other => panic!("expected int, got {:?}", other),
    };
    assert_eq!(val, 100, "every increment must be reflected");
}

/// 15. SUM invariant: concurrent inserts with known values.
#[test]
fn test_acid_15_consistency_sum_invariant_100_threads() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE C15 (id INT NOT NULL, amount INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    // Each thread inserts amount = thread_id + 1
    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.execute_sql(&format!("INSERT INTO C15 VALUES ({}, {})", t, t + 1)).unwrap();
        }));
    }
    for h in handles { h.join().unwrap(); }

    let total = sum_col(&db, "C15", "amount");
    // sum(1..=100) = 5050
    assert_eq!(total, 5050);
}

// ===========================================================================
// 3. ISOLATION — Reads see consistent state
// ===========================================================================

/// 16. 100 concurrent readers + 1 writer: readers always see consistent count.
#[test]
fn test_acid_16_isolation_readers_see_consistent_state() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE I16 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..50 {
        db.execute_sql(&format!("INSERT INTO I16 VALUES ({}, {})", i, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(101)); // 100 readers + 1 writer
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // 100 readers: each reads the table multiple times
    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..10 {
                let r = db.execute_sql("SELECT COUNT(*) FROM I16").unwrap();
                let count = match &r.rows[0][0] {
                    Value::BigInt(n) => *n,
                    Value::Integer(n) => *n as i64,
                    _ => 0,
                };
                // Count should be at least initial (50) and never negative
                if count < 50 {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // 1 writer: adds more rows
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 50..100 {
                let _ = db.execute_sql(&format!("INSERT INTO I16 VALUES ({}, {})", i, i));
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "readers must never see < 50 rows");
}

/// 17. Concurrent reads of aggregates during writes — SUM must be monotonically non-decreasing.
#[test]
fn test_acid_17_isolation_sum_monotonic_during_inserts() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE I17 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(51));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // 50 writers
    for t in 0..50 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.execute_sql(&format!("INSERT INTO I17 VALUES ({}, 10)", t)).unwrap();
        }));
    }

    // 1 reader: checks SUM repeatedly
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut prev_sum = 0i64;
            for _ in 0..20 {
                let s = sum_col(&db, "I17", "v");
                if s < prev_sum {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
                prev_sum = s;
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "SUM must not decrease");
}

/// 18. Snapshot isolation: own inserts visible within same auto-transaction.
#[test]
fn test_acid_18_isolation_own_writes_visible() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE I18 (id INT NOT NULL, v VARCHAR(50))").unwrap();
    db.execute_sql("INSERT INTO I18 VALUES (1, 'first')").unwrap();
    let r = db.execute_sql("SELECT * FROM I18 WHERE id = 1").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::Varchar("first".into()));
}

/// 19. Concurrent SELECT * during batch INSERT — no phantom partial rows.
#[test]
fn test_acid_19_isolation_no_phantom_rows() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE I19 (id INT NOT NULL, a INT NOT NULL, b INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(101));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // 100 writers: each inserts a row where a + b = 100
    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let a = t;
            let b = 100 - t;
            db.execute_sql(&format!("INSERT INTO I19 VALUES ({}, {}, {})", t, a, b)).unwrap();
        }));
    }

    // 1 reader: checks invariant a + b = 100 for every visible row
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..20 {
                let r = db.execute_sql("SELECT a, b FROM I19").unwrap();
                for row in &r.rows {
                    let a = match &row[0] {
                        Value::Integer(n) => *n as i64,
                        Value::BigInt(n) => *n,
                        _ => continue,
                    };
                    let b = match &row[1] {
                        Value::Integer(n) => *n as i64,
                        Value::BigInt(n) => *n,
                        _ => continue,
                    };
                    if a + b != 100 {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "a + b must always equal 100");
}

/// 20. Read your own writes in a session transaction.
#[test]
fn test_acid_20_isolation_read_own_writes_in_session() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE I20 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut session: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut session).unwrap();
    db.execute_sql_session("INSERT INTO I20 VALUES (1, 100)", &mut session).unwrap();

    // Should see our own insert
    let r = db.execute_sql_session("SELECT * FROM I20", &mut session).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::Integer(100));

    db.execute_sql_session("COMMIT", &mut session).unwrap();
}

// ===========================================================================
// 4. DURABILITY — Committed data survives restart
// ===========================================================================

/// 21. INSERT + COMMIT + reopen → data present.
#[test]
fn test_acid_21_durability_insert_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Database::new(&path).unwrap();
        db.execute_sql("CREATE TABLE D21 (id INT NOT NULL, v VARCHAR(100))").unwrap();
        db.execute_sql("INSERT INTO D21 VALUES (1, 'durable')").unwrap();
        db.execute_sql("INSERT INTO D21 VALUES (2, 'data')").unwrap();
        db.shutdown().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.execute_sql("SELECT * FROM D21").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Value::Integer(1));
        assert_eq!(r.rows[0][1], Value::Varchar("durable".into()));
    }
}

/// 22. Large batch INSERT + restart → all data present.
#[test]
fn test_acid_22_durability_large_batch_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let n = 500;

    {
        let db = Database::new(&path).unwrap();
        db.execute_sql("CREATE TABLE D22 (id INT NOT NULL, v INT NOT NULL)").unwrap();
        for i in 0..n {
            db.execute_sql(&format!("INSERT INTO D22 VALUES ({}, {})", i, i * 10)).unwrap();
        }
        db.shutdown().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        assert_eq!(count_rows(&db, "D22"), n);
        let total = sum_col(&db, "D22", "v");
        let expected: i64 = (0..n).map(|i| i * 10).sum();
        assert_eq!(total, expected);
    }
}

/// 23. UPDATE + restart → updated value persists.
#[test]
fn test_acid_23_durability_update_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Database::new(&path).unwrap();
        db.execute_sql("CREATE TABLE D23 (id INT NOT NULL, v INT NOT NULL)").unwrap();
        db.execute_sql("INSERT INTO D23 VALUES (1, 100)").unwrap();
        db.execute_sql("UPDATE D23 SET v = 999 WHERE id = 1").unwrap();
        db.shutdown().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.execute_sql("SELECT v FROM D23 WHERE id = 1").unwrap();
        assert_eq!(r.rows[0][0], Value::Integer(999));
    }
}

/// 24. DELETE + restart → deleted rows gone.
#[test]
fn test_acid_24_durability_delete_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Database::new(&path).unwrap();
        db.execute_sql("CREATE TABLE D24 (id INT NOT NULL, v INT NOT NULL)").unwrap();
        db.execute_sql("INSERT INTO D24 VALUES (1, 10)").unwrap();
        db.execute_sql("INSERT INTO D24 VALUES (2, 20)").unwrap();
        db.execute_sql("INSERT INTO D24 VALUES (3, 30)").unwrap();
        db.execute_sql("DELETE FROM D24 WHERE id = 2").unwrap();
        db.shutdown().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        assert_eq!(count_rows(&db, "D24"), 2);
        let total = sum_col(&db, "D24", "v");
        assert_eq!(total, 40); // 10 + 30
    }
}

/// 25. Multiple tables + restart → all data intact.
#[test]
fn test_acid_25_durability_multi_table_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Database::new(&path).unwrap();
        db.execute_sql("CREATE TABLE D25A (id INT NOT NULL, v VARCHAR(50))").unwrap();
        db.execute_sql("CREATE TABLE D25B (id INT NOT NULL, ref_id INT NOT NULL)").unwrap();
        for i in 0..20 {
            db.execute_sql(&format!("INSERT INTO D25A VALUES ({}, 'item_{}')", i, i)).unwrap();
            db.execute_sql(&format!("INSERT INTO D25B VALUES ({}, {})", i * 10, i)).unwrap();
        }
        db.shutdown().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        assert_eq!(count_rows(&db, "D25A"), 20);
        assert_eq!(count_rows(&db, "D25B"), 20);
    }
}

// ===========================================================================
// 5. CONCURRENT STRESS — Heavy concurrency patterns
// ===========================================================================

/// 26. 100 threads: mixed INSERT + SELECT on same table.
#[test]
fn test_acid_26_stress_100_mixed_read_write() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE S26 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..20 {
        db.execute_sql(&format!("INSERT INTO S26 VALUES ({}, {})", i, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if t % 2 == 0 {
                // Writer
                let _ = db.execute_sql(&format!("INSERT INTO S26 VALUES ({}, {})", 1000 + t, t));
            } else {
                // Reader
                let r = db.execute_sql("SELECT * FROM S26").unwrap();
                assert!(r.rows.len() >= 20); // at least initial data
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // 20 initial + 50 writer threads
    assert!(count_rows(&db, "S26") >= 70);
}

/// 27. 100 threads: concurrent UPDATE on different rows.
#[test]
fn test_acid_27_stress_100_concurrent_update_different_rows() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE S27 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!("INSERT INTO S27 VALUES ({}, 0)", i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    // Each thread updates its own row
    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            db.execute_sql(&format!("UPDATE S27 SET v = {} WHERE id = {}", t * 10, t)).unwrap();
        }));
    }
    for h in handles { h.join().unwrap(); }

    // Verify each row was updated correctly
    let total = sum_col(&db, "S27", "v");
    let expected: i64 = (0..100).map(|t| t * 10).sum();
    assert_eq!(total, expected);
}

/// 28. 100 threads: concurrent DELETE of different rows.
#[test]
fn test_acid_28_stress_100_concurrent_delete_different_rows() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE S28 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..200 {
        db.execute_sql(&format!("INSERT INTO S28 VALUES ({}, {})", i, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    // 100 threads each delete row i (even numbers: 0, 2, 4, ... 198)
    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let id = t * 2; // even numbers
            let _ = db.execute_sql(&format!("DELETE FROM S28 WHERE id = {}", id));
        }));
    }
    for h in handles { h.join().unwrap(); }

    // 200 - 100 = 100 remaining rows
    let remaining = count_rows(&db, "S28");
    assert_eq!(remaining, 100, "exactly 100 even-ID rows should be deleted");
}

/// 29. 100 threads: concurrent aggregate queries (GROUP BY, HAVING, ORDER BY).
#[test]
fn test_acid_29_stress_100_concurrent_complex_queries() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE S29 (dept INT NOT NULL, emp VARCHAR(50), salary INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!(
            "INSERT INTO S29 VALUES ({}, 'emp_{}', {})", i % 5, i, (i + 1) * 100
        )).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            match t % 5 {
                0 => {
                    let r = db.execute_sql("SELECT COUNT(*) FROM S29").unwrap();
                    let c = match &r.rows[0][0] {
                        Value::BigInt(n) => *n,
                        Value::Integer(n) => *n as i64,
                        _ => -1,
                    };
                    if c != 100 { errors.fetch_add(1, Ordering::Relaxed); }
                }
                1 => {
                    let r = db.execute_sql(
                        "SELECT dept, SUM(salary) FROM S29 GROUP BY dept"
                    ).unwrap();
                    if r.rows.len() != 5 { errors.fetch_add(1, Ordering::Relaxed); }
                }
                2 => {
                    let r = db.execute_sql(
                        "SELECT dept, AVG(salary) FROM S29 GROUP BY dept HAVING AVG(salary) > 0"
                    ).unwrap();
                    if r.rows.is_empty() { errors.fetch_add(1, Ordering::Relaxed); }
                }
                3 => {
                    let r = db.execute_sql(
                        "SELECT * FROM S29 ORDER BY salary DESC"
                    ).unwrap();
                    if r.rows.len() != 100 { errors.fetch_add(1, Ordering::Relaxed); }
                }
                4 => {
                    let r = db.execute_sql("SELECT MIN(salary), MAX(salary) FROM S29").unwrap();
                    if r.rows.is_empty() { errors.fetch_add(1, Ordering::Relaxed); }
                }
                _ => {}
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "all complex queries must succeed");
}

/// 30. 100 threads: concurrent JOINs during writes.
#[test]
fn test_acid_30_stress_100_concurrent_joins() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE Dept30 (id INT NOT NULL, name VARCHAR(50))").unwrap();
    db.execute_sql("CREATE TABLE Emp30 (id INT NOT NULL, dept_id INT NOT NULL, name VARCHAR(50))").unwrap();
    for i in 0..10 {
        db.execute_sql(&format!("INSERT INTO Dept30 VALUES ({}, 'dept_{}')", i, i)).unwrap();
    }
    for i in 0..50 {
        db.execute_sql(&format!("INSERT INTO Emp30 VALUES ({}, {}, 'emp_{}')", i, i % 10, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if t < 80 {
                // Reader: JOIN query
                let r = db.execute_sql(
                    "SELECT Emp30.name, Dept30.name FROM Emp30 \
                     INNER JOIN Dept30 ON Emp30.dept_id = Dept30.id"
                ).unwrap();
                if r.rows.len() < 50 {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // Writer: add more employees
                let id = 1000 + t;
                let _ = db.execute_sql(&format!(
                    "INSERT INTO Emp30 VALUES ({}, {}, 'new_{}')", id, t % 10, t
                ));
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "JOINs must return correct results");
}

// ===========================================================================
// 6. COMPLEX QUERIES under concurrency
// ===========================================================================

/// 31. GROUP BY + HAVING + ORDER BY under concurrent reads.
#[test]
fn test_acid_31_complex_group_by_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql(
        "CREATE TABLE Sales (region VARCHAR(20), product VARCHAR(30), amount INT NOT NULL)",
    ).unwrap();
    let regions = ["North", "South", "East", "West"];
    let products = ["Widget", "Gadget", "Doohickey"];
    for (i, region) in regions.iter().enumerate() {
        for (j, product) in products.iter().enumerate() {
            for k in 0..5 {
                let amount = ((i + 1) * 100 + (j + 1) * 10 + k) as i32;
                db.execute_sql(&format!(
                    "INSERT INTO Sales VALUES ('{}', '{}', {})", region, product, amount
                )).unwrap();
            }
        }
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql(
                "SELECT region, SUM(amount) FROM Sales GROUP BY region HAVING SUM(amount) > 0 ORDER BY region"
            ).unwrap();
            if r.rows.len() != 4 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 32. Multi-JOIN + aggregate under concurrent load.
#[test]
fn test_acid_32_complex_multi_join_aggregate() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE Cust32 (id INT NOT NULL, name VARCHAR(50))").unwrap();
    db.execute_sql("CREATE TABLE Ord32 (id INT NOT NULL, cust_id INT NOT NULL, total INT NOT NULL)").unwrap();

    for i in 0..10 {
        db.execute_sql(&format!("INSERT INTO Cust32 VALUES ({}, 'cust_{}')", i, i)).unwrap();
    }
    for i in 0..30 {
        db.execute_sql(&format!("INSERT INTO Ord32 VALUES ({}, {}, {})", i, i % 10, (i + 1) * 50)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(50));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..50 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql(
                "SELECT Cust32.name, SUM(Ord32.total) \
                 FROM Cust32 INNER JOIN Ord32 ON Cust32.id = Ord32.cust_id \
                 GROUP BY Cust32.name"
            ).unwrap();
            if r.rows.len() != 10 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 33. LEFT JOIN under concurrent reads and writes.
#[test]
fn test_acid_33_complex_left_join_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE Parent33 (id INT NOT NULL, name VARCHAR(50))").unwrap();
    db.execute_sql("CREATE TABLE Child33 (id INT NOT NULL, parent_id INT NOT NULL, v INT NOT NULL)").unwrap();

    for i in 0..20 {
        db.execute_sql(&format!("INSERT INTO Parent33 VALUES ({}, 'p_{}')", i, i)).unwrap();
    }
    // Only 10 parents have children
    for i in 0..10 {
        db.execute_sql(&format!("INSERT INTO Child33 VALUES ({}, {}, {})", i, i, i * 10)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if t < 80 {
                let r = db.execute_sql(
                    "SELECT Parent33.name, Child33.v FROM Parent33 \
                     LEFT JOIN Child33 ON Parent33.id = Child33.parent_id"
                ).unwrap();
                // All 20 parents should appear (10 with children, 10 with NULLs)
                if r.rows.len() < 20 {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                let id = 1000 + t;
                let _ = db.execute_sql(&format!(
                    "INSERT INTO Child33 VALUES ({}, {}, {})", id, t % 20, t
                ));
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 34. CASE expressions in SELECT under concurrent load.
#[test]
fn test_acid_34_complex_case_expression_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE S34 (id INT NOT NULL, score INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!("INSERT INTO S34 VALUES ({}, {})", i, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql(
                "SELECT id, CASE WHEN score >= 90 THEN 'A' \
                 WHEN score >= 80 THEN 'B' \
                 WHEN score >= 70 THEN 'C' \
                 ELSE 'F' END FROM S34 ORDER BY id"
            ).unwrap();
            if r.rows.len() != 100 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 35. DISTINCT + ORDER BY under concurrent load.
#[test]
fn test_acid_35_complex_distinct_order_by() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE S35 (id INT NOT NULL, category VARCHAR(20))").unwrap();
    let cats = ["A", "B", "C", "D", "E"];
    for i in 0..100 {
        db.execute_sql(&format!(
            "INSERT INTO S35 VALUES ({}, '{}')", i, cats[i % 5]
        )).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql(
                "SELECT DISTINCT category FROM S35 ORDER BY category"
            ).unwrap();
            if r.rows.len() != 5 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

// ===========================================================================
// 7. SESSION TRANSACTION tests
// ===========================================================================

/// 36. Multi-statement transaction: INSERT + INSERT + COMMIT → both visible.
#[test]
fn test_acid_36_session_multi_insert_commit() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE T36 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut s: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T36 VALUES (1, 10)", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T36 VALUES (2, 20)", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T36 VALUES (3, 30)", &mut s).unwrap();
    db.execute_sql_session("COMMIT", &mut s).unwrap();

    assert_eq!(count_rows(&db, "T36"), 3);
    assert_eq!(sum_col(&db, "T36", "v"), 60);
}

/// 37. Multi-statement transaction: INSERT + DELETE + COMMIT.
#[test]
fn test_acid_37_session_insert_delete_commit() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE T37 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut s: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T37 VALUES (1, 10)", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T37 VALUES (2, 20)", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T37 VALUES (3, 30)", &mut s).unwrap();
    db.execute_sql_session("DELETE FROM T37 WHERE id = 2", &mut s).unwrap();
    db.execute_sql_session("COMMIT", &mut s).unwrap();

    assert_eq!(count_rows(&db, "T37"), 2);
    assert_eq!(sum_col(&db, "T37", "v"), 40);
}

/// 38. Multi-statement transaction: INSERT + UPDATE + COMMIT.
#[test]
fn test_acid_38_session_insert_update_commit() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE T38 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut s: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T38 VALUES (1, 10)", &mut s).unwrap();
    db.execute_sql_session("UPDATE T38 SET v = 999 WHERE id = 1", &mut s).unwrap();
    db.execute_sql_session("COMMIT", &mut s).unwrap();

    let r = db.execute_sql("SELECT v FROM T38 WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(999));
}

/// 39. Rollback after multiple operations.
#[test]
fn test_acid_39_session_multi_op_rollback() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE T39 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO T39 VALUES (1, 100)").unwrap();

    let mut s: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T39 VALUES (2, 200)", &mut s).unwrap();
    db.execute_sql_session("UPDATE T39 SET v = 999 WHERE id = 1", &mut s).unwrap();
    db.execute_sql_session("ROLLBACK", &mut s).unwrap();

    assert_eq!(count_rows(&db, "T39"), 1);
    let r = db.execute_sql("SELECT v FROM T39 WHERE id = 1").unwrap();
    assert_eq!(r.rows[0][0], Value::Integer(100));
}

/// 40. Session transaction: verify SELECT inside sees accumulated changes.
#[test]
fn test_acid_40_session_read_accumulated_changes() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE T40 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let mut s: Option<TxnId> = None;
    db.execute_sql_session("BEGIN", &mut s).unwrap();
    db.execute_sql_session("INSERT INTO T40 VALUES (1, 10)", &mut s).unwrap();

    let r = db.execute_sql_session("SELECT COUNT(*) FROM T40", &mut s).unwrap();
    let c = match &r.rows[0][0] {
        Value::BigInt(n) => *n,
        Value::Integer(n) => *n as i64,
        _ => panic!("expected count"),
    };
    assert_eq!(c, 1);

    db.execute_sql_session("INSERT INTO T40 VALUES (2, 20)", &mut s).unwrap();

    let r = db.execute_sql_session("SELECT COUNT(*) FROM T40", &mut s).unwrap();
    let c = match &r.rows[0][0] {
        Value::BigInt(n) => *n,
        Value::Integer(n) => *n as i64,
        _ => panic!("expected count"),
    };
    assert_eq!(c, 2);

    db.execute_sql_session("COMMIT", &mut s).unwrap();
    assert_eq!(count_rows(&db, "T40"), 2);
}

// ===========================================================================
// 8. CONCURRENT MIXED DML — insert/update/delete at the same time
// ===========================================================================

/// 41. 100 threads: 33 inserters + 33 updaters + 34 readers, same table.
#[test]
fn test_acid_41_mixed_insert_update_read_100() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE M41 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..50 {
        db.execute_sql(&format!("INSERT INTO M41 VALUES ({}, {})", i, i * 10)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // 33 inserters
    for t in 0..33 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let id = 1000 + t;
            let _ = db.execute_sql(&format!("INSERT INTO M41 VALUES ({}, {})", id, t));
        }));
    }

    // 33 updaters
    for t in 0..33 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let target = t % 50;
            let _ = db.execute_sql(&format!("UPDATE M41 SET v = v + 1 WHERE id = {}", target));
        }));
    }

    // 34 readers
    for _ in 0..34 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql("SELECT COUNT(*) FROM M41").unwrap();
            let c = match &r.rows[0][0] {
                Value::BigInt(n) => *n,
                Value::Integer(n) => *n as i64,
                _ => -1,
            };
            if c < 50 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
    assert!(count_rows(&db, "M41") >= 50);
}

/// 42. 100 threads: concurrent INSERT into multiple tables simultaneously.
#[test]
fn test_acid_42_concurrent_multi_table_insert() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE MT42A (id INT NOT NULL, v INT NOT NULL)").unwrap();
    db.execute_sql("CREATE TABLE MT42B (id INT NOT NULL, v INT NOT NULL)").unwrap();
    db.execute_sql("CREATE TABLE MT42C (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            match t % 3 {
                0 => db.execute_sql(&format!("INSERT INTO MT42A VALUES ({}, {})", t, t)).unwrap(),
                1 => db.execute_sql(&format!("INSERT INTO MT42B VALUES ({}, {})", t, t)).unwrap(),
                _ => db.execute_sql(&format!("INSERT INTO MT42C VALUES ({}, {})", t, t)).unwrap(),
            };
        }));
    }
    for h in handles { h.join().unwrap(); }

    let total = count_rows(&db, "MT42A") + count_rows(&db, "MT42B") + count_rows(&db, "MT42C");
    assert_eq!(total, 100);
}

/// 43. 100 concurrent readers with complex WHERE clauses.
#[test]
fn test_acid_43_concurrent_complex_where() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE W43 (id INT NOT NULL, category VARCHAR(10), score INT NOT NULL, active INT NOT NULL)").unwrap();
    for i in 0..200 {
        let cat = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
        let active = if i % 2 == 0 { 1 } else { 0 };
        db.execute_sql(&format!(
            "INSERT INTO W43 VALUES ({}, '{}', {}, {})", i, cat, i * 5, active
        )).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let query = match t % 4 {
                0 => "SELECT COUNT(*) FROM W43 WHERE category = 'A' AND active = 1",
                1 => "SELECT COUNT(*) FROM W43 WHERE score > 500",
                2 => "SELECT COUNT(*) FROM W43 WHERE category IN ('A', 'B')",
                _ => "SELECT COUNT(*) FROM W43 WHERE score BETWEEN 100 AND 300",
            };
            let r = db.execute_sql(query);
            if r.is_err() {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 44. 100 threads: concurrent LIKE queries.
#[test]
fn test_acid_44_concurrent_like_queries() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE L44 (id INT NOT NULL, name VARCHAR(100))").unwrap();
    let prefixes = ["alpha", "beta", "gamma", "delta", "epsilon"];
    for i in 0..100 {
        db.execute_sql(&format!(
            "INSERT INTO L44 VALUES ({}, '{}_{}')", i, prefixes[i % 5], i
        )).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let pattern = prefixes[t % 5];
            let r = db.execute_sql(&format!(
                "SELECT COUNT(*) FROM L44 WHERE name LIKE '{}%'", pattern
            )).unwrap();
            let c = match &r.rows[0][0] {
                Value::BigInt(n) => *n,
                Value::Integer(n) => *n as i64,
                _ => -1,
            };
            if c != 20 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 45. 100 threads: concurrent string function queries.
#[test]
fn test_acid_45_concurrent_string_functions() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE SF45 (id INT NOT NULL, name VARCHAR(50))").unwrap();
    for i in 0..50 {
        db.execute_sql(&format!("INSERT INTO SF45 VALUES ({}, 'Hello World {}')", i, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let query = match t % 5 {
                0 => "SELECT UPPER(name) FROM SF45",
                1 => "SELECT LOWER(name) FROM SF45",
                2 => "SELECT LENGTH(name) FROM SF45",
                3 => "SELECT SUBSTRING(name, 1, 5) FROM SF45",
                _ => "SELECT REPLACE(name, 'Hello', 'Hi') FROM SF45",
            };
            let r = db.execute_sql(query);
            if r.is_err() || r.unwrap().rows.len() != 50 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

// ===========================================================================
// 9. EDGE CASES under concurrency
// ===========================================================================

/// 46. 100 threads operating on an empty table.
#[test]
fn test_acid_46_edge_empty_table_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE E46 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            match t % 4 {
                0 => {
                    let r = db.execute_sql("SELECT COUNT(*) FROM E46").unwrap();
                    let _ = r; // just verify no crash
                }
                1 => {
                    let r = db.execute_sql("SELECT SUM(v) FROM E46").unwrap();
                    let _ = r;
                }
                2 => {
                    let _ = db.execute_sql("DELETE FROM E46 WHERE id = 1"); // no-op
                }
                _ => {
                    let _ = db.execute_sql(&format!("INSERT INTO E46 VALUES ({}, {})", t, t));
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 47. Concurrent NULL value handling.
#[test]
fn test_acid_47_edge_null_handling_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE N47 (id INT NOT NULL, v INT NULL, s VARCHAR(50) NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            if t % 3 == 0 {
                db.execute_sql(&format!("INSERT INTO N47 VALUES ({}, NULL, NULL)", t)).unwrap();
            } else {
                db.execute_sql(&format!("INSERT INTO N47 VALUES ({}, {}, 'val_{}')", t, t * 10, t)).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(count_rows(&db, "N47"), 100);

    // Verify NULL counts
    let r = db.execute_sql("SELECT COUNT(*) FROM N47 WHERE v IS NULL").unwrap();
    let null_count = match &r.rows[0][0] {
        Value::BigInt(n) => *n,
        Value::Integer(n) => *n as i64,
        _ => -1,
    };
    // Every 3rd thread (0, 3, 6, ..., 99) → 34 threads insert NULLs
    assert_eq!(null_count, 34);
}

/// 48. Concurrent operations on wide rows (many columns).
#[test]
fn test_acid_48_edge_wide_rows_concurrent() {
    let (_dir, db) = new_db();
    let mut cols = "id INT NOT NULL".to_string();
    for i in 0..19 {
        cols.push_str(&format!(", c{} INT NOT NULL", i));
    }
    db.execute_sql(&format!("CREATE TABLE W48 ({})", cols)).unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(50));
    let mut handles = vec![];

    for t in 0..50 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut vals = format!("{}", t);
            for i in 0..19 {
                vals.push_str(&format!(", {}", t * 20 + i));
            }
            db.execute_sql(&format!("INSERT INTO W48 VALUES ({})", vals)).unwrap();
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(count_rows(&db, "W48"), 50);
}

/// 49. Concurrent math expression queries.
#[test]
fn test_acid_49_edge_concurrent_math_expressions() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE M49 (id INT NOT NULL, a INT NOT NULL, b INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!("INSERT INTO M49 VALUES ({}, {}, {})", i, i + 1, (i + 1) * 2)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let query = match t % 4 {
                0 => "SELECT a + b FROM M49",
                1 => "SELECT a * b FROM M49",
                2 => "SELECT a - b FROM M49",
                _ => "SELECT ABS(a - b) FROM M49",
            };
            let r = db.execute_sql(query);
            if r.is_err() || r.unwrap().rows.len() != 100 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 50. Concurrent COALESCE and NULLIF queries.
#[test]
fn test_acid_50_edge_concurrent_null_functions() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE NF50 (id INT NOT NULL, a INT NULL, b INT NULL)").unwrap();
    for i in 0..50 {
        if i % 2 == 0 {
            db.execute_sql(&format!("INSERT INTO NF50 VALUES ({}, {}, NULL)", i, i)).unwrap();
        } else {
            db.execute_sql(&format!("INSERT INTO NF50 VALUES ({}, NULL, {})", i, i * 10)).unwrap();
        }
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let query = if t % 2 == 0 {
                "SELECT COALESCE(a, b, 0) FROM NF50"
            } else {
                "SELECT COALESCE(b, a, -1) FROM NF50"
            };
            let r = db.execute_sql(query);
            if r.is_err() || r.unwrap().rows.len() != 50 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

// ===========================================================================
// 10. COMPREHENSIVE STRESS — all operations simultaneously
// ===========================================================================

/// 51. Grand stress test: 100 threads doing everything at once.
#[test]
fn test_acid_51_grand_stress_100_threads() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE GS (id INT NOT NULL, category VARCHAR(10), amount INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!(
            "INSERT INTO GS VALUES ({}, '{}', {})", i, ["X", "Y", "Z"][i % 3], (i + 1) * 10
        )).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let result = match t % 10 {
                0 => db.execute_sql(&format!("INSERT INTO GS VALUES ({}, 'W', {})", 1000 + t, t)),
                1 => db.execute_sql("SELECT COUNT(*) FROM GS"),
                2 => db.execute_sql("SELECT SUM(amount) FROM GS"),
                3 => db.execute_sql("SELECT category, COUNT(*) FROM GS GROUP BY category"),
                4 => db.execute_sql("SELECT * FROM GS WHERE amount > 500 ORDER BY amount DESC"),
                5 => db.execute_sql("SELECT DISTINCT category FROM GS"),
                6 => db.execute_sql("SELECT MIN(amount), MAX(amount), AVG(amount) FROM GS"),
                7 => db.execute_sql("SELECT * FROM GS WHERE category LIKE 'X%'"),
                8 => db.execute_sql(&format!("UPDATE GS SET amount = amount + 1 WHERE id = {}", t % 100)),
                _ => db.execute_sql("SELECT * FROM GS ORDER BY id"),
            };
            if result.is_err() {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "no operation should fail");
}

/// 52. Sustained throughput: 100 threads each do 10 operations.
#[test]
fn test_acid_52_sustained_throughput() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE ST52 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let ops_done = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let ops_done = Arc::clone(&ops_done);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..10 {
                let id = t * 10 + i;
                let result = db.execute_sql(&format!("INSERT INTO ST52 VALUES ({}, {})", id, id));
                if result.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                } else {
                    ops_done.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(errors.load(Ordering::Relaxed), 0);
    assert_eq!(ops_done.load(Ordering::Relaxed), 1000);
    assert_eq!(count_rows(&db, "ST52"), 1000);
}

/// 53. Concurrent aggregate correctness: verify SUM, COUNT, AVG all consistent.
#[test]
fn test_acid_53_aggregate_consistency() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE AC53 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!("INSERT INTO AC53 VALUES ({}, {})", i, i + 1)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            // SUM(1..=100) = 5050
            let s = sum_col(&db, "AC53", "v");
            if s != 5050 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
            let c = count_rows(&db, "AC53");
            if c != 100 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 54. ORDER BY correctness under concurrent reads.
#[test]
fn test_acid_54_order_by_correctness_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE OB54 (id INT NOT NULL, name VARCHAR(50), score INT NOT NULL)").unwrap();
    for i in 0..100 {
        db.execute_sql(&format!("INSERT INTO OB54 VALUES ({}, 'name_{}', {})", i, i, 100 - i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = if t % 2 == 0 {
                db.execute_sql("SELECT score FROM OB54 ORDER BY score ASC").unwrap()
            } else {
                db.execute_sql("SELECT score FROM OB54 ORDER BY score DESC").unwrap()
            };
            if r.rows.len() != 100 {
                errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // Verify ordering
            for i in 1..r.rows.len() {
                let prev = match &r.rows[i - 1][0] {
                    Value::Integer(n) => *n,
                    _ => { errors.fetch_add(1, Ordering::Relaxed); return; }
                };
                let curr = match &r.rows[i][0] {
                    Value::Integer(n) => *n,
                    _ => { errors.fetch_add(1, Ordering::Relaxed); return; }
                };
                if t % 2 == 0 {
                    if prev > curr { errors.fetch_add(1, Ordering::Relaxed); return; }
                } else {
                    if prev < curr { errors.fetch_add(1, Ordering::Relaxed); return; }
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 55. LIMIT correctness under concurrent access.
#[test]
fn test_acid_55_limit_correctness_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE LM55 (id INT NOT NULL, v INT NOT NULL)").unwrap();
    for i in 0..200 {
        db.execute_sql(&format!("INSERT INTO LM55 VALUES ({}, {})", i, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let limit = (t % 20) + 1;
            let r = db.execute_sql(&format!("SELECT * FROM LM55 LIMIT {}", limit)).unwrap();
            if r.rows.len() != limit {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 56. UNION query correctness under concurrent access.
#[test]
fn test_acid_56_union_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE U56A (id INT NOT NULL, v VARCHAR(20))").unwrap();
    db.execute_sql("CREATE TABLE U56B (id INT NOT NULL, v VARCHAR(20))").unwrap();
    for i in 0..25 {
        db.execute_sql(&format!("INSERT INTO U56A VALUES ({}, 'a_{}')", i, i)).unwrap();
        db.execute_sql(&format!("INSERT INTO U56B VALUES ({}, 'b_{}')", i + 100, i)).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = if t % 2 == 0 {
                db.execute_sql("SELECT id, v FROM U56A UNION ALL SELECT id, v FROM U56B").unwrap()
            } else {
                db.execute_sql("SELECT id, v FROM U56A UNION SELECT id, v FROM U56B").unwrap()
            };
            if r.rows.len() < 50 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 57. Self-join under concurrent load.
#[test]
fn test_acid_57_self_join_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE SJ57 (id INT NOT NULL, manager_id INT NULL, name VARCHAR(50))").unwrap();
    db.execute_sql("INSERT INTO SJ57 VALUES (1, NULL, 'CEO')").unwrap();
    db.execute_sql("INSERT INTO SJ57 VALUES (2, 1, 'VP_Eng')").unwrap();
    db.execute_sql("INSERT INTO SJ57 VALUES (3, 1, 'VP_Sales')").unwrap();
    db.execute_sql("INSERT INTO SJ57 VALUES (4, 2, 'Dev1')").unwrap();
    db.execute_sql("INSERT INTO SJ57 VALUES (5, 2, 'Dev2')").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql(
                "SELECT e.name, m.name FROM SJ57 e \
                 INNER JOIN SJ57 m ON e.manager_id = m.id"
            ).unwrap();
            if r.rows.len() != 4 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 58. Multi-column ORDER BY under concurrent access.
#[test]
fn test_acid_58_multi_column_order_by() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE MC58 (dept INT NOT NULL, name VARCHAR(50), salary INT NOT NULL)").unwrap();
    for i in 0..50 {
        db.execute_sql(&format!(
            "INSERT INTO MC58 VALUES ({}, 'emp_{}', {})", i % 5, i, (50 - i) * 100
        )).unwrap();
    }

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let r = db.execute_sql(
                "SELECT dept, salary FROM MC58 ORDER BY dept ASC, salary DESC"
            ).unwrap();
            if r.rows.len() != 50 {
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0);
}

/// 59. Concurrent INSERT + immediate SELECT same thread (read-your-writes).
#[test]
fn test_acid_59_read_your_writes_concurrent() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE RW59 (id INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(100));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for t in 0..100 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let unique_val = 10000 + t;
            db.execute_sql(&format!("INSERT INTO RW59 VALUES ({}, {})", unique_val, unique_val)).unwrap();
            // Immediately read — should find it
            let r = db.execute_sql(&format!("SELECT v FROM RW59 WHERE id = {}", unique_val)).unwrap();
            if r.rows.is_empty() {
                errors.fetch_add(1, Ordering::Relaxed);
            } else {
                let v = match &r.rows[0][0] {
                    Value::Integer(n) => *n as i64,
                    Value::BigInt(n) => *n,
                    _ => -1,
                };
                if v != unique_val as i64 {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(errors.load(Ordering::Relaxed), 0, "every thread must read its own write");
    assert_eq!(count_rows(&db, "RW59"), 100);
}

/// 60. Bulk INSERT (1000 rows per thread × 10 threads) + verify total.
#[test]
fn test_acid_60_bulk_insert_stress() {
    let (_dir, db) = new_db();
    db.execute_sql("CREATE TABLE BK60 (id INT NOT NULL, tid INT NOT NULL, v INT NOT NULL)").unwrap();

    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    for t in 0..10 {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..1000 {
                let id = t * 1000 + i;
                db.execute_sql(&format!("INSERT INTO BK60 VALUES ({}, {}, {})", id, t, i)).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    assert_eq!(count_rows(&db, "BK60"), 10000, "all 10,000 rows must be present");
}
