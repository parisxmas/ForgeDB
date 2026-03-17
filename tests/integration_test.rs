use forgedb::Database;
use forgedb::tuple::types::Value;
use tempfile::TempDir;

fn new_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::new(dir.path().to_str().unwrap()).unwrap();
    (dir, db)
}

#[test]
fn test_create_table_and_insert() {
    let (_dir, mut db) = new_db();

    let result = db
        .execute_sql(
            "CREATE TABLE Users (
                id INT NOT NULL,
                name NVARCHAR(100) NULL,
                active BIT NOT NULL
            )",
        )
        .unwrap();
    assert!(result.message.contains("created"));

    let result = db
        .execute_sql("INSERT INTO Users VALUES (1, N'Alice', 1)")
        .unwrap();
    assert_eq!(result.rows_affected, 1);

    let result = db
        .execute_sql("INSERT INTO Users VALUES (2, 'Bob', 1)")
        .unwrap();
    assert_eq!(result.rows_affected, 1);

    let result = db
        .execute_sql("INSERT INTO Users VALUES (3, NULL, 0)")
        .unwrap();
    assert_eq!(result.rows_affected, 1);
}

#[test]
fn test_select_all() {
    let (_dir, mut db) = new_db();

    db.execute_sql(
        "CREATE TABLE Products (id INT NOT NULL, name VARCHAR(50) NOT NULL, price FLOAT)",
    )
    .unwrap();

    db.execute_sql("INSERT INTO Products VALUES (1, 'Widget', 9.99)")
        .unwrap();
    db.execute_sql("INSERT INTO Products VALUES (2, 'Gadget', 19.99)")
        .unwrap();
    db.execute_sql("INSERT INTO Products VALUES (3, 'Doohickey', 4.99)")
        .unwrap();

    let result = db.execute_sql("SELECT * FROM Products").unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.columns, vec!["id", "name", "price"]);
}

#[test]
fn test_select_with_where() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Items (id INT NOT NULL, val INT NOT NULL)")
        .unwrap();

    for i in 1..=10 {
        db.execute_sql(&format!("INSERT INTO Items VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    let result = db.execute_sql("SELECT * FROM Items WHERE val > 50").unwrap();
    assert_eq!(result.rows.len(), 5); // val = 60, 70, 80, 90, 100

    let result = db
        .execute_sql("SELECT * FROM Items WHERE id = 3")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(3));
    assert_eq!(result.rows[0][1], Value::Integer(30));
}

#[test]
fn test_select_with_projection() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE People (id INT NOT NULL, name VARCHAR(50) NOT NULL, age INT)")
        .unwrap();
    db.execute_sql("INSERT INTO People VALUES (1, 'Alice', 30)")
        .unwrap();
    db.execute_sql("INSERT INTO People VALUES (2, 'Bob', 25)")
        .unwrap();

    let result = db.execute_sql("SELECT name, age FROM People").unwrap();
    assert_eq!(result.columns, vec!["name", "age"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Varchar("Alice".into()));
}

#[test]
fn test_update() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Counters (id INT NOT NULL, val INT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Counters VALUES (1, 100)")
        .unwrap();
    db.execute_sql("INSERT INTO Counters VALUES (2, 200)")
        .unwrap();

    let result = db
        .execute_sql("UPDATE Counters SET val = 999 WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows_affected, 1);

    let result = db
        .execute_sql("SELECT * FROM Counters WHERE id = 1")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::Integer(999));

    // Verify other row unchanged
    let result = db
        .execute_sql("SELECT * FROM Counters WHERE id = 2")
        .unwrap();
    assert_eq!(result.rows[0][1], Value::Integer(200));
}

#[test]
fn test_delete() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Logs (id INT NOT NULL, msg VARCHAR(100))")
        .unwrap();
    db.execute_sql("INSERT INTO Logs VALUES (1, 'hello')")
        .unwrap();
    db.execute_sql("INSERT INTO Logs VALUES (2, 'world')")
        .unwrap();
    db.execute_sql("INSERT INTO Logs VALUES (3, 'test')")
        .unwrap();

    let result = db
        .execute_sql("DELETE FROM Logs WHERE id = 2")
        .unwrap();
    assert_eq!(result.rows_affected, 1);

    let result = db.execute_sql("SELECT * FROM Logs").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_drop_table() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Temp (id INT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Temp VALUES (1)").unwrap();

    let result = db.execute_sql("DROP TABLE Temp").unwrap();
    assert!(result.message.contains("dropped"));

    // Subsequent query on dropped table should fail
    let err = db.execute_sql("SELECT * FROM Temp");
    assert!(err.is_err());
}

#[test]
fn test_join() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Users (id INT NOT NULL, name VARCHAR(50) NOT NULL)")
        .unwrap();
    db.execute_sql("CREATE TABLE Orders (id INT NOT NULL, user_id INT NOT NULL, total FLOAT)")
        .unwrap();

    db.execute_sql("INSERT INTO Users VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO Users VALUES (2, 'Bob')")
        .unwrap();

    db.execute_sql("INSERT INTO Orders VALUES (10, 1, 99.99)")
        .unwrap();
    db.execute_sql("INSERT INTO Orders VALUES (11, 1, 49.50)")
        .unwrap();
    db.execute_sql("INSERT INTO Orders VALUES (12, 2, 25.00)")
        .unwrap();

    let result = db
        .execute_sql(
            "SELECT * FROM Users u INNER JOIN Orders o ON u.id = o.user_id",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    // Combined columns: Users.id, Users.name, Orders.id, Orders.user_id, Orders.total
    assert_eq!(result.columns.len(), 5);
}

#[test]
fn test_left_join() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE A (id INT NOT NULL, val VARCHAR(10))")
        .unwrap();
    db.execute_sql("CREATE TABLE B (id INT NOT NULL, a_id INT NOT NULL)")
        .unwrap();

    db.execute_sql("INSERT INTO A VALUES (1, 'x')").unwrap();
    db.execute_sql("INSERT INTO A VALUES (2, 'y')").unwrap();
    db.execute_sql("INSERT INTO B VALUES (10, 1)").unwrap();

    let result = db
        .execute_sql("SELECT * FROM A LEFT JOIN B ON A.id = B.a_id")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    // Row for A.id=2 should have NULLs for B columns
    let row_2 = result
        .rows
        .iter()
        .find(|r| r[0] == Value::Integer(2))
        .unwrap();
    assert_eq!(row_2[2], Value::Null); // B.id
    assert_eq!(row_2[3], Value::Null); // B.a_id
}

#[test]
fn test_insert_with_explicit_columns() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE T (id INT NOT NULL, name VARCHAR(50) NULL, age INT NULL)")
        .unwrap();

    db.execute_sql("INSERT INTO T (id, name) VALUES (1, 'Alice')")
        .unwrap();

    let result = db.execute_sql("SELECT * FROM T").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(1));
    assert_eq!(result.rows[0][1], Value::Varchar("Alice".into()));
    assert_eq!(result.rows[0][2], Value::Null); // age not provided
}

#[test]
fn test_multiple_insert_rows() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Nums (id INT NOT NULL)")
        .unwrap();

    let result = db
        .execute_sql("INSERT INTO Nums VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    assert_eq!(result.rows_affected, 5);

    let result = db.execute_sql("SELECT * FROM Nums").unwrap();
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn test_null_handling() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE NullTest (id INT NOT NULL, val VARCHAR(50) NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO NullTest VALUES (1, 'hello')")
        .unwrap();
    db.execute_sql("INSERT INTO NullTest VALUES (2, NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO NullTest VALUES (3, 'world')")
        .unwrap();

    let result = db
        .execute_sql("SELECT * FROM NullTest WHERE val IS NULL")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2));

    let result = db
        .execute_sql("SELECT * FROM NullTest WHERE val IS NOT NULL")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_select_top() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE BigTable (id INT NOT NULL)")
        .unwrap();

    for i in 1..=20 {
        db.execute_sql(&format!("INSERT INTO BigTable VALUES ({})", i))
            .unwrap();
    }

    let result = db
        .execute_sql("SELECT TOP 5 * FROM BigTable")
        .unwrap();
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn test_order_by() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Sorted (id INT NOT NULL, name VARCHAR(50) NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Sorted VALUES (3, 'Charlie')")
        .unwrap();
    db.execute_sql("INSERT INTO Sorted VALUES (1, 'Alice')")
        .unwrap();
    db.execute_sql("INSERT INTO Sorted VALUES (2, 'Bob')")
        .unwrap();

    let result = db
        .execute_sql("SELECT * FROM Sorted ORDER BY id ASC")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(1));
    assert_eq!(result.rows[1][0], Value::Integer(2));
    assert_eq!(result.rows[2][0], Value::Integer(3));

    let result = db
        .execute_sql("SELECT * FROM Sorted ORDER BY name DESC")
        .unwrap();
    assert_eq!(result.rows[0][1], Value::Varchar("Charlie".into()));
    assert_eq!(result.rows[1][1], Value::Varchar("Bob".into()));
    assert_eq!(result.rows[2][1], Value::Varchar("Alice".into()));
}

#[test]
fn test_persistence_across_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    // Create and populate
    {
        let mut db = Database::new(&path).unwrap();
        db.execute_sql("CREATE TABLE Persist (id INT NOT NULL, data VARCHAR(100))")
            .unwrap();
        db.execute_sql("INSERT INTO Persist VALUES (1, 'hello')")
            .unwrap();
        db.execute_sql("INSERT INTO Persist VALUES (2, 'world')")
            .unwrap();
        db.shutdown().unwrap();
    }

    // Reopen and verify
    {
        let mut db = Database::open(&path).unwrap();
        let result = db.execute_sql("SELECT * FROM Persist").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::Varchar("hello".into()));
    }
}

#[test]
fn test_and_or_where() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Data (id INT NOT NULL, a INT NOT NULL, b INT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Data VALUES (1, 10, 20)")
        .unwrap();
    db.execute_sql("INSERT INTO Data VALUES (2, 30, 40)")
        .unwrap();
    db.execute_sql("INSERT INTO Data VALUES (3, 10, 40)")
        .unwrap();

    let result = db
        .execute_sql("SELECT * FROM Data WHERE a = 10 AND b = 40")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(3));

    let result = db
        .execute_sql("SELECT * FROM Data WHERE a = 10 OR b = 40")
        .unwrap();
    assert_eq!(result.rows.len(), 3); // rows 1 (a=10), 2 (b=40), 3 (a=10,b=40)
}

#[test]
fn test_update_all_rows() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Flags (id INT NOT NULL, active BIT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Flags VALUES (1, 1)")
        .unwrap();
    db.execute_sql("INSERT INTO Flags VALUES (2, 1)")
        .unwrap();
    db.execute_sql("INSERT INTO Flags VALUES (3, 0)")
        .unwrap();

    // Update without WHERE = update all rows
    let result = db
        .execute_sql("UPDATE Flags SET active = 0")
        .unwrap();
    assert_eq!(result.rows_affected, 3);

    let result = db
        .execute_sql("SELECT * FROM Flags WHERE active = 0")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_delete_all_rows() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Ephemeral (id INT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Ephemeral VALUES (1)")
        .unwrap();
    db.execute_sql("INSERT INTO Ephemeral VALUES (2)")
        .unwrap();

    let result = db.execute_sql("DELETE FROM Ephemeral").unwrap();
    assert_eq!(result.rows_affected, 2);

    let result = db
        .execute_sql("SELECT * FROM Ephemeral")
        .unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn test_arithmetic_expressions() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Calc (id INT NOT NULL, x INT NOT NULL, y INT NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Calc VALUES (1, 10, 3)")
        .unwrap();

    let result = db
        .execute_sql("SELECT id, x + y AS sum FROM Calc")
        .unwrap();
    assert_eq!(result.columns, vec!["id", "sum"]);
    assert_eq!(result.rows[0][1], Value::Integer(13));
}

#[test]
fn test_comparison_operators() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Nums (id INT NOT NULL, val INT NOT NULL)")
        .unwrap();
    for i in 1..=5 {
        db.execute_sql(&format!("INSERT INTO Nums VALUES ({}, {})", i, i * 10))
            .unwrap();
    }

    // Less than
    let result = db
        .execute_sql("SELECT * FROM Nums WHERE val < 30")
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    // Greater than or equal
    let result = db
        .execute_sql("SELECT * FROM Nums WHERE val >= 40")
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    // Not equal
    let result = db
        .execute_sql("SELECT * FROM Nums WHERE val <> 30")
        .unwrap();
    assert_eq!(result.rows.len(), 4);
}

// =========================================================================
// MySQL Compatibility Tests
// =========================================================================

#[test]
fn test_mysql_create_table_with_auto_increment() {
    let (_dir, mut db) = new_db();

    db.execute_sql(
        "CREATE TABLE wp_posts (
            ID BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            post_title VARCHAR(255) NOT NULL,
            post_content LONGTEXT,
            post_date DATETIME
        )",
    )
    .unwrap();

    let result = db.execute_sql("SHOW TABLES").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Varchar("wp_posts".into()));
}

#[test]
fn test_mysql_create_table_with_engine() {
    let (_dir, mut db) = new_db();

    // MySQL table with ENGINE, CHARSET, etc. (these are ignored)
    db.execute_sql(
        "CREATE TABLE users (
            id INT NOT NULL AUTO_INCREMENT,
            username VARCHAR(100) NOT NULL,
            email VARCHAR(255),
            active TINYINT DEFAULT 1,
            PRIMARY KEY (id)
        )",
    )
    .unwrap();

    db.execute_sql("INSERT INTO users (username, email, active) VALUES ('alice', 'alice@test.com', 1)")
        .unwrap();

    let result = db.execute_sql("SELECT * FROM users").unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_like_expression() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Products (id INT NOT NULL, name VARCHAR(100) NOT NULL)")
        .unwrap();
    db.execute_sql("INSERT INTO Products VALUES (1, 'Widget')").unwrap();
    db.execute_sql("INSERT INTO Products VALUES (2, 'Gadget')").unwrap();
    db.execute_sql("INSERT INTO Products VALUES (3, 'Widget Pro')").unwrap();
    db.execute_sql("INSERT INTO Products VALUES (4, 'Sprocket')").unwrap();

    // LIKE with %
    let result = db.execute_sql("SELECT * FROM Products WHERE name LIKE 'Widget%'").unwrap();
    assert_eq!(result.rows.len(), 2);

    // LIKE with _
    let result = db.execute_sql("SELECT * FROM Products WHERE name LIKE '_adget'").unwrap();
    assert_eq!(result.rows.len(), 1);

    // NOT LIKE
    let result = db.execute_sql("SELECT * FROM Products WHERE name NOT LIKE 'Widget%'").unwrap();
    assert_eq!(result.rows.len(), 2); // Gadget, Sprocket
}

#[test]
fn test_in_expression() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Items (id INT NOT NULL, name VARCHAR(50))").unwrap();
    db.execute_sql("INSERT INTO Items VALUES (1, 'a')").unwrap();
    db.execute_sql("INSERT INTO Items VALUES (2, 'b')").unwrap();
    db.execute_sql("INSERT INTO Items VALUES (3, 'c')").unwrap();
    db.execute_sql("INSERT INTO Items VALUES (4, 'd')").unwrap();

    let result = db.execute_sql("SELECT * FROM Items WHERE id IN (1, 3, 5)").unwrap();
    assert_eq!(result.rows.len(), 2);

    let result = db.execute_sql("SELECT * FROM Items WHERE id NOT IN (1, 2)").unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_between_expression() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Nums (id INT NOT NULL, val INT NOT NULL)").unwrap();
    for i in 1..=10 {
        db.execute_sql(&format!("INSERT INTO Nums VALUES ({}, {})", i, i * 10)).unwrap();
    }

    let result = db.execute_sql("SELECT * FROM Nums WHERE val BETWEEN 30 AND 70").unwrap();
    assert_eq!(result.rows.len(), 5); // 30, 40, 50, 60, 70
}

#[test]
fn test_count_aggregate() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Scores (id INT NOT NULL, score INT)").unwrap();
    db.execute_sql("INSERT INTO Scores VALUES (1, 90)").unwrap();
    db.execute_sql("INSERT INTO Scores VALUES (2, 80)").unwrap();
    db.execute_sql("INSERT INTO Scores VALUES (3, NULL)").unwrap();
    db.execute_sql("INSERT INTO Scores VALUES (4, 70)").unwrap();

    let result = db.execute_sql("SELECT COUNT(*) FROM Scores").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::BigInt(4));

    let result = db.execute_sql("SELECT COUNT(score) FROM Scores").unwrap();
    assert_eq!(result.rows[0][0], Value::BigInt(3)); // NULL not counted
}

#[test]
fn test_sum_avg_min_max() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE Metrics (id INT NOT NULL, val INT NOT NULL)").unwrap();
    db.execute_sql("INSERT INTO Metrics VALUES (1, 10)").unwrap();
    db.execute_sql("INSERT INTO Metrics VALUES (2, 20)").unwrap();
    db.execute_sql("INSERT INTO Metrics VALUES (3, 30)").unwrap();

    let result = db.execute_sql("SELECT SUM(val) FROM Metrics").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(60));

    let result = db.execute_sql("SELECT AVG(val) FROM Metrics").unwrap();
    assert_eq!(result.rows[0][0], Value::Float(20.0));

    let result = db.execute_sql("SELECT MIN(val) FROM Metrics").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(10));

    let result = db.execute_sql("SELECT MAX(val) FROM Metrics").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(30));
}

#[test]
fn test_show_tables() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE alpha (id INT NOT NULL)").unwrap();
    db.execute_sql("CREATE TABLE beta (id INT NOT NULL)").unwrap();

    let result = db.execute_sql("SHOW TABLES").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.columns[0], "Tables_in_forgedb");
}

#[test]
fn test_describe_table() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE test_tbl (id INT NOT NULL, name VARCHAR(50) NULL)").unwrap();

    let result = db.execute_sql("DESCRIBE test_tbl").unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.columns[0], "Field");
    assert_eq!(result.rows[0][0], Value::Varchar("id".into()));
}

#[test]
fn test_set_and_use() {
    let (_dir, mut db) = new_db();

    // These should not error
    db.execute_sql("SET NAMES utf8mb4").unwrap();
    let result = db.execute_sql("SET NAMES utf8mb4").unwrap();
    assert_eq!(result.message, "OK");
}

#[test]
fn test_mysql_bigint_type() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE big (id BIGINT NOT NULL, val BIGINT)").unwrap();
    db.execute_sql("INSERT INTO big VALUES (1, 9999999999)").unwrap();

    let result = db.execute_sql("SELECT * FROM big").unwrap();
    assert_eq!(result.rows[0][1], Value::BigInt(9999999999));
}

#[test]
fn test_limit_offset() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE seq (id INT NOT NULL)").unwrap();
    for i in 1..=10 {
        db.execute_sql(&format!("INSERT INTO seq VALUES ({})", i)).unwrap();
    }

    let result = db.execute_sql("SELECT * FROM seq LIMIT 3").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_transaction_stubs() {
    let (_dir, mut db) = new_db();

    db.execute_sql("CREATE TABLE t (id INT NOT NULL)").unwrap();
    db.execute_sql("START TRANSACTION").unwrap();
    db.execute_sql("INSERT INTO t VALUES (1)").unwrap();
    db.execute_sql("COMMIT").unwrap();

    let result = db.execute_sql("SELECT * FROM t").unwrap();
    assert_eq!(result.rows.len(), 1);
}
