//! Comprehensive tests for production database features:
//! GROUP BY, HAVING, DISTINCT, CASE, subqueries, UNION, CROSS JOIN,
//! FULL OUTER JOIN, string/math/date functions, EXPLAIN, ALTER TABLE, etc.

use forgedb::Database;
use tempfile::TempDir;

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::new(dir.path().to_str().unwrap()).unwrap();
    db.execute_sql("CREATE TABLE employees (id INT NOT NULL, name VARCHAR(100), dept VARCHAR(50), salary INT, age INT)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 90000, 30)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 85000, 28)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (3, 'Carol', 'Sales', 70000, 35)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (4, 'Dave', 'Sales', 75000, 32)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (5, 'Eve', 'HR', 65000, 29)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (6, 'Frank', 'Engineering', 95000, 40)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (7, 'Grace', 'HR', 60000, 26)").unwrap();
    db.execute_sql("INSERT INTO employees VALUES (8, 'Hank', 'Sales', 72000, 38)").unwrap();
    (dir, db)
}

fn setup_with_orders() -> (TempDir, Database) {
    let (dir, db) = setup();
    db.execute_sql("CREATE TABLE orders (id INT NOT NULL, emp_id INT, amount INT, product VARCHAR(50))").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (1, 1, 500, 'Widget')").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (2, 1, 300, 'Gadget')").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (3, 2, 400, 'Widget')").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (4, 3, 200, 'Doohickey')").unwrap();
    db.execute_sql("INSERT INTO orders VALUES (5, 5, 150, 'Widget')").unwrap();
    (dir, db)
}

// ============================================================
// GROUP BY tests
// ============================================================

#[test]
fn test_group_by_count() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept").unwrap();
    assert_eq!(result.rows.len(), 3); // 3 departments
    // Find Engineering group
    let eng_row = result.rows.iter().find(|r| {
        matches!(&r[0], forgedb::tuple::types::Value::Varchar(s) if s == "Engineering")
    }).unwrap();
    assert_eq!(eng_row[1], forgedb::tuple::types::Value::BigInt(3));
}

#[test]
fn test_group_by_sum() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT dept, SUM(salary) AS total_sal FROM employees GROUP BY dept").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_group_by_avg() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_group_by_min_max() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT dept, MIN(salary), MAX(salary) FROM employees GROUP BY dept").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_group_by_having() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept HAVING COUNT(*) >= 3"
    ).unwrap();
    assert_eq!(result.rows.len(), 2); // Engineering (3) and Sales (3)
}

#[test]
fn test_group_by_having_sum() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT dept, SUM(salary) AS total FROM employees GROUP BY dept HAVING SUM(salary) > 200000"
    ).unwrap();
    assert_eq!(result.rows.len(), 2); // Engineering: 270000, Sales: 217000
}

#[test]
fn test_group_by_with_order_by() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept ORDER BY cnt DESC"
    ).unwrap();
    assert_eq!(result.rows.len(), 3);
}

// ============================================================
// DISTINCT tests
// ============================================================

#[test]
fn test_distinct_basic() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT DISTINCT dept FROM employees").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_distinct_all_same() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE colors (c VARCHAR(20))").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('red')").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('red')").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('blue')").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('blue')").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('red')").unwrap();
    let result = db.execute_sql("SELECT DISTINCT c FROM colors").unwrap();
    assert_eq!(result.rows.len(), 2);
}

// ============================================================
// CASE expression tests
// ============================================================

#[test]
fn test_case_searched() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT name, CASE WHEN salary > 80000 THEN 'High' WHEN salary > 65000 THEN 'Mid' ELSE 'Low' END AS level FROM employees"
    ).unwrap();
    assert_eq!(result.rows.len(), 8);
    assert_eq!(result.columns[1], "level");
}

#[test]
fn test_case_simple() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT name, CASE dept WHEN 'Engineering' THEN 'ENG' WHEN 'Sales' THEN 'SLS' ELSE 'OTH' END AS dept_code FROM employees"
    ).unwrap();
    assert_eq!(result.rows.len(), 8);
}

// ============================================================
// String function tests
// ============================================================

#[test]
fn test_upper_lower() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT UPPER(name), LOWER(dept) FROM employees WHERE id = 1").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("ALICE".to_string()));
    assert_eq!(result.rows[0][1], forgedb::tuple::types::Value::Varchar("engineering".to_string()));
}

#[test]
fn test_length() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT LENGTH(name) FROM employees WHERE id = 1").unwrap();
    // "Alice" = 5
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Integer(5));
}

#[test]
fn test_substring() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT SUBSTRING(name, 1, 3) FROM employees WHERE id = 1").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("Ali".to_string()));
}

#[test]
fn test_replace() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT REPLACE(dept, 'Engineering', 'Eng') FROM employees WHERE id = 1").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("Eng".to_string()));
}

#[test]
fn test_trim() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE ws (s VARCHAR(50))").unwrap();
    db.execute_sql("INSERT INTO ws VALUES ('  hello  ')").unwrap();
    let result = db.execute_sql("SELECT TRIM(s) FROM ws").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("hello".to_string()));
}

#[test]
fn test_concat_function() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT CONCAT(name, ' - ', dept) FROM employees WHERE id = 1").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("Alice - Engineering".to_string()));
}

#[test]
fn test_left_right() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT LEFT(name, 3), RIGHT(name, 2) FROM employees WHERE id = 1").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("Ali".to_string()));
    assert_eq!(result.rows[0][1], forgedb::tuple::types::Value::Varchar("ce".to_string()));
}

#[test]
fn test_reverse() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT REVERSE(name) FROM employees WHERE id = 2").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("boB".to_string()));
}

#[test]
fn test_lpad_rpad() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT LPAD(name, 8, '*'), RPAD(name, 8, '*') FROM employees WHERE id = 2").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("*****Bob".to_string()));
    assert_eq!(result.rows[0][1], forgedb::tuple::types::Value::Varchar("Bob*****".to_string()));
}

// ============================================================
// Math function tests
// ============================================================

#[test]
fn test_abs() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE nums (n INT)").unwrap();
    db.execute_sql("INSERT INTO nums VALUES (-42)").unwrap();
    let result = db.execute_sql("SELECT ABS(n) FROM nums").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Integer(42));
}

#[test]
fn test_round() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE floats (f FLOAT)").unwrap();
    db.execute_sql("INSERT INTO floats VALUES (3.14159)").unwrap();
    let result = db.execute_sql("SELECT ROUND(f, 2) FROM floats").unwrap();
    match &result.rows[0][0] {
        forgedb::tuple::types::Value::Float(f) => assert!((*f - 3.14).abs() < 0.01),
        other => panic!("expected Float, got {:?}", other),
    }
}

#[test]
fn test_ceil_floor() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE floats (f FLOAT)").unwrap();
    db.execute_sql("INSERT INTO floats VALUES (3.7)").unwrap();
    let r_ceil = db.execute_sql("SELECT CEILING(f) FROM floats").unwrap();
    let r_floor = db.execute_sql("SELECT FLOOR(f) FROM floats").unwrap();
    // CEIL/FLOOR may return Integer or Float depending on implementation
    let ceil_val = match &r_ceil.rows[0][0] {
        forgedb::tuple::types::Value::Float(f) => *f,
        forgedb::tuple::types::Value::Integer(n) => *n as f64,
        other => panic!("expected numeric, got {:?}", other),
    };
    assert_eq!(ceil_val, 4.0);
    let floor_val = match &r_floor.rows[0][0] {
        forgedb::tuple::types::Value::Float(f) => *f,
        forgedb::tuple::types::Value::Integer(n) => *n as f64,
        other => panic!("expected numeric, got {:?}", other),
    };
    assert_eq!(floor_val, 3.0);
}

#[test]
fn test_power_sqrt() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE nums (n INT)").unwrap();
    db.execute_sql("INSERT INTO nums VALUES (9)").unwrap();
    let r_pow = db.execute_sql("SELECT POWER(n, 2) FROM nums").unwrap();
    let r_sqrt = db.execute_sql("SELECT SQRT(n) FROM nums").unwrap();
    match &r_pow.rows[0][0] {
        forgedb::tuple::types::Value::Float(f) => assert_eq!(*f, 81.0),
        other => panic!("expected Float(81.0), got {:?}", other),
    }
    match &r_sqrt.rows[0][0] {
        forgedb::tuple::types::Value::Float(f) => assert_eq!(*f, 3.0),
        other => panic!("expected Float(3.0), got {:?}", other),
    }
}

#[test]
fn test_mod_function() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE nums (n INT)").unwrap();
    db.execute_sql("INSERT INTO nums VALUES (10)").unwrap();
    let result = db.execute_sql("SELECT MOD(n, 3) FROM nums").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Integer(1));
}

#[test]
fn test_sign() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE nums (n INT)").unwrap();
    db.execute_sql("INSERT INTO nums VALUES (-5)").unwrap();
    db.execute_sql("INSERT INTO nums VALUES (0)").unwrap();
    db.execute_sql("INSERT INTO nums VALUES (5)").unwrap();
    let result = db.execute_sql("SELECT SIGN(n) FROM nums").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_greatest_least() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT GREATEST(salary, 80000) FROM employees WHERE id = 5").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Integer(80000));
    let result = db.execute_sql("SELECT LEAST(salary, 80000) FROM employees WHERE id = 5").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Integer(65000));
}

#[test]
fn test_nullif() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT NULLIF(dept, 'Engineering') FROM employees WHERE id = 1").unwrap();
    assert!(result.rows[0][0].is_null());
    let result = db.execute_sql("SELECT NULLIF(dept, 'Sales') FROM employees WHERE id = 1").unwrap();
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_coalesce() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE nullable_test (a INT, b INT)").unwrap();
    db.execute_sql("INSERT INTO nullable_test (b) VALUES (42)").unwrap();
    let result = db.execute_sql("SELECT COALESCE(a, b) FROM nullable_test").unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Integer(42));
}

// ============================================================
// UNION tests
// ============================================================

#[test]
fn test_union_all() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT name FROM employees WHERE dept = 'HR' UNION ALL SELECT name FROM employees WHERE dept = 'HR'"
    ).unwrap();
    assert_eq!(result.rows.len(), 4); // 2 HR employees x 2
}

#[test]
fn test_union_distinct() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT dept FROM employees UNION SELECT dept FROM employees"
    ).unwrap();
    assert_eq!(result.rows.len(), 3); // 3 distinct departments
}

// ============================================================
// CROSS JOIN tests
// ============================================================

#[test]
fn test_cross_join() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE colors (color VARCHAR(10))").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('red')").unwrap();
    db.execute_sql("INSERT INTO colors VALUES ('blue')").unwrap();
    db.execute_sql("CREATE TABLE sizes (sz VARCHAR(10))").unwrap();
    db.execute_sql("INSERT INTO sizes VALUES ('S')").unwrap();
    db.execute_sql("INSERT INTO sizes VALUES ('M')").unwrap();
    db.execute_sql("INSERT INTO sizes VALUES ('L')").unwrap();
    let result = db.execute_sql("SELECT * FROM colors CROSS JOIN sizes").unwrap();
    assert_eq!(result.rows.len(), 6); // 2 * 3
}

// ============================================================
// FULL OUTER JOIN tests
// ============================================================

#[test]
fn test_full_outer_join() {
    let (_dir, db) = setup_with_orders();
    let result = db.execute_sql(
        "SELECT e.name, o.amount FROM employees e FULL OUTER JOIN orders o ON e.id = o.emp_id"
    ).unwrap();
    // All employees + all orders that match + unmatched rows
    assert!(result.rows.len() >= 8); // At least 8 employees
}

// ============================================================
// EXPLAIN tests
// ============================================================

#[test]
fn test_explain_select() {
    let (_dir, db) = setup();
    let result = db.execute_sql("EXPLAIN SELECT * FROM employees WHERE id = 1").unwrap();
    assert!(!result.rows.is_empty());
    assert_eq!(result.columns[0], "Query Plan");
}

#[test]
fn test_explain_join() {
    let (_dir, db) = setup_with_orders();
    let result = db.execute_sql(
        "EXPLAIN SELECT e.name, o.amount FROM employees e INNER JOIN orders o ON e.id = o.emp_id"
    ).unwrap();
    assert!(!result.rows.is_empty());
}

// ============================================================
// ALTER TABLE tests
// ============================================================

#[test]
fn test_alter_table_add_column() {
    let (_dir, db) = setup();
    db.execute_sql("ALTER TABLE employees ADD COLUMN email VARCHAR(100)").unwrap();
    let result = db.execute_sql("DESCRIBE employees").unwrap();
    let has_email = result.rows.iter().any(|r| {
        matches!(&r[0], forgedb::tuple::types::Value::Varchar(s) if s == "email")
    });
    assert!(has_email);
}

#[test]
fn test_alter_table_drop_column() {
    let (_dir, db) = setup();
    db.execute_sql("ALTER TABLE employees DROP COLUMN age").unwrap();
    let result = db.execute_sql("DESCRIBE employees").unwrap();
    let has_age = result.rows.iter().any(|r| {
        matches!(&r[0], forgedb::tuple::types::Value::Varchar(s) if s == "age")
    });
    assert!(!has_age);
}

// ============================================================
// Complex query tests
// ============================================================

#[test]
fn test_group_by_with_join() {
    let (_dir, db) = setup_with_orders();
    let result = db.execute_sql(
        "SELECT e.dept, COUNT(*) AS order_count FROM employees e INNER JOIN orders o ON e.id = o.emp_id GROUP BY e.dept"
    ).unwrap();
    assert!(result.rows.len() >= 2);
}

#[test]
fn test_group_by_with_case() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT CASE WHEN salary > 80000 THEN 'High' ELSE 'Low' END AS tier, COUNT(*) FROM employees GROUP BY CASE WHEN salary > 80000 THEN 'High' ELSE 'Low' END"
    ).unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_distinct_with_order_by() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT DISTINCT dept FROM employees ORDER BY dept"
    ).unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_nested_functions() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT UPPER(SUBSTRING(name, 1, 3)) FROM employees WHERE id = 1"
    ).unwrap();
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::Varchar("ALI".to_string()));
}

#[test]
fn test_group_by_multiple_columns() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT dept, CASE WHEN salary > 80000 THEN 'Senior' ELSE 'Junior' END, COUNT(*) FROM employees GROUP BY dept, CASE WHEN salary > 80000 THEN 'Senior' ELSE 'Junior' END"
    ).unwrap();
    assert!(result.rows.len() >= 3);
}

#[test]
fn test_modulo_operator() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT id, id % 2 AS is_odd FROM employees WHERE id <= 3").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_if_function() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT name, IF(salary > 80000, 'High', 'Normal') AS level FROM employees WHERE id = 1"
    ).unwrap();
    assert_eq!(result.rows[0][1], forgedb::tuple::types::Value::Varchar("High".to_string()));
}

#[test]
fn test_multiple_aggregates_no_group() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT COUNT(*), MIN(salary), MAX(salary), AVG(salary) FROM employees"
    ).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::BigInt(8));
}

#[test]
fn test_implicit_cross_join() {
    let (_dir, db) = setup();
    db.execute_sql("CREATE TABLE t1 (a INT)").unwrap();
    db.execute_sql("CREATE TABLE t2 (b INT)").unwrap();
    db.execute_sql("INSERT INTO t1 VALUES (1)").unwrap();
    db.execute_sql("INSERT INTO t1 VALUES (2)").unwrap();
    db.execute_sql("INSERT INTO t2 VALUES (10)").unwrap();
    db.execute_sql("INSERT INTO t2 VALUES (20)").unwrap();
    // FROM t1, t2 is implicit cross join
    let result = db.execute_sql("SELECT * FROM t1, t2").unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn test_count_distinct() {
    let (_dir, db) = setup();
    let result = db.execute_sql("SELECT COUNT(DISTINCT dept) FROM employees").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], forgedb::tuple::types::Value::BigInt(3));
}

#[test]
fn test_group_by_with_limit() {
    let (_dir, db) = setup();
    let result = db.execute_sql(
        "SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept ORDER BY cnt DESC LIMIT 2"
    ).unwrap();
    assert_eq!(result.rows.len(), 2);
}
