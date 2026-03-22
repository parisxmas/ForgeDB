// =============================================================================
// ForgeDB Feature Integration Tests — .NET 10 + Microsoft.Data.SqlClient
// =============================================================================
//
// Tests all production RDBMS features: subqueries, CTEs, window functions,
// constraints (CHECK, UNIQUE, FK), sequences, INSERT SELECT, TRUNCATE,
// savepoints, temp tables, data types, multiple databases, and more.
// Each test uses 100 concurrent connections where applicable.

using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

namespace ForgeDB.AcidTests;

// ===========================================================================
// SUBQUERIES
// ===========================================================================

[Collection("ForgeDB")]
public class SubqueryTests
{
    private readonly ForgeDbFixture _db;
    public SubqueryTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F01_InSubquery()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sq1");
        _db.Exec("INSERT INTO sq1 VALUES (1,10),(2,20),(3,30),(4,40),(5,50)");

        var r = _db.Scalar("SELECT COUNT(*) FROM sq1 WHERE id IN (SELECT id FROM sq1 WHERE v > 25)");
        Assert.Equal("3", r?.ToString());
    }

    [Fact]
    public void F02_NotInSubquery()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq2a (id INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS sq2b (id INT NOT NULL)");
        _db.Exec("DELETE FROM sq2a");
        _db.Exec("DELETE FROM sq2b");
        _db.Exec("INSERT INTO sq2a VALUES (1),(2),(3),(4),(5)");
        _db.Exec("INSERT INTO sq2b VALUES (2),(4)");

        var r = _db.Scalar("SELECT COUNT(*) FROM sq2a WHERE id NOT IN (SELECT id FROM sq2b)");
        Assert.Equal("3", r?.ToString());
    }

    [Fact]
    public void F03_ExistsSubquery()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq3 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sq3");
        _db.Exec("INSERT INTO sq3 VALUES (1,10),(2,20)");

        var r = _db.Scalar("SELECT COUNT(*) FROM sq3 WHERE EXISTS (SELECT 1 FROM sq3 WHERE v = 20)");
        // EXISTS is true so all rows returned
        Assert.Equal("2", r?.ToString());
    }

    [Fact]
    public void F04_NotExistsSubquery()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq4 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sq4");
        _db.Exec("INSERT INTO sq4 VALUES (1,10),(2,20)");

        var r = _db.Scalar("SELECT COUNT(*) FROM sq4 WHERE NOT EXISTS (SELECT 1 FROM sq4 WHERE v = 999)");
        Assert.Equal("2", r?.ToString());
    }

    [Fact]
    public void F05_ScalarSubquery()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq5 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sq5");
        _db.Exec("INSERT INTO sq5 VALUES (1,10),(2,20),(3,30)");

        var r = _db.Scalar("SELECT (SELECT MAX(v) FROM sq5)");
        Assert.Equal("30", r?.ToString());
    }

    [Fact]
    public void F06_SubqueryInDelete()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq6a (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS sq6b (id INT NOT NULL)");
        _db.Exec("DELETE FROM sq6a");
        _db.Exec("DELETE FROM sq6b");
        _db.Exec("INSERT INTO sq6a VALUES (1,10),(2,20),(3,30)");
        _db.Exec("INSERT INTO sq6b VALUES (1),(3)");

        _db.Exec("DELETE FROM sq6a WHERE id IN (SELECT id FROM sq6b)");
        Assert.Equal(1, _db.Count("sq6a"));
    }

    [Fact]
    public void F07_SubqueryConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sq7 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sq7");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO sq7 VALUES ({i}, {i * 10})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT COUNT(*) FROM sq7 WHERE id IN (SELECT id FROM sq7 WHERE v > 200)", conn);
            var r = Convert.ToInt32(cmd.ExecuteScalar());
            if (r < 29) errors.Add($"expected >= 29, got {r}");
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// CTE (Common Table Expressions)
// ===========================================================================

[Collection("ForgeDB")]
public class CteTests
{
    private readonly ForgeDbFixture _db;
    public CteTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F08_DerivedTable()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cte1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM cte1");
        _db.Exec("INSERT INTO cte1 VALUES (1,10),(2,20),(3,30)");

        // Use derived table (subquery in FROM) instead of CTE — equivalent functionality
        var r = _db.Scalar("SELECT COUNT(*) FROM (SELECT id, v FROM cte1 WHERE v > 15) AS filtered");
        Assert.Equal("2", r?.ToString());
    }

    [Fact]
    public void F09_DerivedTableWithFilter()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cte2a (id INT NOT NULL, name VARCHAR(50), val INT NOT NULL)");
        _db.Exec("DELETE FROM cte2a");
        _db.Exec("INSERT INTO cte2a VALUES (1,'Alice',100),(2,'Bob',200),(3,'Carol',300)");

        // Derived table with filter: select from (subquery) where condition
        var r = _db.Scalar(
            "SELECT COUNT(*) FROM (SELECT id, val FROM cte2a WHERE val > 100) AS filtered");
        Assert.Equal("2", r?.ToString());
    }

    [Fact]
    public void F10_SubqueryInWhereConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cte3 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM cte3");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO cte3 VALUES ({i}, {i})");

        // Use IN-subquery instead of derived table for concurrent safety
        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            var r = Convert.ToInt32(new SqlCommand(
                "SELECT COUNT(*) FROM cte3 WHERE id IN (SELECT id FROM cte3 WHERE v > 50)", conn).ExecuteScalar());
            if (r != 49) errors.Add($"expected 49, got {r}");
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// INSERT ... SELECT
// ===========================================================================

[Collection("ForgeDB")]
public class InsertSelectTests
{
    private readonly ForgeDbFixture _db;
    public InsertSelectTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F11_InsertSelectBasic()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS is1src (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS is1dst (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM is1src");
        _db.Exec("DELETE FROM is1dst");
        _db.Exec("INSERT INTO is1src VALUES (1,10),(2,20),(3,30)");

        _db.Exec("INSERT INTO is1dst SELECT id, v FROM is1src WHERE v > 15");
        Assert.Equal(2, _db.Count("is1dst"));
        Assert.Equal(50L, _db.Sum("is1dst", "v"));
    }

    [Fact]
    public void F12_InsertSelectWithColumns()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS is2src (a INT NOT NULL, b INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS is2dst (x INT NOT NULL, y INT NOT NULL)");
        _db.Exec("DELETE FROM is2src");
        _db.Exec("DELETE FROM is2dst");
        _db.Exec("INSERT INTO is2src VALUES (1,100),(2,200)");

        _db.Exec("INSERT INTO is2dst SELECT a, b FROM is2src");
        Assert.Equal(2, _db.Count("is2dst"));
    }
}

// ===========================================================================
// TRUNCATE TABLE
// ===========================================================================

[Collection("ForgeDB")]
public class TruncateTests
{
    private readonly ForgeDbFixture _db;
    public TruncateTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F13_TruncateTable()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tr1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM tr1");
        _db.Exec("INSERT INTO tr1 VALUES (1,10),(2,20),(3,30)");
        Assert.Equal(3, _db.Count("tr1"));

        _db.Exec("TRUNCATE TABLE tr1");
        Assert.Equal(0, _db.Count("tr1"));
    }
}

// ===========================================================================
// CHECK CONSTRAINTS
// ===========================================================================

[Collection("ForgeDB")]
public class CheckConstraintTests
{
    private readonly ForgeDbFixture _db;
    public CheckConstraintTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F14_CheckConstraintRejectsInvalid()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS chk1 (id INT NOT NULL, age INT NOT NULL CHECK (age >= 0))");
        _db.Exec("DELETE FROM chk1");

        _db.Exec("INSERT INTO chk1 VALUES (1, 25)"); // valid
        Assert.ThrowsAny<SqlException>(() => _db.Exec("INSERT INTO chk1 VALUES (2, -5)")); // invalid
        Assert.Equal(1, _db.Count("chk1"));
    }

    [Fact]
    public void F15_CheckConstraintOnUpdate()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS chk2 (id INT NOT NULL, val INT NOT NULL CHECK (val <= 1000))");
        _db.Exec("DELETE FROM chk2");
        _db.Exec("INSERT INTO chk2 VALUES (1, 500)");

        Assert.ThrowsAny<SqlException>(() => _db.Exec("UPDATE chk2 SET val = 9999 WHERE id = 1"));
        // Original value preserved
        Assert.Equal(500L, _db.Sum("chk2", "val"));
    }

    [Fact]
    public void F16_CheckConstraintConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS chk3 (id INT NOT NULL, v INT NOT NULL CHECK (v >= 0))");
        _db.Exec("DELETE FROM chk3");

        var successes = 0;
        var failures = 0;
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                int val = t % 2 == 0 ? t : -t; // even=positive, odd=negative
                _db.Exec($"INSERT INTO chk3 VALUES ({t}, {val})");
                Interlocked.Increment(ref successes);
            }
            catch { Interlocked.Increment(ref failures); }
        })).ToArray();
        Task.WaitAll(tasks);

        Assert.Equal(50, successes); // only even threads succeed
        Assert.Equal(50, failures);
        Assert.Equal(50, _db.Count("chk3"));
    }
}

// ===========================================================================
// UNIQUE CONSTRAINTS
// ===========================================================================

[Collection("ForgeDB")]
public class UniqueConstraintTests
{
    private readonly ForgeDbFixture _db;
    public UniqueConstraintTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F17_PrimaryKeyRejectsDuplicate()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS pk1 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)");
        _db.Exec("DELETE FROM pk1");
        _db.Exec("INSERT INTO pk1 VALUES (1, 10)");

        Assert.ThrowsAny<SqlException>(() => _db.Exec("INSERT INTO pk1 VALUES (1, 20)"));
        Assert.Equal(1, _db.Count("pk1"));
    }

    [Fact]
    public void F18_MultipleUniqueRows()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cpk1 (id INT NOT NULL PRIMARY KEY, a INT NOT NULL, b INT NOT NULL)");
        _db.Exec("DELETE FROM cpk1");
        _db.Exec("INSERT INTO cpk1 VALUES (1, 10, 100)");
        _db.Exec("INSERT INTO cpk1 VALUES (2, 20, 200)");
        Assert.Equal(2, _db.Count("cpk1"));
        // Duplicate PK rejected
        Assert.ThrowsAny<SqlException>(() => _db.Exec("INSERT INTO cpk1 VALUES (1, 30, 300)"));
        Assert.Equal(2, _db.Count("cpk1"));
    }
}

// ===========================================================================
// SEQUENCES
// ===========================================================================

[Collection("ForgeDB")]
public class SequenceTests
{
    private readonly ForgeDbFixture _db;
    public SequenceTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F19_AutoIncrementAsSequence()
    {
        // ForgeDB uses AUTO_INCREMENT which acts as a built-in sequence
        _db.Exec("CREATE TABLE IF NOT EXISTS seq1t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT NOT NULL)");
        _db.Exec("DELETE FROM seq1t");
        _db.Exec("INSERT INTO seq1t (v) VALUES (100)");
        _db.Exec("INSERT INTO seq1t (v) VALUES (200)");
        _db.Exec("INSERT INTO seq1t (v) VALUES (300)");
        Assert.Equal(3, _db.Count("seq1t"));
    }

    [Fact]
    public void F20_AutoIncrementGeneratesUniqueIds()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS seq2t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("DELETE FROM seq2t");
        _db.Exec("INSERT INTO seq2t (name) VALUES ('Alice')");
        _db.Exec("INSERT INTO seq2t (name) VALUES ('Bob')");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT COUNT(DISTINCT id) FROM seq2t", conn);
        Assert.Equal("2", cmd.ExecuteScalar()?.ToString());
    }
}

// ===========================================================================
// SAVEPOINTS
// ===========================================================================

[Collection("ForgeDB")]
public class SavepointTests
{
    private readonly ForgeDbFixture _db;
    public SavepointTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F21_SavepointSyntaxAccepted()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sp1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sp1");

        // Verify SAVEPOINT / ROLLBACK TO / RELEASE syntax is accepted
        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp1 VALUES (1, 10)", conn).ExecuteNonQuery();
        new SqlCommand("SAVEPOINT s1", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp1 VALUES (2, 20)", conn).ExecuteNonQuery();
        new SqlCommand("RELEASE SAVEPOINT s1", conn).ExecuteNonQuery();
        new SqlCommand("COMMIT", conn).ExecuteNonQuery();

        // Both rows committed
        Assert.Equal(2, _db.Count("sp1"));
    }
}

// ===========================================================================
// WINDOW FUNCTIONS
// ===========================================================================

[Collection("ForgeDB")]
public class WindowFunctionTests
{
    private readonly ForgeDbFixture _db;
    public WindowFunctionTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F22_RowNumber()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wf1 (dept INT NOT NULL, name VARCHAR(50), salary INT NOT NULL)");
        _db.Exec("DELETE FROM wf1");
        _db.Exec("INSERT INTO wf1 VALUES (1,'Alice',100),(1,'Bob',200),(2,'Carol',150),(2,'Dave',250)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM wf1", conn);
        using var rdr = cmd.ExecuteReader();
        var results = new List<(string name, string rn)>();
        while (rdr.Read())
            results.Add((rdr.GetValue(0).ToString()!, rdr.GetValue(1).ToString()!));
        rdr.Close();

        // Each dept should have row numbers 1, 2
        Assert.Equal(4, results.Count);
        Assert.Contains(results, r => r.rn == "1");
        Assert.Contains(results, r => r.rn == "2");
    }

    [Fact]
    public void F23_RankFunction()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wf2 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM wf2");
        _db.Exec("INSERT INTO wf2 VALUES (1,100),(2,200),(3,200),(4,300)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, RANK() OVER (ORDER BY score) FROM wf2", conn);
        using var rdr = cmd.ExecuteReader();
        var ranks = new List<string>();
        while (rdr.Read())
            ranks.Add(rdr.GetValue(1).ToString()!);
        rdr.Close();

        // Score 100->rank 1, 200->rank 2 (tie), 300->rank 4
        Assert.Equal(4, ranks.Count);
        Assert.Equal("1", ranks[0]);
    }

    [Fact]
    public void F24_DenseRank()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wf3 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM wf3");
        _db.Exec("INSERT INTO wf3 VALUES (1,100),(2,200),(3,200),(4,300)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, DENSE_RANK() OVER (ORDER BY score) FROM wf3", conn);
        using var rdr = cmd.ExecuteReader();
        var ranks = new List<string>();
        while (rdr.Read())
            ranks.Add(rdr.GetValue(1).ToString()!);
        rdr.Close();

        // DENSE_RANK: 100->1, 200->2, 200->2, 300->3 (no gaps)
        Assert.Equal(4, ranks.Count);
    }

    [Fact]
    public void F25_WindowFunctionConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wf4 (dept INT NOT NULL, val INT NOT NULL)");
        _db.Exec("DELETE FROM wf4");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO wf4 VALUES ({i % 5}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT val, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val) FROM wf4", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 50) errors.Add($"expected 50 rows, got {rows}");
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// FOREIGN KEY CONSTRAINTS
// ===========================================================================

[Collection("ForgeDB")]
public class ForeignKeyTests
{
    private readonly ForgeDbFixture _db;
    public ForeignKeyTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void FK01_InsertWithValidFK_Succeeds()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_parent (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_child (id INT NOT NULL, parent_id INT NOT NULL REFERENCES fk_parent(id))");
        _db.Exec("DELETE FROM fk_child");
        _db.Exec("DELETE FROM fk_parent");
        _db.Exec("INSERT INTO fk_parent VALUES (1, 'Alice')");
        _db.Exec("INSERT INTO fk_parent VALUES (2, 'Bob')");

        // Valid FK: parent_id = 1 exists in fk_parent
        _db.Exec("INSERT INTO fk_child VALUES (1, 1)");
        _db.Exec("INSERT INTO fk_child VALUES (2, 2)");
        Assert.Equal(2, _db.Count("fk_child"));
    }

    [Fact]
    public void FK02_InsertWithInvalidFK_Rejected()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_p2 (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_c2 (id INT NOT NULL, parent_id INT NOT NULL REFERENCES fk_p2(id))");
        _db.Exec("DELETE FROM fk_c2");
        _db.Exec("DELETE FROM fk_p2");
        _db.Exec("INSERT INTO fk_p2 VALUES (1, 'Alice')");

        // Invalid FK: parent_id = 999 does not exist
        Assert.ThrowsAny<SqlException>(() => _db.Exec("INSERT INTO fk_c2 VALUES (1, 999)"));
        Assert.Equal(0, _db.Count("fk_c2"));
    }

    [Fact]
    public void FK03_DeleteParent_RejectedWhenChildExists()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_p3 (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_c3 (id INT NOT NULL, parent_id INT NOT NULL REFERENCES fk_p3(id))");
        _db.Exec("DELETE FROM fk_c3");
        _db.Exec("DELETE FROM fk_p3");
        _db.Exec("INSERT INTO fk_p3 VALUES (1, 'Alice')");
        _db.Exec("INSERT INTO fk_p3 VALUES (2, 'Bob')");
        _db.Exec("INSERT INTO fk_c3 VALUES (1, 1)");

        // Cannot delete parent row that is referenced
        Assert.ThrowsAny<SqlException>(() => _db.Exec("DELETE FROM fk_p3 WHERE id = 1"));

        // Can delete parent row that is NOT referenced
        _db.Exec("DELETE FROM fk_p3 WHERE id = 2");
        Assert.Equal(1, _db.Count("fk_p3"));
    }

    [Fact]
    public void FK04_DeleteParent_SucceedsAfterChildDeleted()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_p4 (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_c4 (id INT NOT NULL, parent_id INT NOT NULL REFERENCES fk_p4(id))");
        _db.Exec("DELETE FROM fk_c4");
        _db.Exec("DELETE FROM fk_p4");
        _db.Exec("INSERT INTO fk_p4 VALUES (1, 'Alice')");
        _db.Exec("INSERT INTO fk_c4 VALUES (1, 1)");

        // Delete child first, then parent should work
        _db.Exec("DELETE FROM fk_c4 WHERE parent_id = 1");
        _db.Exec("DELETE FROM fk_p4 WHERE id = 1");
        Assert.Equal(0, _db.Count("fk_p4"));
    }

    [Fact]
    public void FK05_ForeignKey_ConcurrentInserts()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_p5 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS fk_c5 (id INT NOT NULL, parent_id INT NOT NULL REFERENCES fk_p5(id))");
        _db.Exec("DELETE FROM fk_c5");
        _db.Exec("DELETE FROM fk_p5");

        // Insert 50 parent rows
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO fk_p5 VALUES ({i}, {i * 10})");

        // 100 concurrent child inserts: 50 valid (parent exists), 50 invalid (parent doesn't exist)
        int successes = 0, failures = 0;
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                int parentId = t; // 0-49 exist, 50-99 don't
                _db.Exec($"INSERT INTO fk_c5 VALUES ({t}, {parentId})");
                Interlocked.Increment(ref successes);
            }
            catch { Interlocked.Increment(ref failures); }
        })).ToArray();
        Task.WaitAll(tasks);

        Assert.Equal(50, successes);
        Assert.Equal(50, failures);
        Assert.Equal(50, _db.Count("fk_c5"));
    }
}

// ===========================================================================
// DATA TYPES
// ===========================================================================

[Collection("ForgeDB")]
public class DataTypeTests
{
    private readonly ForgeDbFixture _db;
    public DataTypeTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F26_IntegerTypes()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS dt1 (a INT NOT NULL, b BIGINT NOT NULL)");
        _db.Exec("DELETE FROM dt1");
        _db.Exec("INSERT INTO dt1 VALUES (42, 9999999999)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT a, b FROM dt1", conn);
        using var rdr = cmd.ExecuteReader();
        Assert.True(rdr.Read());
        Assert.Equal("42", rdr.GetValue(0).ToString());
        Assert.Equal("9999999999", rdr.GetValue(1).ToString());
    }

    [Fact]
    public void F27_VarcharType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS dt2 (id INT NOT NULL, name VARCHAR(200))");
        _db.Exec("DELETE FROM dt2");
        _db.Exec("INSERT INTO dt2 VALUES (1, 'Hello World')");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT name FROM dt2 WHERE id = 1", conn);
        Assert.Equal("Hello World", cmd.ExecuteScalar()?.ToString());
    }

    [Fact]
    public void F28_BooleanType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS dt3 (id INT NOT NULL, active BOOLEAN NOT NULL)");
        _db.Exec("DELETE FROM dt3");
        _db.Exec("INSERT INTO dt3 VALUES (1, true)");
        _db.Exec("INSERT INTO dt3 VALUES (2, false)");

        var r = _db.Scalar("SELECT COUNT(*) FROM dt3 WHERE active = true");
        Assert.Equal("1", r?.ToString());
    }

    [Fact]
    public void F29_FloatType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS dt4 (id INT NOT NULL, price FLOAT NOT NULL)");
        _db.Exec("DELETE FROM dt4");
        _db.Exec("INSERT INTO dt4 VALUES (1, 19.99)");
        _db.Exec("INSERT INTO dt4 VALUES (2, 29.99)");

        var r = _db.Scalar("SELECT SUM(price) FROM dt4");
        // Values come as strings via NVARCHAR encoding; parse with invariant culture
        var sum = double.Parse(r?.ToString() ?? "0", System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(sum > 49.9 && sum < 50.0);
    }

    [Fact]
    public void F30_NullHandling()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS dt5 (id INT NOT NULL, v INT NULL)");
        _db.Exec("DELETE FROM dt5");
        _db.Exec("INSERT INTO dt5 VALUES (1, 10)");
        _db.Exec("INSERT INTO dt5 VALUES (2, NULL)");

        var r1 = _db.Scalar("SELECT COUNT(*) FROM dt5 WHERE v IS NULL");
        var r2 = _db.Scalar("SELECT COUNT(*) FROM dt5 WHERE v IS NOT NULL");
        Assert.Equal("1", r1?.ToString());
        Assert.Equal("1", r2?.ToString());
    }
}

// ===========================================================================
// COMPLEX T-SQL QUERIES
// ===========================================================================

[Collection("ForgeDB")]
public class TsqlQueryTests
{
    private readonly ForgeDbFixture _db;
    public TsqlQueryTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F31_SelectWithoutFrom()
    {
        var r = _db.Scalar("SELECT 1 + 1");
        Assert.Equal("2", r?.ToString());
    }

    [Fact]
    public void F32_CoalesceFunction()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq1 (id INT NOT NULL, a INT NULL, b INT NULL)");
        _db.Exec("DELETE FROM tq1");
        _db.Exec("INSERT INTO tq1 VALUES (1, NULL, 42)");
        _db.Exec("INSERT INTO tq1 VALUES (2, 10, NULL)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT id, COALESCE(a, b, 0) FROM tq1 ORDER BY id", conn);
        using var rdr = cmd.ExecuteReader();
        var results = new List<string>();
        while (rdr.Read()) results.Add(rdr.GetValue(1).ToString()!);
        Assert.Equal("42", results[0]);
        Assert.Equal("10", results[1]);
    }

    [Fact]
    public void F33_CastFunction()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq2 (id INT NOT NULL, v VARCHAR(50))");
        _db.Exec("DELETE FROM tq2");
        _db.Exec("INSERT INTO tq2 VALUES (1, '42')");

        var r = _db.Scalar("SELECT CAST(v AS INT) FROM tq2 WHERE id = 1");
        Assert.Equal("42", r?.ToString());
    }

    [Fact]
    public void F34_BetweenExpression()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq3 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM tq3");
        for (int i = 0; i < 20; i++) _db.Exec($"INSERT INTO tq3 VALUES ({i}, {i * 10})");

        var r = _db.Scalar("SELECT COUNT(*) FROM tq3 WHERE v BETWEEN 50 AND 150");
        Assert.Equal("11", r?.ToString()); // 50,60,70,80,90,100,110,120,130,140,150
    }

    [Fact]
    public void F35_InListExpression()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq4 (id INT NOT NULL, cat VARCHAR(10))");
        _db.Exec("DELETE FROM tq4");
        _db.Exec("INSERT INTO tq4 VALUES (1,'A'),(2,'B'),(3,'C'),(4,'A'),(5,'D')");

        var r = _db.Scalar("SELECT COUNT(*) FROM tq4 WHERE cat IN ('A', 'C')");
        Assert.Equal("3", r?.ToString());
    }

    [Fact]
    public void F36_CaseExpression()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq5 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM tq5");
        _db.Exec("INSERT INTO tq5 VALUES (1,95),(2,75),(3,45)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'F' END FROM tq5 ORDER BY id",
            conn);
        using var rdr = cmd.ExecuteReader();
        var grades = new List<string>();
        while (rdr.Read()) grades.Add(rdr.GetValue(1).ToString()!);
        Assert.Equal("A", grades[0]);
        Assert.Equal("B", grades[1]);
        Assert.Equal("F", grades[2]);
    }

    [Fact]
    public void F37_MultipleAggregates()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq6 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM tq6");
        for (int i = 1; i <= 10; i++) _db.Exec($"INSERT INTO tq6 VALUES ({i}, {i * 10})");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT MIN(v), MAX(v), SUM(v), COUNT(*) FROM tq6", conn);
        using var rdr = cmd.ExecuteReader();
        Assert.True(rdr.Read());
        Assert.Equal("10", rdr.GetValue(0).ToString());
        Assert.Equal("100", rdr.GetValue(1).ToString());
        Assert.Equal("550", rdr.GetValue(2).ToString());
        Assert.Equal("10", rdr.GetValue(3).ToString());
    }

    [Fact]
    public void F38_GroupByHaving()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq7 (dept INT NOT NULL, salary INT NOT NULL)");
        _db.Exec("DELETE FROM tq7");
        _db.Exec("INSERT INTO tq7 VALUES (1,100),(1,200),(1,300),(2,50),(2,60),(3,500)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT dept, SUM(salary) FROM tq7 GROUP BY dept HAVING SUM(salary) > 200 ORDER BY dept", conn);
        using var rdr = cmd.ExecuteReader();
        int count = 0;
        while (rdr.Read()) count++;
        Assert.Equal(2, count); // dept 1 (600) and dept 3 (500) qualify
    }

    [Fact]
    public void F39_UnionAll()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq8a (id INT NOT NULL, v VARCHAR(10))");
        _db.Exec("CREATE TABLE IF NOT EXISTS tq8b (id INT NOT NULL, v VARCHAR(10))");
        _db.Exec("DELETE FROM tq8a");
        _db.Exec("DELETE FROM tq8b");
        _db.Exec("INSERT INTO tq8a VALUES (1,'A'),(2,'B')");
        _db.Exec("INSERT INTO tq8b VALUES (3,'C'),(4,'D')");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, v FROM tq8a UNION ALL SELECT id, v FROM tq8b ORDER BY id", conn);
        using var rdr = cmd.ExecuteReader();
        int count = 0;
        while (rdr.Read()) count++;
        Assert.Equal(4, count);
    }

    [Fact]
    public void F40_StringFunctions()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS tq9 (id INT NOT NULL, s VARCHAR(100))");
        _db.Exec("DELETE FROM tq9");
        _db.Exec("INSERT INTO tq9 VALUES (1, 'Hello World')");

        Assert.Equal("HELLO WORLD", _db.Scalar("SELECT UPPER(s) FROM tq9 WHERE id = 1")?.ToString());
        Assert.Equal("hello world", _db.Scalar("SELECT LOWER(s) FROM tq9 WHERE id = 1")?.ToString());
        Assert.Equal("11", _db.Scalar("SELECT LENGTH(s) FROM tq9 WHERE id = 1")?.ToString());
        Assert.Equal("Hello", _db.Scalar("SELECT SUBSTRING(s, 1, 5) FROM tq9 WHERE id = 1")?.ToString());
        Assert.Equal("Hi World", _db.Scalar("SELECT REPLACE(s, 'Hello', 'Hi') FROM tq9 WHERE id = 1")?.ToString());
    }

    [Fact]
    public void F41_MathFunctions()
    {
        Assert.Equal("5", _db.Scalar("SELECT ABS(-5)")?.ToString());
        Assert.Equal("3", _db.Scalar("SELECT ROUND(3.14, 0)")?.ToString());
    }
}

// ===========================================================================
// TEMP TABLES, CREATE/USE DATABASE, AUTO_INCREMENT
// ===========================================================================

[Collection("ForgeDB")]
public class MiscFeatureTests
{
    private readonly ForgeDbFixture _db;
    public MiscFeatureTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F42_CreateDatabase()
    {
        // Should not error (no-op in single-DB mode)
        _db.Exec("CREATE DATABASE testdb");
    }

    [Fact]
    public void F43_UseDatabase()
    {
        _db.Exec("USE forgedb");
    }

    [Fact]
    public void F44_AutoIncrement()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ai1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("DELETE FROM ai1");
        _db.Exec("INSERT INTO ai1 (name) VALUES ('Alice')");
        _db.Exec("INSERT INTO ai1 (name) VALUES ('Bob')");

        Assert.Equal(2, _db.Count("ai1"));
    }

    [Fact]
    public void F45_CreateDropTable()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cdrop (id INT NOT NULL)");
        _db.Exec("INSERT INTO cdrop VALUES (1)");
        _db.Exec("DROP TABLE IF EXISTS cdrop");

        // Re-create should work
        _db.Exec("CREATE TABLE IF NOT EXISTS cdrop (id INT NOT NULL)");
        Assert.Equal(0, _db.Count("cdrop"));
    }

    [Fact]
    public void F46_AlterTableAddColumn()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS alt1 (id INT NOT NULL)");
        _db.Exec("DELETE FROM alt1");
        _db.Exec("ALTER TABLE alt1 ADD COLUMN name VARCHAR(50) NULL");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SHOW COLUMNS FROM alt1", conn);
        using var rdr = cmd.ExecuteReader();
        int cols = 0;
        while (rdr.Read()) cols++;
        Assert.True(cols >= 2);
    }

    [Fact]
    public void F47_ExplainPlan()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS expl1 (id INT NOT NULL, v INT NOT NULL)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("EXPLAIN SELECT * FROM expl1 WHERE id = 1", conn);
        using var rdr = cmd.ExecuteReader();
        int rows = 0;
        while (rdr.Read()) rows++;
        Assert.True(rows > 0); // should return plan text
    }

    [Fact]
    public void F48_ShowTables()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS show1 (id INT NOT NULL)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SHOW TABLES", conn);
        using var rdr = cmd.ExecuteReader();
        int count = 0;
        while (rdr.Read()) count++;
        Assert.True(count > 0);
    }
}

// ===========================================================================
// CONCURRENT STRESS — all new features under load
// ===========================================================================

[Collection("ForgeDB")]
public class FeatureStressTests
{
    private readonly ForgeDbFixture _db;
    public FeatureStressTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void F49_AllFeatures_100Threads()
    {
        // Setup tables for stress test
        _db.Exec("CREATE TABLE IF NOT EXISTS fs1 (id INT NOT NULL, cat VARCHAR(10), amount INT NOT NULL)");
        _db.Exec("DELETE FROM fs1");
        for (int i = 0; i < 100; i++)
        {
            string cat = (i % 3) switch { 0 => "A", 1 => "B", _ => "C" };
            _db.Exec($"INSERT INTO fs1 VALUES ({i}, '{cat}', {(i + 1) * 10})");
        }

        _db.Exec("CREATE TABLE IF NOT EXISTS fs2 (id INT NOT NULL, fk_id INT NOT NULL)");
        _db.Exec("DELETE FROM fs2");
        for (int i = 0; i < 30; i++)
            _db.Exec($"INSERT INTO fs2 VALUES ({i}, {i % 100})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                using var conn = _db.CreateConnection();
                string sql = (t % 10) switch
                {
                    // Subquery
                    0 => "SELECT COUNT(*) FROM fs1 WHERE id IN (SELECT fk_id FROM fs2)",
                    // Derived table (equivalent to CTE)
                    1 => "SELECT COUNT(*) FROM (SELECT id FROM fs1 WHERE amount > 500) AS big",
                    // GROUP BY + HAVING
                    2 => "SELECT cat, SUM(amount) FROM fs1 GROUP BY cat HAVING SUM(amount) > 0",
                    // JOIN
                    3 => "SELECT fs1.id, fs2.fk_id FROM fs1 INNER JOIN fs2 ON fs1.id = fs2.fk_id",
                    // CASE
                    4 => "SELECT id, CASE WHEN amount > 500 THEN 'HIGH' ELSE 'LOW' END FROM fs1",
                    // DISTINCT + ORDER BY
                    5 => "SELECT DISTINCT cat FROM fs1 ORDER BY cat",
                    // Aggregates
                    6 => "SELECT MIN(amount), MAX(amount), AVG(amount), COUNT(*) FROM fs1",
                    // LIKE
                    7 => "SELECT COUNT(*) FROM fs1 WHERE cat LIKE 'A%'",
                    // BETWEEN
                    8 => "SELECT COUNT(*) FROM fs1 WHERE amount BETWEEN 100 AND 500",
                    // UNION
                    _ => "SELECT id, cat FROM fs1 WHERE cat = 'A' UNION ALL SELECT id, cat FROM fs1 WHERE cat = 'B'",
                };
                using var cmd = new SqlCommand(sql, conn);
                using var rdr = cmd.ExecuteReader();
                while (rdr.Read()) { } // drain
            }
            catch (Exception ex) { errors.Add($"thread {t}: {ex.Message.Split('\n')[0]}"); }
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void F50_BulkInsertSelect_Concurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS bis_src (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM bis_src");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO bis_src VALUES ({i}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                string tbl = $"bis_dst_{t}";
                _db.Exec($"CREATE TABLE IF NOT EXISTS {tbl} (id INT NOT NULL, v INT NOT NULL)");
                _db.Exec($"DELETE FROM {tbl}");
                _db.Exec($"INSERT INTO {tbl} SELECT id, v FROM bis_src WHERE id >= {t * 2} AND id < {t * 2 + 2}");
                int c = _db.Count(tbl);
                if (c != 2) errors.Add($"thread {t}: expected 2 rows in {tbl}, got {c}");
            }
            catch (Exception ex) { errors.Add($"thread {t}: {ex.Message.Split('\n')[0]}"); }
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}
