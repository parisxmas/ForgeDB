// =============================================================================
// SQL Feature + Data Type Tests — .NET 10 + Microsoft.Data.SqlClient
// =============================================================================
// OFFSET, UPSERT, window functions, DECIMAL, DATE/TIME, JSON, UUID, etc.

using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

namespace ForgeDB.AcidTests;

// ===========================================================================
// OFFSET PAGINATION
// ===========================================================================

[Collection("ForgeDB")]
public class OffsetTests
{
    private readonly ForgeDbFixture _db;
    public OffsetTests(ForgeDbFixture db) => _db = db;

    [Fact(Skip = "OFFSET via TDS needs FETCH NEXT syntax")]
    public void OFF01_LimitOffset()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS off1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM off1");
        for (int i = 0; i < 20; i++) _db.Exec($"INSERT INTO off1 VALUES ({i}, {i * 10})");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT id FROM off1 ORDER BY id LIMIT 5 OFFSET 10", conn);
        using var rdr = cmd.ExecuteReader();
        var ids = new List<int>();
        while (rdr.Read()) ids.Add(Convert.ToInt32(rdr.GetValue(0)));
        Assert.Equal(5, ids.Count);
        Assert.Equal(10, ids[0]); // first row after skipping 10
        Assert.Equal(14, ids[4]);
    }

    [Fact(Skip = "OFFSET via TDS needs FETCH NEXT syntax")]
    public void OFF02_OffsetOnly()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS off2 (id INT NOT NULL)");
        _db.Exec("DELETE FROM off2");
        for (int i = 0; i < 10; i++) _db.Exec($"INSERT INTO off2 VALUES ({i})");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT id FROM off2 ORDER BY id LIMIT 100 OFFSET 7", conn);
        using var rdr = cmd.ExecuteReader();
        int count = 0;
        while (rdr.Read()) count++;
        Assert.Equal(3, count); // 10 total - 7 offset = 3 remaining
    }

    [Fact(Skip = "OFFSET via TDS needs FETCH NEXT syntax")]
    public void OFF03_OffsetConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS off3 (id INT NOT NULL)");
        _db.Exec("DELETE FROM off3");
        for (int i = 0; i < 100; i++) _db.Exec($"INSERT INTO off3 VALUES ({i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM off3 LIMIT 10 OFFSET {t * 2}", conn);
            cmd.ExecuteScalar(); // just verify no crash
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// UPSERT / ON DUPLICATE KEY
// ===========================================================================

[Collection("ForgeDB")]
public class UpsertTests
{
    private readonly ForgeDbFixture _db;
    public UpsertTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void UP01_InsertOnDuplicateKeyDoNothing()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS up1 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)");
        _db.Exec("DELETE FROM up1");
        _db.Exec("INSERT INTO up1 VALUES (1, 100)");

        // Insert duplicate with ON DUPLICATE KEY UPDATE — should not error
        _db.Exec("INSERT INTO up1 VALUES (1, 200) ON DUPLICATE KEY UPDATE v = 200");
        Assert.Equal(1, _db.Count("up1"));
    }
}

// ===========================================================================
// WINDOW FUNCTIONS — LAG, LEAD, NTILE
// ===========================================================================

[Collection("ForgeDB")]
public class ExtendedWindowTests
{
    private readonly ForgeDbFixture _db;
    public ExtendedWindowTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void WF01_RowNumber_Partition()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wfe1 (dept INT NOT NULL, name VARCHAR(50), sal INT NOT NULL)");
        _db.Exec("DELETE FROM wfe1");
        _db.Exec("INSERT INTO wfe1 VALUES (1,'A',100),(1,'B',200),(2,'C',150),(2,'D',250),(2,'E',50)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal) FROM wfe1", conn);
        using var rdr = cmd.ExecuteReader();
        int rows = 0;
        while (rdr.Read()) rows++;
        Assert.Equal(5, rows);
    }

    [Fact]
    public void WF02_Rank_WithTies()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wfe2 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM wfe2");
        _db.Exec("INSERT INTO wfe2 VALUES (1,100),(2,200),(3,200),(4,300)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, RANK() OVER (ORDER BY score) FROM wfe2", conn);
        using var rdr = cmd.ExecuteReader();
        var ranks = new List<string>();
        while (rdr.Read()) ranks.Add(rdr.GetValue(1).ToString()!);
        Assert.Equal(4, ranks.Count);
        Assert.Equal("1", ranks[0]); // score 100 -> rank 1
    }

    [Fact]
    public void WF03_DenseRank()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wfe3 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM wfe3");
        _db.Exec("INSERT INTO wfe3 VALUES (1,10),(2,20),(3,20),(4,30)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, DENSE_RANK() OVER (ORDER BY score) FROM wfe3", conn);
        using var rdr = cmd.ExecuteReader();
        var ranks = new List<string>();
        while (rdr.Read()) ranks.Add(rdr.GetValue(1).ToString()!);
        Assert.Equal(4, ranks.Count);
    }

    [Fact]
    public void WF04_WindowConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS wfe4 (dept INT NOT NULL, val INT NOT NULL)");
        _db.Exec("DELETE FROM wfe4");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO wfe4 VALUES ({i % 5}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT val, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val) FROM wfe4", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 50) errors.Add($"expected 50, got {rows}");
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// DATA TYPES — DECIMAL, DATE, TIME, JSON, UUID
// ===========================================================================

[Collection("ForgeDB")]
public class ExtendedDataTypeTests
{
    private readonly ForgeDbFixture _db;
    public ExtendedDataTypeTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void DT01_DecimalType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt1 (id INT NOT NULL, price DECIMAL(10,2) NOT NULL)");
        _db.Exec("DELETE FROM ddt1");
        _db.Exec("INSERT INTO ddt1 VALUES (1, 19.99)");
        _db.Exec("INSERT INTO ddt1 VALUES (2, 29.99)");

        Assert.Equal(2, _db.Count("ddt1"));
        // Values stored and retrievable
        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT price FROM ddt1 WHERE id = 1", conn);
        var price = cmd.ExecuteScalar()?.ToString();
        Assert.Contains("19", price!); // should contain 19.99 or similar
    }

    [Fact]
    public void DT02_DateType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt2 (id INT NOT NULL, d DATE NOT NULL)");
        _db.Exec("DELETE FROM ddt2");
        _db.Exec("INSERT INTO ddt2 VALUES (1, '2024-01-15')");
        Assert.Equal(1, _db.Count("ddt2"));
    }

    [Fact]
    public void DT03_TimeType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt3 (id INT NOT NULL, t TIME NOT NULL)");
        _db.Exec("DELETE FROM ddt3");
        _db.Exec("INSERT INTO ddt3 VALUES (1, '14:30:00')");
        Assert.Equal(1, _db.Count("ddt3"));
    }

    [Fact]
    public void DT04_JsonType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt4 (id INT NOT NULL, data JSON NULL)");
        _db.Exec("DELETE FROM ddt4");
        _db.Exec("INSERT INTO ddt4 VALUES (1, '{\"name\":\"test\"}')");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT data FROM ddt4 WHERE id = 1", conn);
        var json = cmd.ExecuteScalar()?.ToString();
        Assert.Contains("name", json!);
    }

    [Fact]
    public void DT05_UuidType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt5 (id INT NOT NULL, uid VARCHAR(36) NOT NULL)");
        _db.Exec("DELETE FROM ddt5");

        using var conn = _db.CreateConnection();
        // Generate UUID using NEWID() function
        using var cmd = new SqlCommand("SELECT NEWID()", conn);
        var uuid = cmd.ExecuteScalar()?.ToString();
        Assert.NotNull(uuid);
        Assert.Equal(36, uuid!.Length); // UUID format: 8-4-4-4-12
        Assert.Contains("-", uuid);
    }

    [Fact]
    public void DT06_VarBinaryType()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt6 (id INT NOT NULL, data VARBINARY(100) NULL)");
        _db.Exec("DELETE FROM ddt6");
        _db.Exec("INSERT INTO ddt6 VALUES (1, NULL)");
        Assert.Equal(1, _db.Count("ddt6"));
    }

    [Fact]
    public void DT07_DecimalArithmetic()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt7 (id INT NOT NULL, a DECIMAL(10,2), b DECIMAL(10,2))");
        _db.Exec("DELETE FROM ddt7");
        _db.Exec("INSERT INTO ddt7 VALUES (1, 10.50, 20.25)");

        using var conn = _db.CreateConnection();
        var sum = new SqlCommand("SELECT SUM(a) FROM ddt7", conn).ExecuteScalar()?.ToString();
        Assert.NotNull(sum);
    }

    [Fact]
    public void DT08_AllTypesConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ddt8 (id INT NOT NULL, v VARCHAR(100))");
        _db.Exec("DELETE FROM ddt8");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO ddt8 VALUES ({i}, 'val_{i}')");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                using var conn = _db.CreateConnection();
                string sql = (t % 5) switch
                {
                    0 => "SELECT COUNT(*) FROM ddt8",
                    1 => $"SELECT v FROM ddt8 WHERE id = {t % 50}",
                    2 => "SELECT NEWID()",
                    3 => "SELECT 1 + 1",
                    _ => $"INSERT INTO ddt8 VALUES ({1000 + t}, 'new_{t}')",
                };
                using var cmd = new SqlCommand(sql, conn);
                if (sql.StartsWith("INSERT"))
                    cmd.ExecuteNonQuery();
                else
                    cmd.ExecuteScalar();
            }
            catch (Exception ex) { errors.Add($"t{t}: {ex.Message.Split('\n')[0]}"); }
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// COMPLEX QUERY EXPRESSIONS
// ===========================================================================

[Collection("ForgeDB")]
public class ComplexExpressionTests
{
    private readonly ForgeDbFixture _db;
    public ComplexExpressionTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void CE01_SelectExpression()
    {
        var r = _db.Scalar("SELECT 2 + 3 * 4");
        Assert.Equal("14", r?.ToString());
    }

    [Fact]
    public void CE02_SelectStringConcat()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ce2 (first_name VARCHAR(50), last_name VARCHAR(50))");
        _db.Exec("DELETE FROM ce2");
        _db.Exec("INSERT INTO ce2 VALUES ('John', 'Doe')");

        var r = _db.Scalar("SELECT CONCAT(first_name, ' ', last_name) FROM ce2");
        Assert.Equal("John Doe", r?.ToString());
    }

    [Fact]
    public void CE03_CaseWhenNull()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ce3 (id INT NOT NULL, v INT NULL)");
        _db.Exec("DELETE FROM ce3");
        _db.Exec("INSERT INTO ce3 VALUES (1, NULL)");
        _db.Exec("INSERT INTO ce3 VALUES (2, 42)");

        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand(
            "SELECT id, CASE WHEN v IS NULL THEN 'none' ELSE 'has' END FROM ce3 ORDER BY id", conn);
        using var rdr = cmd.ExecuteReader();
        var results = new List<string>();
        while (rdr.Read()) results.Add(rdr.GetValue(1).ToString()!);
        Assert.Equal("none", results[0]);
        Assert.Equal("has", results[1]);
    }

    [Fact]
    public void CE04_NestedFunctions()
    {
        var r = _db.Scalar("SELECT ABS(ROUND(-3.7, 0))");
        Assert.NotNull(r);
    }

    [Fact]
    public void CE05_CountDistinct()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ce5 (id INT NOT NULL, cat VARCHAR(10))");
        _db.Exec("DELETE FROM ce5");
        _db.Exec("INSERT INTO ce5 VALUES (1,'A'),(2,'B'),(3,'A'),(4,'C'),(5,'B')");

        var r = _db.Scalar("SELECT COUNT(DISTINCT cat) FROM ce5");
        Assert.Equal("3", r?.ToString());
    }
}
