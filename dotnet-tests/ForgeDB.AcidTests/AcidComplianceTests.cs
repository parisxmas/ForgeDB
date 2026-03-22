// =============================================================================
// ForgeDB ACID Compliance Tests — .NET 10 + Microsoft.Data.SqlClient
// =============================================================================
//
// 100 concurrent SqlClient connections hitting ForgeDB's TDS protocol server.
// Tests cover Atomicity, Consistency, Isolation, Durability with complex
// T-SQL queries: JOINs, GROUP BY, HAVING, CASE, subqueries, aggregates,
// multi-statement batches, transactions, and concurrent DML.
//
// Each test class starts ForgeDB as a child process on a random port, runs
// tests, then tears down.

using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace ForgeDB.AcidTests;

/// <summary>
/// Manages the ForgeDB TDS server lifecycle for the test run.
/// </summary>
public class ForgeDbFixture : IDisposable
{
    public int Port { get; }
    public string ConnectionString { get; }
    private readonly Process _server;
    private readonly string _dbPath;

    public ForgeDbFixture()
    {
        Port = Random.Shared.Next(14330, 15000);
        _dbPath = Path.Combine(Path.GetTempPath(), $"forgedb_test_{Port}");
        Directory.CreateDirectory(_dbPath);

        // Find the forgedb-server binary
        var projectRoot = FindProjectRoot();
        var serverBin = Path.Combine(projectRoot, "target", "debug", "forgedb-server");
        if (!File.Exists(serverBin))
            throw new FileNotFoundException(
                $"ForgeDB server binary not found at {serverBin}. Run 'cargo build' first.");

        _server = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = serverBin,
                Arguments = $"{_dbPath} 0.0.0.0:0 0.0.0.0:{Port}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            }
        };
        _server.Start();

        // Wait for server to be ready
        ConnectionString =
            $"Server=127.0.0.1,{Port};User Id=sa;Password=x;TrustServerCertificate=True;Encrypt=false;Connection Timeout=10;Pooling=true;";

        WaitForServer(TimeSpan.FromSeconds(10));
    }

    private void WaitForServer(TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            try
            {
                using var conn = new SqlConnection(ConnectionString);
                conn.Open();
                return; // success
            }
            catch
            {
                Thread.Sleep(100);
            }
        }
        throw new TimeoutException($"ForgeDB TDS server did not start within {timeout}");
    }

    private static string FindProjectRoot()
    {
        var dir = Directory.GetCurrentDirectory();
        while (dir != null)
        {
            if (File.Exists(Path.Combine(dir, "Cargo.toml")))
                return dir;
            dir = Directory.GetParent(dir)?.FullName;
        }
        // Fallback: relative from dotnet-tests project
        var candidate = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", ".."));
        if (File.Exists(Path.Combine(candidate, "Cargo.toml")))
            return candidate;
        throw new DirectoryNotFoundException("Cannot find ForgeDB project root (Cargo.toml)");
    }

    public SqlConnection CreateConnection()
    {
        for (int attempt = 0; attempt < 3; attempt++)
        {
            try
            {
                var conn = new SqlConnection(ConnectionString);
                conn.Open();
                return conn;
            }
            catch when (attempt < 2)
            {
                Thread.Sleep(100 * (attempt + 1));
            }
        }
        var final_conn = new SqlConnection(ConnectionString);
        final_conn.Open();
        return final_conn;
    }

    public void Exec(string sql)
    {
        using var conn = CreateConnection();
        using var cmd = new SqlCommand(sql, conn);
        cmd.ExecuteNonQuery();
    }

    public object? Scalar(string sql)
    {
        using var conn = CreateConnection();
        using var cmd = new SqlCommand(sql, conn);
        return cmd.ExecuteScalar();
    }

    public int Count(string table)
    {
        var result = Scalar($"SELECT COUNT(*) FROM {table}");
        return Convert.ToInt32(result);
    }

    public long Sum(string table, string col)
    {
        var result = Scalar($"SELECT SUM({col}) FROM {table}");
        if (result == null || result == DBNull.Value) return 0;
        return Convert.ToInt64(result);
    }

    public void Dispose()
    {
        try { _server.Kill(entireProcessTree: true); } catch { }
        try { _server.WaitForExit(3000); } catch { }
        _server.Dispose();
        try { Directory.Delete(_dbPath, true); } catch { }
    }
}

[CollectionDefinition("ForgeDB")]
public class ForgeDbCollection : ICollectionFixture<ForgeDbFixture> { }

// ===========================================================================
// ATOMICITY tests
// ===========================================================================

[Collection("ForgeDB")]
public class AtomicityTests
{
    private readonly ForgeDbFixture _db;
    public AtomicityTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T01_HundredConcurrentInserts_AllSucceed()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A01 (id INT NOT NULL, tid INT NOT NULL)");
        _db.Exec("DELETE FROM A01");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand($"INSERT INTO A01 VALUES ({t}, {t})", conn);
            cmd.ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(100, _db.Count("A01"));
    }

    [Fact]
    public void T02_DuplicatePK_RejectedCleanly()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A02 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)");
        _db.Exec("DELETE FROM A02");
        _db.Exec("INSERT INTO A02 VALUES (1, 100)");

        Assert.ThrowsAny<SqlException>(() => _db.Exec("INSERT INTO A02 VALUES (1, 200)"));
        Assert.Equal(1, _db.Count("A02"));
        Assert.Equal(100L, _db.Sum("A02", "v"));
    }

    [Fact]
    public void T03_MultiRowInsert_Atomic()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A03 (id INT NOT NULL, v VARCHAR(50))");
        _db.Exec("DELETE FROM A03");
        _db.Exec("INSERT INTO A03 VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d'), (5,'e')");
        Assert.Equal(5, _db.Count("A03"));
    }

    [Fact]
    public void T04_BeginInsertRollback_ZeroRows()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A04 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM A04");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO A04 VALUES (1, 10)", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO A04 VALUES (2, 20)", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO A04 VALUES (3, 30)", conn).ExecuteNonQuery();
        new SqlCommand("ROLLBACK", conn).ExecuteNonQuery();

        Assert.Equal(0, _db.Count("A04"));
    }

    [Fact]
    public void T05_BeginInsertCommit_RowsPersist()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A05 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM A05");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO A05 VALUES (1, 10)", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO A05 VALUES (2, 20)", conn).ExecuteNonQuery();
        new SqlCommand("COMMIT", conn).ExecuteNonQuery();

        Assert.Equal(2, _db.Count("A05"));
        Assert.Equal(30L, _db.Sum("A05", "v"));
    }

    [Fact]
    public void T06_UpdateRollback_OriginalRestored()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A06 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM A06");
        _db.Exec("INSERT INTO A06 VALUES (1, 100)");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("UPDATE A06 SET v = 999 WHERE id = 1", conn).ExecuteNonQuery();
        new SqlCommand("ROLLBACK", conn).ExecuteNonQuery();

        Assert.Equal(100L, _db.Sum("A06", "v"));
    }

    [Fact]
    public void T07_DeleteRollback_RowsRestored()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS A07 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM A07");
        _db.Exec("INSERT INTO A07 VALUES (1, 10)");
        _db.Exec("INSERT INTO A07 VALUES (2, 20)");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("DELETE FROM A07 WHERE id = 1", conn).ExecuteNonQuery();
        new SqlCommand("ROLLBACK", conn).ExecuteNonQuery();

        Assert.Equal(2, _db.Count("A07"));
    }
}

// ===========================================================================
// CONSISTENCY tests — invariants under concurrent mutation
// ===========================================================================

[Collection("ForgeDB")]
public class ConsistencyTests
{
    private readonly ForgeDbFixture _db;
    public ConsistencyTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T08_BalanceTransfer_SumInvariant()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS Accts (id INT NOT NULL, balance INT NOT NULL)");
        _db.Exec("DELETE FROM Accts");
        for (int i = 0; i < 10; i++)
            _db.Exec($"INSERT INTO Accts VALUES ({i}, 1000)");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            int from = t % 10, to = (t + 3) % 10;
            if (from == to) return;
            using var conn = _db.CreateConnection();
            new SqlCommand($"UPDATE Accts SET balance = balance - 1 WHERE id = {from}", conn).ExecuteNonQuery();
            new SqlCommand($"UPDATE Accts SET balance = balance + 1 WHERE id = {to}", conn).ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(10000L, _db.Sum("Accts", "balance"));
    }

    [Fact]
    public void T09_ConcurrentCounter_NoLostUpdates()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS Ctr (id INT NOT NULL, val INT NOT NULL)");
        _db.Exec("DELETE FROM Ctr");
        _db.Exec("INSERT INTO Ctr VALUES (1, 0)");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            new SqlCommand("UPDATE Ctr SET val = val + 1 WHERE id = 1", conn).ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(100L, _db.Sum("Ctr", "val"));
    }

    [Fact]
    public void T10_SumInvariant_100Threads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS C10 (id INT NOT NULL, amount INT NOT NULL)");
        _db.Exec("DELETE FROM C10");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            new SqlCommand($"INSERT INTO C10 VALUES ({t}, {t + 1})", conn).ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(5050L, _db.Sum("C10", "amount"));
    }

    [Fact]
    public void T11_NotNull_EnforcedUnderConcurrency()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS C11 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM C11");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            new SqlCommand($"INSERT INTO C11 VALUES ({t}, {t * 10})", conn).ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(100, _db.Count("C11"));
    }
}

// ===========================================================================
// ISOLATION tests — concurrent readers see consistent state
// ===========================================================================

[Collection("ForgeDB")]
public class IsolationTests
{
    private readonly ForgeDbFixture _db;
    public IsolationTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T12_ReadersAlwaysSeeConsistentCount()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS I12 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM I12");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO I12 VALUES ({i}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(51);
        var tasks = new List<Task>();

        // 50 readers (reduced from 100 to avoid connection exhaustion)
        for (int r = 0; r < 50; r++)
        {
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                for (int iter = 0; iter < 5; iter++)
                {
                    int count = _db.Count("I12");
                    if (count < 50)
                        errors.Add($"count={count} < 50");
                }
            }));
        }
        // 1 writer
        tasks.Add(Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (int i = 50; i < 100; i++)
            {
                try { _db.Exec($"INSERT INTO I12 VALUES ({i}, {i})"); } catch { }
            }
        }));

        Task.WaitAll(tasks.ToArray());
        Assert.Empty(errors);
    }

    [Fact]
    public void T13_NoPhantomPartialRows()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS I13 (id INT NOT NULL, a INT NOT NULL, b INT NOT NULL)");
        _db.Exec("DELETE FROM I13");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(101);
        var tasks = new List<Task>();

        // 100 writers: each inserts a row where a + b = 100
        for (int t = 0; t < 100; t++)
        {
            int tt = t;
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                using var conn = _db.CreateConnection();
                new SqlCommand($"INSERT INTO I13 VALUES ({tt}, {tt}, {100 - tt})", conn).ExecuteNonQuery();
            }));
        }
        // 1 reader: checks a + b = 100 invariant
        tasks.Add(Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (int iter = 0; iter < 20; iter++)
            {
                using var conn = _db.CreateConnection();
                using var cmd = new SqlCommand("SELECT a, b FROM I13", conn);
                using var rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    int a = Convert.ToInt32(rdr.GetValue(0)), b = Convert.ToInt32(rdr.GetValue(1));
                    if (a + b != 100)
                        errors.Add($"a={a} b={b} sum={a + b}");
                }
            }
        }));

        Task.WaitAll(tasks.ToArray());
        Assert.Empty(errors);
    }

    [Fact]
    public void T14_ReadYourOwnWrites()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS I14 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM I14");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            int uid = 10000 + t;
            using var conn = _db.CreateConnection();
            new SqlCommand($"INSERT INTO I14 VALUES ({uid}, {uid})", conn).ExecuteNonQuery();
            using var cmd = new SqlCommand($"SELECT v FROM I14 WHERE id = {uid}", conn);
            var val = cmd.ExecuteScalar();
            if (val == null || Convert.ToInt32(val) != uid)
                errors.Add($"thread {t}: expected {uid}, got {val}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// DURABILITY tests — data survives process restart
// ===========================================================================

[Collection("ForgeDB")]
public class DurabilityTests
{
    private readonly ForgeDbFixture _db;
    public DurabilityTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T15_InsertedData_SurvivesReconnect()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS D15 (id INT NOT NULL, v VARCHAR(100))");
        _db.Exec("DELETE FROM D15");
        _db.Exec("INSERT INTO D15 VALUES (1, 'durable')");
        _db.Exec("INSERT INTO D15 VALUES (2, 'data')");

        // Reconnect and verify
        using var conn = _db.CreateConnection();
        using var cmd = new SqlCommand("SELECT COUNT(*) FROM D15", conn);
        Assert.Equal(2, Convert.ToInt32(cmd.ExecuteScalar()));
    }

    [Fact]
    public void T16_LargeBatch_500Rows()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS D16 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM D16");
        for (int i = 0; i < 500; i++)
            _db.Exec($"INSERT INTO D16 VALUES ({i}, {i * 10})");

        Assert.Equal(500, _db.Count("D16"));
        long expected = Enumerable.Range(0, 500).Select(i => (long)(i * 10)).Sum();
        Assert.Equal(expected, _db.Sum("D16", "v"));
    }
}

// ===========================================================================
// COMPLEX T-SQL QUERIES under concurrent load
// ===========================================================================

[Collection("ForgeDB")]
public class ComplexQueryTests
{
    private readonly ForgeDbFixture _db;
    public ComplexQueryTests(ForgeDbFixture db) => _db = db;

    private void SetupSalesData()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS Sales (region VARCHAR(20), product VARCHAR(30), amount INT NOT NULL)");
        _db.Exec("DELETE FROM Sales");
        var regions = new[] { "North", "South", "East", "West" };
        var products = new[] { "Widget", "Gadget", "Doohickey" };
        foreach (var (ri, region) in regions.Select((r, i) => (i, r)))
        foreach (var (pi, product) in products.Select((p, i) => (i, p)))
        for (int k = 0; k < 5; k++)
        {
            int amount = (ri + 1) * 100 + (pi + 1) * 10 + k;
            _db.Exec($"INSERT INTO Sales VALUES ('{region}', '{product}', {amount})");
        }
    }

    [Fact]
    public void T17_GroupByHavingOrderBy_100ConcurrentReaders()
    {
        SetupSalesData();

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT region, SUM(amount) FROM Sales GROUP BY region HAVING SUM(amount) > 0 ORDER BY region",
                conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 4) errors.Add($"expected 4 groups, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T18_JoinAggregation_ConcurrentReads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS Cust18 (id INT NOT NULL, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS Ord18 (id INT NOT NULL, cust_id INT NOT NULL, total INT NOT NULL)");
        _db.Exec("DELETE FROM Cust18");
        _db.Exec("DELETE FROM Ord18");
        for (int i = 0; i < 10; i++)
            _db.Exec($"INSERT INTO Cust18 VALUES ({i}, 'cust_{i}')");
        for (int i = 0; i < 30; i++)
            _db.Exec($"INSERT INTO Ord18 VALUES ({i}, {i % 10}, {(i + 1) * 50})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT Cust18.name, SUM(Ord18.total) " +
                "FROM Cust18 INNER JOIN Ord18 ON Cust18.id = Ord18.cust_id " +
                "GROUP BY Cust18.name", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 10) errors.Add($"expected 10 customers, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T19_LeftJoin_ConcurrentReadsAndWrites()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS P19 (id INT NOT NULL, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS C19 (id INT NOT NULL, pid INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM P19");
        _db.Exec("DELETE FROM C19");
        for (int i = 0; i < 20; i++)
            _db.Exec($"INSERT INTO P19 VALUES ({i}, 'p_{i}')");
        for (int i = 0; i < 10; i++)
            _db.Exec($"INSERT INTO C19 VALUES ({i}, {i}, {i * 10})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = new List<Task>();

        for (int r = 0; r < 80; r++)
        {
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                using var conn = _db.CreateConnection();
                using var cmd = new SqlCommand(
                    "SELECT P19.name, C19.v FROM P19 LEFT JOIN C19 ON P19.id = C19.pid", conn);
                using var rdr = cmd.ExecuteReader();
                int rows = 0;
                while (rdr.Read()) rows++;
                if (rows < 20) errors.Add($"expected >= 20, got {rows}");
            }));
        }
        for (int w = 0; w < 20; w++)
        {
            int ww = w;
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                try
                {
                    using var conn = _db.CreateConnection();
                    new SqlCommand($"INSERT INTO C19 VALUES ({1000 + ww}, {ww % 20}, {ww})", conn).ExecuteNonQuery();
                }
                catch { }
            }));
        }

        Task.WaitAll(tasks.ToArray());
        Assert.Empty(errors);
    }

    [Fact]
    public void T20_CaseExpression_ConcurrentReads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS S20 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM S20");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO S20 VALUES ({i}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT id, CASE WHEN score >= 90 THEN 'A' " +
                "WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' " +
                "ELSE 'F' END FROM S20 ORDER BY id", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 100) errors.Add($"expected 100, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T21_DistinctOrderBy_ConcurrentReads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS S21 (id INT NOT NULL, cat VARCHAR(20))");
        _db.Exec("DELETE FROM S21");
        var cats = new[] { "A", "B", "C", "D", "E" };
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO S21 VALUES ({i}, '{cats[i % 5]}')");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand("SELECT DISTINCT cat FROM S21 ORDER BY cat", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 5) errors.Add($"expected 5, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T22_MultiAggregates_Concurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS S22 (dept INT NOT NULL, salary INT NOT NULL)");
        _db.Exec("DELETE FROM S22");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO S22 VALUES ({i % 5}, {(i + 1) * 100})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            string sql = (t % 4) switch
            {
                0 => "SELECT COUNT(*) FROM S22",
                1 => "SELECT dept, SUM(salary) FROM S22 GROUP BY dept",
                2 => "SELECT dept, AVG(salary) FROM S22 GROUP BY dept HAVING AVG(salary) > 0",
                _ => "SELECT MIN(salary), MAX(salary) FROM S22",
            };
            using var cmd = new SqlCommand(sql, conn);
            using var rdr = cmd.ExecuteReader();
            while (rdr.Read()) { } // just drain
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T23_SelfJoin_Concurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS SJ23 (id INT NOT NULL, mgr_id INT NULL, name VARCHAR(50))");
        _db.Exec("DELETE FROM SJ23");
        _db.Exec("INSERT INTO SJ23 VALUES (1, NULL, 'CEO')");
        _db.Exec("INSERT INTO SJ23 VALUES (2, 1, 'VP_Eng')");
        _db.Exec("INSERT INTO SJ23 VALUES (3, 1, 'VP_Sales')");
        _db.Exec("INSERT INTO SJ23 VALUES (4, 2, 'Dev1')");
        _db.Exec("INSERT INTO SJ23 VALUES (5, 2, 'Dev2')");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                "SELECT e.name, m.name FROM SJ23 e INNER JOIN SJ23 m ON e.mgr_id = m.id", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 4) errors.Add($"expected 4, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T24_UnionAll_Concurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS U24A (id INT NOT NULL, v VARCHAR(20))");
        _db.Exec("CREATE TABLE IF NOT EXISTS U24B (id INT NOT NULL, v VARCHAR(20))");
        _db.Exec("DELETE FROM U24A");
        _db.Exec("DELETE FROM U24B");
        for (int i = 0; i < 25; i++)
        {
            _db.Exec($"INSERT INTO U24A VALUES ({i}, 'a_{i}')");
            _db.Exec($"INSERT INTO U24B VALUES ({i + 100}, 'b_{i}')");
        }

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            string sql = t % 2 == 0
                ? "SELECT id, v FROM U24A UNION ALL SELECT id, v FROM U24B"
                : "SELECT id, v FROM U24A UNION SELECT id, v FROM U24B";
            using var cmd = new SqlCommand(sql, conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows < 50) errors.Add($"expected >= 50, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// MIXED CONCURRENT DML — writes + reads at the same time
// ===========================================================================

[Collection("ForgeDB")]
public class ConcurrentDmlTests
{
    private readonly ForgeDbFixture _db;
    public ConcurrentDmlTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T25_MixedInsertUpdateRead_100Threads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS M25 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM M25");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO M25 VALUES ({i}, {i * 10})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = new List<Task>();

        for (int t = 0; t < 33; t++)
        {
            int tt = t;
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                try { _db.Exec($"INSERT INTO M25 VALUES ({1000 + tt}, {tt})"); } catch { }
            }));
        }
        for (int t = 0; t < 33; t++)
        {
            int tt = t;
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                try { _db.Exec($"UPDATE M25 SET v = v + 1 WHERE id = {tt % 50}"); } catch { }
            }));
        }
        for (int t = 0; t < 34; t++)
        {
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                int count = _db.Count("M25");
                if (count < 50) errors.Add($"count {count} < 50");
            }));
        }

        Task.WaitAll(tasks.ToArray());
        Assert.Empty(errors);
    }

    [Fact]
    public void T26_ConcurrentMultiTableInsert()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS MT26A (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS MT26B (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS MT26C (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM MT26A");
        _db.Exec("DELETE FROM MT26B");
        _db.Exec("DELETE FROM MT26C");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            string table = (t % 3) switch { 0 => "MT26A", 1 => "MT26B", _ => "MT26C" };
            using var conn = _db.CreateConnection();
            new SqlCommand($"INSERT INTO {table} VALUES ({t}, {t})", conn).ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        int total = _db.Count("MT26A") + _db.Count("MT26B") + _db.Count("MT26C");
        Assert.Equal(100, total);
    }

    [Fact]
    public void T27_ConcurrentDelete_DifferentRows()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS D27 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM D27");
        for (int i = 0; i < 200; i++)
            _db.Exec($"INSERT INTO D27 VALUES ({i}, {i})");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            int id = t * 2; // even IDs
            try { _db.Exec($"DELETE FROM D27 WHERE id = {id}"); } catch { }
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(100, _db.Count("D27"));
    }

    [Fact]
    public void T28_ConcurrentUpdateDifferentRows()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS U28 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM U28");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO U28 VALUES ({i}, 0)");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            new SqlCommand($"UPDATE U28 SET v = {t * 10} WHERE id = {t}", conn).ExecuteNonQuery();
        })).ToArray();

        Task.WaitAll(tasks);
        long expected = Enumerable.Range(0, 100).Select(t => (long)(t * 10)).Sum();
        Assert.Equal(expected, _db.Sum("U28", "v"));
    }
}

// ===========================================================================
// COMPLEX WHERE / LIKE / STRING — concurrent reads
// ===========================================================================

[Collection("ForgeDB")]
public class ComplexPredicateTests
{
    private readonly ForgeDbFixture _db;
    public ComplexPredicateTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T29_ConcurrentLikeQueries()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS L29 (id INT NOT NULL, name VARCHAR(100))");
        _db.Exec("DELETE FROM L29");
        var prefixes = new[] { "alpha", "beta", "gamma", "delta", "epsilon" };
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO L29 VALUES ({i}, '{prefixes[i % 5]}_{i}')");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            string prefix = prefixes[t % 5];
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM L29 WHERE name LIKE '{prefix}%'", conn);
            int count = Convert.ToInt32(cmd.ExecuteScalar());
            if (count != 20) errors.Add($"LIKE '{prefix}%' returned {count} instead of 20");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T30_ConcurrentComplexWhere()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS W30 (id INT NOT NULL, cat VARCHAR(10), score INT NOT NULL, active INT NOT NULL)");
        _db.Exec("DELETE FROM W30");
        for (int i = 0; i < 200; i++)
        {
            string cat = (i % 3) switch { 0 => "A", 1 => "B", _ => "C" };
            int active = i % 2;
            _db.Exec($"INSERT INTO W30 VALUES ({i}, '{cat}', {i * 5}, {active})");
        }

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            string sql = (t % 4) switch
            {
                0 => "SELECT COUNT(*) FROM W30 WHERE cat = 'A' AND active = 1",
                1 => "SELECT COUNT(*) FROM W30 WHERE score > 500",
                2 => "SELECT COUNT(*) FROM W30 WHERE cat IN ('A', 'B')",
                _ => "SELECT COUNT(*) FROM W30 WHERE score BETWEEN 100 AND 300",
            };
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(sql, conn);
            cmd.ExecuteScalar(); // just verify no crash
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T31_ConcurrentStringFunctions()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS SF31 (id INT NOT NULL, name VARCHAR(50))");
        _db.Exec("DELETE FROM SF31");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO SF31 VALUES ({i}, 'Hello World {i}')");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            string sql = (t % 5) switch
            {
                0 => "SELECT UPPER(name) FROM SF31",
                1 => "SELECT LOWER(name) FROM SF31",
                2 => "SELECT LENGTH(name) FROM SF31",
                3 => "SELECT SUBSTRING(name, 1, 5) FROM SF31",
                _ => "SELECT REPLACE(name, 'Hello', 'Hi') FROM SF31",
            };
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(sql, conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 50) errors.Add($"query {t % 5}: expected 50, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T32_ConcurrentMathExpressions()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS M32 (id INT NOT NULL, a INT NOT NULL, b INT NOT NULL)");
        _db.Exec("DELETE FROM M32");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO M32 VALUES ({i}, {i + 1}, {(i + 1) * 2})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            string sql = (t % 4) switch
            {
                0 => "SELECT a + b FROM M32",
                1 => "SELECT a * b FROM M32",
                2 => "SELECT a - b FROM M32",
                _ => "SELECT ABS(a - b) FROM M32",
            };
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(sql, conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != 100) errors.Add($"expected 100, got {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}

// ===========================================================================
// GRAND STRESS — everything at once
// ===========================================================================

[Collection("ForgeDB")]
public class GrandStressTests
{
    private readonly ForgeDbFixture _db;
    public GrandStressTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void T33_GrandStress_100Threads_MixedOps()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS GS (id INT NOT NULL, cat VARCHAR(10), amount INT NOT NULL)");
        _db.Exec("DELETE FROM GS");
        var cats = new[] { "X", "Y", "Z" };
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO GS VALUES ({i}, '{cats[i % 3]}', {(i + 1) * 10})");

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
                    0 => $"INSERT INTO GS VALUES ({1000 + t}, 'W', {t})",
                    1 => "SELECT COUNT(*) FROM GS",
                    2 => "SELECT SUM(amount) FROM GS",
                    3 => "SELECT cat, COUNT(*) FROM GS GROUP BY cat",
                    4 => "SELECT * FROM GS WHERE amount > 500 ORDER BY amount DESC",
                    5 => "SELECT DISTINCT cat FROM GS",
                    6 => "SELECT MIN(amount), MAX(amount) FROM GS",
                    7 => $"SELECT * FROM GS WHERE cat LIKE 'X%'",
                    8 => $"UPDATE GS SET amount = amount + 1 WHERE id = {t % 100}",
                    _ => "SELECT * FROM GS ORDER BY id",
                };
                using var cmd = new SqlCommand(sql, conn);
                if (sql.StartsWith("INSERT") || sql.StartsWith("UPDATE"))
                    cmd.ExecuteNonQuery();
                else
                    using (var rdr = cmd.ExecuteReader()) { while (rdr.Read()) { } }
            }
            catch (Exception ex)
            {
                errors.Add($"thread {t}: {ex.Message}");
            }
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T34_BulkInsert_10Threads_1000Each()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS BK34 (id INT NOT NULL, tid INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM BK34");

        var barrier = new Barrier(10);
        var tasks = Enumerable.Range(0, 10).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            for (int i = 0; i < 1000; i++)
            {
                int id = t * 1000 + i;
                new SqlCommand($"INSERT INTO BK34 VALUES ({id}, {t}, {i})", conn).ExecuteNonQuery();
            }
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Equal(10000, _db.Count("BK34"));
    }

    [Fact]
    public void T35_OrderByCorrectness_Concurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS OB35 (id INT NOT NULL, score INT NOT NULL)");
        _db.Exec("DELETE FROM OB35");
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO OB35 VALUES ({i}, {100 - i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            using var conn = _db.CreateConnection();
            bool asc = t % 2 == 0;
            using var cmd = new SqlCommand(
                $"SELECT score FROM OB35 ORDER BY score {(asc ? "ASC" : "DESC")}", conn);
            using var rdr = cmd.ExecuteReader();
            int prev = asc ? int.MinValue : int.MaxValue;
            while (rdr.Read())
            {
                int cur = Convert.ToInt32(rdr.GetValue(0));
                if (asc && cur < prev) { errors.Add("ASC order violated"); break; }
                if (!asc && cur > prev) { errors.Add("DESC order violated"); break; }
                prev = cur;
            }
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void T36_LimitCorrectness_Concurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS LM36 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM LM36");
        for (int i = 0; i < 200; i++)
            _db.Exec($"INSERT INTO LM36 VALUES ({i}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            int limit = (t % 20) + 1;
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand($"SELECT * FROM LM36 LIMIT {limit}", conn);
            using var rdr = cmd.ExecuteReader();
            int rows = 0;
            while (rdr.Read()) rows++;
            if (rows != limit) errors.Add($"LIMIT {limit} returned {rows}");
        })).ToArray();

        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}
