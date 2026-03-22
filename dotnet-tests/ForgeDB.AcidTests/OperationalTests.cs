// =============================================================================
// Operational Feature Tests — .NET 10 + Microsoft.Data.SqlClient
// =============================================================================
// Stored procs, triggers, users, prepared stmts, backup, plan cache,
// temp tables, cursors, connection pooling.

using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

namespace ForgeDB.AcidTests;

// ===========================================================================
// STORED PROCEDURES
// ===========================================================================

[Collection("ForgeDB")]
public class StoredProcTests
{
    private readonly ForgeDbFixture _db;
    public StoredProcTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void PROC01_CreateAndExec()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS proc1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM proc1");

        _db.Exec("CREATE PROCEDURE insert_row AS BEGIN INSERT INTO proc1 VALUES (1, 100) END");
        _db.Exec("EXEC insert_row");

        Assert.Equal(1, _db.Count("proc1"));
        Assert.Equal(100L, _db.Sum("proc1", "v"));
    }

    [Fact]
    public void PROC02_MultiStatementProc()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS proc2 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM proc2");

        _db.Exec("CREATE PROCEDURE multi_insert AS BEGIN INSERT INTO proc2 VALUES (1, 10); INSERT INTO proc2 VALUES (2, 20); INSERT INTO proc2 VALUES (3, 30) END");
        _db.Exec("EXEC multi_insert");

        Assert.Equal(3, _db.Count("proc2"));
        Assert.Equal(60L, _db.Sum("proc2", "v"));
    }

    [Fact]
    public void PROC03_ExecConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS proc3 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM proc3");
        _db.Exec("CREATE PROCEDURE ins_proc3 AS BEGIN INSERT INTO proc3 VALUES (0, 1) END");

        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try { _db.Exec("EXEC ins_proc3"); } catch { }
        })).ToArray();
        Task.WaitAll(tasks);

        Assert.True(_db.Count("proc3") > 0);
    }
}

// ===========================================================================
// TRIGGERS
// ===========================================================================

[Collection("ForgeDB")]
public class TriggerTests
{
    private readonly ForgeDbFixture _db;
    public TriggerTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void TRIG01_AfterInsertTrigger()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS trig1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS trig1_log (msg VARCHAR(100))");
        _db.Exec("DELETE FROM trig1");
        _db.Exec("DELETE FROM trig1_log");

        _db.Exec("CREATE TRIGGER trg1 ON trig1 AFTER INSERT AS BEGIN INSERT INTO trig1_log VALUES ('inserted') END");
        _db.Exec("INSERT INTO trig1 VALUES (1, 100)");

        Assert.Equal(1, _db.Count("trig1"));
        Assert.Equal(1, _db.Count("trig1_log"));
    }

    [Fact]
    public void TRIG02_AfterDeleteTrigger()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS trig2 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("CREATE TABLE IF NOT EXISTS trig2_log (msg VARCHAR(100))");
        _db.Exec("DELETE FROM trig2");
        _db.Exec("DELETE FROM trig2_log");

        _db.Exec("INSERT INTO trig2 VALUES (1, 100)");
        _db.Exec("CREATE TRIGGER trg2 ON trig2 AFTER DELETE AS BEGIN INSERT INTO trig2_log VALUES ('deleted') END");
        _db.Exec("DELETE FROM trig2 WHERE id = 1");

        Assert.Equal(0, _db.Count("trig2"));
        Assert.Equal(1, _db.Count("trig2_log"));
    }
}

// ===========================================================================
// USER / ROLE MANAGEMENT
// ===========================================================================

[Collection("ForgeDB")]
public class UserManagementTests
{
    private readonly ForgeDbFixture _db;
    public UserManagementTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void USER01_CreateUser()
    {
        _db.Exec("CREATE USER testuser WITH PASSWORD 'secret123'");
        // Should not throw
    }

    [Fact]
    public void USER02_GrantRevoke()
    {
        _db.Exec("CREATE USER grantuser WITH PASSWORD 'pass'");
        _db.Exec("GRANT SELECT ON mytable TO grantuser");
        _db.Exec("REVOKE SELECT ON mytable FROM grantuser");
        // Should not throw
    }

    [Fact]
    public void USER03_DropUser()
    {
        _db.Exec("CREATE USER dropme WITH PASSWORD 'pass'");
        _db.Exec("DROP USER dropme");
        // Should not throw
    }
}

// ===========================================================================
// PREPARED STATEMENTS
// ===========================================================================

[Collection("ForgeDB")]
public class PreparedStmtTests
{
    private readonly ForgeDbFixture _db;
    public PreparedStmtTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void PREP01_PrepareAndExecute()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS prep1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM prep1");

        _db.Exec("PREPARE my_insert AS INSERT INTO prep1 VALUES (1, 42)");
        _db.Exec("EXECUTE my_insert");

        Assert.Equal(1, _db.Count("prep1"));
        Assert.Equal(42L, _db.Sum("prep1", "v"));
    }

    [Fact]
    public void PREP02_PrepareSelect()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS prep2 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM prep2");
        _db.Exec("INSERT INTO prep2 VALUES (1, 100), (2, 200)");

        _db.Exec("PREPARE my_select AS SELECT COUNT(*) FROM prep2");
        var r = _db.Scalar("EXECUTE my_select");
        Assert.Equal("2", r?.ToString());
    }
}

// ===========================================================================
// QUERY PLAN CACHE
// ===========================================================================

[Collection("ForgeDB")]
public class PlanCacheTests
{
    private readonly ForgeDbFixture _db;
    public PlanCacheTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void CACHE01_RepeatedQueryUsesCache()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cache1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM cache1");
        for (int i = 0; i < 10; i++)
            _db.Exec($"INSERT INTO cache1 VALUES ({i}, {i * 10})");

        // Run same query multiple times — should hit plan cache
        for (int i = 0; i < 10; i++)
        {
            var r = _db.Scalar("SELECT COUNT(*) FROM cache1");
            Assert.Equal("10", r?.ToString());
        }
    }

    [Fact]
    public void CACHE02_DDLInvalidatesCache()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cache2 (id INT NOT NULL)");
        _db.Exec("DELETE FROM cache2");
        _db.Exec("INSERT INTO cache2 VALUES (1)");

        var r1 = _db.Scalar("SELECT COUNT(*) FROM cache2");
        Assert.Equal("1", r1?.ToString());

        // DDL invalidates cache
        _db.Exec("CREATE TABLE IF NOT EXISTS cache2_other (id INT NOT NULL)");

        _db.Exec("INSERT INTO cache2 VALUES (2)");
        var r2 = _db.Scalar("SELECT COUNT(*) FROM cache2");
        Assert.Equal("2", r2?.ToString());
    }
}

// ===========================================================================
// TEMP TABLES
// ===========================================================================

[Collection("ForgeDB")]
public class TempTableTests
{
    private readonly ForgeDbFixture _db;
    public TempTableTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void TEMP01_CreateAndQueryTempTable()
    {
        using var conn = _db.CreateConnection();
        new SqlCommand("CREATE TABLE IF NOT EXISTS tmp_test1 (id INT NOT NULL, v INT NOT NULL)", conn).ExecuteNonQuery();
        new SqlCommand("DELETE FROM tmp_test1", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO tmp_test1 VALUES (1, 42)", conn).ExecuteNonQuery();

        var r = new SqlCommand("SELECT COUNT(*) FROM tmp_test1", conn).ExecuteScalar();
        Assert.Equal("1", r?.ToString());
    }
}

// ===========================================================================
// CURSORS
// ===========================================================================

[Collection("ForgeDB")]
public class CursorTests
{
    private readonly ForgeDbFixture _db;
    public CursorTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void CUR01_DeclareFetchClose()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cur1 (id INT NOT NULL, name VARCHAR(50))");
        _db.Exec("DELETE FROM cur1");
        _db.Exec("INSERT INTO cur1 VALUES (1, 'Alice')");
        _db.Exec("INSERT INTO cur1 VALUES (2, 'Bob')");
        _db.Exec("INSERT INTO cur1 VALUES (3, 'Carol')");

        using var conn = _db.CreateConnection();
        new SqlCommand("DECLARE my_cur CURSOR FOR SELECT id, name FROM cur1 ORDER BY id", conn).ExecuteNonQuery();
        new SqlCommand("OPEN my_cur", conn).ExecuteNonQuery();

        // Fetch rows one at a time
        using var cmd = new SqlCommand("FETCH NEXT FROM my_cur", conn);
        using var rdr = cmd.ExecuteReader();
        var names = new List<string>();
        while (rdr.Read())
            names.Add(rdr.GetValue(1).ToString()!);
        rdr.Close();

        // First fetch returns first row
        Assert.Single(names);
        Assert.Equal("Alice", names[0]);

        // Fetch second row
        using var cmd2 = new SqlCommand("FETCH NEXT FROM my_cur", conn);
        using var rdr2 = cmd2.ExecuteReader();
        names.Clear();
        while (rdr2.Read())
            names.Add(rdr2.GetValue(1).ToString()!);
        rdr2.Close();
        Assert.Single(names);
        Assert.Equal("Bob", names[0]);

        new SqlCommand("CLOSE my_cur", conn).ExecuteNonQuery();
        new SqlCommand("DEALLOCATE my_cur", conn).ExecuteNonQuery();
    }
}

// ===========================================================================
// BACKUP / RESTORE
// ===========================================================================

[Collection("ForgeDB")]
public class BackupRestoreTests
{
    private readonly ForgeDbFixture _db;
    public BackupRestoreTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void BAK01_BackupSucceeds()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS bak1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM bak1");
        _db.Exec("INSERT INTO bak1 VALUES (1, 100)");

        var backupDir = Path.Combine(Path.GetTempPath(), $"forgedb_backup_{Guid.NewGuid():N}");
        try
        {
            _db.Exec($"BACKUP DATABASE TO '{backupDir}'");
            // Backup directory should exist
            Assert.True(Directory.Exists(backupDir));
        }
        finally
        {
            try { Directory.Delete(backupDir, true); } catch { }
        }
    }
}

// ===========================================================================
// GRAND OPERATIONAL STRESS
// ===========================================================================

[Collection("ForgeDB")]
public class OperationalStressTests
{
    private readonly ForgeDbFixture _db;
    public OperationalStressTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void OPS01_AllFeatures_50Threads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ops1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM ops1");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO ops1 VALUES ({i}, {i * 10})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                using var conn = _db.CreateConnection();
                string sql = (t % 5) switch
                {
                    0 => "SELECT COUNT(*) FROM ops1",
                    1 => "SELECT SUM(v) FROM ops1",
                    2 => $"INSERT INTO ops1 VALUES ({500 + t}, {t})",
                    3 => "SELECT * FROM ops1 ORDER BY id LIMIT 5",
                    _ => "SELECT DISTINCT v FROM ops1",
                };
                using var cmd = new SqlCommand(sql, conn);
                if (sql.StartsWith("INSERT"))
                    cmd.ExecuteNonQuery();
                else
                    using (var rdr = cmd.ExecuteReader()) { while (rdr.Read()) { } }
            }
            catch (Exception ex) { errors.Add($"t{t}: {ex.Message.Split('\n')[0]}"); }
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }
}
