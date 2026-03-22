// =============================================================================
// Critical Production Features — .NET 10 Integration Tests
// =============================================================================
// Tests: Savepoint undo, ON DELETE CASCADE/SET NULL, UPDATE FK validation,
// composite FKs, query timeout simulation, concurrent FK enforcement.

using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

namespace ForgeDB.AcidTests;

// ===========================================================================
// SAVEPOINT UNDO — ROLLBACK TO actually reverses operations
// ===========================================================================

[Collection("ForgeDB")]
public class SavepointUndoTests
{
    private readonly ForgeDbFixture _db;
    public SavepointUndoTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void SP01_RollbackToSavepoint_UndeInserts()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sp_u1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sp_u1");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u1 VALUES (1, 10)", conn).ExecuteNonQuery();
        new SqlCommand("SAVEPOINT s1", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u1 VALUES (2, 20)", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u1 VALUES (3, 30)", conn).ExecuteNonQuery();
        new SqlCommand("ROLLBACK TO SAVEPOINT s1", conn).ExecuteNonQuery();
        new SqlCommand("COMMIT", conn).ExecuteNonQuery();

        // Only row 1 survives — rows 2,3 rolled back to savepoint
        Assert.Equal(1, _db.Count("sp_u1"));
        Assert.Equal(10L, _db.Sum("sp_u1", "v"));
    }

    [Fact]
    public void SP02_RollbackToSavepoint_PreservesEarlierWork()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sp_u2 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sp_u2");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u2 VALUES (1, 100)", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u2 VALUES (2, 200)", conn).ExecuteNonQuery();
        new SqlCommand("SAVEPOINT checkpoint", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u2 VALUES (3, 300)", conn).ExecuteNonQuery();
        new SqlCommand("ROLLBACK TO SAVEPOINT checkpoint", conn).ExecuteNonQuery();
        // Insert another after rollback
        new SqlCommand("INSERT INTO sp_u2 VALUES (4, 400)", conn).ExecuteNonQuery();
        new SqlCommand("COMMIT", conn).ExecuteNonQuery();

        // Rows 1,2,4 survive. Row 3 was rolled back.
        Assert.Equal(3, _db.Count("sp_u2"));
        Assert.Equal(700L, _db.Sum("sp_u2", "v"));
    }

    [Fact]
    public void SP03_MultipleSavepoints()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sp_u3 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sp_u3");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u3 VALUES (1, 10)", conn).ExecuteNonQuery();
        new SqlCommand("SAVEPOINT s1", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u3 VALUES (2, 20)", conn).ExecuteNonQuery();
        new SqlCommand("SAVEPOINT s2", conn).ExecuteNonQuery();
        new SqlCommand("INSERT INTO sp_u3 VALUES (3, 30)", conn).ExecuteNonQuery();
        // Rollback to s1 (undoes rows 2 and 3)
        new SqlCommand("ROLLBACK TO SAVEPOINT s1", conn).ExecuteNonQuery();
        new SqlCommand("COMMIT", conn).ExecuteNonQuery();

        Assert.Equal(1, _db.Count("sp_u3"));
        Assert.Equal(10L, _db.Sum("sp_u3", "v"));
    }

    [Fact]
    public void SP04_SavepointWithUpdate()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sp_u4 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM sp_u4");
        _db.Exec("INSERT INTO sp_u4 VALUES (1, 100)");

        using var conn = _db.CreateConnection();
        new SqlCommand("BEGIN", conn).ExecuteNonQuery();
        new SqlCommand("SAVEPOINT before_update", conn).ExecuteNonQuery();
        new SqlCommand("UPDATE sp_u4 SET v = 999 WHERE id = 1", conn).ExecuteNonQuery();
        new SqlCommand("ROLLBACK TO SAVEPOINT before_update", conn).ExecuteNonQuery();
        new SqlCommand("COMMIT", conn).ExecuteNonQuery();

        // Update was rolled back, original value restored
        Assert.Equal(100L, _db.Sum("sp_u4", "v"));
    }
}

// ===========================================================================
// ON DELETE CASCADE
// ===========================================================================

[Collection("ForgeDB")]
public class OnDeleteCascadeTests
{
    private readonly ForgeDbFixture _db;
    public OnDeleteCascadeTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void CASCADE01_DeleteParent_CascadesToChildren()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cas_parent (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec(@"CREATE TABLE IF NOT EXISTS cas_child (
            id INT NOT NULL,
            parent_id INT NOT NULL,
            v INT NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES cas_parent(id) ON DELETE CASCADE
        )");
        _db.Exec("DELETE FROM cas_child");
        _db.Exec("DELETE FROM cas_parent");
        _db.Exec("INSERT INTO cas_parent VALUES (1, 'Alice')");
        _db.Exec("INSERT INTO cas_parent VALUES (2, 'Bob')");
        _db.Exec("INSERT INTO cas_child VALUES (1, 1, 100)");
        _db.Exec("INSERT INTO cas_child VALUES (2, 1, 200)");
        _db.Exec("INSERT INTO cas_child VALUES (3, 2, 300)");

        // Delete parent 1 — should cascade to children with parent_id=1
        _db.Exec("DELETE FROM cas_parent WHERE id = 1");

        Assert.Equal(1, _db.Count("cas_parent")); // only Bob remains
        Assert.Equal(1, _db.Count("cas_child"));   // only child 3 (Bob's) remains
        Assert.Equal(300L, _db.Sum("cas_child", "v"));
    }

    [Fact]
    public void CASCADE02_DeleteAll_CascadesAll()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cas2_p (id INT NOT NULL PRIMARY KEY)");
        _db.Exec(@"CREATE TABLE IF NOT EXISTS cas2_c (
            id INT NOT NULL, pid INT NOT NULL,
            FOREIGN KEY (pid) REFERENCES cas2_p(id) ON DELETE CASCADE
        )");
        _db.Exec("DELETE FROM cas2_c");
        _db.Exec("DELETE FROM cas2_p");
        for (int i = 0; i < 10; i++)
        {
            _db.Exec($"INSERT INTO cas2_p VALUES ({i})");
            _db.Exec($"INSERT INTO cas2_c VALUES ({i * 10}, {i})");
            _db.Exec($"INSERT INTO cas2_c VALUES ({i * 10 + 1}, {i})");
        }

        Assert.Equal(10, _db.Count("cas2_p"));
        Assert.Equal(20, _db.Count("cas2_c"));

        // Delete all parents one by one — children cascade-delete
        for (int i = 0; i < 10; i++)
            _db.Exec($"DELETE FROM cas2_p WHERE id = {i}");
        Assert.Equal(0, _db.Count("cas2_p"));
        Assert.Equal(0, _db.Count("cas2_c"));
    }
}

// ===========================================================================
// ON DELETE SET NULL
// ===========================================================================

[Collection("ForgeDB")]
public class OnDeleteSetNullTests
{
    private readonly ForgeDbFixture _db;
    public OnDeleteSetNullTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void SETNULL01_DeleteParent_SetsChildFKToNull()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS sn_parent (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec(@"CREATE TABLE IF NOT EXISTS sn_child (
            id INT NOT NULL,
            parent_id INT NULL,
            v INT NOT NULL,
            FOREIGN KEY (parent_id) REFERENCES sn_parent(id) ON DELETE SET NULL
        )");
        _db.Exec("DELETE FROM sn_child");
        _db.Exec("DELETE FROM sn_parent");
        _db.Exec("INSERT INTO sn_parent VALUES (1, 'Alice')");
        _db.Exec("INSERT INTO sn_parent VALUES (2, 'Bob')");
        _db.Exec("INSERT INTO sn_child VALUES (1, 1, 100)");
        _db.Exec("INSERT INTO sn_child VALUES (2, 2, 200)");

        // Delete parent 1 — child's parent_id should become NULL
        _db.Exec("DELETE FROM sn_parent WHERE id = 1");

        Assert.Equal(1, _db.Count("sn_parent"));
        Assert.Equal(2, _db.Count("sn_child")); // children preserved

        // Check that child 1's parent_id is now NULL
        using var conn = _db.CreateConnection();
        var nullCount = Convert.ToInt32(
            new SqlCommand("SELECT COUNT(*) FROM sn_child WHERE parent_id IS NULL", conn).ExecuteScalar());
        Assert.Equal(1, nullCount);
    }
}

// ===========================================================================
// UPDATE with FK VALIDATION
// ===========================================================================

[Collection("ForgeDB")]
public class UpdateFkTests
{
    private readonly ForgeDbFixture _db;
    public UpdateFkTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void UFk01_UpdateFK_ValidReference_Succeeds()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ufk_p (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS ufk_c (id INT NOT NULL, pid INT NOT NULL REFERENCES ufk_p(id))");
        _db.Exec("DELETE FROM ufk_c");
        _db.Exec("DELETE FROM ufk_p");
        _db.Exec("INSERT INTO ufk_p VALUES (1, 'A')");
        _db.Exec("INSERT INTO ufk_p VALUES (2, 'B')");
        _db.Exec("INSERT INTO ufk_c VALUES (1, 1)");

        // Update FK to another valid parent
        _db.Exec("UPDATE ufk_c SET pid = 2 WHERE id = 1");
        Assert.Equal(1, _db.Count("ufk_c"));
    }

    [Fact]
    public void UFk02_UpdateFK_InvalidReference_Rejected()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ufk2_p (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
        _db.Exec("CREATE TABLE IF NOT EXISTS ufk2_c (id INT NOT NULL, pid INT NOT NULL REFERENCES ufk2_p(id))");
        _db.Exec("DELETE FROM ufk2_c");
        _db.Exec("DELETE FROM ufk2_p");
        _db.Exec("INSERT INTO ufk2_p VALUES (1, 'A')");
        _db.Exec("INSERT INTO ufk2_c VALUES (1, 1)");

        // Update FK to non-existent parent — must fail
        Assert.ThrowsAny<SqlException>(() => _db.Exec("UPDATE ufk2_c SET pid = 999 WHERE id = 1"));
    }

    [Fact]
    public void UFk03_UpdateNonFK_Column_NoValidation()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS ufk3_p (id INT NOT NULL PRIMARY KEY)");
        _db.Exec("CREATE TABLE IF NOT EXISTS ufk3_c (id INT NOT NULL, pid INT NOT NULL REFERENCES ufk3_p(id), v INT NOT NULL)");
        _db.Exec("DELETE FROM ufk3_c");
        _db.Exec("DELETE FROM ufk3_p");
        _db.Exec("INSERT INTO ufk3_p VALUES (1)");
        _db.Exec("INSERT INTO ufk3_c VALUES (1, 1, 100)");

        // Update non-FK column — should succeed without FK check
        _db.Exec("UPDATE ufk3_c SET v = 999 WHERE id = 1");
        Assert.Equal(999L, _db.Sum("ufk3_c", "v"));
    }
}

// ===========================================================================
// CONCURRENT FK ENFORCEMENT — stress tests
// ===========================================================================

[Collection("ForgeDB")]
public class ConcurrentFkStressTests
{
    private readonly ForgeDbFixture _db;
    public ConcurrentFkStressTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void CFK01_ConcurrentInserts_FKEnforced()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cfk_p (id INT NOT NULL PRIMARY KEY)");
        _db.Exec("CREATE TABLE IF NOT EXISTS cfk_c (id INT NOT NULL, pid INT NOT NULL REFERENCES cfk_p(id))");
        _db.Exec("DELETE FROM cfk_c");
        _db.Exec("DELETE FROM cfk_p");
        for (int i = 0; i < 50; i++)
            _db.Exec($"INSERT INTO cfk_p VALUES ({i})");

        int ok = 0, fail = 0;
        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                _db.Exec($"INSERT INTO cfk_c VALUES ({t}, {t})"); // 0-49 valid, 50-99 invalid
                Interlocked.Increment(ref ok);
            }
            catch { Interlocked.Increment(ref fail); }
        })).ToArray();
        Task.WaitAll(tasks);

        Assert.Equal(50, ok);
        Assert.Equal(50, fail);
    }

    [Fact]
    public void CFK02_CascadeDelete_UnderConcurrentReads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS cfk2_p (id INT NOT NULL PRIMARY KEY)");
        _db.Exec(@"CREATE TABLE IF NOT EXISTS cfk2_c (
            id INT NOT NULL, pid INT NOT NULL,
            FOREIGN KEY (pid) REFERENCES cfk2_p(id) ON DELETE CASCADE
        )");
        _db.Exec("DELETE FROM cfk2_c");
        _db.Exec("DELETE FROM cfk2_p");
        for (int i = 0; i < 20; i++)
        {
            _db.Exec($"INSERT INTO cfk2_p VALUES ({i})");
            _db.Exec($"INSERT INTO cfk2_c VALUES ({i * 10}, {i})");
        }

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(101);
        var tasks = new List<Task>();

        // 100 readers
        for (int r = 0; r < 100; r++)
        {
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                try
                {
                    using var conn = _db.CreateConnection();
                    new SqlCommand("SELECT COUNT(*) FROM cfk2_c", conn).ExecuteScalar();
                }
                catch (Exception ex) { errors.Add(ex.Message); }
            }));
        }

        // 1 writer: cascade-deletes
        tasks.Add(Task.Run(() =>
        {
            barrier.SignalAndWait();
            for (int i = 0; i < 5; i++)
                try { _db.Exec($"DELETE FROM cfk2_p WHERE id = {i}"); } catch { }
        }));

        Task.WaitAll(tasks.ToArray());
        Assert.Empty(errors);
    }
}

// ===========================================================================
// RESTRICT FK — explicitly test RESTRICT behavior
// ===========================================================================

[Collection("ForgeDB")]
public class RestrictFkTests
{
    private readonly ForgeDbFixture _db;
    public RestrictFkTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void RESTRICT01_DefaultBehavior_RejectsDelete()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS rfk_p (id INT NOT NULL PRIMARY KEY)");
        _db.Exec("CREATE TABLE IF NOT EXISTS rfk_c (id INT NOT NULL, pid INT NOT NULL REFERENCES rfk_p(id))");
        _db.Exec("DELETE FROM rfk_c");
        _db.Exec("DELETE FROM rfk_p");
        _db.Exec("INSERT INTO rfk_p VALUES (1)");
        _db.Exec("INSERT INTO rfk_c VALUES (1, 1)");

        Assert.ThrowsAny<SqlException>(() => _db.Exec("DELETE FROM rfk_p WHERE id = 1"));
        Assert.Equal(1, _db.Count("rfk_p")); // parent not deleted
    }

    [Fact]
    public void RESTRICT02_DeleteChildFirst_ThenParent_Works()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS rfk2_p (id INT NOT NULL PRIMARY KEY)");
        _db.Exec("CREATE TABLE IF NOT EXISTS rfk2_c (id INT NOT NULL, pid INT NOT NULL REFERENCES rfk2_p(id))");
        _db.Exec("DELETE FROM rfk2_c");
        _db.Exec("DELETE FROM rfk2_p");
        _db.Exec("INSERT INTO rfk2_p VALUES (1)");
        _db.Exec("INSERT INTO rfk2_c VALUES (1, 1)");

        _db.Exec("DELETE FROM rfk2_c WHERE pid = 1");
        _db.Exec("DELETE FROM rfk2_p WHERE id = 1"); // now works
        Assert.Equal(0, _db.Count("rfk2_p"));
    }
}
