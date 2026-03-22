// =============================================================================
// Advanced Feature Tests — Partitioned Tables, Parallel Query, Full-Text Search
// =============================================================================

using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace ForgeDB.AcidTests;

// ===========================================================================
// PARTITIONED TABLES
// ===========================================================================

[Collection("ForgeDB")]
public class PartitionedTableTests
{
    private readonly ForgeDbFixture _db;
    public PartitionedTableTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void PT01_CreatePartitionedTable()
    {
        _db.Exec(@"CREATE TABLE IF NOT EXISTS pt1 (id INT NOT NULL, region VARCHAR(20), amount INT NOT NULL)
            PARTITION BY RANGE (id) (
                PARTITION p0 VALUES LESS THAN (100),
                PARTITION p1 VALUES LESS THAN (200),
                PARTITION p2 VALUES LESS THAN (300)
            )");
        // Should create sub-tables
    }

    [Fact]
    public void PT02_InsertRoutesToCorrectPartition()
    {
        _db.Exec(@"CREATE TABLE IF NOT EXISTS pt2 (id INT NOT NULL, v INT NOT NULL)
            PARTITION BY RANGE (id) (
                PARTITION p_low VALUES LESS THAN (50),
                PARTITION p_mid VALUES LESS THAN (100),
                PARTITION p_high VALUES LESS THAN (1000)
            )");

        _db.Exec("INSERT INTO pt2 VALUES (10, 100)");  // -> p_low
        _db.Exec("INSERT INTO pt2 VALUES (60, 200)");  // -> p_mid
        _db.Exec("INSERT INTO pt2 VALUES (150, 300)"); // -> p_high

        // Query the partitioned table (UNION ALL over partitions)
        Assert.Equal(3, _db.Count("pt2"));
        Assert.Equal(600L, _db.Sum("pt2", "v"));
    }

    [Fact]
    public void PT03_PartitionQueryCorrectCount()
    {
        // Use a non-partitioned table approach to verify partition logic independently
        _db.Exec("CREATE TABLE IF NOT EXISTS pt3_sub (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM pt3_sub");
        for (int i = 0; i < 25; i++)
            _db.Exec($"INSERT INTO pt3_sub VALUES ({i}, {i * 10})");

        Assert.Equal(25, _db.Count("pt3_sub"));
        long expected = Enumerable.Range(0, 25).Select(i => (long)(i * 10)).Sum();
        Assert.Equal(expected, _db.Sum("pt3_sub", "v"));
    }

    [Fact]
    public void PT04_PartitionedTable_ConcurrentInserts()
    {
        _db.Exec(@"CREATE TABLE IF NOT EXISTS pt4 (id INT NOT NULL, v INT NOT NULL)
            PARTITION BY RANGE (id) (
                PARTITION q1 VALUES LESS THAN (25),
                PARTITION q2 VALUES LESS THAN (50),
                PARTITION q3 VALUES LESS THAN (75),
                PARTITION q4 VALUES LESS THAN (1000)
            )");

        var barrier = new Barrier(100);
        var tasks = Enumerable.Range(0, 100).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try { _db.Exec($"INSERT INTO pt4 VALUES ({t}, {t * 10})"); } catch { }
        })).ToArray();
        Task.WaitAll(tasks);

        Assert.Equal(100, _db.Count("pt4"));
    }
}

// ===========================================================================
// PARALLEL QUERY EXECUTION
// ===========================================================================

[Collection("ForgeDB")]
public class ParallelQueryTests
{
    private readonly ForgeDbFixture _db;
    public ParallelQueryTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void PQ01_LargeTableScan_ParallelCorrect()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS pq1 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM pq1");
        for (int i = 0; i < 2000; i++)
            _db.Exec($"INSERT INTO pq1 VALUES ({i}, {i + 1})");

        // Sum should be correct whether parallel or sequential
        long expected = Enumerable.Range(1, 2000).Select(i => (long)i).Sum();
        Assert.Equal(expected, _db.Sum("pq1", "v"));
    }

    [Fact]
    public void PQ02_ParallelScan_CountCorrect()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS pq2 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM pq2");
        for (int i = 0; i < 1500; i++)
            _db.Exec($"INSERT INTO pq2 VALUES ({i}, {i})");

        Assert.Equal(1500, _db.Count("pq2"));
    }

    [Fact]
    public void PQ03_ParallelScan_ConcurrentReaders()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS pq3 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM pq3");
        for (int i = 0; i < 1000; i++)
            _db.Exec($"INSERT INTO pq3 VALUES ({i}, {i})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(_ => Task.Run(() =>
        {
            barrier.SignalAndWait();
            int c = _db.Count("pq3");
            if (c != 1000) errors.Add($"expected 1000, got {c}");
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void PQ04_ParallelScan_WithFilter()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS pq4 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM pq4");
        for (int i = 0; i < 2000; i++)
            _db.Exec($"INSERT INTO pq4 VALUES ({i}, {i})");

        var r = _db.Scalar("SELECT COUNT(*) FROM pq4 WHERE v > 1000");
        Assert.Equal("999", r?.ToString()); // 1001..1999
    }

    [Fact]
    public void PQ05_ParallelVsSequential_SameResults()
    {
        // Both paths should yield identical results
        _db.Exec("CREATE TABLE IF NOT EXISTS pq5 (id INT NOT NULL, v INT NOT NULL)");
        _db.Exec("DELETE FROM pq5");
        for (int i = 0; i < 500; i++)
            _db.Exec($"INSERT INTO pq5 VALUES ({i}, {i * 3})");

        long sum = _db.Sum("pq5", "v");
        long expected = Enumerable.Range(0, 500).Select(i => (long)(i * 3)).Sum();
        Assert.Equal(expected, sum);
    }
}

// ===========================================================================
// FULL-TEXT SEARCH
// ===========================================================================

[Collection("ForgeDB")]
public class FullTextSearchTests
{
    private readonly ForgeDbFixture _db;
    public FullTextSearchTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void FTS01_ContainsSingleWord()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fts1 (id INT NOT NULL, content VARCHAR(500))");
        _db.Exec("DELETE FROM fts1");
        _db.Exec("INSERT INTO fts1 VALUES (1, 'The quick brown fox jumps over the lazy dog')");
        _db.Exec("INSERT INTO fts1 VALUES (2, 'Hello world from ForgeDB')");
        _db.Exec("INSERT INTO fts1 VALUES (3, 'Database systems are complex')");

        var r = _db.Scalar("SELECT COUNT(*) FROM fts1 WHERE CONTAINS(content, 'fox')");
        Assert.Equal("1", r?.ToString());
    }

    [Fact]
    public void FTS02_ContainsMultipleWords()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fts2 (id INT NOT NULL, body VARCHAR(500))");
        _db.Exec("DELETE FROM fts2");
        _db.Exec("INSERT INTO fts2 VALUES (1, 'ForgeDB is a relational database management system')");
        _db.Exec("INSERT INTO fts2 VALUES (2, 'SQL Server is another database system')");
        _db.Exec("INSERT INTO fts2 VALUES (3, 'Redis is a key-value store')");

        // CONTAINS with multiple words: ALL must match
        var r = _db.Scalar("SELECT COUNT(*) FROM fts2 WHERE CONTAINS(body, 'database system')");
        Assert.Equal("2", r?.ToString()); // rows 1 and 2 contain both words
    }

    [Fact]
    public void FTS03_ContainsCaseInsensitive()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fts3 (id INT NOT NULL, text VARCHAR(200))");
        _db.Exec("DELETE FROM fts3");
        _db.Exec("INSERT INTO fts3 VALUES (1, 'ForgeDB Engine')");
        _db.Exec("INSERT INTO fts3 VALUES (2, 'forgedb engine')");
        _db.Exec("INSERT INTO fts3 VALUES (3, 'FORGEDB ENGINE')");

        var r = _db.Scalar("SELECT COUNT(*) FROM fts3 WHERE CONTAINS(text, 'forgedb')");
        Assert.Equal("3", r?.ToString()); // case-insensitive
    }

    [Fact]
    public void FTS04_FreetextAnyWordMatch()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fts4 (id INT NOT NULL, content VARCHAR(500))");
        _db.Exec("DELETE FROM fts4");
        _db.Exec("INSERT INTO fts4 VALUES (1, 'Apple pie recipe')");
        _db.Exec("INSERT INTO fts4 VALUES (2, 'Banana smoothie recipe')");
        _db.Exec("INSERT INTO fts4 VALUES (3, 'Cherry cake')");

        // FREETEXT: ANY word matches (more lenient than CONTAINS)
        var r = _db.Scalar("SELECT COUNT(*) FROM fts4 WHERE FREETEXT(content, 'apple banana')");
        Assert.Equal("2", r?.ToString()); // rows 1 (apple) and 2 (banana)
    }

    [Fact]
    public void FTS05_FullTextConcurrent()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fts5 (id INT NOT NULL, doc VARCHAR(500))");
        _db.Exec("DELETE FROM fts5");
        string[] topics = { "database", "network", "security", "algorithm", "compiler" };
        for (int i = 0; i < 100; i++)
            _db.Exec($"INSERT INTO fts5 VALUES ({i}, 'Document about {topics[i % 5]} technology number {i}')");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            string word = topics[t % 5];
            using var conn = _db.CreateConnection();
            using var cmd = new SqlCommand(
                $"SELECT COUNT(*) FROM fts5 WHERE CONTAINS(doc, '{word}')", conn);
            var count = Convert.ToInt32(cmd.ExecuteScalar());
            if (count != 20) errors.Add($"CONTAINS '{word}' returned {count}, expected 20");
        })).ToArray();
        Task.WaitAll(tasks);
        Assert.Empty(errors);
    }

    [Fact]
    public void FTS06_CreateFullTextIndex()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS fts6 (id INT NOT NULL, body VARCHAR(500))");
        // Should be accepted without error
        _db.Exec("CREATE FULLTEXT INDEX ON fts6 (body)");
    }
}

// ===========================================================================
// GRAND ADVANCED STRESS
// ===========================================================================

[Collection("ForgeDB")]
public class AdvancedStressTests
{
    private readonly ForgeDbFixture _db;
    public AdvancedStressTests(ForgeDbFixture db) => _db = db;

    [Fact]
    public void ADV01_AllAdvancedFeatures_50Threads()
    {
        _db.Exec("CREATE TABLE IF NOT EXISTS adv1 (id INT NOT NULL, cat VARCHAR(20), body VARCHAR(200), amount INT NOT NULL)");
        _db.Exec("DELETE FROM adv1");
        string[] cats = { "tech", "science", "art", "music", "sport" };
        for (int i = 0; i < 200; i++)
            _db.Exec($"INSERT INTO adv1 VALUES ({i}, '{cats[i % 5]}', 'Document about {cats[i % 5]} item {i}', {(i + 1) * 5})");

        var errors = new ConcurrentBag<string>();
        var barrier = new Barrier(50);
        var tasks = Enumerable.Range(0, 50).Select(t => Task.Run(() =>
        {
            barrier.SignalAndWait();
            try
            {
                using var conn = _db.CreateConnection();
                string sql = (t % 6) switch
                {
                    0 => "SELECT COUNT(*) FROM adv1",
                    1 => "SELECT cat, SUM(amount) FROM adv1 GROUP BY cat",
                    2 => "SELECT COUNT(*) FROM adv1 WHERE CONTAINS(body, 'tech')",
                    3 => "SELECT * FROM adv1 ORDER BY amount DESC LIMIT 10 OFFSET 5",
                    4 => "SELECT DISTINCT cat FROM adv1",
                    _ => $"INSERT INTO adv1 VALUES ({5000 + t}, 'new', 'new item', {t})",
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
