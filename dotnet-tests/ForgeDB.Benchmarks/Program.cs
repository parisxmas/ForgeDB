// =============================================================================
// ForgeDB vs PostgreSQL Benchmark — .NET 10
// =============================================================================
//
// Head-to-head comparison across 12 benchmark categories:
// Single-row INSERT, bulk INSERT, point SELECT, full scan, aggregation,
// JOIN, GROUP BY, UPDATE, DELETE, transaction throughput, concurrent
// read/write, and complex analytical queries.
//
// Each benchmark runs identical SQL on both databases, measures wall-clock
// time, and reports ops/sec with a comparison ratio.

using System.Collections.Concurrent;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Npgsql;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const int BENCH_ROWS = 5000;
const int CONCURRENT_CLIENTS = 20;
const int OPS_PER_CLIENT = 50;

var pgUser = Environment.GetEnvironmentVariable("USER") ?? "postgres";
var pgConnStr = $"Host=127.0.0.1;Port=5432;Database=forgedb_bench;Username={pgUser};Pooling=true;";

// Start ForgeDB TDS server
var forgeDbPath = Path.Combine(Path.GetTempPath(), $"forgedb_bench_{Random.Shared.Next()}");
Directory.CreateDirectory(forgeDbPath);
var projectRoot = FindProjectRoot();
var serverBin = Path.Combine(projectRoot, "target", "release", "forgedb-server");
if (!File.Exists(serverBin))
    serverBin = Path.Combine(projectRoot, "target", "debug", "forgedb-server");

var forgePort = Random.Shared.Next(14330, 15000);
var forgeProcess = new Process
{
    StartInfo = new ProcessStartInfo
    {
        FileName = serverBin,
        Arguments = $"{forgeDbPath} 0.0.0.0:0 0.0.0.0:{forgePort}",
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true,
    }
};
forgeProcess.Start();
var forgeConnStr = $"Server=127.0.0.1,{forgePort};User Id=sa;Password=x;Encrypt=false;TrustServerCertificate=True;Pooling=true;Connection Timeout=10;";

// Wait for ForgeDB
Thread.Sleep(1500);
for (int attempt = 0; attempt < 20; attempt++)
{
    try { using var c = new SqlConnection(forgeConnStr); c.Open(); break; }
    catch { Thread.Sleep(200); }
}

Console.WriteLine("╔══════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║            ForgeDB vs PostgreSQL — Benchmark Suite                   ║");
Console.WriteLine("╠══════════════════════════════════════════════════════════════════════╣");
Console.WriteLine($"║ ForgeDB:    TDS on port {forgePort,-42}       ║");
Console.WriteLine($"║ PostgreSQL: 15.x on port 5432{"",-39}       ║");
Console.WriteLine($"║ Rows:       {BENCH_ROWS,-48}       ║");
Console.WriteLine($"║ Clients:    {CONCURRENT_CLIENTS,-48}       ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════╝");
Console.WriteLine();

var results = new List<(string name, double forgeSec, double pgSec, double ratio)>();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

DbConnection ForgeConn()
{
    for (int i = 0; i < 3; i++)
    {
        try { var c = new SqlConnection(forgeConnStr); c.Open(); return c; }
        catch when (i < 2) { Thread.Sleep(50); }
    }
    var final_c = new SqlConnection(forgeConnStr); final_c.Open(); return final_c;
}

DbConnection PgConn()
{
    var c = new NpgsqlConnection(pgConnStr);
    c.Open();
    return c;
}

void ForgeExec(string sql)
{
    using var c = ForgeConn(); using var cmd = c.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery();
}

void PgExec(string sql)
{
    using var c = PgConn(); using var cmd = c.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery();
}

double Measure(Action action)
{
    GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
    var sw = Stopwatch.StartNew();
    action();
    sw.Stop();
    return sw.Elapsed.TotalSeconds;
}

void RunBench(string name, Action forgeAction, Action pgAction)
{
    Console.Write($"  {name,-45}");
    double forgeSec = Measure(forgeAction);
    double pgSec = Measure(pgAction);
    double ratio = pgSec > 0 ? forgeSec / pgSec : 0;
    string winner = ratio < 0.95 ? "ForgeDB" : ratio > 1.05 ? "PG" : "~TIE";
    string detail = ratio < 1.0 ? $"{1.0/ratio:F1}x faster" : $"{ratio:F1}x slower";
    Console.WriteLine($"F:{forgeSec,7:F3}s  PG:{pgSec,7:F3}s  [{winner} {detail}]");
    results.Add((name, forgeSec, pgSec, ratio));
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

Console.WriteLine("Setting up tables...");
ForgeExec("CREATE TABLE IF NOT EXISTS bench (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))");
ForgeExec("DELETE FROM bench");
ForgeExec("CREATE TABLE IF NOT EXISTS bench_join (id INT NOT NULL, bench_id INT NOT NULL, val INT NOT NULL)");
ForgeExec("DELETE FROM bench_join");

PgExec("DROP TABLE IF EXISTS bench_join CASCADE");
PgExec("DROP TABLE IF EXISTS bench CASCADE");
PgExec("CREATE TABLE bench (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))");
PgExec("CREATE TABLE bench_join (id INT NOT NULL, bench_id INT NOT NULL, val INT NOT NULL)");
Console.WriteLine("Running benchmarks...\n");

// ---------------------------------------------------------------------------
// 1. Single-row INSERT
// ---------------------------------------------------------------------------
RunBench($"1. Single-row INSERT ({BENCH_ROWS} rows)",
    () => { using var c = ForgeConn(); for (int i = 0; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bench VALUES ({i}, {i * 10}, 'name_{i}')"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PgConn(); for (int i = 0; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bench VALUES ({i}, {i * 10}, 'name_{i}')"; cmd.ExecuteNonQuery(); } }
);

// Populate join table
{ using var c = ForgeConn(); for (int i = 0; i < BENCH_ROWS / 2; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bench_join VALUES ({i}, {i % BENCH_ROWS}, {i * 5})"; cmd.ExecuteNonQuery(); } }
{ using var c = PgConn(); for (int i = 0; i < BENCH_ROWS / 2; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bench_join VALUES ({i}, {i % BENCH_ROWS}, {i * 5})"; cmd.ExecuteNonQuery(); } }

// ---------------------------------------------------------------------------
// 2. Point SELECT by PK
// ---------------------------------------------------------------------------
RunBench($"2. Point SELECT by PK ({BENCH_ROWS} lookups)",
    () => { using var c = ForgeConn(); for (int i = 0; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT v, name FROM bench WHERE id = {i}"; using var r = cmd.ExecuteReader(); while (r.Read()) { } } },
    () => { using var c = PgConn(); for (int i = 0; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT v, name FROM bench WHERE id = {i}"; using var r = cmd.ExecuteReader(); while (r.Read()) { } } }
);

// ---------------------------------------------------------------------------
// 3. Full table scan
// ---------------------------------------------------------------------------
RunBench("3. Full table scan (SELECT *)",
    () => { using var c = ForgeConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM bench"; using var r = cmd.ExecuteReader(); int n = 0; while (r.Read()) n++; },
    () => { using var c = PgConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM bench"; using var r = cmd.ExecuteReader(); int n = 0; while (r.Read()) n++; }
);

// ---------------------------------------------------------------------------
// 4. Aggregation
// ---------------------------------------------------------------------------
RunBench("4. Aggregate queries (COUNT/SUM/AVG/MIN/MAX)",
    () => { using var c = ForgeConn(); foreach (var a in new[] { "COUNT(*)", "SUM(v)", "AVG(v)", "MIN(v)", "MAX(v)" }) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT {a} FROM bench"; cmd.ExecuteScalar(); } },
    () => { using var c = PgConn(); foreach (var a in new[] { "COUNT(*)", "SUM(v)", "AVG(v)", "MIN(v)", "MAX(v)" }) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT {a} FROM bench"; cmd.ExecuteScalar(); } }
);

// ---------------------------------------------------------------------------
// 5. GROUP BY with HAVING
// ---------------------------------------------------------------------------
RunBench("5. GROUP BY with HAVING",
    () => { using var c = ForgeConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v % 100, COUNT(*), SUM(v) FROM bench GROUP BY v % 100 HAVING COUNT(*) > 0"; using var r = cmd.ExecuteReader(); while (r.Read()) { } },
    () => { using var c = PgConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v % 100, COUNT(*), SUM(v) FROM bench GROUP BY v % 100 HAVING COUNT(*) > 0"; using var r = cmd.ExecuteReader(); while (r.Read()) { } }
);

// ---------------------------------------------------------------------------
// 6. INNER JOIN
// ---------------------------------------------------------------------------
RunBench("6. INNER JOIN",
    () => { using var c = ForgeConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT bench.id, bench.name, bench_join.val FROM bench INNER JOIN bench_join ON bench.id = bench_join.bench_id"; using var r = cmd.ExecuteReader(); int n = 0; while (r.Read()) n++; },
    () => { using var c = PgConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT bench.id, bench.name, bench_join.val FROM bench INNER JOIN bench_join ON bench.id = bench_join.bench_id"; using var r = cmd.ExecuteReader(); int n = 0; while (r.Read()) n++; }
);

// ---------------------------------------------------------------------------
// 7. UPDATE
// ---------------------------------------------------------------------------
RunBench($"7. UPDATE ({BENCH_ROWS / 10} rows)",
    () => { using var c = ForgeConn(); for (int i = 0; i < BENCH_ROWS / 10; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"UPDATE bench SET v = v + 1 WHERE id = {i}"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PgConn(); for (int i = 0; i < BENCH_ROWS / 10; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"UPDATE bench SET v = v + 1 WHERE id = {i}"; cmd.ExecuteNonQuery(); } }
);

// ---------------------------------------------------------------------------
// 8. DELETE + re-INSERT
// ---------------------------------------------------------------------------
RunBench($"8. DELETE + re-INSERT ({BENCH_ROWS / 10} rows)",
    () => { using var c = ForgeConn(); for (int i = BENCH_ROWS - BENCH_ROWS/10; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"DELETE FROM bench WHERE id = {i}"; cmd.ExecuteNonQuery(); } for (int i = BENCH_ROWS - BENCH_ROWS/10; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bench VALUES ({i}, {i*10}, 'new_{i}')"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PgConn(); for (int i = BENCH_ROWS - BENCH_ROWS/10; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"DELETE FROM bench WHERE id = {i}"; cmd.ExecuteNonQuery(); } for (int i = BENCH_ROWS - BENCH_ROWS/10; i < BENCH_ROWS; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bench VALUES ({i}, {i*10}, 'new_{i}')"; cmd.ExecuteNonQuery(); } }
);

// ---------------------------------------------------------------------------
// 9. Transaction throughput
// ---------------------------------------------------------------------------
RunBench("9. Transaction throughput (1000 txns)",
    () => { using var c = ForgeConn(); for (int i = 0; i < 1000; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "BEGIN"; cmd.ExecuteNonQuery(); cmd.CommandText = $"UPDATE bench SET v = {i} WHERE id = {i % BENCH_ROWS}"; cmd.ExecuteNonQuery(); cmd.CommandText = "COMMIT"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PgConn(); for (int i = 0; i < 1000; i++) { using var tx = c.BeginTransaction(); using var cmd = c.CreateCommand(); cmd.Transaction = tx; cmd.CommandText = $"UPDATE bench SET v = {i} WHERE id = {i % BENCH_ROWS}"; cmd.ExecuteNonQuery(); tx.Commit(); } }
);

// ---------------------------------------------------------------------------
// 10. Concurrent reads
// ---------------------------------------------------------------------------
RunBench($"10. Concurrent reads ({CONCURRENT_CLIENTS}×{OPS_PER_CLIENT})",
    () => { var b = new Barrier(CONCURRENT_CLIENTS); Task.WaitAll(Enumerable.Range(0, CONCURRENT_CLIENTS).Select(_ => Task.Run(() => { b.SignalAndWait(); using var c = ForgeConn(); for (int i = 0; i < OPS_PER_CLIENT; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM bench"; cmd.ExecuteScalar(); } })).ToArray()); },
    () => { var b = new Barrier(CONCURRENT_CLIENTS); Task.WaitAll(Enumerable.Range(0, CONCURRENT_CLIENTS).Select(_ => Task.Run(() => { b.SignalAndWait(); using var c = PgConn(); for (int i = 0; i < OPS_PER_CLIENT; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM bench"; cmd.ExecuteScalar(); } })).ToArray()); }
);

// ---------------------------------------------------------------------------
// 11. Concurrent mixed R/W
// ---------------------------------------------------------------------------
RunBench($"11. Concurrent mixed R/W ({CONCURRENT_CLIENTS} clients)",
    () => { var b = new Barrier(CONCURRENT_CLIENTS); Task.WaitAll(Enumerable.Range(0, CONCURRENT_CLIENTS).Select(t => Task.Run(() => { b.SignalAndWait(); using var c = ForgeConn(); for (int i = 0; i < OPS_PER_CLIENT/2; i++) { using var cmd = c.CreateCommand(); if (t%2==0) { cmd.CommandText="SELECT COUNT(*) FROM bench"; cmd.ExecuteScalar(); } else { cmd.CommandText=$"UPDATE bench SET v=v+1 WHERE id={(t*100+i)%BENCH_ROWS}"; cmd.ExecuteNonQuery(); } } })).ToArray()); },
    () => { var b = new Barrier(CONCURRENT_CLIENTS); Task.WaitAll(Enumerable.Range(0, CONCURRENT_CLIENTS).Select(t => Task.Run(() => { b.SignalAndWait(); using var c = PgConn(); for (int i = 0; i < OPS_PER_CLIENT/2; i++) { using var cmd = c.CreateCommand(); if (t%2==0) { cmd.CommandText="SELECT COUNT(*) FROM bench"; cmd.ExecuteScalar(); } else { cmd.CommandText=$"UPDATE bench SET v=v+1 WHERE id={(t*100+i)%BENCH_ROWS}"; cmd.ExecuteNonQuery(); } } })).ToArray()); }
);

// ---------------------------------------------------------------------------
// 12. Complex analytical query
// ---------------------------------------------------------------------------
RunBench("12. Complex analytical query",
    () => { using var c = ForgeConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v % 10, COUNT(*), SUM(v), MIN(v), MAX(v) FROM bench WHERE v > 1000 GROUP BY v % 10 HAVING COUNT(*) > 5"; using var r = cmd.ExecuteReader(); while (r.Read()) { } },
    () => { using var c = PgConn(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v % 10, COUNT(*), SUM(v), MIN(v), MAX(v) FROM bench WHERE v > 1000 GROUP BY v % 10 HAVING COUNT(*) > 5"; using var r = cmd.ExecuteReader(); while (r.Read()) { } }
);

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("╔══════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║                          RESULTS SUMMARY                            ║");
Console.WriteLine("╠══════════════════════════════════════════════════════════════════════╣");
int fw = 0, pw = 0;
foreach (var (name, fs, ps, ratio) in results)
{
    string v;
    if (ratio < 0.95) { v = $"ForgeDB {1.0/ratio:F1}x faster"; fw++; }
    else if (ratio > 1.05) { v = $"PG {ratio:F1}x faster"; pw++; }
    else { v = "~TIE"; }
    Console.WriteLine($"║  {name,-40} {v,-26}║");
}
Console.WriteLine("╠══════════════════════════════════════════════════════════════════════╣");
Console.WriteLine($"║  ForgeDB wins: {fw}    PostgreSQL wins: {pw}    Ties: {results.Count-fw-pw,-14}║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════════════╝");

// Cleanup
try { forgeProcess.Kill(true); } catch { }
try { forgeProcess.WaitForExit(3000); } catch { }
try { Directory.Delete(forgeDbPath, true); } catch { }
try { PgExec("DROP TABLE IF EXISTS bench_join CASCADE"); PgExec("DROP TABLE IF EXISTS bench CASCADE"); } catch { }

static string FindProjectRoot()
{
    var dir = Directory.GetCurrentDirectory();
    while (dir != null) { if (File.Exists(Path.Combine(dir, "Cargo.toml"))) return dir; dir = Directory.GetParent(dir)?.FullName; }
    return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", ".."));
}
