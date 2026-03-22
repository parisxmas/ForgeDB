// =============================================================================
// ForgeDB vs PostgreSQL — Comprehensive Benchmark Suite (.NET 10)
// =============================================================================
// 20 benchmarks covering: INSERT, SELECT, UPDATE, DELETE, aggregation, JOIN,
// GROUP BY, transactions, concurrency, parameterized queries, analytical.
// Each runs identical SQL on both databases with wall-clock timing.

using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Npgsql;

const int N = 5000;
const int CLIENTS = 20;

var pgUser = Environment.GetEnvironmentVariable("USER") ?? "postgres";
var pgCs = $"Host=127.0.0.1;Port=5432;Database=forgedb_bench;Username={pgUser};Pooling=true;";

// Start ForgeDB
var dbPath = Path.Combine(Path.GetTempPath(), $"forgedb_bench_{Random.Shared.Next()}");
Directory.CreateDirectory(dbPath);
var root = FindProjectRoot();
var bin = Path.Combine(root, "target", "release", "forgedb-server");
if (!File.Exists(bin)) bin = Path.Combine(root, "target", "debug", "forgedb-server");
var port = Random.Shared.Next(14330, 15000);
var fwPort = port + 1000;
var srv = Process.Start(new ProcessStartInfo { FileName = bin, Arguments = $"{dbPath} 0.0.0.0:0 0.0.0.0:{port} 0.0.0.0:{fwPort}", RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true })!;
var fCs = $"Server=127.0.0.1,{port};User Id=sa;Password=x;Encrypt=false;TrustServerCertificate=True;Pooling=true;Connection Timeout=10;";
Thread.Sleep(1500);
for (int i = 0; i < 20; i++) { try { using var c = new SqlConnection(fCs); c.Open(); break; } catch { Thread.Sleep(200); } }

SqlConnection FC() { for (int i = 0; i < 3; i++) { try { var c = new SqlConnection(fCs); c.Open(); return c; } catch when (i < 2) { Thread.Sleep(50); } } var f = new SqlConnection(fCs); f.Open(); return f; }
NpgsqlConnection PC() { var c = new NpgsqlConnection(pgCs); c.Open(); return c; }
void FE(string s) { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = s; cmd.ExecuteNonQuery(); }
void PE(string s) { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = s; cmd.ExecuteNonQuery(); }
double T(Action a) { GC.Collect(); GC.WaitForPendingFinalizers(); var sw = Stopwatch.StartNew(); a(); return sw.Elapsed.TotalSeconds; }

var R = new List<(string name, double f, double p)>();
void B(string name, Action fa, Action pa)
{
    Console.Write($"  {name,-52}");
    try {
        double ft = T(fa), pt = T(pa);
        double ratio = pt > 0 ? ft / pt : 0;
        string w = ratio < 0.95 ? $"ForgeDB {1/ratio:F1}x faster" : ratio > 1.05 ? $"PG {ratio:F1}x faster" : "~TIE";
        Console.WriteLine($"F:{ft,7:F3}s PG:{pt,7:F3}s [{w}]");
        R.Add((name, ft, pt));
    } catch (Exception ex) {
        Console.WriteLine($"SKIP ({ex.Message.Split('\n')[0].Split('.').Last()})");
    }
}

// --forgewire-only: skip TDS benchmarks, run only ForgeWire vs PG
if (args.Contains("--forgewire-only"))
{
    ForgeWireBench.Run(fwPort, pgCs);
    try { srv.Kill(true); } catch {} try { srv.WaitForExit(3000); } catch {} srv.Dispose();
    try { Directory.Delete(dbPath, true); } catch {}
    return;
}

Console.WriteLine("╔════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║          ForgeDB vs PostgreSQL 15 — Full Benchmark Report              ║");
Console.WriteLine($"║  Rows: {N}  Clients: {CLIENTS}  ForgeDB port: {port,-30}║");
Console.WriteLine("╚════════════════════════════════════════════════════════════════════════╝\n");

// Setup
FE("CREATE TABLE IF NOT EXISTS b (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))");
FE("DELETE FROM b"); FE("CREATE TABLE IF NOT EXISTS bj (id INT NOT NULL, bid INT NOT NULL, val INT NOT NULL)"); FE("DELETE FROM bj");
PE("DROP TABLE IF EXISTS bj CASCADE"); PE("DROP TABLE IF EXISTS b CASCADE");
PE("CREATE TABLE b (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))");
PE("CREATE TABLE bj (id INT NOT NULL, bid INT NOT NULL, val INT NOT NULL)");

Console.WriteLine("── WRITE OPERATIONS ─────────────────────────────────────────────────────\n");

// 1. Single INSERT
B($"1.  Single-row INSERT ({N} rows)",
    () => { using var c = FC(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO b VALUES ({i},{i*10},'n_{i}')"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PC(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO b VALUES ({i},{i*10},'n_{i}')"; cmd.ExecuteNonQuery(); } });

// Populate join table
{ using var c = FC(); for (int i = 0; i < N/2; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bj VALUES ({i},{i%N},{i*5})"; cmd.ExecuteNonQuery(); } }
{ using var c = PC(); for (int i = 0; i < N/2; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bj VALUES ({i},{i%N},{i*5})"; cmd.ExecuteNonQuery(); } }

// 2. Parameterized INSERT
FE("CREATE TABLE IF NOT EXISTS bp (id INT NOT NULL, v INT NOT NULL, name VARCHAR(100))"); FE("DELETE FROM bp");
PE("DROP TABLE IF EXISTS bp CASCADE"); PE("CREATE TABLE bp (id INT NOT NULL, v INT NOT NULL, name VARCHAR(100))");
B($"2.  Parameterized INSERT ({N} rows)",
    () => { using var c = FC(); for (int i = 0; i < N; i++) { using var cmd = new SqlCommand("INSERT INTO bp VALUES (@a,@b,@c)", c); cmd.Parameters.Add("@a", SqlDbType.Int).Value = i; cmd.Parameters.Add("@b", SqlDbType.Int).Value = i*10; cmd.Parameters.Add("@c", SqlDbType.NVarChar, 100).Value = $"n_{i}"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PC(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "INSERT INTO bp VALUES ($1,$2,$3)"; cmd.Parameters.Add(new NpgsqlParameter { Value = i }); cmd.Parameters.Add(new NpgsqlParameter { Value = i*10 }); cmd.Parameters.Add(new NpgsqlParameter { Value = $"n_{i}" }); cmd.ExecuteNonQuery(); } });

// 3. Multi-row INSERT
FE("CREATE TABLE IF NOT EXISTS bm (id INT NOT NULL, v INT NOT NULL)"); FE("DELETE FROM bm");
PE("DROP TABLE IF EXISTS bm CASCADE"); PE("CREATE TABLE bm (id INT NOT NULL, v INT NOT NULL)");
B($"3.  Multi-row INSERT (100 batches × 50 rows)",
    () => { using var c = FC(); for (int b = 0; b < 100; b++) { var vals = string.Join(",", Enumerable.Range(b*50, 50).Select(i => $"({i},{i*10})")); using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bm VALUES {vals}"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PC(); for (int b = 0; b < 100; b++) { var vals = string.Join(",", Enumerable.Range(b*50, 50).Select(i => $"({i},{i*10})")); using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bm VALUES {vals}"; cmd.ExecuteNonQuery(); } });

// 4. UPDATE by PK
B($"4.  UPDATE by PK ({N/5} rows)",
    () => { using var c = FC(); for (int i = 0; i < N/5; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"UPDATE b SET v=v+1 WHERE id={i}"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PC(); for (int i = 0; i < N/5; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"UPDATE b SET v=v+1 WHERE id={i}"; cmd.ExecuteNonQuery(); } });

// 5. DELETE + re-INSERT
B($"5.  DELETE + re-INSERT ({N/5} rows)",
    () => { using var c = FC(); for (int i = N-N/5; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"DELETE FROM b WHERE id={i}"; cmd.ExecuteNonQuery(); } for (int i = N-N/5; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO b VALUES ({i},{i*10},'r_{i}')"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PC(); for (int i = N-N/5; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"DELETE FROM b WHERE id={i}"; cmd.ExecuteNonQuery(); } for (int i = N-N/5; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO b VALUES ({i},{i*10},'r_{i}')"; cmd.ExecuteNonQuery(); } });

// 6. Transaction throughput
B("6.  Transaction throughput (1000 BEGIN/UPDATE/COMMIT)",
    () => { using var c = FC(); for (int i = 0; i < 1000; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "BEGIN"; cmd.ExecuteNonQuery(); cmd.CommandText = $"UPDATE b SET v={i} WHERE id={i%N}"; cmd.ExecuteNonQuery(); cmd.CommandText = "COMMIT"; cmd.ExecuteNonQuery(); } },
    () => { using var c = PC(); for (int i = 0; i < 1000; i++) { using var tx = c.BeginTransaction(); using var cmd = c.CreateCommand(); cmd.Transaction = tx; cmd.CommandText = $"UPDATE b SET v={i} WHERE id={i%N}"; cmd.ExecuteNonQuery(); tx.Commit(); } });

Console.WriteLine("\n── READ OPERATIONS ──────────────────────────────────────────────────────\n");

// 7. Point SELECT by PK
B($"7.  Point SELECT by PK ({N} lookups)",
    () => { using var c = FC(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT v,name FROM b WHERE id={i}"; using var r = cmd.ExecuteReader(); while (r.Read()) {} } },
    () => { using var c = PC(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT v,name FROM b WHERE id={i}"; using var r = cmd.ExecuteReader(); while (r.Read()) {} } });

// 8. Full table scan
B("8.  Full table scan (SELECT *)",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM b"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM b"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; });

// 9. COUNT(*)
B("9.  COUNT(*)",
    () => { using var c = FC(); for (int i = 0; i < 100; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM b"; cmd.ExecuteScalar(); } },
    () => { using var c = PC(); for (int i = 0; i < 100; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM b"; cmd.ExecuteScalar(); } });

// 10. Aggregates
B("10. Aggregate queries (COUNT/SUM/AVG/MIN/MAX × 20)",
    () => { using var c = FC(); for (int i = 0; i < 20; i++) foreach (var a in new[]{"COUNT(*)","SUM(v)","AVG(v)","MIN(v)","MAX(v)"}) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT {a} FROM b"; cmd.ExecuteScalar(); } },
    () => { using var c = PC(); for (int i = 0; i < 20; i++) foreach (var a in new[]{"COUNT(*)","SUM(v)","AVG(v)","MIN(v)","MAX(v)"}) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT {a} FROM b"; cmd.ExecuteScalar(); } });

// 11. GROUP BY + HAVING
B("11. GROUP BY + HAVING",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%100,COUNT(*),SUM(v) FROM b GROUP BY v%100 HAVING COUNT(*)>0"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%100,COUNT(*),SUM(v) FROM b GROUP BY v%100 HAVING COUNT(*)>0"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

// 12. INNER JOIN
B("12. INNER JOIN",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT b.id,b.name,bj.val FROM b INNER JOIN bj ON b.id=bj.bid"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT b.id,b.name,bj.val FROM b INNER JOIN bj ON b.id=bj.bid"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; });

// 13. LEFT JOIN
B("13. LEFT JOIN",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT b.id,bj.val FROM b LEFT JOIN bj ON b.id=bj.bid"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT b.id,bj.val FROM b LEFT JOIN bj ON b.id=bj.bid"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; });

// 14. Complex analytical
B("14. Complex analytical (WHERE+GROUP BY+HAVING+ORDER BY)",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%10,COUNT(*),SUM(v),MIN(v),MAX(v) FROM b WHERE v>1000 GROUP BY v%10 HAVING COUNT(*)>5"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%10,COUNT(*),SUM(v),MIN(v),MAX(v) FROM b WHERE v>1000 GROUP BY v%10 HAVING COUNT(*)>5"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

// 15. Subquery
B("15. Subquery (IN subquery)",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM b WHERE id IN (SELECT bid FROM bj WHERE val>10000)"; cmd.ExecuteScalar(); },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM b WHERE id IN (SELECT bid FROM bj WHERE val>10000)"; cmd.ExecuteScalar(); });

// 16. DISTINCT + ORDER BY
B("16. DISTINCT + ORDER BY",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT DISTINCT name FROM b ORDER BY name"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT DISTINCT name FROM b ORDER BY name"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

// 17. CASE expression
B("17. CASE expression",
    () => { using var c = FC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT id,CASE WHEN v>40000 THEN 'H' WHEN v>20000 THEN 'M' ELSE 'L' END FROM b"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
    () => { using var c = PC(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT id,CASE WHEN v>40000 THEN 'H' WHEN v>20000 THEN 'M' ELSE 'L' END FROM b"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

Console.WriteLine("\n── CONCURRENCY ──────────────────────────────────────────────────────────\n");

// 18. Concurrent reads
B($"18. Concurrent reads ({CLIENTS} clients × 50 COUNT(*))",
    () => { var br = new Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(_ => Task.Run(() => { br.SignalAndWait(); using var c = FC(); for (int i = 0; i < 50; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM b"; cmd.ExecuteScalar(); } })).ToArray()); },
    () => { var br = new Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(_ => Task.Run(() => { br.SignalAndWait(); using var c = PC(); for (int i = 0; i < 50; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM b"; cmd.ExecuteScalar(); } })).ToArray()); });

// 19. Concurrent mixed R/W
B($"19. Concurrent mixed R/W ({CLIENTS} clients)",
    () => { var br = new Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(t => Task.Run(() => { br.SignalAndWait(); using var c = FC(); for (int i = 0; i < 50; i++) { using var cmd = c.CreateCommand(); if (t%2==0) { cmd.CommandText = "SELECT COUNT(*) FROM b"; cmd.ExecuteScalar(); } else { cmd.CommandText = $"UPDATE b SET v=v+1 WHERE id={(t*50+i)%N}"; cmd.ExecuteNonQuery(); } } })).ToArray()); },
    () => { var br = new Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(t => Task.Run(() => { br.SignalAndWait(); using var c = PC(); for (int i = 0; i < 50; i++) { using var cmd = c.CreateCommand(); if (t%2==0) { cmd.CommandText = "SELECT COUNT(*) FROM b"; cmd.ExecuteScalar(); } else { cmd.CommandText = $"UPDATE b SET v=v+1 WHERE id={(t*50+i)%N}"; cmd.ExecuteNonQuery(); } } })).ToArray()); });

// 20. Concurrent INSERT
FE("CREATE TABLE IF NOT EXISTS bc (id INT NOT NULL, tid INT NOT NULL)"); FE("DELETE FROM bc");
PE("DROP TABLE IF EXISTS bc CASCADE"); PE("CREATE TABLE bc (id INT NOT NULL, tid INT NOT NULL)");
B($"20. Concurrent INSERT ({CLIENTS} clients × 50 rows)",
    () => { var br = new Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(t => Task.Run(() => { br.SignalAndWait(); using var c = FC(); for (int i = 0; i < 50; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bc VALUES ({t*50+i},{t})"; cmd.ExecuteNonQuery(); } })).ToArray()); },
    () => { var br = new Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(t => Task.Run(() => { br.SignalAndWait(); using var c = PC(); for (int i = 0; i < 50; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO bc VALUES ({t*50+i},{t})"; cmd.ExecuteNonQuery(); } })).ToArray()); });

// ── ForgeWire vs TDS vs PostgreSQL ─────────────────────────────────────

Console.WriteLine("\n── FORGEWIRE PROTOCOL (native binary) ───────────────────────────────────\n");

var fwCs2 = $"Host=127.0.0.1;Port={fwPort}";

ForgeDB.Client.ForgeConnection? FWC() {
    try { var c = new ForgeDB.Client.ForgeConnection(fwCs2); c.Open(); return c; }
    catch { return null; }
}

var fwConn = FWC();
if (fwConn != null) {
    fwConn.Close();

    // Setup ForgeWire tables
    { using var c = FWC()!; var cmd = c.CreateCommand(); cmd.CommandText = "CREATE TABLE IF NOT EXISTS fw_b (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))"; cmd.ExecuteNonQuery(); cmd.CommandText = "DELETE FROM fw_b"; cmd.ExecuteNonQuery(); }

    // FW1. Single INSERT
    double fwInsert = T(() => { using var c = FWC()!; var cmd = c.CreateCommand(); for (int i = 0; i < N; i++) { cmd.CommandText = $"INSERT INTO fw_b VALUES ({i},{i*10},'n_{i}')"; cmd.ExecuteNonQuery(); } });
    double tdsInsert = R.FirstOrDefault(r => r.name.Contains("Single-row INSERT")).f;
    double pgInsert = R.FirstOrDefault(r => r.name.Contains("Single-row INSERT")).p;
    Console.WriteLine($"  FW1. INSERT {N} rows          ForgeWire:{fwInsert,7:F3}s  TDS:{tdsInsert,7:F3}s  PG:{pgInsert,7:F3}s");

    // FW2. Point SELECT
    double fwSelect = T(() => { using var c = FWC()!; var cmd = c.CreateCommand(); for (int i = 0; i < N; i++) { cmd.CommandText = $"SELECT v,name FROM fw_b WHERE id={i}"; using var r2 = cmd.ExecuteReader(); while (r2.Read()) {} } });
    double tdsSelect = R.FirstOrDefault(r => r.name.Contains("Point SELECT")).f;
    double pgSelect = R.FirstOrDefault(r => r.name.Contains("Point SELECT")).p;
    Console.WriteLine($"  FW2. Point SELECT {N}      ForgeWire:{fwSelect,7:F3}s  TDS:{tdsSelect,7:F3}s  PG:{pgSelect,7:F3}s");

    // FW3. COUNT(*)
    double fwCount = T(() => { using var c = FWC()!; var cmd = c.CreateCommand(); for (int i = 0; i < 100; i++) { cmd.CommandText = "SELECT COUNT(*) FROM fw_b"; cmd.ExecuteScalar(); } });
    double tdsCount = R.FirstOrDefault(r => r.name.Contains("COUNT(*)")).f;
    double pgCount = R.FirstOrDefault(r => r.name.Contains("COUNT(*)")).p;
    Console.WriteLine($"  FW3. COUNT(*) × 100       ForgeWire:{fwCount,7:F3}s  TDS:{tdsCount,7:F3}s  PG:{pgCount,7:F3}s");

    // FW4. Full scan
    double fwScan = T(() => { using var c = FWC()!; var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM fw_b"; using var r2 = cmd.ExecuteReader(); int n=0; while (r2.Read()) n++; });
    double tdsScan = R.FirstOrDefault(r => r.name.Contains("Full table scan")).f;
    double pgScan = R.FirstOrDefault(r => r.name.Contains("Full table scan")).p;
    Console.WriteLine($"  FW4. Full scan             ForgeWire:{fwScan,7:F3}s  TDS:{tdsScan,7:F3}s  PG:{pgScan,7:F3}s");

    // FW5. Aggregates
    double fwAgg = T(() => { using var c = FWC()!; var cmd = c.CreateCommand(); for (int i = 0; i < 20; i++) foreach (var a in new[]{"COUNT(*)","SUM(v)","AVG(v)","MIN(v)","MAX(v)"}) { cmd.CommandText = $"SELECT {a} FROM fw_b"; cmd.ExecuteScalar(); } });
    double tdsAgg = R.FirstOrDefault(r => r.name.Contains("Aggregate queries")).f;
    double pgAgg = R.FirstOrDefault(r => r.name.Contains("Aggregate queries")).p;
    Console.WriteLine($"  FW5. Aggregates × 100     ForgeWire:{fwAgg,7:F3}s  TDS:{tdsAgg,7:F3}s  PG:{pgAgg,7:F3}s");

    // FW6. UPDATE
    double fwUpd = T(() => { using var c = FWC()!; var cmd = c.CreateCommand(); for (int i = 0; i < N/5; i++) { cmd.CommandText = $"UPDATE fw_b SET v=v+1 WHERE id={i}"; cmd.ExecuteNonQuery(); } });
    double tdsUpd = R.FirstOrDefault(r => r.name.Contains("UPDATE by PK")).f;
    double pgUpd = R.FirstOrDefault(r => r.name.Contains("UPDATE by PK")).p;
    Console.WriteLine($"  FW6. UPDATE {N/5} rows       ForgeWire:{fwUpd,7:F3}s  TDS:{tdsUpd,7:F3}s  PG:{pgUpd,7:F3}s");

    // FW7. BATCH INSERT — one round-trip for all rows
    { using var c = FWC()!; var cmd = c.CreateCommand(); cmd.CommandText = "CREATE TABLE IF NOT EXISTS fw_batch (id INT NOT NULL, v INT NOT NULL, name VARCHAR(100))"; cmd.ExecuteNonQuery(); cmd.CommandText = "DELETE FROM fw_batch"; cmd.ExecuteNonQuery(); }
    var batchRows = new object?[N][];
    for (int i = 0; i < N; i++) batchRows[i] = new object?[] { i, i * 10, $"n_{i}" };

    double fwBatch = T(() => { using var c = FWC()!; c.BatchInsert("fw_batch", batchRows); });
    Console.WriteLine($"  FW7. BATCH INSERT {N} rows  ForgeWire:{fwBatch,7:F3}s  TDS:{tdsInsert,7:F3}s  PG:{pgInsert,7:F3}s");
    Console.WriteLine($"       Batch vs single:      {fwInsert/fwBatch:F1}x faster than ForgeWire single");
    Console.WriteLine($"       Batch vs PG single:   {(pgInsert/fwBatch > 1 ? $"ForgeDB {pgInsert/fwBatch:F1}x faster ★" : $"PG {fwBatch/pgInsert:F1}x faster")}");

    Console.WriteLine();
    Console.WriteLine($"  ForgeWire vs TDS speedup:  INSERT {tdsInsert/fwInsert:F1}x  SELECT {tdsSelect/fwSelect:F1}x  COUNT {tdsCount/fwCount:F1}x  UPDATE {tdsUpd/fwUpd:F1}x");
    Console.WriteLine($"  ForgeWire vs PG:           INSERT {pgInsert/fwInsert:F1}x  SELECT {pgSelect/fwSelect:F1}x  COUNT {pgCount/fwCount:F1}x  UPDATE {pgUpd/fwUpd:F1}x");
    Console.WriteLine($"  BATCH INSERT vs PG:        {(pgInsert/fwBatch > 1 ? $"ForgeDB {pgInsert/fwBatch:F1}x faster ★" : $"PG {fwBatch/pgInsert:F1}x faster")}");
} else {
    Console.WriteLine("  ForgeWire server not available on port 5433 — skipped");
}

// Summary
Console.WriteLine("\n╔════════════════════════════════════════════════════════════════════════╗");
Console.WriteLine("║                         FULL RESULTS SUMMARY                           ║");
Console.WriteLine("╠════════════════════════════════════════════════════════════════════════╣");
int fw2=0, pw=0, tie=0;
foreach (var (name, f, p) in R) {
    double ratio = p > 0 ? f / p : 0;
    string v;
    if (ratio < 0.95) { v = $"ForgeDB {1/ratio:F1}x faster ★"; fw2++; }
    else if (ratio > 1.05) { v = $"PG {ratio:F1}x faster"; pw++; }
    else { v = "~TIE"; tie++; }
    Console.WriteLine($"║  {name,-48} {v,-22}║");
}
Console.WriteLine("╠════════════════════════════════════════════════════════════════════════╣");
Console.WriteLine($"║  ForgeDB wins: {fw2}    PostgreSQL wins: {pw}    Ties: {tie,-21}║");
Console.WriteLine("╚════════════════════════════════════════════════════════════════════════╝");

// ── ForgeWire dedicated benchmark ──
if (args.Length == 0 || args.Contains("--forgewire"))
{
    ForgeWireBench.Run(fwPort, pgCs);
}

try { srv.Kill(true); } catch {} try { srv.WaitForExit(3000); } catch {} srv.Dispose();
try { Directory.Delete(dbPath, true); } catch {}
try { PE("DROP TABLE IF EXISTS bj CASCADE"); PE("DROP TABLE IF EXISTS b CASCADE"); PE("DROP TABLE IF EXISTS bp CASCADE"); PE("DROP TABLE IF EXISTS bm CASCADE"); PE("DROP TABLE IF EXISTS bc CASCADE"); } catch {}

static string FindProjectRoot() { var d = Directory.GetCurrentDirectory(); while (d != null) { if (File.Exists(Path.Combine(d, "Cargo.toml"))) return d; d = Directory.GetParent(d)?.FullName; } return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "..")); }
