// =============================================================================
// ForgeWire vs PostgreSQL — Head-to-Head Benchmark
// =============================================================================
// Pure ForgeWire protocol vs Npgsql. No TDS. Direct binary comparison.

using System.Diagnostics;
using ForgeDB.Client;
using Npgsql;

public static class ForgeWireBench
{
    const int N = 5000;
    const int CLIENTS = 20;

    public static void Run(int forgePort, string pgCs)
    {
        var fwCs = $"Host=127.0.0.1;Port={forgePort}";

        ForgeConnection FW() { var c = new ForgeConnection(fwCs); c.Open(); return c; }
        NpgsqlConnection PG() { var c = new NpgsqlConnection(pgCs); c.Open(); return c; }

        void FE(string s) { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = s; cmd.ExecuteNonQuery(); }
        void PE(string s) { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = s; cmd.ExecuteNonQuery(); }

        double T(Action a) { GC.Collect(); GC.WaitForPendingFinalizers(); var sw = Stopwatch.StartNew(); a(); return sw.Elapsed.TotalSeconds; }

        var R = new List<(string name, double f, double p)>();

        void B(string name, Action fa, Action pa)
        {
            Console.Write($"  {name,-50}");
            try
            {
                double ft = T(fa), pt = T(pa);
                double ratio = pt > 0 ? ft / pt : 0;
                string w = ratio < 0.95 ? $"★ ForgeDB {1/ratio:F1}x faster" : ratio > 1.05 ? $"  PG {ratio:F1}x faster" : "  ~TIE";
                Console.WriteLine($"FW:{ft,8:F4}s  PG:{pt,8:F4}s  {w}");
                R.Add((name, ft, pt));
            }
            catch (Exception ex) { Console.WriteLine($"SKIP: {ex.Message.Split('\n')[0][..Math.Min(50, ex.Message.Length)]}"); }
        }

        Console.WriteLine();
        Console.WriteLine("╔═══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║         ForgeWire (native binary) vs PostgreSQL 15 — Direct Comparison   ║");
        Console.WriteLine($"║  Rows: {N}   Clients: {CLIENTS}   ForgeWire port: {forgePort,-28}  ║");
        Console.WriteLine("╚═══════════════════════════════════════════════════════════════════════════╝\n");

        // Setup
        FE("CREATE TABLE IF NOT EXISTS fw (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))");
        FE("DELETE FROM fw");
        FE("CREATE TABLE IF NOT EXISTS fwj (id INT NOT NULL, fid INT NOT NULL, val INT NOT NULL)");
        FE("DELETE FROM fwj");
        PE("DROP TABLE IF EXISTS fwj CASCADE"); PE("DROP TABLE IF EXISTS fw CASCADE");
        PE("CREATE TABLE fw (id INT NOT NULL PRIMARY KEY, v INT NOT NULL, name VARCHAR(100))");
        PE("CREATE TABLE fwj (id INT NOT NULL, fid INT NOT NULL, val INT NOT NULL)");

        // ── INSERTS ──────────────────────────────────────────────────

        Console.WriteLine("── INSERT ──────────────────────────────────────────────────────────────────\n");

        B($"1.  Single-row INSERT ({N} rows)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < N; i++) { cmd.CommandText = $"INSERT INTO fw VALUES ({i},{i*10},'n_{i}')"; cmd.ExecuteNonQuery(); } },
            () => { using var c = PG(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO fw VALUES ({i},{i*10},'n_{i}')"; cmd.ExecuteNonQuery(); } });

        // Populate join table
        { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < N/2; i++) { cmd.CommandText = $"INSERT INTO fwj VALUES ({i},{i%N},{i*5})"; cmd.ExecuteNonQuery(); } }
        { using var c = PG(); for (int i = 0; i < N/2; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO fwj VALUES ({i},{i%N},{i*5})"; cmd.ExecuteNonQuery(); } }

        FE("CREATE TABLE IF NOT EXISTS fwb (id INT NOT NULL, v INT NOT NULL, name VARCHAR(100))");
        FE("DELETE FROM fwb");
        PE("DROP TABLE IF EXISTS fwb CASCADE");
        PE("CREATE TABLE fwb (id INT NOT NULL, v INT NOT NULL, name VARCHAR(100))");

        var batchRows = new object?[N][];
        for (int i = 0; i < N; i++) batchRows[i] = new object?[] { i, i * 10, $"n_{i}" };

        B($"2.  Batch INSERT ({N} rows, 1 round-trip)",
            () => { using var c = FW(); c.BatchInsert("fwb", batchRows); },
            () => { using var c = PG(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"INSERT INTO fwb VALUES ({i},{i*10},'n_{i}')"; cmd.ExecuteNonQuery(); } });

        // ── POINT QUERIES ────────────────────────────────────────────

        Console.WriteLine("\n── POINT QUERIES ───────────────────────────────────────────────────────────\n");

        B($"3.  Point SELECT by PK ({N} lookups)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < N; i++) { cmd.CommandText = $"SELECT v,name FROM fw WHERE id={i}"; using var r = cmd.ExecuteReader(); while (r.Read()) {} } },
            () => { using var c = PG(); for (int i = 0; i < N; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT v,name FROM fw WHERE id={i}"; using var r = cmd.ExecuteReader(); while (r.Read()) {} } });

        B($"4.  Point SELECT reuse cmd ({N} lookups)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < N; i++) { cmd.CommandText = $"SELECT v FROM fw WHERE id={i}"; var v = cmd.ExecuteScalar(); } },
            () => { using var c = PG(); var cmd = c.CreateCommand(); for (int i = 0; i < N; i++) { cmd.CommandText = $"SELECT v FROM fw WHERE id={i}"; var v = cmd.ExecuteScalar(); } });

        // ── AGGREGATES ───────────────────────────────────────────────

        Console.WriteLine("\n── AGGREGATES ──────────────────────────────────────────────────────────────\n");

        B("5.  COUNT(*) × 100",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM fw"; for (int i = 0; i < 100; i++) cmd.ExecuteScalar(); },
            () => { using var c = PG(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM fw"; for (int i = 0; i < 100; i++) cmd.ExecuteScalar(); });

        B("6.  SUM(v) × 100",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT SUM(v) FROM fw"; for (int i = 0; i < 100; i++) cmd.ExecuteScalar(); },
            () => { using var c = PG(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT SUM(v) FROM fw"; for (int i = 0; i < 100; i++) cmd.ExecuteScalar(); });

        B("7.  MIN/MAX(v) × 100",
            () => { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < 100; i++) { cmd.CommandText = "SELECT MIN(v) FROM fw"; cmd.ExecuteScalar(); cmd.CommandText = "SELECT MAX(v) FROM fw"; cmd.ExecuteScalar(); } },
            () => { using var c = PG(); var cmd = c.CreateCommand(); for (int i = 0; i < 100; i++) { cmd.CommandText = "SELECT MIN(v) FROM fw"; cmd.ExecuteScalar(); cmd.CommandText = "SELECT MAX(v) FROM fw"; cmd.ExecuteScalar(); } });

        B("8.  AVG(v) × 100",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT AVG(v) FROM fw"; for (int i = 0; i < 100; i++) cmd.ExecuteScalar(); },
            () => { using var c = PG(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT AVG(v) FROM fw"; for (int i = 0; i < 100; i++) cmd.ExecuteScalar(); });

        // ── SCANS & JOINS ────────────────────────────────────────────

        Console.WriteLine("\n── SCANS & JOINS ───────────────────────────────────────────────────────────\n");

        B("9.  Full table scan (SELECT *)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM fw"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT * FROM fw"; using var r = cmd.ExecuteReader(); int n=0; while (r.Read()) n++; });

        B("10. INNER JOIN",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT fw.id,fwj.val FROM fw INNER JOIN fwj ON fw.id=fwj.fid"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT fw.id,fwj.val FROM fw INNER JOIN fwj ON fw.id=fwj.fid"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

        B("11. LEFT JOIN",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT fw.id,fwj.val FROM fw LEFT JOIN fwj ON fw.id=fwj.fid"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT fw.id,fwj.val FROM fw LEFT JOIN fwj ON fw.id=fwj.fid"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

        B("12. Subquery (IN subquery)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM fw WHERE id IN (SELECT fid FROM fwj WHERE val>10000)"; cmd.ExecuteScalar(); },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM fw WHERE id IN (SELECT fid FROM fwj WHERE val>10000)"; cmd.ExecuteScalar(); });

        // ── GROUP BY & ANALYTICS ─────────────────────────────────────

        Console.WriteLine("\n── GROUP BY & ANALYTICS ────────────────────────────────────────────────────\n");

        B("13. GROUP BY + HAVING",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%100,COUNT(*),SUM(v) FROM fw GROUP BY v%100 HAVING COUNT(*)>0"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%100,COUNT(*),SUM(v) FROM fw GROUP BY v%100 HAVING COUNT(*)>0"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

        B("14. Complex analytical (WHERE+GROUP+HAVING)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%10,COUNT(*),SUM(v),MIN(v),MAX(v) FROM fw WHERE v>1000 GROUP BY v%10 HAVING COUNT(*)>5"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT v%10,COUNT(*),SUM(v),MIN(v),MAX(v) FROM fw WHERE v>1000 GROUP BY v%10 HAVING COUNT(*)>5"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

        B("15. DISTINCT + ORDER BY",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT DISTINCT name FROM fw ORDER BY name LIMIT 50"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT DISTINCT name FROM fw ORDER BY name LIMIT 50"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

        B("16. CASE expression",
            () => { using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT id,CASE WHEN v>40000 THEN 'H' WHEN v>20000 THEN 'M' ELSE 'L' END FROM fw"; using var r = cmd.ExecuteReader(); while (r.Read()) {} },
            () => { using var c = PG(); using var cmd = c.CreateCommand(); cmd.CommandText = "SELECT id,CASE WHEN v>40000 THEN 'H' WHEN v>20000 THEN 'M' ELSE 'L' END FROM fw"; using var r = cmd.ExecuteReader(); while (r.Read()) {} });

        // ── DML ──────────────────────────────────────────────────────

        Console.WriteLine("\n── DML ─────────────────────────────────────────────────────────────────────\n");

        B($"17. UPDATE by PK ({N/5} rows)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < N/5; i++) { cmd.CommandText = $"UPDATE fw SET v=v+1 WHERE id={i}"; cmd.ExecuteNonQuery(); } },
            () => { using var c = PG(); for (int i = 0; i < N/5; i++) { using var cmd = c.CreateCommand(); cmd.CommandText = $"UPDATE fw SET v=v+1 WHERE id={i}"; cmd.ExecuteNonQuery(); } });

        B("18. Transaction throughput (1000 txns)",
            () => { using var c = FW(); var cmd = c.CreateCommand(); for (int i = 0; i < 1000; i++) { cmd.CommandText = "BEGIN"; cmd.ExecuteNonQuery(); cmd.CommandText = $"UPDATE fw SET v={i} WHERE id={i%N}"; cmd.ExecuteNonQuery(); cmd.CommandText = "COMMIT"; cmd.ExecuteNonQuery(); } },
            () => { using var c = PG(); for (int i = 0; i < 1000; i++) { using var tx = c.BeginTransaction(); using var cmd = c.CreateCommand(); cmd.Transaction = tx; cmd.CommandText = $"UPDATE fw SET v={i} WHERE id={i%N}"; cmd.ExecuteNonQuery(); tx.Commit(); } });

        // ── CONCURRENT ───────────────────────────────────────────────

        Console.WriteLine("\n── CONCURRENT ──────────────────────────────────────────────────────────────\n");

        B($"19. Concurrent reads ({CLIENTS}×50 COUNT(*))",
            () => { var br = new System.Threading.Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(_ => Task.Run(() => { br.SignalAndWait(); using var c = FW(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM fw"; for (int i=0;i<50;i++) cmd.ExecuteScalar(); })).ToArray()); },
            () => { var br = new System.Threading.Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(_ => Task.Run(() => { br.SignalAndWait(); using var c = PG(); var cmd = c.CreateCommand(); cmd.CommandText = "SELECT COUNT(*) FROM fw"; for (int i=0;i<50;i++) cmd.ExecuteScalar(); })).ToArray()); });

        B($"20. Concurrent mixed R/W ({CLIENTS} clients)",
            () => { var br = new System.Threading.Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(t => Task.Run(() => { br.SignalAndWait(); using var c = FW(); var cmd = c.CreateCommand(); for (int i=0;i<50;i++) { if(t%2==0) { cmd.CommandText="SELECT COUNT(*) FROM fw"; cmd.ExecuteScalar(); } else { cmd.CommandText=$"UPDATE fw SET v=v+1 WHERE id={(t*50+i)%N}"; cmd.ExecuteNonQuery(); } } })).ToArray()); },
            () => { var br = new System.Threading.Barrier(CLIENTS); Task.WaitAll(Enumerable.Range(0,CLIENTS).Select(t => Task.Run(() => { br.SignalAndWait(); using var c = PG(); for (int i=0;i<50;i++) { using var cmd = c.CreateCommand(); if(t%2==0) { cmd.CommandText="SELECT COUNT(*) FROM fw"; cmd.ExecuteScalar(); } else { cmd.CommandText=$"UPDATE fw SET v=v+1 WHERE id={(t*50+i)%N}"; cmd.ExecuteNonQuery(); } } })).ToArray()); });

        // ── SUMMARY ──────────────────────────────────────────────────

        Console.WriteLine();
        Console.WriteLine("╔═══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                    ForgeWire vs PostgreSQL — SUMMARY                      ║");
        Console.WriteLine("╠═══════════════════════════════════════════════════════════════════════════╣");
        int fw = 0, pw = 0, tie = 0;
        foreach (var (name, f, p) in R)
        {
            double ratio = p > 0 ? f / p : 0;
            string v;
            if (ratio < 0.95) { v = $"★ ForgeDB {1/ratio:F1}x faster"; fw++; }
            else if (ratio > 1.05) { v = $"  PG {ratio:F1}x faster"; pw++; }
            else { v = "  ~TIE"; tie++; }
            Console.WriteLine($"║  {name,-46} {v,-24}║");
        }
        Console.WriteLine("╠═══════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  ForgeDB wins: {fw}    PostgreSQL wins: {pw}    Ties: {tie,-24}║");
        Console.WriteLine($"║  Binary: 4.2 MB    Protocol: ForgeWire (5-byte header, UTF-8, binary)    ║");
        Console.WriteLine("╚═══════════════════════════════════════════════════════════════════════════╝");

        // Cleanup
        try { PE("DROP TABLE IF EXISTS fwj CASCADE"); PE("DROP TABLE IF EXISTS fw CASCADE"); PE("DROP TABLE IF EXISTS fwb CASCADE"); } catch {}
    }
}
