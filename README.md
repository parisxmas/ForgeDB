# ForgeDB

A production-grade relational database engine written from scratch in Rust. **4.2 MB** single binary with three wire protocols: ForgeWire (native binary), MySQL, and SQL Server (TDS).

**Beats PostgreSQL 15** on 9 out of 20 benchmarks via ForgeWire protocol — up to **112x faster** on concurrent mixed workloads, **10x faster** on aggregates, and **10x faster** on batch inserts.

## Performance vs PostgreSQL 15

ForgeWire (native binary protocol) head-to-head, 5000 rows, 20 concurrent clients:

| # | Benchmark | ForgeDB | PostgreSQL | Result |
|---|-----------|---------|------------|--------|
| 1 | Single-row INSERT (5000) | 2.005s | 0.399s | PG 5.0x faster |
| 2 | Batch INSERT (5000, 1 round-trip) | 0.035s | 0.352s | **ForgeDB 10.0x faster** |
| 3 | Point SELECT by PK (5000) | 0.162s | 0.172s | **ForgeDB 1.1x faster** |
| 4 | Point SELECT reuse cmd (5000) | 0.041s | 0.175s | **ForgeDB 4.3x faster** |
| 5 | COUNT(*) × 100 | 0.002s | 0.014s | **ForgeDB 5.8x faster** |
| 6 | SUM(v) × 100 | 0.002s | 0.016s | **ForgeDB 8.2x faster** |
| 7 | MIN/MAX(v) × 100 | 0.003s | 0.033s | **ForgeDB 10.1x faster** |
| 8 | AVG(v) × 100 | 0.002s | 0.018s | **ForgeDB 9.3x faster** |
| 9 | Full table scan (SELECT *) | 0.004s | 0.001s | PG 5.2x faster |
| 10 | INNER JOIN | 0.002s | 0.001s | PG 2.2x faster |
| 11 | LEFT JOIN | 0.005s | 0.001s | PG 5.0x faster |
| 12 | Subquery (IN subquery) | 0.001s | 0.000s | PG 3.0x faster |
| 13 | GROUP BY + HAVING | 0.001s | 0.001s | PG 2.9x faster |
| 14 | Complex analytical | 0.001s | 0.001s | PG 2.9x faster |
| 15 | DISTINCT + ORDER BY | 0.003s | 0.001s | PG 2.6x faster |
| 16 | CASE expression | 0.005s | 0.001s | PG 7.6x faster |
| 17 | UPDATE by PK (1000) | 0.105s | 0.085s | PG 1.2x faster |
| 18 | Transaction throughput (1000) | 0.151s | 0.105s | PG 1.4x faster |
| 19 | Concurrent reads (20×50) | 0.006s | 0.032s | **ForgeDB 5.3x faster** |
| 20 | Concurrent mixed R/W (20 clients) | 0.056s | 6.323s | **ForgeDB 112.8x faster** |

**ForgeDB wins: 9 | PostgreSQL wins: 11**

## Features

### SQL Engine
- Full DML: INSERT, SELECT, UPDATE, DELETE with complex WHERE clauses
- JOINs: INNER, LEFT, RIGHT, FULL OUTER, CROSS, self-join
- Aggregates: COUNT, SUM, AVG, MIN, MAX with GROUP BY / HAVING
- Subqueries: IN, NOT IN, EXISTS, NOT EXISTS, scalar subqueries
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE
- CTEs (WITH ... AS), UNION / UNION ALL, DISTINCT, ORDER BY, LIMIT + OFFSET
- CASE expressions, CAST, COALESCE, LIKE, BETWEEN, IN list
- INSERT ... SELECT, TRUNCATE TABLE, INSERT ON DUPLICATE KEY UPDATE
- String functions: UPPER, LOWER, LENGTH, SUBSTRING, REPLACE, TRIM, CONCAT, REVERSE, LPAD, RPAD
- Math functions: ABS, ROUND, CEIL, FLOOR, SQRT, POWER, MOD, SIGN
- Full-text search: CONTAINS(), FREETEXT()

### Data Types
- INT, BIGINT, FLOAT, DECIMAL(p,s), BOOLEAN
- VARCHAR(n), TEXT, JSON, UUID
- DATE, TIME, DATETIME
- VARBINARY(n)

### ACID Transactions
- MVCC snapshot isolation with version headers (xmin/xmax)
- Write-Ahead Log (WAL) with crash recovery (redo-only)
- Undo log for ROLLBACK with physical tuple restoration
- BEGIN / COMMIT / ROLLBACK with session-level transactions
- Savepoints (SAVEPOINT / ROLLBACK TO / RELEASE)
- Row-level lock manager with deadlock detection (wait-for graph cycle detection)
- Configurable lock timeout (SET LOCK_TIMEOUT)

### Constraints & Integrity
- PRIMARY KEY (single and composite)
- UNIQUE constraints
- CHECK constraints (enforced on INSERT and UPDATE)
- FOREIGN KEY with ON DELETE CASCADE / SET NULL / RESTRICT
- NOT NULL with DEFAULT values
- AUTO_INCREMENT / IDENTITY

### Storage Engine
- 16KB page-based heap file storage with slotted pages
- Concurrent buffer pool with per-page RwLocks and atomic pin counts
- Lock-free page reads via `read_page_direct()` — single RwLock acquisition, no pin/unpin overhead
- B-tree secondary indexes (single and composite)
- Clustered B+ tree indexes on primary keys
- Overflow pages for large tuples (TOAST-style)
- COUNT(*) fast path: slot-counting without tuple deserialization
- SIMD-accelerated aggregates (ARM64 NEON) for SUM/MIN/MAX/filter

### Execution Engines
- **PostgreSQL-style hash join** — 32KB dense chunk arena, power-of-2 bucket array with linked list chains, Murmur3 hash stored per tuple, projection-aware encoding
- **Columnar hash join** — ColumnArray-based (Int32/Int64/Float64/Str) for zero-Value join path
- **Vectorized execution** — DuckDB-style 1024-tuple batch processing with DataChunk/ColumnVector
- **Volcano iterator model** — pull-based per-tuple streaming with SeqScan, Filter, Projection, Sort, GroupBy, Limit, Distinct operators
- **Aggregate fast path** — single-column deserialization with SIMD batch processing

### Query Optimizer
- Cost-based optimizer with table/column statistics (ANALYZE TABLE)
- Index scan selection via selectivity estimation
- Index-only scans when covering index available
- Automatic hash join selection for large tables
- Grace hash join with disk-based partitioning for huge datasets
- LIMIT pushdown into sequential scans
- Query plan cache (RwLock-based, bounded at 10K entries)
- SQL parse cache (hash-based, 50K entries)
- Fast-path SQL dispatch: SELECT/INSERT/UPDATE/DELETE skip pre-parse overhead
- EXPLAIN plan output

### Wire Protocols
- **ForgeWire (native binary)** — 5-byte frame header, UTF-8 SQL, little-endian binary values, prepared statements, batch INSERT, TCP pipelining
- **MySQL protocol** — compatible with mysql CLI, DBeaver, WordPress
- **TDS protocol (SQL Server)** — compatible with Microsoft.Data.SqlClient, .NET applications
  - Binary encoding: SQLINT4/INT8 for integers, SQLFLT8 for floats, SQLBIT for booleans
  - NVARCHAR for strings with proper collation
  - Full PRELOGIN/LOGIN7 handshake, FEATUREEXTACK, sp_reset_connection
  - T-SQL rewriting: IDENTITY→AUTO_INCREMENT, TOP→LIMIT, ISNULL→COALESCE

### Operational Features
- Stored procedures (CREATE PROCEDURE / EXEC)
- Triggers (AFTER INSERT/UPDATE/DELETE)
- User management (CREATE USER / GRANT / REVOKE)
- Sequences (CREATE SEQUENCE / NEXTVAL)
- Prepared statements (PREPARE / EXECUTE)
- Cursors (DECLARE / OPEN / FETCH NEXT / CLOSE / DEALLOCATE)
- Backup / Restore (BACKUP DATABASE TO / RESTORE DATABASE FROM)
- Partitioned tables (PARTITION BY RANGE)
- Temp tables with connection-scoped lifecycle
- Multiple database support (CREATE DATABASE / USE)
- Query plan caching with DDL invalidation

## Quick Start

```bash
# Build
cargo build --release

# Start server (MySQL on 3307, TDS on 1433, ForgeWire on 15433)
./target/release/forgedb-server ./data

# Connect via SQL Server client
sqlcmd -S 127.0.0.1,1433 -U sa -P x -C

# Connect via MySQL client
mysql -h 127.0.0.1 -P 3307
```

## .NET Integration

### ForgeWire (native, fastest)
```csharp
using ForgeDB.Client;

var conn = new ForgeConnection("Host=127.0.0.1;Port=15433");
conn.Open();

var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT COUNT(*) FROM users";
var count = cmd.ExecuteScalar();

// Batch insert — single round-trip for N rows
conn.BatchInsert("users", new object?[][] {
    new object?[] { 1, "Alice" },
    new object?[] { 2, "Bob" },
});
```

### SQL Server (TDS)
```csharp
using Microsoft.Data.SqlClient;

var conn = new SqlConnection("Server=127.0.0.1,1433;User Id=sa;Password=x;Encrypt=false;TrustServerCertificate=True;");
conn.Open();

new SqlCommand("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))", conn).ExecuteNonQuery();
new SqlCommand("INSERT INTO users VALUES (1, 'Alice')", conn).ExecuteNonQuery();

using var reader = new SqlCommand("SELECT * FROM users", conn).ExecuteReader();
while (reader.Read())
    Console.WriteLine($"{reader.GetInt32(0)}: {reader.GetValue(1)}");
```

## Testing

```bash
# 572 Rust tests
cargo test

# .NET integration tests
cd dotnet-tests/ForgeDB.AcidTests
dotnet test

# Benchmarks vs PostgreSQL (ForgeWire only)
cd dotnet-tests/ForgeDB.Benchmarks
dotnet run -c Release -- --forgewire-only
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Wire Protocols (ForgeWire + MySQL + TDS)        │
├─────────────────────────────────────────────────┤
│  SQL Parser (sqlparser + fast-path INSERT cache) │
├─────────────────────────────────────────────────┤
│  Query Planner (cost-based + plan cache)         │
├─────────────────────────────────────────────────┤
│  Execution Engines                               │
│  ┌──────────┬──────────┬───────────┬──────────┐ │
│  │ PG Hash  │ Columnar │ Vectorized│ Volcano  │ │
│  │  Join    │   Join   │  (batch)  │(iterator)│ │
│  └──────────┴──────────┴───────────┴──────────┘ │
├─────────────────────────────────────────────────┤
│  Transaction Manager (MVCC + WAL + Undo)         │
├─────────────────────────────────────────────────┤
│  Lock Manager (row-level + deadlock detection)   │
├─────────────────────────────────────────────────┤
│  Buffer Pool (RwLock meta + atomic pin counts)   │
├─────────────────────────────────────────────────┤
│  Storage (heap files + B-tree + overflow/TOAST)  │
├─────────────────────────────────────────────────┤
│  Disk Manager (16KB pages)                       │
└─────────────────────────────────────────────────┘
```

## Binary Size

```
$ ls -lh target/release/forgedb-server
4.2M  forgedb-server
```

Full RDBMS with three wire protocols, four execution engines, and SIMD acceleration in a 4.2 MB binary.

## License

MIT
